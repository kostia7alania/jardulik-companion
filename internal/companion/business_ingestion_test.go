package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const ingestionConnectionJSON = `{
  "update_id": 100,
  "business_connection": {"id":"ingestion-fixture","user":{"id":1,"is_bot":false},"user_chat_id":1,"date":1794180000,"is_enabled":true,"unknown_rights":{"future":true}},
  "future_connection_field": 9007199254740993
}`

const ingestionMessageJSON = `{
  "update_id": 101,
  "business_message": {
    "message_id":77,"business_connection_id":"ingestion-fixture","date":1794180060,
    "from":{"id":42,"is_bot":true},"chat":{"id":42,"type":"private"},
    "caption":"Exact original source caption",
    "reply_markup":{"inline_keyboard":[[{"text":"Посмотреть анкету","web_app":{"url":"https://mini.jardulik.app/mutuals?targetUserId=88&noCheck=1"}}],[{"text":"Открыть в Telegram","url":"https://t.me/Example16"}]]},
    "future_message_field":{"large":9007199254740993,"original":"\\u003csource\\u003e"}
  },
  "future_update_field": [1, 2, 3]
}`

func TestBusinessUpdateUnmarshalKeepsExactOriginalAndClearsReusedValue(t *testing.T) {
	for name, raw := range map[string]string{
		"connection":     ingestionConnectionJSON,
		"new-message":    ingestionMessageJSON,
		"edited-message": strings.Replace(ingestionMessageJSON, `"business_message":`, `"edited_business_message":`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			original := []byte(raw)
			var update Update
			if err := json.Unmarshal(original, &update); err != nil {
				t.Fatal(err)
			}
			if string(update.RawJSON) != raw {
				t.Fatal("unmarshal normalized or discarded source JSON")
			}
			original[0] = ' '
			if string(update.RawJSON) != raw {
				t.Fatal("source receipt aliases the decoder input buffer")
			}
			if err := json.Unmarshal([]byte(`{"update_id":102,"message":{"message_id":10,"from":{"id":1},"chat":{"id":1,"type":"private"},"text":"/status"}}`), &update); err != nil {
				t.Fatal(err)
			}
			if update.ID != 102 || update.Message == nil || update.RawJSON != nil || update.BusinessConnection != nil || update.BusinessMessage != nil || update.EditedBusinessMessage != nil {
				t.Fatal("ordinary update retained stale business payload or copied unrelated chat JSON")
			}
		})
	}
}

type ingestionPoll struct {
	Offset  int64    `json:"offset"`
	Allowed []string `json:"allowed_updates"`
}

// The normal runtime uses a real Store and workers; only network transports are replaced.
func ingestionRunPolls(s *Service, next func(ingestionPoll) (updates string, stop bool, err error), outbound *atomic.Int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var transportErr error
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		result := ""
		switch filepath.Base(r.URL.Path) {
		case "getMe":
			result = `{"id":900,"is_bot":true,"username":"ingestion_fixture"}`
		case "getWebhookInfo":
			result = `{"url":""}`
		case "setMyCommands":
			result = `true`
		case "getUpdates":
			var poll ingestionPoll
			if err := json.NewDecoder(r.Body).Decode(&poll); err != nil {
				transportErr = err
				cancel()
				return nil, err
			}
			for _, required := range []string{"business_connection", "business_message", "edited_business_message"} {
				found := false
				for _, allowed := range poll.Allowed {
					found = found || allowed == required
				}
				if !found {
					transportErr = fmt.Errorf("poll omitted %s", required)
					cancel()
					return nil, transportErr
				}
			}
			var stop bool
			var err error
			result, stop, err = next(poll)
			if err != nil {
				transportErr = err
				cancel()
				return nil, err
			}
			if stop {
				cancel()
				return nil, context.Canceled
			}
		default:
			outbound.Add(1)
			cancel()
			return nil, errors.New("receipt ingestion attempted an outbound Telegram operation")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":` + result + `}`))}, nil
	})
	err := s.RunBotParser(ctx)
	return errors.Join(err, transportErr)
}

func ingestionRuntimeFixture(t *testing.T) (*Service, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	s := runtimeServiceFixture(t)
	ctx := context.Background()
	if err := s.Store.User(ctx, 1, "business-ingestion-fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, 1, Session{UserID: 500, Generation: "ingestion-generation", Token: "fixture", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	// Source protection remains active, including during runtime setup and background work.
	s.Store.SessionState(ctx, 1, "paused", "fixture_source_protection")
	source, outbound := new(atomic.Int64), new(atomic.Int64)
	s.Client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		source.Add(1)
		return nil, errors.New("contact receipt attempted source HTTP")
	})
	return s, source, outbound
}

func ingestionAssertReceipt(s *Service) error {
	var count int
	var raw, url string
	var owner, upstream, profile int64
	if err := s.Store.DB.QueryRow(`SELECT COUNT(*) FROM profile_business_contacts`).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("receipt count=%d, want 1", count)
	}
	if err := s.Store.DB.QueryRow(`SELECT owner_id,upstream_id,profile_id,raw_json,contact_url FROM profile_business_contacts WHERE update_id=101`).Scan(&owner, &upstream, &profile, &raw, &url); err != nil {
		return err
	}
	if owner != 1 || upstream != 500 || profile != 88 || raw != ingestionMessageJSON || url != "https://t.me/Example16" {
		return errors.New("receipt lost account binding, source bytes or confirmed URL")
	}
	return nil
}

func TestBusinessPollCommitsConnectionAndReceiptBeforeOffsetAndDeduplicatesReplay(t *testing.T) {
	s, source, outbound := ingestionRuntimeFixture(t)
	ctx := context.Background()
	if err := s.Store.initBusinessContacts(ctx); err != nil {
		t.Fatal(err)
	}
	// Any acknowledgment of the card must be preceded by its durable receipt.
	if _, err := s.Store.DB.Exec(`CREATE TRIGGER ingestion_offset_order BEFORE INSERT ON meta
 WHEN NEW.key='telegram_offset' AND CAST(NEW.value AS INTEGER)>=102
 AND NOT EXISTS(SELECT 1 FROM profile_business_contacts WHERE update_id=101)
 BEGIN SELECT RAISE(ABORT,'contact acknowledged before commit'); END;`); err != nil {
		t.Fatal(err)
	}
	polls := 0
	err := ingestionRunPolls(s, func(p ingestionPoll) (string, bool, error) {
		polls++
		switch polls {
		case 1:
			if p.Offset != 0 {
				return "", false, fmt.Errorf("initial offset=%d", p.Offset)
			}
			return "[" + ingestionConnectionJSON + "," + ingestionMessageJSON + "]", false, nil
		case 2:
			if p.Offset != 102 || s.Store.Meta(ctx, "telegram_offset") != "102" {
				return "", false, errors.New("next poll did not acknowledge the persisted card")
			}
			if err := ingestionAssertReceipt(s); err != nil {
				return "", false, err
			}
			return "[" + ingestionMessageJSON + "]", false, nil
		case 3:
			if err := ingestionAssertReceipt(s); err != nil {
				return "", false, err
			}
			return "", true, nil
		default:
			return "", false, errors.New("unexpected extra poll")
		}
	}, outbound)
	if err != nil {
		t.Fatal(err)
	}
	if polls != 3 {
		t.Fatalf("polls=%d, want 3", polls)
	}
	if err := ingestionAssertReceipt(s); err != nil {
		t.Fatal(err)
	}
	var claimed int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM updates WHERE id IN (100,101)").Scan(&claimed); err != nil || claimed != 0 {
		t.Fatalf("business receipt crossed asynchronous command claims: count=%d err=%v", claimed, err)
	}
	contact, err := s.Store.ProfileTelegramContact(ctx, 1, 88)
	if err != nil || contact == nil || contact.ContactURL != "https://t.me/Example16" {
		t.Fatalf("parsed contact not available without profile hydration: contact=%+v err=%v", contact, err)
	}
	if source.Load() != 0 || outbound.Load() != 0 {
		t.Fatalf("ingestion made network side effects: source=%d outbound=%d", source.Load(), outbound.Load())
	}
	session, err := s.Store.Session(ctx, 1)
	if err != nil || session.State != "paused" || session.Generation != "ingestion-generation" {
		t.Fatalf("ingestion changed protected source session: %+v err=%v", session, err)
	}
}

func TestBusinessPollDatabaseFailureKeepsOffsetAndRestartReplaysOnce(t *testing.T) {
	s, source, outbound := ingestionRuntimeFixture(t)
	ctx := context.Background()
	if err := s.Store.initBusinessContacts(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`CREATE TRIGGER ingestion_fail_contact BEFORE INSERT ON profile_business_contacts BEGIN SELECT RAISE(ABORT,'fixture contact database failure'); END;`); err != nil {
		t.Fatal(err)
	}
	polls := 0
	err := ingestionRunPolls(s, func(p ingestionPoll) (string, bool, error) {
		polls++
		if polls != 1 {
			return "", false, errors.New("failed contact was acknowledged or polling continued")
		}
		return "[" + ingestionConnectionJSON + "," + ingestionMessageJSON + "]", false, nil
	}, outbound)
	if err == nil || !strings.Contains(err.Error(), "fixture contact database failure") {
		t.Fatalf("database write failure did not stop the poller: %v", err)
	}
	if got := s.Store.Meta(ctx, "telegram_offset"); got != "101" {
		t.Fatalf("failed card acknowledged: offset=%q, want 101", got)
	}
	var contacts, claims int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM profile_business_contacts").Scan(&contacts); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM updates WHERE id=101").Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if contacts != 0 || claims != 0 {
		t.Fatalf("failed receipt left partial state: contacts=%d claims=%d", contacts, claims)
	}
	if _, err := s.Store.DB.Exec("DROP TRIGGER ingestion_fail_contact"); err != nil {
		t.Fatal(err)
	}
	polls = 0
	err = ingestionRunPolls(s, func(p ingestionPoll) (string, bool, error) {
		polls++
		switch polls {
		case 1:
			if p.Offset != 101 {
				return "", false, fmt.Errorf("restart skipped failed card: offset=%d", p.Offset)
			}
			return "[" + ingestionMessageJSON + "]", false, nil
		case 2:
			if p.Offset != 102 {
				return "", false, fmt.Errorf("replayed card not acknowledged: offset=%d", p.Offset)
			}
			if err := ingestionAssertReceipt(s); err != nil {
				return "", false, err
			}
			return "[" + ingestionMessageJSON + "]", false, nil
		case 3:
			if err := ingestionAssertReceipt(s); err != nil {
				return "", false, err
			}
			return "", true, nil
		default:
			return "", false, errors.New("unexpected extra replay poll")
		}
	}, outbound)
	if err != nil {
		t.Fatal(err)
	}
	if polls != 3 {
		t.Fatalf("restart polls=%d, want 3", polls)
	}
	if err := ingestionAssertReceipt(s); err != nil {
		t.Fatal(err)
	}
	if got := s.Store.Meta(ctx, "telegram_offset"); got != "102" {
		t.Fatalf("successful replay offset=%q, want 102", got)
	}
	if source.Load() != 0 || outbound.Load() != 0 {
		t.Fatalf("restart made network side effects: source=%d outbound=%d", source.Load(), outbound.Load())
	}
}
