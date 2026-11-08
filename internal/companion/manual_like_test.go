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
	"testing"
	"time"
)

func manualLikeFixture(t *testing.T, account, reaction string) (*Service, *[]uxMessage, *[]string) {
	t.Helper()
	s, sent := uxFixture(t)
	s.Config.AllowReactions = true
	s.Store.cfg.AllowReactions = true
	uxProfiles(t, s, 1, 1)
	if err := s.Store.SaveSession(context.Background(), 1, Session{Token: "test-token", Refresh: "test-refresh", UserID: 101, Generation: "test-login", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	var calls []string
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		body := ""
		switch r.URL.Path {
		case "/user/getMe":
			if r.Method != http.MethodGet || r.URL.Query().Get("userId") != "101" {
				t.Fatal("wrong account request")
			}
			body = account
		case "/reactions/addNew":
			if r.Method != http.MethodPost {
				t.Fatal("wrong reaction method")
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["userId"] != float64(1) || payload["fromUserId"] != float64(101) || payload["reactionType"] != float64(1) {
				t.Fatalf("wrong reaction identity: %v", payload)
			}
			if reaction == "transport-error" {
				return nil, errors.New("test response lost")
			}
			body = reaction
		default:
			t.Fatalf("unexpected source call: %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	return s, sent, &calls
}

func TestManualLikeDisabledAndPausedMakeNoSourceRequests(t *testing.T) {
	for _, gate := range []string{"service", "store", "paused"} {
		t.Run(gate, func(t *testing.T) {
			s, sent := uxFixture(t)
			s.Config.AllowReactions, s.Store.cfg.AllowReactions = true, true
			switch gate {
			case "service":
				s.Config.AllowReactions = false
			case "store":
				s.Store.cfg.AllowReactions = false
			case "paused":
				s.Store.Pause(context.Background(), 1, true)
			}
			uxTap(s, 1, "l:1")
			if len(*sent) != 1 || (!strings.Contains((*sent)[0].Text, "выключены") && !strings.Contains((*sent)[0].Text, "приостановлены")) {
				t.Fatalf("missing gate response: %v", *sent)
			}
		})
	}
}

func TestManualLikePrivateOwnerAndProfileIsolation(t *testing.T) {
	s, sent := uxFixture(t)
	s.Config.AllowReactions, s.Store.cfg.AllowReactions = true, true
	uxProfiles(t, s, 1, 1)
	for _, chat := range []TGChat{{ID: 1, Type: "private"}, {ID: -1, Type: "supergroup"}} {
		s.Handle(context.Background(), Update{Callback: &Callback{ID: "tap", From: TGUser{ID: 2}, Message: Message{Chat: chat}, Data: "l:1"}})
	}
	for _, data := range []string{"l:0", "l:-1", "l:x"} {
		uxTap(s, 1, data)
	}
	if len(*sent) != 0 {
		t.Fatal("invalid owner/chat/ID was accepted")
	}
	uxTap(s, 2, "l:1")
	if len(*sent) != 1 || (*sent)[0].Text != "Анкета недоступна." {
		t.Fatalf("foreign profile accepted: %v", *sent)
	}
}

func TestManualLikePrivateCardKeepsFavoriteAndCorrectProfileID(t *testing.T) {
	s, sent := uxFixture(t)
	uxProfiles(t, s, 1, 1)
	p, err := s.Store.Profile(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.sendProfileCard(context.Background(), 1, p); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 {
		t.Fatal("card should remain a single delivery")
	}
	foundLike, foundFavorite := false, false
	for _, row := range (*sent)[0].Markup.Rows {
		for _, button := range row {
			foundLike = foundLike || button.Text == "❤️ Лайк" && button.Data == "l:1"
			foundFavorite = foundFavorite || button.Data == "f:1"
		}
	}
	if !foundLike || !foundFavorite {
		t.Fatal("missing separate like/favorite actions")
	}
}

func TestManualLikeSuccessMutualAndDuplicate(t *testing.T) {
	for _, mutual := range []bool{false, true} {
		reaction := `{"IsSuccess":true}`
		if mutual {
			reaction = `{"IsSuccess":true,"IsMutual":true}`
		}
		s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, reaction)
		uxTap(s, 1, "l:1")
		if len(*calls) != 2 || len(*sent) != 1 {
			t.Fatalf("unexpected calls/messages: %v / %v", *calls, *sent)
		}
		text := (*sent)[0].Text
		if mutual && !strings.Contains(text, "/matches") || !mutual && text != "Лайк отправлен." {
			t.Fatalf("incorrect success message: %s", text)
		}
		uxTap(s, 1, "l:1")
		if len(*calls) != 2 || (*sent)[1].Text != "Лайк уже отправлен." {
			t.Fatal("duplicate callback repeated source request")
		}
	}
}

func TestManualLikeSinglePhotoCarriesProfileCallback(t *testing.T) {
	s, p := cardFixture(t, 1)
	calls := 0
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if filepath.Base(r.URL.Path) != "sendPhoto" {
			t.Fatalf("unexpected extra message: %s", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		var message uxMessage
		if err := json.Unmarshal([]byte(r.FormValue("reply_markup")), &message.Markup); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range message.Markup.Rows {
			for _, button := range row {
				found = found || button.Text == "❤️ Лайк" && button.Data == "l:10"
			}
		}
		if !found {
			t.Fatal("photo action did not target the profile ID")
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":20,"chat":{"id":1}}}`)
	})}
	ids, err := s.sendProfileCard(context.Background(), 1, p)
	if err != nil || len(ids) != 1 || calls != 1 {
		t.Fatalf("photo card delivery changed: ids=%v calls=%d err=%v", ids, calls, err)
	}
}

func TestManualLikeAccountFailureDoesNotClaimReaction(t *testing.T) {
	s, sent, calls := manualLikeFixture(t, `{"userId":999,"freezeState":false}`, `{"IsSuccess":true}`)
	uxTap(s, 1, "l:1")
	if len(*calls) != 1 || len(*sent) != 1 || !strings.Contains((*sent)[0].Text, "Лайк не отправлен") {
		t.Fatal("failed fresh account check reached reaction")
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT count(*) FROM actions").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed account check claimed an action")
	}
}

func TestManualLikeUnknownNeverRetriesAfterResume(t *testing.T) {
	s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, "transport-error")
	uxTap(s, 1, "l:1")
	if len(*calls) != 2 || !s.Store.Paused(context.Background(), 1) || !strings.Contains((*sent)[0].Text, "не подтверждён") {
		t.Fatal("unknown outcome not stopped")
	}
	s.Store.Pause(context.Background(), 1, false)
	uxTap(s, 1, "l:1")
	if len(*calls) != 2 || !strings.Contains((*sent)[1].Text, "Повторно не отправляю") {
		t.Fatal("unknown outcome was retried")
	}
}

func TestManualLikeFreshFreezePreventsReaction(t *testing.T) {
	s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":true}`, `{"IsSuccess":true}`)
	uxTap(s, 1, "l:1")
	if len(*calls) != 1 || len(*sent) != 1 || !strings.Contains((*sent)[0].Text, "заморожена") {
		t.Fatal("fresh frozen account was not blocked")
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT count(*) FROM actions").Scan(&count); err != nil || count != 0 {
		t.Fatal("frozen account claimed an action")
	}
}

func TestManualLikeRejectionReportsSourceTimer(t *testing.T) {
	s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, `{"IsSuccess":false,"MessageKind":-1,"HoursLeft":2,"MinutesLeft":17}`)
	uxTap(s, 1, "l:1")
	if len(*calls) != 2 || len(*sent) != 1 || !strings.Contains((*sent)[0].Text, "Лимит") || !strings.Contains((*sent)[0].Text, "2 ч 17 мин") || s.Store.Paused(context.Background(), 1) {
		t.Fatal("rejection or source timer was misreported")
	}
	uxTap(s, 1, "l:1")
	if len(*calls) != 2 || !strings.Contains((*sent)[1].Text, "2 ч 17 мин") {
		t.Fatalf("source wait caused a repeated request or lost its timer: calls=%v messages=%v", *calls, *sent)
	}
}

func TestManualLikeLastIsNotQuotaAndGenericRejectionIsNotSuccess(t *testing.T) {
	for _, tc := range []struct {
		reaction string
		want     string
		paused   bool
	}{
		{`{"IsSuccess":false,"IsLast":true,"HoursLeft":5}`, "Анкеты закончились.", false},
		{`{"IsSuccess":true,"IsLast":true,"HoursLeft":5}`, "Лайк отправлен.", false},
		{`{"IsSuccess":false,"HoursLeft":5}`, "Лайк не подтверждён.", true},
		{`{"IsSuccess":false,"MessageKind":1,"HoursLeft":5}`, "Лайк не подтверждён.", true},
		{`{"IsSuccess":false,"MessageKind":-3}`, "нужен Premium", false},
	} {
		s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, tc.reaction)
		uxTap(s, 1, "l:1")
		if len(*calls) != 2 || len(*sent) != 1 || !strings.Contains((*sent)[0].Text, tc.want) || strings.Contains((*sent)[0].Text, "До следующего лайка") || s.Store.Paused(context.Background(), 1) != tc.paused {
			t.Fatalf("source result misrepresented: %v", *sent)
		}
		if strings.Contains(tc.reaction, `"IsSuccess":true`) && strings.Contains((*sent)[0].Text, "Анкеты закончились") {
			t.Fatal("successful Last flag was treated as exhaustion")
		}
	}
}

func TestManualLikeAlbumCommandUsesSameGatesAndDedup(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		s, sent := uxFixture(t)
		uxSend(s, 1, "/like_1")
		if len(*sent) != 1 || (*sent)[0].Text != "Лайки сейчас выключены." {
			t.Fatal("album command bypassed reaction gate")
		}
	})
	t.Run("success and repeated callback", func(t *testing.T) {
		s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, `{"IsSuccess":true}`)
		uxSend(s, 1, "/like_1@our_bot")
		uxTap(s, 1, "l:1")
		if len(*calls) != 2 || len(*sent) != 2 || (*sent)[0].Text != "Лайк отправлен." || (*sent)[1].Text != "Лайк уже отправлен." {
			t.Fatal("album command and button do not share deduplication")
		}
	})
	t.Run("owner and malformed ID", func(t *testing.T) {
		s, sent := uxFixture(t)
		s.Config.AllowReactions, s.Store.cfg.AllowReactions = true, true
		uxProfiles(t, s, 1, 1)
		for _, command := range []string{"/like_0", "/like_-1", "/like_foo", "/like_9223372036854775808"} {
			uxSend(s, 1, command)
		}
		s.Handle(context.Background(), Update{Message: &Message{From: TGUser{ID: 2}, Chat: TGChat{ID: 1, Type: "private"}, Text: "/like_1"}})
		if len(*sent) != 0 {
			t.Fatal("invalid album command accepted")
		}
		uxSend(s, 2, "/like_1")
		if len(*sent) != 1 || (*sent)[0].Text != "Анкета недоступна." {
			t.Fatal("album command leaked another owner's profile")
		}
	})
}

func TestManualLikePersonalCapCountsSuccessAndUnknown(t *testing.T) {
	for _, reaction := range []string{`{"IsSuccess":true}`, "transport-error"} {
		t.Run(reaction, func(t *testing.T) {
			s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, reaction)
			uxProfiles(t, s, 1, 2)
			ctx := context.Background()
			if _, err := NewAutoliker(s).settings(ctx, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec("UPDATE autolike_settings SET daily_cap=1 WHERE owner_id=1"); err != nil {
				t.Fatal(err)
			}
			uxTap(s, 1, "l:1")
			if len(*calls) != 2 {
				t.Fatalf("first manual like was not attempted: %v", *calls)
			}
			if reaction == "transport-error" {
				// Explicitly restore manual mode; uncertain attempts still consume the budget.
				uxSend(s, 1, "/resume")
			}
			uxTap(s, 1, "l:2")
			last := (*sent)[len(*sent)-1].Text
			if len(*calls) != 2 || !strings.Contains(last, "твой лимит") || !strings.Contains(last, "/settings") {
				t.Fatalf("manual personal cap bypassed: calls=%v last=%s", *calls, last)
			}
			var actions int
			if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions WHERE owner_id=1").Scan(&actions); err != nil || actions != 1 {
				t.Fatalf("blocked manual attempt claimed action: count=%d err=%v", actions, err)
			}
		})
	}
}

func TestManualLikePersonalCapIsOwnerScoped(t *testing.T) {
	s, sent, calls := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, `{"IsSuccess":true}`)
	ctx := context.Background()
	if _, err := NewAutoliker(s).settings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE autolike_settings SET daily_cap=1 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("INSERT INTO actions(owner_id,profile_id,kind,state) VALUES(2,1,'Like','succeeded')"); err != nil {
		t.Fatal(err)
	}
	uxTap(s, 1, "l:1")
	if len(*calls) != 2 || len(*sent) != 1 || (*sent)[0].Text != "Лайк отправлен." {
		t.Fatalf("another owner's budget blocked manual like: calls=%v messages=%v", *calls, *sent)
	}
}
