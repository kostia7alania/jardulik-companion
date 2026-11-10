package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func miniAppTestCardFixture(t *testing.T, photos int) (*Service, StoredProfile) {
	t.Helper()
	s, p := cardFixture(t, photos)
	s.Config.MiniAppListen, s.Config.MiniAppPublicOrigin = "127.0.0.1:8790", "https://companion.example"
	s.Config.ArchivePaused = true
	s.Client = NewClient(s.Store, s.Config)
	s.Client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("test card contacted the Jardulik API")
		return nil, errors.New("source API is forbidden for the test card")
	})
	return s, p
}

func miniAppTestCardAck(chat, message int64, photos int) string {
	return fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d,"type":"private"},"rich_message":%s}}`, message, chat, richAck(photos))
}

func TestMiniAppTestCardOneOwnerMessagePreservesSourcesAndReplay(t *testing.T) {
	for _, photos := range []int{0, 2} {
		t.Run(fmt.Sprint(photos), func(t *testing.T) {
			s, p := miniAppTestCardFixture(t, photos)
			ctx := context.Background()
			jobs, err := s.Store.MediaJobs(ctx, 1, p.VersionID)
			if err != nil {
				t.Fatal(err)
			}
			want, err := BuildRichProfileCard(p, jobs, "")
			if err != nil {
				t.Fatal(err)
			}
			wantJSON, _ := json.Marshal(want.Content)
			calls := 0
			s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if filepath.Base(r.URL.Path) != "sendRichMessage" {
					t.Fatal("test card used another provider or a second message", r.URL.Path)
				}
				var chat int64
				var rich, markup json.RawMessage
				if photos == 0 {
					var payload struct {
						Chat   int64           `json:"chat_id"`
						Rich   json.RawMessage `json:"rich_message"`
						Markup json.RawMessage `json:"reply_markup"`
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					chat, rich, markup = payload.Chat, payload.Rich, payload.Markup
				} else {
					if err := r.ParseMultipartForm(1 << 20); err != nil {
						t.Fatal(err)
					}
					defer r.MultipartForm.RemoveAll()
					if r.FormValue("chat_id") != "1" || len(r.MultipartForm.File) != photos {
						t.Fatal("test card lost owner binding or original photos")
					}
					chat, rich, markup = 1, json.RawMessage(r.FormValue("rich_message")), json.RawMessage(r.FormValue("reply_markup"))
					for i := 0; i < photos; i++ {
						file, _, err := r.FormFile(fmt.Sprintf("p%d", i))
						if err != nil {
							t.Fatal(err)
						}
						data, err := io.ReadAll(file)
						_ = file.Close()
						if err != nil || string(data) != fmt.Sprintf("photo-%d", i) {
							t.Fatal("test card changed an original photo", err)
						}
					}
				}
				if chat != s.Config.OwnerID || !bytes.Equal(rich, wantJSON) || !strings.Contains(string(rich), "Полное описание") {
					t.Fatal("test card bypassed the shared renderer or owner recipient")
				}
				var keyboard struct {
					Rows [][]struct {
						Text   string            `json:"text"`
						WebApp map[string]string `json:"web_app"`
						URL    string            `json:"url"`
						Action string            `json:"callback_data"`
					} `json:"inline_keyboard"`
				}
				if err := json.Unmarshal(markup, &keyboard); err != nil || len(keyboard.Rows) != 1 || len(keyboard.Rows[0]) != 2 {
					t.Fatal("test card must have exactly two Mini App buttons", err)
				}
				buttons := keyboard.Rows[0]
				if buttons[0].Text != "🗃 Анкета" || buttons[0].WebApp["url"] != "https://companion.example/viewer?id=10" || buttons[1].Text != "Jardulik · прокси" || buttons[1].WebApp["url"] != "https://companion.example/mutuals?targetUserId=10&noCheck=1" || buttons[0].URL != "" || buttons[1].URL != "" || buttons[0].Action != "" || buttons[1].Action != "" {
					t.Fatal("Mini App provider/profile binding changed", string(markup))
				}
				check, cancel := context.WithTimeout(r.Context(), time.Second)
				defer cancel()
				var intent string
				if err := s.Store.DB.QueryRowContext(check, "SELECT value FROM meta WHERE key LIKE 'miniapp-test-card:v1:%'").Scan(&intent); err != nil || !strings.Contains(intent, `"state":"sending"`) {
					t.Fatal("HTTP started before a committed intent or while holding SQL", err)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(miniAppTestCardAck(1, 321, photos))), Request: r}, nil
			})
			first, err := s.SendMiniAppTestCard(ctx, p.ID)
			if err != nil || first.AlreadySent || first.Message.ID != 321 || first.Message.Chat.ID != 1 {
				t.Fatal(first, err)
			}
			// A receipt replay must not depend on retained photo files or resend.
			for _, job := range jobs {
				if err := os.Remove(job.Path); err != nil {
					t.Fatal(err)
				}
			}
			restarted := &Service{Config: s.Config, Store: s.Store, TG: s.TG, Client: s.Client}
			replay, err := restarted.SendMiniAppTestCard(ctx, p.ID)
			if err != nil || !replay.AlreadySent || replay.Message.ID != 321 || replay.Message.Chat.ID != 1 || calls != 1 {
				t.Fatal("known receipt resent or lost its binding", replay, calls, err)
			}
			var raw string
			var delivered int
			if err := s.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&raw); err != nil || raw != p.RawJSON {
				t.Fatal("test card changed raw source JSON", err)
			}
			if err := s.Store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=?", p.ID).Scan(&delivered); err != nil || delivered != 0 || watchCount(t, s.Store, "deliveries") != 0 || watchCount(t, s.Store, "favorites") != 0 {
				t.Fatal("test card changed normal deliveries or favorites", err)
			}
		})
	}
}

func TestMiniAppTestCardUnknownOrInvalidReceiptNeverRepeats(t *testing.T) {
	for _, outcome := range []string{"transport", "wrong chat", "missing id", "receipt write"} {
		t.Run(outcome, func(t *testing.T) {
			s, p := miniAppTestCardFixture(t, 0)
			calls := 0
			s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if outcome == "transport" {
					return nil, errors.New("synthetic transport failure")
				}
				chat, message := int64(1), int64(321)
				if outcome == "wrong chat" {
					chat = 2
				}
				if outcome == "missing id" {
					message = 0
				}
				if outcome == "receipt write" {
					if _, err := s.Store.DB.Exec(`CREATE TRIGGER refuse_test_receipt BEFORE UPDATE ON meta WHEN NEW.key LIKE 'miniapp-test-card:v1:%' BEGIN SELECT RAISE(FAIL,'fixture receipt write failed'); END`); err != nil {
						t.Fatal(err)
					}
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(miniAppTestCardAck(chat, message, 0))), Request: r}, nil
			})
			if _, err := s.SendMiniAppTestCard(context.Background(), p.ID); !errors.Is(err, ErrMiniAppTestCardUncertain) {
				t.Fatal("unconfirmed send outcome was not recorded", err)
			}
			if _, err := s.SendMiniAppTestCard(context.Background(), p.ID); !errors.Is(err, ErrMiniAppTestCardUncertain) || calls != 1 {
				t.Fatal("unknown outcome was sent again", calls, err)
			}
		})
	}
}

func TestMiniAppTestCardConcurrentIntentAllowsOneSend(t *testing.T) {
	s, p := miniAppTestCardFixture(t, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(miniAppTestCardAck(1, 321, 0))), Request: r}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.SendMiniAppTestCard(ctx, p.ID); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first send never began")
	}
	_, secondErr := s.SendMiniAppTestCard(ctx, p.ID)
	close(release)
	if err := <-done; err != nil || !errors.Is(secondErr, ErrMiniAppTestCardUncertain) || calls.Load() != 1 {
		t.Fatal("concurrent experiment sent twice or failed its known receipt", err, secondErr, calls.Load())
	}
}

func TestMiniAppTestCardRejectsForeignProfileAndInvalidOrigin(t *testing.T) {
	s, p := miniAppTestCardFixture(t, 0)
	ctx := context.Background()
	if err := s.Store.User(ctx, 2, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.Archive(ctx, 2, []Profile{{"userId": 99, "name": "Other tenant"}}, "test"); err != nil {
		t.Fatal(err)
	}
	s.TG.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid experiment reached Telegram")
		return nil, errors.New("unexpected HTTP")
	})
	if _, err := s.SendMiniAppTestCard(ctx, 99); err == nil {
		t.Fatal("test card read another tenant's profile")
	}
	for _, origin := range []string{"", "http://companion.example", "https://companion.example/viewer", "https://user:pass@companion.example", "https://companion.example?token=secret"} {
		s.Config.MiniAppPublicOrigin = origin
		if _, err := s.SendMiniAppTestCard(ctx, p.ID); err == nil {
			t.Fatal("invalid Mini App origin accepted", origin)
		}
	}
	var intents int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM meta WHERE key LIKE 'miniapp-test-card:v1:%'").Scan(&intents); err != nil || intents != 0 {
		t.Fatal("invalid experiment claimed a send intent", intents, err)
	}
}
