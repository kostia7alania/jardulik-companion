package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func contactControlsFixture(t *testing.T) (*Service, StoredProfile, ContactEvidenceInput) {
	t.Helper()
	s, _ := uxFixture(t)
	s.Config.SourceBotID, s.Store.cfg.SourceBotID = 456, 456
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		uxProfiles(t, s, owner, 1)
		if err := s.Store.SaveSession(ctx, owner, Session{UserID: 400 + owner*100, Token: "offline-fixture", Generation: "contact-controls", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	p, err := s.Store.Profile(ctx, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC)
	input := ContactEvidenceInput{
		UpstreamID: 500, ProfileID: p.ID, VersionID: p.VersionID,
		SourceBotID: 456, SourcePeerID: 456, SourceLocatorKind: "telegram_web_dom",
		SourceMessageLocator: "data-mid:synthetic-contact-card", SourceMessageTimestamp: observed.Add(-time.Minute).Unix(),
		ContactURL: "https://t.me/SyntheticContact", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=1&noCheck=1", ObservedAt: observed,
	}
	return s, p, input
}

func contactControlURL(t *testing.T, markup any) string {
	t.Helper()
	raw, err := json.Marshal(markup)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Rows [][]map[string]string `json:"inline_keyboard"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	var found string
	for _, row := range decoded.Rows {
		for _, button := range row {
			if button["text"] == "💬 Telegram" {
				if found != "" || button["url"] == "" || button["callback_data"] != "" {
					t.Fatal("contact is not one clickable URL button")
				}
				found = button["url"]
			}
		}
	}
	return found
}

func TestContactControlsOwnerAccountStateAndSourceIsolation(t *testing.T) {
	for _, state := range []string{"connected", "owner-paused", "session-paused", "reconnect-required", "foreign-owner", "account-switched", "same-account-refreshed", "disconnected-state", "disconnected", "source-changed"} {
		t.Run(state, func(t *testing.T) {
			s, p, input := contactControlsFixture(t)
			ctx := context.Background()
			if _, err := s.Store.SaveContactEvidence(ctx, 1, input); err != nil {
				t.Fatal(err)
			}
			owner, want := int64(1), input.ContactURL
			switch state {
			case "owner-paused":
				s.Store.Pause(ctx, 1, true)
			case "session-paused":
				s.Store.SessionState(ctx, 1, "paused", "fixture")
			case "reconnect-required":
				s.Store.SessionState(ctx, 1, "reconnect_required", "fixture")
			case "foreign-owner":
				owner, want = 2, ""
			case "account-switched", "same-account-refreshed":
				upstream := int64(500)
				if state == "account-switched" {
					upstream, want = 700, ""
				}
				if err := s.Store.SaveSession(ctx, 1, Session{UserID: upstream, Token: "refreshed-fixture", Generation: "new-generation"}); err != nil {
					t.Fatal(err)
				}
			case "disconnected-state":
				s.Store.SessionState(ctx, 1, "disconnected", "fixture")
				want = ""
			case "disconnected":
				if err := s.Store.Disconnect(ctx, 1); err != nil {
					t.Fatal(err)
				}
				want = ""
			case "source-changed":
				s.Config.SourceBotID, s.Store.cfg.SourceBotID = 789, 789
				want = ""
			}
			got := contactControlURL(t, s.privateProfileButtons(ctx, owner, p, true, true))
			if got != want {
				t.Fatalf("contact visibility in %s: got=%q want=%q", state, got, want)
			}
			after, err := s.Store.Version(ctx, 1, p.VersionID)
			if err != nil || after.RawJSON != p.RawJSON {
				t.Fatal("contact projection mutated exact source JSON", err)
			}
		})
	}
}

func TestContactControlsNewLegacyRichAndKeyboardMenuUseSavedURL(t *testing.T) {
	for _, rich := range []bool{false, true} {
		t.Run(fmt.Sprintf("rich%t", rich), func(t *testing.T) {
			s, p, input := contactControlsFixture(t)
			s.Config.RichCards = rich
			ctx := context.Background()
			if _, err := s.Store.SaveContactEvidence(ctx, 1, input); err != nil {
				t.Fatal(err)
			}
			var markups []any
			s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := filepath.Base(r.URL.Path)
				if method != "sendMessage" && method != "sendRichMessage" {
					t.Fatalf("unexpected contact operation: %s", method)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				markups = append(markups, body["reply_markup"])
				fmt.Fprint(w, `{"ok":true,"result":{"message_id":100,"chat":{"id":1},"rich_message":{"blocks":[]}}}`)
			})}
			if err := s.Deliver(ctx, 1, p); err != nil {
				t.Fatal(err)
			}
			if len(markups) != 1 || contactControlURL(t, markups[0]) != input.ContactURL {
				t.Fatal("new card omitted saved contact or sent an extra message")
			}
			s.Store.Pause(ctx, 1, true)
			m := keyboardMessage(1, 101, keyboardMenu)
			s.Handle(ctx, Update{Message: &m})
			if len(markups) != 2 || contactControlURL(t, markups[1]) != input.ContactURL {
				t.Fatal("paused owner's explicit menu omitted cached contact")
			}
			after, err := s.Store.Version(ctx, 1, p.VersionID)
			if err != nil || after.RawJSON != p.RawJSON {
				t.Fatal("rendering contacts changed source JSON", err)
			}
		})
	}
}

func TestContactControlsCaptionRefreshAddsNewSavedURL(t *testing.T) {
	for _, rich := range []bool{false, true} {
		t.Run(fmt.Sprintf("rich%t", rich), func(t *testing.T) {
			s, p, input := contactControlsFixture(t)
			s.Config.RichCards = rich
			ctx := context.Background()
			var methods []string
			var markups []any
			s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := filepath.Base(r.URL.Path)
				methods = append(methods, method)
				if method != "sendMessage" && method != "sendRichMessage" && method != "editMessageText" && method != "editMessageReplyMarkup" {
					t.Fatalf("unexpected caption contact operation: %s", method)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				markups = append(markups, body["reply_markup"])
				fmt.Fprint(w, `{"ok":true,"result":{"message_id":100,"chat":{"id":1},"rich_message":{"blocks":[]}}}`)
			})}
			if err := s.Deliver(ctx, 1, p); err != nil {
				t.Fatal(err)
			}
			if contactControlURL(t, markups[0]) != "" {
				t.Fatal("contact appeared before saved official UI evidence")
			}
			if _, err := s.Store.SaveContactEvidence(ctx, 1, input); err != nil {
				t.Fatal(err)
			}
			if err := s.RefreshCardCaptions(ctx); err != nil {
				t.Fatal(err)
			}
			if len(markups) < 2 || contactControlURL(t, markups[len(markups)-1]) != input.ContactURL {
				t.Fatalf("existing card refresh omitted newly saved contact: methods=%v", methods)
			}
			for _, method := range methods[1:] {
				if method != "editMessageText" && method != "editMessageReplyMarkup" {
					t.Fatal("contact refresh sent a duplicate card")
				}
			}
		})
	}
}
