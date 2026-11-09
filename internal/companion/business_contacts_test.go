package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func businessTestUpdate(t *testing.T, body map[string]any) Update {
	t.Helper()
	raw, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var update Update
	if err = json.Unmarshal(raw, &update); err != nil {
		t.Fatal(err)
	}
	return update
}

func businessTestConnection(t *testing.T, owner, updateID int64, enabled bool) Update {
	t.Helper()
	return businessTestUpdate(t, map[string]any{"update_id": updateID, "business_connection": map[string]any{
		"id": fmt.Sprintf("fixture-connection-%d", owner), "user": map[string]any{"id": owner, "is_bot": false},
		"user_chat_id": owner, "date": int64(1794170000), "is_enabled": enabled, "unknown_connection_field": "retained",
	}})
}

func businessTestMessage(t *testing.T, source, owner, updateID int64, contact string) Update {
	t.Helper()
	return businessTestUpdate(t, map[string]any{"update_id": updateID, "unknown_update_field": []any{"retained", 42}, "business_message": map[string]any{
		"message_id": int64(900), "business_connection_id": fmt.Sprintf("fixture-connection-%d", owner), "date": int64(1794170010),
		"from": map[string]any{"id": source, "is_bot": true}, "chat": map[string]any{"id": source, "type": "private"},
		"caption": "Original fixture caption", "unknown_message_field": map[string]any{"keep": true},
		"reply_markup": map[string]any{"inline_keyboard": []any{
			[]any{map[string]any{"text": "Посмотреть анкету", "web_app": map[string]any{"url": "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1"}}},
			[]any{map[string]any{"text": "Открыть в Telegram", "url": contact}},
		}},
	}})
}

func mutateBusinessUpdate(t *testing.T, update Update, mutate func(map[string]any)) Update {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(update.RawJSON, &body); err != nil {
		t.Fatal(err)
	}
	mutate(body)
	return businessTestUpdate(t, body)
}

func businessTestIngest(t *testing.T, s *Store, update Update) {
	t.Helper()
	handled, err := s.IngestBusinessContactUpdate(context.Background(), update)
	if err != nil || !handled {
		t.Fatalf("business ingestion failed: handled=%v err=%v", handled, err)
	}
}

func businessTestCount(t *testing.T, s *Store) int {
	t.Helper()
	if err := s.initBusinessContacts(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM profile_business_contacts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestBusinessContactsCaptureRawIdempotenceAndHistory(t *testing.T) {
	s, cfg, input, _ := contactFixture(t)
	ctx := context.Background()
	before, err := s.Version(ctx, 1, input.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	u := businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/Example16")
	businessTestIngest(t, s, u)
	businessTestIngest(t, s, u)
	if businessTestCount(t, s) != 1 {
		t.Fatal("duplicate receipt was not idempotent")
	}
	var raw string
	if err := s.DB.QueryRow("SELECT raw_json FROM profile_business_contacts WHERE update_id=101").Scan(&raw); err != nil || raw != string(u.RawJSON) || !strings.Contains(raw, "unknown_message_field") {
		t.Fatal("original update JSON was changed or dropped", err)
	}
	contact, err := s.ProfileTelegramContact(ctx, 1, 77)
	if err != nil || contact == nil || contact.ContactURL != "https://t.me/Example16" || contact.EvidenceKind != "telegram_business_message" || contact.SourceLocatorKind != "bot_api_message" || contact.VersionID != 0 || contact.SourceMessageLocator != "business:fixture-connection-1:900" || contact.UpstreamID != 500 {
		t.Fatalf("wrong saved contact: %+v %v", contact, err)
	}
	var projection ContactEvidenceInput
	if err := json.Unmarshal([]byte(contact.InputJSON), &projection); err != nil || projection.ContactURL != contact.ContactURL {
		t.Fatal("existing contact projection unavailable", err)
	}
	changed := mutateBusinessUpdate(t, businessTestMessage(t, cfg.SourceBotID, 1, 102, "https://t.me/Changed17"), func(body map[string]any) {
		body["business_message"].(map[string]any)["date"] = int64(1794170020)
	})
	businessTestIngest(t, s, changed)
	older := mutateBusinessUpdate(t, businessTestMessage(t, cfg.SourceBotID, 1, 103, "https://t.me/OlderName"), func(body map[string]any) {
		body["business_message"].(map[string]any)["date"] = int64(1794170005)
	})
	businessTestIngest(t, s, older)
	contact, err = s.ProfileTelegramContact(ctx, 1, 77)
	if err != nil || contact == nil || contact.ContactURL != "https://t.me/Changed17" || businessTestCount(t, s) != 3 {
		t.Fatal("receipt history or source chronology lost", err)
	}
	conflict := mutateBusinessUpdate(t, u, func(body map[string]any) { body["unknown_update_field"] = "changed source for same update" })
	if _, err := s.IngestBusinessContactUpdate(ctx, conflict); !errors.Is(err, ErrContactEvidenceConflict) || businessTestCount(t, s) != 3 {
		t.Fatal("same update accepted conflicting source proof", err)
	}
	after, err := s.Version(ctx, 1, input.VersionID)
	if err != nil || after.RawJSON != before.RawJSON {
		t.Fatal("Telegram capture changed Jardulik raw profile JSON", err)
	}
	var actions int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("contact receipt created a reaction", err)
	}
}

func TestBusinessContactsRejectSpoofedAmbiguousAndStaleMessages(t *testing.T) {
	tests := map[string]func(map[string]any){
		"other-sender": func(m map[string]any) { m["from"].(map[string]any)["id"] = 999 },
		"non-bot":      func(m map[string]any) { m["from"].(map[string]any)["is_bot"] = false },
		"other-chat":   func(m map[string]any) { m["chat"].(map[string]any)["id"] = 999 },
		"group":        func(m map[string]any) { m["chat"].(map[string]any)["type"] = "supergroup" },
		"forward": func(m map[string]any) {
			m["forward_origin"] = map[string]any{"type": "user", "sender_user": map[string]any{"id": 42}}
		},
		"business-sender":    func(m map[string]any) { m["sender_business_bot"] = map[string]any{"id": 42, "is_bot": true} },
		"unknown-connection": func(m map[string]any) { m["business_connection_id"] = "unknown" },
		"before-connection":  func(m map[string]any) { m["date"] = 1794169999 },
		"missing-markup":     func(m map[string]any) { delete(m, "reply_markup") },
		"extra-contact": func(m map[string]any) {
			markup := m["reply_markup"].(map[string]any)
			markup["inline_keyboard"] = append(markup["inline_keyboard"].([]any), []any{map[string]any{"url": "https://t.me/Another17"}})
		},
		"extra-profile": func(m map[string]any) {
			markup := m["reply_markup"].(map[string]any)
			markup["inline_keyboard"] = append(markup["inline_keyboard"].([]any), []any{map[string]any{"url": "https://mini.jardulik.app/mutuals?targetUserId=78&noCheck=1"}})
		},
		"unconfirmed-profile-route": func(m map[string]any) {
			row := m["reply_markup"].(map[string]any)["inline_keyboard"].([]any)[0].([]any)
			row[0].(map[string]any)["web_app"].(map[string]any)["url"] = "https://mini.jardulik.app/profile?targetUserId=77"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s, cfg, _, _ := contactFixture(t)
			businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
			u := mutateBusinessUpdate(t, businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/Example16"), func(body map[string]any) { mutate(body["business_message"].(map[string]any)) })
			businessTestIngest(t, s, u)
			if businessTestCount(t, s) != 0 {
				t.Fatal("untrusted or ambiguous source created a contact")
			}
		})
	}
	for _, rawURL := range []string{"https://t.me/+invite", "https://t.me/c/123/456", "https://t.me/Example16?start=x", "https://t.me.evil/Example16", "https://user@t.me/Example16", "tg://user?id=77", "https://t.me/Example16#x", "https://t.me/1Example16", "https://t.me/Ex%61mple16"} {
		if validBusinessTelegramURL(rawURL) {
			t.Fatalf("accepted unconfirmed contact URL %s", rawURL)
		}
	}
	for _, rawURL := range []string{"https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1&x=1", "https://mini.jardulik.app/mutuals?targetUserId=077&noCheck=1", "https://mini.jardulik.app/mutuals?targetUserId=77&targetUserId=78&noCheck=1", "https://mini.jardulik.app/mutuals?targetUserId=-1&noCheck=1"} {
		if businessProfileTarget(rawURL) != 0 {
			t.Fatalf("accepted unconfirmed profile URL %s", rawURL)
		}
	}
}

func TestBusinessContactsConnectionRevocationAndAccountBinding(t *testing.T) {
	s, cfg, _, _ := contactFixture(t)
	ctx := context.Background()
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	s.SessionState(ctx, 1, "paused", "fixture")
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/Example16"))
	businessTestIngest(t, s, businessTestConnection(t, 1, 110, false))
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 111, "https://t.me/DeniedLater"))
	if businessTestCount(t, s) != 1 {
		t.Fatal("revoked connection accepted contact or stale enable event")
	}
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact == nil || contact.ContactURL != "https://t.me/Example16" {
		t.Fatal("business disconnection removed retained contact", err)
	}
	if contact, err := s.ProfileTelegramContact(ctx, 2, 77); err != nil || contact != nil {
		t.Fatal("another owner saw contact", err)
	}
	if err := s.SaveSession(ctx, 1, Session{UserID: 999, Generation: "changed", Token: "offline"}); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 112, "https://t.me/DeniedAgain"))
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact != nil || businessTestCount(t, s) != 1 {
		t.Fatal("new Jardulik account inherited old contact", err)
	}
	if _, err := s.IngestBusinessContactUpdate(ctx, businessTestConnection(t, 1, 113, true)); err != nil || businessTestCount(t, s) != 1 {
		t.Fatal("stale business binding should be ignored after account switch", err)
	}
	if err := s.Disconnect(ctx, 1); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 114, false))
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact != nil {
		t.Fatal("disconnected Jardulik account exposed saved contact", err)
	}
}

func TestBusinessContactsNewSessionGenerationNeedsFreshConnection(t *testing.T) {
	s, cfg, _, _ := contactFixture(t)
	ctx := context.Background()
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	if err := s.SaveSession(ctx, 1, Session{UserID: 500, Generation: "new-generation", Token: "offline"}); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/NotAttached"))
	if businessTestCount(t, s) != 0 {
		t.Fatal("stale connection attached to a new session generation")
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 102, true))
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/StaleBeforeRebind"))
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 103, "https://t.me/Example16"))
	if businessTestCount(t, s) != 1 {
		t.Fatal("fresh connection failed or old delayed update attached after rebind")
	}
}

func TestBusinessContactsEditedMessageAndDOMChronology(t *testing.T) {
	s, cfg, input, _ := contactFixture(t)
	ctx := context.Background()
	input.SourceMessageTimestamp = 1794170015
	if _, err := s.SaveContactEvidence(ctx, 1, input); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	businessTestIngest(t, s, businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/OlderBusiness"))
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact == nil || contact.EvidenceKind != "official_bot_ui" {
		t.Fatal("older source message replaced newer UI proof", err)
	}
	edited := mutateBusinessUpdate(t, businessTestMessage(t, cfg.SourceBotID, 1, 102, "https://t.me/UpdatedBusiness"), func(body map[string]any) {
		message := body["business_message"].(map[string]any)
		message["edit_date"] = 1794170020
		delete(body, "business_message")
		body["edited_business_message"] = message
	})
	businessTestIngest(t, s, edited)
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact == nil || contact.ContactURL != "https://t.me/UpdatedBusiness" || contact.SourceMessageTimestamp != 1794170020 {
		t.Fatal("edited official contact not projected", err)
	}
	if err := s.DeleteUser(ctx, 1); err != nil || businessTestCount(t, s) != 0 {
		t.Fatal("owner deletion did not cascade business contacts", err)
	}
	var count int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_connections").Scan(&count); err != nil || count != 0 {
		t.Fatal("owner deletion retained business connection", err)
	}
}

func TestBusinessContactsUnknownOwnerAndConcurrentReceipts(t *testing.T) {
	s, cfg, _, _ := contactFixture(t)
	ctx := context.Background()
	businessTestIngest(t, s, businessTestConnection(t, 999, 99, true))
	var users int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM users WHERE id=999").Scan(&users); err != nil || users != 0 {
		t.Fatal("unsolicited business update registered a new owner", err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	u := businessTestMessage(t, cfg.SourceBotID, 1, 101, "https://t.me/Example16")
	second, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var group sync.WaitGroup
	problems := make(chan error, 12)
	for i := 0; i < cap(problems); i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			store := s
			if i%2 != 0 {
				store = second
			}
			_, err := store.IngestBusinessContactUpdate(ctx, u)
			problems <- err
		}(i)
	}
	group.Wait()
	close(problems)
	for err := range problems {
		if err != nil {
			t.Fatal(err)
		}
	}
	if businessTestCount(t, s) != 1 {
		t.Fatal("concurrent retries duplicated contact receipt")
	}
}
