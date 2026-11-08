package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func contactFixture(t *testing.T) (*Store, Config, ContactEvidenceInput, int64) {
	t.Helper()
	s, c := testStore(t)
	ctx := context.Background()
	for owner := int64(1); owner <= 2; owner++ {
		if err := s.User(ctx, owner, "contact-fixture"); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveSession(ctx, owner, Session{UserID: 400 + owner*100, Token: "offline-access", Refresh: "offline-refresh", Generation: "initial"}); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := s.Archive(ctx, 1, []Profile{{"userId": 77, "name": "Fixture", "unknown_source_field": "unchanged"}}, "contact-fixture")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.Archive(ctx, 2, []Profile{{"userId": 77, "name": "Other owner"}}, "contact-fixture")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC)
	input := ContactEvidenceInput{UpstreamID: 500, ProfileID: 77, VersionID: ids[0], SourceBotID: c.SourceBotID, SourcePeerID: c.SourceBotID, SourceLocatorKind: "telegram_web_dom", SourceMessageLocator: "data-mid:001-offline-proof", SourceMessageTimestamp: now.Add(-time.Hour).Unix(), ContactURL: "https://t.me/Example16", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1", ObservedAt: now}
	return s, c, input, other[0]
}

func contactCount(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM profile_contact_evidence").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestContactEvidenceIdempotenceHistoryAndSourceChronology(t *testing.T) {
	s, _, input, _ := contactFixture(t)
	ctx := context.Background()
	before, err := s.Version(ctx, 1, input.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.SaveContactEvidence(ctx, 1, input)
	wantInput, _ := json.Marshal(input)
	if err != nil || first.ID <= 0 || first.OwnerID != 1 || first.SourceMessageLocator != input.SourceMessageLocator || first.InputJSON != string(wantInput) || first.EvidenceKind != "official_bot_ui" {
		t.Fatalf("source UI evidence lost: %+v %v", first, err)
	}
	reimport := input
	reimport.ObservedAt = reimport.ObservedAt.Add(time.Hour)
	second, err := s.SaveContactEvidence(ctx, 1, reimport)
	if err != nil || second.ID != first.ID || !second.ObservedAt.Equal(input.ObservedAt) || contactCount(t, s) != 1 {
		t.Fatal("same proof reimport changed its original capture or duplicated history", err)
	}
	changed := reimport
	changed.ContactURL = "https://t.me/Changed17"
	latest, err := s.SaveContactEvidence(ctx, 1, changed)
	if err != nil || latest.ID == first.ID || contactCount(t, s) != 2 {
		t.Fatal("changed observed contact did not retain history", err)
	}
	older := changed
	older.SourceMessageTimestamp -= 3600
	older.SourceMessageLocator = "data-mid:older-proof"
	older.ContactURL = "https://t.me/OlderName"
	older.ObservedAt = older.ObservedAt.Add(time.Hour)
	if _, err := s.SaveContactEvidence(ctx, 1, older); err != nil {
		t.Fatal(err)
	}
	got, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID)
	if err != nil || got == nil || got.ID != latest.ID {
		t.Fatal("late import of an old source message replaced a newer contact", err)
	}
	conflict := input
	conflict.SourceMessageTimestamp++
	if _, err := s.SaveContactEvidence(ctx, 1, conflict); !errors.Is(err, ErrContactEvidenceConflict) || contactCount(t, s) != 3 {
		t.Fatal("conflicting source timestamp silently replaced existing proof", err)
	}
	after, err := s.Version(ctx, 1, input.VersionID)
	if err != nil || before.RawJSON != after.RawJSON {
		t.Fatal("contact import altered the source profile JSON", err)
	}
}

func TestContactEvidenceRejectsUnobservedURLsIdentityAndLocator(t *testing.T) {
	s, _, input, _ := contactFixture(t)
	if err := s.initContactEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*ContactEvidenceInput){
		"source-bot":     func(p *ContactEvidenceInput) { p.SourceBotID++ },
		"source-peer":    func(p *ContactEvidenceInput) { p.SourcePeerID++ },
		"locator-kind":   func(p *ContactEvidenceInput) { p.SourceLocatorKind = "mtproto_message_id" },
		"empty-locator":  func(p *ContactEvidenceInput) { p.SourceMessageLocator = "data-mid:" },
		"unsafe-locator": func(p *ContactEvidenceInput) { p.SourceMessageLocator += "\nsecret" },
		"source-time":    func(p *ContactEvidenceInput) { p.SourceMessageTimestamp = p.ObservedAt.Unix() + 1 },
		"capture-time":   func(p *ContactEvidenceInput) { p.ObservedAt = time.Time{} },
		"target-id":      func(p *ContactEvidenceInput) { p.ProfileID++ },
		"negative-version": func(p *ContactEvidenceInput) {
			p.VersionID = -1
		},
	}
	for _, contact := range []string{"http://t.me/Example16", "https://t.me.evil.invalid/Example16", "https://evil@t.me/Example16", "https://t.me/1234567", "https://t.me/c/123/456", "https://t.me/+invite-code", "https://t.me/Example16?start=token", "https://t.me/Example16#tgWebAppData=private", "https://t.me/%45xample16", "https://t.me/Example16/"} {
		value := contact
		cases[contact] = func(p *ContactEvidenceInput) { p.ContactURL = value }
	}
	for _, profile := range []string{"https://mini.jardulik.app/", "https://mini.jardulik.app/mutuals?targetUserId=78&noCheck=1", "https://mini.jardulik.app/mutuals?targetUserId=77&targetUserId=78&noCheck=1", "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=0", "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1&tgWebAppData=private", "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1#tgWebAppData=private", "https://mini.jardulik.app.evil.invalid/mutuals?targetUserId=77&noCheck=1"} {
		value := profile
		cases[profile] = func(p *ContactEvidenceInput) { p.ProfileURL = value }
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			bad := input
			change(&bad)
			if _, err := s.SaveContactEvidence(context.Background(), 1, bad); err == nil || contactCount(t, s) != 0 {
				t.Fatal("invalid contact evidence was persisted", err)
			}
		})
	}
}

func TestContactEvidenceOwnerOptionalVersionAndDeleteCascade(t *testing.T) {
	s, _, input, foreignVersion := contactFixture(t)
	ctx := context.Background()
	bad := input
	bad.VersionID = foreignVersion
	if _, err := s.SaveContactEvidence(ctx, 1, bad); !errors.Is(err, ErrContactVersionMismatch) {
		t.Fatal("foreign owner's version accepted", err)
	}
	bad = input
	bad.ProfileID = 78
	bad.ProfileURL = "https://mini.jardulik.app/mutuals?targetUserId=78&noCheck=1"
	if _, err := s.SaveContactEvidence(ctx, 1, bad); !errors.Is(err, ErrContactVersionMismatch) {
		t.Fatal("a version was attached to another profile", err)
	}
	input.VersionID = 0
	if result, err := s.SaveContactEvidence(ctx, 1, input); err != nil || result.VersionID != 0 {
		t.Fatal("UI evidence incorrectly claimed a full snapshot", err)
	}
	if result, err := s.ProfileTelegramContact(ctx, 2, input.ProfileID); err != nil || result != nil {
		t.Fatal("contact revealed to another owner", err)
	}
	other := input
	other.UpstreamID, other.ContactURL = 600, "https://t.me/OtherUser"
	if _, err := s.SaveContactEvidence(ctx, 2, other); err != nil {
		t.Fatal(err)
	}
	if result, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID); err != nil || result == nil || result.ContactURL != input.ContactURL {
		t.Fatal("another owner overwrote the contact", err)
	}
	if err := s.DeleteUser(ctx, 1); err != nil || contactCount(t, s) != 1 {
		t.Fatal("owner deletion did not cascade contact history", err)
	}
	if result, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID); err != nil || result != nil {
		t.Fatal("deleted owner's contact remained projected", err)
	}
}

func TestContactEvidenceSessionStatesAndRefreshDoNotLeakAccounts(t *testing.T) {
	s, _, input, _ := contactFixture(t)
	ctx := context.Background()
	if _, err := s.SaveContactEvidence(ctx, 1, input); err != nil {
		t.Fatal(err)
	}
	s.Pause(ctx, 1, true)
	for _, state := range []string{"connected", "paused", "reconnect_required", "disconnected"} {
		s.SessionState(ctx, 1, state, "offline-fixture")
		result, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID)
		if err != nil || (result == nil) != (state == "disconnected") {
			t.Fatalf("wrong cached projection in %s: %+v %v", state, result, err)
		}
		_, err = s.SaveContactEvidence(ctx, 1, input)
		if (state == "connected" || state == "paused") && err != nil || (state == "reconnect_required" || state == "disconnected") && !errors.Is(err, ErrContactSessionUnavailable) {
			t.Fatalf("wrong import state guard in %s: %v", state, err)
		}
	}
	if err := s.Disconnect(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if result, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID); err != nil || result != nil {
		t.Fatal("missing session exposed contact", err)
	}
	for _, upstream := range []int64{500, 700, 500} {
		if err := s.SaveSession(ctx, 1, Session{UserID: upstream, Token: "offline-new-token", Refresh: "offline-new-refresh", Generation: fmt.Sprint(upstream)}); err != nil {
			t.Fatal(err)
		}
		result, err := s.ProfileTelegramContact(ctx, 1, input.ProfileID)
		if err != nil || (result == nil) != (upstream != 500) {
			t.Fatal("refresh generation lost contacts or account switch leaked them", upstream, err)
		}
		if upstream != input.UpstreamID {
			if _, err := s.SaveContactEvidence(ctx, 1, input); !errors.Is(err, ErrContactSessionChanged) {
				t.Fatal("stale CLI account binding was accepted", err)
			}
		}
	}
}

func TestContactEvidenceConcurrentImportsAndAccountRebind(t *testing.T) {
	s, cfg, input, _ := contactFixture(t)
	second, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	ids := make(chan int64, 8)
	start := make(chan struct{})
	for i := range 8 {
		store := s
		if i%2 != 0 {
			store = second
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			evidence, err := store.SaveContactEvidence(ctx, 1, input)
			errs <- err
			ids <- evidence.ID
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatal("simultaneous contact import failed", err)
		}
	}
	var first int64
	for id := range ids {
		if first == 0 {
			first = id
		}
		if id != first {
			t.Fatal("simultaneous imports did not share an idempotent row")
		}
	}
	if contactCount(t, s) != 1 {
		t.Fatal("concurrent imports duplicated evidence")
	}
	holder, err := second.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.ExecContext(ctx, "UPDATE sessions SET upstream_id=700 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	stale := input
	stale.SourceMessageLocator = "data-mid:pending-old-account"
	result := make(chan error, 1)
	go func() {
		_, err := s.SaveContactEvidence(ctx, 1, stale)
		result <- err
	}()
	select {
	case err = <-result:
		t.Fatal("contact import bypassed the active account rebind transaction", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err = holder.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, ErrContactSessionChanged) || strings.Contains(err.Error(), "SQLITE_BUSY_SNAPSHOT") || contactCount(t, s) != 1 {
		t.Fatal("import failed to recheck the newly committed account", err)
	}
}
