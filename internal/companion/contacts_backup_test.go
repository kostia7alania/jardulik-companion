package companion

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestContactBackupLegacyAndEmptyEvidenceDoNotDirty(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	before := fullTestFingerprint(t, s)
	if exists, err := snapshotHasTable(ctx, s.DB, "profile_contact_evidence"); err != nil || exists {
		t.Fatalf("fingerprint created optional contact table: exists=%v err=%v", exists, err)
	}
	if err := s.initContactEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != before {
		t.Fatal("empty contact evidence changed a legacy fingerprint")
	}
}

func TestContactBackupTracksEvidenceHistoryWithoutCaptureNoise(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	s.cfg.SourceBotID = 4242
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "fixture", Generation: "fixture"}); err != nil {
		t.Fatal(err)
	}
	versions, err := s.Archive(ctx, 123, []Profile{{"userId": 77, "name": "Fixture profile", "description": "Unmodified source JSON"}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	var original string
	if err := s.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=?", versions[0]).Scan(&original); err != nil {
		t.Fatal(err)
	}
	observed := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	input := ContactEvidenceInput{
		UpstreamID: 500, ProfileID: 77, SourceBotID: 4242, SourcePeerID: 4242,
		SourceLocatorKind: "telegram_web_dom", SourceMessageLocator: "data-mid:offline-backup-fixture",
		SourceMessageTimestamp: observed.Add(-time.Hour).Unix(), ObservedAt: observed,
		ContactURL: "https://t.me/Example16", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1",
	}
	before := fullTestFingerprint(t, s)
	first, err := s.SaveContactEvidence(ctx, 123, input)
	if err != nil {
		t.Fatal(err)
	}
	firstFingerprint := fullTestFingerprint(t, s)
	if firstFingerprint == before {
		t.Fatal("contact-only evidence addition did not dirty the backup")
	}
	for i := 0; i < 2; i++ {
		contact, err := s.ProfileTelegramContact(ctx, 123, 77)
		if err != nil || contact == nil || contact.ID != first.ID {
			t.Fatalf("contact lookup failed: %+v err=%v", contact, err)
		}
	}
	reimport := input
	reimport.ObservedAt = observed.Add(24 * time.Hour)
	reimport.VersionID = versions[0]
	repeated, err := s.SaveContactEvidence(ctx, 123, reimport)
	if err != nil || repeated.ID != first.ID {
		t.Fatalf("idempotent reimport created different evidence: %+v err=%v", repeated, err)
	}
	if fullTestFingerprint(t, s) != firstFingerprint {
		t.Fatal("lookup or repeated capture dirtied the backup")
	}
	changed := input
	changed.ContactURL = "https://t.me/FixtureChanged17"
	changed.ObservedAt = observed.Add(48 * time.Hour)
	second, err := s.SaveContactEvidence(ctx, 123, changed)
	if err != nil || second.ID == first.ID {
		t.Fatalf("changed URL did not create history: %+v err=%v", second, err)
	}
	changedFingerprint := fullTestFingerprint(t, s)
	if changedFingerprint == firstFingerprint {
		t.Fatal("new contact URL did not dirty the backup")
	}
	var count int
	var retainedURL string
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM profile_contact_evidence").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT contact_url FROM profile_contact_evidence WHERE id=?", first.ID).Scan(&retainedURL); err != nil {
		t.Fatal(err)
	}
	if count != 2 || retainedURL != input.ContactURL {
		t.Fatal("changed contact erased earlier evidence")
	}
	// Local association and capture metadata do not change the observed source proof.
	metadata, err := json.Marshal(reimport)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, "UPDATE profile_contact_evidence SET version_id=?,observed_at_ms=?,input_json=? WHERE id=?", versions[0], reimport.ObservedAt.UnixMilli(), string(metadata), first.ID); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != changedFingerprint {
		t.Fatal("local association or capture metadata dirtied the backup")
	}
	var saved string
	if err := s.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=?", versions[0]).Scan(&saved); err != nil || saved != original {
		t.Fatalf("contact evidence changed source profile JSON: %q err=%v", saved, err)
	}
}
