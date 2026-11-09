package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

func businessBackupIngest(t *testing.T, s *Store, raw string) {
	t.Helper()
	var update Update
	if err := json.Unmarshal([]byte(raw), &update); err != nil {
		t.Fatal(err)
	}
	handled, err := s.IngestBusinessContactUpdate(context.Background(), update)
	if err != nil || !handled {
		t.Fatalf("business receipt not committed: handled=%v err=%v", handled, err)
	}
}

func businessBackupMessage(update, message int, contact string) string {
	return fmt.Sprintf(`{"update_id":%d,"business_message":{"message_id":%d,"date":101,"business_connection_id":"fixture-business","from":{"id":4242,"is_bot":true},"chat":{"id":4242,"type":"private"},"reply_markup":{"inline_keyboard":[[{"text":"Profile","web_app":{"url":"https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1"}}],[{"text":"Telegram","url":%q}]]},"unknown_source_field":{"untouched":true}}}`, update, message, contact)
}

func TestBusinessContactBackupEmptySchemaDoesNotDirty(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	s.cfg.SourceBotID = 4242
	before := fullTestFingerprint(t, s)
	for _, table := range []string{"business_contact_connections", "profile_business_contacts"} {
		if exists, err := snapshotHasTable(ctx, s.DB, table); err != nil || exists {
			t.Fatalf("fingerprint created optional table %s: exists=%v err=%v", table, exists, err)
		}
	}
	if err := s.initBusinessContacts(ctx); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != before {
		t.Fatal("ignored receipt or empty optional tables dirtied the backup")
	}
}

func TestBusinessContactBackupTracksBindingsAndSourceHistoryWithoutCaptureNoise(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	s.cfg.SourceBotID = 4242
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "fixture", Generation: "fixture"}); err != nil {
		t.Fatal(err)
	}
	before := fullTestFingerprint(t, s)
	businessBackupIngest(t, s, `{"update_id":50,"business_connection":{"id":"fixture-business","user":{"id":123},"user_chat_id":123,"date":100,"is_enabled":true}}`)
	connected := fullTestFingerprint(t, s)
	if connected == before {
		t.Fatal("new business binding did not dirty the backup")
	}
	businessBackupIngest(t, s, businessBackupMessage(51, 1, "https://t.me/Example16"))
	captured := fullTestFingerprint(t, s)
	if captured == connected {
		t.Fatal("new source contact did not dirty the backup")
	}
	// The same projection received again retains raw proof without requiring another backup.
	businessBackupIngest(t, s, businessBackupMessage(52, 1, "https://t.me/Example16"))
	if fullTestFingerprint(t, s) != captured {
		t.Fatal("repeated contact projection dirtied the backup")
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE business_contact_connections SET update_id=60,raw_json='{"capture":"changed"}' WHERE id='fixture-business';
UPDATE profile_business_contacts SET observed_at_ms=999,raw_json='{"unknown_source_field":"preserved"}'`); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != captured {
		t.Fatal("raw proof or capture metadata dirtied the backup")
	}
	businessBackupIngest(t, s, businessBackupMessage(61, 2, "https://t.me/Changed17"))
	changed := fullTestFingerprint(t, s)
	if changed == captured {
		t.Fatal("changed source contact did not dirty the backup")
	}
	var earlier int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM profile_business_contacts WHERE contact_url='https://t.me/Example16'`).Scan(&earlier); err != nil || earlier == 0 {
		t.Fatalf("new contact erased old proof: count=%d err=%v", earlier, err)
	}
	businessBackupIngest(t, s, `{"update_id":62,"business_connection":{"id":"fixture-business","user":{"id":123},"user_chat_id":123,"date":100,"is_enabled":false}}`)
	disabled := fullTestFingerprint(t, s)
	if disabled == changed {
		t.Fatal("revoked business connection did not dirty the backup")
	}
	// The snapshot used by a full backup preserves raw receipts, including unknown source fields.
	path := filepath.Join(t.TempDir(), "business.sqlite")
	if err := s.Snapshot(ctx, path); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw string
	if err = db.QueryRowContext(ctx, "SELECT raw_json FROM profile_business_contacts WHERE update_id=61").Scan(&raw); err != nil || raw != businessBackupMessage(61, 2, "https://t.me/Changed17") {
		t.Fatalf("snapshot lost exact source receipt: %q err=%v", raw, err)
	}
}
