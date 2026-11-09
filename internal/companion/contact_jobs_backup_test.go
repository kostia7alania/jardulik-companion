package companion

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestContactJobsBackupEmptySchemaDoesNotDirty(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	before := fullTestFingerprint(t, s)
	if exists, err := snapshotHasTable(ctx, s.DB, "contact_requests"); err != nil || exists {
		t.Fatalf("fingerprint created optional contact queue: exists=%v err=%v", exists, err)
	}
	if err := s.initContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != before {
		t.Fatal("empty contact queue changed a legacy fingerprint")
	}
}

func TestContactJobsBackupTracksResponsesWithoutProofAndRetryNoise(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	if err := s.initContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	before := fullTestFingerprint(t, s)
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO contact_requests(owner_id,upstream_id,profile_id,generation,proof_at_ms,state,created_at_ms,updated_at_ms)
 VALUES(123,500,77,'fixture',1,'queued',1,1)`); err != nil {
		t.Fatal(err)
	}
	queued := fullTestFingerprint(t, s)
	if queued == before {
		t.Fatal("new durable contact job did not dirty the backup")
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE contact_requests SET generation='refreshed-proof',proof_at_ms=200,attempted_at_ms=300,completed_at_ms=400,created_at_ms=500,updated_at_ms=600,retry_after_ms=700`); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != queued {
		t.Fatal("proof, generation or retry metadata dirtied the backup")
	}
	request := `{"fromUserId":500,"targetUserId":77}`
	if _, err := s.DB.ExecContext(ctx, "UPDATE contact_requests SET request_json=?,state='sending'", request); err != nil {
		t.Fatal(err)
	}
	sending := fullTestFingerprint(t, s)
	if sending == queued {
		t.Fatal("claimed request and state did not dirty the backup")
	}
	// The body is intentionally not JSON and contains binary bytes from an unknown reply.
	body := []byte{' ', '{', 'x', 0, 255, '}', '\n'}
	if _, err := s.DB.ExecContext(ctx, "UPDATE contact_requests SET state='unknown',response_body=?,http_status=502,reason='transport_unknown'", body); err != nil {
		t.Fatal(err)
	}
	unknown := fullTestFingerprint(t, s)
	if unknown == sending {
		t.Fatal("unknown raw POST response did not dirty the backup")
	}
	changedBody := append(append([]byte(nil), body...), ' ')
	if _, err := s.DB.ExecContext(ctx, "UPDATE contact_requests SET response_body=?", changedBody); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) == unknown {
		t.Fatal("exact raw response bytes were normalized in the fingerprint")
	}
	path := filepath.Join(t.TempDir(), "contact-jobs.sqlite")
	if err := s.Snapshot(ctx, path); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var savedRequest string
	var savedBody []byte
	if err = db.QueryRowContext(ctx, "SELECT request_json,response_body FROM contact_requests WHERE owner_id=123 AND profile_id=77").Scan(&savedRequest, &savedBody); err != nil || savedRequest != request || !bytes.Equal(savedBody, changedBody) {
		t.Fatalf("snapshot changed raw contact request or response: request=%q body=%v err=%v", savedRequest, savedBody, err)
	}
}

func TestContactInboxBackupKeepsUnresolvedRawReceiptsWithoutCaptureNoise(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	if err := s.initBusinessContacts(ctx); err != nil {
		t.Fatal(err)
	}
	before := fullTestFingerprint(t, s)
	raw := " {\n  \"unknown_reply\": {\"future_field\": true}\n} "
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO business_contact_inbox(owner_id,upstream_id,source_bot_id,connection_id,source_message_id,source_message_timestamp,update_id,captured_at_ms,parse_state,raw_json)
 VALUES(123,500,4242,'fixture-business',1,101,47,200,'unresolved',?)`, raw); err != nil {
		t.Fatal(err)
	}
	captured := fullTestFingerprint(t, s)
	if captured == before {
		t.Fatal("unresolved official source receipt did not dirty the backup")
	}
	if _, err := s.DB.ExecContext(ctx, "UPDATE business_contact_inbox SET captured_at_ms=300"); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != captured {
		t.Fatal("repeated source capture timing dirtied the backup")
	}
	// An unknown incoming source is retained verbatim, even when its JSON shape is not understood.
	changedRaw := raw + " "
	if _, err := s.DB.ExecContext(ctx, "UPDATE business_contact_inbox SET raw_json=?", changedRaw); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) == captured {
		t.Fatal("raw receipt bytes were normalized in the fingerprint")
	}
	path := filepath.Join(t.TempDir(), "contact-inbox.sqlite")
	if err := s.Snapshot(ctx, path); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var saved string
	if err = db.QueryRowContext(ctx, "SELECT raw_json FROM business_contact_inbox WHERE update_id=47").Scan(&saved); err != nil || saved != changedRaw {
		t.Fatalf("snapshot lost exact unresolved source receipt: %q err=%v", saved, err)
	}
}

func TestContactInboxBackupNewSourceUpdateChangesFingerprint(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	s.cfg.SourceBotID = 4242
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "fixture", Generation: "fixture"}); err != nil {
		t.Fatal(err)
	}
	businessBackupIngest(t, s, `{"update_id":50,"business_connection":{"id":"fixture-business","user":{"id":123},"user_chat_id":123,"date":100,"is_enabled":true}}`)
	businessBackupIngest(t, s, businessBackupMessage(51, 1, "https://t.me/Example16"))
	captured := fullTestFingerprint(t, s)
	// The contact projection stays the same, but a new source update has its own raw receipt.
	businessBackupIngest(t, s, businessBackupMessage(52, 1, "https://t.me/Example16"))
	if fullTestFingerprint(t, s) == captured {
		t.Fatal("new source update did not dirty the backup")
	}
	var receipts, contacts int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM business_contact_inbox").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(DISTINCT contact_url) FROM profile_business_contacts").Scan(&contacts); err != nil {
		t.Fatal(err)
	}
	if receipts != 2 || contacts != 1 {
		t.Fatalf("new source update must retain its proof without inventing another contact: receipts=%d contacts=%d", receipts, contacts)
	}
}
