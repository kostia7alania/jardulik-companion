package companion

import (
	"context"
	"testing"
)

func TestSyncBackupIgnoresRoutineProgressButKeepsBlockedIntent(t *testing.T) {
	_, store := archiveTestStore(t)
	ctx := context.Background()
	if err := store.InitSyncCycles(ctx); err != nil {
		t.Fatal(err)
	}
	base := fullTestFingerprint(t, store)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := store.DB.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES('cycle',123,5696,'fixture','search','run',1,1,100)`)
	exec(`INSERT INTO sync_refresh_items(cycle_id,profile_id,state) VALUES('cycle',99,'pending')`)
	if base != fullTestFingerprint(t, store) {
		t.Fatal("read-only cycle planning created a backup without source changes")
	}
	exec(`UPDATE sync_cycles SET last_tick_ms=50,updated_at_ms=50,stage='refresh'`)
	exec(`UPDATE sync_refresh_items SET state='available',next_attempt_ms=50,observed_at_ms=50`)
	if base != fullTestFingerprint(t, store) {
		t.Fatal("routine refresh progress dirtied source fingerprint")
	}
	exec(`UPDATE sync_cycles SET stage='blocked',reason='source_changed'`)
	blocked := fullTestFingerprint(t, store)
	if blocked == base {
		t.Fatal("backup skipped durable blocked source intent")
	}
	exec(`UPDATE sync_cycles SET last_tick_ms=99,updated_at_ms=99`)
	if blocked != fullTestFingerprint(t, store) {
		t.Fatal("blocked polling timestamps dirtied backup")
	}
}

func TestOpeningBackupKeepsUnknownSendWithoutEditNoise(t *testing.T) {
	_, store := archiveTestStore(t)
	ctx := context.Background()
	svc := &Service{Store: store}
	if err := svc.initOpeningPosts(ctx); err != nil {
		t.Fatal(err)
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := store.DB.ExecContext(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO opening_audit_topics(id,owner_id,upstream_id,generation,chat_id,state) VALUES(1,123,5696,'fixture',-100777,'pending')`)
	pending := fullTestFingerprint(t, store)
	exec(`UPDATE opening_audit_topics SET state='uncertain'`)
	if pending == fullTestFingerprint(t, store) {
		t.Fatal("unconfirmed topic was absent from backup fingerprint")
	}
	exec(`UPDATE opening_audit_topics SET state='sent',thread_id=700`)
	exec(`INSERT INTO opening_audit_posts(topic_id,item_key,kind,state,payload_json,desired_sha,source_json,source_sha) VALUES(1,'draft:1','draft','pending','{}','first','{}','source')`)
	pending = fullTestFingerprint(t, store)
	exec(`UPDATE opening_audit_posts SET state='sending'`)
	if pending == fullTestFingerprint(t, store) {
		t.Fatal("unconfirmed audit message was absent from backup fingerprint")
	}
	exec(`UPDATE opening_audit_posts SET state='sent',message_id=2000,sent_sha='first'`)
	confirmed := fullTestFingerprint(t, store)
	exec(`UPDATE opening_audit_posts SET state='pending',payload_json='{"text":"updated"}',desired_sha='second',next_attempt=100`)
	if confirmed != fullTestFingerprint(t, store) {
		t.Fatal("known message edit queued another full backup")
	}
}
