package companion

import (
	"context"
	"testing"
)

func TestProfileIndexBackupKeepsUnconfirmedEffectsWithoutEditNoise(t *testing.T) {
	_, store := archiveTestStore(t)
	service := &Service{Store: store}
	ctx := context.Background()
	if err := service.initProfileIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := store.DB.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO profile_index_topics(id,owner_id,upstream_id,generation,kind,chat_id,state) VALUES(1,123,5696,'fixture','age',-100777,'pending')`)
	pending := fullTestFingerprint(t, store)
	exec(`UPDATE profile_index_topics SET state='sending' WHERE id=1`)
	sending := fullTestFingerprint(t, store)
	if sending == pending {
		t.Fatal("backup lost the non-idempotent topic reservation")
	}
	exec(`UPDATE profile_index_topics SET error='transient',next_attempt=999 WHERE id=1`)
	if sending != fullTestFingerprint(t, store) {
		t.Fatal("topic retry metadata dirtied the backup")
	}
	exec(`UPDATE profile_index_topics SET state='uncertain' WHERE id=1`)
	if pending == fullTestFingerprint(t, store) {
		t.Fatal("backup skipped an unconfirmed created topic")
	}
	exec(`UPDATE profile_index_topics SET state='sent',thread_id=700 WHERE id=1`)
	exec(`INSERT INTO profile_index_pages(topic_id,page_no,state,payload_json,desired_sha) VALUES(1,1,'pending','{}','first')`)
	pagePending := fullTestFingerprint(t, store)
	exec(`UPDATE profile_index_pages SET state='sending' WHERE topic_id=1 AND page_no=1`)
	pageSending := fullTestFingerprint(t, store)
	if pageSending == pagePending {
		t.Fatal("backup lost the non-idempotent page reservation")
	}
	exec(`UPDATE profile_index_pages SET state='uncertain' WHERE topic_id=1 AND page_no=1`)
	if pagePending == fullTestFingerprint(t, store) {
		t.Fatal("backup skipped an unconfirmed sent page")
	}
	exec(`UPDATE profile_index_pages SET state='sent',message_id=2000,sent_sha='first' WHERE topic_id=1 AND page_no=1`)
	confirmed := fullTestFingerprint(t, store)
	exec(`UPDATE profile_index_pages SET state='error',payload_json='{"text":"updated"}',desired_sha='second',error='edit retry',next_attempt=1000 WHERE topic_id=1 AND page_no=1`)
	if confirmed != fullTestFingerprint(t, store) {
		t.Fatal("editing a confirmed page dirtied the backup without new IDs or source data")
	}
}
