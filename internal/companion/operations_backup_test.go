package companion

import (
	"context"
	"testing"
)

func TestOperationsBackupPreservesReceiptsWithoutHeartbeatNoise(t *testing.T) {
	f := newOperationsFixture(t)
	s := f.s.Store
	exec := func(query string) {
		t.Helper()
		if _, err := s.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	pending := fullTestFingerprint(t, s)
	exec("UPDATE operations_channel SET topic_state='sending'")
	if pending == fullTestFingerprint(t, s) {
		t.Fatal("backup lost the unconfirmed new topic")
	}
	exec("UPDATE operations_channel SET topic_state='sent',thread_id=811,status_state='uncertain'")
	uncertain := fullTestFingerprint(t, s)
	exec("UPDATE operations_channel SET status_state='pending'")
	if uncertain == fullTestFingerprint(t, s) {
		t.Fatal("backup lost the unconfirmed new status message")
	}
	exec("UPDATE operations_channel SET status_state='sent',status_message_id=902")
	f.addEvent(t)
	before := fullTestFingerprint(t, s)
	exec("UPDATE operations_events SET state='uncertain'")
	if before == fullTestFingerprint(t, s) {
		t.Fatal("backup lost an unknown error-post outcome")
	}
	exec("UPDATE operations_events SET state='sent',message_id=903")
	confirmed := fullTestFingerprint(t, s)
	exec("UPDATE operations_events SET occurrence_count=2,last_at_ms=last_at_ms+1000,state='pending',next_attempt=999")
	exec("UPDATE operations_channel SET status_sha='new-text',status_next=999,status_error='retry'")
	if err := s.UpdateOperationsHeartbeat(context.Background(), RuntimePoster); err != nil {
		t.Fatal(err)
	}
	if confirmed != fullTestFingerprint(t, s) {
		t.Fatal("heartbeat or an edit of a known message dirtied the daily backup")
	}
}

func TestOpeningTimelineBackupPreservesUnknownAndConfirmedDeliveries(t *testing.T) {
	f := newProfileIndexesFixture(t)
	f.add(t, 10, 23, "Name", "2003-03-01")
	action := timelineIntent(t, f, 10, 1, "actual message")
	if err := f.s.initOpeningTimeline(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := f.s.Store.DB.Exec(`INSERT INTO opening_timeline_posts
 (owner_id,upstream_id,action_id,profile_id,chat_id,thread_id,attempted_at_ms,source_json,source_sha,payload_json,payload_sha)
 VALUES(123,5696,?,10,-100123,110,1,'{}','source','{}','payload')`, action)
	if err != nil {
		t.Fatal(err)
	}
	pending := fullTestFingerprint(t, f.s.Store)
	if _, err = f.s.Store.DB.Exec("UPDATE opening_timeline_posts SET state='uncertain'"); err != nil {
		t.Fatal(err)
	}
	if pending == fullTestFingerprint(t, f.s.Store) {
		t.Fatal("backup lost an unknown opener timeline delivery")
	}
	if _, err = f.s.Store.DB.Exec("UPDATE opening_timeline_posts SET state='sent',message_id=2001"); err != nil {
		t.Fatal(err)
	}
	confirmed := fullTestFingerprint(t, f.s.Store)
	if _, err = f.s.Store.DB.Exec("UPDATE opening_timeline_posts SET error='retry detail',updated_at='2099-01-01'"); err != nil {
		t.Fatal(err)
	}
	if confirmed != fullTestFingerprint(t, f.s.Store) {
		t.Fatal("timeline bookkeeping dirtied the daily backup")
	}
}
