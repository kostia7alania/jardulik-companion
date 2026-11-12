package companion

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

const syncAuthRecoveryFixtureEvent = 69
const syncAuthRecoveryFixtureFields = `{"source":"Jardulik","stage":"/user/getMe","method":"GET","kind":"Jardulik: request_rejected (HTTP 400)","tenant":1,"http_status":400}`

func syncAuthRecoveryExec(t *testing.T, s *Service, query string, args ...any) {
	t.Helper()
	if _, err := s.Store.DB.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func syncAuthRecoveryFixture(t *testing.T) (*Service, *time.Time, SyncCycle, *CrawlJob) {
	t.Helper()
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("initial cycle start=%v err=%v", started, err)
	}
	initial := syncStatus(t, s)
	stopped := now.Add(2 * time.Second).UnixMilli()
	syncAuthRecoveryExec(t, s, "UPDATE feed_crawls SET state='stopped',reason='auth_required',updated_at=? WHERE owner_id=1", stopped)
	s.Store.SessionState(ctx, 1, "reconnect_required", "refresh_not_confirmed")
	if err := s.Store.InitOperations(ctx); err != nil {
		t.Fatal(err)
	}
	syncAuthRecoveryExec(t, s, `INSERT INTO operations_events(id,owner_id,chat_id,fingerprint,role,level,message,fields_json,first_at_ms,last_at_ms)
 VALUES(?,1,0,'fixture-auth-error','bot-parser','WARN','API operation failed',?,?,?)`, syncAuthRecoveryFixtureEvent, syncAuthRecoveryFixtureFields, stopped-2, stopped-2)
	*now = now.Add(time.Hour)
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "fresh", Refresh: "refresh", UserID: initial.UserID, Generation: "source-two", Expires: now.Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	syncAuthRecoveryExec(t, s, "INSERT INTO events(owner_id,kind,created_at) VALUES(1,'connect_success',?)", now.Format("2006-01-02 15:04:05"))
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	old := syncStatus(t, s)
	if old.ID != initial.ID || old.Stage != "blocked" || old.Reason != syncCycleBindingChanged {
		t.Fatalf("fixture did not block on the changed source: %+v", old)
	}
	oldJob, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || oldJob == nil {
		t.Fatalf("stopped crawl=%+v err=%v", oldJob, err)
	}
	return s, now, old, oldJob
}

func assertUnchangedAuthRecovery(t *testing.T, s *Service, old SyncCycle, oldJob *CrawlJob) {
	t.Helper()
	ctx := context.Background()
	if after := syncStatus(t, s); after != old {
		t.Fatalf("old cycle changed: before=%+v after=%+v", old, after)
	}
	if after, err := s.Store.CrawlStatus(ctx, 1); err != nil || !reflect.DeepEqual(oldJob, after) {
		t.Fatalf("old crawl changed: before=%+v after=%+v err=%v", oldJob, after, err)
	}
	if got := s.Store.Meta(ctx, "sync_auth_recovery:"+old.ID); got != "" {
		t.Fatal("unsuccessful recovery left a history record")
	}
	if got := syncPublicationCount(t, s.Store, `SELECT count(*) FROM crawl_presence_profiles
 WHERE owner_id=1 AND run_id=? AND profile_id=7 AND is_baseline=1 AND seen=0`, oldJob.RunID); got != 1 {
		t.Fatalf("old crawl baseline changed: count=%d", got)
	}
}

func TestSyncAuthRecoveryCommitsCrawlAndCycleTogether(t *testing.T) {
	s, now, old, oldJob := syncAuthRecoveryFixture(t)
	ctx := context.Background()
	gap := now.Add(30 * time.Second).UnixMilli()
	if err := s.Store.SetMeta(ctx, "feed_crawl_next", strconv.FormatInt(gap, 10)); err != nil {
		t.Fatal(err)
	}
	syncAuthRecoveryExec(t, s, `CREATE TRIGGER reject_auth_recovery_cycle BEFORE INSERT ON sync_cycles
 BEGIN SELECT RAISE(ABORT,'fixture recovery cycle write failure'); END`)
	if _, err := s.RecoverAuthStoppedCycle(ctx, 1, old.ID, oldJob.RunID, syncAuthRecoveryFixtureEvent); err == nil || !strings.Contains(err.Error(), "fixture recovery cycle write failure") {
		t.Fatalf("expected the new cycle insert to fail, got %v", err)
	}
	assertUnchangedAuthRecovery(t, s, old, oldJob)
	syncAuthRecoveryExec(t, s, "DROP TRIGGER reject_auth_recovery_cycle")
	fresh, err := s.RecoverAuthStoppedCycle(ctx, 1, old.ID, oldJob.RunID, syncAuthRecoveryFixtureEvent)
	if err != nil || fresh == nil {
		t.Fatalf("retry did not recover the cycle: %+v %v", fresh, err)
	}
	if *fresh != syncStatus(t, s) || fresh.ID == old.ID || fresh.Stage != "search" || fresh.Generation != "source-two" || fresh.UserID != old.UserID || fresh.StartedAt >= old.NextCycle {
		t.Fatalf("unexpected replacement cycle: %+v", fresh)
	}
	job, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || job == nil || job.RunID == oldJob.RunID || job.RunID != fresh.SearchRunID || job.State != "queued" || job.Mode != 0 || job.Page != 1 || job.NextRequest != gap || job.Generation != fresh.Generation {
		t.Fatalf("unexpected replacement crawl or lost pacing: %+v %v", job, err)
	}
	original, err := scanSyncCycle(s.Store.DB.QueryRow("SELECT "+syncCycleColumns+" FROM sync_cycles WHERE id=?", old.ID))
	if err != nil || original == nil {
		t.Fatalf("old cycle was lost: %+v %v", original, err)
	}
	if original.Stage != "superseded" || original.Reason != old.Reason || original.SearchRunID != oldJob.RunID || original.Generation != old.Generation || original.NextCycle != old.NextCycle {
		t.Fatalf("old cycle provenance changed: %+v", original)
	}
	history := s.Store.Meta(ctx, "sync_auth_recovery:"+old.ID)
	var receipt struct {
		Crawl       *CrawlJob `json:"crawl"`
		EventID     int64     `json:"operations_event_id"`
		NextCycleID string    `json:"next_cycle_id"`
	}
	archivedJob := *oldJob
	archivedJob.Generation = "" // The original cycle already retains the generation.
	if err := json.Unmarshal([]byte(history), &receipt); err != nil || !reflect.DeepEqual(receipt.Crawl, &archivedJob) ||
		receipt.EventID != syncAuthRecoveryFixtureEvent || receipt.NextCycleID != fresh.ID {
		t.Fatalf("recovery did not preserve the stopped crawl and evidence: %+v %v", receipt, err)
	}
	if err := s.Store.requireSyncPublication(ctx, 1); err != errSyncPublicationDeferred {
		t.Fatalf("replacement crawl should keep publication closed: %v", err)
	}
	_, _ = s.RecoverAuthStoppedCycle(ctx, 1, old.ID, oldJob.RunID, syncAuthRecoveryFixtureEvent)
	if got := syncPublicationCount(t, s.Store, "SELECT count(*) FROM sync_cycles WHERE owner_id=1"); got != 2 {
		t.Fatalf("same operator request created another cycle: %d", got)
	}
	if after, err := s.Store.CrawlStatus(ctx, 1); err != nil || !reflect.DeepEqual(job, after) || s.Store.Meta(ctx, "sync_auth_recovery:"+old.ID) != history {
		t.Fatalf("same operator request changed the recovery: crawl=%+v err=%v", after, err)
	}
}

func TestSyncAuthRecoveryRejectsUnprovenStops(t *testing.T) {
	for _, fixture := range []struct {
		name, query string
	}{
		{"progressed", "UPDATE feed_crawls SET received=1 WHERE owner_id=1"},
		{"operator pause", "UPDATE users SET paused=1 WHERE id=1"},
		{"source pause", "UPDATE sessions SET state='paused',last_error='access_denied' WHERE owner_id=1"},
		{"frozen publication", `INSERT INTO sync_publication_manifests(cycle_id,owner_id,upstream_id,generation,frozen_at_ms)
 SELECT id,owner_id,upstream_id,generation,updated_at_ms FROM sync_cycles WHERE owner_id=1`},
		{"persisted operator pause", "INSERT INTO meta(key,value) VALUES('daily_crawl_block:1','1')"},
		{"access denied", "UPDATE operations_events SET fields_json=json_set(fields_json,'$.http_status',403) WHERE id=69"},
		{"rate limited", "UPDATE operations_events SET fields_json=json_set(fields_json,'$.http_status',429) WHERE id=69"},
		{"challenge", "UPDATE operations_events SET fields_json=json_set(fields_json,'$.kind','unexpected_response_or_challenge') WHERE id=69"},
		{"stale evidence", "UPDATE operations_events SET first_at_ms=first_at_ms-60000,last_at_ms=last_at_ms-60000 WHERE id=69"},
		{"missing reconnect receipt", "DELETE FROM events WHERE owner_id=1 AND kind='connect_success'"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			s, _, old, _ := syncAuthRecoveryFixture(t)
			ctx := context.Background()
			syncAuthRecoveryExec(t, s, fixture.query)
			job, err := s.Store.CrawlStatus(ctx, 1)
			if err != nil || job == nil {
				t.Fatalf("fixture crawl=%+v err=%v", job, err)
			}
			if fresh, err := s.RecoverAuthStoppedCycle(ctx, 1, old.ID, job.RunID, syncAuthRecoveryFixtureEvent); err == nil || fresh != nil {
				t.Fatalf("unproven stop recovered: cycle=%+v err=%v", fresh, err)
			}
			assertUnchangedAuthRecovery(t, s, old, job)
		})
	}
}

func TestSyncAuthRecoveryRequiresExactCycleOwnerAndNewSource(t *testing.T) {
	s, _, old, job := syncAuthRecoveryFixture(t)
	ctx := context.Background()
	if fresh, err := s.RecoverAuthStoppedCycle(ctx, 2, old.ID, job.RunID, syncAuthRecoveryFixtureEvent); err == nil || fresh != nil {
		t.Fatalf("different owner recovered: %+v %v", fresh, err)
	}
	if fresh, err := s.RecoverAuthStoppedCycle(ctx, 1, old.ID, old.ID, syncAuthRecoveryFixtureEvent); err == nil || fresh != nil {
		t.Fatalf("different crawl recovered: %+v %v", fresh, err)
	}
	if fresh, err := s.RecoverAuthStoppedCycle(ctx, 1, job.RunID, job.RunID, syncAuthRecoveryFixtureEvent); err == nil || fresh != nil {
		t.Fatalf("different cycle recovered: %+v %v", fresh, err)
	}
	for _, identity := range []Session{
		{UserID: old.UserID + 1, Generation: "source-two"},
		{UserID: old.UserID, Generation: old.Generation},
	} {
		if err := s.Store.SaveSession(ctx, 1, identity); err != nil {
			t.Fatal(err)
		}
		if fresh, err := s.RecoverAuthStoppedCycle(ctx, 1, old.ID, job.RunID, syncAuthRecoveryFixtureEvent); err == nil || fresh != nil {
			t.Fatalf("old or different source recovered: %+v %v", fresh, err)
		}
	}
	assertUnchangedAuthRecovery(t, s, old, job)
}
