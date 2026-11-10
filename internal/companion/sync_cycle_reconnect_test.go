package companion

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSyncCycleReconnectSupersedesFrozenPublicationAtDueTime(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := s.Store.DB.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	step := func() {
		t.Helper()
		if _, err := s.SyncCycleStep(ctx); err != nil {
			t.Fatal(err)
		}
	}
	const oldIndex = "CREATE UNIQUE INDEX IF NOT EXISTS sync_cycle_active_owner ON sync_cycles(owner_id) WHERE stage<>'complete';"
	exec("DROP INDEX sync_cycle_active_owner;" + oldIndex)
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("legacy cycle did not start: %v %v", started, err)
	}
	old := syncStatus(t, s)
	versions := syncFinishSearch(t, s, old, *now, Profile{"userId": 7, "age": 30})
	for range 3 {
		step()
	}
	if syncStatus(t, s).Stage != "publishing" {
		t.Fatal("fixture did not reach publication")
	}
	version := versions[0]
	exec("INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids) VALUES(?,1,7,'test','sent',23,'[42]')", version)
	if err := s.Store.InitCompatibility(ctx); err != nil {
		t.Fatal(err)
	}
	exec("INSERT INTO compatibility_pairs(id,owner_id,from_user_id,target_user_id,created_at_ms,updated_at_ms) VALUES(1,1,101,7,?,?)", old.StartedAt, old.StartedAt)
	exec(`INSERT INTO compatibility_jobs(id,pair_id,generation,version_id,own_hash,target_hash,input_hash,filter_hash,own_profile,target_profile,state,created_at_ms,updated_at_ms)
 VALUES(1,1,?,?,'own','target','input','archive-adult-v1','{}','{}','unknown',?,?)`, old.Generation, version, old.StartedAt, old.StartedAt)
	exec("INSERT INTO compatibility_cycle_scans(cycle_id,owner_id,upstream_id,generation,own_hash,updated_at_ms) VALUES(?,1,101,?,'own',?)", old.ID, old.Generation, old.StartedAt)
	exec("INSERT INTO compatibility_cycle_targets(cycle_id,profile_id,job_id,state,updated_at_ms) VALUES(?,7,1,'unknown',?)", old.ID, old.StartedAt)

	reconnect := func(upstream int64, generation string) {
		t.Helper()
		if err := s.Store.SaveSession(ctx, 1, Session{Token: "test", Refresh: "refresh", UserID: upstream, Generation: generation}); err != nil {
			t.Fatal(err)
		}
	}
	assertBlocked := func() {
		t.Helper()
		c := syncStatus(t, s)
		if c.ID != old.ID || c.Stage != "blocked" || c.Reason != syncCycleBindingChanged {
			t.Fatalf("old publication did not stay blocked: %+v", c)
		}
	}
	*now = time.UnixMilli(old.NextCycle - 1)
	reconnect(old.UserID, "new-generation")
	step()
	step()
	assertBlocked()

	*now = time.UnixMilli(old.NextCycle)
	reconnect(old.UserID, old.Generation)
	step()
	assertBlocked()
	reconnect(202, "new-generation")
	step()
	assertBlocked()
	reconnect(old.UserID, "new-generation")
	s.Store.Pause(ctx, 1, true)
	step()
	assertBlocked()
	s.Store.Pause(ctx, 1, false)
	if err := s.Store.SetMeta(ctx, dailyCrawlBlockKey(1), "1"); err != nil {
		t.Fatal(err)
	}
	step()
	assertBlocked()
	if err := s.Store.SetMeta(ctx, dailyCrawlBlockKey(1), "0"); err != nil {
		t.Fatal(err)
	}

	// A failed new cycle insert must leave the old cycle blocked and retryable.
	exec("CREATE TRIGGER reject_replacement_cycle BEFORE INSERT ON sync_cycles BEGIN SELECT RAISE(ABORT,'fixture write failure'); END")
	if _, err := s.SyncCycleStep(ctx); err == nil {
		t.Fatal("fixture did not abort the replacement")
	}
	assertBlocked()
	exec("DROP TRIGGER reject_replacement_cycle")
	step()
	fresh := syncStatus(t, s)
	job, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || job == nil || job.State != "queued" || job.Page != 1 || job.RunID == old.SearchRunID || job.RunID != fresh.SearchRunID || job.Generation != "new-generation" {
		t.Fatalf("replacement did not start a fresh crawl: %+v %v", job, err)
	}
	if fresh.ID == old.ID || fresh.Stage != "search" || fresh.StartedAt != old.NextCycle ||
		fresh.Generation != "new-generation" || fresh.UserID != old.UserID || fresh.NextCycle != fresh.StartedAt+(24*time.Hour).Milliseconds() {
		t.Fatalf("unexpected replacement: %+v", fresh)
	}
	if err := s.Store.requireSyncPublication(ctx, 1); !errors.Is(err, errSyncPublicationDeferred) {
		t.Fatalf("replacement search did not keep the publication gate closed: %v", err)
	}
	// The legacy schema's IF NOT EXISTS can still run without restoring the old index.
	exec(oldIndex)
	step()
	original, err := scanSyncCycle(s.Store.DB.QueryRow("SELECT "+syncCycleColumns+" FROM sync_cycles WHERE id=?", old.ID))
	if err != nil || original == nil || original.Stage != "superseded" || original.Reason != syncCycleBindingChanged ||
		original.Generation != old.Generation || original.SearchRunID != old.SearchRunID || original.NextCycle != old.NextCycle {
		t.Fatalf("old publication history lost: %+v %v", original, err)
	}
	for _, check := range []struct {
		query string
		arg   any
	}{
		{"SELECT COUNT(*) FROM sync_publication_manifests WHERE cycle_id=? AND generation='source-one'", old.ID},
		{"SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=? AND kind='version'", old.ID},
		{"SELECT COUNT(*) FROM sync_refresh_items WHERE cycle_id=? AND state='available'", old.ID},
		{"SELECT COUNT(*) FROM compatibility_cycle_targets WHERE cycle_id=? AND state='unknown' AND job_id=1", old.ID},
		{"SELECT COUNT(*) FROM compatibility_jobs WHERE id=? AND state='unknown' AND generation='source-one'", 1},
		{"SELECT COUNT(*) FROM archive_versions WHERE version_id=? AND state='sent' AND message_ids='[42]'", version},
	} {
		if count := syncPublicationCount(t, s.Store, check.query, check.arg); count != 1 {
			t.Fatalf("old receipt or checkpoint changed: %s (%d)", check.query, count)
		}
	}
}
