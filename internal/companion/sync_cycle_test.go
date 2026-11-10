package companion

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func syncFixture(t *testing.T) (*Service, *time.Time) {
	t.Helper()
	cfg := testConfig(t)
	cfg.SyncCycleEnabled = true
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.Client.Now = func() time.Time { return now }
	s.Crawler.now = func() time.Time { return now }
	s.Watcher.now = func() time.Time { return now }
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected source HTTP %s %s", r.Method, r.URL.Path)
	})}
	ctx := context.Background()
	if err = s.Store.User(ctx, 1, "sync test"); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SaveSession(ctx, 1, Session{Token: "test", Refresh: "refresh", UserID: 101, Generation: "source-one", Expires: now.Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.InitSyncCycles(ctx); err != nil {
		t.Fatal(err)
	}
	return s, &now
}

func syncStatus(t *testing.T, s *Service) SyncCycle {
	t.Helper()
	c, err := s.Store.SyncCycle(context.Background(), 1)
	if err != nil || c == nil {
		t.Fatalf("cycle=%v error=%v", c, err)
	}
	return *c
}

func TestSyncCycleAdoptsBroadRunAndRestartPreservesCursor(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	j, err := s.Crawler.Request(ctx, 1, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE feed_crawls SET mode=3,page=69,hydrated=68 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	before, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	started, err := s.startSyncCycle(ctx, 1, *now)
	if err != nil || !started {
		t.Fatalf("start=%v err=%v", started, err)
	}
	c := syncStatus(t, s)
	if c.SearchRunID != j.RunID || c.StartedAt != j.StartedAt || c.Stage != "search" {
		t.Fatalf("bad adoption %+v", c)
	}
	restarted, err := NewService(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.Client.Now = func() time.Time { return *now }
	restarted.Crawler.now = func() time.Time { return *now }
	if started, err = restarted.startSyncCycle(ctx, 1, now.Add(48*time.Hour)); err != nil || started {
		t.Fatalf("restart duplicated cycle: %v %v", started, err)
	}
	after, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("cursor changed: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestSyncCycleHardPauseAndSourceSwitchNeverRestart(t *testing.T) {
	for _, kind := range []string{"hard_pause", "user_pause", "generation", "upstream"} {
		t.Run(kind, func(t *testing.T) {
			s, now := syncFixture(t)
			ctx := context.Background()
			if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
				t.Fatal(err)
			}
			before := syncStatus(t, s)
			switch kind {
			case "hard_pause":
				if err := s.Store.SetMeta(ctx, dailyCrawlBlockKey(1), "1"); err != nil {
					t.Fatal(err)
				}
			case "user_pause":
				s.Store.Pause(ctx, 1, true)
			default:
				ss, err := s.Store.Session(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "generation" {
					ss.Generation = "new-generation"
				} else {
					ss.UserID = 202
				}
				if err = s.Store.SaveSession(ctx, 1, ss); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.SyncCycleStep(ctx); err != nil {
				t.Fatal(err)
			}
			c := syncStatus(t, s)
			if c.ID != before.ID || c.Stage != "blocked" || c.SearchRunID != before.SearchRunID {
				t.Fatalf("unsafe lifecycle %+v", c)
			}
			if started, err := s.startSyncCycle(ctx, 1, now.Add(72*time.Hour)); err != nil || started {
				t.Fatalf("blocked restarted %v %v", started, err)
			}
			if kind == "hard_pause" {
				v := s.Store.Meta(ctx, dailyCrawlBlockKey(1))
				if v != "1" {
					t.Fatal("hard pause cleared")
				}
			}
		})
	}
}

func syncFinishSearch(t *testing.T, s *Service, c SyncCycle, now time.Time, profiles ...Profile) []int64 {
	t.Helper()
	ctx := context.Background()
	ids, err := s.Store.ArchiveViewed(ctx, 1, profiles, "GET /user/getUser (crawl catalog)")
	if err != nil {
		t.Fatal(err)
	}
	raw := `[{"userId":1,"totalCount":1}]`
	result, err := s.Store.DB.Exec(`INSERT INTO location_batches(owner_id,run_id,upstream_id,generation,country,city,source_page,page_size,total_count,raw_json,source_sha,captured_at) VALUES(?,?,101,'source-one','','',1,20,?,?,'test',?)`, 1, c.SearchRunID, len(profiles), raw, now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range profiles {
		if _, err = s.Store.DB.Exec("INSERT INTO crawl_location_results(owner_id,run_id,profile_id,first_batch_id,state,version_id,completed_at) VALUES(?,?,?,?,?,?,?)", 1, c.SearchRunID, p.ID(), batch, "hydrated", ids[i], now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Store.DB.Exec("UPDATE feed_crawls SET state='done',reason='empty_page',mode=3 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE crawl_presence_runs SET finished_at=? WHERE owner_id=1 AND run_id=?", now.Format(time.RFC3339Nano), c.SearchRunID); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestSyncCycleFiniteRefreshReusesAcceptedFullHydrations(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids := syncFinishSearch(t, s, c, *now, Profile{"userId": 7, "age": 30}, Profile{"userId": 8, "age": 40})
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if syncStatus(t, s).Stage != "refresh" {
		t.Fatal("did not enter refresh")
	}
	// A discovery arriving after snapshot is not allowed to extend the finite sweep.
	if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 9, "age": 20}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	// A repeated seed after a committed stage transition must not enlarge membership.
	if err := s.Store.seedSyncRefresh(ctx, c, *now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if syncStatus(t, s).Stage != "draining" {
		t.Fatal("repeated full GETs or extended cohort")
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM sync_refresh_items WHERE cycle_id=? AND state='available'", c.ID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("cohort count %d %v", count, err)
	}
	var version int64
	if err := s.Store.DB.QueryRow("SELECT version_id FROM sync_refresh_items WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&version); err != nil || version != ids[0] {
		t.Fatal("hydration version not pinned")
	}
}

func TestSyncCycleSearchCompletionMustBeProven(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	if _, err := s.Store.DB.Exec("UPDATE feed_crawls SET state='done',reason='empty_page',mode=3 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.seedSyncRefresh(ctx, c, *now); err == nil {
		t.Fatal("unproven finished presence ledger accepted")
	}
	if syncStatus(t, s).Stage != "search" {
		t.Fatal("advanced partial source branch")
	}
}

func TestSyncCycleFreshReceiptBindingAndRefreshBudget(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}, {"userId": 8, "age": 30}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []int64{7, 8} {
		if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, id, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	c.Stage = "refresh"
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.RecordSyncFullCapture(ctx, 1, ss, 7, ids[0], now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wrong := ss
	wrong.Generation = "foreign"
	if err = s.Store.RecordSyncFullCapture(ctx, 1, wrong, 8, ids[1], now.Add(time.Second)); err == nil {
		t.Fatal("foreign source receipt accepted")
	}
	if err = s.Store.reuseSyncRefresh(ctx, c); err != nil {
		t.Fatal(err)
	}
	target, err := s.claimSyncRefresh(ctx, c, *now)
	if err != nil || target != 8 {
		t.Fatalf("target=%d err=%v", target, err)
	}
	if target, err = s.claimSyncRefresh(ctx, c, *now); err != nil || target != 0 {
		t.Fatalf("gap bypass target=%d err=%v", target, err)
	}
	if _, err = s.Store.DB.Exec("UPDATE profile_watch_budget SET used=?,next_request=0", s.Watcher.Config.WatchDailyLimit); err != nil {
		t.Fatal(err)
	}
	if target, err = s.claimSyncRefresh(ctx, c, now.Add(time.Minute)); err != nil || target != 0 {
		t.Fatalf("cap bypass target=%d err=%v", target, err)
	}
}

func TestSyncCycleRefreshConfirmedMissingDoesNotFetchOrDeleteArchive(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, 7, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	c.Stage = "refresh"
	if _, err = s.Store.recordProfilePresence(ctx, 1, 7, false, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// An old unavailable state alone must not be mistaken for a fresh bound GET.
	calls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(http.StatusNoContent) })}
	ss, _ := s.Store.Session(ctx, 1)
	if _, err = s.syncRefreshOne(ctx, c, ss, *now); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("confirmed missing GET calls=%d", calls)
	}
	var state string
	if err = s.Store.DB.QueryRow("SELECT state FROM sync_refresh_items WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&state); err != nil || state != "unavailable" {
		t.Fatalf("state %s error %v", state, err)
	}
	if _, err = s.Store.Version(ctx, 1, ids[0]); err != nil {
		t.Fatal("archive lost", err)
	}
}

func TestSyncCycleRefreshMakesOnlyOneBoundedGETAndCheckpoints(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, 7, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != "/user/getUser" || r.URL.Query().Get("viewerUserId") != "101" || r.URL.Query().Get("targetUserId") != "7" {
			t.Error("wrong refresh request")
		}
		_ = json.NewEncoder(w).Encode(Profile{"userId": 7, "age": 30, "name": "updated"})
	})}
	if _, err = s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || syncStatus(t, s).Stage != "draining" {
		t.Fatalf("calls=%d cycle=%+v", calls, syncStatus(t, s))
	}
}

func TestSyncCyclePublicationGateFailsClosedWithoutCycle(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if allowed, err := s.Store.SyncPublicationAllowed(ctx, 1); err != nil || allowed {
		t.Fatalf("startup barrier=%v %v", allowed, err)
	}
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	for _, stage := range []string{"search", "refresh", "draining", "blocked", "publishing", "complete"} {
		if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage=? WHERE id=?", stage, c.ID); err != nil {
			t.Fatal(err)
		}
		allowed, err := s.Store.SyncPublicationAllowed(ctx, 1)
		if err != nil || allowed != (stage == "publishing" || stage == "complete") {
			t.Fatalf("stage %s allowed %v %v", stage, allowed, err)
		}
	}
	if _, err := s.startSyncCycle(ctx, 1, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if syncStatus(t, s).ID != c.ID {
		t.Fatal("cycle overlapped before daily due")
	}
}

func TestSyncCycleNewerMissingReadInvalidatesPositiveReuse(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	ss, _ := s.Store.Session(ctx, 1)
	if err = s.Store.RecordSyncFullCapture(ctx, 1, ss, 7, ids[0], now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.recordProfilePresence(ctx, 1, 7, false, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, 7, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.reuseSyncRefresh(ctx, c); err != nil {
		t.Fatal(err)
	}
	var state string
	if err = s.Store.DB.QueryRow("SELECT state FROM sync_refresh_items WHERE cycle_id=?", c.ID).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("positive receipt hid newer missing state %s %v", state, err)
	}
}

func TestSyncCycleFailedTargetDoesNotStarveOthers(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30}, {"userId": 8, "age": 30}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []int64{7, 8} {
		if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, id, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	var calls []string
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("targetUserId")
		calls = append(calls, id)
		if id == "7" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_ = json.NewEncoder(w).Encode(Profile{"userId": 8, "age": 30})
	})}
	if _, err = s.SyncCycleStep(ctx); err == nil {
		t.Fatal("source500 hidden")
	}
	*now = now.Add(s.Watcher.Config.WatchGap)
	if _, err = s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"7", "8"}) {
		t.Fatalf("broken target monopolized slot %v", calls)
	}
	var state string
	if err = s.Store.DB.QueryRow("SELECT state FROM sync_refresh_items WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("source error became missing %s %v", state, err)
	}
}

func TestSyncCycleSourceSwitchDuringGETDiscardsOldResponse(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if _, err := s.startSyncCycle(ctx, 1, *now); err != nil {
		t.Fatal(err)
	}
	c := syncStatus(t, s)
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "age": 30, "name": "before"}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,version_id) VALUES(?,?,?)", c.ID, 7, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ss, e := s.Store.Session(ctx, 1)
		if e != nil {
			t.Fatal(e)
		}
		ss.Generation = "new-generation"
		if e = s.Store.SaveSession(ctx, 1, ss); e != nil {
			t.Fatal(e)
		}
		_ = json.NewEncoder(w).Encode(Profile{"userId": 7, "age": 30, "name": "stale response"})
	})}
	if _, err = s.SyncCycleStep(ctx); err == nil {
		t.Fatal("source switch accepted")
	}
	p, err := s.Store.Profile(ctx, 1, 7)
	if err != nil || p.Profile.String("name") != "before" {
		t.Fatalf("old reply archived %+v %v", p, err)
	}
	if _, err = s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if syncStatus(t, s).Stage != "blocked" {
		t.Fatal("source switch did not block old cycle")
	}
}

func TestSyncCycleIncludesAllConnectedUnpausedOwners(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	for _, owner := range []int64{2, 3} {
		if err := s.Store.User(ctx, owner, "sync test"); err != nil {
			t.Fatal(err)
		}
		if err := s.Store.SaveSession(ctx, owner, Session{Token: "test", Refresh: "refresh", UserID: owner + 100, Generation: "source", Expires: now.Add(time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	s.Store.Pause(ctx, 3, true)
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []int64{1, 2} {
		c, err := s.Store.SyncCycle(ctx, owner)
		if err != nil || c == nil || c.UserID != owner+100 {
			t.Fatalf("owner %d cycle %+v %v", owner, c, err)
		}
	}
	if c, err := s.Store.SyncCycle(ctx, 3); err != nil || c != nil {
		t.Fatalf("paused owner enrolled %+v %v", c, err)
	}
}

func TestSyncCycleStartWaitsForCrossProcessPublicationLock(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	unlock, acquired, err := s.Store.trySyncPublicationLock()
	if err != nil || !acquired {
		t.Fatalf("lock %v %v", acquired, err)
	}
	held := true
	defer func() {
		if held {
			unlock()
		}
	}()
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || started {
		t.Fatalf("cycle crossed in-flight send %v %v", started, err)
	}
	unlock()
	held = false
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("cycle failed after confirmed send %v %v", started, err)
	}
}

func TestSyncCycleInitialCompletedAdoptionOnlyWhileFresh(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "stale"}[stale], func(t *testing.T) {
			s, now := syncFixture(t)
			ctx := context.Background()
			j, err := s.Crawler.Request(ctx, 1, true, true)
			if err != nil {
				t.Fatal(err)
			}
			syncFinishSearch(t, s, SyncCycle{SearchRunID: j.RunID}, *now, Profile{"userId": 7, "age": 30})
			if stale {
				*now = now.Add(25 * time.Hour)
			}
			if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
				t.Fatalf("start %v %v", started, err)
			}
			c := syncStatus(t, s)
			if (c.SearchRunID == j.RunID) == stale {
				t.Fatalf("completed adoption stale=%v old=%s current=%s", stale, j.RunID, c.SearchRunID)
			}
			if !stale {
				if _, err = s.SyncCycleStep(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err = s.SyncCycleStep(ctx); err != nil {
					t.Fatal(err)
				}
				if syncStatus(t, s).Stage != "draining" {
					t.Fatal("fresh completed hydration repeated")
				}
			}
		})
	}
}
