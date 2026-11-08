package companion

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func dailyCrawlFixture(t *testing.T) (*FeedCrawler, *time.Time) {
	t.Helper()
	c, now, _, calls := crawlFixture(t, func(int, int) ([]Profile, int) {
		t.Fatal("daily scheduling performed HTTP")
		return nil, 0
	})
	c.Service.Config.OwnerID = 1
	c.Service.Config.DailyCrawl = true
	t.Cleanup(func() {
		if *calls != 0 {
			t.Fatalf("daily scheduling made %d HTTP calls", *calls)
		}
	})
	return c, now
}

func dailySchedule(t *testing.T, c *FeedCrawler, want bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if started, err := c.ScheduleDaily(ctx); err != nil || started != want {
		t.Fatalf("daily scheduled=%v, want %v, err=%v", started, want, err)
	}
}

func dailyMarker(t *testing.T, c *FeedCrawler) dailyCrawlMarker {
	t.Helper()
	var raw string
	if err := c.Service.Store.DB.QueryRow("SELECT value FROM meta WHERE key=?", dailyCrawlKey(1)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var marker dailyCrawlMarker
	if err := json.Unmarshal([]byte(raw), &marker); err != nil {
		t.Fatal(err)
	}
	return marker
}

func dailyJobState(t *testing.T, c *FeedCrawler, state, reason string) {
	t.Helper()
	if _, err := c.Service.Store.DB.Exec("UPDATE feed_crawls SET state=?,reason=? WHERE owner_id=1", state, reason); err != nil {
		t.Fatal(err)
	}
}

func TestDailyCrawlStartsOnlyAfter24HoursFromRunStart(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	first := crawlJob(t, c)
	marker := dailyMarker(t, c)
	if !first.IncludeOthers || !first.IncludeLocations || marker.NextAt != now.Add(24*time.Hour).UnixMilli() {
		t.Fatalf("wrong broad job or next day: job=%+v marker=%+v", first, marker)
	}
	// Completion time does not postpone the next day.
	*now = now.Add(3 * time.Hour)
	dailyJobState(t, c, "done", "empty_page")
	*now = time.UnixMilli(marker.NextAt - 1)
	dailySchedule(t, c, false)
	if dailyMarker(t, c) != marker || crawlJob(t, c).RunID != first.RunID {
		t.Fatal("pre-due tick moved the baseline")
	}
	*now = now.Add(time.Millisecond)
	dailySchedule(t, c, true)
	second := crawlJob(t, c)
	if second.RunID == first.RunID || second.StartedAt != now.UnixMilli() || dailyMarker(t, c).NextAt != now.Add(24*time.Hour).UnixMilli() {
		t.Fatal("due tick did not create exactly one new baseline")
	}
	dailySchedule(t, c, false)
}

func TestDailyCrawlAdoptsExistingActiveRunWithoutReplacingCursor(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Service.Store.DB.Exec("UPDATE feed_crawls SET state='running',mode=1,page=17,pages=16,received=42 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	before := crawlJob(t, c)
	*now = now.Add(48 * time.Hour)
	dailySchedule(t, c, false)
	marker := dailyMarker(t, c)
	dailySchedule(t, c, false)
	if !reflect.DeepEqual(crawlJob(t, c), before) || dailyMarker(t, c) != marker || marker.NextAt != before.StartedAt+(24*time.Hour).Milliseconds() {
		t.Fatal("daily adoption changed an active cursor or its next day")
	}
}

func TestDailyCrawlRestartUsesPersistedBaselineOnce(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	first := crawlJob(t, c).RunID
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(4 * 24 * time.Hour)
	store, err := OpenStore(c.Service.Store.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	// No client exists on this process replacement: scheduling must be DB-only.
	restarted := NewFeedCrawler(&Service{Config: c.Service.Config, Store: store})
	restarted.now = func() time.Time { return *now }
	dailySchedule(t, restarted, true)
	second := crawlJob(t, restarted).RunID
	dailySchedule(t, restarted, false)
	if second == first || crawlJob(t, restarted).RunID != second {
		t.Fatal("restart duplicated or skipped the single due run")
	}
}

func TestDailyCrawlAdoptsOldSuccessfulCompletion(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	if _, err := c.Request(context.Background(), 1, true, true); err != nil {
		t.Fatal(err)
	}
	old := crawlJob(t, c).RunID
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(24 * time.Hour)
	dailySchedule(t, c, true)
	if crawlJob(t, c).RunID == old {
		t.Fatal("old completed run was not adopted as the due baseline")
	}
}

func TestDailyCrawlNeverResurrectsTerminalRuns(t *testing.T) {
	for _, terminal := range []struct{ state, reason string }{
		{"stopped", "paused"}, {"stopped", "http_429"}, {"stopped", "session_changed"},
		{"stopped", "checkpoint_failed"}, {"error", "request_failed"}, {"done", "unknown"},
	} {
		t.Run(terminal.reason, func(t *testing.T) {
			c, now := dailyCrawlFixture(t)
			dailySchedule(t, c, true)
			dailyJobState(t, c, terminal.state, terminal.reason)
			old := crawlJob(t, c)
			*now = now.Add(48 * time.Hour)
			dailySchedule(t, c, false)
			if !reflect.DeepEqual(old, crawlJob(t, c)) {
				t.Fatal("daily scheduler replaced a terminal run")
			}
			// Only a fresh explicit request becomes a new scheduling baseline.
			if _, err := c.Request(context.Background(), 1, true, true); err != nil {
				t.Fatal(err)
			}
			dailySchedule(t, c, false)
			if dailyMarker(t, c).RunID == old.RunID {
				t.Fatal("explicit request did not establish a fresh baseline")
			}
		})
	}
}

func TestDailyCrawlPauseAndReconnectRequireExplicitNewRun(t *testing.T) {
	for _, restriction := range []string{"pause", "disconnected", "reconnect_required", "new generation", "new upstream"} {
		t.Run(restriction, func(t *testing.T) {
			ctx := context.Background()
			c, now := dailyCrawlFixture(t)
			dailySchedule(t, c, true)
			dailyJobState(t, c, "done", "empty_page")
			original, err := c.Service.Store.Session(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			switch restriction {
			case "pause":
				c.Service.Store.Pause(ctx, 1, true)
			case "disconnected":
				if err := c.Service.Store.Disconnect(ctx, 1); err != nil {
					t.Fatal(err)
				}
			case "reconnect_required":
				c.Service.Store.SessionState(ctx, 1, "reconnect_required", "test")
			default:
				changed := original
				if restriction == "new generation" {
					changed.Generation = "reconnected"
				} else {
					changed.UserID = 999
				}
				if err := c.Service.Store.SaveSession(ctx, 1, changed); err != nil {
					t.Fatal(err)
				}
			}
			*now = now.Add(48 * time.Hour)
			dailySchedule(t, c, false)
			if !dailyMarker(t, c).Blocked {
				t.Fatal("restriction did not persistently block the old baseline")
			}
			c.Service.Store.Pause(ctx, 1, false)
			if err := c.Service.Store.SaveSession(ctx, 1, original); err != nil {
				t.Fatal(err)
			}
			dailySchedule(t, c, false)
			if _, err := c.Request(ctx, 1, true, true); err != nil {
				t.Fatal(err)
			}
			dailySchedule(t, c, false)
			if dailyMarker(t, c).Blocked {
				t.Fatal("explicit new request did not clear the old block")
			}
		})
	}
}

func TestDailyCrawlOwnerIsolationAndDisabledDefault(t *testing.T) {
	ctx := context.Background()
	c, _ := dailyCrawlFixture(t)
	c.Service.Config.DailyCrawl = false
	dailySchedule(t, c, false)
	if exists, err := c.Service.Store.hasCrawlTable(ctx); err != nil || exists {
		t.Fatalf("disabled scheduler initialized a crawl: exists=%v err=%v", exists, err)
	}
	c.Service.Config.DailyCrawl = true
	if _, err := c.Request(ctx, 2, true, true); err != nil {
		t.Fatal(err)
	}
	other, err := c.Service.Store.CrawlStatus(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	c.Service.Store.SessionState(ctx, 1, "reconnect_required", "test")
	dailySchedule(t, c, false)
	if own, err := c.Service.Store.CrawlStatus(ctx, 1); err != nil || own != nil {
		t.Fatal("selected another connected tenant when owner was disconnected")
	}
	c.Service.Store.SessionState(ctx, 1, "connected", "")
	dailySchedule(t, c, true)
	if after, err := c.Service.Store.CrawlStatus(ctx, 2); err != nil || !reflect.DeepEqual(other, after) {
		t.Fatal("daily owner schedule changed another tenant's run")
	}
	for _, owner := range []int64{1, 2} {
		state, err := c.Service.Store.filterState(ctx, owner)
		if err != nil || state.Applied != nil || state.Draft != nil || state.Others {
			t.Fatal("daily schedule changed owner UI filters")
		}
	}
}

func TestDailyCrawlConcurrentTicksStartOnce(t *testing.T) {
	c, _ := dailyCrawlFixture(t)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started, err := c.ScheduleDaily(context.Background())
			results <- started
			errors <- err
		}()
	}
	wg.Wait()
	count := 0
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		if <-results {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("concurrent ticks started %d runs", count)
	}
}

func TestDailyCrawlDoesNotRecreateMissingOrUnadoptedStoppedRun(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unadopted stopped", true: "missing tracked job"}[missing], func(t *testing.T) {
			c, now := dailyCrawlFixture(t)
			if _, err := c.Request(context.Background(), 1, true, true); err != nil {
				t.Fatal(err)
			}
			if missing {
				dailySchedule(t, c, false)
				if _, err := c.Service.Store.DB.Exec("DELETE FROM feed_crawls WHERE owner_id=1"); err != nil {
					t.Fatal(err)
				}
			} else {
				dailyJobState(t, c, "stopped", "manual")
			}
			*now = now.Add(48 * time.Hour)
			dailySchedule(t, c, false)
			j, err := c.Service.Store.CrawlStatus(context.Background(), 1)
			if err != nil || (missing && j != nil) || (!missing && (j == nil || j.State != "stopped")) {
				t.Fatalf("scheduler recreated or replaced a stopped/missing job: %+v err=%v", j, err)
			}
		})
	}
}

func dailyBeforeEnqueue(t *testing.T, c *FeedCrawler, statement string) {
	t.Helper()
	if err := c.init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The third migration occurs inside requestLocked, after scheduler eligibility.
	_, err := c.Service.Store.DB.Exec(`INSERT INTO meta(key,value) VALUES('daily_test_migrations','0');
 CREATE TRIGGER count_daily_test_migrations BEFORE UPDATE ON meta
 WHEN NEW.key='feed_crawl_next' AND NEW.value=OLD.value BEGIN
 UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='daily_test_migrations'; END;
 CREATE TRIGGER change_before_daily_enqueue BEFORE UPDATE ON meta
 WHEN NEW.key='daily_test_migrations' AND NEW.value='3' BEGIN ` + statement + `; END;`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDailyCrawlPauseAfterEligibilityIsBenign(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	dailyJobState(t, c, "done", "empty_page")
	old := crawlJob(t, c)
	*now = now.Add(24 * time.Hour)
	dailyBeforeEnqueue(t, c, "UPDATE users SET paused=1 WHERE id=1")
	dailySchedule(t, c, false)
	if !dailyMarker(t, c).Blocked || crawlJob(t, c).RunID != old.RunID {
		t.Fatal("late pause did not retain and block the previous run")
	}
}

func TestDailyCrawlActiveRunAfterEligibilityIsBenign(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(24 * time.Hour)
	dailyBeforeEnqueue(t, c, "UPDATE feed_crawls SET run_id='cli-run',state='queued',reason='',started_at=started_at+86400000 WHERE owner_id=1")
	dailySchedule(t, c, false)
	if job := crawlJob(t, c); job.RunID != "cli-run" || !job.active() {
		t.Fatalf("scheduler replaced the new CLI run: %+v", job)
	}
	if dailyMarker(t, c).Blocked {
		t.Fatal("obsolete marker update blocked a different active run")
	}
	dailySchedule(t, c, false)
	if marker := dailyMarker(t, c); marker.RunID != "cli-run" || marker.Blocked {
		t.Fatal("next tick failed to adopt the CLI run")
	}
}

func TestDailyCrawlSessionLostAfterEligibilityIsBenign(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(24 * time.Hour)
	dailyBeforeEnqueue(t, c, "DELETE FROM sessions WHERE owner_id=1")
	dailySchedule(t, c, false)
	if !dailyMarker(t, c).Blocked {
		t.Fatal("late session removal did not block the previous baseline")
	}
}

func TestDailyCrawlActualFailuresAfterEligibilityRemainFatal(t *testing.T) {
	for _, statement := range []string{
		"SELECT RAISE(ABORT,'fixture scheduler database failure')",
		"UPDATE sessions SET encrypted=X'00' WHERE owner_id=1",
	} {
		t.Run(statement, func(t *testing.T) {
			c, now := dailyCrawlFixture(t)
			dailySchedule(t, c, true)
			dailyJobState(t, c, "done", "empty_page")
			*now = now.Add(24 * time.Hour)
			dailyBeforeEnqueue(t, c, statement)
			started, err := c.ScheduleDaily(context.Background())
			var lifecycle *crawlLifecycleError
			if err == nil || started || errors.As(err, &lifecycle) {
				t.Fatalf("real storage failure classified as benign: started=%v err=%v", started, err)
			}
		})
	}
}

func TestCrawlRequestLifecycleErrorsPreserveHumanMessages(t *testing.T) {
	for _, tc := range []struct{ state, want string }{
		{"paused", "аккаунт на паузе; сначала /resume, затем новый запрос обхода"},
		{"active", "обход уже выполняется; смотри crawl-status"},
		{"session_unavailable", "для обхода нужна подключённая сессия Jardulik"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			c, _ := dailyCrawlFixture(t)
			switch tc.state {
			case "paused":
				c.Service.Store.Pause(context.Background(), 1, true)
			case "active":
				if _, err := c.Request(context.Background(), 1, true); err != nil {
					t.Fatal(err)
				}
			default:
				if err := c.Service.Store.Disconnect(context.Background(), 1); err != nil {
					t.Fatal(err)
				}
			}
			_, err := c.Request(context.Background(), 1, true)
			var lifecycle *crawlLifecycleError
			if !errors.As(err, &lifecycle) || lifecycle.state != tc.state || err.Error() != tc.want {
				t.Fatalf("lifecycle type or message changed: %v", err)
			}
		})
	}
}

func TestDailyCrawlPauseResumeAfterEligibilityRetainsBarrier(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	dailySchedule(t, c, true)
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(24 * time.Hour)
	dailyBeforeEnqueue(t, c, `UPDATE users SET paused=1 WHERE id=1;
 INSERT OR REPLACE INTO meta(key,value) VALUES('daily_crawl_block:1','1');
 UPDATE users SET paused=0 WHERE id=1`)
	dailySchedule(t, c, false)
	var blocked bool
	if err := c.Service.Store.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1')", dailyCrawlBlockKey(1)).Scan(&blocked); err != nil || !blocked {
		t.Fatalf("daily launch cleared a late pause barrier: blocked=%v err=%v", blocked, err)
	}
	if !dailyMarker(t, c).Blocked {
		t.Fatal("late pause/resume did not block the previous baseline")
	}
	if _, err := c.Request(context.Background(), 1, true, true); err != nil {
		t.Fatal(err)
	}
	dailySchedule(t, c, false)
	if dailyMarker(t, c).Blocked {
		t.Fatal("explicit request failed to rearm daily scheduling")
	}
}

func TestDailyCrawlConnectedIdentityChangedAfterEligibilityDoesNotRearm(t *testing.T) {
	for _, change := range []string{"generation", "upstream"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			c, now := dailyCrawlFixture(t)
			dailySchedule(t, c, true)
			dailyJobState(t, c, "done", "empty_page")
			old := crawlJob(t, c)
			*now = now.Add(24 * time.Hour)
			changed, err := c.Service.Store.Session(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if change == "generation" {
				changed.Generation = "reconnected-generation"
			} else {
				changed.UserID = 999
			}
			raw, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			encrypted, err := seal(c.Service.Store.cfg.SessionKey, raw, sessionPurpose(1))
			if err != nil {
				t.Fatal(err)
			}
			statement := "UPDATE sessions SET encrypted=X'" + hex.EncodeToString(encrypted) + "' WHERE owner_id=1"
			if change == "upstream" {
				statement += "; UPDATE sessions SET upstream_id=999 WHERE owner_id=1"
			}
			dailyBeforeEnqueue(t, c, statement)
			dailySchedule(t, c, false)
			if crawlJob(t, c).RunID != old.RunID || !dailyMarker(t, c).Blocked {
				t.Fatal("late connected identity change automatically rearmed scheduling")
			}
			current, err := c.Service.Store.Session(ctx, 1)
			if err != nil || current.State != "connected" || current.Generation != changed.Generation || current.UserID != changed.UserID {
				t.Fatalf("identity-change fixture did not execute: %+v err=%v", current, err)
			}
		})
	}
}
