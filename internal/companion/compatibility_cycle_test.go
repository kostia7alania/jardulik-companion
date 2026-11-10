package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func compatibilityCycle(t *testing.T, s *Service, owner int64, now time.Time) SyncCycle {
	t.Helper()
	s.Config.CompatibilityEnabled = true
	s.Config.SyncCycleEnabled = true
	s.Store.cfg.SyncCycleEnabled = true
	if err := s.Store.InitSyncCycles(context.Background()); err != nil {
		t.Fatal(err)
	}
	ss, err := s.Store.Session(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	c := SyncCycle{ID: fmt.Sprintf("cycle-%d-%d", owner, now.UnixMilli()), OwnerID: owner, UserID: ss.UserID, Generation: ss.Generation, Stage: "search", StartedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(), NextCycle: now.Add(24 * time.Hour).UnixMilli()}
	_, err = s.Store.DB.Exec(`INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES(?,?,?,?,?,'',?,?,?)`, c.ID, c.OwnerID, c.UserID, c.Generation, c.Stage, c.StartedAt, c.UpdatedAt, c.NextCycle)
	if err != nil {
		t.Fatal(err)
	}
	s.Client.Now = func() time.Time { return now }
	return c
}

func compatibilityRefreshCohort(t *testing.T, s *Service, c SyncCycle) {
	t.Helper()
	if _, err := s.Store.DB.Exec(`INSERT INTO sync_refresh_items(cycle_id,profile_id,state,version_id) SELECT ?,profile_id,'available',version_id FROM queue WHERE owner_id=?`, c.ID, c.OwnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='draining' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
}

func TestSyncCompatibilityArchiveScopeKeepsPersonalFiltersAndOwnerIsolation(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	before, err := s.Autoliker.filters(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	compatibilityAdd(t, s, 1, 7, 34)
	compatibilityAdd(t, s, 1, 8, 111)
	compatibilityAdd(t, s, 1, 9, 17)
	compatibilityAdd(t, s, 2, 10, 30)
	// No height is needed for broad archive compatibility.
	if _, err = s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 11, "age": 45, "name": "No height"}}, "GET /user/getUser"); err != nil {
		t.Fatal(err)
	}
	c := compatibilityCycle(t, s, 1, now)
	if ready, err := s.PlanSyncCompatibility(ctx, c, false); err != nil || ready {
		t.Fatal(ready, err)
	}
	var count int
	if err = s.Store.DB.QueryRow(`SELECT COUNT(*) FROM compatibility_jobs`).Scan(&count); err != nil || count != 3 {
		t.Fatal("archive eligibility", count, err)
	}
	after, err := s.Autoliker.filters(ctx, 1)
	if err != nil || after != before {
		t.Fatal("personal filters changed", before, after, err)
	}
	j := compatibilityClaim(t, s, now)
	if j.OwnerID != 1 || j.FromUserID != 101 || j.TargetUserID != 7 {
		t.Fatal("wrong directed owner", j)
	}
	target, err := s.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.compatibilityEligible(ctx, *j, target.Profile); err != nil || !allowed {
		t.Fatal("broad scope rejected", allowed, err)
	}
	if allowed, err := s.compatibilityEligible(ctx, *j, Profile{"age": 17}); err != nil || allowed {
		t.Fatal("minor allowed", allowed, err)
	}
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='blocked' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.compatibilityEligible(ctx, *j, target.Profile); err != nil || allowed {
		t.Fatal("blocked scope remained active", allowed, err)
	}
	if err = s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, compatibilityRequest(*j), now, 10*time.Second); !errors.Is(err, ErrCompatibilityUnavailable) {
		t.Fatal("last SQL guard lost", err)
	}
	var sending int
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs WHERE state='sending'").Scan(&sending); err != nil || sending != 0 {
		t.Fatal(sending, err)
	}
}

func TestSyncCompatibilityFinalFrontierIsFiniteAndTerminal(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	for i := int64(1); i <= compatibilityPlanBatch+1; i++ {
		compatibilityAdd(t, s, 1, 1000+i, 34)
	}
	c := compatibilityCycle(t, s, 1, now)
	compatibilityRefreshCohort(t, s, c)
	compatibilityAdd(t, s, 1, 8000, 40)
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	var outside int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_pairs WHERE target_user_id=8000").Scan(&outside); err != nil || outside != 0 {
		t.Fatal("late foreground spent outside refresh cohort", outside, err)
	}
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='draining' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.PlanSyncCompatibility(ctx, c, true); err != nil || ready {
		t.Fatal(ready, err)
	}
	compatibilityAdd(t, s, 1, 9000, 40)
	tick := now
	for i := 0; i < 5; i++ {
		tick = tick.Add(2 * time.Second)
		s.Client.Now = func() time.Time { return tick }
		if _, err := s.PlanSyncCompatibility(ctx, c, true); err != nil {
			t.Fatal(err)
		}
	}
	var members int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_cycle_targets WHERE cycle_id=? AND final_member=1", c.ID).Scan(&members); err != nil || members != compatibilityPlanBatch+1 {
		t.Fatal("frontier grew", members, err)
	}
	if _, err := s.Store.DB.Exec("UPDATE compatibility_jobs SET state='rejected',reason='test_terminal' WHERE state='queued'"); err != nil {
		t.Fatal(err)
	}
	ready := false
	for i := 0; i < 5; i++ {
		tick = tick.Add(2 * time.Second)
		s.Client.Now = func() time.Time { return tick }
		var err error
		ready, err = s.PlanSyncCompatibility(ctx, c, true)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("finite terminal frontier never completed")
	}
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='publishing' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if j, err := s.Store.ClaimCompatibilityJob(ctx, tick); err != nil || j != nil {
		t.Fatal("post-frontier dispatch", j, err)
	}
}

func TestSyncCompatibilityReusesSavedResultAndNeverReplaysUnknown(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 34)
	q := compatibilityAdd(t, s, 1, 8, 40)
	compatibilityAdd(t, s, 1, 9, 30)
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{UpstreamID: ss.UserID, ProfileID: p.ID, VersionID: p.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: ss.Generation, RequestBody: []byte(`{"fromUserId":101,"targetUserId":7}`), ResponseBody: []byte(`{"content":"Совместимость: 75%","model":"reported"}`), OwnProfile: ss.Account, TargetProfile: []byte(p.RawJSON)})
	if err != nil {
		t.Fatal(err)
	}
	compatibilityQueue(t, s, 1, q, now)
	if _, err = s.Store.DB.Exec("UPDATE compatibility_jobs SET state='unknown',http_status=502,reason='transport_unconfirmed'"); err != nil {
		t.Fatal(err)
	}
	c := compatibilityCycle(t, s, 1, now)
	if _, err = s.Store.recordProfilePresence(ctx, 1, 9, false, now); err != nil {
		t.Fatal(err)
	}
	compatibilityRefreshCohort(t, s, c)
	if ready, err := s.PlanSyncCompatibility(ctx, c, true); err != nil || !ready {
		t.Fatal("cached/unknown/unavailable must be terminal", ready, err)
	}
	rows, err := s.Store.DB.Query("SELECT profile_id,state FROM compatibility_cycle_targets WHERE cycle_id=? ORDER BY profile_id", c.ID)
	if err != nil {
		t.Fatal(err)
	}
	states := map[int64]string{}
	for rows.Next() {
		var id int64
		var state string
		if err = rows.Scan(&id, &state); err != nil {
			t.Fatal(err)
		}
		states[id] = state
	}
	rows.Close()
	if states[7] != "succeeded" || states[8] != "unknown" || states[9] != "unavailable" {
		t.Fatal(states)
	}
	var saved int64
	if err = s.Store.DB.QueryRow("SELECT latest_analysis_id FROM compatibility_pairs WHERE owner_id=1 AND target_user_id=7").Scan(&saved); err != nil || saved != a.ID {
		t.Fatal(saved, err)
	}
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(time.Minute)); err != nil || j != nil {
		t.Fatal("terminal replay", j, err)
	}
	var state string
	var status int
	if err = s.Store.DB.QueryRow("SELECT state,http_status FROM compatibility_jobs WHERE target_hash=?", compatibilityProfileHash(q.Profile)).Scan(&state, &status); err != nil || state != "unknown" || status != 502 {
		t.Fatal(state, status, err)
	}
}

func TestSyncCompatibilityPlanningPreservesPreflightBackoff(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityAdd(t, s, 1, 7, 34)
	c := compatibilityCycle(t, s, 1, now)
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	j := compatibilityClaim(t, s, now)
	if err := s.Store.ReleaseCompatibilityJob(ctx, *j, now, "preflight_unavailable"); err != nil {
		t.Fatal(err)
	}
	tick := now.Add(20 * time.Second)
	s.Client.Now = func() time.Time { return tick }
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	var next int64
	if err := s.Store.DB.QueryRow("SELECT next_at_ms FROM compatibility_jobs WHERE id=?", j.ID).Scan(&next); err != nil || next != now.Add(time.Minute).UnixMilli() {
		t.Fatal("planner erased backoff", next, err)
	}
	if got, err := s.Store.ClaimCompatibilityJob(ctx, tick); err != nil || got != nil {
		t.Fatal("early retry", got, err)
	}
}

func TestSyncCompatibilityExecutorBroadScopeAndFinalBinding(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(fmt.Sprint(block), func(t *testing.T) {
			f := newCompatibilityExecutorFixture(t)
			s := f.service
			ctx := context.Background()
			f.target["age"] = 40
			delete(f.target, "height")
			if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{f.target}, "GET /user/getUser"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec("DELETE FROM compatibility_pairs"); err != nil {
				t.Fatal(err)
			}
			c := compatibilityCycle(t, s, 1, f.now)
			s.Config.CompatibilityGap = 10 * time.Second
			if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
				t.Fatal(err)
			}
			if block {
				f.onTarget = func() {
					if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='blocked' WHERE id=?", c.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			ran, err := s.compatibilityStep(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if block {
				if ran || f.posts != 0 {
					t.Fatal("scope race posted", ran, f.posts)
				}
			} else {
				if !ran || f.posts != 1 || f.state(t) != "succeeded" {
					t.Fatal(ran, f.posts, f.state(t))
				}
				var full int
				if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM sync_full_reads WHERE owner_id=1 AND profile_id=7 AND generation=?", c.Generation).Scan(&full); err != nil || full != 1 {
					t.Fatal("full read evidence missing", full, err)
				}
			}
			filters, err := s.Autoliker.filters(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(filters)
			if filters.AgeMax != 25 {
				t.Fatal("like filters widened", string(raw))
			}
		})
	}
}

func TestSyncCompatibilityReturnedSameInputJoinsFinalFrontier(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			s, now := compatibilityFixture(t)
			ctx := context.Background()
			p := compatibilityAdd(t, s, 1, 7, 34)
			c := compatibilityCycle(t, s, 1, now)
			if cancelled {
				if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
					t.Fatal(err)
				}
				j := compatibilityClaim(t, s, now)
				if err := s.Store.FinishCompatibilityJob(ctx, *j, "cancelled", 0, nil, 0, "target_unavailable", now); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Store.recordProfilePresence(ctx, 1, 7, false, now); err != nil {
				t.Fatal(err)
			}
			tick := now.Add(20 * time.Second)
			s.Client.Now = func() time.Time { return tick }
			if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
				t.Fatal(err)
			}
			var before string
			if err := s.Store.DB.QueryRow("SELECT state FROM compatibility_cycle_targets WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&before); err != nil || before != "unavailable" {
				t.Fatal(before, err)
			}
			tick = tick.Add(time.Second)
			if _, err := s.Store.recordProfilePresence(ctx, 1, 7, true, tick); err != nil {
				t.Fatal(err)
			}
			ss, err := s.Store.Session(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Store.RecordSyncFullCapture(ctx, 1, ss, 7, p.VersionID, tick); err != nil {
				t.Fatal(err)
			}
			compatibilityRefreshCohort(t, s, c)
			if ready, err := s.PlanSyncCompatibility(ctx, c, true); err != nil || ready {
				t.Fatal("returned target skipped", ready, err)
			}
			var state string
			if err = s.Store.DB.QueryRow("SELECT state FROM compatibility_cycle_targets WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&state); err != nil || state != "pending" {
				t.Fatal(state, err)
			}
			if j, err := s.Store.ClaimCompatibilityJob(ctx, tick); err != nil || j == nil || j.TargetUserID != 7 {
				t.Fatal("fresh source evidence did not clear unsent cooldown", j, err)
			}
		})
	}
}

func TestSyncCompatibilityDisabledAndHardPauseCannotDispatch(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityAdd(t, s, 1, 7, 34)
	c := compatibilityCycle(t, s, 1, now)
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	s.Config.SyncCycleEnabled = false
	s.Store.cfg.SyncCycleEnabled = false
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now); err != nil || j != nil {
		t.Fatal("disabled cycle dispatched", j, err)
	}
	s.Config.SyncCycleEnabled = true
	s.Store.cfg.SyncCycleEnabled = true
	if _, err := s.Store.DB.Exec("INSERT OR REPLACE INTO meta(key,value) VALUES(?,'1')", dailyCrawlBlockKey(1)); err != nil {
		t.Fatal(err)
	}
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now); err != nil || j != nil {
		t.Fatal("hard pause dispatched", j, err)
	}
}
