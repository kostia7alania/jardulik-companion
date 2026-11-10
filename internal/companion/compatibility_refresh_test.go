package companion

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCompatibilityTextCacheReusesHistoricalAnalysisAcrossPhotoChanges(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	old := compatibilityQueue(t, s, 1, p, now)
	// Simulate an immutable job written by the previous hashing policy.
	if _, err := s.Store.DB.Exec("UPDATE compatibility_jobs SET own_hash='legacy-own-v1',target_hash='legacy-target-v1',input_hash='legacy-pair-v1',state='succeeded'"); err != nil {
		t.Fatal(err)
	}
	response := []byte(`{"content":"Совместимость 75%","model":"reported"}`)
	a, err := s.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{UpstreamID: old.FromUserID, ProfileID: p.ID, VersionID: p.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: old.Generation, OwnProfile: old.OwnProfile, TargetProfile: old.TargetProfile, RequestBody: compatibilityRequest(old), ResponseBody: response})
	if err != nil {
		t.Fatal(err)
	}
	own, _ := debugProfile(old.OwnProfile)
	own["photos"], own["miniPhotos"], own["type"], own["fire"] = "new-own-photo", "new-own-preview", 2, 1
	if err = s.Store.SaveAccount(ctx, 1, own); err != nil {
		t.Fatal(err)
	}
	p.Profile["photos"], p.Profile["miniPhotos"], p.Profile["distance"], p.Profile["type"] = "new-photo", "new-preview", 123, 1
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Store.Version(ctx, 1, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	compatibilityQueue(t, s, 1, p, now.Add(time.Minute))
	var id int64
	var state, reason string
	if err = s.Store.DB.QueryRow("SELECT COALESCE(analysis_id,0),state,reason FROM compatibility_jobs ORDER BY id DESC LIMIT 1").Scan(&id, &state, &reason); err != nil || id != a.ID || state != "succeeded" || reason != "cached_analysis" {
		t.Fatal(id, state, reason, err)
	}
	var savedOwn, savedTarget, savedResponse []byte
	if err = s.Store.DB.QueryRow("SELECT own_profile,target_profile,response_body FROM profile_analyses WHERE id=?", a.ID).Scan(&savedOwn, &savedTarget, &savedResponse); err != nil || !bytes.Equal(savedOwn, old.OwnProfile) || !bytes.Equal(savedTarget, old.TargetProfile) || !bytes.Equal(savedResponse, response) {
		t.Fatal("historical analysis changed", err)
	}
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(2*time.Minute)); err != nil || j != nil {
		t.Fatal("photo change requested paid work", j, err)
	}
	p.Profile["description"] = "meaningful new text"
	ids, err = s.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Store.Version(ctx, 1, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	compatibilityQueue(t, s, 1, p, now.Add(3*time.Minute))
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(3*time.Minute)); err != nil || j == nil {
		t.Fatal("text change failed to queue", j, err)
	}
}

func TestCompatibilityFinalGuardRejectsAvailabilityOrConcurrentOwnChange(t *testing.T) {
	for _, change := range []string{"unavailable", "own_text", "own_frozen"} {
		t.Run(change, func(t *testing.T) {
			s, now := compatibilityFixture(t)
			ctx := context.Background()
			compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 7, 23), now)
			j := compatibilityClaim(t, s, now)
			if change == "unavailable" {
				if _, err := s.Store.recordProfilePresence(ctx, 1, 7, false, now); err != nil {
					t.Fatal(err)
				}
			} else {
				own, _ := debugProfile(j.OwnProfile)
				if change == "own_frozen" {
					own["freezeState"] = true
				} else {
					own["description"] = "changed during target read"
				}
				if err := s.Store.SaveAccount(ctx, 1, own); err != nil {
					t.Fatal(err)
				}
			}
			err := s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, compatibilityRequest(*j), now, time.Minute)
			if !errors.Is(err, ErrCompatibilityUnavailable) && !errors.Is(err, ErrCompatibilityInputChanged) {
				t.Fatal("stale preflight accepted", err)
			}
			var state string
			var body []byte
			if err = s.Store.DB.QueryRow("SELECT state,request_body FROM compatibility_jobs WHERE id=?", j.ID).Scan(&state, &body); err != nil || state != "preparing" || len(body) != 0 {
				t.Fatal(state, string(body), err)
			}
		})
	}
}

func TestCompatibilityOwnReadTTLAndReconnectProof(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx := context.Background()
	s := f.service
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	j := compatibilityClaim(t, s, f.now)
	call := func(expected Session) {
		t.Helper()
		if _, _, err := s.compatibilityOwnProfile(ctx, *j, expected); err != nil {
			t.Fatal(err)
		}
	}
	call(ss)
	call(ss)
	if len(f.requests) != 1 {
		t.Fatal("getMe was repeated", f.requests)
	}
	// Counter-only local saves do not force a GET, but cannot renew its age.
	f.own["likes"] = 3
	if err = s.Store.SaveAccount(ctx, 1, f.own); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(4 * time.Minute)
	call(ss)
	if len(f.requests) != 1 {
		t.Fatal("local counters bypassed cache", f.requests)
	}
	f.now = f.now.Add(time.Minute)
	call(ss)
	if len(f.requests) != 2 {
		t.Fatal("local save renewed HTTP proof", f.requests)
	}
	ss, err = s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	ss.Generation = "reconnected-source"
	if err = s.Store.SaveSession(ctx, 1, ss); err != nil {
		t.Fatal(err)
	}
	call(ss)
	if len(f.requests) != 3 {
		t.Fatal("reconnect reused old HTTP proof", f.requests)
	}
}

func TestCompatibilityFreshFullReadQueuesAndAvoidsDuplicateGET(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx := context.Background()
	s := f.service
	// Replace the regular filter job with the active daily archive cycle.
	if _, err := s.Store.DB.Exec("DELETE FROM compatibility_pairs"); err != nil {
		t.Fatal(err)
	}
	c := compatibilityCycle(t, s, 1, f.now)
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	p, _, observed, err := s.Client.checkedProfileObserved(ctx, 1, ss, 7)
	if err != nil {
		t.Fatal(err)
	}
	mu := s.Store.SessionMutex(1)
	mu.Lock()
	stored, err := s.captureCheckedProfileLocked(ctx, 1, ss, p, observed)
	if err == nil {
		err = s.queueCheckedCompatibility(ctx, 1, ss, stored)
	}
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := s.compatibilityStep(ctx); err != nil || !ran || f.posts != 1 {
		t.Fatal(ran, f.posts, err)
	}
	count := 0
	for _, r := range f.requests {
		if strings.HasSuffix(r, "/user/getUser") {
			count++
		}
	}
	if count != 1 {
		t.Fatal("duplicate profile GET", f.requests)
	}
}

func TestCompatibilityFailureChecksDisappearanceWithoutRetryingGPT(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx := context.Background()
	s := f.service
	original := s.Client.HTTP.Transport
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/user/getUser") && f.posts > 0 {
			return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		}
		return original.RoundTrip(r)
	})
	f.status, f.response = 400, `{"title":"profile unavailable"}`
	if ran, err := s.compatibilityStep(ctx); err != nil || !ran || f.posts != 1 || f.state(t) != "rejected" {
		t.Fatal(ran, f.posts, f.state(t), err)
	}
	missing, err := profileKnownUnavailable(ctx, s.Store.DB, 1, 7)
	if err != nil || !missing {
		t.Fatal("missing profile not checked", missing, err)
	}
	f.now = f.now.Add(24 * time.Hour)
	if err = s.planCompatibility(ctx, f.now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.compatibilityStep(ctx); err != nil || f.posts != 1 {
		t.Fatal("paid request repeated", f.posts, err)
	}
}

func TestSyncCompatibilityPhotoVersionAdvancesWithoutRecalculation(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	c := compatibilityCycle(t, s, 1, now)
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	own, _ := debugProfile(ss.Account)
	hash := compatibilityProfileHash(own)
	if _, err = s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	if err = s.planSyncCompatibilityTarget(ctx, c, ss, hash, p, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE compatibility_cycle_targets SET state='succeeded' WHERE cycle_id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	p.Profile["photos"] = "changed-image"
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Store.Version(ctx, 1, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = s.planSyncCompatibilityTarget(ctx, c, ss, hash, p, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var version int64
	var state string
	if err = s.Store.DB.QueryRow("SELECT version_id,state FROM compatibility_cycle_targets WHERE cycle_id=?", c.ID).Scan(&version, &state); err != nil || version != p.VersionID || state != "succeeded" {
		t.Fatal(version, state, err)
	}
}

func TestCompatibilityLegacyUnknownBlocksNewTextCacheJob(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	j := compatibilityQueue(t, s, 1, p, now)
	if _, err := s.Store.DB.Exec("UPDATE compatibility_jobs SET own_hash='legacy-own-v1',target_hash='legacy-target-v1',input_hash='legacy-pair-v1',state='unknown',reason='transport_unconfirmed'"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{UpstreamID: j.FromUserID, ProfileID: p.ID, VersionID: p.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: j.Generation, OwnProfile: j.OwnProfile, TargetProfile: j.TargetProfile, RequestBody: compatibilityRequest(j), ResponseBody: []byte(`{"content":"75%"}`)})
	if err != nil {
		t.Fatal(err)
	}
	compatibilityQueue(t, s, 1, p, now.Add(time.Minute))
	var count int
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&count); err != nil || count != 1 {
		t.Fatal("v2 cache bypassed uncertain v1 attempt", count, err)
	}
	if claimed, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(time.Minute)); err != nil || claimed != nil {
		t.Fatal("unknown pair became dispatchable", claimed, err)
	}
}

func TestCompatibilityRefreshEnqueueFailureDoesNotReplayGET(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx, s := context.Background(), f.service
	if _, err := s.Store.DB.Exec("DELETE FROM compatibility_pairs"); err != nil {
		t.Fatal(err)
	}
	c := compatibilityCycle(t, s, 1, f.now)
	if _, err := s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("DELETE FROM compatibility_pairs"); err != nil {
		t.Fatal(err)
	}
	compatibilityRefreshCohort(t, s, c)
	c.Stage = "refresh"
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE sync_refresh_items SET state='pending' WHERE cycle_id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("CREATE TRIGGER reject_gpt_queue BEFORE INSERT ON compatibility_jobs BEGIN SELECT RAISE(ABORT,'test enqueue failure'); END"); err != nil {
		t.Fatal(err)
	}
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.syncRefreshOne(ctx, c, ss, f.now); err != nil {
		t.Fatal("enqueue failure interrupted refresh checkpoint", err)
	}
	var state string
	if err = s.Store.DB.QueryRow("SELECT state FROM sync_refresh_items WHERE cycle_id=? AND profile_id=7", c.ID).Scan(&state); err != nil || state != "available" {
		t.Fatal("successful GET checkpoint lost", state, err)
	}
	if _, err = s.Store.DB.Exec("DROP TRIGGER reject_gpt_queue"); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	s.Client.Now = func() time.Time { return f.now }
	if _, err = s.PlanSyncCompatibility(ctx, c, false); err != nil {
		t.Fatal(err)
	}
	var jobs int
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs WHERE state='queued'").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatal("planner did not repair queue", jobs, err)
	}
	if _, err = s.syncRefreshOne(ctx, c, ss, f.now); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 1 || !strings.HasSuffix(f.requests[0], "/user/getUser") || f.posts != 0 {
		t.Fatal("noncritical enqueue failure replayed source/paid HTTP", f.requests, f.posts)
	}
}
