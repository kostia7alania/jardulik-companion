package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"
)

func compatibilityFixture(t *testing.T) (*Service, time.Time) {
	t.Helper()
	f := newAutoFixture(t)
	s := f.worker.Service
	s.Autoliker = f.worker
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		own := Profile{"userId": owner + 100, "name": "Own", "age": 33, "height": 177, "freezeState": false, "fire": 60}
		if err := s.Store.SaveAccount(ctx, owner, own); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Store.InitCompatibility(ctx); err != nil {
		t.Fatal(err)
	}
	return s, f.now
}
func compatibilityAdd(t *testing.T, s *Service, owner, target, age int64) StoredProfile {
	t.Helper()
	ctx := context.Background()
	_, err := s.Store.ArchiveViewed(ctx, owner, []Profile{{"userId": target, "name": "Target", "age": age, "height": 170, "description": "same text", "goal": "family"}}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.Profile(ctx, owner, target)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func compatibilityQueue(t *testing.T, s *Service, owner int64, p StoredProfile, now time.Time) CompatibilityJob {
	t.Helper()
	ctx := context.Background()
	ss, err := s.Store.Session(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	own, err := debugProfile(ss.Account)
	if err != nil {
		t.Fatal(err)
	}
	j := CompatibilityJob{OwnerID: owner, FromUserID: ss.UserID, TargetUserID: p.ID, VersionID: p.VersionID, Generation: ss.Generation, OwnHash: compatibilityProfileHash(own), TargetHash: compatibilityProfileHash(p.Profile), FilterHash: "fixture", OwnProfile: append([]byte(nil), ss.Account...), TargetProfile: []byte(p.RawJSON)}
	j.InputHash = compatibilityInputHash(j.OwnHash, j.TargetHash)
	if err = s.Store.enqueueCompatibility(ctx, j, now); err != nil {
		t.Fatal(err)
	}
	return j
}
func compatibilityClaim(t *testing.T, s *Service, now time.Time) *CompatibilityJob {
	t.Helper()
	j, err := s.Store.ClaimCompatibilityJob(context.Background(), now)
	if err != nil || j == nil {
		t.Fatalf("claim=%+v err=%v", j, err)
	}
	return j
}
func compatibilityRequest(j CompatibilityJob) []byte {
	b, _ := json.Marshal(map[string]int64{"fromUserId": j.FromUserID, "targetUserId": j.TargetUserID})
	return b
}

func TestCompatibilityStoreManyToManyOwnerIsolationAndHash(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	compatibilityQueue(t, s, 1, p, now)
	compatibilityQueue(t, s, 1, p, now)
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 8, 24), now)
	compatibilityQueue(t, s, 2, compatibilityAdd(t, s, 2, 7, 23), now)
	var pairs, jobs int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_pairs").Scan(&pairs); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if pairs != 3 || jobs != 3 {
		t.Fatalf("wrong M:N/dedup %d %d", pairs, jobs)
	}
	j := compatibilityClaim(t, s, now)
	for _, change := range []string{"owner", "from", "target", "ownhash", "targethash", "inputhash"} {
		copy := *j
		switch change {
		case "owner":
			copy.OwnerID = 2
		case "from":
			copy.FromUserID = 102
		case "target":
			copy.TargetUserID = 8
		case "ownhash":
			copy.OwnHash = "other"
		case "targethash":
			copy.TargetHash = "other"
		case "inputhash":
			copy.InputHash = "other"
		}
		if err := s.Store.ReleaseCompatibilityJob(ctx, copy, now, "busy"); err == nil {
			t.Fatalf("release accepted forged %s", change)
		}
		if err := s.Store.FinishCompatibilityJob(ctx, copy, "cancelled", 0, nil, 0, "guard", now); err == nil {
			t.Fatalf("finish accepted forged %s", change)
		}
		if err := s.Store.StartCompatibilityJob(ctx, copy, j.OwnProfile, j.TargetProfile, compatibilityRequest(*j), now, time.Minute); err == nil {
			t.Fatalf("start accepted forged %s", change)
		}
	}
	if err := s.Store.ReleaseCompatibilityJob(ctx, *j, now, "foreground_busy"); err != nil {
		t.Fatal(err)
	}
	next := compatibilityClaim(t, s, now.Add(time.Second))
	if next.OwnerID != 2 {
		t.Fatal("preflight-only owner starved another owner", next.OwnerID)
	}
	own, _ := debugProfile(j.OwnProfile)
	before := compatibilityProfileHash(own)
	own["fire"] = 1
	own["likes"] = 9
	own["orderEndDate"] = "later"
	if before != compatibilityProfileHash(own) {
		t.Fatal("counters invalidated cache")
	}
	own["description"] = "changed"
	if before == compatibilityProfileHash(own) {
		t.Fatal("profile change did not invalidate cache")
	}
}

func TestCompatibilityStoreRecoveryUnknownAndCooldown(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	compatibilityQueue(t, s, 1, p, now)
	j := compatibilityClaim(t, s, now)
	if err := s.Store.RecoverCompatibilityJobs(ctx, now); err != nil {
		t.Fatal(err)
	}
	j = compatibilityClaim(t, s, now.Add(time.Second))
	req := compatibilityRequest(*j)
	if err := s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, req, now.Add(time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.RecoverCompatibilityJobs(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	p.Profile["description"] = "new input"
	raw, _ := json.Marshal(p.Profile)
	p.RawJSON = string(raw)
	compatibilityQueue(t, s, 1, p, now.Add(2*time.Second))
	if j, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(2*time.Minute)); err != nil || j != nil {
		t.Fatal("unknown result was queued again", j, err)
	}
	var state string
	var saved []byte
	if err := s.Store.DB.QueryRow("SELECT state,request_body FROM compatibility_jobs WHERE id=?", j.ID).Scan(&state, &saved); err != nil {
		t.Fatal(err)
	}
	if state != "unknown" || !bytes.Equal(req, saved) {
		t.Fatal("crash lost unknown audit")
	}
}

func TestCompatibilityStoreExactOutcomeAndSessionGuard(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 7, 23), now)
	j := compatibilityClaim(t, s, now)
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	original := ss.Generation
	ss.Generation = "new-login"
	if err = s.Store.SaveSession(ctx, 1, ss); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, compatibilityRequest(*j), now, time.Minute); err == nil {
		t.Fatal("stale generation passed")
	}
	ss.Generation = original
	if err = s.Store.SaveSession(ctx, 1, ss); err != nil {
		t.Fatal(err)
	}
	req := compatibilityRequest(*j)
	if err = s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, req, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	response := []byte("{\n \"content\":\"75% prose\",\"model\":\"reported\"\n}\n")
	a, err := s.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{UpstreamID: j.FromUserID, ProfileID: j.TargetUserID, VersionID: j.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: j.Generation, RequestBody: req, ResponseBody: response, OwnProfile: j.OwnProfile, TargetProfile: j.TargetProfile})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.FinishCompatibilityJob(ctx, *j, "succeeded", 200, []byte("different"), a.ID, "", now); err == nil {
		t.Fatal("mismatched audit succeeded")
	}
	if err = s.Store.FinishCompatibilityJob(ctx, *j, "succeeded", 200, response, a.ID, "", now); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	var analysis int64
	if err = s.Store.DB.QueryRow("SELECT response_body,analysis_id FROM compatibility_jobs WHERE id=?", j.ID).Scan(&raw, &analysis); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, response) || analysis != a.ID {
		t.Fatal("outcome raw changed")
	}
	var linked int64
	if err = s.Store.DB.QueryRow("SELECT latest_analysis_id FROM compatibility_pairs WHERE id=?", j.PairID).Scan(&linked); err != nil || linked != a.ID {
		t.Fatal("pair did not link exact analysis", err)
	}
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 8, 24), now)
	if next, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(time.Second)); err != nil || next != nil {
		t.Fatal("global cooldown bypass", next, err)
	}
}

func TestCompatibilityStoreBackupDirtyOnlyForDurableChanges(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityAdd(t, s, 1, 7, 23)
	before := fullTestFingerprint(t, s.Store)
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	planned := fullTestFingerprint(t, s.Store)
	if before == planned {
		t.Fatal("new pair and queue did not dirty backup")
	}
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := s.planCompatibility(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := fullTestFingerprint(t, s.Store); got != planned {
		t.Fatal("unchanged planner scan dirtied backup")
	}
	j := compatibilityClaim(t, s, now.Add(time.Minute))
	request := compatibilityRequest(*j)
	if err := s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, request, now.Add(time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	response := []byte(`{"content":"complete","model":"reported"}`)
	analysis, err := s.Store.SaveAnalysisCapture(ctx, j.OwnerID, AnalysisCaptureInput{UpstreamID: j.FromUserID, ProfileID: j.TargetUserID, VersionID: j.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.Add(time.Minute).UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: j.Generation, RequestBody: request, ResponseBody: response, OwnProfile: j.OwnProfile, TargetProfile: j.TargetProfile})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.FinishCompatibilityJob(ctx, *j, "succeeded", 200, response, analysis.ID, "", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	completed := fullTestFingerprint(t, s.Store)
	if completed == planned {
		t.Fatal("saved result did not dirty backup")
	}
}

func TestCompatibilityStorePreflightErrorBacksOffWholeOwner(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 7, 23), now)
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 8, 23), now)
	j := compatibilityClaim(t, s, now)
	if err := s.Store.ReleaseCompatibilityJob(ctx, *j, now, "preflight_unavailable"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(6*time.Second)); err != nil || got != nil {
		t.Fatal("next job bypassed account backoff", got, err)
	}
	if got, err := s.Store.ClaimCompatibilityJob(ctx, now.Add(time.Minute)); err != nil || got == nil {
		t.Fatal("account did not resume after preflight backoff", got, err)
	}
}

func TestCompatibilityStoreRechecksManualAnalysisBeforePost(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	compatibilityQueue(t, s, 1, compatibilityAdd(t, s, 1, 7, 23), now)
	j := compatibilityClaim(t, s, now)
	var own bytes.Buffer
	if err := json.Indent(&own, j.OwnProfile, "", "  "); err != nil {
		t.Fatal(err)
	}
	own.WriteByte('\n')
	target := append(append([]byte("[ "), j.TargetProfile...), []byte(" ]\n")...)
	request := []byte("{\n  \"fromUserId\":101, \"targetUserId\":7\n}\n")
	response := []byte("{\n  \"content\":\"manual result\", \"model\":\"reported\"\n}\n")
	a, err := s.Store.SaveAnalysisCapture(ctx, j.OwnerID, AnalysisCaptureInput{UpstreamID: j.FromUserID, ProfileID: j.TargetUserID, VersionID: j.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.Add(time.Second).UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: j.Generation, RequestBody: request, ResponseBody: response, OwnProfile: own.Bytes(), TargetProfile: target})
	if err != nil {
		t.Fatal(err)
	}
	next := strconv.FormatInt(now.Add(10*time.Minute).UnixMilli(), 10)
	if err = s.Store.SetMeta(ctx, "compatibility_next_at_ms", next); err != nil {
		t.Fatal(err)
	}
	err = s.Store.StartCompatibilityJob(ctx, *j, j.OwnProfile, j.TargetProfile, compatibilityRequest(*j), now.Add(2*time.Second), time.Minute)
	if !errors.Is(err, ErrCompatibilityCached) {
		t.Fatal("manual result did not resolve queued job", err)
	}
	saved, err := scanCompatibilityJob(s.Store.DB.QueryRowContext(ctx, "SELECT "+compatibilityJobColumns+" FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id WHERE j.id=?", j.ID))
	if err != nil {
		t.Fatal(err)
	}
	if saved.State != "succeeded" || saved.AnalysisID != a.ID || saved.HTTPStatus != 200 || saved.Reason != "cached_analysis" {
		t.Fatalf("wrong cached receipt: %+v", saved)
	}
	for name, pair := range map[string][2][]byte{"own": {saved.OwnProfile, own.Bytes()}, "target": {saved.TargetProfile, target}, "request": {saved.RequestBody, request}, "response": {saved.ResponseBody, response}} {
		if !bytes.Equal(pair[0], pair[1]) {
			t.Fatal("changed cached raw bytes", name)
		}
	}
	var latest int64
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT latest_analysis_id FROM compatibility_pairs WHERE id=?", j.PairID).Scan(&latest); err != nil || latest != a.ID {
		t.Fatal("pair did not resolve to manual result", latest, err)
	}
	if s.Store.Meta(ctx, "compatibility_next_at_ms") != next {
		t.Fatal("cache hit consumed POST cooldown")
	}
}
