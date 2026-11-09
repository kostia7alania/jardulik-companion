package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestCompatibilityPlannerFiltersVersionsAndImportedCache(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	compatibilityAdd(t, s, 1, 8, 30)
	compatibilityAdd(t, s, 1, 9, 17)
	compatibilityAdd(t, s, 2, 10, 24)
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	var jobs int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatal("wrong owner/filter targets", jobs, err)
	}
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil || jobs != 2 {
		t.Fatal("planner did not give next owner a turn", jobs, err)
	}
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{UpstreamID: ss.UserID, ProfileID: p.ID, VersionID: p.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: ss.Generation, RequestBody: []byte(`{"fromUserId":101,"targetUserId":7}`), ResponseBody: []byte(`{"content":"cached","model":"reported"}`), OwnProfile: ss.Account, TargetProfile: []byte(p.RawJSON)})
	if err != nil {
		t.Fatal(err)
	}
	// A previously captured analysis is reused when planning a previously unqueued pair.
	if _, err = s.Store.DB.Exec("DELETE FROM compatibility_pairs WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var analysis int64
	if err = s.Store.DB.QueryRow("SELECT latest_analysis_id FROM compatibility_pairs WHERE owner_id=1 AND target_user_id=7").Scan(&analysis); err != nil || analysis != a.ID {
		t.Fatal("imported result was not reused", analysis, err)
	}
	_, err = s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "name": "Target", "age": 23, "height": 170, "description": "changed"}}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id WHERE p.owner_id=1 AND p.target_user_id=7").Scan(&jobs); err != nil || jobs != 2 {
		t.Fatal("new profile input lost history", jobs, err)
	}
}

func TestCompatibilityPlannerBoundedCursorAndAges(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	for i := int64(1); i <= compatibilityPlanBatch+1; i++ {
		compatibilityAdd(t, s, 1, 1000+i, 23)
	}
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	// Let owner2 take its turn, then continue owner1's cursor.
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := s.planCompatibility(ctx, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	var jobs int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil || jobs != compatibilityPlanBatch+1 {
		t.Fatal("cursor lost suffix", jobs, err)
	}
	filters := DefaultFeedFilters()
	filters.AgeMin = 0
	filters.AgeMax = 111
	for _, age := range []int{17, 18, 100, 101, 111, 112} {
		p := Profile{"userId": 1, "age": age, "height": 170}
		if got := compatibilityMatchesFilters(p, filters); got != (age >= 18 && age <= 111) {
			t.Fatal(fmt.Sprint(age), got)
		}
	}
	filters.City = "Vladikavkaz"
	filters.Country = "Russia"
	if compatibilityMatchesFilters(Profile{"age": 23, "height": 170, "country": "Russia"}, filters) {
		t.Fatal("missing required city matched")
	}
}

func TestCompatibilityPlannerDoesNotTouchHTTPOrProfileRaw(t *testing.T) {
	s, now := compatibilityFixture(t)
	ctx := context.Background()
	p := compatibilityAdd(t, s, 1, 7, 23)
	if err := s.planCompatibility(ctx, now); err != nil {
		t.Fatal(err)
	}
	after, err := s.Store.Profile(ctx, 1, 7)
	if err != nil || after.RawJSON != p.RawJSON {
		t.Fatal("planner changed raw profile", err)
	}
	ss, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	own, _ := debugProfile(ss.Account)
	own["fire"] = 58
	own["likes"] = 12
	if err = s.Store.SaveAccount(ctx, 1, own); err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var jobs int
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatal("counter produced another job", jobs, err)
	}
	own["description"] = "updated own profile"
	if err = s.Store.SaveAccount(ctx, 1, own); err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = s.planCompatibility(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.DB.QueryRow("SELECT COUNT(*) FROM compatibility_jobs").Scan(&jobs); err != nil || jobs != 2 {
		t.Fatal("own changes were not scheduled", jobs, err)
	}
	// Cache identity is canonical, independent of object field order.
	raw, _ := json.Marshal(own)
	decoded, _ := debugProfile(raw)
	if compatibilityProfileHash(own) != compatibilityProfileHash(decoded) {
		t.Fatal("JSON formatting affected cache")
	}
}
