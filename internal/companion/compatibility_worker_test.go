package companion

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type compatibilityExecutorFixture struct {
	service     *Service
	now         time.Time
	own, target Profile
	posts       int
	requests    []string
	status      int
	response    string
	lost        bool
	onTarget    func()
}

func newCompatibilityExecutorFixture(t *testing.T) *compatibilityExecutorFixture {
	t.Helper()
	base := newAutoFixture(t)
	s := base.worker.Service
	s.Autoliker = base.worker
	s.Config.CompatibilityEnabled = true
	s.Config.CompatibilityGap = time.Minute
	s.Config.MinFreeBytes = 0
	f := &compatibilityExecutorFixture{service: s, now: base.now, status: 200, response: "{\n  \"content\": \"raw result\", \"model\": \"reported-model\"\n}\n",
		own:    Profile{"userId": int64(101), "name": "owner", "age": 33, "height": 177, "type": 1, "freezeState": false},
		target: Profile{"userId": int64(7), "name": "target", "age": 23, "height": 170, "description": "one"}}
	s.Client.Now = func() time.Time { return f.now }
	ctx := context.Background()
	if err := s.Store.SaveAccount(ctx, 1, f.own); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{f.target}, "GET /user/getUser"); err != nil {
		t.Fatal(err)
	}
	if err := s.planCompatibility(ctx, f.now); err != nil {
		t.Fatal(err)
	}
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.requests = append(f.requests, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer test" || r.Header.Get("User-Agent") == "" {
			t.Fatal("source headers were not supplied by shared client")
		}
		var body []byte
		status := 200
		switch strings.TrimPrefix(r.URL.Path, "/api") {
		case "/user/getMe":
			body, _ = json.Marshal(f.own)
		case "/user/getUser":
			body, _ = json.Marshal(f.target)
			if f.onTarget != nil {
				f.onTarget()
			}
		case "/GPT/checkCompatibility":
			if r.Method != "POST" {
				t.Fatal("wrong GPT method")
			}
			f.posts++
			var request map[string]int64
			if json.NewDecoder(r.Body).Decode(&request) != nil || len(request) != 2 || request["fromUserId"] != 101 || request["targetUserId"] != 7 {
				t.Fatal("wrong GPT pair", request)
			}
			if f.lost {
				return nil, errors.New("response lost")
			}
			status, body = f.status, []byte(f.response)
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
	})
	return f
}

func (f *compatibilityExecutorFixture) state(t *testing.T) string {
	t.Helper()
	var state string
	if err := f.service.Store.DB.QueryRow("SELECT state FROM compatibility_jobs ORDER BY id DESC LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCompatibilityExecutorStoresExactResponseAndNeverLikes(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	if ran, err := f.service.compatibilityStep(context.Background()); err != nil || !ran {
		t.Fatal("job did not execute", ran, err)
	}
	if f.posts != 1 || f.state(t) != "succeeded" {
		t.Fatal("job not completed", f.posts, f.state(t))
	}
	var analysisID int64
	var raw []byte
	var model string
	if err := f.service.Store.DB.QueryRow(`SELECT a.id,a.response_body,a.model FROM compatibility_pairs p JOIN profile_analyses a ON a.id=p.latest_analysis_id WHERE p.owner_id=1 AND p.from_user_id=101 AND p.target_user_id=7`).Scan(&analysisID, &raw, &model); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, []byte(f.response)) || model != "reported-model" {
		t.Fatal("GPT raw/model changed")
	}
	f.now = f.now.Add(2 * time.Minute)
	if err := f.service.planCompatibility(context.Background(), f.now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.compatibilityStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.posts != 1 {
		t.Fatal("completed pair was recalculated")
	}
}

func TestCompatibilityExecutorYieldsAndRechecksCurrentFilters(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	f.service.beginForeground()
	if ran, err := f.service.compatibilityStep(context.Background()); ran || err != nil || len(f.requests) != 0 {
		t.Fatal("foreground did not take priority", ran, err)
	}
	f.service.endForeground()
	filters := DefaultFeedFilters()
	filters.AgeMax = 22
	raw, _ := json.Marshal(filters)
	if _, err := f.service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.compatibilityStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.posts != 0 || len(f.requests) != 0 || f.state(t) != "cancelled" {
		t.Fatal("stale filter job reached source", f.requests)
	}
}

func TestCompatibilityExecutorForegroundBeforePostLeavesJobQueued(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	f.onTarget = func() { f.service.beginForeground() }
	defer f.service.endForeground()
	if ran, err := f.service.compatibilityStep(context.Background()); ran || err != nil {
		t.Fatal(ran, err)
	}
	if f.posts != 0 || f.state(t) != "queued" {
		t.Fatal("foreground race sent GPT")
	}
}

func TestCompatibilityExecutorFreshChangePlansNewInputWithoutOldPost(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	f.target["description"] = "changed"
	if ran, err := f.service.compatibilityStep(context.Background()); ran || err != nil {
		t.Fatal(ran, err)
	}
	if f.posts != 0 || f.state(t) != "cancelled" {
		t.Fatal("outdated job sent")
	}
	f.now = f.now.Add(2 * time.Minute)
	// Owner 2 also exists in the shared fixture; fair planning visits both.
	for i := 0; i < 3; i++ {
		if err := f.service.planCompatibility(context.Background(), f.now); err != nil {
			t.Fatal(err)
		}
	}
	if ran, err := f.service.compatibilityStep(context.Background()); !ran || err != nil {
		t.Fatal("updated input did not execute", ran, err)
	}
	if f.posts != 1 || f.state(t) != "succeeded" {
		t.Fatal("new input not completed")
	}
}

func TestCompatibilityExecutorUnknownAnd401DoNotRepeatPost(t *testing.T) {
	for _, kind := range []string{"lost", "401"} {
		t.Run(kind, func(t *testing.T) {
			f := newCompatibilityExecutorFixture(t)
			if kind == "lost" {
				f.lost = true
			} else {
				f.status = 401
				f.response = `{"error":"unauthorized"}`
			}
			if ran, err := f.service.compatibilityStep(context.Background()); !ran || err != nil {
				t.Fatal(ran, err)
			}
			if f.posts != 1 || f.state(t) != "unknown" {
				t.Fatal("uncertain POST not retained", f.posts, f.state(t))
			}
			f.now = f.now.Add(48 * time.Hour)
			if err := f.service.Store.RecoverCompatibilityJobs(context.Background(), f.now); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if err := f.service.planCompatibility(context.Background(), f.now); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.service.compatibilityStep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.posts != 1 {
				t.Fatal("uncertain POST repeated")
			}
		})
	}
}

func TestCompatibilityRuntimePlannerHasOwnLockAndNoHTTP(t *testing.T) {
	s := runtimeServiceFixture(t)
	s.Config.CompatibilityEnabled = true
	var calls int
	s.Client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("planner attempted HTTP") })
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := s.RunBackground(ctx, RuntimeCompatibility)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if second, err := s.RunBackground(ctx, RuntimeCompatibility); err == nil {
		second()
		cancel()
		wait()
		t.Fatal("duplicate planner accepted")
	}
	cancel()
	wait()
	if calls != 0 {
		t.Fatal("standalone planner performed source HTTP")
	}
}

func TestCompatibilityConfigDefaultsAndBounds(t *testing.T) {
	for key, value := range map[string]string{
		"APP_DATA_DIR": t.TempDir(), "OWNER_TELEGRAM_ID": "1",
		"SESSION_KEY":           base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"BACKUP_KEY":            base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"COMPATIBILITY_ENABLED": "", "COMPATIBILITY_INTERVAL_SECONDS": "",
	} {
		t.Setenv(key, value)
	}
	cfg, err := LoadConfig()
	if err != nil || cfg.CompatibilityEnabled || cfg.CompatibilityGap != time.Minute {
		t.Fatal("unsafe compatibility defaults", err)
	}
	t.Setenv("COMPATIBILITY_ENABLED", "true")
	t.Setenv("COMPATIBILITY_INTERVAL_SECONDS", "120")
	cfg, err = LoadConfig()
	if err != nil || !cfg.CompatibilityEnabled || cfg.CompatibilityGap != 2*time.Minute {
		t.Fatal("explicit configuration ignored", err)
	}
	for _, raw := range []string{"0", "-1", "bad", "9223372037"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("COMPATIBILITY_INTERVAL_SECONDS", raw)
			if _, err := LoadConfig(); err == nil {
				t.Fatal("invalid interval accepted")
			}
		})
	}
	t.Setenv("COMPATIBILITY_ENABLED", "yes")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("invalid enable flag accepted")
	}
}

func TestCompatibilityExecutorReusesManualAnalysisAfterQueue(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx := context.Background()
	ownRaw, _ := json.Marshal(f.own)
	targetRaw, _ := json.Marshal(f.target)
	_, err := f.service.Store.SaveAnalysisCapture(ctx, 1, AnalysisCaptureInput{
		UpstreamID: 101, ProfileID: 7, Method: http.MethodPost, Path: "/GPT/checkCompatibility",
		ObservedAtMillis: f.now.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: "1",
		RequestBody: []byte(`{"fromUserId":101,"targetUserId":7}`), ResponseBody: []byte(f.response), OwnProfile: ownRaw, TargetProfile: targetRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := f.service.compatibilityStep(ctx); err != nil || ran {
		t.Fatal(ran, err)
	}
	if f.posts != 0 || f.state(t) != "succeeded" {
		t.Fatal("manual analysis cache was ignored")
	}
}
