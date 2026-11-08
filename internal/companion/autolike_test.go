package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type autoFixture struct {
	worker   *Autoliker
	now      time.Time
	full     map[int64]Profile
	feed     []Profile
	sent     []int64
	requests []string
	results  []string
	hook     func(*http.Request)
	freeze   bool
}

func newAutoFixture(t *testing.T) *autoFixture {
	t.Helper()
	s, _ := uxFixture(t)
	s.Config.AllowReactions = true
	s.Store.cfg.AllowReactions = true
	f := &autoFixture{worker: NewAutoliker(s), now: time.Now().UTC().Truncate(time.Second), full: map[int64]Profile{}}
	f.worker.now = func() time.Time { return f.now }
	s.Client.Now = f.worker.now
	ctx := context.Background()
	if _, err := s.Store.DB.ExecContext(ctx, filterSchema); err != nil {
		t.Fatal(err)
	}
	filters := DefaultFeedFilters()
	filters.AgeMax = 25
	raw, _ := json.Marshal(filters)
	for _, owner := range []int64{1, 2} {
		if err := s.Store.SaveSession(ctx, owner, Session{Token: "test", Refresh: "test-refresh", UserID: owner + 100, Generation: fmt.Sprint(owner), Expires: f.now.Add(72 * time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Store.DB.ExecContext(ctx, "INSERT INTO feed_filters(owner_id,applied_json) VALUES(?,?)", owner, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.requests = append(f.requests, r.Method+" "+r.URL.Path)
		if f.hook != nil {
			f.hook(r)
		}
		var body string
		switch r.URL.Path {
		case "/user/getMe":
			body = fmt.Sprintf(`{"userId":%s,"type":1,"freezeState":%t}`, r.URL.Query().Get("userId"), f.freeze)
		case "/user/getUsers":
			if r.URL.Query().Get("all") != "0" {
				t.Fatal("auto consumed all=1 discovery")
			}
			b, _ := json.Marshal(f.feed)
			if f.feed == nil {
				b = []byte(`[]`)
			}
			body = string(b)
		case "/user/getUser":
			id, _ := strconv.ParseInt(r.URL.Query().Get("targetUserId"), 10, 64)
			b, _ := json.Marshal(f.full[id])
			body = string(b)
		case "/reactions/addNew":
			var p struct {
				ID   int64 `json:"userId"`
				From int64 `json:"fromUserId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				t.Fatal(err)
			}
			if p.From != 101 {
				t.Fatal("cross-owner reaction")
			}
			f.sent = append(f.sent, p.ID)
			body = `{"IsSuccess":true,"IsLast":true}`
			if len(f.results) > 0 {
				body = f.results[0]
				f.results = f.results[1:]
			}
			if body == "transport-error" {
				return nil, errors.New("lost fixture response")
			}
		default:
			t.Fatalf("unexpected source request: %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	return f
}
func (f *autoFixture) add(t *testing.T, owner, id, age int64, favorite bool) {
	t.Helper()
	p := Profile{"userId": id, "age": age, "height": 170, "name": "fixture"}
	f.full[id] = p
	if _, err := f.worker.Service.Store.ArchiveViewed(context.Background(), owner, []Profile{p}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if favorite {
		if err := f.worker.Service.Store.Favorite(context.Background(), owner, id); err != nil {
			t.Fatal(err)
		}
	}
}
func (f *autoFixture) step(t *testing.T) {
	t.Helper()
	if _, err := f.worker.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func (f *autoFixture) advance() { f.now = f.now.Add(31 * time.Second) }

func TestAutoOffByDefaultAndExplicitRestartDurability(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 55, true)
	f.step(t)
	if len(f.requests) != 0 {
		t.Fatal("default-off worker requested source")
	}
	if err := f.worker.Start(context.Background(), 1, "series", 1); err != nil {
		t.Fatal(err)
	}
	replacement := NewAutoliker(f.worker.Service)
	replacement.now = f.worker.now
	f.worker = replacement
	f.step(t)
	if len(f.sent) != 1 || f.sent[0] != 8 {
		t.Fatal("explicit persisted enable did not resume")
	}
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Mode != "off" || j.Reason != "series_done" {
		t.Fatalf("unbounded series: %+v", j)
	}
}
func TestAutoFavoritesFirstThenFreshStrictFeedNeverGlobalArchive(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 9, 23, false)
	f.add(t, 1, 8, 55, true)
	f.add(t, 2, 99, 23, true)
	f.feed = []Profile{{"userId": 1, "age": 23, "height": 170}, {"userId": 7, "age": 45, "height": 170}}
	for _, p := range f.feed {
		f.full[p.ID()] = p
	}
	if err := f.worker.Start(context.Background(), 1, "series", 2); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	if len(f.sent) != 1 || f.sent[0] != 8 {
		t.Fatal("favorite outside filters lost priority")
	}
	f.advance()
	f.step(t)
	if _, err := f.worker.Service.Store.Profile(context.Background(), 1, 7); err != nil {
		t.Fatal("whole source batch was not archived")
	}
	f.advance()
	f.step(t)
	if len(f.sent) != 2 || f.sent[1] != 1 {
		t.Fatalf("global/nonmatching archive was liked: %v", f.sent)
	}
	var favorites int
	if err := f.worker.Service.Store.DB.QueryRow("SELECT COUNT(*) FROM favorites WHERE owner_id=1 AND profile_id=8").Scan(&favorites); err != nil || favorites != 1 {
		t.Fatal("favorite removed after sending")
	}
}
func TestAutoFilterChangeInvalidatesPendingProvenance(t *testing.T) {
	f := newAutoFixture(t)
	f.feed = []Profile{{"userId": 1, "age": 23, "height": 170}}
	f.full[1] = f.feed[0]
	if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	filters := DefaultFeedFilters()
	filters.AgeMax = 20
	raw, _ := json.Marshal(filters)
	if _, err := f.worker.Service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
		t.Fatal(err)
	}
	f.advance()
	f.step(t)
	if len(f.sent) != 0 {
		t.Fatal("old-filter candidate was sent")
	}
	var pending int
	if err := f.worker.Service.Store.DB.QueryRow("SELECT COUNT(*) FROM autolike_candidates WHERE owner_id=1 AND state='pending'").Scan(&pending); err != nil || pending != 0 {
		t.Fatal("old provenance survived")
	}
}
func TestAutoKnownQuotaWaitSurvivesRestartWithoutEarlyRetry(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 23, true)
	f.results = []string{`{"IsSuccess":false,"MessageKind":-1,"MinutesLeft":2}`, `{"IsSuccess":true}`}
	if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Mode != "automatic" || j.Reason != "quota" || j.NextAttempt <= f.now.Add(2*time.Minute).UnixMilli() {
		t.Fatalf("server wait lost: %+v", j)
	}
	oldCalls := len(f.requests)
	f.advance()
	f.worker = NewAutoliker(f.worker.Service)
	f.worker.now = func() time.Time { return f.now }
	f.step(t)
	if len(f.requests) != oldCalls {
		t.Fatal("quota retried early after restart")
	}
	f.now = time.UnixMilli(j.NextAttempt).Add(time.Second)
	f.step(t)
	if len(f.sent) != 2 {
		t.Fatalf("known-quota due retry missing: %v", f.sent)
	}
}
func TestAutoUnknownQuotaAndTransportStopWithoutResend(t *testing.T) {
	for _, result := range []string{`{"IsSuccess":false,"MessageKind":1,"MinutesLeft":2}`, `{"IsSuccess":false,"MessageKind":-1}`, `{"IsSuccess":false,"MessageKind":-1,"MinutesLeft":-1}`, "transport-error"} {
		t.Run(result, func(t *testing.T) {
			f := newAutoFixture(t)
			f.add(t, 1, 8, 23, true)
			f.results = []string{result}
			if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
				t.Fatal(err)
			}
			f.step(t)
			j, _ := f.worker.settings(context.Background(), 1)
			if j.Mode != "off" {
				t.Fatalf("unknown failure remained enabled: %+v", j)
			}
			f.now = f.now.Add(48 * time.Hour)
			f.step(t)
			if len(f.sent) != 1 {
				t.Fatal("blind automatic resend")
			}
		})
	}
}
func TestAutoOwnRollingBudgetDoesNotRejectItsOwnClaim(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 23, true)
	f.add(t, 1, 9, 23, true)
	j, _ := f.worker.settings(context.Background(), 1)
	if err := f.worker.updateSetting(context.Background(), 1, j.Nonce, "cap", 1); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	if len(f.sent) != 1 {
		t.Fatal("last permitted action rejected itself")
	}
	f.advance()
	before := len(f.requests)
	f.step(t)
	j, _ = f.worker.settings(context.Background(), 1)
	if j.Reason != "own_cap" || j.NextAttempt < f.now.Add(23*time.Hour).UnixMilli() || len(f.requests) != before {
		t.Fatal("own rolling budget not enforced")
	}
}
func TestAutoStopDuringFreshAccountPreventsPostAndOldMenuEnable(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 23, true)
	j, _ := f.worker.settings(context.Background(), 1)
	oldNonce := j.Nonce
	if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getMe" {
			if err := f.worker.Service.DisableAutolike(context.Background(), 1, "paused"); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.step(t)
	if len(f.sent) != 0 {
		t.Fatal("off during account preflight still sent")
	}
	_, err := f.worker.Service.HandleSettingsCallback(context.Background(), 1, "as:"+oldNonce+":automatic")
	if err == nil {
		t.Fatal("stale menu reenabled after stop")
	}
}
func TestAutoInterruptedSendingAndPauseFreezeNeverResume(t *testing.T) {
	for _, reason := range []string{"sending", "pause", "freeze", "auth"} {
		t.Run(reason, func(t *testing.T) {
			f := newAutoFixture(t)
			f.add(t, 1, 8, 23, true)
			if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "sending":
				j, _ := f.worker.settings(context.Background(), 1)
				_, err := f.worker.Service.Store.DB.Exec("INSERT INTO autolike_candidates(owner_id,run_id,profile_id,source,filter_hash,generation,page,fetched_at,state) VALUES(?, ?,8,'favorite','',?,0,0,'sending')", 1, j.RunID, j.Generation)
				if err != nil {
					t.Fatal(err)
				}
			case "pause":
				f.worker.Service.Store.Pause(context.Background(), 1, true)
			case "freeze":
				f.freeze = true
			case "auth":
				f.worker.Service.Store.SessionState(context.Background(), 1, "needs_reconnect", "fixture")
			}
			f.step(t)
			j, _ := f.worker.settings(context.Background(), 1)
			if j.Mode != "off" || len(f.sent) != 0 {
				t.Fatal("stop condition sent or stayed enabled")
			}
		})
	}
}

func TestAutoFilterChangeDuringHydrationPreventsPost(t *testing.T) {
	f := newAutoFixture(t)
	f.feed = []Profile{{"userId": 1, "age": 23, "height": 170}}
	f.full[1] = f.feed[0]
	if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getUser" {
			filters := DefaultFeedFilters()
			filters.AgeMax = 20
			b, _ := json.Marshal(filters)
			if _, err := f.worker.Service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(b)); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.advance()
	f.step(t)
	if len(f.sent) != 0 {
		t.Fatal("filter changed during hydration but reaction sent")
	}
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Mode != "off" {
		t.Fatal("changed authorization kept running")
	}
}
func TestAutoForbiddenAndRateLimitStopWithoutAutomaticRetry(t *testing.T) {
	for _, status := range []int{403, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := newAutoFixture(t)
			f.add(t, 1, 8, 23, true)
			f.worker.Service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" {
					t.Fatal("blocked source sent reaction")
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"fixture"}`)), Request: r}, nil
			})
			if err := f.worker.Start(context.Background(), 1, "automatic", 10); err != nil {
				t.Fatal(err)
			}
			f.step(t)
			j, _ := f.worker.settings(context.Background(), 1)
			if j.Mode != "off" {
				t.Fatal("403/429 scheduled automatic retry")
			}
			f.now = f.now.Add(48 * time.Hour)
			if worked, err := f.worker.Step(context.Background()); err != nil || worked {
				t.Fatal("blocked worker resumed without enable")
			}
		})
	}
}
func TestAutoSuccessCursorFailureDoesNotContinueSeriesAfterRestart(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 23, true)
	f.add(t, 1, 9, 23, true)
	if err := f.worker.Start(context.Background(), 1, "series", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.worker.Service.Store.DB.Exec(`CREATE TRIGGER auto_cursor_failure BEFORE UPDATE OF sent ON autolike_settings BEGIN SELECT RAISE(ABORT,'fixture cursor write failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.worker.Step(context.Background()); err == nil {
		t.Fatal("cursor persistence failure hidden")
	}
	if _, err := f.worker.Service.Store.DB.Exec(`DROP TRIGGER auto_cursor_failure`); err != nil {
		t.Fatal(err)
	}
	replacement := NewAutoliker(f.worker.Service)
	replacement.now = f.worker.now
	f.worker = replacement
	f.advance()
	f.step(t)
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Mode != "off" || len(f.sent) != 1 {
		t.Fatal("uncertain series cursor continued after restart")
	}
}

func TestSharedOwnBudgetCountsUnknownAndKeepsOwnersIsolated(t *testing.T) {
	f := newAutoFixture(t)
	ctx := context.Background()
	s := f.worker.Service
	j, _ := f.worker.settings(ctx, 1)
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "cap", 1); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"pending", "processing", "succeeded", "unknown"} {
		if _, err := s.Store.DB.Exec("DELETE FROM actions WHERE owner_id=1"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Store.DB.Exec("INSERT INTO actions(owner_id,profile_id,kind,state,created_at) VALUES(1,8,'Like',?,?)", state, f.now.UTC().Format("2006-01-02 15:04:05")); err != nil {
			t.Fatal(err)
		}
		if err := s.ManualLikeBudgetGuard(ctx, 1); !errors.Is(err, ErrOwnLikeLimit) {
			t.Fatalf("%s did not consume own cap: %v", state, err)
		}
		if err := s.ManualLikeBudgetGuard(ctx, 2); err != nil {
			t.Fatal("another owner's cap leaked")
		}
	}
	if _, err := s.Store.DB.Exec("UPDATE actions SET state='quota_retried' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ManualLikeBudgetGuard(ctx, 1); err != nil {
		t.Fatal("confirmed nonconsuming quota was counted")
	}
	f.now = f.now.Add(25 * time.Hour)
	if _, err := s.Store.DB.Exec("UPDATE actions SET state='unknown' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ManualLikeBudgetGuard(ctx, 1); err != nil {
		t.Fatal("rolling window did not expire")
	}
}

// A successful reaction removes its target from the live feed. Offset paging
// after that mutation must not skip the profiles that shifted toward page one.
func TestAutoSuccessfulLikesRestartLiveFeedAtFirstPage(t *testing.T) {
	f := newAutoFixture(t)
	for _, id := range []int64{1, 2, 3, 4} {
		f.full[id] = Profile{"userId": id, "age": 23, "height": 170}
	}
	var pages []int
	base := f.worker.Service.Client.HTTP.Transport
	f.worker.Service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/user/getUsers" {
			page, _ := strconv.Atoi(r.URL.Query().Get("pageNumber"))
			pages = append(pages, page)
			var remaining []Profile
			for _, id := range []int64{1, 2, 3, 4} {
				liked := false
				for _, sent := range f.sent {
					liked = liked || sent == id
				}
				if !liked {
					remaining = append(remaining, f.full[id])
				}
			}
			start := (page - 1) * 2
			f.feed = nil
			if start < len(remaining) {
				end := start + 2
				if end > len(remaining) {
					end = len(remaining)
				}
				f.feed = remaining[start:end]
			}
		}
		return base.RoundTrip(r)
	})
	if err := f.worker.Start(context.Background(), 1, "series", 4); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		f.step(t)
		f.advance()
		j, _ := f.worker.settings(context.Background(), 1)
		if j.Mode == "off" {
			break
		}
	}
	if len(f.sent) != 4 {
		t.Fatalf("live offset skipped eligible profiles: sent=%v pages=%v", f.sent, pages)
	}
	for i, id := range []int64{1, 2, 3, 4} {
		if f.sent[i] != id {
			t.Fatalf("wrong series order: %v", f.sent)
		}
	}
	if len(pages) != 2 || pages[0] != 1 || pages[1] != 1 {
		t.Fatalf("fresh live batch used stale offset: %v", pages)
	}
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Reason != "series_done" {
		t.Fatalf("series ended prematurely: %+v", j)
	}
}

func TestAutoTypedExhaustedUsesOwnDailyScheduleOnlyInAutomaticMode(t *testing.T) {
	for _, mode := range []string{"series", "automatic"} {
		t.Run(mode, func(t *testing.T) {
			f := newAutoFixture(t)
			f.add(t, 1, 8, 23, true)
			f.results = []string{`{"IsSuccess":false,"IsLast":true}`}
			if err := f.worker.Start(context.Background(), 1, mode, 4); err != nil {
				t.Fatal(err)
			}
			f.step(t)
			j, _ := f.worker.settings(context.Background(), 1)
			if j.Reason != "feed_empty" {
				t.Fatalf("typed exhaustion reported as generic failure: %+v", j)
			}
			if mode == "automatic" {
				if j.Mode != "automatic" || j.NextAttempt != f.now.Add(24*time.Hour).UnixMilli() {
					t.Fatalf("missing own daily cycle: %+v", j)
				}
				count := len(f.requests)
				f.advance()
				f.step(t)
				if len(f.requests) != count {
					t.Fatal("daily feed checked too early")
				}
			} else if j.Mode != "off" {
				t.Fatal("bounded series stayed enabled after source exhaustion")
			}
		})
	}
}
func TestAutoKnownQuotaHasPriorityOverFailureLast(t *testing.T) {
	f := newAutoFixture(t)
	f.add(t, 1, 8, 23, true)
	f.results = []string{`{"IsSuccess":false,"IsLast":true,"MessageKind":-1,"MinutesLeft":2}`}
	if err := f.worker.Start(context.Background(), 1, "automatic", 4); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	j, _ := f.worker.settings(context.Background(), 1)
	if j.Reason != "quota" || j.NextAttempt > f.now.Add(3*time.Minute).UnixMilli() {
		t.Fatalf("known quota was replaced with daily-feed guess: %+v", j)
	}
}
