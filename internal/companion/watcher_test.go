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

type watchFixture struct {
	store   *Store
	watch   *ProfileWatcher
	now     time.Time
	calls   []int64
	owners  []int64
	respond func(*http.Request, int64) (*http.Response, error)
}

func newWatchFixture(t *testing.T) *watchFixture {
	t.Helper()
	s, c := testStore(t)
	c.WatchInterval, c.WatchGap, c.WatchDailyLimit = 24*time.Hour, 20*time.Second, 300
	f := &watchFixture{store: s, now: time.Date(2030, 1, 2, 10, 0, 0, 0, time.UTC)}
	client := NewClient(s, c)
	client.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/user/getUser" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			return nil, errors.New("unexpected request")
		}
		id, _ := strconv.ParseInt(r.URL.Query().Get("targetUserId"), 10, 64)
		viewer, _ := strconv.ParseInt(r.URL.Query().Get("viewerUserId"), 10, 64)
		owner := viewer / 1000
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", owner) {
			t.Error("request used another owner's session")
		}
		if _, err := s.Profile(r.Context(), owner, id); err != nil {
			t.Error("request used a profile outside its owner's archive")
		}
		f.calls = append(f.calls, id)
		f.owners = append(f.owners, owner)
		return f.respond(r, id)
	})}
	f.watch = NewProfileWatcher(c, s, client)
	f.watch.now = func() time.Time { return f.now }
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		return watchResponse(200, watchProfile(id)), nil
	}
	return f
}

func watchProfile(id int64) Profile {
	return Profile{"userId": id, "name": "Пример", "description": "Исходная анкета", "distance": 10, "photos": []string{"https://mini.jardulik.app/old.jpg"}}
}

func watchResponse(status int, p Profile) *http.Response {
	b, _ := json.Marshal(p)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(b)))}
}

func (f *watchFixture) save(t *testing.T, owner, id int64, age time.Duration) int64 {
	t.Helper()
	ctx := context.Background()
	if err := f.store.User(ctx, owner, "watch-test"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SaveSession(ctx, owner, Session{Token: fmt.Sprintf("token-%d", owner), Refresh: "test-refresh", UserID: owner * 1000}); err != nil {
		t.Fatal(err)
	}
	versions, err := f.store.Archive(ctx, owner, []Profile{watchProfile(id)}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.DB.ExecContext(ctx, "UPDATE versions SET first_seen=?,last_seen=? WHERE id=?", f.now.Add(-age).Format("2006-01-02 15:04:05"), f.now.Add(-age).Format("2006-01-02 15:04:05"), versions[0]); err != nil {
		t.Fatal(err)
	}
	return versions[0]
}

func (f *watchFixture) check(t *testing.T, want bool) {
	t.Helper()
	got, err := f.watch.CheckOne(context.Background())
	if err != nil || got != want {
		t.Fatalf("CheckOne() = %v, %v; want %v", got, err, want)
	}
}

func watchCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestProfileWatcherWaitsForLastSeenAndIgnoresViewerFields(t *testing.T) {
	f := newWatchFixture(t)
	version := f.save(t, 1, 90, 0)
	f.check(t, false)
	if len(f.calls) != 0 {
		t.Fatal("fresh profiles caused immediate HTTP requests")
	}
	f.now = f.now.Add(24 * time.Hour)
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		p := watchProfile(id)
		p["distance"], p["reactionView"], p["isFire"], p["fireText"] = 999, "changed", true, "viewer-only"
		p["photos"] = `["https://mini.jardulik.app/old.jpg"]`
		return watchResponse(200, p), nil
	}
	before, _ := f.store.Version(context.Background(), 1, version)
	f.check(t, true)
	after, _ := f.store.Version(context.Background(), 1, version)
	if len(f.calls) != 1 || watchCount(t, f.store, "versions") != 1 || watchCount(t, f.store, "observations") != 1 || before.LastSeen != after.LastSeen {
		t.Fatal("unchanged check created a version, observation or artificial capture date")
	}
	var result string
	if err := f.store.DB.QueryRow("SELECT last_result FROM profile_watch").Scan(&result); err != nil || result != "unchanged" {
		t.Fatalf("unchanged result not recorded: %q, %v", result, err)
	}
	f.now = f.now.Add(20 * time.Second)
	f.check(t, false)
}

func TestProfileWatcherArchivesChangedContentAndPhotoSources(t *testing.T) {
	for _, field := range []string{"description", "photos"} {
		t.Run(field, func(t *testing.T) {
			f := newWatchFixture(t)
			old := f.save(t, 1, 90, 48*time.Hour)
			f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
				p := watchProfile(id)
				if field == "photos" {
					p[field] = []string{"https://mini.jardulik.app/new.jpg", "https://mini.jardulik.app/second.webp"}
				} else {
					p[field] = "Новое описание"
				}
				return watchResponse(200, p), nil
			}
			f.check(t, true)
			current, err := f.store.Profile(context.Background(), 1, 90)
			if err != nil || current.VersionID == old || watchCount(t, f.store, "versions") != 2 || watchCount(t, f.store, "observations") != 2 {
				t.Fatalf("changed profile was not archived as a new version: %+v, %v", current, err)
			}
			original, err := f.store.Version(context.Background(), 1, old)
			if err != nil || original.Profile.String("description") != "Исходная анкета" || original.Profile.Photos("photos")[0] != "https://mini.jardulik.app/old.jpg" {
				t.Fatal("old profile version was overwritten")
			}
			jobs, err := f.store.MediaJobs(context.Background(), 1, current.VersionID)
			wantJobs := 1
			if field == "photos" {
				wantJobs = 2
			}
			if err != nil || len(jobs) != wantJobs || jobs[0].State != "pending" {
				t.Fatalf("new photo jobs not queued: %+v, %v", jobs, err)
			}
			if field == "description" && current.Profile.String(field) != "Новое описание" {
				t.Fatal("raw response fields were lost")
			}
		})
	}
}

func TestProfileWatcherFairOwnersAndInactiveAccounts(t *testing.T) {
	f := newWatchFixture(t)
	f.save(t, 1, 90, 48*time.Hour)
	f.save(t, 1, 91, 48*time.Hour)
	f.save(t, 2, 90, 48*time.Hour)
	f.save(t, 3, 93, 48*time.Hour)
	f.save(t, 4, 94, 48*time.Hour)
	f.save(t, 5, 95, 48*time.Hour)
	f.store.Pause(context.Background(), 3, true)
	f.store.SessionState(context.Background(), 4, "reconnect_required", "test")
	f.store.SessionState(context.Background(), 5, "paused", "test")
	f.check(t, true)
	f.now = f.now.Add(20 * time.Second)
	f.check(t, true)
	f.now = f.now.Add(20 * time.Second)
	f.check(t, true)
	f.now = f.now.Add(20 * time.Second)
	f.check(t, false)
	if fmt.Sprint(f.owners) != "[1 2 1]" || fmt.Sprint(f.calls) != "[90 90 91]" {
		t.Fatalf("unfair or inactive owner requests: owners=%v profiles=%v", f.owners, f.calls)
	}
}

func TestProfileWatcherDropsDisconnectedAndDeletedResponses(t *testing.T) {
	for _, mode := range []string{"disconnect", "reconnect", "delete"} {
		t.Run(mode, func(t *testing.T) {
			f := newWatchFixture(t)
			f.save(t, 1, 90, 48*time.Hour)
			f.respond = func(r *http.Request, id int64) (*http.Response, error) {
				var err error
				if mode == "delete" {
					err = f.store.DeleteUser(r.Context(), 1)
				} else {
					err = f.store.Disconnect(r.Context(), 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				if mode == "reconnect" {
					if err = f.store.SaveSession(r.Context(), 1, Session{Token: "new-token", Refresh: "new-refresh", UserID: 1000}); err != nil {
						t.Fatal(err)
					}
				}
				p := watchProfile(id)
				p["description"] = "Must not be saved"
				return watchResponse(200, p), nil
			}
			f.check(t, true)
			want := 1
			if mode == "delete" {
				want = 0
			}
			if watchCount(t, f.store, "versions") != want || watchCount(t, f.store, "profile_watch") != 0 {
				t.Fatal("response survived disconnect or recreated erased profile data")
			}
		})
	}
}

func TestProfileWatcherGlobalGapQuotaAndMoscowDay(t *testing.T) {
	f := newWatchFixture(t)
	f.watch.Config.WatchDailyLimit = 1
	f.now = time.Date(2030, 1, 2, 20, 59, 30, 0, time.UTC)
	f.save(t, 1, 90, 48*time.Hour)
	f.save(t, 2, 91, 48*time.Hour)
	f.check(t, true)
	f.now = f.now.Add(19 * time.Second)
	f.check(t, false)
	f.now = f.now.Add(time.Second)
	f.check(t, false)
	f.now = f.now.Add(10 * time.Second)
	f.check(t, true)
	if len(f.calls) != 2 {
		t.Fatal("daily budget did not reset at Moscow midnight")
	}
	var used int
	var day string
	if err := f.store.DB.QueryRow("SELECT used,day FROM profile_watch_budget").Scan(&used, &day); err != nil || used != 1 || day != "2030-01-03" {
		t.Fatalf("invalid durable budget: %d %q %v", used, day, err)
	}
}

func TestProfileWatcherGlobalGapWithoutQuota(t *testing.T) {
	for _, code := range []int{200, 500, 403} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			f := newWatchFixture(t)
			f.save(t, 1, 90, 48*time.Hour)
			f.save(t, 2, 91, 48*time.Hour)
			f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
				f.now = f.now.Add(30 * time.Second)
				status := 200
				if len(f.calls) == 1 {
					status = code
				}
				return watchResponse(status, watchProfile(id)), nil
			}
			if worked, err := f.watch.CheckOne(context.Background()); !worked || (err != nil) != (code != 200) {
				t.Fatalf("first check: %v %v", worked, err)
			}
			f.check(t, false)
			f.now = f.now.Add(19 * time.Second)
			f.check(t, false)
			f.now = f.now.Add(time.Second)
			f.check(t, true)
		})
	}
}

func TestProfileWatcherErrorBackoffAndPausedChallenge(t *testing.T) {
	for _, code := range []int{500, 403, 429} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			f := newWatchFixture(t)
			f.save(t, 1, 90, 48*time.Hour)
			f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
				response := watchResponse(code, watchProfile(id))
				response.Header.Set("Retry-After", "172800")
				return response, nil
			}
			worked, err := f.watch.CheckOne(context.Background())
			if !worked || err == nil || watchCount(t, f.store, "versions") != 1 {
				t.Fatalf("request failure was not preserved: %v %v", worked, err)
			}
			var next int64
			if err = f.store.DB.QueryRow("SELECT next_check FROM profile_watch").Scan(&next); err != nil || next < f.now.Add(48*time.Hour).UnixMilli() {
				t.Fatal("error caused an immediate retry")
			}
			f.now = f.now.Add(time.Hour)
			f.check(t, false)
			if code != 500 {
				f.now = f.now.Add(48 * time.Hour)
				f.check(t, false)
				session, err := f.store.Session(context.Background(), 1)
				if err != nil || session.State != "paused" {
					t.Fatal("challenge did not remain paused")
				}
			}
			if len(f.calls) != 1 {
				t.Fatal("failure or challenge caused automatic retry")
			}
		})
	}
}

func TestProfileWatcherCancellation(t *testing.T) {
	f := newWatchFixture(t)
	f.save(t, 1, 90, 48*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if worked, err := f.watch.CheckOne(ctx); worked || !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatalf("cancelled check made a request: %v %v", worked, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	f.respond = func(r *http.Request, _ int64) (*http.Response, error) {
		cancel()
		return nil, r.Context().Err()
	}
	if worked, err := f.watch.CheckOne(ctx); !worked || !errors.Is(err, context.Canceled) || watchCount(t, f.store, "versions") != 1 {
		t.Fatalf("in-flight cancellation was ignored: %v %v", worked, err)
	}
	done := make(chan struct{})
	go func() { f.watch.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop after cancellation")
	}
}
