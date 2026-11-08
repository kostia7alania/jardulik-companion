package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type locationSource struct {
	catalog func(*http.Request) (string, int)
	full    func(*http.Request) (string, int)
	before  func(http.ResponseWriter, *http.Request) bool
	calls   []string
	targets []int64
}

func locationFixture(t *testing.T) (*FeedCrawler, *time.Time, *locationSource) {
	t.Helper()
	crawler, now, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
	source := &locationSource{}
	base := crawler.Service.Client.HTTP.Transport
	crawler.Service.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("collection attempted mutation: %s", r.URL)
		}
		source.calls = append(source.calls, r.URL.Path)
		if source.before != nil && source.before(w, r) {
			return
		}
		var body string
		status := http.StatusOK
		switch r.URL.Path {
		case "/user/getUsersLocations":
			q := r.URL.Query()
			if q.Get("userId") != "101" || q.Get("pageSize") != "20" || len(q) != 3 {
				t.Fatalf("unexpected catalog query: %s", r.URL)
			}
			for _, key := range []string{"country", "city"} {
				if _, ok := q[key]; ok {
					t.Fatalf("catalog scope changed: %s", r.URL)
				}
			}
			body = "[]"
			if source.catalog != nil {
				body, status = source.catalog(r)
			}
		case "/user/getUser":
			q := r.URL.Query()
			if q.Get("viewerUserId") != "101" || len(q) != 2 {
				t.Fatalf("unexpected full-profile query: %s", r.URL)
			}
			target, err := strconv.ParseInt(q.Get("targetUserId"), 10, 64)
			if err != nil || target <= 0 {
				t.Fatalf("invalid target: %s", r.URL)
			}
			source.targets = append(source.targets, target)
			body = fmt.Sprintf(`{"userId":%d,"name":"Full","description":"complete source card","photos":["https://mini.jardulik.app/%d.webp"]}`, target, target)
			if source.full != nil {
				body, status = source.full(r)
			}
		default:
			response, err := base.RoundTrip(r)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			w.WriteHeader(response.StatusCode)
			var value any
			if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(value)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	})}
	return crawler, now, source
}

func TestLocationsFrozenOmitsOnlyEmptyGeography(t *testing.T) {
	for _, geography := range [][2]string{{"", ""}, {"Russia", ""}, {"", "Vladikavkaz"}, {"Russia", "Vladikavkaz"}} {
		t.Run(geography[0]+"/"+geography[1], func(t *testing.T) {
			crawler, _, source := locationFixture(t)
			calls := 0
			source.before = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/user/getUsersLocations" {
					return false
				}
				calls++
				query := r.URL.Query()
				wantKeys := 3
				for i, key := range []string{"country", "city"} {
					values, exists := query[key]
					if geography[i] == "" {
						if exists {
							t.Fatalf("empty %s was sent to the source", key)
						}
					} else {
						wantKeys++
						if !exists || len(values) != 1 || values[0] != geography[i] {
							t.Fatalf("nonempty %s changed: %v", key, values)
						}
					}
				}
				if len(query) != wantKeys || query.Get("userId") != "101" || query.Get("pageNumber") != "2" || query.Get("pageSize") != "20" {
					t.Fatalf("unexpected geography request: %s", r.URL)
				}
				fmt.Fprint(w, "[]")
				return true
			}
			page, err := crawler.Service.Client.LocationsFrozen(context.Background(), 1, Session{UserID: 101, Generation: "1"}, geography[0], geography[1], 2)
			if err != nil || calls != 1 || page.Country != geography[0] || page.City != geography[1] {
				t.Fatalf("geography request failed: %+v %v", page, err)
			}
		})
	}
}

func TestLocationsFrozenStopsFreshlyFrozenAccount(t *testing.T) {
	crawler, _, source := locationFixture(t)
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/user/getMe" {
			fmt.Fprint(w, `{"userId":101,"type":1,"freezeState":true}`)
			return true
		}
		return false
	}
	_, err := crawler.Service.Client.LocationsFrozen(context.Background(), 1, Session{UserID: 101, Generation: "1"}, "", "", 1)
	if !errors.Is(err, ErrAccountFrozen) || len(source.calls) != 1 || source.calls[0] != "/user/getMe" {
		t.Fatalf("frozen source account reached catalogue: %v %v", source.calls, err)
	}
}

func locationBody(total int, ids ...int64) string {
	rows := make([]string, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, fmt.Sprintf(`{ "userId": %d, "name": "Preview", "miniPhotos": "preview.webp", "totalCount": %d, "pageCount": 20 }`, id, total))
	}
	return " \n[" + strings.Join(rows, ", \n") + "]\t"
}

func startLocationPhase(t *testing.T, crawler *FeedCrawler, now *time.Time) {
	t.Helper()
	if _, err := crawler.Request(context.Background(), 1, true, true); err != nil {
		t.Fatal(err)
	}
	crawlStep(t, crawler, now)
	crawlStep(t, crawler, now)
	if job := crawlJob(t, crawler); job.Mode != 2 || !job.IncludeLocations {
		t.Fatalf("catalog phase not reached: %+v", job)
	}
}

func TestCrawlLocationsDurableCatalogThenDeduplicatedHydration(t *testing.T) {
	crawler, now, source := locationFixture(t)
	first := make([]int64, 20)
	for i := range first {
		first[i] = int64(201 + i)
	}
	rawFirst := locationBody(21, first...)
	source.catalog = func(r *http.Request) (string, int) {
		switch r.URL.Query().Get("pageNumber") {
		case "1":
			return rawFirst, 200
		case "2":
			return locationBody(21, 220, 221), 200
		default:
			t.Fatalf("pageCount=20 used as number of pages: %s", r.URL)
			return "[]", 200
		}
	}
	source.full = func(r *http.Request) (string, int) {
		id := number(r.URL.Query().Get("targetUserId"))
		if id == 201 {
			return `{"error":"not found"}`, 404
		}
		if id == 202 {
			return `{"error":"gone"}`, 410
		}
		return fmt.Sprintf(`{"userId":%d,"description":"full card","photos":["https://mini.jardulik.app/%d.webp"]}`, id, id), 200
	}
	startLocationPhase(t, crawler, now)
	crawlStep(t, crawler, now)
	job := crawlJob(t, crawler)
	if job.Mode != 2 || job.Page != 2 || job.CatalogProfiles != 20 || job.Pages != 0 || job.Received != 0 {
		t.Fatalf("mixed feed/catalog counters: %+v", job)
	}
	var raw, run, generation, country, city string
	var upstream, pageSize int64
	if err := crawler.Service.Store.DB.QueryRow(`SELECT raw_json,run_id,generation,upstream_id,country,city,page_size FROM location_batches WHERE owner_id=1 AND source_page=1`).Scan(&raw, &run, &generation, &upstream, &country, &city, &pageSize); err != nil {
		t.Fatal(err)
	}
	if raw != rawFirst || run != job.RunID || generation != job.Generation || upstream != job.UserID || country != "" || city != "" || pageSize != 20 {
		t.Fatal("exact raw batch/provenance not preserved")
	}
	if counts := crawler.Service.Store.Counts(context.Background(), 1); counts["profiles"] != 0 || counts["versions"] != 0 || counts["pending_photos"] != 0 {
		t.Fatalf("partial map previews published as profiles: %v", counts)
	}
	if err := crawler.Service.Store.KnownProfile(context.Background(), 1, 201); err != nil {
		t.Fatal("catalog evidence not usable", err)
	}
	calls := len(source.calls)
	if _, err := crawler.Service.Client.FullProfile(context.Background(), 2, 201); err == nil || len(source.calls) != calls {
		t.Fatal("another owner used catalog provenance")
	}
	if _, err := crawler.Service.Client.FullProfileFrozen(context.Background(), 1, Session{UserID: 101, Generation: "1"}, 99999); err == nil || len(source.calls) != calls {
		t.Fatal("guessed ID reached HTTP")
	}
	crawlStep(t, crawler, now)
	if job = crawlJob(t, crawler); job.Mode != 3 || job.CatalogPages != 2 || job.CatalogReceived != 22 || job.CatalogProfiles != 21 {
		t.Fatalf("wrong catalog completion: %+v", job)
	}
	for range 21 {
		crawlStep(t, crawler, now)
	}
	job = crawlJob(t, crawler)
	if job.State != "done" || job.Reason != "empty_page" || job.Mode != 3 || job.Hydrated != 19 || job.Unavailable != 2 || job.Pages != 0 || job.Received != 0 {
		t.Fatalf("wrong hydration completion: %+v", job)
	}
	if len(source.targets) != 21 {
		t.Fatalf("catalog overlap repeated full-profile calls: %v", source.targets)
	}
	for i, id := range source.targets {
		if id != int64(201+i) {
			t.Fatal("hydration used guessed/repeated ID")
		}
	}
	var pending, viewed, seen, actions int
	db := crawler.Service.Store.DB
	if err := db.QueryRow("SELECT COUNT(*) FROM crawl_location_results WHERE state='pending'").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM queue WHERE delivered=1").Scan(&viewed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM crawl_presence_profiles WHERE seen=1").Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || viewed != 19 || seen != 19 || actions != 0 {
		t.Fatalf("partial/unavailable profiles leaked into feed/seen/actions: %d %d %d %d", pending, viewed, seen, actions)
	}
	if handled, err := crawler.Step(context.Background()); handled || err != nil {
		t.Fatal("finished catalog repeated HTTP", err)
	}
}

func TestCrawlLocationsCursorFailureRetainsSourceAndStops(t *testing.T) {
	for _, phase := range []int{2, 3} {
		t.Run(strconv.Itoa(phase), func(t *testing.T) {
			crawler, now, source := locationFixture(t)
			source.catalog = func(*http.Request) (string, int) { return locationBody(1, 301), 200 }
			startLocationPhase(t, crawler, now)
			if phase == 3 {
				crawlStep(t, crawler, now)
			}
			db := crawler.Service.Store.DB
			if _, err := db.Exec(`CREATE TRIGGER fail_collection_cursor BEFORE UPDATE OF page ON feed_crawls BEGIN SELECT RAISE(ABORT,'cursor fault'); END`); err != nil {
				t.Fatal(err)
			}
			crawlStep(t, crawler, now)
			job := crawlJob(t, crawler)
			if job.State != "stopped" || job.Reason != "checkpoint_failed" || job.Page != 1 || job.Hydrated != 0 {
				t.Fatalf("cursor failure advanced/retried: %+v", job)
			}
			var batches, entries, pending, seen int
			if err := db.QueryRow("SELECT COUNT(*) FROM location_batches").Scan(&batches); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM location_profiles").Scan(&entries); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM crawl_location_results WHERE state='pending'").Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM crawl_presence_profiles WHERE seen=1").Scan(&seen); err != nil {
				t.Fatal(err)
			}
			if batches != 1 || entries != 1 || pending != 1 || seen != 0 {
				t.Fatal("source lost or presence/result committed ahead of cursor")
			}
			versions := crawler.Service.Store.Counts(context.Background(), 1)["versions"]
			if (phase == 2 && versions != 0) || (phase == 3 && versions != 1) {
				t.Fatalf("wrong source-before-cursor ordering: %d", versions)
			}
			calls := len(source.calls)
			*now = now.Add(crawlGap)
			if handled, err := crawler.Step(context.Background()); handled || err != nil || len(source.calls) != calls {
				t.Fatal("failed cursor blindly retried HTTP", err)
			}
		})
	}
}

func TestCrawlLocationsRepeatAndCapsDoNotLimitHydrationCount(t *testing.T) {
	t.Run("repeat", func(t *testing.T) {
		crawler, now, source := locationFixture(t)
		source.catalog = func(r *http.Request) (string, int) {
			if r.URL.Query().Get("pageNumber") == "2" {
				return locationBody(100, 12), 200
			}
			return locationBody(100, 11), 200
		}
		startLocationPhase(t, crawler, now)
		for range 3 {
			crawlStep(t, crawler, now)
		}
		job := crawlJob(t, crawler)
		if job.State != "stopped" || job.Reason != "repeated_page" || job.CatalogProfiles != 2 || len(source.targets) != 0 {
			t.Fatalf("catalog repeat loop not stopped: %+v", job)
		}
	})
	t.Run("catalogcap", func(t *testing.T) {
		crawler, now, source := locationFixture(t)
		source.catalog = func(*http.Request) (string, int) { return locationBody(20001, 11), 200 }
		startLocationPhase(t, crawler, now)
		if _, err := crawler.Service.Store.DB.Exec("UPDATE feed_crawls SET page=1000 WHERE owner_id=1"); err != nil {
			t.Fatal(err)
		}
		crawlStep(t, crawler, now)
		if job := crawlJob(t, crawler); job.State != "stopped" || job.Reason != "page_cap" || job.Page != 1001 {
			t.Fatalf("catalog cap not enforced: %+v", job)
		}
	})
	t.Run("hydrationnottablepagecap", func(t *testing.T) {
		crawler, now, source := locationFixture(t)
		source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
		startLocationPhase(t, crawler, now)
		crawlStep(t, crawler, now)
		if _, err := crawler.Service.Store.DB.Exec("UPDATE feed_crawls SET page=1001 WHERE owner_id=1"); err != nil {
			t.Fatal(err)
		}
		crawlStep(t, crawler, now)
		if job := crawlJob(t, crawler); job.State != "done" || job.Hydrated != 1 || job.Page != 1002 {
			t.Fatalf("profile count used as catalog page cap: %+v", job)
		}
	})
}

func TestCrawlLocationsGuardEveryRequestAndArchive(t *testing.T) {
	for _, phase := range []int{2, 3} {
		for _, kind := range []string{"premium", "pause-before", "pause-after", "session-before", "session-after", "403", "429", "400", "challenge", "disk-after", "wrong-id"} {
			if phase == 2 && kind == "wrong-id" {
				continue
			}
			t.Run(fmt.Sprintf("%d/%s", phase, kind), func(t *testing.T) {
				crawler, now, source := locationFixture(t)
				source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
				startLocationPhase(t, crawler, now)
				if phase == 3 {
					crawlStep(t, crawler, now)
				}
				path := "/user/getUsersLocations"
				if phase == 3 {
					path = "/user/getUser"
				}
				targetCalls := 0
				source.before = func(w http.ResponseWriter, r *http.Request) bool {
					if r.URL.Path == "/user/getMe" {
						switch kind {
						case "premium":
							fmt.Fprint(w, `{"userId":101,"type":0}`)
							return true
						case "pause-before":
							crawler.Service.Store.Pause(context.Background(), 1, true)
						case "session-before":
							if err := crawler.Service.Store.SaveSession(context.Background(), 1, Session{Token: "changed", Refresh: "r", UserID: 101, Generation: "changed"}); err != nil {
								t.Fatal(err)
							}
						}
					}
					if r.URL.Path != path {
						return false
					}
					targetCalls++
					switch kind {
					case "pause-after":
						crawler.Service.Store.Pause(context.Background(), 1, true)
					case "session-after":
						if err := crawler.Service.Store.SaveSession(context.Background(), 1, Session{Token: "changed", Refresh: "r", UserID: 101, Generation: "changed"}); err != nil {
							t.Fatal(err)
						}
					case "disk-after":
						crawler.Service.Media.Config.MinFreeBytes = math.MaxUint64
					case "403", "429", "400":
						status, _ := strconv.Atoi(kind)
						w.WriteHeader(status)
						fmt.Fprint(w, `{"error":"fixture"}`)
						return true
					case "challenge":
						w.Header().Set("Content-Type", "text/html")
						fmt.Fprint(w, "<html>challenge</html>")
						return true
					case "wrong-id":
						fmt.Fprint(w, `{"userId":999}`)
						return true
					}
					return false
				}
				crawlStep(t, crawler, now)
				job := crawlJob(t, crawler)
				if job.State != "stopped" || job.Page != 1 || job.Hydrated != 0 || job.Unavailable != 0 {
					t.Fatalf("restriction advanced cursor: %+v", job)
				}
				if counts := crawler.Service.Store.Counts(context.Background(), 1); counts["versions"] != 0 {
					t.Fatal("invalidated response archived")
				}
				if (kind == "premium" || kind == "pause-before" || kind == "session-before") && targetCalls != 0 {
					t.Fatal("target request started after failed preflight")
				}
				reasons := map[string]string{"premium": "premium_required", "pause-before": "paused", "pause-after": "paused", "session-before": "session_changed", "session-after": "session_changed", "403": "http_403", "429": "http_429", "400": "http_400", "challenge": "challenge", "disk-after": "disk_reserve", "wrong-id": "request_failed"}
				if job.Reason != reasons[kind] {
					t.Fatalf("restriction reason hidden: %+v", job)
				}
			})
		}
	}
}

func TestLocationsRejectInvalidDTOAndDoNotInterpretPageCountAsPages(t *testing.T) {
	for _, raw := range []string{"null", `{"items":[]}`, `[null]`, `[{"userId":1}]`, `[{"userId":0,"totalCount":1}]`, `[{"userId":1,"totalCount":-1}]`, `[{"userId":1,"totalCount":1.5}]`, locationBody(1, 1) + "[]"} {
		if _, err := decodeLocations([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid catalog: %s", raw)
		}
	}
	if _, err := decodeLocations([]byte(locationBody(21, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21))); err == nil {
		t.Fatal("oversized page accepted")
	}
	page, err := decodeLocations([]byte(locationBody(839, 987654321)))
	if err != nil || page.Total != 839 || len(page.Entries) != 1 {
		t.Fatalf("valid DTO rejected: %+v %v", page, err)
	}
}

func TestLocationsEmptyCatalogCompletesWithoutHydrationHTTP(t *testing.T) {
	crawler, now, source := locationFixture(t)
	startLocationPhase(t, crawler, now)
	crawlStep(t, crawler, now)
	calls := len(source.calls)
	crawlStep(t, crawler, now)
	if job := crawlJob(t, crawler); job.State != "done" || job.Mode != 3 || job.CatalogProfiles != 0 || len(source.calls) != calls {
		t.Fatalf("empty catalog failed local completion: %+v", job)
	}
}

func TestLocationsKnownProvenanceWorksWithOrdinaryFullProfile(t *testing.T) {
	crawler, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(1, 77), 200 }
	startLocationPhase(t, crawler, now)
	crawlStep(t, crawler, now)
	if err := crawler.Service.Store.KnownProfile(context.Background(), 2, 77); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("catalog tenant guard failed", err)
	}
	profile, err := crawler.Service.Client.FullProfile(context.Background(), 1, 77)
	if err != nil || profile.ID() != 77 {
		t.Fatal("ordinary full profile rejected saved catalog evidence", err)
	}
}

func TestLocationsConcurrentRequestsUseOneSharedRefresh(t *testing.T) {
	crawler, _, _ := locationFixture(t)
	client := crawler.Service.Client
	store := crawler.Service.Store
	ctx := context.Background()
	if err := store.SaveSession(ctx, 1, Session{Token: "expired", Refresh: "refresh-once", UserID: 101, Generation: "g", Expires: time.Now().Add(-time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	var refreshes atomic.Int64
	client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/appuser/refresh":
			refreshes.Add(1)
			fmt.Fprint(w, `{"token":"fresh","refreshToken":"rotated"}`)
		case "/user/getMe":
			fmt.Fprint(w, `{"userId":101,"type":1}`)
		case "/user/getUsersLocations":
			fmt.Fprint(w, "[]")
		default:
			t.Errorf("unexpected request: %s", r.URL)
		}
	})}
	var wait sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := client.LocationsFrozen(ctx, 1, Session{UserID: 101, Generation: "g"}, "", "", 1)
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if refreshes.Load() != 1 {
		t.Fatalf("parallel location reads refreshed %d times", refreshes.Load())
	}
}

func TestCrawlCatalogCrashReplayDoesNotDuplicateRawOrHydration(t *testing.T) {
	crawler, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
	startLocationPhase(t, crawler, now)
	job := crawlJob(t, crawler)
	page, err := decodeLocations([]byte(locationBody(1, 11)))
	if err != nil {
		t.Fatal(err)
	}
	page.Page = 1
	// Simulate a crash after immutable source commit, before its cursor checkpoint.
	if err = crawler.Service.Store.saveCrawlLocations(context.Background(), job, page, *now); err != nil {
		t.Fatal(err)
	}
	cfg, transport := crawler.Service.Config, crawler.Service.Client.HTTP.Transport
	if err = crawler.Service.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.Client.HTTP.Transport = transport
	crawler = restarted.Crawler
	crawler.now = func() time.Time { return *now }
	crawlStep(t, crawler, now)
	crawlStep(t, crawler, now)
	var batches, entries, results int
	if err = restarted.Store.DB.QueryRow("SELECT COUNT(*) FROM location_batches").Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Store.DB.QueryRow("SELECT COUNT(*) FROM location_profiles").Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Store.DB.QueryRow("SELECT COUNT(*) FROM crawl_location_results").Scan(&results); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || entries != 1 || results != 1 || len(source.targets) != 1 || crawlJob(t, crawler).State != "done" {
		t.Fatalf("replay duplicated source/results: %d %d %d %v", batches, entries, results, source.targets)
	}
}

func TestCrawlLaterConfirmedUnavailableDoesNotBecomePresentAtFinish(t *testing.T) {
	crawler, now, source := locationFixture(t)
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/user/getUsers" && r.URL.Query().Get("all") == "0" && r.URL.Query().Get("pageNumber") == "1" {
			fmt.Fprint(w, `[{"userId":11,"description":"full feed card"}]`)
			return true
		}
		return false
	}
	source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
	source.full = func(*http.Request) (string, int) { return `{"error":"gone"}`, 410 }
	if _, err := crawler.Request(context.Background(), 1, true, true); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		crawlStep(t, crawler, now)
	}
	var availability, reason string
	if err := crawler.Service.Store.DB.QueryRow("SELECT availability,reason FROM profile_presence WHERE owner_id=1 AND profile_id=11").Scan(&availability, &reason); err != nil {
		t.Fatal(err)
	}
	if availability != "unavailable" || reason != "direct_unavailable" || crawlJob(t, crawler).State != "done" {
		t.Fatalf("finish resurrected later direct unavailability: %s %s", availability, reason)
	}
}

func TestCrawlLocationsArchiveFailureNeverAdvancesCursor(t *testing.T) {
	crawler, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
	startLocationPhase(t, crawler, now)
	if _, err := crawler.Service.Store.DB.Exec(`CREATE TRIGGER reject_catalog_entry BEFORE INSERT ON location_profiles BEGIN SELECT RAISE(ABORT,'fixture archive failure'); END`); err != nil {
		t.Fatal(err)
	}
	crawlStep(t, crawler, now)
	var batches int
	if err := crawler.Service.Store.DB.QueryRow("SELECT COUNT(*) FROM location_batches").Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if job := crawlJob(t, crawler); job.State != "stopped" || job.Reason != "archive_failed" || job.Page != 1 || batches != 0 {
		t.Fatalf("partial source transaction advanced cursor: %+v batches=%d", job, batches)
	}
}

func TestCrawlEmptyHydrationStillHonorsDiskReserve(t *testing.T) {
	crawler, now, source := locationFixture(t)
	startLocationPhase(t, crawler, now)
	crawlStep(t, crawler, now)
	crawler.Service.Media.Config.MinFreeBytes = math.MaxUint64
	calls := len(source.calls)
	crawlStep(t, crawler, now)
	if job := crawlJob(t, crawler); job.State != "stopped" || job.Reason != "disk_reserve" || len(source.calls) != calls {
		t.Fatalf("empty final phase bypassed disk reserve: %+v", job)
	}
}

func crawlRetryMeta(t *testing.T, c *FeedCrawler, owner int64) *crawlRetry {
	t.Helper()
	var raw string
	err := c.Service.Store.DB.QueryRow("SELECT value FROM meta WHERE key=?", crawlRetryKey(owner)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var retry crawlRetry
	if err = json.Unmarshal([]byte(raw), &retry); err != nil {
		t.Fatal(err)
	}
	return &retry
}

func retryPhaseFixture(t *testing.T, phase int) (*FeedCrawler, *time.Time, *locationSource, string) {
	t.Helper()
	c, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(1, 11), 200 }
	path := "/user/getUsers"
	if phase == 0 {
		if _, err := c.Request(context.Background(), 1, true, true); err != nil {
			t.Fatal(err)
		}
	} else {
		startLocationPhase(t, c, now)
		path = "/user/getUsersLocations"
		if phase == 3 {
			crawlStep(t, c, now)
			path = "/user/getUser"
		}
	}
	return c, now, source, path
}

func TestCrawlTransientGETRetryRestartsSameFrontierAndSuccessClearsBudget(t *testing.T) {
	for _, phase := range []int{0, 2, 3} {
		t.Run(strconv.Itoa(phase), func(t *testing.T) {
			c, now, source, path := retryPhaseFixture(t, phase)
			attempts := 0
			source.before = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != path {
					return false
				}
				attempts++
				if attempts == 1 {
					w.WriteHeader(500)
					fmt.Fprint(w, `{"error":"transient"}`)
					return true
				}
				if phase == 0 {
					_ = json.NewEncoder(w).Encode(crawlProfiles(11))
					return true
				}
				return false
			}
			before := crawlJob(t, c)
			failedAt := *now
			if ok, err := c.Step(context.Background()); err != nil || !ok {
				t.Fatal(err)
			}
			job := crawlJob(t, c)
			if job.State != "queued" || job.Reason != "retry_http_500" || job.Page != before.Page || job.Mode != before.Mode || job.NextRequest != failedAt.Add(time.Minute).UnixMilli() || job.Pages != before.Pages || job.Received != before.Received || job.CatalogReceived != before.CatalogReceived || job.Hydrated != 0 || job.Unavailable != 0 {
				t.Fatalf("5xx advanced source frontier: %+v", job)
			}
			retry := crawlRetryMeta(t, c, 1)
			if retry == nil || retry.Failures != 1 || retry.RunID != job.RunID || retry.Mode != phase || retry.Page != 1 || (phase == 3 && retry.Target != 11) {
				t.Fatalf("retry not persisted by frontier: %+v", retry)
			}
			var versions, seen, finished int
			db := c.Service.Store.DB
			if err := db.QueryRow("SELECT COUNT(*) FROM versions").Scan(&versions); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM crawl_presence_profiles WHERE seen=1").Scan(&seen); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM crawl_presence_runs WHERE finished_at IS NOT NULL").Scan(&finished); err != nil {
				t.Fatal(err)
			}
			if versions != 0 || seen != 0 || finished != 0 {
				t.Fatal("failed read archived profile or marked presence complete")
			}
			status := job.shortStatus()
			if !strings.Contains(status, "по нашему расписанию") || !strings.Contains(status, "UTC+3") || !strings.Contains(status, "ожидает") {
				t.Fatalf("retry time/status missing: %s", status)
			}
			calls := len(source.calls)
			*now = failedAt.Add(time.Minute - time.Millisecond)
			if ok, err := c.Step(context.Background()); err != nil || ok || len(source.calls) != calls {
				t.Fatal("source retried before durable backoff", err)
			}
			cfg, transport := c.Service.Config, c.Service.Client.HTTP.Transport
			if err := c.Service.Close(); err != nil {
				t.Fatal(err)
			}
			service, err := NewService(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			service.Client.HTTP.Transport = transport
			c = service.Crawler
			c.now = func() time.Time { return *now }
			*now = failedAt.Add(time.Minute)
			if ok, err := c.Step(context.Background()); err != nil || !ok {
				t.Fatal("due retry failed after restart", err)
			}
			if attempts != 2 || crawlRetryMeta(t, c, 1) != nil {
				t.Fatal("retry budget survived successful checkpoint")
			}
			job = crawlJob(t, c)
			if phase == 0 && (job.Page != 2 || job.Pages != 1) || phase == 2 && job.Mode != 3 || phase == 3 && (job.State != "done" || job.Hydrated != 1) {
				t.Fatalf("successful retry did not progress: %+v", job)
			}
		})
	}
}

func TestCrawlTransientGETStopsOnThirdFailure(t *testing.T) {
	for _, phase := range []int{0, 2, 3} {
		t.Run(strconv.Itoa(phase), func(t *testing.T) {
			c, now, source, path := retryPhaseFixture(t, phase)
			attempts := 0
			source.before = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != path {
					return false
				}
				attempts++
				w.WriteHeader(503)
				fmt.Fprint(w, `{"error":"transient"}`)
				return true
			}
			for attempt := 1; attempt <= 3; attempt++ {
				failedAt := *now
				if ok, err := c.Step(context.Background()); err != nil || !ok {
					t.Fatal(err)
				}
				job := crawlJob(t, c)
				retry := crawlRetryMeta(t, c, 1)
				if retry == nil || retry.Failures != attempt || job.Page != 1 || job.Hydrated != 0 || job.Unavailable != 0 {
					t.Fatalf("retry advanced or reset budget: %+v %+v", job, retry)
				}
				if attempt < 3 {
					delay := time.Minute
					if attempt == 2 {
						delay = 5 * time.Minute
					}
					if job.State != "queued" || job.NextRequest != failedAt.Add(delay).UnixMilli() {
						t.Fatalf("wrong backoff: %+v", job)
					}
					*now = time.UnixMilli(job.NextRequest)
				} else if job.State != "stopped" || job.Reason != "http_503" {
					t.Fatalf("third failed GET did not stop: %+v", job)
				}
			}
			*now = now.Add(24 * time.Hour)
			if ok, err := c.Step(context.Background()); err != nil || ok || attempts != 3 {
				t.Fatal("terminal retries looped", err)
			}
		})
	}
}

func TestCrawlRetryAfterAndOwnerIsolationPreserveGlobalGap(t *testing.T) {
	c, now, source, path := retryPhaseFixture(t, 0)
	if _, err := c.Request(context.Background(), 2, false); err != nil {
		t.Fatal(err)
	}
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != path {
			return false
		}
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(502)
		fmt.Fprint(w, `{"error":"transient"}`)
		return true
	}
	failedAt := *now
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	if job := crawlJob(t, c); job.NextRequest != failedAt.Add(10*time.Minute).UnixMilli() {
		t.Fatalf("Retry-After ignored: %+v", job)
	}
	if ok, err := c.Step(context.Background()); err != nil || ok {
		t.Fatal("global 20s gap bypassed", err)
	}
	*now = failedAt.Add(crawlGap)
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal("owner backoff incorrectly blocked other owner", err)
	}
	a, b := crawlRetryMeta(t, c, 1), crawlRetryMeta(t, c, 2)
	if a == nil || b == nil || a.Failures != 1 || b.Failures != 1 || a.RunID == b.RunID {
		t.Fatalf("owners shared retry budget: %+v %+v", a, b)
	}
}

func TestCrawlRetryBudgetIsPerHydratedTarget(t *testing.T) {
	c, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(2, 11, 12), 200 }
	startLocationPhase(t, c, now)
	crawlStep(t, c, now)
	attempts := map[string]int{}
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/user/getUser" {
			return false
		}
		id := r.URL.Query().Get("targetUserId")
		attempts[id]++
		if attempts[id] == 1 {
			w.WriteHeader(504)
			fmt.Fprint(w, `{"error":"transient"}`)
			return true
		}
		return false
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	first := crawlRetryMeta(t, c, 1)
	if first == nil || first.Target != 11 || first.Failures != 1 {
		t.Fatal("missing first target retry")
	}
	*now = time.UnixMilli(crawlJob(t, c).NextRequest)
	crawlStep(t, c, now)
	if crawlRetryMeta(t, c, 1) != nil {
		t.Fatal("successful target left stale retries")
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	second := crawlRetryMeta(t, c, 1)
	if second == nil || second.Target != 12 || second.Page != 2 || second.Failures != 1 {
		t.Fatalf("different target inherited retry budget: %+v", second)
	}
}

func TestCrawlRetriesMe500ButNeverRefreshPUT500(t *testing.T) {
	t.Run("me", func(t *testing.T) {
		c, now, source, _ := retryPhaseFixture(t, 0)
		calls := 0
		source.before = func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path != "/user/getMe" {
				return false
			}
			calls++
			if calls == 1 {
				w.WriteHeader(500)
				fmt.Fprint(w, `{"error":"transient"}`)
				return true
			}
			return false
		}
		if ok, err := c.Step(context.Background()); err != nil || !ok {
			t.Fatal(err)
		}
		if len(source.calls) != 1 || crawlJob(t, c).State != "queued" {
			t.Fatal("Me failure continued to source page")
		}
		*now = time.UnixMilli(crawlJob(t, c).NextRequest)
		if ok, err := c.Step(context.Background()); err != nil || !ok || calls != 2 {
			t.Fatal("Me retry did not recover", err)
		}
	})
	t.Run("refresh", func(t *testing.T) {
		c, now, _, _ := retryPhaseFixture(t, 0)
		if err := c.Service.Store.SaveSession(context.Background(), 1, Session{Token: "expired", Refresh: "fake-refresh", UserID: 101, Generation: "1", Expires: time.Now().Add(-time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
		calls := 0
		c.Service.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.Method != "PUT" || r.URL.Path != "/appuser/refresh" {
				t.Fatalf("unexpected auth request: %s %s", r.Method, r.URL)
			}
			w.WriteHeader(500)
			fmt.Fprint(w, `{"error":"auth unavailable"}`)
		})}
		if ok, err := c.Step(context.Background()); err != nil || !ok {
			t.Fatal(err)
		}
		if job := crawlJob(t, c); job.State != "stopped" || job.Reason != "auth_required" || crawlRetryMeta(t, c, 1) != nil {
			t.Fatalf("refresh replay scheduled: %+v", job)
		}
		*now = now.Add(10 * time.Minute)
		if ok, err := c.Step(context.Background()); err != nil || ok || calls != 1 {
			t.Fatal("auth mutation retried", err)
		}
	})
}

func TestCrawlRetryPersistenceFailureStopsWithoutMoreHTTP(t *testing.T) {
	c, now, source, path := retryPhaseFixture(t, 0)
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != path {
			return false
		}
		w.WriteHeader(500)
		fmt.Fprint(w, `{"error":"transient"}`)
		return true
	}
	if _, err := c.Service.Store.DB.Exec(`CREATE TRIGGER fail_crawl_retry BEFORE INSERT ON meta WHEN NEW.key LIKE 'feed_crawl_retry:%' BEGIN SELECT RAISE(ABORT,'retry checkpoint fault'); END`); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); !ok || err == nil {
		t.Fatal("retry persistence failure not returned")
	}
	if job := crawlJob(t, c); job.State != "stopped" || job.Reason != "checkpoint_failed" {
		t.Fatalf("failed retry bookmark left job active: %+v", job)
	}
	calls := len(source.calls)
	*now = now.Add(10 * time.Minute)
	if ok, err := c.Step(context.Background()); err != nil || ok || len(source.calls) != calls {
		t.Fatal("failed retry persistence repeated HTTP", err)
	}
}

func TestCrawlRetryClassificationIsGETOnlyAndStrict(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504} {
		for _, endpoint := range []string{"/user/getMe", "/user/getUsers", "/user/getUsersLocations", "/user/getUser"} {
			if _, ok := crawlRetryableGET(&UpstreamError{Status: status, Kind: "request_rejected", Endpoint: endpoint}); !ok {
				t.Fatalf("valid transient GET rejected: %d %s", status, endpoint)
			}
		}
	}
	for _, err := range []error{&UpstreamError{Status: 500, Kind: "request_rejected", Endpoint: "/appuser/refresh"}, &UpstreamError{Status: 500, Kind: "request_rejected", Endpoint: "/reactions/addNew"}, &UpstreamError{Status: 500, Kind: "unexpected_response_or_challenge", Endpoint: "/user/getUsers"}, errors.New("format changed"), ErrProfileUnavailable} {
		if _, ok := crawlRetryableGET(err); ok {
			t.Fatalf("non-GET/challenge/format error retried: %v", err)
		}
	}
	for _, status := range []int{400, 401, 403, 404, 408, 410, 429, 501, 505} {
		if _, ok := crawlRetryableGET(&UpstreamError{Status: status, Kind: "request_rejected", Endpoint: "/user/getUsers"}); ok {
			t.Fatalf("non-transient response retried: %d", status)
		}
	}
}

func TestCrawlQueuedRetryPauseAndNewRequestAreExplicit(t *testing.T) {
	c, now, source, path := retryPhaseFixture(t, 0)
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != path {
			return false
		}
		w.WriteHeader(500)
		fmt.Fprint(w, `{"error":"transient"}`)
		return true
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	old := crawlJob(t, c)
	uxSend(c.Service, 1, "/pause")
	uxSend(c.Service, 1, "/resume")
	*now = time.UnixMilli(old.NextRequest)
	calls := len(source.calls)
	if ok, err := c.Step(context.Background()); err != nil || ok || len(source.calls) != calls {
		t.Fatal("resume restarted queued retry", err)
	}
	if job := crawlJob(t, c); job.State != "stopped" || job.Reason != "paused" {
		t.Fatalf("pause lost terminal meaning: %+v", job)
	}
	next, err := c.Request(context.Background(), 1, true, true)
	if err != nil || next.RunID == old.RunID || crawlRetryMeta(t, c, 1) != nil {
		t.Fatal("explicit new request did not clear old retry frontier", err)
	}
}

func TestCrawlRetryLifecycleRaceDoesNotStopPeerOwner(t *testing.T) {
	c, now, source, path := retryPhaseFixture(t, 0)
	if _, err := c.Request(context.Background(), 2, false); err != nil {
		t.Fatal(err)
	}
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != path {
			return false
		}
		if r.URL.Query().Get("userId") == "101" {
			w.WriteHeader(500)
			fmt.Fprint(w, `{"error":"transient"}`)
		} else {
			_ = json.NewEncoder(w).Encode(crawlProfiles(22))
		}
		return true
	}
	// The first no-op is post-HTTP invalid(); the second is retryGET's writer lock.
	_, err := c.Service.Store.DB.Exec(`INSERT INTO meta(key,value)VALUES('fixture_pause_counter','0');
 CREATE TRIGGER pause_at_retry BEFORE UPDATE ON meta
 WHEN NEW.key='feed_crawl_next' AND NEW.value=OLD.value AND EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=1 AND state='running')
 BEGIN
 UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='fixture_pause_counter';
 UPDATE users SET paused=1 WHERE id=1 AND (SELECT CAST(value AS INTEGER) FROM meta WHERE key='fixture_pause_counter')=2;
 END;`)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := c.Step(context.Background()); err != nil || !handled {
		t.Fatal("lifecycle race stopped the global worker", err)
	}
	if job := crawlJob(t, c); job.State == "queued" || job.Reason == "checkpoint_failed" || crawlRetryMeta(t, c, 1) != nil {
		t.Fatalf("obsolete retry requeued/stopped the owner: %+v", job)
	}
	// SQLite rolls back the injected pause with the abandoned retry transaction.
	// A real pause commits separately; ensure the next Step still serves the peer.
	*now = now.Add(crawlGap)
	if handled, err := c.Step(context.Background()); err != nil || !handled {
		t.Fatal("peer owner lost its worker", err)
	}
	peer, err := c.Service.Store.CrawlStatus(context.Background(), 2)
	if err != nil || peer.Page != 2 || peer.Pages != 1 {
		t.Fatalf("peer did not progress after obsolete retry: %+v %v", peer, err)
	}
}

func TestCrawlRetryAbandonsChangedOrDeletedLifecycle(t *testing.T) {
	for _, kind := range []string{"paused", "stopped", "newrun", "mode", "page", "sessionstate", "sessiongeneration", "disconnect", "deletejob", "deleteuser"} {
		t.Run(kind, func(t *testing.T) {
			c, _, _, _ := retryPhaseFixture(t, 0)
			ctx := context.Background()
			job := crawlJob(t, c)
			store := c.Service.Store
			var err error
			switch kind {
			case "paused":
				store.Pause(ctx, 1, true)
			case "stopped":
				err = store.StopCrawl(ctx, 1, "paused")
			case "newrun":
				_, err = store.DB.Exec("UPDATE feed_crawls SET run_id='new-run' WHERE owner_id=1")
			case "mode":
				_, err = store.DB.Exec("UPDATE feed_crawls SET mode=1 WHERE owner_id=1")
			case "page":
				_, err = store.DB.Exec("UPDATE feed_crawls SET page=2 WHERE owner_id=1")
			case "sessionstate":
				store.SessionState(ctx, 1, "reconnect_required", "fixture")
			case "sessiongeneration":
				err = store.SaveSession(ctx, 1, Session{Token: "new", Refresh: "r", UserID: 101, Generation: "new"})
			case "disconnect":
				err = store.Disconnect(ctx, 1)
			case "deletejob":
				_, err = store.DB.Exec("DELETE FROM feed_crawls WHERE owner_id=1")
			case "deleteuser":
				err = store.DeleteUser(ctx, 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			var before string
			scanErr := store.DB.QueryRow("SELECT run_id||':'||state||':'||reason FROM feed_crawls WHERE owner_id=1").Scan(&before)
			if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
				t.Fatal(scanErr)
			}
			err = c.retryGET(ctx, job, 0, &UpstreamError{Status: 500, Kind: "request_rejected", Endpoint: "/user/getUsers"})
			if !errors.Is(err, errCrawlRetryObsolete) {
				t.Fatal("normal lifecycle change was treated as a worker failure", err)
			}
			if crawlRetryMeta(t, c, 1) != nil {
				t.Fatal("obsolete lifecycle created a retry bookmark")
			}
			var after string
			afterErr := store.DB.QueryRow("SELECT run_id||':'||state||':'||reason FROM feed_crawls WHERE owner_id=1").Scan(&after)
			if before != after || !errors.Is(afterErr, scanErr) {
				t.Fatal("obsolete retry changed lifecycle state", before, after, afterErr)
			}
		})
	}
}

func TestCrawlCorruptRetryCheckpointRemainsFatal(t *testing.T) {
	c, _, source, path := retryPhaseFixture(t, 0)
	source.before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != path {
			return false
		}
		w.WriteHeader(500)
		fmt.Fprint(w, `{"error":"transient"}`)
		return true
	}
	if _, err := c.Service.Store.DB.Exec("INSERT INTO meta(key,value)VALUES(?,?)", crawlRetryKey(1), `{"failures":"corrupt"}`); err != nil {
		t.Fatal(err)
	}
	if handled, err := c.Step(context.Background()); !handled || err == nil || errors.Is(err, errCrawlRetryObsolete) {
		t.Fatal("corrupt retry data was treated as benign", err)
	}
	if job := crawlJob(t, c); job.State != "stopped" || job.Reason != "checkpoint_failed" {
		t.Fatalf("corrupt retry data did not stop the owner: %+v", job)
	}
}
