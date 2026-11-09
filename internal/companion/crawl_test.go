package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type crawlCall struct{ Mode, Page int }

func crawlProfiles(ids ...int64) []Profile {
	out := make([]Profile, 0, len(ids))
	for _, id := range ids {
		out = append(out, Profile{"userId": id, "name": fmt.Sprintf("P%d", id), "description": "full feed card", "photos": []any{fmt.Sprintf("https://mini.jardulik.app/%d.webp", id)}})
	}
	return out
}

func crawlFixture(t *testing.T, response func(mode, page int) ([]Profile, int)) (*FeedCrawler, *time.Time, *[]crawlCall, *int) {
	t.Helper()
	s, _, _, _ := filterFixture(t)
	c := NewFeedCrawler(s)
	now := time.Now().Truncate(time.Millisecond)
	c.now = func() time.Time { return now }
	var calls []crawlCall
	httpCalls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls++
		if r.Method != "GET" {
			t.Fatalf("crawl attempted mutation: %s %s", r.Method, r.URL)
		}
		switch r.URL.Path {
		case "/user/getMe":
			fmt.Fprintf(w, `{"userId":%s,"type":1,"minAge":18,"maxAge":25}`, r.URL.Query().Get("userId"))
		case "/user/getUsers":
			q := r.URL.Query()
			var unfiltered bool
			upstream, _ := strconv.ParseInt(q.Get("userId"), 10, 64)
			requestService, _ := r.Context().Value(crawlPriorityKey{}).(*Service)
			if requestService == nil {
				t.Fatal("crawl request lost its worker context")
			}
			if err := requestService.Store.DB.QueryRow("SELECT unfiltered FROM feed_crawls WHERE owner_id=?", upstream-100).Scan(&unfiltered); err != nil {
				t.Fatal(err)
			}
			if unfiltered {
				if len(q) != 6 || q.Get("pageSize") != "150" || q.Get("ageMin") != "0" || q.Get("ageMax") != "111" {
					t.Fatalf("broad crawl sent personal filters: %s", r.URL)
				}
				for _, key := range []string{"heightMin", "heightMax", "goal", "children", "worldview", "country", "city"} {
					if _, ok := q[key]; ok {
						t.Fatalf("unexpected %s query: %v", key, q)
					}
				}
			} else if q.Get("ageMin") != "0" || q.Get("ageMax") != "100" || q.Get("heightMin") != "100" || q.Get("heightMax") != "250" || q.Get("pageSize") != "150" {
				t.Fatalf("legacy frozen filter contract changed: %s", r.URL)
			}
			if q.Get("userId") != "101" && q.Get("userId") != "102" {
				t.Fatal("queried an unrelated user")
			}
			mode, _ := strconv.Atoi(q.Get("all"))
			page, _ := strconv.Atoi(q.Get("pageNumber"))
			calls = append(calls, crawlCall{mode, page})
			profiles, status := response(mode, page)
			if status == 0 {
				status = 200
			}
			w.WriteHeader(status)
			if status != 200 {
				fmt.Fprint(w, `{"error":"source refused"}`)
				return
			}
			if profiles == nil {
				profiles = []Profile{}
			}
			_ = json.NewEncoder(w).Encode(profiles)
		default:
			t.Fatalf("crawl used hydration, reactions or another endpoint: %s", r.URL)
		}
	})}
	return c, &now, &calls, &httpCalls
}

func crawlJob(t *testing.T, c *FeedCrawler) *CrawlJob {
	t.Helper()
	j, err := c.Service.Store.CrawlStatus(context.Background(), 1)
	if err != nil || j == nil {
		t.Fatalf("job missing: %v", err)
	}
	return j
}
func crawlStep(t *testing.T, c *FeedCrawler, now *time.Time) {
	t.Helper()
	ok, err := c.Step(context.Background())
	if err != nil || !ok {
		t.Fatalf("crawl page failed: handled=%v err=%v", ok, err)
	}
	*now = now.Add(crawlGap)
}

func TestCrawlRequestIsLocalAtomicAndRespectsPause(t *testing.T) {
	c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("request performed HTTP"); return nil, 200 })
	ctx := context.Background()
	s := c.Service
	s.Store.Pause(ctx, 1, true)
	if _, err := c.Request(ctx, 1, true); err == nil {
		t.Fatal("request silently resumed a paused owner")
	}
	s.Store.Pause(ctx, 1, false)
	if _, err := s.Store.Archive(ctx, 1, crawlProfiles(11), "existing"); err != nil {
		t.Fatal(err)
	}
	s.Store.Delivered(ctx, 1, 11)
	if err := s.Store.Favorite(ctx, 1, 11); err != nil {
		t.Fatal(err)
	}
	j, err := c.Request(ctx, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if j.Filters.AgeMin != 0 || j.Filters.AgeMax != 111 || !j.IncludeOthers || j.Page != 1 || j.State != "queued" {
		t.Fatalf("bad new job: %+v", j)
	}
	state, err := s.Store.filterState(ctx, 1)
	if err != nil || state.Applied != nil || state.Draft != nil || state.Others {
		t.Fatal("collection changed manual UI filters")
	}
	if _, err = c.Request(ctx, 1, false); err == nil {
		t.Fatal("second active job accepted")
	}
	if other, err := s.Store.filterState(ctx, 2); err != nil || other.Applied != nil {
		t.Fatal("another owner's filters changed")
	}
	if _, err := s.Store.Profile(ctx, 1, 11); err != nil {
		t.Fatal("archive lost")
	}
	if fav, err := s.Store.List(ctx, 1, true, 0); err != nil || len(fav) != 1 {
		t.Fatal("favorites lost")
	}
	if *httpCalls != 0 {
		t.Fatal("DB-only request/status made an HTTP call")
	}
}

func TestCrawlTwoPagesEmptyAndDurableRestartGap(t *testing.T) {
	c, now, calls, _ := crawlFixture(t, func(mode, page int) ([]Profile, int) {
		if mode != 0 {
			t.Fatal("unrequested all=1 mode")
		}
		switch page {
		case 1:
			return crawlProfiles(11, 12), 200
		case 2:
			return crawlProfiles(12, 13), 200
		case 3:
			return nil, 200
		}
		t.Fatalf("unexpected page %d", page)
		return nil, 200
	})
	ctx := context.Background()
	if _, err := c.Request(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	// Simulate archive commit followed by a crash before recording its bookmark.
	if _, err := c.Service.Store.Archive(ctx, 1, crawlProfiles(11, 12), "pre-checkpoint-crash"); err != nil {
		t.Fatal(err)
	}
	ok, err := c.Step(ctx)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if j := crawlJob(t, c); j.Page != 2 || j.Pages != 1 {
		t.Fatalf("cursor not checkpointed: %+v", j)
	}
	transport := c.Service.Client.HTTP.Transport
	cfg := c.Service.Config
	if err := c.Service.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.Client.HTTP.Transport = transport
	c = s2.Crawler
	c.now = func() time.Time { return *now }
	if ok, err = c.Step(ctx); err != nil || ok || len(*calls) != 1 {
		t.Fatal("restart bypassed durable completion gap")
	}
	*now = now.Add(crawlGap)
	crawlStep(t, c, now)
	crawlStep(t, c, now)
	j := crawlJob(t, c)
	if j.State != "done" || j.Reason != "empty_page" || j.Page != 3 || j.Pages != 2 || j.Received != 4 {
		t.Fatalf("empty page reset cursor or looped: %+v", j)
	}
	if ok, err = c.Step(ctx); err != nil || ok || len(*calls) != 3 {
		t.Fatal("completed job requested another page")
	}
	n := s2.Store.Counts(ctx, 1)
	if n["profiles"] != 3 || n["versions"] != 3 {
		t.Fatalf("archive replay/overlap duplicated profiles: %v", n)
	}
	var media int
	if err := s2.Store.DB.QueryRow("SELECT COUNT(*) FROM media_jobs WHERE owner_id=1").Scan(&media); err != nil || media != 3 {
		t.Fatalf("replay duplicated media jobs: %d %v", media, err)
	}
}

func TestCrawlDetectsABAAndModeFingerprintsAreSeparate(t *testing.T) {
	t.Run("aba", func(t *testing.T) {
		c, now, calls, _ := crawlFixture(t, func(_ int, page int) ([]Profile, int) {
			switch page {
			case 1:
				return crawlProfiles(11, 12), 200
			case 2:
				return crawlProfiles(13), 200
			case 3:
				return crawlProfiles(12, 11), 200
			}
			t.Fatal("repeat did not stop")
			return nil, 200
		})
		if _, err := c.Request(context.Background(), 1, false); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			crawlStep(t, c, now)
		}
		j := crawlJob(t, c)
		if j.State != "stopped" || j.Reason != "repeated_page" || j.Page != 3 {
			t.Fatalf("A-B-A loop not stopped: %+v", j)
		}
		if ok, err := c.Step(context.Background()); err != nil || ok || len(*calls) != 3 {
			t.Fatal("repeated page retried")
		}
	})
	t.Run("explicit-others", func(t *testing.T) {
		c, now, calls, _ := crawlFixture(t, func(mode, page int) ([]Profile, int) {
			if page == 1 {
				return crawlProfiles(11, 12), 200
			}
			if mode == 1 && page == 2 {
				return crawlProfiles(13), 200
			}
			return nil, 200
		})
		if _, err := c.Request(context.Background(), 1, true); err != nil {
			t.Fatal(err)
		}
		for range 5 {
			crawlStep(t, c, now)
		}
		j := crawlJob(t, c)
		want := []crawlCall{{0, 1}, {0, 2}, {1, 1}, {1, 2}, {1, 3}}
		if fmt.Sprint(*calls) != fmt.Sprint(want) || j.State != "done" || j.Mode != 1 || j.Page != 3 {
			t.Fatalf("wrong explicit phase traversal: calls=%v job=%+v", *calls, j)
		}
		if n := c.Service.Store.Counts(context.Background(), 1); n["profiles"] != 3 || n["versions"] != 3 {
			t.Fatalf("all-mode overlap duplicated archive: %v", n)
		}
	})
}

func TestCrawlGuardsBeforeArchiveAndNoCursorOnErrors(t *testing.T) {
	for _, kind := range []string{"pause", "disconnect", "session", "http403", "http429", "http400", "disk-after"} {
		t.Run(kind, func(t *testing.T) {
			var c *FeedCrawler
			c, _, _, _ = crawlFixture(t, func(int, int) ([]Profile, int) {
				s := c.Service
				ctx := context.Background()
				switch kind {
				case "pause":
					s.Store.Pause(ctx, 1, true)
				case "disconnect":
					if err := s.Store.Disconnect(ctx, 1); err != nil {
						t.Fatal(err)
					}
				case "session":
					if err := s.Store.SaveSession(ctx, 1, Session{Token: "new", Refresh: "r", UserID: 101, Generation: "new", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
						t.Fatal(err)
					}
				case "http403":
					return nil, 403
				case "http429":
					return nil, 429
				case "http400":
					return nil, 400
				case "disk-after":
					s.Media.Config.MinFreeBytes = math.MaxUint64
				}
				return crawlProfiles(11), 200
			})
			if _, err := c.Request(context.Background(), 1, false); err != nil {
				t.Fatal(err)
			}
			if ok, err := c.Step(context.Background()); err != nil || !ok {
				t.Fatalf("stop failed: %v", err)
			}
			j := crawlJob(t, c)
			if j.State != "stopped" || j.Page != 1 || j.Pages != 0 || j.Received != 0 {
				t.Fatalf("failed page advanced checkpoint: %+v", j)
			}
			if n := c.Service.Store.Counts(context.Background(), 1); n["profiles"] != 0 || n["versions"] != 0 {
				t.Fatalf("invalidated response archived: %v", n)
			}
			if kind == "http403" && j.Reason != "http_403" || kind == "http429" && j.Reason != "http_429" {
				t.Fatalf("source restriction hidden: %+v", j)
			}
		})
	}
}

func TestCrawlPauseIsTerminalAndDiskCapAreBounded(t *testing.T) {
	t.Run("pause-resume", func(t *testing.T) {
		c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("paused job queried source"); return nil, 200 })
		if _, err := c.Request(context.Background(), 1, false); err != nil {
			t.Fatal(err)
		}
		uxSend(c.Service, 1, "/pause")
		uxSend(c.Service, 1, "/resume")
		if ok, err := c.Step(context.Background()); err != nil || ok || *httpCalls != 0 {
			t.Fatal("resume restarted a paused crawl")
		}
		if j := crawlJob(t, c); j.State != "stopped" || j.Reason != "paused" {
			t.Fatalf("pause not durable: %+v", j)
		}
	})
	t.Run("disk-before", func(t *testing.T) {
		c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
		if _, err := c.Request(context.Background(), 1, false); err != nil {
			t.Fatal(err)
		}
		c.Service.Media.Config.MinFreeBytes = math.MaxUint64
		if _, err := c.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		if j := crawlJob(t, c); j.Reason != "disk_reserve" || j.Page != 1 || *httpCalls != 0 {
			t.Fatalf("disk reserve did not stop before HTTP: %+v", j)
		}
	})
	t.Run("page-cap", func(t *testing.T) {
		c, _, calls, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
		if _, err := c.Request(context.Background(), 1, false); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Service.Store.DB.Exec("UPDATE feed_crawls SET page=? WHERE owner_id=1", crawlPageCap); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		j := crawlJob(t, c)
		if j.State != "stopped" || j.Reason != "page_cap" || j.Page != crawlPageCap+1 || len(*calls) != 1 {
			t.Fatalf("page cap reported completion or kept looping: %+v", j)
		}
	})
}

func TestCrawlProcessCancellationResumesAfterGap(t *testing.T) {
	var cancel context.CancelFunc
	c, now, calls, _ := crawlFixture(t, func(int, int) ([]Profile, int) {
		if cancel != nil {
			cancel()
		}
		return crawlProfiles(11), 200
	})
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	cancel = stop
	if _, err := c.Step(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("process cancellation not propagated: %v", err)
	}
	if j := crawlJob(t, c); !j.active() || j.Page != 1 {
		t.Fatalf("normal process stop ended crawl: %+v", j)
	}
	cancel = nil
	if ok, err := c.Step(context.Background()); err != nil || ok {
		t.Fatal("canceled process lost durable gap")
	}
	*now = now.Add(crawlGap)
	if ok, err := c.Step(context.Background()); err != nil || !ok || len(*calls) != 2 {
		t.Fatalf("checkpoint did not resume: %v", err)
	}
	if j := crawlJob(t, c); j.Page != 2 || j.Pages != 1 {
		t.Fatalf("resumed cursor wrong: %+v", j)
	}
}

func TestCrawlStatusDoesNotExposeSessionOrCallHTTP(t *testing.T) {
	c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	j, err := c.Service.Store.CrawlStatus(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "generation") || strings.Contains(string(raw), "token") || *httpCalls != 0 {
		t.Fatal("status leaked session or accessed source")
	}
}

func TestCrawlGlobalGapStartsAtCompletionAcrossOwners(t *testing.T) {
	var now *time.Time
	c, clock, calls, _ := crawlFixture(t, func(int, int) ([]Profile, int) {
		*now = now.Add(7 * time.Second)
		return crawlProfiles(11), 200
	})
	now = clock
	start := *now
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(context.Background(), 2, false); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	*now = start.Add(crawlGap)
	restarted := NewFeedCrawler(c.Service)
	restarted.now = func() time.Time { return *now }
	if ok, err := restarted.Step(context.Background()); err != nil || ok || len(*calls) != 1 {
		t.Fatal("global gap counted from page start instead of completion")
	}
	*now = start.Add(7*time.Second + crawlGap)
	if ok, err := restarted.Step(context.Background()); err != nil || !ok || len(*calls) != 2 {
		t.Fatal("another owner did not receive the next global slot")
	}
}

func TestCrawlCheckpointFailureStopsUntilExplicitNewRequest(t *testing.T) {
	c, now, calls, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	_, err := c.Service.Store.DB.Exec(`CREATE TRIGGER fail_crawl_bookmark BEFORE UPDATE OF page ON feed_crawls BEGIN SELECT RAISE(ABORT,'fixture checkpoint interruption'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatalf("checkpoint failure was not persisted as terminal: %v", err)
	}
	var fingerprints int
	if err := c.Service.Store.DB.QueryRow("SELECT COUNT(*) FROM feed_crawl_pages").Scan(&fingerprints); err != nil || fingerprints != 0 {
		t.Fatal("fingerprint committed without cursor")
	}
	if j := crawlJob(t, c); j.State != "stopped" || j.Reason != "checkpoint_failed" || j.Page != 1 || j.Pages != 0 {
		t.Fatalf("failed checkpoint did not stop: %+v", j)
	}
	if n := c.Service.Store.Counts(context.Background(), 1); n["profiles"] != 1 {
		t.Fatal("batch was not archived before bookmark")
	}
	*now = now.Add(2 * crawlGap)
	if ok, err := c.Step(context.Background()); err != nil || ok || len(*calls) != 1 {
		t.Fatal("persistent checkpoint failure repeated HTTP")
	}
	if _, err := c.Service.Store.DB.Exec("DROP TRIGGER fail_crawl_bookmark"); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); err != nil || ok || len(*calls) != 1 {
		t.Fatal("removing DB fault silently restarted terminal job")
	}
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	if j := crawlJob(t, c); j.Page != 2 || j.Pages != 1 || j.State != "running" {
		t.Fatalf("explicit replay did not checkpoint: %+v", j)
	}
	if n := c.Service.Store.Counts(context.Background(), 1); n["versions"] != 1 || n["pending_photos"] != 1 {
		t.Fatalf("explicit replay duplicated archive: %v", n)
	}
}

func TestCrawlPauseDuringMeDoesNotStartPageRequest(t *testing.T) {
	c, _, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("page after pause"); return nil, 200 })
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.Service.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/user/getMe" {
			t.Fatalf("new page started after pause: %s", r.URL)
		}
		c.Service.Store.Pause(context.Background(), 1, true)
		fmt.Fprint(w, `{"userId":101,"type":1}`)
	})}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatal(err)
	}
	j := crawlJob(t, c)
	if calls != 1 || j.State != "stopped" || j.Reason != "paused" || j.Page != 1 || j.Pages != 0 {
		t.Fatalf("pause during Me did not end job before page: %+v calls=%d", j, calls)
	}
}

func TestCrawlRejectsNonArrayDiscoveryWithoutFalseCompletion(t *testing.T) {
	for _, body := range []string{" \nnull\t", `{"items":[]}`} {
		t.Run(strings.TrimSpace(body), func(t *testing.T) {
			c, _, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("unexpected fixture request"); return nil, 200 })
			if _, err := c.Request(context.Background(), 1, false); err != nil {
				t.Fatal(err)
			}
			c.Service.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/user/getMe":
					fmt.Fprint(w, `{"userId":101,"type":1}`)
				case "/user/getUsers":
					fmt.Fprint(w, body)
				default:
					t.Fatalf("unexpected discovery endpoint: %s", r.URL)
				}
			})}
			if ok, err := c.Step(context.Background()); err != nil || !ok {
				t.Fatalf("malformed discovery handling failed: %v", err)
			}
			j := crawlJob(t, c)
			if j.State != "stopped" || j.Reason != "request_failed" || j.Page != 1 || j.Pages != 0 || j.Received != 0 {
				t.Fatalf("non-array response falsely completed/advanced crawl: %+v", j)
			}
			if n := c.Service.Store.Counts(context.Background(), 1); n["profiles"] != 0 {
				t.Fatal("non-array response was archived")
			}
		})
	}
}

func TestCrawlRunExitsWhenCheckpointAndStopCannotPersist(t *testing.T) {
	c, _, calls, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	if _, err := c.Request(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	_, err := c.Service.Store.DB.Exec(`
	CREATE TRIGGER fail_crawl_checkpoint BEFORE UPDATE OF page ON feed_crawls BEGIN SELECT RAISE(ABORT,'fixture checkpoint failure'); END;
	CREATE TRIGGER fail_crawl_stop BEFORE UPDATE OF state ON feed_crawls WHEN NEW.state='stopped' BEGIN SELECT RAISE(ABORT,'fixture stop failure'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("worker ignored an unpersistable failure instead of exiting")
	}
	if ctx.Err() != nil || len(*calls) != 1 {
		t.Fatal("worker exit depended on cancellation or repeated HTTP")
	}
	if j := crawlJob(t, c); j.Page != 1 || j.Pages != 0 {
		t.Fatalf("failed persistence advanced job: %+v", j)
	}
}

func TestCrawlArchiveFailureStopsOnlyFailingOwner(t *testing.T) {
	c, now, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	for _, owner := range []int64{1, 2} {
		if _, err := c.Request(context.Background(), owner, false); err != nil {
			t.Fatal(err)
		}
	}
	_, err := c.Service.Store.DB.Exec(`CREATE TRIGGER fail_owner_archive BEFORE INSERT ON versions WHEN NEW.owner_id=1 BEGIN SELECT RAISE(ABORT,'fixture owner archive failure'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatalf("persisted owner failure killed global worker: %v", err)
	}
	if j := crawlJob(t, c); j.State != "stopped" || j.Reason != "archive_failed" || j.Page != 1 {
		t.Fatalf("failed owner not stopped: %+v", j)
	}
	*now = now.Add(crawlGap)
	if ok, err := c.Step(context.Background()); err != nil || !ok {
		t.Fatalf("healthy owner could not continue: %v", err)
	}
	peer, err := c.Service.Store.CrawlStatus(context.Background(), 2)
	if err != nil || peer.Page != 2 || peer.Pages != 1 || !peer.active() {
		t.Fatalf("healthy owner did not checkpoint: %+v %v", peer, err)
	}
}

func TestCrawlCollectionPreservesManualFiltersCursorAndPending(t *testing.T) {
	c, now, _, _ := crawlFixture(t, func(_ int, page int) ([]Profile, int) {
		if page == 1 {
			return crawlProfiles(11, 12), 200
		}
		return nil, 200
	})
	ctx := context.Background()
	store := c.Service.Store
	if _, err := store.Archive(ctx, 1, crawlProfiles(11), "manual"); err != nil {
		t.Fatal(err)
	}
	state, err := store.filterState(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	draft := filterDraft{ID: "manual-draft", Values: DefaultFeedFilters()}
	draft.Values.AgeMin = 35
	draft.Values.AgeMax = 60
	if err = store.saveFilterDraft(ctx, 1, &draft); err != nil {
		t.Fatal(err)
	}
	if err = store.applyFilterDraft(ctx, 1, draft); err != nil {
		t.Fatal(err)
	}
	draft.ID = "pending-edit"
	draft.Values.AgeMax = 65
	if err = store.saveFilterDraft(ctx, 1, &draft); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("UPDATE feed_filters SET include_others=1,others_id='manual-nonce' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("UPDATE users SET feed_page=8 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("UPDATE queue SET delivered=0 WHERE owner_id=1 AND profile_id=11"); err != nil {
		t.Fatal(err)
	}
	state, err = store.filterState(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(state)
	if _, err = c.Request(ctx, 1, true, true); err != nil {
		t.Fatal(err)
	}
	crawlStep(t, c, now)
	afterState, err := store.filterState(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(afterState)
	var page, oldDelivered, newDelivered int
	if err = store.DB.QueryRow("SELECT feed_page FROM users WHERE id=1").Scan(&page); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=11").Scan(&oldDelivered); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=12").Scan(&newDelivered); err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || page != 8 || oldDelivered != 0 || newDelivered != 1 {
		t.Fatalf("collection changed manual filters/cursor/pending: %s %s %d %d %d", before, after, page, oldDelivered, newDelivered)
	}
	draft.ID = "another-edit"
	draft.Values.AgeMax = 70
	if err = store.saveFilterDraft(ctx, 1, &draft); err != nil {
		t.Fatal(err)
	}
	if err = store.applyFilterDraft(ctx, 1, draft); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("UPDATE feed_crawls SET next_request=0 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	// Applying a UI filter may stop other workflows; collection uses its own frozen bounds.
	if handled, err := c.Step(ctx); err != nil || !handled {
		t.Fatal("UI filter change stopped independent collection", err)
	}
	if job := crawlJob(t, c); !job.active() || job.Mode != 1 {
		t.Fatalf("collection depended on mutable manual filters: %+v", job)
	}
}

const legacyCrawlTable = `CREATE TABLE feed_crawls (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 run_id TEXT NOT NULL,upstream_id INTEGER NOT NULL,generation TEXT NOT NULL,
 filters_json TEXT NOT NULL,include_others INTEGER NOT NULL,mode INTEGER NOT NULL DEFAULT 0,
 page INTEGER NOT NULL DEFAULT 1,next_request INTEGER NOT NULL,state TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '',pages INTEGER NOT NULL DEFAULT 0,received INTEGER NOT NULL DEFAULT 0,
 started_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)`

func TestCrawlLegacySchemaStatusMigratesWithoutChangingJob(t *testing.T) {
	crawler, now, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	db := crawler.Service.Store.DB
	_, err := db.Exec(legacyCrawlTable)
	if err != nil {
		t.Fatal(err)
	}
	filters := DefaultFeedFilters()
	filters.AgeMin = 0
	filters.AgeMax = 100
	raw, _ := json.Marshal(filters)
	if _, err = db.Exec("INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,next_request,state,started_at,updated_at) VALUES(1,'legacy',101,'1',?,0,0,'running',1,1)", string(raw)); err != nil {
		t.Fatal(err)
	}
	job, err := crawler.Service.Store.CrawlStatus(context.Background(), 1)
	if err != nil || job.RunID != "legacy" || job.IncludeLocations || job.CatalogProfiles != 0 || job.Page != 1 {
		t.Fatalf("legacy status migration lost job: %+v %v", job, err)
	}
	crawlStep(t, crawler, now)
	if job = crawlJob(t, crawler); job.Page != 2 || job.Pages != 1 || job.RunID != "legacy" {
		t.Fatalf("legacy job did not resume safely: %+v", job)
	}
}

func TestCrawlMigrationSerializesIndependentStoresBeforeSchemaRead(t *testing.T) {
	crawler, _, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("migration made an HTTP request"); return nil, 200 })
	first := crawler.Service.Store
	if _, err := first.DB.Exec(legacyCrawlTable); err != nil {
		t.Fatal(err)
	}
	second, err := OpenStore(crawler.Service.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.DB.Close()
	filters := DefaultFeedFilters()
	filters.AgeMin = 0
	filters.AgeMax = 100
	raw, _ := json.Marshal(filters)
	if _, err = first.DB.Exec("INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,next_request,state,started_at,updated_at) VALUES(1,'legacy',101,'1',?,0,0,'running',1,1)", string(raw)); err != nil {
		t.Fatal(err)
	}
	// Keep a separate connection writing while both stores start their migration.
	first.DB.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, err := first.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='feed_crawl_next'"); err != nil {
		t.Fatal(err)
	}
	type result struct {
		job *CrawlJob
		err error
	}
	results := make(chan result, 2)
	started := make(chan struct{}, 2)
	for _, store := range []*Store{first, second} {
		go func(store *Store) {
			started <- struct{}{}
			job, err := store.CrawlStatus(ctx, 1)
			results <- result{job, err}
		}(store)
	}
	<-started
	<-started
	var received []result
	select {
	case r := <-results:
		received = append(received, r)
	case <-time.After(100 * time.Millisecond):
	}
	if err = holder.Commit(); err != nil {
		t.Fatal(err)
	}
	returnedBeforeWriterReleased := len(received) > 0
	for len(received) < 2 {
		select {
		case r := <-results:
			received = append(received, r)
		case <-ctx.Done():
			t.Fatal("concurrent migrations did not finish", ctx.Err())
		}
	}
	if returnedBeforeWriterReleased {
		t.Fatalf("migration inspected the old schema before acquiring its writer lock: %v", received[0].err)
	}
	for _, r := range received {
		if r.err != nil {
			t.Fatal("concurrent schema migration failed", r.err)
		}
		if r.job == nil || r.job.RunID != "legacy" || r.job.Page != 1 || r.job.IncludeLocations || r.job.CatalogProfiles != 0 {
			t.Fatalf("migration changed the legacy job: %+v", r.job)
		}
	}
	// Both connections can now inspect the same fully migrated schema.
	for _, store := range []*Store{first, second} {
		var count int
		if err = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('feed_crawls') WHERE name IN ('include_locations','catalog_pages','catalog_received','catalog_profiles','hydrated','unavailable')").Scan(&count); err != nil || count != 6 {
			t.Fatalf("partial schema after concurrent migration: %d %v", count, err)
		}
	}
}

func TestCrawlLegacyActiveCursorKeepsFrozenQueryUntilExplicitNewRun(t *testing.T) {
	c, now, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
	ctx := context.Background()
	if _, err := c.Service.Store.DB.Exec(legacyCrawlTable); err != nil {
		t.Fatal(err)
	}
	filters := DefaultFeedFilters()
	filters.AgeMin, filters.AgeMax = 22, 44
	raw, _ := json.Marshal(filters)
	if _, err := c.Service.Store.DB.Exec(`INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,page,next_request,state,started_at,updated_at)
 VALUES(1,'legacy-page-two',101,'1',?,0,2,0,'running',1,1)`, string(raw)); err != nil {
		t.Fatal(err)
	}
	job := crawlJob(t, c)
	if job.Unfiltered || job.Page != 2 || job.RunID != "legacy-page-two" {
		t.Fatalf("migration changed the legacy cursor or query contract: %+v", job)
	}
	var queries []map[string][]string
	c.Service.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user/getMe":
			fmt.Fprint(w, `{"userId":101,"type":1,"freezeState":false}`)
		case "/user/getUsers":
			queries = append(queries, r.URL.Query())
			fmt.Fprint(w, "[]")
		default:
			t.Fatalf("unexpected endpoint: %s", r.URL)
		}
	})}
	crawlStep(t, c, now)
	if len(queries) != 1 || queries[0]["pageNumber"][0] != "2" || queries[0]["ageMin"][0] != "22" || queries[0]["ageMax"][0] != "44" {
		t.Fatalf("legacy page resumed with a different query: %v", queries)
	}
	job, err := c.Request(ctx, 1, true)
	if err != nil || !job.Unfiltered || job.Page != 1 {
		t.Fatalf("explicit new run did not persist the raw query contract: %+v err=%v", job, err)
	}
	crawlStep(t, c, now)
	if len(queries) != 2 || len(queries[1]) != 6 || queries[1]["ageMin"][0] != "0" || queries[1]["ageMax"][0] != "111" || queries[1]["pageNumber"][0] != "1" || queries[1]["userId"][0] != "101" || queries[1]["pageSize"][0] != "150" || queries[1]["all"][0] != "0" {
		t.Fatalf("new run retained legacy filters: %v", queries)
	}
}
