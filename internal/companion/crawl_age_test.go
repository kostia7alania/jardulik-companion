package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func oldBroadCrawlCursor(t *testing.T, c *FeedCrawler) *CrawlJob {
	t.Helper()
	ctx := context.Background()
	job, err := c.Request(ctx, 1, true, true)
	if err != nil {
		t.Fatal(err)
	}
	old := job.Filters
	old.AgeMax = 100
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Service.Store.DB.Exec(`UPDATE feed_crawls SET filters_json=?,state='running',mode=1,page=17,pages=16,received=42,catalog_pages=3,catalog_received=4,catalog_profiles=5,hydrated=6,unavailable=7 WHERE owner_id=1`, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Service.Store.DB.Exec(`INSERT INTO feed_crawl_pages(owner_id,run_id,mode,fingerprint,page) VALUES(1,?,1,'old-query-page',16)`, job.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Service.Store.DB.Exec(`INSERT INTO meta(key,value) VALUES(?,?)`, crawlRetryKey(1), `{"run_id":"old-query","failures":1}`); err != nil {
		t.Fatal(err)
	}
	return crawlJob(t, c)
}

func TestCrawlBroadAgeUpgradeResetsCursorAndKeepsPersonalFilters(t *testing.T) {
	c, now, calls, httpCalls := crawlFixture(t, func(mode, page int) ([]Profile, int) {
		if mode != 0 || page != 1 {
			t.Fatalf("new query reused old cursor: mode=%d page=%d", mode, page)
		}
		return crawlProfiles(11), 200
	})
	ctx := context.Background()
	old := oldBroadCrawlCursor(t, c)
	personal := DefaultFeedFilters()
	personal.AgeMin, personal.AgeMax = 0, 25
	raw, err := json.Marshal(personal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Service.Store.DB.Exec(`INSERT INTO feed_filters(owner_id,applied_json) VALUES(1,?)`, string(raw)); err != nil {
		t.Fatal(err)
	}
	if handled, err := c.Step(ctx); err != nil || !handled {
		t.Fatal("upgrade failed", err)
	}
	next := crawlJob(t, c)
	if next.RunID == old.RunID || !next.Unfiltered || next.Filters.AgeMin != 0 || next.Filters.AgeMax != 111 || next.Page != 1 || next.Mode != 0 || next.State != "queued" || next.Pages != 0 || next.Received != 0 || next.CatalogPages != 0 || next.CatalogReceived != 0 || next.CatalogProfiles != 0 || next.Hydrated != 0 || next.Unavailable != 0 || !next.IncludeOthers || !next.IncludeLocations {
		t.Fatalf("query change did not create a clean broad run: %+v", next)
	}
	if *httpCalls != 0 {
		t.Fatal("cursor upgrade performed HTTP")
	}
	var fingerprints, oldPresence, newPresence int
	if err = c.Service.Store.DB.QueryRow(`SELECT COUNT(*) FROM feed_crawl_pages WHERE owner_id=1`).Scan(&fingerprints); err != nil || fingerprints != 0 {
		t.Fatal("old query fingerprints remained", err)
	}
	if err = c.Service.Store.DB.QueryRow(`SELECT COUNT(*) FROM crawl_presence_runs WHERE owner_id=1 AND run_id=?`, old.RunID).Scan(&oldPresence); err != nil || oldPresence != 0 {
		t.Fatal("old presence baseline remained", err)
	}
	if err = c.Service.Store.DB.QueryRow(`SELECT COUNT(*) FROM crawl_presence_runs WHERE owner_id=1 AND run_id=?`, next.RunID).Scan(&newPresence); err != nil || newPresence != 1 {
		t.Fatal("new presence baseline missing", err)
	}
	var retry string
	if err = c.Service.Store.DB.QueryRow(`SELECT value FROM meta WHERE key=?`, crawlRetryKey(1)).Scan(&retry); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("old query retry remained", err)
	}
	state, err := c.Service.Store.filterState(ctx, 1)
	if err != nil || state.Applied == nil || !reflect.DeepEqual(*state.Applied, personal) {
		t.Fatal("global upgrade changed personal0..25 filters", err)
	}
	// The existing source fixture asserts all actual getUsers query values.
	crawlStep(t, c, now)
	if len(*calls) != 1 || (*calls)[0] != (crawlCall{Mode: 0, Page: 1}) {
		t.Fatal("wide source cursor", *calls)
	}
	state, err = c.Service.Store.filterState(ctx, 1)
	if err != nil || state.Applied == nil || !reflect.DeepEqual(*state.Applied, personal) {
		t.Fatal("wide GET changed personal filters", err)
	}
}

func TestCrawlBroadAgeUpgradeDoesNotReviveUnavailableRun(t *testing.T) {
	for _, restriction := range []string{"stopped", "owner-paused", "session-paused", "generation", "pause-barrier"} {
		t.Run(restriction, func(t *testing.T) {
			c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("unavailable upgrade made HTTP"); return nil, 200 })
			ctx := context.Background()
			old := oldBroadCrawlCursor(t, c)
			switch restriction {
			case "stopped":
				if err := c.Service.Store.StopCrawl(ctx, 1, "paused"); err != nil {
					t.Fatal(err)
				}
			case "owner-paused":
				c.Service.Store.Pause(ctx, 1, true)
			case "session-paused":
				c.Service.Store.SessionState(ctx, 1, "paused", "challenge")
			case "generation":
				session, err := c.Service.Store.Session(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				session.Generation = "replacement"
				if err = c.Service.Store.SaveSession(ctx, 1, session); err != nil {
					t.Fatal(err)
				}
			case "pause-barrier":
				c.Service.Store.Pause(ctx, 1, true)
				c.Service.Store.Pause(ctx, 1, false)
			}
			if _, err := c.Step(ctx); err != nil {
				t.Fatal(err)
			}
			got := crawlJob(t, c)
			if got.RunID != old.RunID || got.Filters != old.Filters || got.Page != old.Page || got.Mode != old.Mode || got.active() {
				t.Fatalf("restriction revived or changed broad cursor: %+v", got)
			}
			if *httpCalls != 0 {
				t.Fatal("unavailable upgrade made HTTP")
			}
		})
	}
}

func TestCrawlBroadAgeUpgradeFailureRollsBackCursorAndLedger(t *testing.T) {
	c, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("failed upgrade made HTTP"); return nil, 200 })
	old := oldBroadCrawlCursor(t, c)
	if _, err := c.Service.Store.DB.Exec(`CREATE TRIGGER reject_broad_upgrade BEFORE UPDATE OF run_id ON feed_crawls BEGIN SELECT RAISE(ABORT,'fixture upgrade failure'); END`); err != nil {
		t.Fatal(err)
	}
	if handled, err := c.Step(context.Background()); err == nil || !handled {
		t.Fatal("upgrade failure was ignored", err)
	}
	got := crawlJob(t, c)
	if !reflect.DeepEqual(got, old) {
		t.Fatalf("failed upgrade changed saved cursor: before=%+v after=%+v", old, got)
	}
	var fingerprints, presence int
	if err := c.Service.Store.DB.QueryRow(`SELECT COUNT(*) FROM feed_crawl_pages WHERE owner_id=1 AND run_id=?`, old.RunID).Scan(&fingerprints); err != nil || fingerprints != 1 {
		t.Fatal("failed upgrade lost old fingerprint", err)
	}
	if err := c.Service.Store.DB.QueryRow(`SELECT COUNT(*) FROM crawl_presence_runs WHERE owner_id=1 AND run_id=?`, old.RunID).Scan(&presence); err != nil || presence != 1 {
		t.Fatal("failed upgrade lost old baseline", err)
	}
	var retry string
	if err := c.Service.Store.DB.QueryRow(`SELECT value FROM meta WHERE key=?`, crawlRetryKey(1)).Scan(&retry); err != nil || retry != `{"run_id":"old-query","failures":1}` {
		t.Fatal("failed upgrade lost old retry", err)
	}
	if *httpCalls != 0 {
		t.Fatal("failed upgrade made HTTP")
	}
}
