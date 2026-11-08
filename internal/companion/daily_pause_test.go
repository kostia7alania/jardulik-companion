package companion

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDailyCrawlPauseResumeBetweenTicksStaysBlocked(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	ctx := context.Background()
	dailySchedule(t, c, true)
	dailyJobState(t, c, "done", "empty_page")
	old := crawlJob(t, c)
	c.Service.Store.Pause(ctx, 1, true)
	c.Service.Store.Pause(ctx, 1, false)
	*now = now.Add(48 * time.Hour)
	dailySchedule(t, c, false)
	if got := crawlJob(t, c); got.RunID != old.RunID {
		t.Fatal("fast pause/resume restarted daily crawl")
	}
	if _, err := c.Request(ctx, 1, true, true); err != nil {
		t.Fatal(err)
	}
	dailySchedule(t, c, false)
	if got := crawlJob(t, c); got.RunID == old.RunID {
		t.Fatal("explicit new request failed to re-arm")
	}
	dailyJobState(t, c, "done", "empty_page")
	*now = now.Add(24 * time.Hour)
	dailySchedule(t, c, true)
}

func TestDailyCrawlPauseBeforeFirstTickCannotCreateRun(t *testing.T) {
	c, _ := dailyCrawlFixture(t)
	ctx := context.Background()
	c.Service.Store.Pause(ctx, 1, true)
	c.Service.Store.Pause(ctx, 1, false)
	dailySchedule(t, c, false)
	j, err := c.Service.Store.CrawlStatus(ctx, 1)
	if err != nil || j != nil {
		t.Fatalf("daily activated after initial pause/resume: %+v %v", j, err)
	}
	if _, err = c.Request(ctx, 1, true, true); err != nil {
		t.Fatal(err)
	}
	dailySchedule(t, c, false)
	if j, err = c.Service.Store.CrawlStatus(ctx, 1); err != nil || j == nil {
		t.Fatalf("explicit request did not work: %+v %v", j, err)
	}
}

func TestDailyCrawlPauseBarrierIsOwnerScopedAndAtomic(t *testing.T) {
	c, _ := dailyCrawlFixture(t)
	ctx := context.Background()
	c.Service.Store.Pause(ctx, 2, true)
	dailySchedule(t, c, true)
	var marker string
	if err := c.Service.Store.DB.QueryRow("SELECT value FROM meta WHERE key=?", dailyCrawlBlockKey(2)).Scan(&marker); err != nil || marker != "1" {
		t.Fatal("other owner pause barrier missing", err)
	}
	c.Service.Store.Pause(ctx, 999, true)
	var orphan int
	if err := c.Service.Store.DB.QueryRow("SELECT COUNT(*) FROM meta WHERE key=?", dailyCrawlBlockKey(999)).Scan(&orphan); err != nil || orphan != 0 {
		t.Fatal("unknown owner acquired a pause barrier", err)
	}
	if _, err := c.Service.Store.DB.Exec(`CREATE TRIGGER reject_daily_pause BEFORE INSERT ON meta WHEN NEW.key='daily_crawl_block:1' BEGIN SELECT RAISE(ABORT,'test barrier failure'); END`); err != nil {
		t.Fatal(err)
	}
	c.Service.Store.Pause(ctx, 1, true)
	if c.Service.Store.Paused(ctx, 1) {
		t.Fatal("failed durable barrier committed only half a pause")
	}
}

func TestDailyCrawlStatusShowsScheduleAndDurablePause(t *testing.T) {
	c, now := dailyCrawlFixture(t)
	ctx := context.Background()
	dailySchedule(t, c, true)
	status := c.DailyStatus(ctx, 1)
	if !strings.Contains(status, "UTC+3") || !strings.Contains(status, "следующий запуск") {
		t.Fatal(status)
	}
	if c.DailyStatus(ctx, 2) != "" {
		t.Fatal("private source schedule leaked to another tenant")
	}
	*now = now.Add(25 * time.Hour)
	if got := c.DailyStatus(ctx, 1); !strings.Contains(got, "после завершения") {
		t.Fatal(got)
	}
	c.Service.Store.Pause(ctx, 1, true)
	c.Service.Store.Pause(ctx, 1, false)
	if got := c.DailyStatus(ctx, 1); !strings.Contains(got, "остановлен") {
		t.Fatal(got)
	}
}
