package companion

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestResumeStoppedCrawlKeepsHydrationAndPacing(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	s.Config.OwnerID = 1
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("start=%v err=%v", started, err)
	}
	cycle := syncStatus(t, s)
	stopped := now.Add(time.Second)
	syncAuthRecoveryExec(t, s, "UPDATE feed_crawls SET state='stopped',reason='request_failed',mode=3,page=58,hydrated=57,updated_at=? WHERE owner_id=1", stopped.UnixMilli())
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.InitOperations(ctx); err != nil {
		t.Fatal(err)
	}
	eventAt := stopped.Add(-time.Millisecond).UnixMilli()
	syncAuthRecoveryExec(t, s, `INSERT INTO operations_events(id,owner_id,chat_id,fingerprint,role,level,message,fields_json,first_at_ms,last_at_ms)
 VALUES(77,1,0,'fixture-read-error','bot-parser','WARN','API operation failed',?,?,?)`,
		`{"source":"Jardulik","stage":"/user/getMe","method":"GET","kind":"operation_failed","tenant":1}`, eventAt, eventAt)
	*now = now.Add(2 * time.Second)
	next := now.Add(time.Minute).UnixMilli()
	syncAuthRecoveryExec(t, s, holdCollectionSlot, next)
	if _, err := s.ResumeStoppedCrawl(ctx, 1, cycle.ID, cycle.SearchRunID, 76); err == nil {
		t.Fatal("an unrelated event unlocked the crawl")
	}
	syncAuthRecoveryExec(t, s, "INSERT INTO meta(key,value) VALUES(?,'1')", dailyCrawlBlockKey(1))
	if _, err := s.ResumeStoppedCrawl(ctx, 1, cycle.ID, cycle.SearchRunID, 77); err == nil {
		t.Fatal("recovery ignored the operator pause")
	}
	syncAuthRecoveryExec(t, s, "DELETE FROM meta WHERE key=?", dailyCrawlBlockKey(1))
	got, err := s.ResumeStoppedCrawl(ctx, 1, cycle.ID, cycle.SearchRunID, 77)
	if err != nil || got.Stage != "search" || got.ID != cycle.ID {
		t.Fatalf("resume=%+v err=%v", got, err)
	}
	job, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || job.State != "queued" || job.RunID != cycle.SearchRunID || job.Mode != 3 || job.Page != 58 || job.Hydrated != 57 || job.NextRequest != next {
		t.Fatalf("resumed checkpoint=%+v err=%v", job, err)
	}
	if s.Store.Meta(ctx, "sync_crawl_resume:"+cycle.ID) == "" {
		t.Fatal("recovery receipt missing")
	}
	if _, err := s.ResumeStoppedCrawl(ctx, 1, cycle.ID, cycle.SearchRunID, 77); err == nil {
		t.Fatal("same recovery repeated")
	}
}

func TestSyncCrawlResumesSameCheckpointAfterFreeSpaceReturns(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("start=%v err=%v", started, err)
	}
	cycle := syncStatus(t, s)
	syncAuthRecoveryExec(t, s, "UPDATE feed_crawls SET mode=3,page=499,hydrated=498 WHERE owner_id=1")
	reserve := s.Media.Config.MinFreeBytes
	s.Media.Config.MinFreeBytes = ^uint64(0)
	if worked, err := s.Crawler.Step(ctx); !worked || err != nil {
		t.Fatalf("disk stop: worked=%v err=%v", worked, err)
	}
	if _, err := s.SyncCycleStep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := syncStatus(t, s); got.Stage != "blocked" || got.Reason != "search_incomplete" {
		t.Fatalf("stop was not recorded: %+v", got)
	}
	if worked, err := s.SyncCycleStep(ctx); worked || err != nil {
		t.Fatalf("reserve still missing: worked=%v err=%v", worked, err)
	}
	s.Media.Config.MinFreeBytes = reserve
	next := now.Add(time.Minute).UnixMilli()
	syncAuthRecoveryExec(t, s, holdCollectionSlot, next)
	if worked, err := s.SyncCycleStep(ctx); !worked || err != nil {
		t.Fatalf("disk recovery: worked=%v err=%v", worked, err)
	}
	if got := syncStatus(t, s); got.ID != cycle.ID || got.Stage != "search" {
		t.Fatalf("wrong cycle resumed: %+v", got)
	}
	job, err := s.Store.CrawlStatus(ctx, 1)
	if err != nil || job.State != "queued" || job.RunID != cycle.SearchRunID || job.Mode != 3 || job.Page != 499 || job.Hydrated != 498 || job.NextRequest != next {
		t.Fatalf("resumed checkpoint=%+v err=%v", job, err)
	}
}

func TestSyncCrawlRetriesInterruptedGETWithoutLosingThePage(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("start=%v err=%v", started, err)
	}
	cycle := syncStatus(t, s)
	calls := 0
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/user/getMe" {
			t.Fatalf("unexpected source request: %s %s", r.Method, r.URL.Path)
		}
		return nil, io.ErrUnexpectedEOF
	})
	for i, delay := range []time.Duration{time.Minute, 5 * time.Minute, time.Hour} {
		if worked, err := s.Crawler.Step(ctx); !worked || err != nil {
			t.Fatalf("step %d: worked=%v err=%v", i, worked, err)
		}
		job, err := s.Store.CrawlStatus(ctx, 1)
		if err != nil || job.State != "queued" || job.Reason != "retry_transport_interrupted" || job.RunID != cycle.SearchRunID || job.Page != 1 || job.Pages != 0 || job.NextRequest != now.Add(delay).UnixMilli() {
			t.Fatalf("step %d: retry checkpoint=%+v err=%v", i, job, err)
		}
		if worked, err := s.Crawler.Step(ctx); worked || err != nil {
			t.Fatalf("early retry %d: worked=%v err=%v", i, worked, err)
		}
		*now = now.Add(delay)
	}
	if calls != 3 {
		t.Fatalf("interrupted reads=%d want=3", calls)
	}
}
