package companion

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestCrawlQueuedForegroundKeepsCursorAndHTTPBudget(t *testing.T) {
	c, now, calls, requests := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	ctx := context.Background()
	if _, err := c.Request(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	before := crawlJob(t, c)
	queued := c.Service.prepareQueuedUpdate(priorityMessage(9, 2))
	if ok, err := c.Step(ctx); err != nil || ok {
		t.Fatalf("background stepped before other owner command: %t %v", ok, err)
	}
	after := crawlJob(t, c)
	if *requests != 0 || after.Page != before.Page || after.NextRequest != before.NextRequest || after.State != before.State {
		t.Fatalf("yield consumed source or cursor: %+v", after)
	}
	c.Service.releaseQueuedUpdate(queued)
	crawlStep(t, c, now)
	if len(*calls) != 1 || crawlJob(t, c).Page != 2 {
		t.Fatal("background did not resume its original cursor")
	}
}

func TestCrawlOwnerBusyDoesNotWaitOnUserLock(t *testing.T) {
	c, _, _, requests := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
	ctx := context.Background()
	if _, err := c.Request(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	mu := c.Service.userLock(1)
	mu.Lock()
	done := make(chan error, 1)
	go func() {
		ok, err := c.Step(ctx)
		if ok && err == nil {
			err = errors.New("background handled a busy owner")
		}
		done <- err
	}()
	select {
	case err := <-done:
		mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		mu.Unlock()
		<-done
		t.Fatal("background blocked behind a user lock")
	}
	if *requests != 0 {
		t.Fatal("busy owner caused HTTP")
	}
}

func TestCrawlQueuedCommandBetweenGETsResumesWithoutStopping(t *testing.T) {
	c, now, calls, requests := crawlFixture(t, func(int, int) ([]Profile, int) { return crawlProfiles(11), 200 })
	ctx := context.Background()
	if _, err := c.Request(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	original := c.Service.Client.HTTP.Transport
	var queued queuedUpdate
	first := true
	c.Service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(r)
		if first && r.URL.Path == "/user/getMe" {
			first = false
			queued = c.Service.prepareQueuedUpdate(priorityMessage(10, 1))
		}
		return response, err
	})
	if ok, err := c.Step(ctx); err != nil || !ok {
		t.Fatalf("read boundary yield failed: %t %v", ok, err)
	}
	j := crawlJob(t, c)
	if len(*calls) != 0 || *requests != 1 || j.Page != 1 || j.State == "stopped" || j.Reason != "" {
		t.Fatalf("yield advanced/stopped job: %+v requests=%d", j, *requests)
	}
	c.Service.releaseQueuedUpdate(queued)
	*now = now.Add(crawlGap)
	crawlStep(t, c, now)
	if len(*calls) != 1 || crawlJob(t, c).Page != 2 {
		t.Fatal("yielded page was not resumed")
	}
}

func TestCrawlPriorityLeavesForegroundGETAndRefreshUntouched(t *testing.T) {
	c, _, _, _ := crawlFixture(t, func(int, int) ([]Profile, int) { return nil, 200 })
	s := c.Service
	s.beginForeground()
	defer s.endForeground()
	count := 0
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) { count++; return nil, errors.New("test transport") })
	if _, err := s.Client.exchange(context.Background(), http.MethodGet, "/user/getMe", "", nil); errors.Is(err, errCrawlYield) || count != 1 {
		t.Fatal("foreground GET was yielded")
	}
	background := withCrawlPriority(context.Background(), s)
	if _, err := s.Client.exchange(background, http.MethodGet, "/user/getMe", "", nil); !errors.Is(err, errCrawlYield) || count != 1 {
		t.Fatal("background GET passed foreground queue")
	}
	if _, err := s.Client.exchange(background, http.MethodPut, "/appuser/refresh", "", nil); errors.Is(err, errCrawlYield) || count != 2 {
		t.Fatal("token rotation was interrupted")
	}
	if _, err := s.Client.exchange(background, http.MethodPost, "/user/reaction", "", nil); errors.Is(err, errCrawlYield) || count != 3 {
		t.Fatal("a reaction was interrupted")
	}
}

func TestDailyCrawlDefersForForegroundWithoutCreatingRun(t *testing.T) {
	c, _ := dailyCrawlFixture(t)
	queued := c.Service.prepareQueuedUpdate(priorityMessage(11, 2))
	dailySchedule(t, c, false)
	if j, err := c.Service.Store.CrawlStatus(context.Background(), 1); err != nil || j != nil {
		t.Fatalf("scheduler created a run during foreground: %+v %v", j, err)
	}
	c.Service.releaseQueuedUpdate(queued)
	dailySchedule(t, c, true)
}
