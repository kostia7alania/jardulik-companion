package companion

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func priorityMessage(id, owner int64) Update {
	return Update{ID: id, Message: &Message{From: TGUser{ID: owner}, Chat: TGChat{ID: owner, Type: "private"}, Text: "/help"}}
}

func awaitPrioritySignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("foreground handler did not reach expected phase")
	}
}

func blockFirstPriorityReply(s *Service) (<-chan struct{}, func()) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int64
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":7,"chat":{"id":1}}}`)), Request: r}, nil
	})
	return entered, func() { once.Do(func() { close(release) }) }
}

func TestForegroundDirectHandleRemainsPendingUntilReplyCompletes(t *testing.T) {
	s, _ := uxFixture(t)
	entered, release := blockFirstPriorityReply(s)
	defer release()
	done := make(chan struct{})
	go func() { defer close(done); s.Handle(context.Background(), priorityMessage(1, 1)) }()
	awaitPrioritySignal(t, entered)
	if !s.foregroundPending() || s.foreground.pending.Load() != 1 {
		t.Fatal("executing direct interaction did not reserve priority exactly once")
	}
	release()
	awaitPrioritySignal(t, done)
	if s.foregroundPending() || s.foreground.pending.Load() != 0 {
		t.Fatal("completed direct interaction leaked foreground priority")
	}
}

func TestForegroundQueuedCommandKeepsPriorityBetweenHandlers(t *testing.T) {
	s, _ := uxFixture(t)
	ctx := context.Background()
	for _, id := range []int64{1, 2} {
		if ok, err := s.Store.ClaimUpdate(ctx, id); !ok || err != nil {
			t.Fatalf("claim fixture update: %t %v", ok, err)
		}
	}
	jobs := make(chan queuedUpdate, 1)
	entered, release := blockFirstPriorityReply(s)
	defer release()
	first := s.prepareQueuedUpdate(priorityMessage(1, 1))
	done := make(chan struct{})
	go func() { defer close(done); s.handleQueuedUpdate(ctx, first) }()
	awaitPrioritySignal(t, entered)
	if !s.enqueueUpdate(ctx, jobs, s.prepareQueuedUpdate(priorityMessage(2, 1))) {
		t.Fatal("enqueue failed")
	}
	if s.foreground.pending.Load() != 2 {
		t.Fatal("queued command not counted alongside executing command")
	}
	release()
	awaitPrioritySignal(t, done)
	if !s.foregroundPending() || s.foreground.pending.Load() != 1 {
		t.Fatal("priority dropped while a command remained queued")
	}
	var state string
	if err := s.Store.DB.QueryRow("SELECT state FROM updates WHERE id=1").Scan(&state); err != nil || state != "done" {
		t.Fatalf("first update unfinished after priority release: %s %v", state, err)
	}
	s.handleQueuedUpdate(ctx, <-jobs)
	if s.foregroundPending() || s.foreground.pending.Load() != 0 {
		t.Fatal("worker double-counted Handle or leaked its reservation")
	}
	if err := s.Store.DB.QueryRow("SELECT state FROM updates WHERE id=2").Scan(&state); err != nil || state != "done" {
		t.Fatalf("second update unfinished: %s %v", state, err)
	}
}

func TestForegroundCancelledEnqueueAndShutdownDrainBalance(t *testing.T) {
	s := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.enqueueUpdate(ctx, make(chan queuedUpdate), s.prepareQueuedUpdate(priorityMessage(1, 1))) {
		t.Fatal("cancelled unbuffered enqueue succeeded")
	}
	if s.foreground.pending.Load() != 0 {
		t.Fatal("cancelled enqueue leaked its reservation")
	}
	jobs := make(chan queuedUpdate, 3)
	for _, u := range []Update{priorityMessage(2, 1), {}, priorityMessage(3, 2)} {
		if !s.enqueueUpdate(context.Background(), jobs, s.prepareQueuedUpdate(u)) {
			t.Fatal("buffered enqueue failed")
		}
	}
	if s.foreground.pending.Load() != 2 {
		t.Fatal("unhandled update was counted or owner update was lost")
	}
	close(jobs)
	s.discardQueuedUpdates(jobs)
	if s.foreground.pending.Load() != 0 {
		t.Fatal("shutdown drain leaked foreground reservations")
	}
}

func TestForegroundEligibilityMatchesPrivateAndArchiveRoutes(t *testing.T) {
	s := &Service{Config: Config{OwnerID: 99, BackupChatID: -100123}}
	callback := func(owner, chat int64, kind, data string) Update {
		return Update{Callback: &Callback{From: TGUser{ID: owner}, Message: Message{Chat: TGChat{ID: chat, Type: kind}}, Data: data}}
	}
	for _, tc := range []struct {
		update Update
		want   bool
	}{
		{priorityMessage(1, 1), true},
		{callback(1, 1, "private", "l:7"), true},
		{callback(2, 1, "private", "l:7"), false},
		{callback(1, -100, "supergroup", "l:7"), false},
		{callback(99, -100123, "supergroup", "av:7"), true},
		{callback(99, -100123, "supergroup", "az:7"), true},
		{callback(99, -100123, "supergroup", "aj:7"), true},
		{callback(1, -100123, "supergroup", "av:7"), false},
		{callback(99, -100124, "supergroup", "av:7"), false},
		{callback(99, -100123, "supergroup", "l:7"), false},
		{Update{}, false},
	} {
		job := s.prepareQueuedUpdate(tc.update)
		if job.foreground != tc.want || s.foregroundPending() != tc.want {
			t.Fatalf("incorrect priority attribution: %+v want=%t", tc.update, tc.want)
		}
		s.releaseQueuedUpdate(job)
		if s.foreground.pending.Load() != 0 {
			t.Fatal("unattributed callback changed priority count")
		}
	}
}

func TestForegroundConcurrentOwnerInteractionsAreNotLost(t *testing.T) {
	s, _ := uxFixture(t)
	ready := make(chan struct{}, 32)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		ready <- struct{}{}
		<-release
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":7,"chat":{"id":1}}}`)), Request: r}, nil
	})
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(owner int64) {
			defer workers.Done()
			s.Handle(context.Background(), priorityMessage(owner, owner))
		}(int64(i + 1))
	}
	for i := 0; i < 32; i++ {
		awaitPrioritySignal(t, ready)
	}
	if !s.foregroundPending() || s.foreground.pending.Load() != 32 {
		t.Error("concurrent owner reservations were lost")
	}
	unblock()
	workers.Wait()
	if s.foregroundPending() || s.foreground.pending.Load() != 0 {
		t.Fatal("concurrent reservations did not balance")
	}
}
