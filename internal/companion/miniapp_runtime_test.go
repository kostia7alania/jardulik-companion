package companion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMiniAppEndpointConfiguration(t *testing.T) {
	for _, test := range []struct {
		listen, origin string
		valid          bool
	}{
		{"", "", true},
		{"127.0.0.1:8790", "https://miniapp.example", true},
		{"0.0.0.0:8790", "https://miniapp.example", true},
		{"172.18.0.1:8790", "https://miniapp.example", true},
		{"[::1]:8790", "https://miniapp.example", true},
		{"", "https://miniapp.example", false},
		{"127.0.0.1:8790", "", false},
		{"localhost:8790", "https://miniapp.example", false},
		{"8.8.8.8:8790", "https://miniapp.example", false},
		{"127.0.0.1:0", "https://miniapp.example", false},
		{"127.0.0.1:8790", "http://miniapp.example", false},
		{"127.0.0.1:8790", "https://user:pass@miniapp.example", false},
		{"127.0.0.1:8790", "https://miniapp.example/profile", false},
		{"127.0.0.1:8790", "https://miniapp.example?token=value", false},
		{"127.0.0.1:8790", "https://miniapp.example#token", false},
	} {
		if err := validateMiniAppEndpoint(test.listen, test.origin); (err == nil) != test.valid {
			t.Errorf("endpoint %q/%q valid=%v: %v", test.listen, test.origin, test.valid, err)
		}
	}
}

func TestMiniAppRequestSlotsWaitAndHonorCancellation(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	h := limitMiniAppRequests(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	}), 2)
	var active sync.WaitGroup
	for range 2 {
		active.Add(1)
		go func() {
			defer active.Done()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "https://miniapp.example/assets/entry.js", nil))
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("active request did not enter")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "https://miniapp.example/assets/entry.js", nil).WithContext(ctx))
	if calls.Load() != 2 {
		t.Fatal("canceled request exceeded the active-work budget")
	}
	close(release)
	active.Wait()
}

func TestRuntimeAllHeartbeatDoesNotClaimStandaloneMiniApp(t *testing.T) {
	f := newOperationsFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); f.s.runOperationsHeartbeat(ctx, RuntimeAll) }()
	until := time.Now().Add(2 * time.Second)
	for f.s.Store.Meta(context.Background(), "operations_heartbeat:compatibility") == "" {
		if time.Now().After(until) {
			t.Fatal("main-role heartbeat unavailable")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if f.s.Store.Meta(context.Background(), "operations_heartbeat:miniapp") != "" {
		t.Fatal("main runtime claimed a standalone Mini App heartbeat")
	}
}
