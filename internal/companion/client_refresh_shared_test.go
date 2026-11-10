package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedClientRefreshRotatesOnce(t *testing.T) {
	store, cfg := testStore(t)
	other, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = store.User(ctx, 1, "fixture"); err != nil {
		t.Fatal(err)
	}
	old := jwt(time.Now().Add(-time.Hour).Unix())
	next := jwt(time.Now().Add(time.Hour).Unix())
	if err = store.SaveSession(ctx, 1, Session{Token: old, Refresh: "first", UserID: 9, Generation: "same-login", Expires: tokenExpiry(old)}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var refreshes atomic.Int64
	handler := handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/appuser/refresh" {
			refreshes.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"token": next, "refreshToken": "second"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+next {
			t.Error("request used an expired token")
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})}
	first, second := NewClient(store, cfg), NewClient(other, cfg)
	first.HTTP.Transport, second.HTTP.Transport = handler, handler
	done := make(chan error, 2)
	go func() { _, err := first.Request(ctx, 1, "GET", "/read", nil); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first refresh did not start")
	}
	go func() { _, err := second.Request(ctx, 1, "GET", "/read", nil); done <- err }()
	select {
	case <-entered:
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("shared stores refreshed the same session %d times; want 1", got)
	}
}

func TestSharedClientRefreshPreservesConcurrentSessionChange(t *testing.T) {
	for _, change := range []string{"paused", "reconnect_required", "replacement", "disconnect"} {
		for _, response := range []string{"success", "unauthorized", "invalid"} {
			t.Run(change+"/"+response, func(t *testing.T) {
				store, cfg := testStore(t)
				other, err := OpenStore(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { other.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := store.User(ctx, 1, "fixture"); err != nil {
					t.Fatal(err)
				}
				old := jwt(time.Now().Add(-time.Hour).Unix())
				original := Session{Token: old, Refresh: "original-refresh", UserID: 9, Generation: "original", Expires: tokenExpiry(old)}
				if err := store.SaveSession(ctx, 1, original); err != nil {
					t.Fatal(err)
				}
				var reads int
				client := NewClient(store, cfg)
				client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/appuser/refresh" {
						reads++
						_, _ = w.Write([]byte(`{}`))
						return
					}
					switch change {
					case "paused", "reconnect_required":
						other.SessionState(ctx, 1, change, "concurrent-change")
					case "replacement":
						// The same upstream account and tokens still represent a new login generation.
						replacement := original
						replacement.Generation = "replacement"
						if err := other.SaveSession(ctx, 1, replacement); err != nil {
							t.Error(err)
						}
					case "disconnect":
						if err := other.Disconnect(ctx, 1); err != nil {
							t.Error(err)
						}
					}
					switch response {
					case "success":
						_ = json.NewEncoder(w).Encode(map[string]string{"token": jwt(time.Now().Add(time.Hour).Unix()), "refreshToken": "rotated"})
					case "unauthorized":
						w.WriteHeader(http.StatusUnauthorized)
						_, _ = w.Write([]byte(`{}`))
					case "invalid":
						_, _ = w.Write([]byte(`{"token":"incomplete"}`))
					}
				})}
				if _, err := client.Request(ctx, 1, "GET", "/read", nil); err == nil {
					t.Error("stale refresh permitted a request")
				}
				if reads != 0 {
					t.Fatal("stale session reached upstream")
				}
				current, err := store.Session(ctx, 1)
				if change == "disconnect" {
					if !errors.Is(err, sql.ErrNoRows) {
						t.Fatal("refresh resurrected disconnected session", err)
					}
					return
				}
				if err != nil || current.Token != original.Token || current.Refresh != original.Refresh || current.UserID != original.UserID {
					t.Fatal("refresh overwrote concurrent session change", current.State, err)
				}
				if change == "replacement" {
					if current.Generation != "replacement" || current.State != "connected" {
						t.Fatal("old refresh altered new login", current.State)
					}
				} else if current.State != change {
					t.Fatal("refresh overwrote pause state", current.State)
				}
				var reason string
				if err := store.DB.QueryRowContext(ctx, "SELECT last_error FROM sessions WHERE owner_id=1").Scan(&reason); err != nil {
					t.Fatal(err)
				}
				if change != "replacement" && reason != "concurrent-change" {
					t.Fatal("refresh overwrote pause reason", reason)
				}
			})
		}
	}
}

func TestSharedClientSessionFileLockCancels(t *testing.T) {
	store, cfg := testStore(t)
	first := NewClient(store, cfg)
	lock, err := first.sessionFileLock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	other, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	second := NewClient(other, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := second.token(ctx, 1, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting for another process did not respect cancellation", err)
	}
	info, err := os.Stat(filepath.Join(cfg.DataDir, "session-1.lock"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("session lock permissions changed", err)
	}
}

func TestSharedClientLoginWaitsForRefreshAndReloadsBinding(t *testing.T) {
	store, cfg, login, raw := atomicLoginFixture(t, false)
	other, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := store.SaveSession(ctx, 1, Session{Token: "expired", Refresh: "original", UserID: 9, Generation: "old", Expires: login.Now().Add(-time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	entered, loginEntered, release := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
	transport := handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/appuser/refresh":
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			_, _ = w.Write([]byte(`{"token":"refreshed","refreshToken":"rotated"}`))
		case "/appuser/login/telegram":
			loginEntered <- struct{}{}
			_, _ = w.Write([]byte(validAtomicLogin))
		case "/user/getMe":
			_, _ = w.Write([]byte(`{"userId":5}`))
		}
	})}
	refresh := NewClient(other, cfg)
	refresh.Now = login.Now
	refresh.HTTP.Transport, login.HTTP.Transport = transport, transport
	refreshDone, loginDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := refresh.token(ctx, 1, ""); refreshDone <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("refresh did not start")
	}
	go func() { _, err := login.LoginAccount(ctx, 1, raw); loginDone <- err }()
	select {
	case <-loginEntered:
		t.Error("login raced refresh HTTP")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-refreshDone; err != nil {
		t.Error(err)
	}
	if err := <-loginDone; err != nil {
		t.Fatal("login did not reload refreshed binding", err)
	}
	current, err := store.Session(ctx, 1)
	if err != nil || current.UserID != 5 || current.Token != "candidate-access-token" || current.Generation == "old" {
		t.Fatal("refresh overwrote new login", err)
	}
}

func TestSharedClientSessionFileLockRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			store, cfg := testStore(t)
			path := filepath.Join(cfg.DataDir, "session-1.lock")
			var err error
			if kind == "symlink" {
				err = os.Symlink(filepath.Join(cfg.DataDir, "jardulik.sqlite"), path)
			} else {
				err = os.Mkdir(path, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if lock, err := NewClient(store, cfg).sessionFileLock(context.Background(), 1); err == nil {
				lock.Close()
				t.Fatal("accepted unsafe lock file")
			}
		})
	}
}
