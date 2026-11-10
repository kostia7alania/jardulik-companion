package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestStaleRequestCannotPauseOrRefreshNewLogin(t *testing.T) {
	for _, status := range []int{401, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, c := testStore(t)
			ctx := context.Background()
			_ = s.User(ctx, 1, "")
			if err := s.SaveSession(ctx, 1, Session{Token: "old", Refresh: "old-r", UserID: 5, Generation: "old-login"}); err != nil {
				t.Fatal(err)
			}
			client := NewClient(s, c)
			calls := 0
			client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if strings.Contains(r.URL.Path, "refresh") {
					t.Fatal("old request refreshed a new login")
				}
				if err := s.Disconnect(ctx, 1); err != nil {
					t.Fatal(err)
				}
				if err := s.SaveSession(ctx, 1, Session{Token: "fresh", Refresh: "fresh-r", UserID: 5, Generation: "new-login"}); err != nil {
					t.Fatal(err)
				}
				w.WriteHeader(status)
				fmt.Fprint(w, `{}`)
			})}
			if _, err := client.Request(ctx, 1, "GET", "/user/getMe?userId=5", nil); err == nil {
				t.Fatal("old failure became success")
			}
			fresh, err := s.Session(ctx, 1)
			if err != nil || fresh.State != "connected" || fresh.Generation != "new-login" || calls != 1 {
				t.Fatalf("new session mutated: %+v %v calls=%d", fresh, err, calls)
			}
		})
	}
}

func TestStaleAccountResponseCannotOverwriteNewLogin(t *testing.T) {
	s, c := testStore(t)
	ctx := context.Background()
	_ = s.User(ctx, 1, "")
	_ = s.SaveSession(ctx, 1, Session{Token: "old", Refresh: "old-r", UserID: 5, Generation: "old-login"})
	client := NewClient(s, c)
	client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = s.Disconnect(ctx, 1)
		_ = s.SaveSession(ctx, 1, Session{Token: "fresh", Refresh: "fresh-r", UserID: 5, Generation: "new-login"})
		_ = s.SaveAccount(ctx, 1, Profile{"userId": 5, "description": "fresh account"})
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"userId":5,"description":"stale account"}`)
	})}
	if _, err := client.Me(ctx, 1); err == nil {
		t.Fatal("old account result accepted")
	}
	fresh, _ := s.Session(ctx, 1)
	var p Profile
	_ = json.Unmarshal(fresh.Account, &p)
	if p.String("description") != "fresh account" {
		t.Fatal("fresh account overwritten")
	}
}

func TestProviderReadFailureCannotPauseRotatedRefreshToken(t *testing.T) {
	for _, debug := range []bool{false, true} {
		t.Run(fmt.Sprintf("debug=%t", debug), func(t *testing.T) {
			store, cfg := testStore(t)
			other, err := OpenStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			ctx := context.Background()
			if err := store.User(ctx, 1, ""); err != nil {
				t.Fatal(err)
			}
			initial := Session{Token: "access", Refresh: "before", UserID: 5, Generation: "same-login"}
			if err := store.SaveSession(ctx, 1, initial); err != nil {
				t.Fatal(err)
			}
			client := NewClient(store, cfg)
			client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+initial.Token {
					t.Fatal("request did not use the initial token")
				}
				rotated := initial
				rotated.Refresh = "after"
				if err := other.SaveSession(ctx, 1, rotated); err != nil {
					t.Fatal(err)
				}
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{}`))
			})}
			if debug {
				_, _, err = client.debugRead(ctx, 1, initial, "/read")
			} else {
				_, err = client.Request(ctx, 1, http.MethodGet, "/read", nil)
			}
			if err == nil {
				t.Fatal("provider failure became success")
			}
			current, err := store.Session(ctx, 1)
			if err != nil || current.State != "connected" || current.Refresh != "after" {
				t.Fatalf("provider failure changed the rotated session: state=%s, err=%v", current.State, err)
			}
		})
	}
}

func TestProviderReactionFailureCannotPauseReplacementSession(t *testing.T) {
	s, _, _ := manualLikeFixture(t, `{"userId":101}`, `{}`)
	other, err := OpenStore(s.Store.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx := context.Background()
	base := s.Client.HTTP.Transport
	posts := 0
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/reactions/addNew" {
			return base.RoundTrip(r)
		}
		posts++
		if err := other.SaveSession(ctx, 1, Session{Token: "fresh", Refresh: "fresh-refresh", UserID: 101, Generation: "replacement"}); err != nil {
			t.Fatal(err)
		}
		return albumTestResponse(r, http.StatusUnauthorized, `{}`), nil
	})
	if _, err := s.Client.Like(ctx, 1, 1); err == nil || posts != 1 {
		t.Fatalf("expected one failed reaction POST, got %d, err=%v", posts, err)
	}
	current, err := s.Store.Session(ctx, 1)
	if err != nil || current.State != "connected" || current.Generation != "replacement" {
		t.Fatalf("reaction failure changed the replacement session: state=%s, err=%v", current.State, err)
	}
	var actionState string
	if err := s.Store.DB.QueryRowContext(ctx, `SELECT state FROM actions WHERE owner_id=1 AND profile_id=1`).Scan(&actionState); err != nil || actionState != "unknown" || !s.Store.Paused(ctx, 1) {
		t.Fatalf("failed POST did not keep its retry guard: state=%s, err=%v", actionState, err)
	}
}

func TestProviderReadFailurePausesSessionAfterRefresh(t *testing.T) {
	store, cfg := testStore(t)
	ctx := context.Background()
	if err := store.User(ctx, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSession(ctx, 1, Session{Token: "old", Refresh: "before", UserID: 5, Generation: "same-login"}); err != nil {
		t.Fatal(err)
	}
	client := NewClient(store, cfg)
	client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/appuser/refresh" {
			_, _ = w.Write([]byte(`{"token":"fresh","refreshToken":"after"}`))
			return
		}
		switch r.Header.Get("Authorization") {
		case "Bearer old":
			w.WriteHeader(http.StatusUnauthorized)
		case "Bearer fresh":
			w.WriteHeader(http.StatusForbidden)
		default:
			t.Fatal("request used an unexpected token")
		}
		_, _ = w.Write([]byte(`{}`))
	})}
	if _, err := client.Request(ctx, 1, http.MethodGet, "/read", nil); err == nil {
		t.Fatal("provider failure became success")
	}
	current, err := store.Session(ctx, 1)
	if err != nil || current.State != "paused" || current.Refresh != "after" {
		t.Fatalf("retry failure did not pause the refreshed session: state=%s, err=%v", current.State, err)
	}
}

func TestStableRequestHeaders(t *testing.T) {
	for _, image := range []bool{false, true} {
		r, _ := http.NewRequest("GET", "https://mini.jardulik.app/image.webp", nil)
		upstreamHeaders(r, "", image)
		if r.UserAgent() != defaultUserAgent || strings.Contains(r.UserAgent(), "JardulikCompanion") {
			t.Fatal("unexpected client marker")
		}
		if image {
			if r.Header.Get("Sec-Fetch-Dest") != "image" || r.Header.Get("Sec-Fetch-Site") != "same-origin" {
				t.Fatal(r.Header)
			}
		} else {
			if r.Header.Get("Origin") != "https://mini.jardulik.app" || r.Header.Get("Sec-Fetch-Mode") != "cors" {
				t.Fatal(r.Header)
			}
		}
	}
}
