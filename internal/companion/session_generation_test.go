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
