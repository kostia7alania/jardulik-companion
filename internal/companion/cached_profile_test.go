package companion

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func cachedProfileFixture(t *testing.T, source string) (*Service, *[]uxMessage, StoredProfile, *int) {
	t.Helper()
	s, sent := uxFixture(t)
	ctx := context.Background()
	if err := s.Store.SaveSession(ctx, 1, Session{UserID: 101, Token: "offline-cached-profile", Generation: "cached-profile", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	// Rich fields are deliberately insufficient proof without a full-source observation.
	if _, err := s.Store.Archive(ctx, 1, []Profile{{"userId": 77, "name": "Saved", "age": 26, "height": 165, "description": "Stored biography", "birthday": "2000-01-01", "country": "Russia", "city": "Moscow", "photos": []string{}, "miniPhotos": []string{}}}, source); err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.Profile(ctx, 1, 77)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/user/getUser" || r.URL.Query().Get("viewerUserId") != "101" || r.URL.Query().Get("targetUserId") != "77" {
			t.Fatalf("cache navigation attempted another source request: %s %s", r.Method, r.URL.Path)
		}
		fmt.Fprint(w, `{"userId":77,"name":"Hydrated","age":26,"height":165,"description":"Full source biography"}`)
	})}
	return s, sent, p, &calls
}

func assertCachedProfileDelivery(t *testing.T, s *Service, sent *[]uxMessage, wantName string) {
	t.Helper()
	if len(*sent) == 0 || !strings.Contains((*sent)[len(*sent)-1].Text, wantName) {
		t.Fatal("saved profile was not delivered")
	}
	var delivered, actions int
	if err := s.Store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=77").Scan(&delivered); err != nil || delivered != 1 {
		t.Fatal("normal delivery queue semantics changed", err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("viewing a cached profile created a reaction", err)
	}
}

func TestCachedProfileExactFullSourcesAvoidNextHTTP(t *testing.T) {
	for _, source := range []string{"GET /user/getUser", "GET /user/getUser (crawl catalog)", "GET /user/getUser (autolike)", "profile_watch", "incoming_viewed", "mutual_viewed"} {
		t.Run(source, func(t *testing.T) {
			s, sent, _, calls := cachedProfileFixture(t, source)
			s.next(context.Background(), 1)
			if *calls != 0 {
				t.Fatal("confirmed full version was downloaded again")
			}
			assertCachedProfileDelivery(t, s, sent, "Saved")
		})
	}
}

func TestCachedProfileUnknownSourcesHydrateOnlyOnceWithoutGuessingFields(t *testing.T) {
	for _, source := range []string{"GET /user/getUsers", "GET /user/getUsers (crawl)", "GET /user/getUser (crawl catalog) extra", "profile_catalog"} {
		t.Run(source, func(t *testing.T) {
			s, sent, _, calls := cachedProfileFixture(t, source)
			s.next(context.Background(), 1)
			if *calls != 1 {
				t.Fatal("unproven preview skipped normal hydration")
			}
			assertCachedProfileDelivery(t, s, sent, "Hydrated")
			if _, err := s.Store.DB.Exec("UPDATE queue SET delivered=0 WHERE owner_id=1 AND profile_id=77"); err != nil {
				t.Fatal(err)
			}
			s.next(context.Background(), 1)
			if *calls != 1 {
				t.Fatal("later view repeated successful hydration")
			}
			assertCachedProfileDelivery(t, s, sent, "Hydrated")
		})
	}
}

func TestCachedProfileFullProofIsOwnerProfileAndVersionScoped(t *testing.T) {
	s, sent, original, calls := cachedProfileFixture(t, "GET /user/getUser")
	ctx := context.Background()
	if _, err := s.Store.Archive(ctx, 2, []Profile{{"userId": 77, "name": "Other owner"}}, "GET /user/getUser"); err != nil {
		t.Fatal(err)
	}
	for _, identity := range [][3]int64{{2, 77, original.VersionID}, {1, 78, original.VersionID}, {1, 77, original.VersionID + 1000}} {
		if full, err := s.Store.ProfileHasFullCapture(ctx, identity[0], identity[1], identity[2]); err != nil || full {
			t.Fatal("full-capture proof escaped its owner/profile/version", identity, err)
		}
	}
	if _, err := s.Store.Archive(ctx, 1, []Profile{{"userId": 77, "name": "New partial version"}}, "GET /user/getUsers"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.Favorite(ctx, 1, 77); err != nil {
		t.Fatal(err)
	}
	s.next(ctx, 1)
	if *calls != 1 {
		t.Fatal("an older full capture or another owner made the new preview look complete")
	}
	assertCachedProfileDelivery(t, s, sent, "Hydrated")
	var otherDelivered, favorites int
	if err := s.Store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=2 AND profile_id=77").Scan(&otherDelivered); err != nil || otherDelivered != 0 {
		t.Fatal("another owner's queue changed", err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM favorites WHERE owner_id=1 AND profile_id=77").Scan(&favorites); err != nil || favorites != 1 {
		t.Fatal("hydration changed favorites", err)
	}
}

func TestCachedProfilePausedSessionStaysPausedWithoutSourceHTTP(t *testing.T) {
	for _, source := range []string{"GET /user/getUser", "GET /user/getUsers"} {
		t.Run(source, func(t *testing.T) {
			s, sent, _, calls := cachedProfileFixture(t, source)
			ctx := context.Background()
			s.Store.SessionState(ctx, 1, "paused", "fixture-challenge")
			s.next(ctx, 1)
			session, err := s.Store.Session(ctx, 1)
			if err != nil || session.State != "paused" || *calls != 0 {
				t.Fatal("cached navigation resumed or contacted the paused session", err)
			}
			assertCachedProfileDelivery(t, s, sent, "Saved")
		})
	}
}

func TestCachedProfileProofReadErrorDoesNotFallBackToSourceHTTP(t *testing.T) {
	s, sent, _, calls := cachedProfileFixture(t, "GET /user/getUsers")
	if _, err := s.Store.DB.Exec("DROP TABLE observations"); err != nil {
		t.Fatal(err)
	}
	s.next(context.Background(), 1)
	if *calls != 0 {
		t.Fatal("cache proof read failure triggered a source request")
	}
	assertCachedProfileDelivery(t, s, sent, "Saved")
}
