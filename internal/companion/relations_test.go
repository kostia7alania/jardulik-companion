package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func relationFixture(t *testing.T) (*Service, *[]uxMessage, *[]string) {
	t.Helper()
	s, sent := uxFixture(t)
	for _, owner := range []int64{1, 2} {
		if err := s.Store.SaveSession(context.Background(), owner, Session{Token: fmt.Sprintf("token-%d", owner), Refresh: "test-refresh", UserID: owner + 100, Generation: fmt.Sprintf("g-%d", owner), Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	s.Media.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		t.Fatalf("relationship listing unexpectedly downloaded an image: %s", r.URL.Host)
		return nil, errors.New("no image download allowed")
	})
	calls := []string{}
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.URL.Path)
		if r.Method != "GET" {
			t.Fatalf("relationship feature attempted action: %s %s", r.Method, r.URL.Path)
		}
		body := ""
		switch {
		case strings.HasSuffix(r.URL.Path, "/getReactUsers"):
			if r.URL.Query().Get("pageSize") != "1000" || r.URL.Query().Get("userId") != "101" {
				t.Fatal("wrong owner/source query")
			}
			body = `[{"userId":77,"name":"Preview","age":24,"height":170,"type":1,"isFire":true,"fireText":"context only","reactionView":true,"miniPhotos":"https://mini.jardulik.app/preview.jpg"}]`
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			body = `{"userId":101,"type":1}`
		case strings.HasSuffix(r.URL.Path, "/getUser"):
			body = `{"userId":77,"name":"Full","age":24,"description":"Complete biography","unknown_source_field":"preserved"}`
		default:
			t.Fatalf("unexpected API request %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	return s, sent, &calls
}

func TestRelationsPreviewSavedBeforeDisplayWithoutHydration(t *testing.T) {
	s, sent, calls := relationFixture(t)
	s.OpenRelations(context.Background(), 1, false)
	if len(*calls) != 1 || !strings.HasSuffix((*calls)[0], "/getReactUsers") || len(*sent) != 1 {
		t.Fatalf("list hydrated or did not display: %v", *calls)
	}
	if watchCount(t, s.Store, "versions") != 0 || watchCount(t, s.Store, "media_jobs") != 0 || watchCount(t, s.Store, "relation_batches") != 1 || watchCount(t, s.Store, "relation_profiles") != 1 {
		t.Fatal("partial preview entered archive or was not preserved")
	}
	var raw string
	if err := s.Store.DB.QueryRow("SELECT raw_json FROM relation_batches").Scan(&raw); err != nil || !strings.Contains(raw, "context only") {
		t.Fatal("source relation context lost")
	}
	if err := s.Store.KnownProfile(context.Background(), 1, 77); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.KnownProfile(context.Background(), 2, 77); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("relation provenance leaked to another owner")
	}
	if !strings.HasPrefix((*sent)[0].Markup.Rows[0][0].Data, "ri:") || !strings.Contains((*sent)[0].Text, "Загружено записей: 1") {
		t.Fatal("list omitted explicit loaded count or open button")
	}
	if watchCount(t, s.Store, "actions") != 0 {
		t.Fatal("listing created reaction action")
	}
}

func TestRelationsFullClickArchivesViewedAndOwnerGuard(t *testing.T) {
	s, sent, calls := relationFixture(t)
	s.OpenRelations(context.Background(), 1, true)
	callback := (*sent)[0].Markup.Rows[0][0].Data
	if !s.HandleRelationCallback(context.Background(), 2, callback) || len(*calls) != 1 {
		t.Fatal("foreign owner hydrated another user's relationship")
	}
	if !s.HandleRelationCallback(context.Background(), 1, callback) {
		t.Fatal("full callback not handled")
	}
	if len(*calls) != 2 || !strings.HasSuffix((*calls)[1], "/getUser") {
		t.Fatalf("mutual click gated or hydrated unexpectedly: %v", *calls)
	}
	stored, err := s.Store.Profile(context.Background(), 1, 77)
	if err != nil || stored.Profile.String("name") != "Full" || stored.Profile.String("unknown_source_field") != "preserved" {
		t.Fatalf("full source profile not preserved: %+v %v", stored, err)
	}
	if _, err = s.Store.Next(context.Background(), 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("viewed relation was added to discovery queue")
	}
	if watchCount(t, s.Store, "actions") != 0 {
		t.Fatal("full view sent a reaction")
	}
}

func TestRelationsIncomingChecksViewerPremiumNotTargetOrFire(t *testing.T) {
	s, sent, calls := relationFixture(t)
	s.OpenRelations(context.Background(), 1, false)
	callback := (*sent)[0].Markup.Rows[0][0].Data
	base := s.Client.HTTP.Transport
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			*calls = append(*calls, r.URL.Path)
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"userId":101,"type":0}`)), Request: r}, nil
		}
		return base.RoundTrip(r)
	})
	s.HandleRelationCallback(context.Background(), 1, callback)
	if len(*calls) != 2 || !strings.HasSuffix((*calls)[1], "/getMe") || watchCount(t, s.Store, "versions") != 0 {
		t.Fatal("target Premium/isFire bypassed viewer gate")
	}
	if !strings.Contains((*sent)[len(*sent)-1].Text, "Premium Jardulik") {
		t.Fatal("viewer gate was not explained")
	}
}

func TestRelationsStrictSourceArrayAndDisconnect(t *testing.T) {
	for _, body := range []string{`{"users":[]}`, `[{"name":"missing id"}]`, `[1]`, `[] {}`} {
		t.Run(body, func(t *testing.T) {
			if _, err := decodeRelations([]byte(body)); err == nil {
				t.Fatal("invalid relationship shape accepted")
			}
		})
	}
	for _, body := range []string{`null`, `[]`} {
		if rows, err := decodeRelations([]byte(body)); err != nil || len(rows) != 0 {
			t.Fatal("empty source list rejected")
		}
	}
	s, _, _ := relationFixture(t)
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if err := s.Store.Disconnect(r.Context(), 1); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[{"userId":77}]`)), Request: r}, nil
	})
	if _, err := s.Client.ReactUsers(context.Background(), 1, false, 1); err == nil {
		t.Fatal("disconnected response was retained")
	}
	if err := s.Store.initRelations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, s.Store, "relation_batches") != 0 {
		t.Fatal("stale response became provenance")
	}
}

func TestArchiveViewedPreservesExistingDiscoveryState(t *testing.T) {
	s, _, _ := relationFixture(t)
	ctx := context.Background()
	if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 10, "name": "Viewed"}}, "view"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.Next(ctx, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("new viewed profile queued for discovery")
	}
	if _, err := s.Store.Archive(ctx, 1, []Profile{{"userId": 20, "name": "Pending"}}, "discovery"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 20, "name": "Updated full"}}, "view"); err != nil {
		t.Fatal(err)
	}
	var delivered int
	if err := s.Store.DB.QueryRow("SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=20").Scan(&delivered); err != nil || delivered != 0 {
		t.Fatal("pending discovery marked delivered before actual delivery")
	}
	next, err := s.Store.Next(ctx, 1)
	if err != nil || next.ID != 20 {
		t.Fatal("existing discovery queue was changed")
	}
	if err := s.Store.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.KnownProfile(ctx, 2, 10); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("deletion/owner provenance invalid")
	}
}

func TestRelationsPaginationDoesNotFetchNextSourcePageEarly(t *testing.T) {
	s, sent, calls := relationFixture(t)
	var profiles []Profile
	for i := 1; i <= 1000; i++ {
		profiles = append(profiles, Profile{"userId": i, "name": "Preview"})
	}
	body, _ := json.Marshal(profiles)
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		*calls = append(*calls, r.URL.Query().Get("pageNumber"))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
	})
	s.OpenRelations(context.Background(), 1, false)
	message := (*sent)[len(*sent)-1]
	next := message.Markup.Rows[10][0].Data
	if len(*calls) != 1 || len(message.Markup.Rows) != 11 {
		t.Fatal("first relation page overfetched")
	}
	s.HandleRelationCallback(context.Background(), 1, next)
	if len(*calls) != 1 {
		t.Fatal("cached next ten triggered another source request")
	}
	var batch int64
	if err := s.Store.DB.QueryRow("SELECT id FROM relation_batches").Scan(&batch); err != nil {
		t.Fatal(err)
	}
	s.HandleRelationCallback(context.Background(), 1, fmt.Sprintf("rl:%d:1000", batch))
	if len(*calls) != 2 || (*calls)[1] != "2" {
		t.Fatal("next source page was not loaded on demand")
	}
}

func TestRelationsPremiumIncomingHydratesOnlyAfterFreshCheck(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			s, sent, calls := relationFixture(t)
			s.OpenRelations(context.Background(), 1, false)
			data := (*sent)[0].Markup.Rows[0][0].Data
			if failed {
				base := s.Client.HTTP.Transport
				s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/getMe") {
						*calls = append(*calls, r.URL.Path)
						return &http.Response{StatusCode: 500, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
					}
					return base.RoundTrip(r)
				})
			}
			s.HandleRelationCallback(context.Background(), 1, data)
			expected := 3
			if failed {
				expected = 2
			}
			if len(*calls) != expected {
				t.Fatalf("unexpected incoming hydration calls: %v", *calls)
			}
			if !failed && watchCount(t, s.Store, "versions") != 1 {
				t.Fatal("Premium viewer did not get the full archived snapshot")
			}
			if failed && watchCount(t, s.Store, "versions") != 0 {
				t.Fatal("failed fresh plan check hydrated or trusted cache")
			}
		})
	}
}

func TestRelationsSourceErrorDoesNotCreateProvenanceAndDeleteCascades(t *testing.T) {
	s, _, _ := relationFixture(t)
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
	})
	if _, err := s.Client.ReactUsers(context.Background(), 1, false, 1); err == nil {
		t.Fatal("failed source request accepted")
	}
	if err := s.Store.initRelations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, s.Store, "relation_batches") != 0 {
		t.Fatal("source failure was cached")
	}
	s, _, _ = relationFixture(t)
	if _, err := s.Client.ReactUsers(context.Background(), 1, true, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteUser(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, s.Store, "relation_batches") != 0 || watchCount(t, s.Store, "relation_profiles") != 0 {
		t.Fatal("deleted user's relationship provenance survived")
	}
}

func TestRelationsDiskReserveAndMalformedPageDoNotRequest(t *testing.T) {
	s, _, calls := relationFixture(t)
	s.Store.cfg.MinFreeBytes = ^uint64(0)
	if _, err := s.Client.ReactUsers(context.Background(), 1, false, 1); err == nil || len(*calls) != 0 {
		t.Fatal("low disk collected another relationship page")
	}
	if _, err := s.Client.ReactUsers(context.Background(), 1, false, 0); err == nil || len(*calls) != 0 {
		t.Fatal("invalid page reached source")
	}
}
