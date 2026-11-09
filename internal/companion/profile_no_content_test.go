package companion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func profileTestResponse(r *http.Request, status int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func TestProfileNoContentFullProfileKeepsSourceConnected(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprint(frozen), func(t *testing.T) {
			f := newDebugFixture(t)
			client := f.service.Client
			observed := client.Now()
			client.Now = func() time.Time { return observed }
			base := client.HTTP.Transport
			calls := 0
			client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet {
					t.Fatal("profile read attempted mutation")
				}
				if r.URL.Path == "/api/user/getUser" {
					calls++
					return profileTestResponse(r, 204, "", ""), nil
				}
				return base.RoundTrip(r)
			})
			ctx := context.Background()
			ss, err := client.Store.Session(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if frozen {
				_, err = client.FullProfileFrozen(ctx, 1, ss, 7)
			} else {
				_, err = client.FullProfile(ctx, 1, 7)
			}
			if !errors.Is(err, ErrProfileUnavailable) || calls != 1 {
				t.Fatal("missing profile was not classified", err, calls)
			}
			assertMissingProfileRecorded(t, client.Store, 1, 7, observed, 1)
			current, err := client.Store.Session(ctx, 1)
			if err != nil || current.State != "connected" || current.Generation != ss.Generation {
				t.Fatal("missing profile paused source", err)
			}
		})
	}
}

func TestProfileNoContentDebugReadPreservesRawStatusAndSource(t *testing.T) {
	f := newDebugFixture(t)
	client := f.service.Client
	observed := client.Now()
	client.Now = func() time.Time { return observed }
	client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/user/getUser" {
			t.Fatal("unexpected request")
		}
		return profileTestResponse(r, 204, "", " \n"), nil
	})
	ctx := context.Background()
	ss, _ := client.Store.Session(ctx, 1)
	status, raw, err := client.debugRead(ctx, 1, ss, "/user/getUser?viewerUserId=101&targetUserId=7")
	if status != 204 || string(raw) != " \n" || !errors.Is(err, ErrProfileUnavailable) {
		t.Fatal("debug response was changed", status, raw, err)
	}
	assertMissingProfileRecorded(t, client.Store, 1, 7, observed, 1)
	current, err := client.Store.Session(ctx, 1)
	if err != nil || current.State != "connected" {
		t.Fatal("debug missing profile paused source", err)
	}
}

func TestProfileNoContentDoesNotLoosenSourceStops(t *testing.T) {
	cases := []struct {
		name, method, path, contentType, body string
		status                                int
	}{
		{"html200", "GET", "/user/getUser", "text/html", "<html>challenge</html>", 200},
		{"empty200", "GET", "/user/getUser", "application/json", "", 200},
		{"html204", "GET", "/user/getUser", "TEXT/HTML; charset=utf-8", "", 204},
		{"body204", "GET", "/user/getUser", "application/json", "<html>challenge</html>", 204},
		{"denied", "GET", "/user/getUser", "application/json", "{}", 403},
		{"limited", "GET", "/user/getUser", "application/json", "{}", 429},
		{"account204", "GET", "/user/getMe", "", "", 204},
		{"gpt204", "POST", "/GPT/checkCompatibility", "", "", 204},
		{"otherPath204", "GET", "/user/getUserExtra", "", "", 204},
		{"otherMethod204", "POST", "/user/getUser", "", "", 204},
	}
	for _, tc := range cases {
		for _, debug := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/debug=%v", tc.name, debug), func(t *testing.T) {
				f := newDebugFixture(t)
				client := f.service.Client
				calls := 0
				client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
					calls++
					return profileTestResponse(r, tc.status, tc.contentType, tc.body), nil
				})
				ctx := context.Background()
				var err error
				if debug {
					ss, _ := client.Store.Session(ctx, 1)
					if tc.method == http.MethodGet {
						_, _, err = client.debugRead(ctx, 1, ss, tc.path)
					} else {
						_, _, err = client.debugPost(ctx, 1, ss, tc.path, []byte(`{}`))
					}
				} else {
					_, err = client.Request(ctx, 1, tc.method, tc.path, nil)
				}
				if err == nil || errors.Is(err, ErrProfileUnavailable) || calls != 1 {
					t.Fatal("source stop weakened", err, calls)
				}
				current, e := client.Store.Session(ctx, 1)
				if e != nil || current.State != "paused" {
					t.Fatal("source was not paused", e, current.State)
				}
			})
		}
	}
}

func TestProfileNoContentCompatibilityCancelsUnavailableWithoutPost(t *testing.T) {
	for _, status := range []int{204, 404, 410} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newCompatibilityExecutorFixture(t)
			base := f.service.Client.HTTP.Transport
			f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				if strings.TrimPrefix(r.URL.Path, "/api") == "/user/getUser" {
					f.requests = append(f.requests, r.Method+" "+r.URL.Path)
					return profileTestResponse(r, status, "", ""), nil
				}
				return base.RoundTrip(r)
			})
			if ran, err := f.service.compatibilityStep(context.Background()); err != nil || ran {
				t.Fatal("unavailable target was dispatched", ran, err)
			}
			var reason string
			if err := f.service.Store.DB.QueryRow("SELECT reason FROM compatibility_jobs ORDER BY id DESC LIMIT 1").Scan(&reason); err != nil {
				t.Fatal(err)
			}
			if f.state(t) != "cancelled" || reason != "target_unavailable" || f.posts != 0 {
				t.Fatal("unavailable job was not cancelled", f.state(t), reason, f.posts)
			}
			assertMissingProfileRecorded(t, f.service.Store, 1, 7, f.now, 1)
			current, err := f.service.Store.Session(context.Background(), 1)
			if err != nil || current.State != "connected" {
				t.Fatal("unavailable target paused source", err)
			}
		})
	}
}

func TestProfileNoContentHydrationContinuesToNextKnownTarget(t *testing.T) {
	crawler, now, source := locationFixture(t)
	source.catalog = func(*http.Request) (string, int) { return locationBody(2, 201, 202), 200 }
	source.full = func(r *http.Request) (string, int) {
		if r.URL.Query().Get("targetUserId") == "201" {
			return "", 204
		}
		return `{"userId":202,"name":"Full"}`, 200
	}
	startLocationPhase(t, crawler, now)
	crawlStep(t, crawler, now)
	crawlStep(t, crawler, now)
	job := crawlJob(t, crawler)
	if job.Unavailable != 1 || job.Hydrated != 0 || job.State == "stopped" {
		t.Fatal("missing target stopped hydration", job)
	}
	crawlStep(t, crawler, now)
	job = crawlJob(t, crawler)
	if job.State != "done" || job.Unavailable != 1 || job.Hydrated != 1 || !reflect.DeepEqual(source.targets, []int64{201, 202}) {
		t.Fatal("hydration did not advance", job, source.targets)
	}
	current, err := crawler.Service.Store.Session(context.Background(), 1)
	if err != nil || current.State != "connected" {
		t.Fatal("hydration paused source", err)
	}
}
