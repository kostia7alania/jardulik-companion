package companion

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type pauseMediaTransport func(*http.Request) (*http.Response, error)

func (f pauseMediaTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func pauseMediaFixture(t *testing.T) (*Store, *MediaManager, []MediaJob, *int) {
	t.Helper()
	s, cfg := testStore(t)
	ctx := context.Background()
	var jobs []MediaJob
	for _, owner := range []int64{1, 2} {
		if err := s.User(ctx, owner, "test"); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveSession(ctx, owner, Session{Token: "test-access", Refresh: "test-refresh", UserID: owner + 100}); err != nil {
			t.Fatal(err)
		}
		ids, err := s.Archive(ctx, owner, []Profile{{"userId": owner + 200, "photos": []string{"https://mini.jardulik.app/photo.webp"}}}, "test")
		if err != nil {
			t.Fatal(err)
		}
		media, err := s.MediaJobs(ctx, owner, ids[0])
		if err != nil || len(media) != 1 {
			t.Fatalf("media fixture: %v %v", media, err)
		}
		jobs = append(jobs, media[0])
	}
	calls := new(int)
	m := NewMediaManager(s, cfg)
	m.HTTP.Transport = pauseMediaTransport(func(r *http.Request) (*http.Response, error) {
		*calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"image/webp"}}, Body: io.NopCloser(strings.NewReader("whole test source bytes")), Request: r}, nil
	})
	return s, m, jobs, calls
}

func TestPausedBackgroundMediaSkipsOwnerAndContinuesOthers(t *testing.T) {
	s, m, jobs, calls := pauseMediaFixture(t)
	ctx := context.Background()
	s.Pause(ctx, jobs[0].OwnerID, true)
	selected, err := s.PendingMedia(ctx)
	if err != nil || selected.ID != jobs[1].ID {
		t.Fatalf("paused owner blocked or entered background queue: %+v %v", selected, err)
	}
	if err = m.ensure(ctx, selected, true); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 {
		t.Fatalf("active owner's photo was not fetched once: %d", *calls)
	}
	if _, err = s.PendingMedia(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("paused job remained eligible: %v", err)
	}
	s.Pause(ctx, jobs[0].OwnerID, false)
	selected, err = s.PendingMedia(ctx)
	if err != nil || selected.ID != jobs[0].ID {
		t.Fatalf("resume did not restore pending job: %+v %v", selected, err)
	}
}

func TestBackgroundMediaRechecksPauseAfterSelection(t *testing.T) {
	s, m, jobs, calls := pauseMediaFixture(t)
	ctx := context.Background()
	selected, err := s.PendingMedia(ctx)
	if err != nil || selected.ID != jobs[0].ID {
		t.Fatalf("oldest active media not selected: %+v %v", selected, err)
	}
	s.Pause(ctx, selected.OwnerID, true)
	if err = m.ensure(ctx, selected, true); err != nil {
		t.Fatal(err)
	}
	var state, reason string
	var attempts int
	if err = s.DB.QueryRowContext(ctx, "SELECT state,attempts,error FROM media_jobs WHERE id=?", selected.ID).Scan(&state, &attempts, &reason); err != nil {
		t.Fatal(err)
	}
	if *calls != 0 || state != "pending" || attempts != 0 || reason != "" {
		t.Fatalf("paused claim fetched or penalized: requests=%d state=%s attempts=%d error=%s", *calls, state, attempts, reason)
	}
}

func TestManualProfileMediaRemainsAvailableWhilePaused(t *testing.T) {
	s, m, jobs, calls := pauseMediaFixture(t)
	ctx := context.Background()
	manual := jobs[0]
	s.Pause(ctx, manual.OwnerID, true)
	// Deliver uses this same explicit path for /next and opening a saved profile.
	if err := m.EnsureProfile(ctx, manual.OwnerID, manual.VersionID); err != nil {
		t.Fatal(err)
	}
	stored, err := s.MediaJobs(ctx, manual.OwnerID, manual.VersionID)
	if err != nil || len(stored) != 1 || stored[0].State != "complete" || *calls != 1 {
		t.Fatalf("manual delivery was paused: jobs=%+v requests=%d error=%v", stored, *calls, err)
	}
	if !s.Paused(ctx, manual.OwnerID) {
		t.Fatal("manual delivery resumed background activity")
	}
}
