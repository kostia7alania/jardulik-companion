package companion

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func assertMissingProfileRecorded(t *testing.T, store *Store, owner, profile int64, observed time.Time, events int) {
	t.Helper()
	var availability, reason, changed string
	if err := store.DB.QueryRow("SELECT availability,reason,changed_at FROM profile_presence WHERE owner_id=? AND profile_id=?", owner, profile).Scan(&availability, &reason, &changed); err != nil {
		t.Fatal(err)
	}
	if availability != "unavailable" || reason != "direct_unavailable" || changed != observed.UTC().Format(time.RFC3339Nano) {
		t.Fatal("missing profile or detection time not saved", availability, reason, changed)
	}
	var count int
	if err := store.DB.QueryRow("SELECT COUNT(*) FROM profile_presence_events WHERE owner_id=? AND profile_id=?", owner, profile).Scan(&count); err != nil || count != events {
		t.Fatal("missing transition lost or duplicated", count, err)
	}
}

func TestProfileObservationDeduplicatesEmptyJSONAndKeepsDetectionTime(t *testing.T) {
	for _, body := range []string{"null", "[]", "[ \n ]"} {
		t.Run(body, func(t *testing.T) {
			f := newDebugFixture(t)
			client := f.service.Client
			observed := client.Now()
			first := observed
			client.Now = func() time.Time { return observed }
			client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				return profileTestResponse(r, 200, "application/json", body), nil
			})
			for range 2 {
				if _, err := client.FullProfile(context.Background(), 1, 7); !errors.Is(err, ErrProfileUnavailable) {
					t.Fatal(err)
				}
				assertMissingProfileRecorded(t, client.Store, 1, 7, first, 1)
				observed = observed.Add(time.Minute)
			}
			var checked string
			if err := client.Store.DB.QueryRow("SELECT checked_at FROM profile_presence WHERE owner_id=1 AND profile_id=7").Scan(&checked); err != nil || checked != first.Add(time.Minute).UTC().Format(time.RFC3339Nano) {
				t.Fatal("last check did not advance", checked, err)
			}
		})
	}
}

func TestProfileObservationWriteFailureDoesNotLoseCompatibilityNotice(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	store := f.service.Store
	ctx := context.Background()
	if _, err := store.recordProfilePresence(ctx, 1, 7, true, f.now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("CREATE TRIGGER reject_presence_event BEFORE INSERT ON profile_presence_events BEGIN SELECT RAISE(ABORT,'test event failure'); END"); err != nil {
		t.Fatal(err)
	}
	base := f.service.Client.HTTP.Transport
	f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.TrimPrefix(r.URL.Path, "/api") == "/user/getUser" {
			return profileTestResponse(r, 204, "", ""), nil
		}
		return base.RoundTrip(r)
	})
	if ran, err := f.service.compatibilityStep(ctx); err != nil || ran || f.state(t) != "queued" || f.posts != 0 {
		t.Fatal("lost notice cancelled the job or dispatched GPT", ran, err, f.state(t))
	}
	var availability string
	if err := store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=7").Scan(&availability); err != nil || availability != "available" {
		t.Fatal("failed event committed partial state", availability, err)
	}
	if _, err := store.DB.Exec("DROP TRIGGER reject_presence_event"); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(2 * time.Minute)
	if ran, err := f.service.compatibilityStep(ctx); err != nil || ran || f.state(t) != "cancelled" || f.posts != 0 {
		t.Fatal("recovered notice did not settle target", ran, err, f.state(t))
	}
	assertMissingProfileRecorded(t, store, 1, 7, f.now, 1)
}

func TestProfileObservationOldResponseCannotUndoReturn(t *testing.T) {
	f := newPresenceFixture(t)
	first := f.now
	if !presenceObserve(t, f, false) {
		t.Fatal("missing transition lost")
	}
	f.now = first.Add(2 * time.Minute)
	if !presenceObserve(t, f, true) {
		t.Fatal("return transition lost")
	}
	if changed, err := f.store.recordProfilePresence(context.Background(), 1, 90, false, first.Add(time.Minute)); err != nil || changed {
		t.Fatal("late response changed availability", changed, err)
	}
	var availability, checked string
	if err := f.store.DB.QueryRow("SELECT availability,checked_at FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability, &checked); err != nil || availability != "available" || checked != f.now.UTC().Format(time.RFC3339Nano) || watchCount(t, f.store, "profile_presence_events") != 2 {
		t.Fatal("late response overwrote return or generated a notice", availability, checked, err)
	}
}

func TestProfileObservationRejectsChangedSourceSession(t *testing.T) {
	for _, debug := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "debug"}[debug], func(t *testing.T) {
			f := newDebugFixture(t)
			client := f.service.Client
			ctx := context.Background()
			expected, err := client.Store.Session(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				next := expected
				next.Generation += "new"
				if err := client.Store.SaveSession(ctx, 1, next); err != nil {
					t.Fatal(err)
				}
				return profileTestResponse(r, 204, "", ""), nil
			})
			if debug {
				_, _, err = client.debugRead(ctx, 1, expected, "/user/getUser?viewerUserId=101&targetUserId=7")
			} else {
				_, err = client.FullProfile(ctx, 1, 7)
			}
			if err == nil || errors.Is(err, ErrProfileUnavailable) {
				t.Fatal("old source response accepted", err)
			}
			var count int
			if err := client.Store.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='profile_presence'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count > 0 {
				if err := client.Store.DB.QueryRow("SELECT COUNT(*) FROM profile_presence_events").Scan(&count); err != nil || count != 0 {
					t.Fatal("old response created a notice", count, err)
				}
			}
		})
	}
}

func TestCompatibilityObservationRecordsReturnAndChangesBeforeRescheduling(t *testing.T) {
	f := newCompatibilityExecutorFixture(t)
	ctx := context.Background()
	if _, err := f.service.Store.recordProfilePresence(ctx, 1, 7, false, f.now); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	f.target["description"] = "returned with new text"
	if ran, err := f.service.compatibilityStep(ctx); err != nil || ran || f.state(t) != "cancelled" || f.posts != 0 {
		t.Fatal("changed input reached GPT", ran, err)
	}
	var availability, changes string
	if err := f.service.Store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=7").Scan(&availability); err != nil || availability != "available" {
		t.Fatal(availability, err)
	}
	if err := f.service.Store.DB.QueryRow("SELECT changes_text FROM profile_presence_events WHERE owner_id=1 AND profile_id=7 AND availability='available'").Scan(&changes); err != nil || !strings.Contains(changes, "description") || !strings.Contains(changes, "returned with new text") {
		t.Fatal("return notice lost actual changes", changes, err)
	}
}
