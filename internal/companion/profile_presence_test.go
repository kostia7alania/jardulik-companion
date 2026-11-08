package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newPresenceFixture(t *testing.T) *watchFixture {
	t.Helper()
	f := newWatchFixture(t)
	f.save(t, 1, 90, 48*time.Hour)
	if err := f.watch.init(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = seedProfilePresence(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	c := f.watch.Config
	c.BackupChatID = -100123
	tg := NewTelegram(c)
	a := NewArchiver(c, f.store, tg, nil)
	var ticks atomic.Int64
	start := f.now
	a.now = func() time.Time { return start.Add(time.Duration(ticks.Add(1)) * 5 * time.Second) }
	f.watch.Archiver = a
	return f
}

func presenceObserve(t *testing.T, f *watchFixture, available bool) bool {
	t.Helper()
	changed, err := f.watch.recordProfilePresence(context.Background(), 1, 90, available, f.now)
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func presenceEventState(t *testing.T, f *watchFixture) string {
	t.Helper()
	var state string
	if err := f.store.DB.QueryRow("SELECT state FROM profile_presence_events ORDER BY id LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func presenceTopic(t *testing.T, f *watchFixture) {
	t.Helper()
	if _, err := f.store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(90,55)"); err != nil {
		t.Fatal(err)
	}
	rows, err := f.store.DB.Query("SELECT id FROM versions WHERE owner_id=1 AND profile_id=90")
	if err != nil {
		t.Fatal(err)
	}
	var versions []int64
	for rows.Next() {
		var version int64
		if err = rows.Scan(&version); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		versions = append(versions, version)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	for _, version := range versions {
		presenceConfirmVersion(t, f, version, "sent", true)
	}
}

func presenceConfirmVersion(t *testing.T, f *watchFixture, version int64, state string, layout bool) {
	t.Helper()
	if err := f.watch.Archiver.init(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids) SELECT id,owner_id,profile_id,fingerprint,?,55,'[40]' FROM versions WHERE id=? ON CONFLICT(version_id) DO UPDATE SET state=excluded.state`, state, version)
	if err != nil {
		t.Fatal(err)
	}
	if layout {
		if _, err = f.store.DB.Exec("INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]','[40]') ON CONFLICT(version_id) DO UPDATE SET state='sent'", version); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProfilePresenceTransitionsAreDurableAndDoNotSpamInitialAvailability(t *testing.T) {
	f := newPresenceFixture(t)
	ctx := context.Background()
	if changed, err := f.watch.recordProfilePresence(ctx, 1, 91, true, f.now); err != nil || changed {
		t.Fatalf("initial availability produced a notification: %v %v", changed, err)
	}
	if presenceObserve(t, f, true) || watchCount(t, f.store, "profile_presence_events") != 0 {
		t.Fatal("known available profile spammed initial event")
	}
	if !presenceObserve(t, f, false) || presenceObserve(t, f, false) {
		t.Fatal("missing transition lost or repeated")
	}
	f.now = f.now.Add(24 * time.Hour)
	if presenceObserve(t, f, false) || !presenceObserve(t, f, true) || presenceObserve(t, f, true) {
		t.Fatal("return transition lost or repeated")
	}
	if watchCount(t, f.store, "profile_presence_events") != 2 || watchCount(t, f.store, "versions") != 1 || watchCount(t, f.store, "media_jobs") != 1 {
		t.Fatal("transitions duplicated or original archive was changed")
	}
	var availability string
	var transition int
	if err := f.store.DB.QueryRow("SELECT availability,transition FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability, &transition); err != nil || availability != "available" || transition != 2 {
		t.Fatalf("durable availability state lost: %s %d %v", availability, transition, err)
	}
	// Reopening the observer does not reset the deduplication sequence.
	w := NewProfileWatcher(f.watch.Config, f.store, f.watch.Client)
	if err := w.init(ctx); err != nil {
		t.Fatal(err)
	}
	if changed, err := w.recordProfilePresence(ctx, 1, 90, true, f.now); err != nil || changed || watchCount(t, f.store, "profile_presence_events") != 2 {
		t.Fatal("restart replayed a transition")
	}
}

func TestProfilePresenceWatcherKeepsCheckingMissingProfilesDaily(t *testing.T) {
	f := newPresenceFixture(t)
	f.respond = func(r *http.Request, _ int64) (*http.Response, error) {
		return albumTestResponse(r, 404, `{"error":"missing target"}`), nil
	}
	f.check(t, true)
	if watchCount(t, f.store, "profile_presence_events") != 1 || watchCount(t, f.store, "versions") != 1 {
		t.Fatal("first disappearance did not preserve original archive")
	}
	var next int64
	if err := f.store.DB.QueryRow("SELECT next_check FROM profile_watch WHERE owner_id=1 AND profile_id=90").Scan(&next); err != nil || next != f.now.Add(24*time.Hour).UnixMilli() {
		t.Fatal("missing profile was not scheduled for the next day")
	}
	f.now = f.now.Add(23 * time.Hour)
	f.check(t, false)
	f.now = f.now.Add(time.Hour)
	f.check(t, true)
	if watchCount(t, f.store, "profile_presence_events") != 1 || len(f.calls) != 2 {
		t.Fatal("repeated absence spammed or stopped checks")
	}
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		return watchResponse(200, watchProfile(id)), nil
	}
	f.now = f.now.Add(24 * time.Hour)
	f.check(t, true)
	if watchCount(t, f.store, "profile_presence_events") != 2 || watchCount(t, f.store, "versions") != 1 {
		t.Fatal("unchanged returning profile did not create exactly one return event")
	}
}

func TestProfilePresenceAcceptsOnlyConfirmedEmptyOrMissingTargets(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{{404, `{"error":"missing target"}`}, {410, `{"error":"missing target"}`}, {200, `null`}, {200, `[]`}} {
		t.Run(fmt.Sprintf("%d_%s", tc.status, tc.body), func(t *testing.T) {
			f := newPresenceFixture(t)
			f.respond = func(r *http.Request, _ int64) (*http.Response, error) {
				return albumTestResponse(r, tc.status, tc.body), nil
			}
			f.check(t, true)
			var availability string
			if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability); err != nil || availability != "unavailable" || watchCount(t, f.store, "profile_presence_events") != 1 {
				t.Fatalf("confirmed absence was not observed once: %s %v", availability, err)
			}
		})
	}
}

func TestProfilePresenceIgnoresAccountTransportAndParserFailures(t *testing.T) {
	for _, kind := range []string{"403", "429", "500", "timeout", "bad_json", "wrong_id", "auth_refresh_failed", "auth_refresh_404", "auth_refresh_410"} {
		t.Run(kind, func(t *testing.T) {
			f := newPresenceFixture(t)
			if strings.HasPrefix(kind, "auth_refresh_") {
				f.watch.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/appuser/refresh" {
						status := map[string]int{"auth_refresh_failed": 400, "auth_refresh_404": 404, "auth_refresh_410": 410}[kind]
						return watchResponse(status, Profile{"error": "test"}), nil
					}
					return watchResponse(401, Profile{"error": "test"}), nil
				})
			} else {
				f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
					switch kind {
					case "timeout":
						return nil, context.DeadlineExceeded
					case "bad_json":
						r := watchResponse(200, nil)
						r.Body = http.NoBody
						return r, nil
					case "wrong_id":
						return watchResponse(200, watchProfile(id+1)), nil
					default:
						status := map[string]int{"403": 403, "429": 429, "500": 500}[kind]
						return watchResponse(status, Profile{"error": "test"}), nil
					}
				}
			}
			if worked, err := f.watch.CheckOne(context.Background()); !worked || err == nil {
				t.Fatalf("operational failure hidden: %v %v", worked, err)
			}
			var availability string
			if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability); err != nil || availability != "available" || watchCount(t, f.store, "profile_presence_events") != 0 {
				t.Fatalf("account failure was called a disappearance: %s %v", availability, err)
			}
		})
	}
}

func TestProfilePresenceSendsSeparateMessagesOnlyToKnownThread(t *testing.T) {
	f := newPresenceFixture(t)
	presenceObserve(t, f, false)
	titles := []string{"Анкета недоступна", "Анкета снова доступна"}
	calls := 0
	f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Fatalf("presence attempted another Telegram action: %s", r.URL.Path)
		}
		var payload struct {
			Chat   int64  `json:"chat_id"`
			Thread int64  `json:"message_thread_id"`
			Text   string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.Chat != -100123 || payload.Thread != 55 || !strings.HasPrefix(payload.Text, titles[calls]+"\n") || !strings.Contains(payload.Text, displayTime(f.now.Format(time.RFC3339))) {
			t.Fatalf("wrong presence destination or event time: %+v", payload)
		}
		calls++
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":55,"chat":{"id":-100123}}}`, 40+calls)), nil
	})
	if worked, err := f.watch.deliverProfilePresence(context.Background()); worked || err != nil || calls != 0 || presenceEventState(t, f) != "pending" {
		t.Fatal("unknown topic caused an external effect or lost event")
	}
	presenceTopic(t, f)
	if worked, err := f.watch.deliverProfilePresence(context.Background()); !worked || err != nil || presenceEventState(t, f) != "sent" {
		t.Fatalf("known thread notice failed: %v %v", worked, err)
	}
	if worked, err := f.watch.deliverProfilePresence(context.Background()); worked || err != nil || calls != 1 {
		t.Fatal("sent notice was repeated")
	}
	f.now = f.now.Add(24 * time.Hour)
	presenceObserve(t, f, true)
	if worked, err := f.watch.deliverProfilePresence(context.Background()); !worked || err != nil || calls != 2 {
		t.Fatalf("return notice not delivered separately: %v %v", worked, err)
	}
	if watchCount(t, f.store, "versions") != 1 || watchCount(t, f.store, "media_jobs") != 1 {
		t.Fatal("presence messaging modified source archive")
	}
}

func TestProfilePresenceUnknownDeliveryAndCrashAreNeverReplayed(t *testing.T) {
	for _, mode := range []string{"transport", "bad_receipt", "restart"} {
		t.Run(mode, func(t *testing.T) {
			f := newPresenceFixture(t)
			presenceObserve(t, f, false)
			presenceTopic(t, f)
			calls := 0
			f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if mode == "transport" {
					return nil, errors.New("connection lost after write")
				}
				return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":42,"message_thread_id":999,"chat":{"id":-100123}}}`), nil
			})
			if mode == "restart" {
				if _, err := f.store.DB.Exec("UPDATE profile_presence_events SET state='processing'"); err != nil {
					t.Fatal(err)
				}
				if err := f.watch.recoverProfilePresence(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if worked, err := f.watch.deliverProfilePresence(context.Background()); !worked || err == nil || calls != 1 {
				t.Fatalf("unknown delivery outcome hidden: %v %v", worked, err)
			}
			if presenceEventState(t, f) != "uncertain" {
				t.Fatal("uncertain notice was made retryable")
			}
			before := calls
			if worked, err := f.watch.deliverProfilePresence(context.Background()); worked || err != nil || calls != before {
				t.Fatal("uncertain notice replayed blindly")
			}
		})
	}
}

func TestProfilePresenceExplicitRateLimitAndPreSendCancellationRemainPending(t *testing.T) {
	f := newPresenceFixture(t)
	presenceObserve(t, f, false)
	presenceTopic(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	f.watch.Archiver.mu.Lock()
	_, err := f.watch.deliverProfilePresence(ctx)
	f.watch.Archiver.mu.Unlock()
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || presenceEventState(t, f) != "pending" {
		t.Fatal("cancellation before claim changed the event")
	}
	start := f.now
	var tick atomic.Int64
	f.watch.Archiver.now = func() time.Time { return start.Add(time.Duration(tick.Add(1)) * time.Minute) }
	calls := 0
	f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"description":"test","parameters":{"retry_after":60}}`), nil
	})
	if worked, err := f.watch.deliverProfilePresence(context.Background()); !worked || err == nil || calls != 3 || presenceEventState(t, f) != "pending" {
		t.Fatalf("confirmed 429 incorrectly classified: %v %v requests=%d", worked, err, calls)
	}
	if worked, err := f.watch.deliverProfilePresence(context.Background()); worked || err != nil || calls != 3 {
		t.Fatal("durable retry_after was ignored")
	}
}

func TestProfilePresenceCanceledRateLimitWaitRemainsPending(t *testing.T) {
	f := newPresenceFixture(t)
	presenceObserve(t, f, false)
	presenceTopic(t, f)
	calls := 0
	f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"description":"test","parameters":{"retry_after":3600}}`), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	worked, err := f.watch.deliverProfilePresence(ctx)
	if !worked || !errors.Is(err, context.DeadlineExceeded) || calls != 1 || presenceEventState(t, f) != "pending" {
		t.Fatalf("confirmed rejection was made uncertain after waiting: %v %v requests=%d state=%s", worked, err, calls, presenceEventState(t, f))
	}
}

func TestProfilePresenceTransitionsRemainOwnerScopedAndAtomic(t *testing.T) {
	f := newPresenceFixture(t)
	f.save(t, 2, 90, 48*time.Hour)
	ctx := context.Background()
	if _, err := f.watch.recordProfilePresence(ctx, 2, 90, true, f.now); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := f.watch.recordProfilePresence(ctx, 1, 90, false, f.now); results <- err }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var availability string
	if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=2 AND profile_id=90").Scan(&availability); err != nil || availability != "available" || watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("duplicate transition or cross-owner presence change")
	}
	if err := f.store.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, f.store, "profile_presence_events") != 0 {
		t.Fatal("owner deletion retained its queued notifications")
	}
	if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("owner deletion retained presence state")
	}
}
