package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func presenceRun(t *testing.T, f *watchFixture, owner int64, run string) *CrawlJob {
	t.Helper()
	ctx := context.Background()
	tx, err := f.store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, crawlSchema+crawlPresenceSchema); err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,mode,page,next_request,state,started_at,updated_at)
 VALUES(?,?,?,'fixture','{}',1,1,3,0,'running',0,0)
 ON CONFLICT(owner_id) DO UPDATE SET run_id=excluded.run_id,state='running',reason='',include_others=1,mode=1`, owner, run, owner*1000)
	if err != nil {
		t.Fatal(err)
	}
	if err = initCrawlPresenceTx(ctx, tx, owner, run); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return &CrawlJob{Owner: owner, RunID: run, IncludeOthers: true, Mode: 1, State: "running"}
}

func presenceSeen(t *testing.T, f *watchFixture, j *CrawlJob, profiles ...Profile) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = recordCrawlSeenTx(ctx, tx, j, profiles, f.now); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func presenceFinish(t *testing.T, f *watchFixture, j *CrawlJob, state string) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE feed_crawls SET state=?,reason='empty_page' WHERE owner_id=? AND run_id=?", state, j.Owner, j.RunID); err != nil {
		t.Fatal(err)
	}
	if err = finishCrawlPresenceTx(ctx, tx, j, f.now); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCrawlPresenceOnlyCompleteRunDeclaresAbsenceOnce(t *testing.T) {
	f := newPresenceFixture(t)
	f.save(t, 1, 91, 48*time.Hour)
	for _, state := range []string{"running", "stopped"} {
		j := presenceRun(t, f, 1, state)
		presenceSeen(t, f, j, watchProfile(91))
		presenceFinish(t, f, j, state)
		if watchCount(t, f.store, "profile_presence_events") != 0 {
			t.Fatal("partial/stopped run declared absence")
		}
	}
	j := presenceRun(t, f, 1, "complete")
	presenceSeen(t, f, j, watchProfile(91))
	presenceFinish(t, f, j, "done")
	var reason string
	var baseline int64
	if err := f.store.DB.QueryRow("SELECT reason,baseline_version_id FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&reason, &baseline); err != nil || reason != "feed_absent" || baseline <= 0 {
		t.Fatalf("missing reason/baseline: %s %d %v", reason, baseline, err)
	}
	if watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("complete absence not queued once")
	}
	presenceFinish(t, f, j, "done")
	j = presenceRun(t, f, 1, "still-missing")
	presenceSeen(t, f, j, watchProfile(91))
	presenceFinish(t, f, j, "done")
	if watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("repeated missing spammed")
	}
	// An incomplete single-mode pass cannot classify feed absence.
	j = presenceRun(t, f, 1, "single-mode")
	if _, err := f.store.DB.Exec("UPDATE feed_crawls SET include_others=0 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	presenceFinish(t, f, j, "done")
	if watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("single-mode run declared absence")
	}
}

func TestCrawlPresenceReturnUsesDurableMissingBaseline(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			f := newPresenceFixture(t)
			ctx := context.Background()
			old, err := f.store.Profile(ctx, 1, 90)
			if err != nil {
				t.Fatal(err)
			}
			j := presenceRun(t, f, 1, "missing")
			presenceFinish(t, f, j, "done")
			p := watchProfile(90)
			if changed {
				p["description"] = "Новое описание"
				p["photos"] = []string{"https://mini.jardulik.app/new.jpg", "https://mini.jardulik.app/second.jpg"}
				p["distance"] = 999
			}
			if _, err = f.store.Archive(ctx, 1, []Profile{p}, "full-return"); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(24 * time.Hour)
			j = presenceRun(t, f, 1, "return")
			presenceSeen(t, f, j, p)
			presenceSeen(t, f, j, p)
			var before, after int64
			var changes string
			if err = f.store.DB.QueryRow("SELECT old_version_id,version_id,changes_text FROM profile_presence_events WHERE availability='available'").Scan(&before, &after, &changes); err != nil {
				t.Fatal(err)
			}
			if before != old.VersionID || after == 0 || watchCount(t, f.store, "profile_presence_events") != 2 {
				t.Fatal("return baseline lost or notification repeated")
			}
			if changed && (!strings.Contains(changes, "description:") || !strings.Contains(changes, "Исходная анкета") || !strings.Contains(changes, "Новое описание") || !strings.Contains(changes, "photos: ссылок добавлено 2, убрано 1") || strings.Contains(changes, "distance")) {
				t.Fatalf("source diff wrong: %s", changes)
			}
			if !changed && changes != "" {
				t.Fatalf("unchanged return invented changes: %s", changes)
			}
			// A later confirmed direct miss must not be overwritten by finishing seen IDs.
			presenceObserve(t, f, false)
			presenceFinish(t, f, j, "done")
			var availability, reason string
			if err = f.store.DB.QueryRow("SELECT availability,reason FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&availability, &reason); err != nil || availability != "unavailable" || reason != "direct_unavailable" {
				t.Fatal("finish resurrected earlier-seen profile after later404")
			}
		})
	}
}

func TestCrawlPresenceNewProfilesAndOwnersRemainIsolated(t *testing.T) {
	f := newPresenceFixture(t)
	f.save(t, 2, 90, 48*time.Hour)
	ctx := context.Background()
	if _, err := f.watch.recordProfilePresence(ctx, 2, 90, true, f.now); err != nil {
		t.Fatal(err)
	}
	peer := presenceRun(t, f, 2, "peer")
	j := presenceRun(t, f, 1, "own")
	if _, err := f.store.Archive(ctx, 1, []Profile{watchProfile(91), watchProfile(92)}, "during-run"); err != nil {
		t.Fatal(err)
	}
	presenceSeen(t, f, j, watchProfile(92))
	presenceFinish(t, f, j, "done")
	var peerState string
	if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=2 AND profile_id=90").Scan(&peerState); err != nil || peerState != "available" {
		t.Fatal("another owner marked absent")
	}
	var unavailableNew int
	if err := f.store.DB.QueryRow("SELECT COUNT(*) FROM profile_presence WHERE owner_id=1 AND profile_id IN (91,92) AND availability='unavailable'").Scan(&unavailableNew); err != nil || unavailableNew != 0 {
		t.Fatal("new profile during run falsely absent")
	}
	if watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("new availability or peer caused notification")
	}
	if err := f.store.Disconnect(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var ownLedger, peerLedger int
	if err := f.store.DB.QueryRow("SELECT COUNT(*) FROM crawl_presence_runs WHERE owner_id=1").Scan(&ownLedger); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DB.QueryRow("SELECT COUNT(*) FROM crawl_presence_runs WHERE owner_id=? AND run_id=?", peer.Owner, peer.RunID).Scan(&peerLedger); err != nil {
		t.Fatal(err)
	}
	if ownLedger != 0 || peerLedger != 1 {
		t.Fatal("disconnect ledger cascade crossed owners")
	}
}

func TestCrawlPresenceNotificationsWaitForCardsAndKeepUncertain(t *testing.T) {
	f := newPresenceFixture(t)
	ctx := context.Background()
	p, err := f.store.Profile(ctx, 1, 90)
	if err != nil {
		t.Fatal(err)
	}
	j := presenceRun(t, f, 1, "missing")
	presenceFinish(t, f, j, "done")
	if _, err = f.store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(90,55)"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if calls == 1 && !strings.HasPrefix(payload.Text, "Анкета в этом обходе не обнаружена\n") {
			t.Fatalf("feed absence title wrong: %s", payload.Text)
		}
		if calls == 2 {
			if !strings.HasPrefix(payload.Text, "Анкета снова доступна\n") || !strings.Contains(payload.Text, "Новое описание") {
				t.Fatalf("return summary missing: %s", payload.Text)
			}
			return nil, errors.New("ambiguous send after write")
		}
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":77,"message_thread_id":55,"chat":{"id":-100123}}}`), nil
	})
	if worked, err := f.watch.deliverProfilePresence(ctx); worked || err != nil || calls != 0 {
		t.Fatal("absence overtook unposted baseline card")
	}
	presenceConfirmVersion(t, f, p.VersionID, "sent", false)
	if worked, err := f.watch.deliverProfilePresence(ctx); worked || err != nil || calls != 0 {
		t.Fatal("absence overtook pending layout")
	}
	presenceConfirmVersion(t, f, p.VersionID, "sent", true)
	if worked, err := f.watch.deliverProfilePresence(ctx); !worked || err != nil || calls != 1 {
		t.Fatalf("confirmed baseline did not release absence: %v", err)
	}
	next := watchProfile(90)
	next["description"] = "Новое описание"
	ids, err := f.store.Archive(ctx, 1, []Profile{next}, "returned")
	if err != nil {
		t.Fatal(err)
	}
	j = presenceRun(t, f, 1, "return")
	presenceSeen(t, f, j, next)
	if worked, err := f.watch.deliverProfilePresence(ctx); worked || err != nil || calls != 1 {
		t.Fatal("return overtook updated card")
	}
	presenceConfirmVersion(t, f, ids[0], "partial", true)
	if worked, err := f.watch.deliverProfilePresence(ctx); !worked || err == nil || calls != 2 {
		t.Fatal("uncertain return delivery not classified")
	}
	var state string
	if err = f.store.DB.QueryRow("SELECT state FROM profile_presence_events WHERE availability='available'").Scan(&state); err != nil || state != "uncertain" {
		t.Fatal("ambiguous return was made retryable")
	}
	if worked, err := f.watch.deliverProfilePresence(ctx); worked || err != nil || calls != 2 {
		t.Fatal("uncertain return resent blindly")
	}
}

func TestProfilePresenceMigrationExistingDBIsConcurrentAndIdempotent(t *testing.T) {
	f := newWatchFixture(t)
	version := f.save(t, 1, 90, 48*time.Hour)
	ctx := context.Background()
	_, err := f.store.DB.Exec(`
 CREATE TABLE profile_presence(owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,profile_id INTEGER NOT NULL,availability TEXT NOT NULL,transition INTEGER NOT NULL DEFAULT 0,changed_at TEXT NOT NULL,checked_at TEXT NOT NULL,PRIMARY KEY(owner_id,profile_id));
 CREATE TABLE profile_presence_events(id INTEGER PRIMARY KEY AUTOINCREMENT,owner_id INTEGER NOT NULL,profile_id INTEGER NOT NULL,transition INTEGER NOT NULL,availability TEXT NOT NULL,observed_at TEXT NOT NULL,state TEXT NOT NULL DEFAULT'pending',next_attempt INTEGER NOT NULL DEFAULT 0,chat_id INTEGER NOT NULL DEFAULT 0,thread_id INTEGER NOT NULL DEFAULT 0,message_id INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT'',updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,UNIQUE(owner_id,profile_id,transition),FOREIGN KEY(owner_id,profile_id)REFERENCES profile_presence(owner_id,profile_id)ON DELETE CASCADE);`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.DB.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at)VALUES(1,90,'unavailable',1,?,?)", f.now.Format(time.RFC3339), f.now.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.DB.Exec("INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at)VALUES(1,90,1,'unavailable',?)", f.now.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	other, err := OpenStore(f.store.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	results := make(chan error, 2)
	for _, store := range []*Store{f.store, other} {
		go func(s *Store) {
			tx, err := s.DB.BeginTx(ctx, nil)
			if err != nil {
				results <- err
				return
			}
			defer tx.Rollback()
			if err = migrateProfilePresenceTx(ctx, tx); err == nil {
				err = tx.Commit()
			}
			results <- err
		}(store)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var reason string
	var baseline int64
	if err = f.store.DB.QueryRow("SELECT reason,baseline_version_id FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&reason, &baseline); err != nil || reason != "direct_unavailable" || baseline != version {
		t.Fatalf("migration lost prior baseline: %s %d %v", reason, baseline, err)
	}
	if watchCount(t, f.store, "profile_presence_events") != 1 {
		t.Fatal("migration created notifications")
	}
}

func TestCrawlPresenceDailyWatcherClosesFeedAbsenceWithDiff(t *testing.T) {
	f := newPresenceFixture(t)
	j := presenceRun(t, f, 1, "missing-before-watch")
	presenceFinish(t, f, j, "done")
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		p := watchProfile(id)
		p["description"] = "Вернулось описание"
		return watchResponse(200, p), nil
	}
	f.check(t, true)
	var state, changes string
	if err := f.store.DB.QueryRow("SELECT availability FROM profile_presence WHERE owner_id=1 AND profile_id=90").Scan(&state); err != nil || state != "available" {
		t.Fatal("daily full profile did not close feed absence")
	}
	if err := f.store.DB.QueryRow("SELECT changes_text FROM profile_presence_events WHERE availability='available'").Scan(&changes); err != nil || !strings.Contains(changes, "Вернулось описание") || !strings.Contains(changes, "Исходная анкета") {
		t.Fatalf("watcher lost return diff: %s %v", changes, err)
	}
}

func TestCrawlPresenceDuplicateCardUsesConfirmedContent(t *testing.T) {
	f := newPresenceFixture(t)
	ctx := context.Background()
	old, err := f.store.Profile(ctx, 1, 90)
	if err != nil {
		t.Fatal(err)
	}
	j := presenceRun(t, f, 1, "missing-duplicate")
	presenceFinish(t, f, j, "done")
	presenceTopic(t, f)
	p := watchProfile(90)
	p["distance"] = 999
	ids, err := f.store.Archive(ctx, 1, []Profile{p}, "viewer-only-return")
	if err != nil {
		t.Fatal(err)
	}
	j = presenceRun(t, f, 1, "return-duplicate")
	presenceSeen(t, f, j, p)
	// This is the archive worker's duplicate-content outcome, not a new send.
	_, err = f.store.DB.Exec("INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id) SELECT ?,1,90,fingerprint,'duplicate',55 FROM archive_versions WHERE version_id=?", ids[0], old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":55,"chat":{"id":-100123}}}`, 100+calls)), nil
	})
	for range 2 {
		if worked, err := f.watch.deliverProfilePresence(ctx); err != nil || !worked {
			t.Fatalf("confirmed duplicate content blocked notice: %v", err)
		}
	}
	if calls != 2 {
		t.Fatal("duplicate content notification repeated")
	}
}
