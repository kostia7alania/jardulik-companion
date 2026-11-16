package companion

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func runtimeServiceFixture(t *testing.T) *Service {
	t.Helper()
	c := testConfig(t)
	c.Token = "runtime-fixture"
	c.ArchivePaused = true
	c.BackupChatID = 0
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func runtimeAwait(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime worker did not start")
	}
}

func TestRuntimeRolesPreventDuplicateOwnersAndReleasePartialLocks(t *testing.T) {
	s := runtimeServiceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	posterWait, err := s.RunBackground(ctx, RuntimePoster)
	if err != nil {
		t.Fatal(err)
	}
	defer posterWait()
	if wait, err := s.RunBackground(ctx, RuntimeAll); err == nil {
		wait()
		t.Fatal("all-in-one overlapped the existing poster")
	}
	// The failed union must release its first lock so the separate parser can start.
	botWait, err := s.RunBackground(ctx, RuntimeBotParser)
	if err != nil {
		t.Fatal("poster prevented the separate bot/parser role", err)
	}
	defer botWait()
	if wait, err := s.RunBackground(ctx, RuntimeBotParser); err == nil {
		wait()
		t.Fatal("duplicate source and poller role was accepted")
	}
	botWait()
	posterWait()
	allWait, err := s.RunBackground(ctx, RuntimeAll)
	if err != nil {
		t.Fatal("role locks were not released on shutdown", err)
	}
	allWait()
}

func TestRuntimeRoleRejectsUnknownRoleAndMissingPosterToken(t *testing.T) {
	s := runtimeServiceFixture(t)
	if _, err := s.RunBackground(context.Background(), RuntimeRole("typo")); err == nil {
		t.Fatal("unknown role started workers")
	}
	if err := (&Service{}).RunPoster(context.Background()); err == nil || err.Error() != "TELEGRAM_BOT_TOKEN is required" {
		t.Fatalf("poster started without its Telegram token: %v", err)
	}
}

func TestBotParserRoleChecksSourceWithoutClaimingPosterWork(t *testing.T) {
	f := newPresenceFixture(t)
	presenceObserve(t, f, false)
	if _, err := f.store.DB.Exec("UPDATE profile_presence_events SET state='processing'; UPDATE media_jobs SET state='failed'"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetMeta(context.Background(), "private_card_repair", "processing"); err != nil {
		t.Fatal(err)
	}
	c := f.watch.Config
	c.ArchivePaused = true
	f.watch.Archiver.Config.ArchivePaused = true
	s := &Service{Config: c, Store: f.store, Client: f.watch.Client, TG: f.watch.Archiver.TG, Archiver: f.watch.Archiver, Watcher: f.watch}
	s.Media = NewMediaManager(f.store, c)
	s.Crawler = NewFeedCrawler(s)
	s.Autoliker = NewAutoliker(s)
	called := make(chan struct{})
	var once sync.Once
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		once.Do(func() { close(called) })
		return watchResponse(http.StatusOK, watchProfile(id)), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := s.RunBackground(ctx, RuntimeBotParser)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); wait() }()
	runtimeAwait(t, called)
	deadline := time.Now().Add(3 * time.Second)
	for f.store.Meta(context.Background(), "private_card_repair") != "uncertain" {
		if time.Now().After(deadline) {
			t.Fatal("bot/parser did not recover its private-card worker")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wait()
	if presenceEventState(t, f) != "processing" {
		t.Fatal("source role claimed poster presence recovery or delivery work")
	}
}

func TestPosterRoleRecoversPresenceWithoutSourcePollingOrPrivateCardRepair(t *testing.T) {
	s := runtimeServiceFixture(t)
	s.Config.ArchivePaused = false
	s.Archiver.Config.ArchivePaused = false
	ctx := context.Background()
	if err := s.Store.User(ctx, 1, "runtime fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, 1, Session{UserID: 101, Generation: "fixture", Token: "fixture", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	job, err := s.Crawler.Request(ctx, 1, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetMeta(ctx, "private_card_repair", "processing"); err != nil {
		t.Fatal(err)
	}
	if err := s.Watcher.init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.ExecContext(ctx, `INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at)
 VALUES(1,90,'unavailable',1,'fixture','fixture');
 INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,state)
 VALUES(1,90,1,'unavailable','fixture','processing')`); err != nil {
		t.Fatal(err)
	}
	var sourceCalls, polls atomic.Int64
	s.Client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		sourceCalls.Add(1)
		return nil, errors.New("poster called source authentication or discovery")
	})
	s.TG.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		polls.Add(1)
		return nil, errors.New("idle poster attempted Telegram polling or bot setup")
	})
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = s.RunPoster(runCtx); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("poster did not stop")
		}
	}()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var state string
		if err := s.Store.DB.QueryRowContext(ctx, "SELECT state FROM profile_presence_events WHERE owner_id=1 AND profile_id=90").Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "uncertain" && s.Store.Meta(ctx, "caption_design_revision") == captionDesignRevision {
			break
		}
		select {
		case <-done:
			t.Fatalf("poster stopped before recovering delivery: %v", runErr)
		case <-deadline.C:
			t.Fatal("poster did not recover interrupted delivery")
		case <-tick.C:
		}
	}
	if s.Store.Meta(ctx, "private_card_repair") != "processing" {
		t.Fatal("poster claimed bot-owned private-card repair")
	}
	if sourceCalls.Load() != 0 || polls.Load() != 0 {
		t.Fatalf("poster crossed process roles: source=%d polling=%d", sourceCalls.Load(), polls.Load())
	}
	if current, err := s.Store.CrawlStatus(ctx, 1); err != nil || current.RunID != job.RunID || current.Page != 1 || current.State != "queued" {
		t.Fatalf("poster advanced the parser cursor: %+v err=%v", current, err)
	}
}

func TestBotParserRuntimeCancellationReleasesProcessRole(t *testing.T) {
	s := runtimeServiceFixture(t)
	polling := make(chan struct{})
	var once sync.Once
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		switch filepath.Base(r.URL.Path) {
		case "getMe":
			return watchResponse(http.StatusOK, Profile{"ok": true, "result": Profile{"id": 1, "is_bot": true, "username": "fixture"}}), nil
		case "getWebhookInfo":
			return watchResponse(http.StatusOK, Profile{"ok": true, "result": Profile{"url": ""}}), nil
		case "setMyCommands":
			return watchResponse(http.StatusOK, Profile{"ok": true, "result": true}), nil
		case "getUpdates":
			once.Do(func() { close(polling) })
			<-r.Context().Done()
			return nil, r.Context().Err()
		default:
			return nil, fmt.Errorf("unexpected bot runtime method: %s", r.URL.Path)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = s.RunBotParser(ctx); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("bot runtime did not stop")
		}
	}()
	runtimeAwait(t, polling)
	cancel()
	select {
	case <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bot runtime did not cancel polling and workers")
	}
	wait, err := s.RunBackground(ctx, RuntimeAll)
	if err != nil {
		t.Fatal("bot runtime retained its process lock after cancellation", err)
	}
	wait()
}

func TestPausedPosterRoleDoesNotScheduleCaptionEdits(t *testing.T) {
	s := runtimeServiceFixture(t)
	if err := s.Store.SetMeta(context.Background(), "caption_design_revision", "previous design"); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	s.TG.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("paused poster attempted Telegram delivery")
	})
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := s.RunBackground(ctx, RuntimePoster)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); wait() }()
	// Keep the paused role running through startup; no maintenance task may be scheduled.
	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if s.Store.Meta(context.Background(), "caption_design_revision") != "previous design" || calls.Load() != 0 {
			t.Fatal("archive pause allowed caption maintenance or Telegram delivery")
		}
		select {
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}

func TestBotParserCollectsWhileTelegramSetupIsUnavailable(t *testing.T) {
	f := newPresenceFixture(t)
	presenceObserve(t, f, false)
	ctx, cancel := context.WithCancel(context.Background())
	c := f.watch.Config
	c.Token = "fixture"
	c.ArchivePaused = true
	// A failed auxiliary listener must not prevent starting collection.
	c.DebugListen = "0.0.0.0:0"
	s := &Service{Config: c, Store: f.store, Client: f.watch.Client, TG: f.watch.Archiver.TG, Archiver: f.watch.Archiver, Watcher: f.watch}
	s.Media = NewMediaManager(f.store, c)
	s.Crawler = NewFeedCrawler(s)
	s.Autoliker = NewAutoliker(s)
	if err := s.Store.SetMeta(ctx, "private_card_repair", "processing"); err != nil {
		cancel()
		t.Fatal(err)
	}
	setupCalled, sourceCalled := make(chan struct{}), make(chan struct{})
	var setup, observed sync.Once
	var unexpected atomic.Int64
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if filepath.Base(r.URL.Path) != "getMe" {
			unexpected.Add(1)
		}
		setup.Do(func() { close(setupCalled) })
		return nil, errors.New("fixture: Telegram unreachable")
	})
	f.respond = func(_ *http.Request, id int64) (*http.Response, error) {
		observed.Do(func() { close(sourceCalled) })
		return watchResponse(http.StatusOK, watchProfile(id)), nil
	}
	done := make(chan error, 1)
	go func() { done <- s.RunBotParser(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("bot setup/collection did not stop")
		}
	}()
	runtimeAwait(t, setupCalled)
	select {
	case <-sourceCalled:
	case <-time.After(8 * time.Second):
		t.Fatalf("saved-profile checks did not start: last error %q", s.Store.Meta(ctx, "watch_last_error"))
	}
	if unexpected.Load() != 0 || s.Store.Meta(ctx, "private_card_repair") != "processing" {
		t.Fatal("Telegram-dependent work started before bot setup")
	}
}

func TestBotParserDailySearchCheckpointsWhileTelegramIsUnavailable(t *testing.T) {
	s, _ := syncFixture(t)
	s.Config.Token = "fixture"
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if filepath.Base(r.URL.Path) != "getMe" {
			t.Errorf("unexpected Telegram operation during setup: %s", filepath.Base(r.URL.Path))
		}
		return nil, errors.New("fixture: Telegram unreachable")
	})
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user/getMe":
			_, _ = w.Write([]byte(`{"userId":101,"type":1}`))
		case "/user/getUsers":
			_, _ = w.Write([]byte(`[{"userId":90,"name":"fixture"}]`))
		default:
			t.Errorf("unexpected source operation: %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.RunBotParser(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("bot and daily collector did not stop")
		}
	}()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			job, err := s.Store.CrawlStatus(ctx, 1)
			t.Fatalf("daily search did not save the next page: job=%+v err=%v", job, err)
		case <-tick.C:
			j, err := s.Store.CrawlStatus(ctx, 1)
			if err == nil && j != nil && j.Page == 2 && j.Pages == 1 {
				return
			}
		}
	}
}

func TestBotParserChecksSavedProfileDuringBlockedSearchAndTelegramFailure(t *testing.T) {
	s, now := syncFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Config.Token = "fixture"
	ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{{"userId": 90, "name": "fixture"}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	syncAuthRecoveryExec(t, s, "UPDATE versions SET last_seen=? WHERE id=?", now.Add(-48*time.Hour).Format(time.DateTime), ids[0])
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatalf("start=%v err=%v", started, err)
	}
	syncAuthRecoveryExec(t, s, "UPDATE feed_crawls SET state='stopped',reason='request_failed' WHERE owner_id=1")
	if _, err := s.SyncCycleStep(ctx); err != nil || syncStatus(t, s).Stage != "blocked" {
		t.Fatalf("blocked search err=%v", err)
	}
	s.TG.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("fixture: Telegram unreachable")
	})
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user/getUser" || r.Method != http.MethodGet || r.URL.Query().Get("targetUserId") != "90" {
			t.Errorf("unexpected source request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"userId":90,"name":"fixture","description":"current"}`))
	})}
	done := make(chan error, 1)
	go func() { done <- s.RunBotParser(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("bot and source workers did not stop")
		}
	}()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("watcher did not save a full read while search was blocked")
		case <-tick.C:
			var version, checked int64
			err := s.Store.DB.QueryRowContext(ctx, `SELECT r.version_id,w.last_checked FROM sync_full_reads r
 JOIN profile_watch w ON w.owner_id=r.owner_id AND w.profile_id=r.profile_id
 WHERE r.owner_id=1 AND r.upstream_id=101 AND r.generation='source-one' AND r.profile_id=90 AND w.last_result='available'`).Scan(&version, &checked)
			if err != nil {
				continue
			}
			if version == ids[0] || checked != now.UnixMilli() || syncStatus(t, s).Stage != "blocked" {
				t.Fatalf("watcher did not preserve independent progress: version=%d checked=%d", version, checked)
			}
			ready, err := collectionSlotReady(ctx, s.Store.DB, *now)
			if err != nil || ready {
				t.Fatalf("watch did not reserve the shared collection gap: ready=%v err=%v", ready, err)
			}
			return
		}
	}
}
