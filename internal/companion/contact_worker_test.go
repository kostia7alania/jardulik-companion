package companion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type contactWorkerFixture struct {
	service *Service
	now     time.Time
	account any
	pages   map[int][]Profile
	fail    bool
	hook    func(*http.Request)
	mu      sync.Mutex
	calls   []string
}

func newContactWorkerFixture(t *testing.T) *contactWorkerFixture {
	t.Helper()
	s := runtimeServiceFixture(t)
	f := &contactWorkerFixture{service: s, now: time.Now().UTC().Truncate(time.Millisecond), account: false, pages: map[int][]Profile{}}
	contactWorkerOwner(t, s, 1, 101, "worker-1", 100)
	s.Client.Now = func() time.Time { return f.now }
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
		f.mu.Unlock()
		if f.hook != nil {
			f.hook(r)
		}
		if r.URL.Path == "/user/getMe" {
			id, _ := strconv.ParseInt(r.URL.Query().Get("userId"), 10, 64)
			return watchResponse(http.StatusOK, Profile{"userId": id, "freezeState": f.account}), nil
		}
		if r.URL.Path == "/user/getReactUsers" {
			if f.fail {
				return nil, errors.New("fixture upstream failure")
			}
			if r.URL.Query().Get("isMutual") != "true" || r.URL.Query().Get("pageSize") != "1000" {
				t.Fatal("contact daily check requested another relationship list")
			}
			page, _ := strconv.Atoi(r.URL.Query().Get("pageNumber"))
			profiles := f.pages[page]
			if profiles == nil {
				profiles = []Profile{}
			}
			raw, err := json.Marshal(profiles)
			if err != nil {
				t.Fatal(err)
			}
			return albumTestResponse(r, http.StatusOK, string(raw)), nil
		}
		if r.URL.Path == "/telegram/getTG" && r.Method == http.MethodPost {
			return albumTestResponse(r, http.StatusOK, ""), nil
		}
		t.Fatalf("unexpected contact worker request: %s %s", r.Method, r.URL.RequestURI())
		return nil, errors.New("unexpected contact worker request")
	})
	return f
}

func contactWorkerOwner(t *testing.T, s *Service, owner, upstream int64, generation string, update int64) {
	t.Helper()
	ctx := context.Background()
	if err := s.Store.User(ctx, owner, "contact-worker-fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, owner, Session{Token: "offline-contact-worker", UserID: upstream, Generation: generation, Expires: time.Now().Add(48 * time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveAccount(ctx, owner, Profile{"userId": upstream, "freezeState": false}); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s.Store, businessTestConnection(t, owner, update, true))
}

func (f *contactWorkerFixture) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func contactWorkerState(t *testing.T, s *Store, owner int64) (page int, next, updated int64, reason string) {
	t.Helper()
	if err := s.DB.QueryRow("SELECT page,next_at_ms,updated_at_ms,last_error FROM contact_sync_state WHERE owner_id=?", owner).Scan(&page, &next, &updated, &reason); err != nil {
		t.Fatal(err)
	}
	return
}

func TestContactWorkerDailyCadenceAndForegroundYield(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	s.beginForeground()
	if worked, err := s.ContactStep(ctx, f.now); err != nil || worked || f.count() != 0 {
		t.Fatal("contact worker did not yield before source HTTP", worked, err)
	}
	s.endForeground()
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 2 {
		t.Fatal("daily source account/list check failed", worked, err)
	}
	page, next, _, reason := contactWorkerState(t, s.Store, 1)
	if page != 1 || next != f.now.Add(24*time.Hour).UnixMilli() || reason != "" {
		t.Fatal("empty mutual page did not schedule the next day", page, next, reason)
	}
	if worked, err := s.ContactStep(ctx, f.now.Add(24*time.Hour-time.Millisecond)); err != nil || worked || f.count() != 2 {
		t.Fatal("daily mutual list was fetched early", worked, err)
	}
	f.now = f.now.Add(24 * time.Hour)
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 4 {
		t.Fatal("daily mutual check did not resume when due", worked, err)
	}
}

func TestContactWorkerJobsTakePriorityAndNeverRepeatPost(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	f.pages[1] = []Profile{{"userId": 77, "name": "Mutual"}}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 2 {
		t.Fatal("fresh mutual page failed", err)
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 4 {
		t.Fatal("queued contact was not processed before another daily page", err)
	}
	var state string
	if err := s.Store.DB.QueryRow("SELECT state FROM contact_requests WHERE owner_id=1 AND profile_id=77").Scan(&state); err != nil || state != "awaiting_receipt" {
		t.Fatal("empty successful POST did not wait for an official card", state, err)
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || worked || f.count() != 4 {
		t.Fatal("waiting contact caused a repeated POST", worked, err)
	}
	var actions int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("contact sync created a like", err)
	}
}

func TestContactWorkerFrozenOrUnknownAccountDefersAndCanRefreshNextDay(t *testing.T) {
	for name, flag := range map[string]any{"frozen": true, "invalid": "false"} {
		t.Run(name, func(t *testing.T) {
			f := newContactWorkerFixture(t)
			f.account = flag
			if worked, err := f.service.ContactStep(context.Background(), f.now); err != nil || !worked || f.count() != 1 {
				t.Fatal("restricted account reached relationship/contact HTTP", worked, err)
			}
			_, next, _, reason := contactWorkerState(t, f.service.Store, 1)
			if next != f.now.Add(24*time.Hour).UnixMilli() || !strings.HasPrefix(reason, "account_") {
				t.Fatal("account restriction did not retain daily backoff", next, reason)
			}
			f.now = f.now.Add(24 * time.Hour)
			f.account = false
			if worked, err := f.service.ContactStep(context.Background(), f.now); err != nil || !worked || f.count() != 3 {
				t.Fatal("cached freeze prevented the next read-only account check", worked, err)
			}
		})
	}
}

func TestContactWorkerPageCursorCommitsOnlyAfterPageAndDatabaseSuccess(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	// Repeated source IDs keep this fixture about pagination, without contact POSTs.
	input := ContactEvidenceInput{UpstreamID: 101, ProfileID: 77, SourceBotID: s.Config.SourceBotID, SourcePeerID: s.Config.SourceBotID, SourceLocatorKind: "telegram_web_dom", SourceMessageLocator: "data-mid:worker-known", SourceMessageTimestamp: f.now.Add(-time.Hour).Unix(), ContactURL: "https://t.me/WorkerKnown", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1", ObservedAt: f.now}
	if _, err := s.Store.SaveContactEvidence(ctx, 1, input); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		f.pages[1] = append(f.pages[1], Profile{"userId": 77})
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked {
		t.Fatal(err)
	}
	page, next, _, _ := contactWorkerState(t, s.Store, 1)
	if page != 2 || next != f.now.Add(20*time.Second).UnixMilli() {
		t.Fatal("full mutual page did not advance by one with a gap", page, next)
	}
	f.now = f.now.Add(20 * time.Second)
	if _, err := s.Store.DB.Exec(`CREATE TRIGGER fail_contact_cursor BEFORE UPDATE ON contact_sync_state BEGIN SELECT RAISE(ABORT,'fixture cursor failure'); END`); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.ContactStep(ctx, f.now); !worked || !errors.Is(err, ErrContactPersistence) {
		t.Fatal("cursor write failure was swallowed", worked, err)
	}
	if got, _, _, _ := contactWorkerState(t, s.Store, 1); got != 2 {
		t.Fatal("failed cursor write skipped a source page")
	}
	if _, err := s.Store.DB.Exec("DROP TRIGGER fail_contact_cursor"); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked {
		t.Fatal("read-only page could not be replayed after database recovery", err)
	}
	if got, next, _, _ := contactWorkerState(t, s.Store, 1); got != 1 || next != f.now.Add(24*time.Hour).UnixMilli() {
		t.Fatal("last page did not complete the daily cycle", got, next)
	}
	if f.count() != 4 { // getMe+page1, page2 twice. No extra getMe for page2.
		t.Fatal("pagination repeated account preflight or a contact POST", f.calls)
	}
}

func TestContactWorkerUpstreamFailureBackoffAndPauseDoNotSpin(t *testing.T) {
	f := newContactWorkerFixture(t)
	f.fail = true
	if worked, err := f.service.ContactStep(context.Background(), f.now); err != nil || !worked || f.count() != 2 {
		t.Fatal("ordinary source failure stopped or retried the worker", worked, err)
	}
	_, next, _, reason := contactWorkerState(t, f.service.Store, 1)
	if next != f.now.Add(24*time.Hour).UnixMilli() || reason == "" {
		t.Fatal("source failure was not deferred", next, reason)
	}
	if worked, err := f.service.ContactStep(context.Background(), f.now.Add(time.Hour)); err != nil || worked || f.count() != 2 {
		t.Fatal("source failure was immediately retried", worked, err)
	}
	f.service.Store.SessionState(context.Background(), 1, "paused", "fixture_protection")
	if worked, err := f.service.ContactStep(context.Background(), f.now.Add(48*time.Hour)); err != nil || worked || f.count() != 2 {
		t.Fatal("paused source made daily HTTP", worked, err)
	}
}

func TestContactWorkerStaleBindingDoesNotStarveAnotherOwnerAndFreshBindingResets(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	if _, err := s.ContactStep(ctx, f.now); err != nil {
		t.Fatal(err)
	}
	contactWorkerOwner(t, s, 2, 102, "worker-2", 101)
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "offline-new", UserID: 101, Generation: "worker-new", Expires: f.now.Add(48 * time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 4 {
		t.Fatal("stale first owner prevented the second account's daily check", worked, err)
	}
	if !strings.Contains(f.calls[len(f.calls)-1], "userId=102") {
		t.Fatal("daily worker crossed the account binding", f.calls)
	}
	businessTestIngest(t, s.Store, businessTestConnection(t, 1, 102, true))
	f.now = f.now.Add(time.Millisecond)
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 6 {
		t.Fatal("newly bound source did not reset the daily cursor", worked, err)
	}
	var generation string
	if err := s.Store.DB.QueryRow("SELECT generation FROM contact_sync_state WHERE owner_id=1").Scan(&generation); err != nil || generation != "worker-new" {
		t.Fatal("daily cursor retained an old session generation", generation, err)
	}
}

func TestContactWorkerRequestSyncDoesNotRelockAndOldReplyCannotOverwriteIt(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	session, err := s.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.initContactSync(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := s.Store.pinContactSync(ctx, 1, session, f.now)
	if err != nil || old == nil {
		t.Fatal(err)
	}
	lock := s.Store.SessionMutex(1)
	lock.Lock()
	done := make(chan error, 1)
	go func() { done <- s.Store.RequestContactSync(ctx, 1, session, f.now) }()
	select {
	case err = <-done:
		lock.Unlock()
	case <-time.After(2 * time.Second):
		lock.Unlock()
		<-done
		t.Fatal("sync request attempted to relock the caller's session mutex")
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.finishContactSync(ctx, *old, 1, f.now.Add(24*time.Hour), "old reply", f.now); err != nil {
		t.Fatal(err)
	}
	_, next, _, reason := contactWorkerState(t, s.Store, 1)
	if next != f.now.UnixMilli() || reason != "" {
		t.Fatal("in-flight old page overwrote a newer mutual notification", next, reason)
	}
}

func TestContactWorkerRuntimeRoleAndStatusHaveNoUnexpectedSourceCalls(t *testing.T) {
	f := newContactWorkerFixture(t)
	s := f.service
	if text := s.ContactStatusText(context.Background(), 1); !strings.Contains(text, "Telegram: сохранено 0") || f.count() != 0 {
		t.Fatal("contact status fetched the source", text)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wait, err := s.RunBackground(ctx, RuntimePoster)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	wait()
	if f.count() != 0 {
		t.Fatal("poster started contact requests")
	}
	ready := make(chan struct{})
	var once sync.Once
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getReactUsers" {
			once.Do(func() { close(ready) })
		}
	}
	ctx, cancel = context.WithCancel(context.Background())
	wait, err = s.RunBackground(ctx, RuntimeBotParser)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		cancel()
		wait()
		t.Fatal("bot-parser role did not start the contact check")
	}
	cancel()
	wait()
	if f.count() != 2 {
		t.Fatal("contact role repeated source HTTP", f.calls)
	}
}

func TestContactWorkerDoesNotExposeAnotherOwnersCounts(t *testing.T) {
	f := newContactWorkerFixture(t)
	contactWorkerOwner(t, f.service, 2, 102, "worker-2", 101)
	raw := businessTestMessage(t, f.service.Config.SourceBotID, 2, 102, "https://t.me/OtherOwner")
	businessTestIngest(t, f.service.Store, raw)
	if text := f.service.ContactStatusText(context.Background(), 1); !strings.Contains(text, "сохранено 0") {
		t.Fatal("status included another owner's contact", text)
	}
	if text := f.service.ContactStatusText(context.Background(), 2); !strings.Contains(text, "сохранено 1") {
		t.Fatal("status did not show this owner's contact", text)
	}
	f.service.Store.SessionState(context.Background(), 1, "paused", "fixture_protection")
	if text := f.service.ContactStatusText(context.Background(), 1); !strings.Contains(text, "сбор на паузе") || f.count() != 0 {
		t.Fatal("status missed source pause or fetched source", text)
	}
}

func TestContactWorkerBusinessRevokedDuringAccountReadStopsBeforeMutualList(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getMe" {
			businessTestIngest(t, s.Store, businessTestConnection(t, 1, 101, false))
		}
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 1 {
		t.Fatal("business revocation during account read did not stop later HTTP", worked, err, f.calls)
	}
	_, next, _, reason := contactWorkerState(t, s.Store, 1)
	if next != f.now.UnixMilli() || reason != "" {
		t.Fatal("revoked read unexpectedly advanced the daily cursor", next, reason)
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || worked || f.count() != 1 {
		t.Fatal("revoked business connection caused another source request", worked, err)
	}
	var jobs int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM contact_requests").Scan(&jobs); err != nil || jobs != 0 {
		t.Fatal("revoked read queued a contact", jobs, err)
	}
}

func TestContactWorkerNewExplicitSyncDuringAccountReadKeepsItsCursor(t *testing.T) {
	f := newContactWorkerFixture(t)
	s, ctx := f.service, context.Background()
	var once sync.Once
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getMe" {
			once.Do(func() {
				session, err := s.Store.Session(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				lock := s.Store.SessionMutex(1)
				lock.Lock()
				defer lock.Unlock()
				if err := s.Store.RequestContactSync(ctx, 1, session, f.now); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 1 {
		t.Fatal("older account read proceeded after a newer explicit sync", worked, err, f.calls)
	}
	page, next, updated, reason := contactWorkerState(t, s.Store, 1)
	if page != 1 || next != f.now.UnixMilli() || updated <= f.now.UnixMilli() || reason != "" {
		t.Fatal("older account read replaced the newer sync cursor", page, next, updated, reason)
	}
	f.hook = nil
	if worked, err := s.ContactStep(ctx, f.now); err != nil || !worked || f.count() != 3 {
		t.Fatal("new explicit sync did not run after the older step yielded", worked, err)
	}
	_, next, _, _ = contactWorkerState(t, s.Store, 1)
	if next != f.now.Add(24*time.Hour).UnixMilli() {
		t.Fatal("fresh sync did not complete normally", next)
	}
}
