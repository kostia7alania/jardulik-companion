package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type reactionFixture struct {
	store   *Store
	client  *Client
	now     time.Time
	account string
	reply   string
	status  int
	getHook func()
	posts   atomic.Int64
	gets    atomic.Int64
}

func newReactionFixture(t *testing.T) *reactionFixture {
	t.Helper()
	s, cfg := testStore(t)
	s.cfg.AllowReactions = true
	f := &reactionFixture{store: s, now: time.Now().UTC().Truncate(time.Second), account: `{"userId":101,"freezeState":false}`, reply: `{"success":true}`, status: 200}
	ctx := context.Background()
	if err := s.User(ctx, 1, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSession(ctx, 1, Session{Token: "test-access", Refresh: "test-refresh", UserID: 101, Generation: "generation-1", Expires: f.now.Add(24 * time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Archive(ctx, 1, []Profile{{"userId": 7}, {"userId": 8}}, "test"); err != nil {
		t.Fatal(err)
	}
	f.client = NewClient(s, cfg)
	f.client.Now = func() time.Time { return f.now }
	f.client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/user/getMe":
			f.gets.Add(1)
			if r.Method != http.MethodGet || r.URL.Query().Get("userId") != "101" {
				t.Fatal("read-only preflight used the wrong account")
			}
			if f.getHook != nil {
				f.getHook()
			}
			return albumTestResponse(r, 200, f.account), nil
		case "/reactions/addNew":
			f.posts.Add(1)
			var p map[string]any
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				t.Fatal(err)
			}
			if r.Method != http.MethodPost || p["fromUserId"] != float64(101) || p["userId"] != float64(7) || p["reactionType"] != float64(1) || r.Header.Get("Authorization") != "Bearer test-access" {
				t.Fatalf("reaction was not pinned to the correct viewer/target: %#v", p)
			}
			if f.reply == "transport_error" {
				return nil, errors.New("test transport outcome unknown")
			}
			return albumTestResponse(r, f.status, f.reply), nil
		default:
			t.Fatalf("unexpected source request: %s %s", r.Method, r.URL.Path)
			return nil, errors.New("unexpected request")
		}
	})
	return f
}

func TestReactionResultDecisionsRespectNegativeKindsAndFailureOnlyLast(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name, body string
		decision   ReactionDecision
		wait       time.Duration
		exhausted  bool
		unknown    bool
	}{
		{"success", `{"success":true}`, ReactionSuccess, 0, false, false},
		{"mutual", `{"IsSuccess":true,"IsMutual":true}`, ReactionSuccess, 0, false, false},
		{"success_last", `{"success":true,"last":true,"kind":-1,"hoursLeft":3}`, ReactionSuccess, 0, false, false},
		{"quota", `{"success":false,"kind":-1,"hoursLeft":2,"minutesLeft":17}`, ReactionQuota, 137 * time.Minute, false, false},
		{"quota_string", `{"success":false,"kind":-1,"minutesLeft":"2"}`, ReactionQuota, 2 * time.Minute, false, false},
		{"quota_no_timer", `{"success":false,"kind":-1}`, ReactionQuota, 0, false, false},
		{"quota_invalid_timer", `{"success":false,"kind":-1,"minutesLeft":60}`, ReactionQuota, 0, false, false},
		{"quota_negative_timer", `{"success":false,"kind":-1,"hoursLeft":-2}`, ReactionQuota, 0, false, false},
		{"quota_overflow", `{"success":false,"kind":-1,"hoursLeft":9223372036854775807}`, ReactionQuota, 0, false, false},
		{"fire", `{"success":false,"kind":-2}`, ReactionFireUnavailable, 0, false, false},
		{"premium", `{"success":false,"kind":-3}`, ReactionPremiumRequired, 0, false, false},
		{"exhausted", `{"success":false,"last":true}`, ReactionExhausted, 0, true, false},
		{"positive_not_quota", `{"success":false,"kind":1,"hoursLeft":1}`, ReactionUnknown, 0, false, true},
		{"generic_not_quota", `{"success":false,"hoursLeft":1}`, ReactionUnknown, 0, false, true},
		{"missing_success", `{"kind":-1,"minutesLeft":1}`, ReactionUnknown, 0, false, true},
		{"contradictory_success", `{"success":false,"IsSuccess":true,"kind":-1,"minutesLeft":1}`, ReactionUnknown, 0, false, true},
		{"contradictory_mutual", `{"success":false,"mutual":true,"kind":-1,"minutesLeft":1}`, ReactionUnknown, 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := decodeReactionResult([]byte(tc.body), now)
			if (err != nil) != tc.unknown || result.Decision != tc.decision || result.Wait != tc.wait || result.Exhausted != tc.exhausted || (tc.wait > 0 && !result.RetryAt.Equal(now.Add(tc.wait))) || (tc.wait == 0 && !result.RetryAt.IsZero()) {
				t.Fatalf("reaction decision incorrect: %+v %v", result, err)
			}
		})
	}
}

func TestReactionQuotaPersistsDeadlineBlocksOwnerAndRetriesOnlyWhenDue(t *testing.T) {
	f := newReactionFixture(t)
	f.reply = `{"success":false,"kind":-1,"minutesLeft":1}`
	quota := f.reply
	result, err := f.client.Like(context.Background(), 1, 7)
	if err != nil || result.Decision != ReactionQuota || result.Wait != time.Minute || f.store.Paused(context.Background(), 1) {
		t.Fatalf("definite quota incorrectly stopped account: %+v %v", result, err)
	}
	var state string
	var retry int64
	if err = f.store.DB.QueryRow("SELECT state,retry_at FROM actions").Scan(&state, &retry); err != nil || state != "quota_waiting" || retry != f.now.Add(time.Minute).Unix() {
		t.Fatal("source deadline was not saved", state, retry, err)
	}
	f.now = f.now.Add(59 * time.Second)
	for _, target := range []int64{7, 8} {
		_, err = f.client.Like(context.Background(), 1, target)
		var blocked *ActionBlockedError
		if !errors.As(err, &blocked) || blocked.State != "quota_waiting" || blocked.RetryAt.Unix() != retry || f.gets.Load() != 1 || f.posts.Load() != 1 {
			t.Fatal("quota was retried early or bypassed on another profile", err)
		}
	}
	f.now = f.now.Add(time.Second)
	f.reply = `{"success":true,"mutual":true,"last":true}`
	result, err = f.client.Like(context.Background(), 1, 7)
	if err != nil || result.Decision != ReactionSuccess || !result.Mutual || result.Exhausted || f.posts.Load() != 2 || f.store.Paused(context.Background(), 1) {
		t.Fatalf("due quota did not yield one confirmed retry: %+v %v", result, err)
	}
	var oldResponse string
	if err = f.store.DB.QueryRow("SELECT state,response FROM actions ORDER BY id LIMIT 1").Scan(&state, &oldResponse); err != nil || state != "quota_retried" || oldResponse != quota {
		t.Fatal("old quota observation was discarded")
	}
	if _, err = f.client.Like(context.Background(), 1, 7); !errors.Is(err, ErrActionBlocked) || f.posts.Load() != 2 {
		t.Fatal("success could be repeated")
	}
}

func TestReactionQuotaWithoutValidSourceTimerNeverInventsRetry(t *testing.T) {
	f := newReactionFixture(t)
	f.reply = `{"success":false,"kind":-1,"hoursLeft":0,"minutesLeft":99}`
	r, err := f.client.Like(context.Background(), 1, 7)
	if err != nil || r.Decision != ReactionQuota || !r.RetryAt.IsZero() || r.Wait != 0 {
		t.Fatal("invalid service timer became an invented schedule")
	}
	_, err = f.store.ClaimOwnerAction(context.Background(), 1, 7, f.now.Add(365*24*time.Hour))
	var blocked *ActionBlockedError
	if !errors.As(err, &blocked) || !blocked.RetryAt.IsZero() || f.posts.Load() != 1 {
		t.Fatal("unknown deadline allowed automatic retry")
	}
}

func TestReactionUnknownOutcomesNeverReplayAndKeepSessionSafety(t *testing.T) {
	for _, tc := range []struct {
		name, body, session string
		status              int
	}{
		{"transport", "transport_error", "connected", 200},
		{"decode", `{"success":`, "paused", 200},
		{"unknown_code", `{"success":false,"kind":1}`, "connected", 200},
		{"auth", `{"error":"test"}`, "reconnect_required", 401},
		{"access_denied", `{"error":"test"}`, "paused", 403},
		{"rate_limit", `{"error":"test"}`, "paused", 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReactionFixture(t)
			f.reply, f.status = tc.body, tc.status
			if _, err := f.client.Like(context.Background(), 1, 7); err == nil || f.posts.Load() != 1 || !f.store.Paused(context.Background(), 1) {
				t.Fatal("uncertain reaction was not stopped")
			}
			session, err := f.store.Session(context.Background(), 1)
			if err != nil || session.State != tc.session {
				t.Fatal("account safety state lost", session.State, err)
			}
			f.store.Pause(context.Background(), 1, false)
			if _, err = f.client.Like(context.Background(), 1, 7); err == nil || f.posts.Load() != 1 {
				t.Fatal("uncertain result was blindly replayed")
			}
		})
	}
}

func TestReactionDisabledPausedForeignAndFrozenDoNotPost(t *testing.T) {
	for _, mode := range []string{"disabled", "paused", "foreign", "frozen", "fresh_pause", "fresh_generation"} {
		t.Run(mode, func(t *testing.T) {
			f := newReactionFixture(t)
			wantReads := int64(0)
			target := int64(7)
			switch mode {
			case "disabled":
				f.store.cfg.AllowReactions = false
			case "paused":
				f.store.Pause(context.Background(), 1, true)
			case "foreign":
				target = 99
			case "frozen":
				f.account = `{"userId":101,"freezeState":true}`
				wantReads = 1
			case "fresh_pause":
				f.getHook = func() { f.store.Pause(context.Background(), 1, true) }
				wantReads = 1
			case "fresh_generation":
				f.getHook = func() {
					if err := f.store.SaveSession(context.Background(), 1, Session{Token: "other", Refresh: "other", UserID: 303, Generation: "generation-2"}); err != nil {
						t.Fatal(err)
					}
				}
				wantReads = 1
			}
			_, err := f.client.Like(context.Background(), 1, target)
			if err == nil || f.posts.Load() != 0 || f.gets.Load() != wantReads || watchCount(t, f.store, "actions") != 0 {
				t.Fatal("reaction guard permitted a POST or durable claim", err)
			}
			if mode == "frozen" && !errors.Is(err, ErrAccountFrozen) {
				t.Fatal("fresh freeze was not typed")
			}
		})
	}
}

func TestReactionPostClaimGuardCancelAllowsKnownQuotaRetryAfterResume(t *testing.T) {
	f := newReactionFixture(t)
	f.reply = `{"success":false,"kind":-1,"minutesLeft":1}`
	if _, err := f.client.Like(context.Background(), 1, 7); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	stop := errors.New("auto mode disabled")
	guard := func(ctx context.Context) error {
		if ClaimedLikeActionID(ctx) != 0 {
			return stop
		}
		return nil
	}
	if _, err := f.client.LikeGuarded(context.Background(), 1, 7, guard); !errors.Is(err, stop) || f.posts.Load() != 1 || watchCount(t, f.store, "actions") != 1 {
		t.Fatal("canceling an unsent due claim lost history or reached POST", err)
	}
	if ok, err := f.store.CanAttemptLike(context.Background(), 1, 7, f.now); err != nil || !ok {
		t.Fatal("old quota history blocked an unsent retry forever", err)
	}
	f.reply = `{"success":true}`
	if _, err := f.client.Like(context.Background(), 1, 7); err != nil || f.posts.Load() != 2 {
		t.Fatal("explicit resumed request could not perform exactly one real retry", err)
	}
}

func TestReactionLastMomentGuardKeepsBindingAndExcludesOwnBudgetReservation(t *testing.T) {
	t.Run("session_changed_after_claim", func(t *testing.T) {
		f := newReactionFixture(t)
		guard := func(ctx context.Context) error {
			if ClaimedLikeActionID(ctx) > 0 {
				return f.store.SaveSession(ctx, 1, Session{Token: "replacement", Refresh: "replacement", UserID: 303, Generation: "generation-2"})
			}
			return nil
		}
		if _, err := f.client.LikeGuarded(context.Background(), 1, 7, guard); !errors.Is(err, ErrReactionSessionChanged) || f.posts.Load() != 0 || watchCount(t, f.store, "actions") != 0 {
			t.Fatal("last-moment binding change reached a reaction", err)
		}
	})
	t.Run("one_slot_budget", func(t *testing.T) {
		f := newReactionFixture(t)
		guard := func(ctx context.Context) error {
			var used int
			if err := f.store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions WHERE owner_id=1 AND id!=? AND state IN ('pending','processing','succeeded','unknown')", ClaimedLikeActionID(ctx)).Scan(&used); err != nil {
				return err
			}
			if used >= 1 {
				return errors.New("own bot budget reached")
			}
			return nil
		}
		if _, err := f.client.LikeGuarded(context.Background(), 1, 7, guard); err != nil || f.posts.Load() != 1 {
			t.Fatal("claim counted itself against the remaining budget slot", err)
		}
		if _, err := f.client.LikeGuarded(context.Background(), 1, 8, guard); err == nil || f.posts.Load() != 1 || f.gets.Load() != 1 {
			t.Fatal("budget did not include an already confirmed manual/shared action")
		}
	})
}

func TestReactionClaimsSerializeAndProtectUnknownSuccessAndProcessing(t *testing.T) {
	f := newReactionFixture(t)
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := f.store.ClaimOwnerAction(context.Background(), 1, 7, f.now); results <- err }()
	}
	claimed := 0
	for range 2 {
		if err := <-results; err == nil {
			claimed++
		}
	}
	if claimed != 1 || watchCount(t, f.store, "actions") != 1 {
		t.Fatal("racing manual/auto claims both won")
	}
	for _, state := range []string{"pending", "processing", "succeeded", "unknown", "failed", "rejected"} {
		if _, err := f.store.DB.Exec("UPDATE actions SET state=?", state); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.ClaimOwnerAction(context.Background(), 1, 7, f.now.Add(48*time.Hour)); !errors.Is(err, ErrActionBlocked) {
			t.Fatalf("protected state %s was replayable: %v", state, err)
		}
	}
}

func TestReactionMigrationSerializesTwoLegacyStoreOpeners(t *testing.T) {
	cfg := testConfig(t)
	db, err := sql.Open("sqlite", "file:"+filepath.Join(cfg.DataDir, "jardulik.sqlite")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO users(id) VALUES(1); INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(1,7,'Like','failed','{"success":false}')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			s, err := OpenStore(cfg)
			if err == nil {
				var response string
				err = s.DB.QueryRow("SELECT response FROM actions WHERE owner_id=1 AND profile_id=7").Scan(&response)
				if err == nil && response != `{"success":false}` {
					err = errors.New("legacy action history changed")
				}
				s.Close()
			}
			results <- err
		}()
	}
	close(start)
	for range 2 {
		if err = <-results; err != nil {
			t.Fatal("parallel idempotent reaction migration failed", err)
		}
	}
}
