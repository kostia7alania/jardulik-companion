package companion

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

const emptyFireOpener = "Привет! Рад знакомству 🙂 @fixture_user"

func setEmptyFireOpenerPolicy(t *testing.T, f *positiveReactionFixture, enabled bool) {
	t.Helper()
	_, err := f.auto.worker.Service.Store.ConfigureReactionMessagePolicy(context.Background(), 1, ReactionMessagePolicyConfig{
		Mode:               "rotate",
		Templates:          []string{emptyFireOpener, "Второй текст @fixture_user"},
		TelegramUsername:   "fixture_user",
		SuperBeforeAge:     23,
		OrdinaryWhenNoFire: enabled,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReactionMessagesEmptyFireModeUsesFreshFireBeforeSinglePost(t *testing.T) {
	for _, tc := range []struct {
		name    string
		age     int64
		cached  int64
		fresh   any
		enabled bool
		want    PositiveReactionType
		wantErr error
	}{
		{name: "zero at adult lower boundary", age: 18, cached: 7, fresh: int64(0), enabled: true, want: PositiveLike},
		{name: "zero just below cutoff", age: 22, cached: 7, fresh: int64(0), enabled: true, want: PositiveLike},
		{name: "positive just below cutoff", age: 22, cached: 0, fresh: int64(7), enabled: true, want: PositiveSuperLike},
		{name: "at cutoff with missing fire", age: 23, cached: 7, enabled: true, want: PositiveLike},
		{name: "missing fire", age: 22, cached: 0, enabled: true, wantErr: ErrReactionFireUnknown},
		{name: "malformed fire", age: 22, cached: 0, fresh: "unknown", enabled: true, wantErr: ErrReactionFireUnknown},
		{name: "fractional fire", age: 22, cached: 0, fresh: 0.5, enabled: true, wantErr: ErrReactionFireUnknown},
		{name: "zero without opt-in", age: 22, cached: 7, fresh: int64(0), wantErr: ErrReactionFireUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newPositiveReactionFixture(t, tc.fresh)
			f.profile(t, 81, tc.age)
			setEmptyFireOpenerPolicy(t, f, tc.enabled)
			service := f.auto.worker.Service
			if err := service.Store.SaveAccount(ctx, 1, Profile{"userId": 101, "age": 33, "type": 1, "freezeState": false, "fire": tc.cached}); err != nil {
				t.Fatal(err)
			}
			result, err := service.Client.AutomaticLikeGuarded(ctx, 1, 81, nil)
			policy, policyErr := service.Store.ReactionMessagePolicy(ctx, 1)
			if policyErr != nil {
				t.Fatal(policyErr)
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || len(f.posts) != 0 || policy.NextCursor != 0 {
					t.Fatalf("preflight rejection should preserve the cursor and make no POST: posts=%d cursor=%d err=%v", len(f.posts), policy.NextCursor, err)
				}
				return
			}
			if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 1 || policy.NextCursor != 1 {
				t.Fatalf("automatic dispatch did not make one confirmed POST: decision=%s posts=%d cursor=%d err=%v", result.Decision, len(f.posts), policy.NextCursor, err)
			}
			var wire struct {
				Kind    PositiveReactionType `json:"reactionType"`
				Message string               `json:"message"`
			}
			if err = json.Unmarshal(f.posts[0], &wire); err != nil || wire.Kind != tc.want || wire.Message != emptyFireOpener {
				t.Fatalf("wrong reaction or opener: type=%d want=%d message=%q err=%v", wire.Kind, tc.want, wire.Message, err)
			}
		})
	}
}

func TestReactionMessagesEmptyFireModeDoesNotReplaceSubmittedSuper(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reply     string
		state     string
		reason    string
		secondErr error
	}{
		{name: "confirmed fire rejection", reply: `{"IsSuccess":false,"MessageKind":-2}`, state: "fire_waiting", reason: "fire_unavailable", secondErr: errPositiveAlreadyAttempted},
		{name: "uncertain transport", reply: "transport-error", state: "unknown", reason: "reaction_failed", secondErr: ErrActionBlocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newPositiveReactionFixture(t, int64(7))
			f.profile(t, 81, 22)
			setEmptyFireOpenerPolicy(t, f, true)
			service := f.auto.worker.Service
			if err := service.Store.Favorite(ctx, 1, 81); err != nil {
				t.Fatal(err)
			}
			f.auto.results = []string{tc.reply}
			if err := f.auto.worker.Start(ctx, 1, "automatic", 10); err != nil {
				t.Fatal(err)
			}
			f.auto.step(t)
			job, err := f.auto.worker.settings(ctx, 1)
			if err != nil || job.Mode != "off" || job.Reason != tc.reason || len(f.posts) != 1 {
				t.Fatalf("submitted Super did not stop after one POST: job=%+v posts=%d err=%v", job, len(f.posts), err)
			}
			var state string
			var kind PositiveReactionType
			if err = service.Store.DB.QueryRowContext(ctx, `SELECT a.state,i.reaction_type FROM actions a JOIN reaction_message_intents i ON i.action_id=a.id WHERE a.owner_id=1 AND a.profile_id=81`).Scan(&state, &kind); err != nil || state != tc.state || kind != PositiveSuperLike {
				t.Fatalf("wrong preserved Super outcome: state=%s kind=%d err=%v", state, kind, err)
			}
			f.auto.now = f.auto.now.Add(61 * time.Second)
			f.fire = int64(0)
			service.Store.Pause(ctx, 1, false)
			if _, err = service.Client.AutomaticLikeGuarded(ctx, 1, 81, nil); !errors.Is(err, tc.secondErr) || len(f.posts) != 1 {
				t.Fatalf("fresh zero Fire replaced an already submitted Super: posts=%d err=%v", len(f.posts), err)
			}
		})
	}
}

func TestReactionMessagesEmptyFireModePreservesOrdinaryQuotaAndPastAttempt(t *testing.T) {
	ctx := context.Background()
	f := newPositiveReactionFixture(t, int64(0))
	f.profile(t, 81, 22)
	f.profile(t, 82, 22)
	setEmptyFireOpenerPolicy(t, f, true)
	service := f.auto.worker.Service

	action, err := service.Store.ClaimOwnerAction(ctx, 1, 81, f.auto.now)
	if err != nil {
		t.Fatal(err)
	}
	const rejected = `{"IsSuccess":false,"MessageKind":-1,"MinutesLeft":2}`
	retryAt := f.auto.now.Add(2 * time.Minute)
	if err = service.Store.finishReactionAction(ctx, action, "quota_waiting", []byte(rejected), retryAt); err != nil {
		t.Fatal(err)
	}
	_, err = service.Client.AutomaticLikeGuarded(ctx, 1, 82, nil)
	var blocked *ActionBlockedError
	if !errors.As(err, &blocked) || blocked.State != "quota_waiting" || len(f.posts) != 0 {
		t.Fatalf("zero Fire bypassed account-wide ordinary quota: %v", err)
	}

	f.auto.now = retryAt.Add(time.Second)
	if _, err = service.Client.AutomaticLikeGuarded(ctx, 1, 81, nil); !errors.Is(err, errPositiveAlreadyAttempted) || len(f.posts) != 0 {
		t.Fatalf("elapsed quota let first-only mode repeat a prior attempt: %v", err)
	}
	var state, response string
	var rows int
	if err = service.Store.DB.QueryRow("SELECT state,response FROM actions WHERE id=?", action).Scan(&state, &response); err != nil {
		t.Fatal(err)
	}
	if err = service.Store.DB.QueryRow("SELECT count(*) FROM actions WHERE owner_id=1").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if state != "quota_waiting" || response != rejected || rows != 1 {
		t.Fatalf("skip changed the historical attempt: state=%s response=%q rows=%d", state, response, rows)
	}
}

func TestReactionMessagesEmptyFireModeRechecksZeroAfterIntent(t *testing.T) {
	ctx := context.Background()
	f := newPositiveReactionFixture(t, int64(0))
	f.profile(t, 81, 22)
	setEmptyFireOpenerPolicy(t, f, true)
	service := f.auto.worker.Service
	changed := false
	guard := func(ctx context.Context) error {
		action := ClaimedLikeActionID(ctx)
		if action == 0 || changed {
			return nil
		}
		var prepared bool
		if err := service.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM reaction_message_intents WHERE action_id=?)", action).Scan(&prepared); err != nil {
			return err
		}
		if !prepared {
			return nil
		}
		changed = true
		return service.Store.SaveAccount(ctx, 1, Profile{"userId": 101, "fire": 5})
	}
	_, err := service.Client.AutomaticLikeGuarded(ctx, 1, 81, guard)
	if !changed || !errors.Is(err, ErrReactionFireUnknown) || len(f.posts) != 0 {
		t.Fatalf("changed balance permitted a stale conversion: changed=%v posts=%d err=%v", changed, len(f.posts), err)
	}
	policy, err := service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 0 {
		t.Fatalf("cancelled intent advanced opener cursor: policy=%+v err=%v", policy, err)
	}
}
