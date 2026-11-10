package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestPositiveReactionConfirmedFireFailureAllowsExplicitRetryWithoutFallback(t *testing.T) {
	f := newPositiveReactionFixture(t, int64(7))
	f.profile(t, 81, 22)
	f.policy(t, "rotate", 23)
	ctx := context.Background()
	service := f.auto.worker.Service
	store := service.Store
	client := service.Client
	if err := store.Favorite(ctx, 1, 81); err != nil {
		t.Fatal(err)
	}
	if _, err := f.auto.worker.settings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, "UPDATE autolike_settings SET daily_cap=1 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	const fireFailure = `{"IsSuccess":false,"MessageKind":-2}`
	f.auto.results = []string{fireFailure}
	if err := f.auto.worker.Start(ctx, 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	f.auto.step(t)
	if len(f.posts) != 1 {
		t.Fatalf("confirmed fire-refusal fixture did not send exactly once: %d", len(f.posts))
	}
	var mode, state, reason string
	if err := store.DB.QueryRowContext(ctx, "SELECT mode,state,reason FROM autolike_settings WHERE owner_id=1").Scan(&mode, &state, &reason); err != nil || mode != "off" || state != "off" || reason != "fire_unavailable" {
		t.Fatalf("automatic Super refusal did not stop explicitly: %s/%s %s err=%v", mode, state, reason, err)
	}
	var originalAction int64
	var originalRequest, originalResponse []byte
	if err := store.DB.QueryRowContext(ctx, `SELECT a.id,a.state,a.response,i.request_body FROM actions a JOIN reaction_message_intents i ON i.action_id=a.id WHERE a.owner_id=1 AND a.profile_id=81 ORDER BY a.id LIMIT 1`).Scan(&originalAction, &state, &originalResponse, &originalRequest); err != nil || state != "fire_waiting" || !bytes.Equal(originalRequest, f.posts[0]) || string(originalResponse) != fireFailure {
		t.Fatalf("confirmed -2 did not retain its precise fire_waiting outcome: state=%s err=%v", state, err)
	}
	var firstBody map[string]any
	if err := json.Unmarshal(originalRequest, &firstBody); err != nil || firstBody["reactionType"] != float64(3) {
		t.Fatalf("refused automatic request was not Super3: %v", err)
	}
	policy, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 1 {
		t.Fatalf("a confirmed POST refusal must consume its reserved variant: %+v %v", policy, err)
	}
	if _, err = client.PositiveReactionGuarded(ctx, 1, 81, PositiveSuperLike, nil, nil); !errors.Is(err, ErrPositiveSpacing) || len(f.posts) != 1 {
		t.Fatalf("immediate retry after a confirmed POST bypassed spacing: posts=%d err=%v", len(f.posts), err)
	}
	f.auto.now = f.auto.now.Add(61 * time.Second)
	if err = service.ManualLikeBudgetGuard(ctx, 1); err != nil {
		t.Fatalf("confirmed fire refusal consumed the rolling cap: %v", err)
	}

	// A later current profile would choose ordinary type1; it must not replace a refused Super.
	f.profile(t, 81, 23)
	if _, err = client.AutomaticLikeGuarded(ctx, 1, 81, nil); !errors.Is(err, ErrReactionFireUnavailable) || len(f.posts) != 1 {
		t.Fatalf("automatic type1 silently replaced a refused Super: posts=%d err=%v", len(f.posts), err)
	}
	f.fire = int64(0)
	if _, err = client.PositiveReactionGuarded(ctx, 1, 81, PositiveSuperLike, nil, nil); !errors.Is(err, ErrReactionFireUnavailable) || len(f.posts) != 1 {
		t.Fatalf("fresh fire0 reached another POST: posts=%d err=%v", len(f.posts), err)
	}
	policy, err = store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 1 {
		t.Fatalf("unsent fire0 or forbidden fallback consumed a variant: %+v %v", policy, err)
	}
	var intents int
	if err = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM reaction_message_intents WHERE owner_id=1 AND profile_id=81").Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("unsent fire checks retained a second intent: %d %v", intents, err)
	}

	f.fire = int64(3)
	result, err := client.PositiveReactionGuarded(ctx, 1, 81, PositiveSuperLike, nil, func(checkCtx context.Context) error { return service.ManualLikeBudgetGuard(checkCtx, 1) })
	if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 2 {
		t.Fatalf("explicit retry with fresh positive fire failed: %+v posts=%d err=%v", result, len(f.posts), err)
	}
	var retryBody map[string]any
	if err = json.Unmarshal(f.posts[1], &retryBody); err != nil || retryBody["reactionType"] != float64(3) || retryBody["message"] != policy.Templates[1] {
		t.Fatalf("retry lost Super3 or its next exact template: %v", err)
	}
	var latestAction int64
	if err = store.DB.QueryRowContext(ctx, "SELECT id,state FROM actions WHERE owner_id=1 AND profile_id=81 ORDER BY id DESC LIMIT 1").Scan(&latestAction, &state); err != nil || latestAction == originalAction || state != "succeeded" {
		t.Fatalf("retry did not create a distinct successful action: id=%d state=%s err=%v", latestAction, state, err)
	}
	var savedRequest, savedResponse []byte
	if err = store.DB.QueryRowContext(ctx, `SELECT i.request_body,a.response FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE a.id=?`, originalAction).Scan(&savedRequest, &savedResponse); err != nil || !bytes.Equal(savedRequest, originalRequest) || !bytes.Equal(savedResponse, originalResponse) {
		t.Fatal("successful retry rewrote the first request or confirmed refusal")
	}
	policy, err = store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 2 {
		t.Fatalf("two actual POST attempts must reserve two variants: %+v %v", policy, err)
	}
	if err = service.ManualLikeBudgetGuard(ctx, 1); !errors.Is(err, ErrOwnLikeLimit) {
		t.Fatalf("successful retry was not counted by the same cap1 that excluded the refusal: %v", err)
	}
	var quota int
	if err = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions WHERE owner_id=1 AND state='quota_waiting'").Scan(&quota); err != nil || quota != 0 {
		t.Fatalf("fire refusal became an unrelated timed quota: %d %v", quota, err)
	}
	if err = store.DB.QueryRowContext(ctx, "SELECT mode FROM autolike_settings WHERE owner_id=1").Scan(&mode); err != nil || mode != "off" {
		t.Fatal("manual retry unexpectedly restarted the automatic worker")
	}

	// An uncertain POST remains immutable even though confirmed fire refusals can be retried.
	if _, err = store.DB.ExecContext(ctx, "UPDATE autolike_settings SET daily_cap=2 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	f.auto.now = f.auto.now.Add(61 * time.Second)
	f.profile(t, 82, 22)
	f.auto.results = []string{"transport-error"}
	if _, err = client.PositiveReactionGuarded(ctx, 1, 82, PositiveSuperLike, nil, nil); err == nil || len(f.posts) != 3 {
		t.Fatalf("unknown fixture did not make one distinct POST: posts=%d err=%v", len(f.posts), err)
	}
	var unknownRequest []byte
	if err = store.DB.QueryRowContext(ctx, `SELECT i.request_body,a.state FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.owner_id=1 AND i.profile_id=82`).Scan(&unknownRequest, &state); err != nil || state != "unknown" {
		t.Fatalf("uncertain Super was not preserved as unknown: state=%s err=%v", state, err)
	}
	store.Pause(ctx, 1, false)
	f.auto.now = f.auto.now.Add(61 * time.Second)
	for _, kind := range []PositiveReactionType{PositiveLike, PositiveSuperLike} {
		if _, err = client.PositiveReactionGuarded(ctx, 1, 82, kind, nil, nil); !errors.Is(err, ErrActionBlocked) {
			t.Fatalf("unknown Super became retryable as positive type%d: %v", kind, err)
		}
	}
	var afterUnknown []byte
	if err = store.DB.QueryRowContext(ctx, "SELECT request_body FROM reaction_message_intents WHERE owner_id=1 AND profile_id=82").Scan(&afterUnknown); err != nil || !bytes.Equal(afterUnknown, unknownRequest) || len(f.posts) != 3 {
		t.Fatal("blocked unknown retry changed its exact request or sent again")
	}
	policy, err = store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 3 {
		t.Fatalf("unknown should consume once without further rotation or refund: %+v %v", policy, err)
	}
}

func TestPositiveReactionSpacingUsesActualAttemptClockAfterClaimDelay(t *testing.T) {
	f := newPositiveReactionFixture(t, int64(7))
	f.profile(t, 81, 22)
	f.profile(t, 82, 22)
	f.policy(t, "rotate", 23)
	client := f.auto.worker.Service.Client
	store := f.auto.worker.Service.Store
	ctx := context.Background()
	began := f.auto.now
	var postAt time.Time
	base := client.HTTP.Transport
	client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/reactions/addNew" {
			postAt = f.auto.now
		}
		return base.RoundTrip(r)
	})
	delayed := false
	guard := func(checkCtx context.Context) error {
		if ClaimedLikeActionID(checkCtx) > 0 && !delayed {
			delayed = true
			f.auto.now = f.auto.now.Add(20 * time.Second)
		}
		return nil
	}
	result, err := client.PositiveReactionGuarded(ctx, 1, 81, PositiveSuperLike, nil, guard)
	if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 1 || !delayed || !postAt.Equal(began.Add(20*time.Second)) {
		t.Fatalf("fixture did not delay one claimed request before its POST: result=%+v posts=%d delayed=%v postAt=%v err=%v", result, len(f.posts), delayed, postAt, err)
	}
	var attempted int64
	if err = store.DB.QueryRowContext(ctx, "SELECT attempted_at_ms FROM reaction_message_intents WHERE owner_id=1 AND profile_id=81").Scan(&attempted); err != nil || attempted != postAt.UnixMilli() {
		t.Fatalf("attempt timestamp used stale pre-claim time: got=%d want=%d err=%v", attempted, postAt.UnixMilli(), err)
	}
	firstPost := postAt
	f.auto.now = firstPost.Add(59 * time.Second)
	_, err = client.PositiveReactionGuarded(ctx, 1, 82, PositiveSuperLike, nil, nil)
	var spacing *PositiveSpacingError
	if !errors.As(err, &spacing) || spacing.RetryAt.Before(firstPost.Add(time.Minute)) || len(f.posts) != 1 {
		t.Fatalf("second attempt was allowed before actual POST+60s: posts=%d retry=%+v err=%v", len(f.posts), spacing, err)
	}
	policy, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 1 {
		t.Fatalf("spacing rejection consumed the next message variant: %+v %v", policy, err)
	}
	f.auto.now = firstPost.Add(time.Minute)
	result, err = client.PositiveReactionGuarded(ctx, 1, 82, PositiveSuperLike, nil, nil)
	if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 2 {
		t.Fatalf("second request did not become eligible at actual POST+60s: result=%+v posts=%d err=%v", result, len(f.posts), err)
	}
	if err = store.DB.QueryRowContext(ctx, "SELECT attempted_at_ms FROM reaction_message_intents WHERE owner_id=1 AND profile_id=82").Scan(&attempted); err != nil || attempted < firstPost.Add(time.Minute).UnixMilli() {
		t.Fatalf("second durable attempt predates the actual spacing boundary: %d %v", attempted, err)
	}
}
