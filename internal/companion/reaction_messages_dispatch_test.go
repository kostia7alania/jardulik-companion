package companion

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func TestPositiveDispatchRechecksStopAfterWaitingForWriter(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	f.add(t, 1, 7, 22, false)
	if err := s.SaveAccount(ctx, 1, Profile{"userId": 101, "fire": 3, "freezeState": false}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "rotate", Templates: []string{"one", "two"}, SuperBeforeAge: 23}); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Start(ctx, 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	j, err := f.worker.settings(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.worker.guard(ctx, j); err != nil {
		t.Fatal(err)
	}
	session, err := s.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	action, err := s.ClaimOwnerAction(ctx, 1, 7, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.preparePositiveMessage(ctx, 1, action, target, session, nil, PositiveLike, true); err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(s.cfg.DataDir, "jardulik.sqlite")}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	other, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	blocker, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	// The reader's earlier guard saw active; off is not visible until this writer commits.
	if _, err = blocker.ExecContext(ctx, "UPDATE autolike_settings SET mode='off',state='off',reason='user_off' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	dispatch := context.WithValue(ctx, automaticDispatchContextKey{}, automaticDispatchBinding{Owner: 1, RunID: j.RunID, Generation: j.Generation, Upstream: j.UserID, FilterHash: j.FilterHash})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- s.markPositiveAttempt(dispatch, 1, action, session, true, func() time.Time { return f.now })
	}()
	<-started
	select {
	case err = <-done:
		t.Fatalf("dispatch bypassed writer reservation: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if err = blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not finish after writer released")
	}
	if !errors.Is(err, errAutoChanged) {
		t.Fatalf("off committed during wait was ignored: %v", err)
	}
	var attempted int64
	if err = s.DB.QueryRow("SELECT attempted_at_ms FROM reaction_message_intents WHERE action_id=?", action).Scan(&attempted); err != nil || attempted != 0 {
		t.Fatalf("stopped dispatch stamped attempt: %d %v", attempted, err)
	}
	if err = s.releaseUnsentReaction(ctx, action); err != nil {
		t.Fatal(err)
	}
	policy, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 0 {
		t.Fatalf("unsent stop did not refund cursor: %+v %v", policy, err)
	}
	if len(f.requests) != 0 || len(f.sent) != 0 {
		t.Fatal("dispatch test performed source HTTP")
	}
}
