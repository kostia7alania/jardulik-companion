package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func positiveBudgetAudit(t *testing.T, s *Store, owner, upstream, target, kind int64, state string, at time.Time, action any) int64 {
	t.Helper()
	r, err := s.DB.Exec(`INSERT INTO debug_api_calls(owner_id,request_id,upstream_id,generation,path,target_id,reaction_type,request_body,response_body,http_status,state,created_at,updated_at,action_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, owner, fmt.Sprintf("budget-%d-%d-%d-%s", owner, upstream, target, state), upstream, "fixture", "/reactions/addNew", target, kind, []byte(`{"fixture":true}`), []byte(`{"IsSuccess":true}`), 200, state, at.UTC().Format("2006-01-02 15:04:05"), at.UTC().Format("2006-01-02 15:04:05"), action)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func positiveBudgetAction(t *testing.T, s *Store, owner, target int64, state string, at time.Time) int64 {
	t.Helper()
	r, err := s.DB.Exec("INSERT INTO actions(owner_id,profile_id,kind,state,created_at) VALUES(?,?,'Like',?,?)", owner, target, state, at.UTC().Format("2006-01-02 15:04:05"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func positiveBudgetIntent(t *testing.T, s *Store, action, owner, upstream, target, kind int64) {
	t.Helper()
	if _, err := s.DB.Exec(`INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reserved_cursor,reaction_type,attempted_at_ms) SELECT ?,?,?,?,?,'fixture',?,'explicit',0,-1,-1,?,CAST(strftime('%s',created_at) AS INTEGER)*1000 FROM actions WHERE id=?`, action, owner, upstream, target, "fixture", []byte(`{"fixture":true}`), kind, action); err != nil {
		t.Fatal(err)
	}
}

func TestPositiveBudgetLegacySuperBlocksTargetAndKeepsSpacing(t *testing.T) {
	for _, state := range []string{"received", "sending", "unknown"} {
		t.Run(state, func(t *testing.T) {
			f := newAutoFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			f.add(t, 1, 7, 23, false)
			f.add(t, 2, 7, 23, false)
			if err := s.initReactionMessages(ctx); err != nil {
				t.Fatal(err)
			}
			positiveBudgetAudit(t, s, 1, 101, 7, 3, state, f.now, nil)
			ok, err := s.CanAttemptLike(ctx, 1, 7, f.now)
			var blocked *ActionBlockedError
			expected := state
			if state == "received" {
				expected = "succeeded"
			}
			if ok || !errors.As(err, &blocked) || blocked.State != expected {
				t.Fatalf("legacy Super allowed duplicate: ok=%v err=%v", ok, err)
			}
			if ok, err = s.CanAttemptLike(ctx, 2, 7, f.now); !ok || err != nil {
				t.Fatalf("another owner inherited block: %v %v", ok, err)
			}
			budget, err := s.positiveBudget(ctx, 1, f.now)
			if err != nil || budget.Count != 1 || !budget.LatestSuper.Equal(f.now.Add(time.Second)) {
				t.Fatalf("legacy Super absent from budget: %+v %v", budget, err)
			}
			var spacing *PositiveSpacingError
			err = s.checkPositiveBudget(ctx, 1, f.now.Add(30*time.Second), false)
			if !errors.As(err, &spacing) || spacing.RetryAt.Before(f.now.Add(time.Minute)) {
				t.Fatalf("Super spacing missing: %v", err)
			}
			if err = s.checkPositiveBudget(ctx, 1, f.now.Add(61*time.Second), false); err != nil {
				t.Fatalf("spacing never releases: %v", err)
			}
			if len(f.requests) != 0 || len(f.sent) != 0 {
				t.Fatal("budget/dedup issued source requests")
			}
		})
	}
}

func TestPositiveBudgetIgnoresOtherSourcesAndDefinitiveFailures(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		owner, upstream, kind int64
		state                 string
	}{
		{"other source", 1, 999, 3, "received"},
		{"other owner", 2, 102, 3, "received"},
		{"ordinary audit", 1, 101, 1, "received"},
		{"rejected", 1, 101, 3, "rejected"},
		{"cancelled", 1, 101, 3, "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAutoFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			f.add(t, 1, 7, 23, false)
			if err := s.initReactionMessages(ctx); err != nil {
				t.Fatal(err)
			}
			positiveBudgetAudit(t, s, tc.owner, tc.upstream, 7, tc.kind, tc.state, f.now, nil)
			if ok, err := s.CanAttemptLike(ctx, 1, 7, f.now); !ok || err != nil {
				t.Fatalf("unrelated audit blocked target: %v %v", ok, err)
			}
			budget, err := s.positiveBudget(ctx, 1, f.now)
			if err != nil || budget.Count != 0 {
				t.Fatalf("unrelated or rejected audit consumed cap: %+v %v", budget, err)
			}
			rejected := tc.state == "rejected"
			if rejected && !budget.LatestSuper.Equal(f.now.Add(time.Second)) || !rejected && !budget.LatestSuper.IsZero() {
				t.Fatalf("rejected POST spacing mixed with quota/isolation: %+v", budget)
			}
			err = s.checkPositiveBudget(ctx, 1, f.now, false)
			if errors.Is(err, ErrPositiveSpacing) != rejected || err != nil && !errors.Is(err, ErrPositiveSpacing) {
				t.Fatalf("rejected POST must space only: %v", err)
			}
			if len(f.requests) != 0 {
				t.Fatal("read-only guards called source")
			}
		})
	}
}

func TestPositiveBudgetLinksDebugWithoutDoubleCountingAndExcludesOwnClaim(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	if err := s.initReactionMessages(ctx); err != nil {
		t.Fatal(err)
	}
	ordinary := positiveBudgetAction(t, s, 1, 7, "succeeded", f.now.Add(-5*time.Minute))
	positiveBudgetIntent(t, s, ordinary, 1, 101, 7, 1)
	positiveBudgetAudit(t, s, 1, 101, 7, 1, "received", f.now.Add(-5*time.Minute), ordinary)
	super := positiveBudgetAction(t, s, 1, 8, "succeeded", f.now.Add(-3*time.Minute))
	positiveBudgetIntent(t, s, super, 1, 101, 8, 3)
	positiveBudgetAudit(t, s, 1, 101, 8, 3, "received", f.now.Add(-3*time.Minute), super)
	positiveBudgetAudit(t, s, 1, 101, 9, 3, "received", f.now.Add(-2*time.Minute), nil)
	ownAction := positiveBudgetAction(t, s, 1, 10, "pending", f.now)
	ownAudit := positiveBudgetAudit(t, s, 1, 101, 10, 3, "sending", f.now, nil)
	ownCtx := context.WithValue(ctx, claimedLikeContextKey{}, ownAction)
	ownCtx = context.WithValue(ownCtx, positiveAuditContextKey{}, ownAudit)
	budget, err := s.positiveBudget(ownCtx, 1, f.now)
	if err != nil || budget.Count != 3 || !budget.LatestSuper.Equal(f.now.Add(-2*time.Minute+time.Second)) {
		t.Fatalf("own reservation consumed budget or linked audit counted twice: %+v %v", budget, err)
	}
	if err = s.checkPositiveBudget(ownCtx, 1, f.now, true); err != nil {
		t.Fatalf("own claim blocked itself: %v", err)
	}
	positiveBudgetIntent(t, s, ownAction, 1, 101, 10, 3)
	if _, err = s.DB.Exec("UPDATE debug_api_calls SET action_id=? WHERE id=?", ownAction, ownAudit); err != nil {
		t.Fatal(err)
	}
	budget, err = s.positiveBudget(ctx, 1, f.now)
	if err != nil || budget.Count != 4 || !budget.LatestSuper.Equal(f.now) {
		t.Fatalf("linked Super was not exactly one reservation: %+v %v", budget, err)
	}
	if err = s.checkPositiveBudget(ctx, 1, f.now, true); !errors.Is(err, ErrPositiveSpacing) {
		t.Fatalf("unrelated next send bypassed pending Super spacing: %v", err)
	}
}

func TestPositiveBudgetCap100IncludesManualSuperInRollingWindow(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	if _, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "first", Templates: []string{"fixture"}, SuperBeforeAge: 23}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.worker.settings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("UPDATE autolike_settings SET daily_cap=500 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	oldest := f.now.Add(-2 * time.Hour)
	for n := int64(0); n < 99; n++ {
		positiveBudgetAction(t, s, 1, 1000+n, "succeeded", oldest)
	}
	legacy := positiveBudgetAudit(t, s, 1, 101, 7, 3, "received", f.now.Add(-time.Hour), nil)
	positiveBudgetAction(t, s, 1, 2000, "unknown", f.now.Add(-24*time.Hour))
	positiveBudgetAction(t, s, 1, 2001, "fire_waiting", f.now.Add(-2*time.Minute))
	positiveBudgetAction(t, s, 2, 2002, "succeeded", f.now)
	var limit *OwnLikeLimitError
	err := s.checkPositiveBudget(ctx, 1, f.now, false)
	if !errors.As(err, &limit) || limit.Limit != 100 || !limit.RetryAt.Equal(oldest.Add(24*time.Hour+time.Second)) {
		t.Fatalf("manual Super omitted from hard100 or rolling boundary wrong: %v", err)
	}
	if _, err = s.DB.Exec("UPDATE debug_api_calls SET state='cancelled' WHERE id=?", legacy); err != nil {
		t.Fatal(err)
	}
	budget, err := s.positiveBudget(ctx, 1, f.now)
	if err != nil || budget.Count != 99 {
		t.Fatalf("definitive failure consumed cap: %+v %v", budget, err)
	}
	if err = s.checkPositiveBudget(ctx, 1, f.now, false); err != nil {
		t.Fatalf("cancelled manual Super kept cap blocked: %v", err)
	}
	if len(f.requests) != 0 {
		t.Fatal("budget checked source over HTTP")
	}
}

func TestPositiveBudgetConfirmedRejectedAttemptSpacesWithoutConsumingCap(t *testing.T) {
	for _, state := range []string{"rejected", "fire_waiting"} {
		t.Run(state, func(t *testing.T) {
			f := newAutoFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			if err := s.initReactionMessages(ctx); err != nil {
				t.Fatal(err)
			}
			action := positiveBudgetAction(t, s, 1, 7, state, f.now.Add(-2*time.Hour))
			positiveBudgetIntent(t, s, action, 1, 101, 7, 3)
			attempted := f.now.Add(-30*time.Second + 500*time.Millisecond)
			if _, err := s.DB.Exec("UPDATE reaction_message_intents SET attempted_at_ms=? WHERE action_id=?", attempted.UnixMilli(), action); err != nil {
				t.Fatal(err)
			}
			budget, err := s.positiveBudget(ctx, 1, f.now)
			if err != nil || budget.Count != 0 || !budget.LatestSuper.Equal(attempted) {
				t.Fatalf("confirmed refusal changed cap or lost actual dispatch time: %+v %v", budget, err)
			}
			var spacing *PositiveSpacingError
			err = s.checkPositiveBudget(ctx, 1, f.now, false)
			if !errors.As(err, &spacing) || !spacing.RetryAt.Equal(attempted.Add(time.Minute)) {
				t.Fatalf("confirmed refusal bypassed spacing: %v", err)
			}
			if err = s.checkPositiveBudget(ctx, 1, attempted.Add(time.Minute), false); err != nil {
				t.Fatalf("exact millisecond spacing did not release: %v", err)
			}
			if len(f.requests) != 0 {
				t.Fatal("budget checked source over HTTP")
			}
		})
	}
}

func TestPositiveBudgetLegacySchemaBackupAndLazyMigration(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	const legacy = `
 CREATE TABLE reaction_message_settings(owner_id INTEGER PRIMARY KEY,mode TEXT NOT NULL,templates_json TEXT NOT NULL,telegram_username TEXT NOT NULL,prefer_drafts INTEGER NOT NULL DEFAULT 0,iron_mix INTEGER NOT NULL DEFAULT 0,next_cursor INTEGER NOT NULL DEFAULT 0,revision INTEGER NOT NULL DEFAULT 1,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
 CREATE TABLE reaction_message_intents(action_id INTEGER PRIMARY KEY REFERENCES actions(id) ON DELETE CASCADE,owner_id INTEGER NOT NULL,upstream_id INTEGER NOT NULL,profile_id INTEGER NOT NULL,generation TEXT NOT NULL,message TEXT NOT NULL,request_body BLOB NOT NULL,message_mode TEXT NOT NULL,policy_revision INTEGER NOT NULL,template_index INTEGER NOT NULL,reserved_cursor INTEGER NOT NULL DEFAULT -1,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
 CREATE TABLE debug_api_calls(id INTEGER PRIMARY KEY,owner_id INTEGER NOT NULL,request_id TEXT NOT NULL,upstream_id INTEGER NOT NULL,generation TEXT NOT NULL,path TEXT NOT NULL,target_id INTEGER NOT NULL,reaction_type INTEGER NOT NULL,request_body BLOB NOT NULL,response_body BLOB NOT NULL,http_status INTEGER NOT NULL,state TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,UNIQUE(owner_id,request_id));
 INSERT INTO users(id) VALUES(1);
 INSERT INTO reaction_message_settings(owner_id,mode,templates_json,telegram_username,next_cursor,revision) VALUES(1,'first','["unchanged"]','fixture_user',8,17);
 INSERT INTO actions(id,owner_id,profile_id,kind,state) VALUES(9,1,7,'Like','unknown');
 INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index) VALUES(9,1,101,7,'fixture','unchanged',X'7B7D','first',17,0);
 INSERT INTO debug_api_calls(id,owner_id,request_id,upstream_id,generation,path,target_id,reaction_type,request_body,response_body,http_status,state) VALUES(11,1,'old-super',101,'fixture','/reactions/addNew',8,3,X'7B7D',X'7B7D',200,'received');`
	if _, err = db.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = fullDurableStateRecords(ctx, db); err != nil {
		t.Fatal("pre-upgrade backup projection failed", err)
	}
	s := &Store{DB: db}
	if err = s.initReactionActions(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.SuperBeforeAge != 0 || p.NextCursor != 8 || p.Revision != 17 || p.Mode != "first" || !reflect.DeepEqual(p.Templates, []string{"unchanged"}) {
		t.Fatalf("migration changed old policy: %+v %v", p, err)
	}
	var kind int64
	var link sql.NullInt64
	if err = db.QueryRow("SELECT reaction_type FROM reaction_message_intents WHERE action_id=9").Scan(&kind); err != nil || kind != 1 {
		t.Fatalf("old intent lost ordinary type: %d %v", kind, err)
	}
	if err = db.QueryRow("SELECT action_id FROM debug_api_calls WHERE id=11").Scan(&link); err != nil || link.Valid {
		t.Fatalf("migration fabricated legacy action link: %v %v", link, err)
	}
	before, err := fullDurableStateRecords(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(before)
	if err = s.SetSuperBeforeAge(ctx, 1, 23); err != nil {
		t.Fatal(err)
	}
	after, err := fullDurableStateRecords(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("cutoff-only policy change did not dirty backup")
	}
	p, err = s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.SuperBeforeAge != 23 || p.NextCursor != 8 || p.Revision != 18 {
		t.Fatalf("cutoff setter changed rotation or lost revision: %+v %v", p, err)
	}
}
