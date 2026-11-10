package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func holdResolutionFixture(t *testing.T) (*autoFixture, int64) {
	t.Helper()
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	p, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["age"] = 18
	p.Profile["birthday"] = time.Now().UTC().AddDate(-18, 0, 0).Format("2006-01-02")
	if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "adult hold evidence"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "hold", ""))); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err = s.DB.QueryRow("SELECT id FROM reaction_message_drafts WHERE state='hold'").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return f, id
}

func assertActiveHold(t *testing.T, s *Store, want bool) {
	t.Helper()
	tx, err := s.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = reactionDraftHoldTx(context.Background(), tx, 1, 101, 7)
	if errors.Is(err, ErrReactionMessageReviewHold) != want || err != nil && !errors.Is(err, ErrReactionMessageReviewHold) {
		t.Fatalf("active hold want=%v: %v", want, err)
	}
}

func TestReactionHoldResolveAdultAuditExactAndIdempotent(t *testing.T) {
	f, id := holdResolutionFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	var capture []byte
	var captureSHA string
	if err := s.DB.QueryRow("SELECT capture_json,capture_sha FROM reaction_message_drafts WHERE id=?", id).Scan(&capture, &captureSHA); err != nil {
		t.Fatal(err)
	}
	beforePolicy, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec("UPDATE users SET paused=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	result, err := s.ResolveReactionMessageHold(ctx, 1, id, "  Current source confirms adult DOB; old school bio.  ")
	if err != nil || result.AlreadyResolved || result.DraftID != id || result.OwnerID != 1 || result.UpstreamID != 101 || result.ProfileID != 7 {
		t.Fatalf("resolve: %+v %v", result, err)
	}
	assertActiveHold(t, s, false)
	var raw []byte
	var sha, created string
	if err = s.DB.QueryRow("SELECT evidence_json,evidence_sha,created_at FROM reaction_message_hold_resolutions WHERE draft_id=?", id).Scan(&raw, &sha, &created); err != nil {
		t.Fatal(err)
	}
	var evidence reactionMessageHoldEvidence
	if err = json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	p, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if sha != digest(raw) || evidence.TargetRawJSON != p.RawJSON || evidence.TargetRawSHA != digest([]byte(p.RawJSON)) || evidence.HoldCaptureSHA != captureSHA || evidence.Reason != "  Current source confirms adult DOB; old school bio.  " || evidence.ObservedAt == "" || evidence.ResolvedAt == "" {
		t.Fatal("resolution lost exact raw evidence or reason")
	}
	result, err = s.ResolveReactionMessageHold(ctx, 1, id, evidence.Reason)
	if err != nil || !result.AlreadyResolved {
		t.Fatalf("repeat: %+v %v", result, err)
	}
	if _, err = s.ResolveReactionMessageHold(ctx, 1, id, "different review"); err == nil {
		t.Fatal("conflicting review overwrote audit")
	}
	var after, oldCapture []byte
	var afterSHA, afterCreated, oldSHA string
	if err = s.DB.QueryRow("SELECT evidence_json,evidence_sha,created_at FROM reaction_message_hold_resolutions WHERE draft_id=?", id).Scan(&after, &afterSHA, &afterCreated); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRow("SELECT capture_json,capture_sha FROM reaction_message_drafts WHERE id=? AND state='hold'", id).Scan(&oldCapture, &oldSHA); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, after) || sha != afterSHA || created != afterCreated || !bytes.Equal(capture, oldCapture) || captureSHA != oldSHA {
		t.Fatal("repeat rewrote immutable evidence")
	}
	status, err := s.ReactionMessageDraftStatus(ctx, 1)
	if err != nil || status["hold"] != 0 || status["resolved_hold"] != 1 {
		t.Fatalf("status: %v %v", status, err)
	}
	afterPolicy, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || !reflect.DeepEqual(beforePolicy, afterPolicy) {
		t.Fatal("resolution changed policy", err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", "Reviewed @fixture_user"))); err != nil {
		t.Fatal("resolved hold still blocks reviewed import", err)
	}
	var paused, actions int
	if err = s.DB.QueryRow("SELECT paused FROM users WHERE id=1").Scan(&paused); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRow("SELECT count(*) FROM actions").Scan(&actions); err != nil {
		t.Fatal(err)
	}
	if paused != 1 || actions != 0 || len(f.sent) != 0 {
		t.Fatal("resolution unpaused owner or sent a reaction")
	}
}

func TestReactionHoldResolveRequiresKnownAdultDOB(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  any
		dob  any
	}{
		{"underage source", 17, "2000-01-01"}, {"unknown source", nil, "2000-01-01"},
		{"missing birthday", 18, nil}, {"invalid birthday", 18, "2000-02-31"},
		{"future birthday", 18, time.Now().UTC().AddDate(1, 0, 0).Format("2006-01-02")},
		{"contradictory minor birthday", 18, time.Now().UTC().AddDate(-17, 0, 0).Format("2006-01-02")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, id := holdResolutionFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			p, err := s.Profile(ctx, 1, 7)
			if err != nil {
				t.Fatal(err)
			}
			p.Profile["age"] = tc.age
			p.Profile["birthday"] = tc.dob
			if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "changed current age evidence"); err != nil {
				t.Fatal(err)
			}
			if _, err = s.ResolveReactionMessageHold(ctx, 1, id, "review"); !errors.Is(err, ErrReactionMessageTargetAge) {
				t.Fatalf("adult evidence bypassed: %v", err)
			}
			var count int
			if err = s.DB.QueryRow("SELECT count(*) FROM reaction_message_hold_resolutions").Scan(&count); err != nil || count != 0 {
				t.Fatalf("invalid resolution persisted: %d %v", count, err)
			}
			assertActiveHold(t, s, true)
		})
	}
}

func TestReactionHoldResolveExactDraftAndIsolation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		owner    int64
		mutation string
	}{
		{"other owner", 2, ""},
		{"changed upstream", 1, "UPDATE sessions SET upstream_id=999 WHERE owner_id=1"},
		{"mismatched account", 1, `UPDATE sessions SET account_json='{"userId":999}' WHERE owner_id=1`},
		{"missing target", 1, "DELETE FROM queue WHERE owner_id=1 AND profile_id=7"},
		{"approved row", 1, "UPDATE reaction_message_drafts SET state='approved'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, id := holdResolutionFixture(t)
			s := f.worker.Service.Store
			if tc.mutation != "" {
				if _, err := s.DB.Exec(tc.mutation); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ResolveReactionMessageHold(context.Background(), tc.owner, id, "review"); err == nil {
				t.Fatal("binding bypassed")
			}
			var count int
			if err := s.DB.QueryRow("SELECT count(*) FROM reaction_message_hold_resolutions").Scan(&count); err != nil || count != 0 {
				t.Fatalf("wrong binding persisted: %d %v", count, err)
			}
		})
	}
	f, first := holdResolutionFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	p, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["description"] = "a different reviewed concern"
	if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "second hold source"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "hold", ""))); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveReactionMessageHold(ctx, 1, first, "only first hold resolved"); err != nil {
		t.Fatal(err)
	}
	assertActiveHold(t, s, true)
	var second int64
	if err = s.DB.QueryRow("SELECT max(id) FROM reaction_message_drafts").Scan(&second); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveReactionMessageHold(ctx, 1, second, "second reviewed separately"); err != nil {
		t.Fatal(err)
	}
	assertActiveHold(t, s, false)
	p.Profile["description"] = "a later independent concern"
	if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "later hold source"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "hold", ""))); err != nil {
		t.Fatal(err)
	}
	assertActiveHold(t, s, true)
}

func TestReactionHoldResolveRollbackAndUnknownUnchanged(t *testing.T) {
	f, id := holdResolutionFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	action, err := s.ClaimOwnerAction(ctx, 1, 7, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.finishReactionAction(ctx, action, "unknown", []byte(`{"fixture":"unconfirmed"}`), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`CREATE TRIGGER fail_hold_resolution BEFORE INSERT ON reaction_message_hold_resolutions BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveReactionMessageHold(ctx, 1, id, "review"); err == nil {
		t.Fatal("expected insert failure")
	}
	assertActiveHold(t, s, true)
	if _, err = s.DB.Exec("DROP TRIGGER fail_hold_resolution"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveReactionMessageHold(ctx, 1, id, "review"); err != nil {
		t.Fatal(err)
	}
	var state, response string
	if err = s.DB.QueryRow("SELECT state,response FROM actions WHERE id=?", action).Scan(&state, &response); err != nil {
		t.Fatal(err)
	}
	if state != "unknown" || response != `{"fixture":"unconfirmed"}` || len(f.sent) != 0 {
		t.Fatal("resolution changed unknown action or sent HTTP")
	}
	if _, err = s.ClaimOwnerAction(ctx, 1, 7, f.now); err == nil {
		t.Fatal("resolution made unknown replayable")
	}
}

func TestReactionHoldResolutionBackupFingerprintAndExactSnapshot(t *testing.T) {
	f, id := holdResolutionFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	before, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(before)
	if _, err = s.ResolveReactionMessageHold(ctx, 1, id, "backup audit"); err != nil {
		t.Fatal(err)
	}
	after, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("resolution only change absent from backup fingerprint")
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.sqlite")
	if err = s.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite", "file:"+snapshot+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var original, copied []byte
	var originalSHA, copiedSHA string
	if err = s.DB.QueryRow("SELECT evidence_json,evidence_sha FROM reaction_message_hold_resolutions WHERE draft_id=?", id).Scan(&original, &originalSHA); err != nil {
		t.Fatal(err)
	}
	if err = restored.QueryRow("SELECT evidence_json,evidence_sha FROM reaction_message_hold_resolutions WHERE draft_id=?", id).Scan(&copied, &copiedSHA); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, copied) || originalSHA != copiedSHA {
		t.Fatal("snapshot changed exact resolution evidence")
	}
}
