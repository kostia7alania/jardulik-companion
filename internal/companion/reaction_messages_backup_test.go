package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReactionMessageBackupPolicyOnlyEditIsDirty(t *testing.T) {
	ctx := context.Background()
	cfg, store := archiveTestStore(t)
	if _, err := store.SetReactionMessagePolicy(ctx, 123, "off", []string{"original template"}); err != nil {
		t.Fatal(err)
	}
	archiver := NewArchiver(cfg, store, nil, nil)
	first, err := archiver.BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	fullTestReceipt(t, archiver, first)
	unchanged, err := archiver.BuildFullBackup(ctx, true)
	if err != nil || !unchanged.Unchanged {
		t.Fatalf("confirmed baseline must be clean: %+v %v", unchanged, err)
	}
	before := fullTestFingerprint(t, store)
	want := []string{"  Новый текст 😀 @fixture\n", "second template", "third template"}
	policy, err := store.SetReactionMessagePolicy(ctx, 123, "off", want)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Mode != "off" || !reflect.DeepEqual(policy.Templates, want) {
		t.Fatal("saving inactive templates changed their text or enabled sending")
	}
	if before == fullTestFingerprint(t, store) {
		t.Fatal("editing only inactive templates did not dirty the logical fingerprint")
	}
	changed, err := archiver.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Unchanged || changed.SourceFingerprint == first.SourceFingerprint {
		t.Fatal("if-changed skipped a policy-only edit")
	}
	var actions int
	if err = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatalf("saving policy unexpectedly created actions: %d %v", actions, err)
	}
}

func TestReactionMessageBackupIntentOnlyChangeIsDirty(t *testing.T) {
	ctx := context.Background()
	_, store := archiveTestStore(t)
	store.cfg.AllowReactions = true
	session := Session{UserID: 500, Token: "fixture-access", Refresh: "fixture-refresh", Generation: "backup-generation", Expires: time.Now().Add(time.Hour).Unix()}
	if err := store.SaveSession(ctx, 123, session); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Archive(ctx, 123, []Profile{{"userId": 99, "age": 23}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetReactionMessagePolicy(ctx, 123, "first", []string{"exact text 😀"}); err != nil {
		t.Fatal(err)
	}
	action, err := store.ClaimOwnerAction(ctx, 123, 99, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Profile(ctx, 123, 99)
	if err != nil {
		t.Fatal(err)
	}
	beforePolicy, err := store.ReactionMessagePolicy(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	before := fullTestFingerprint(t, store)
	if _, err = store.prepareLikeMessage(ctx, 123, action, target, session, nil); err != nil {
		t.Fatal(err)
	}
	afterPolicy, err := store.ReactionMessagePolicy(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforePolicy, afterPolicy) {
		t.Fatal("first mode must not advance or rewrite its policy")
	}
	var state string
	if err = store.DB.QueryRowContext(ctx, "SELECT state FROM actions WHERE id=?", action).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("intent preparation changed the reaction outcome: %q %v", state, err)
	}
	if before == fullTestFingerprint(t, store) {
		t.Fatal("new durable intent was absent from the logical fingerprint")
	}
}

func TestReactionMessageBackupRoundTripPreservesExactTemplatesCursorAndIntent(t *testing.T) {
	ctx := context.Background()
	cfg, store := archiveTestStore(t)
	store.cfg.AllowReactions = true
	cfg.AllowReactions = true
	session := Session{UserID: 500, Token: "fixture-access", Refresh: "fixture-refresh", Generation: "backup-generation", Expires: time.Now().Add(time.Hour).Unix()}
	if err := store.SaveSession(ctx, 123, session); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Archive(ctx, 123, []Profile{{"userId": 99, "age": 23, "name": "Fixture"}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	templates := []string{strings.Repeat("😀", 250), "  Привет @fixture\n", "<third>&\""}
	if _, err := store.SetReactionMessagePolicy(ctx, 123, "rotate", templates); err != nil {
		t.Fatal(err)
	}
	action, err := store.ClaimOwnerAction(ctx, 123, 99, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Profile(ctx, 123, 99)
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.prepareLikeMessage(ctx, 123, action, target, session, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.finishReactionAction(ctx, action, "unknown", []byte(`{"fixture":"unconfirmed"}`), time.Time{}); err != nil {
		t.Fatal(err)
	}
	wantPolicy, err := store.ReactionMessagePolicy(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if wantPolicy.NextCursor != 1 || !reflect.DeepEqual(wantPolicy.Templates, templates) {
		t.Fatal("fixture lost exact templates or did not advance once")
	}
	var templatesJSON string
	if err = store.DB.QueryRowContext(ctx, "SELECT templates_json FROM reaction_message_settings WHERE owner_id=123").Scan(&templatesJSON); err != nil {
		t.Fatal(err)
	}
	check := func(db *sql.DB) {
		t.Helper()
		var gotJSON, message, state string
		var cursor, revision int64
		if e := db.QueryRowContext(ctx, "SELECT templates_json,next_cursor,revision FROM reaction_message_settings WHERE owner_id=123").Scan(&gotJSON, &cursor, &revision); e != nil {
			t.Fatal(e)
		}
		if gotJSON != templatesJSON || cursor != wantPolicy.NextCursor || revision != wantPolicy.Revision {
			t.Fatal("backup changed whole template JSON or rotation metadata")
		}
		var gotTemplates []string
		if e := json.Unmarshal([]byte(gotJSON), &gotTemplates); e != nil || !reflect.DeepEqual(gotTemplates, templates) {
			t.Fatal("backup truncated or normalized a template")
		}
		var gotRequest []byte
		var from, targetID, owner, slot, policyRevision int64
		var generation string
		if e := db.QueryRowContext(ctx, `SELECT i.owner_id,i.upstream_id,i.profile_id,i.generation,i.message,i.request_body,i.template_index,i.policy_revision,a.state FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.action_id=?`, action).Scan(&owner, &from, &targetID, &generation, &message, &gotRequest, &slot, &policyRevision, &state); e != nil {
			t.Fatal(e)
		}
		if owner != 123 || from != 500 || targetID != 99 || generation != session.Generation || slot != 0 || policyRevision != wantPolicy.Revision || state != "unknown" || message != templates[0] || !bytes.Equal(gotRequest, request) {
			t.Fatal("backup changed the exact bound intent or its unknown outcome")
		}
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.sqlite")
	if err = store.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	snap, err := openFullSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	check(snap)
	if err = snap.Close(); err != nil {
		t.Fatal(err)
	}
	artifact, err := NewArchiver(cfg, store, nil, nil).BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err = RestoreArchive(artifact.Path, dest, cfg.BackupKey); err != nil {
		t.Fatal(err)
	}
	restoredCfg := cfg
	restoredCfg.DataDir = dest
	restored, err := OpenStore(restoredCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	check(restored.DB)
	gotPolicy, err := restored.ReactionMessagePolicy(ctx, 123)
	if err != nil || !reflect.DeepEqual(gotPolicy, wantPolicy) {
		t.Fatalf("restored policy differs: %+v %v", gotPolicy, err)
	}
	_, err = restored.ClaimOwnerAction(ctx, 123, 99, time.Now())
	var blocked *ActionBlockedError
	if !errors.As(err, &blocked) || blocked.State != "unknown" {
		t.Fatalf("restored unknown action became replayable: %v", err)
	}
}
