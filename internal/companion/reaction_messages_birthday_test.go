package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestReactionMessageBirthdayPolicyOptInAndUsernameValidation(t *testing.T) {
	f := newAutoFixture(t)
	store := f.worker.Service.Store
	ctx := context.Background()
	before, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode != "off" || before.TelegramUsername != "" || len(before.Templates) != 0 || before.NextCursor != 0 {
		t.Fatalf("new owner was not off by default: %+v", before)
	}
	for _, username := range []string{"", "abcd", "@fixture_user", "fixture-user", "fixture user", "юзернейм", "fixture_user\n", "abcdefghijklmnopqrstuvwxyz1234567"} {
		if _, err = store.SetReactionMessagePolicy(ctx, 1, "birthday", nil, username); err == nil {
			t.Fatalf("invalid Telegram username accepted: %q", username)
		}
		after, e := store.ReactionMessagePolicy(ctx, 1)
		if e != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("invalid birthday configuration changed policy: %+v %v", after, e)
		}
	}
	valid, err := store.SetReactionMessagePolicy(ctx, 1, "birthday", nil, "fixture_user")
	if err != nil {
		t.Fatal(err)
	}
	if valid.Mode != "birthday" || valid.TelegramUsername != "fixture_user" || len(valid.Templates) != 0 || valid.NextCursor != 0 {
		t.Fatalf("birthday policy did not save its handle independently from templates: %+v", valid)
	}
	other, err := store.ReactionMessagePolicy(ctx, 2)
	if err != nil || other.Mode != "off" || other.TelegramUsername != "" || len(other.Templates) != 0 {
		t.Fatalf("birthday opt-in leaked to another owner: %+v %v", other, err)
	}
	if len(f.requests) != 0 || len(f.sent) != 0 {
		t.Fatal("configuring birthday messages performed HTTP or sent a reaction")
	}
}

func TestReactionMessageBirthdayUsesCurrentRawAndRetainsUnknownIntent(t *testing.T) {
	f := newAutoFixture(t)
	store := f.worker.Service.Store
	ctx := context.Background()
	const username = "fixture_user"
	oldProfile := Profile{"userId": int64(81), "age": int64(23), "height": int64(170), "name": "Old fixture", "birthday": "2003-01-01T00:00:00"}
	if _, err := store.ArchiveViewed(ctx, 1, []Profile{oldProfile}, "birthday-old-fixture"); err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := store.Profile(ctx, 1, 81)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetReactionMessagePolicy(ctx, 1, "birthday", nil, username); err != nil {
		t.Fatal(err)
	}
	action, err := store.ClaimOwnerAction(ctx, 1, 81, f.now)
	if err != nil {
		t.Fatal(err)
	}
	newProfile := Profile{"userId": int64(81), "age": int64(23), "height": int64(170), "name": "New fixture", "birthday": "2003-02-20T00:00:00"}
	if _, err = store.ArchiveViewed(ctx, 1, []Profile{newProfile}, "birthday-new-fixture"); err != nil {
		t.Fatal(err)
	}
	current, err := store.Profile(ctx, 1, 81)
	if err != nil {
		t.Fatal(err)
	}
	if current.VersionID == oldSnapshot.VersionID {
		t.Fatal("fixture did not replace the current raw profile")
	}
	wantMessage, wantIndex, err := birthdayReactionMessage(current.Profile, username, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldMessage, oldIndex, err := birthdayReactionMessage(oldSnapshot.Profile, username, 0)
	if err != nil {
		t.Fatal(err)
	}
	if wantMessage == oldMessage && wantIndex == oldIndex {
		t.Fatal("fixture must distinguish the old and current birthday")
	}
	session, err := store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.prepareLikeMessage(ctx, 1, action, oldSnapshot, session, nil)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err = json.Unmarshal(request, &body); err != nil {
		t.Fatal(err)
	}
	if body["reactionType"] != float64(1) || body["isDeleted"] != false || body["userId"] != float64(81) || body["fromUserId"] != float64(101) || body["message"] != wantMessage {
		t.Fatal("prepared Like used stale input, changed reaction identity or lost the generated text")
	}
	policy, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 1 {
		t.Fatalf("one birthday intent must advance the cursor exactly once: %+v %v", policy, err)
	}
	var savedMessage string
	var savedRequest []byte
	var savedIndex, savedRevision int64
	if err = store.DB.QueryRowContext(ctx, "SELECT message,request_body,template_index,policy_revision FROM reaction_message_intents WHERE action_id=?", action).Scan(&savedMessage, &savedRequest, &savedIndex, &savedRevision); err != nil {
		t.Fatal(err)
	}
	if savedMessage != wantMessage || savedIndex != wantIndex || savedRevision != policy.Revision || !bytes.Equal(savedRequest, request) {
		t.Fatal("intent and cursor did not preserve the exact current-profile choice together")
	}
	if err = store.finishReactionAction(ctx, action, "unknown", []byte(`{"fixture":"unconfirmed"}`), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.prepareLikeMessage(ctx, 1, action, current, session, nil); !errors.Is(err, ErrActionBlocked) {
		t.Fatalf("unknown birthday intent became replayable: %v", err)
	}
	_, err = store.ClaimOwnerAction(ctx, 1, 81, f.now)
	var blocked *ActionBlockedError
	if !errors.As(err, &blocked) || blocked.State != "unknown" {
		t.Fatalf("new claim bypassed the prior unknown result: %v", err)
	}
	after, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil || after.NextCursor != 1 {
		t.Fatalf("blocked replay advanced the birthday cursor: %+v %v", after, err)
	}
	if err = store.DB.QueryRowContext(ctx, "SELECT request_body FROM reaction_message_intents WHERE action_id=?", action).Scan(&savedRequest); err != nil || !bytes.Equal(savedRequest, request) {
		t.Fatal("blocked replay changed the original outgoing intent")
	}
	if len(f.requests) != 0 || len(f.sent) != 0 {
		t.Fatal("preparing or inspecting the intent performed HTTP")
	}
}

func TestReactionMessageBirthdayRejectsCurrentUnderageRaw(t *testing.T) {
	f := newAutoFixture(t)
	store := f.worker.Service.Store
	ctx := context.Background()
	f.add(t, 1, 81, 23, false)
	oldSnapshot, err := store.Profile(ctx, 1, 81)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetReactionMessagePolicy(ctx, 1, "birthday", nil, "fixture_user"); err != nil {
		t.Fatal(err)
	}
	action, err := store.ClaimOwnerAction(ctx, 1, 81, f.now)
	if err != nil {
		t.Fatal(err)
	}
	younger := Profile{"userId": int64(81), "age": int64(17), "height": int64(170), "birthday": "2009-02-20T00:00:00"}
	if _, err = store.ArchiveViewed(ctx, 1, []Profile{younger}, "birthday-current-age-fixture"); err != nil {
		t.Fatal(err)
	}
	session, err := store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.prepareLikeMessage(ctx, 1, action, oldSnapshot, session, nil); !errors.Is(err, ErrReactionMessageTargetAge) {
		t.Fatalf("stale adult snapshot bypassed current age guard: %v", err)
	}
	policy, err := store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 0 {
		t.Fatalf("rejected current age advanced the birthday cursor: %+v %v", policy, err)
	}
	var intents int
	if err = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM reaction_message_intents WHERE action_id=?", action).Scan(&intents); err != nil || intents != 0 {
		t.Fatalf("rejected age created an outgoing intent: %d %v", intents, err)
	}
	if len(f.requests) != 0 || len(f.sent) != 0 {
		t.Fatal("current-age rejection performed HTTP or sent a reaction")
	}
}
