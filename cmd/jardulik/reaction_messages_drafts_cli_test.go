package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func greetingDraftCLIFixture(t *testing.T) (*companion.Service, map[string]any, *int) {
	t.Helper()
	service, _, calls := contactImportFixture(t)
	ctx := context.Background()
	if err := service.Store.SaveAccount(ctx, 1, companion.Profile{"userId": int64(101), "age": int64(33), "type": int64(1), "freezeState": false, "minAge": int64(18), "maxAge": int64(25)}); err != nil {
		t.Fatal(err)
	}
	basis := map[string]any{"name": "Fixture", "age": int64(23), "height": int64(170), "description": "", "profession": "", "city": "", "country": "", "languages": ""}
	profile := companion.Profile{"userId": int64(77)}
	for key, value := range basis {
		profile[key] = value
	}
	if _, err := service.Store.Archive(ctx, 1, []companion.Profile{profile}, "greeting-cli-fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Store.ConfigureReactionMessagePolicy(ctx, 1, companion.ReactionMessagePolicyConfig{Mode: "off", TelegramUsername: "fixture_user"}); err != nil {
		t.Fatal(err)
	}
	draft := map[string]any{
		"profile_id": int64(77), "basis": basis, "state": "approved",
		"message":    "  Привет! Буду рад знакомству. @fixture_user\n",
		"review":     map[string]any{"approved": true, "notes": "Sparse profile; greeting adds no unsupported claims.", "evidence": []any{}},
		"provenance": map[string]any{"limited": true, "source": "cli-test-fixture"},
	}
	bundle := map[string]any{"schema": "jardulik-reviewed-greetings-v1", "owner_id": int64(1), "upstream_id": int64(101), "telegram_username": "fixture_user", "drafts": []any{draft}}
	return service, bundle, calls
}

func TestReactionMessageDraftsCLIImportIsIdempotentWithoutActivationOrNetwork(t *testing.T) {
	service, bundle, calls := greetingDraftCLIFixture(t)
	ctx := context.Background()
	before, err := service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode != "off" || before.PreferDrafts {
		t.Fatalf("fixture must start with sending disabled: %+v", before)
	}
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var originalID int64
	for pass := 0; pass < 2; pass++ {
		var output bytes.Buffer
		if err = importReactionMessageDrafts(ctx, service, 1, bytes.NewReader(raw), &output); err != nil {
			t.Fatal(err)
		}
		var result companion.ReactionMessageDraftImportResult
		if err = json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Approved != 1 || result.Hold != 0 || pass == 0 && (result.Imported != 1 || result.Unchanged != 0) || pass == 1 && (result.Imported != 0 || result.Unchanged != 1) {
			t.Fatalf("wrong import receipt on pass %d: %+v", pass, result)
		}
		var count int
		var id int64
		var message, state string
		if err = service.Store.DB.QueryRowContext(ctx, "SELECT count(*),min(id),min(message),min(state) FROM reaction_message_drafts WHERE owner_id=1 AND upstream_id=101 AND profile_id=77 AND telegram_username='fixture_user'").Scan(&count, &id, &message, &state); err != nil {
			t.Fatal(err)
		}
		wantMessage := bundle["drafts"].([]any)[0].(map[string]any)["message"].(string)
		if count != 1 || id <= 0 || message != wantMessage || state != "approved" {
			t.Fatalf("import lost the exact final message or account binding: count=%d id=%d state=%s", count, id, state)
		}
		if pass > 0 && id != originalID {
			t.Fatal("idempotent import created a replacement draft")
		}
		originalID = id
	}
	after, err := service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("import activated or rewrote the message policy: %+v %v", after, err)
	}
	var actions int
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatalf("import created a reaction: %d %v", actions, err)
	}
	if *calls != 0 {
		t.Fatalf("import called a provider or Telegram: %d", *calls)
	}
}

func TestReactionMessageDraftsCLIRejectsBindingAndAliasesWithoutOutput(t *testing.T) {
	cases := []struct {
		name     string
		owner    int64
		change   func(map[string]any)
		trailing bool
	}{
		{name: "caller owner", owner: 2},
		{name: "bundle owner", change: func(b map[string]any) { b["owner_id"] = int64(2) }},
		{name: "upstream account", change: func(b map[string]any) { b["upstream_id"] = int64(102) }},
		{name: "owner handle", change: func(b map[string]any) {
			b["telegram_username"] = "other_user"
			b["drafts"].([]any)[0].(map[string]any)["message"] = "Привет! @other_user"
		}},
		{name: "unknown target", change: func(b map[string]any) { b["drafts"].([]any)[0].(map[string]any)["profile_id"] = int64(78) }},
		{name: "top level alias", change: func(b map[string]any) { b["Owner_id"] = int64(1) }},
		{name: "basis alias", change: func(b map[string]any) {
			b["drafts"].([]any)[0].(map[string]any)["basis"].(map[string]any)["Name"] = "Fixture"
		}},
		{name: "extra message alias", change: func(b map[string]any) {
			b["drafts"].([]any)[0].(map[string]any)["message_text"] = "another final message @fixture_user"
		}},
		{name: "missing source field", change: func(b map[string]any) {
			delete(b["drafts"].([]any)[0].(map[string]any)["basis"].(map[string]any), "languages")
		}},
		{name: "trailing object", trailing: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, bundle, calls := greetingDraftCLIFixture(t)
			ctx := context.Background()
			before, err := service.Store.ReactionMessagePolicy(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if tc.change != nil {
				tc.change(bundle)
			}
			raw, err := json.Marshal(bundle)
			if err != nil {
				t.Fatal(err)
			}
			if tc.trailing {
				raw = append(raw, []byte(" {}")...)
			}
			owner := tc.owner
			if owner == 0 {
				owner = 1
			}
			var output bytes.Buffer
			if err = importReactionMessageDrafts(ctx, service, owner, bytes.NewReader(raw), &output); err == nil {
				t.Fatal("invalid greeting bundle was accepted")
			}
			if output.Len() != 0 {
				t.Fatalf("rejected bundle emitted a success receipt: %q", output.String())
			}
			var drafts, actions int
			if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM reaction_message_drafts").Scan(&drafts); err != nil {
				t.Fatal(err)
			}
			if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions").Scan(&actions); err != nil {
				t.Fatal(err)
			}
			after, err := service.Store.ReactionMessagePolicy(ctx, 1)
			if err != nil || !reflect.DeepEqual(before, after) || drafts != 0 || actions != 0 || *calls != 0 {
				t.Fatalf("rejected bundle changed state or performed network: drafts=%d actions=%d calls=%d policy=%+v err=%v", drafts, actions, *calls, after, err)
			}
		})
	}
}
