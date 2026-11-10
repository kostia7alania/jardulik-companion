package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func greetingHoldCLIFixture(t *testing.T) (*companion.Service, int64, *int) {
	t.Helper()
	service, bundle, calls := greetingDraftCLIFixture(t)
	ctx := context.Background()
	draft := bundle["drafts"].([]any)[0].(map[string]any)
	draft["state"] = "hold"
	draft["message"] = ""
	draft["review"] = map[string]any{"approved": false, "notes": "Дата рождения требует проверки.", "evidence": []any{map[string]any{"field": "age", "quote": "23"}}}
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Store.ImportReactionMessageDrafts(ctx, 1, raw)
	if err != nil || result.Imported != 1 || result.Hold != 1 {
		t.Fatalf("hold fixture import failed: %+v %v", result, err)
	}
	var draftID int64
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT id FROM reaction_message_drafts WHERE owner_id=1 AND profile_id=77 AND state='hold'").Scan(&draftID); err != nil {
		t.Fatal(err)
	}
	current := companion.Profile{"userId": int64(77), "birthday": "2003-03-12T00:00:00"}
	for key, value := range draft["basis"].(map[string]any) {
		current[key] = value
	}
	if _, err = service.Store.Archive(ctx, 1, []companion.Profile{current}, "hold-cli-adult-date-fixture"); err != nil {
		t.Fatal(err)
	}
	return service, draftID, calls
}

func TestReactionMessageHoldCLIResolvesAdultReviewWithoutNetworkOrActivation(t *testing.T) {
	service, draftID, calls := greetingHoldCLIFixture(t)
	ctx := context.Background()
	policyBefore, err := service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var originalCapture []byte
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT capture_json FROM reaction_message_drafts WHERE id=?", draftID).Scan(&originalCapture); err != nil {
		t.Fatal(err)
	}
	const reason = "  Проверена текущая дата рождения: совершеннолетняя.  "
	raw, err := json.Marshal(map[string]any{"draft_id": draftID, "reason": reason})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		var output bytes.Buffer
		if err = resolveReactionMessageHold(ctx, service, 1, bytes.NewReader(raw), &output); err != nil {
			t.Fatal(err)
		}
		var receipt companion.ReactionMessageHoldResolution
		if err = json.Unmarshal(output.Bytes(), &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.DraftID != draftID || receipt.OwnerID != 1 || receipt.UpstreamID != 101 || receipt.ProfileID != 77 || receipt.AlreadyResolved != (pass > 0) {
			t.Fatalf("wrong hold-resolution receipt on pass %d: %+v", pass, receipt)
		}
	}
	var count int
	var evidenceJSON []byte
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT count(*),min(evidence_json) FROM reaction_message_hold_resolutions WHERE draft_id=? AND owner_id=1 AND upstream_id=101 AND profile_id=77", draftID).Scan(&count, &evidenceJSON); err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		Reason string `json:"reason"`
	}
	if err = json.Unmarshal(evidenceJSON, &evidence); err != nil || count != 1 || evidence.Reason != reason {
		t.Fatalf("resolution lost the exact review reason or duplicated its audit: count=%d err=%v", count, err)
	}
	var capture []byte
	var state, message string
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT capture_json,state,message FROM reaction_message_drafts WHERE id=?", draftID).Scan(&capture, &state, &message); err != nil {
		t.Fatal(err)
	}
	if state != "hold" || message != "" || !bytes.Equal(capture, originalCapture) {
		t.Fatal("resolving the hold rewrote its original review capture")
	}
	policyAfter, err := service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || !reflect.DeepEqual(policyBefore, policyAfter) || policyAfter.Mode != "off" || policyAfter.PreferDrafts {
		t.Fatalf("resolution activated or changed message policy: %+v %v", policyAfter, err)
	}
	var actions int
	if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 || *calls != 0 {
		t.Fatalf("resolution sent a reaction or called the network: actions=%d calls=%d err=%v", actions, *calls, err)
	}
}

func TestReactionMessageHoldCLIRejectsInvalidInputWithoutOutputOrNetwork(t *testing.T) {
	cases := []struct {
		name   string
		change func(map[string]any)
		owner  int64
		raw    string
		suffix string
	}{
		{name: "malformed", raw: "{"},
		{name: "unknown field", change: func(p map[string]any) { p["extra"] = true }},
		{name: "profile_id alias", change: func(p map[string]any) { p["profile_id"] = p["draft_id"]; delete(p, "draft_id") }},
		{name: "trailing JSON", suffix: " {}"},
		{name: "zero draft ID", change: func(p map[string]any) { p["draft_id"] = int64(0) }},
		{name: "negative draft ID", change: func(p map[string]any) { p["draft_id"] = int64(-1) }},
		{name: "string draft ID", change: func(p map[string]any) { p["draft_id"] = "77" }},
		{name: "empty reason", change: func(p map[string]any) { p["reason"] = "" }},
		{name: "whitespace reason", change: func(p map[string]any) { p["reason"] = " \n\t" }},
		{name: "reason over limit", change: func(p map[string]any) { p["reason"] = strings.Repeat("a", 2001) }},
		{name: "foreign owner", owner: 2},
		{name: "invalid UTF8", raw: string([]byte{'{', '"', 'r', 'e', 'a', 's', 'o', 'n', '"', ':', '"', 0xff, '"', '}'})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, draftID, calls := greetingHoldCLIFixture(t)
			ctx := context.Background()
			before, err := service.Store.ReactionMessagePolicy(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			input := map[string]any{"draft_id": draftID, "reason": "Проверена текущая дата рождения."}
			if tc.change != nil {
				tc.change(input)
			}
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			raw = append(raw, []byte(tc.suffix)...)
			owner := tc.owner
			if owner == 0 {
				owner = 1
			}
			var output bytes.Buffer
			if err = resolveReactionMessageHold(ctx, service, owner, bytes.NewReader(raw), &output); err == nil {
				t.Fatal("invalid hold-resolution input was accepted")
			}
			var resolutions, actions int
			if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM reaction_message_hold_resolutions").Scan(&resolutions); err != nil {
				t.Fatal(err)
			}
			if err = service.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM actions").Scan(&actions); err != nil {
				t.Fatal(err)
			}
			after, err := service.Store.ReactionMessagePolicy(ctx, 1)
			if err != nil || !reflect.DeepEqual(before, after) || resolutions != 0 || actions != 0 || output.Len() != 0 || *calls != 0 {
				t.Fatalf("rejected input changed state or emitted a receipt: resolutions=%d actions=%d output=%q calls=%d err=%v", resolutions, actions, output.String(), *calls, err)
			}
		})
	}
}
