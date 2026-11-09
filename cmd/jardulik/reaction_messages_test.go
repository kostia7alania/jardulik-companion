package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func TestReactionMessageCLIStoresOffWithoutNetwork(t *testing.T) {
	s, _, calls := contactImportFixture(t)
	templates := []string{"  Привет 😀 @kostia7alania  \n", "second", "third"}
	raw, _ := json.Marshal(map[string]any{"mode": "off", "templates": templates})
	var output bytes.Buffer
	if err := setReactionMessagePolicy(context.Background(), s, 1, bytes.NewReader(raw), &output); err != nil {
		t.Fatal(err)
	}
	var policy companion.ReactionMessagePolicy
	if err := json.Unmarshal(output.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Mode != "off" || policy.NextCursor != 0 || policy.Templates[0] != templates[0] || *calls != 0 {
		t.Fatal("save-only changed content or used network", policy, *calls)
	}
	other, err := s.Store.ReactionMessagePolicy(context.Background(), 2)
	if err != nil || other.Mode != "off" || len(other.Templates) != 0 {
		t.Fatal("another owner received templates", other, err)
	}
}

func TestReactionMessageCLIRejectsMalformedInput(t *testing.T) {
	for _, input := range []string{
		`{ "mode":"off","owner_id":2 }`,
		`{ "mode":"rotate","templates":[] }`,
		`{ "mode":"off" } {}`,
		strings.Repeat(" ", 16*1024+1),
		string([]byte{0xff}),
	} {
		s, _, calls := contactImportFixture(t)
		var output bytes.Buffer
		if err := setReactionMessagePolicy(context.Background(), s, 1, strings.NewReader(input), &output); err == nil {
			t.Fatal("invalid policy accepted")
		}
		if *calls != 0 || output.Len() != 0 {
			t.Fatal("invalid policy had side effects", *calls, output.String())
		}
	}
}
