package companion

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func premiumBadgeProfile(t *testing.T, raw string) StoredProfile {
	t.Helper()
	var profile Profile
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&profile); err != nil {
		t.Fatal(err)
	}
	return StoredProfile{ID: 7, VersionID: 1, Profile: profile, RawJSON: raw}
}

func TestPremiumBadgeUsesOnlyJardulikTypeOne(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		premium     bool
	}{
		{"zero", `,"type":0`, false},
		{"one", `,"type":1`, true},
		{"missing", "", false},
		{"unknown", `,"type":2`, false},
		{"unknown_text", `,"type":"premium"`, false},
		{"boolean", `,"type":true`, false},
		{"null", `,"type":null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"userId":7,"name":"Ира","surName":"Иванова","age":25,"height":170,"city":"Moscow","is_premium":true` + tc.field + `}`
			p := premiumBadgeProfile(t, raw)
			want := "Ира Иванова, 25/170"
			if tc.premium {
				want += " ⭐ Premium"
			}
			text := profileText(p)
			if first, _, _ := strings.Cut(text, "\n"); first != want {
				t.Fatalf("first line %q, want %q", first, want)
			}
			if p.RawJSON != raw {
				t.Fatal("badge changed stored source JSON")
			}
		})
	}
}

func TestPremiumBadgeSharedPrivateArchiveAndRichKeepsExactRaw(t *testing.T) {
	const raw = `{"userId":7,"name":"Ира","age":25,"height":170,"type":1,"large":9007199254740993,"decimal":1.00}`
	p := premiumBadgeProfile(t, raw)
	originalProfile, err := json.Marshal(p.Profile)
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err = json.Indent(&pretty, []byte(raw), "", "  "); err != nil {
		t.Fatal(err)
	}
	const heading = "Ира, 25/170 ⭐ Premium"
	for _, private := range []bool{false, true} {
		card, err := renderProfileCard(p, nil, 0, true, "", 4096, private)
		if err != nil {
			t.Fatal(err)
		}
		if first, _, _ := strings.Cut(card.Text, "\n"); first != heading {
			t.Fatalf("shared card heading %q", first)
		}
		if !card.SourceJSONInline || !strings.HasSuffix(photoQuoteText(t, card), pretty.String()) {
			t.Fatal("badge modified inline source JSON")
		}
	}
	rich, err := BuildRichProfileCard(p, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	blocks := rich.Content["blocks"].([]any)
	paragraph := blocks[0].(map[string]any)["text"].(string)
	if first, _, _ := strings.Cut(paragraph, "\n"); first != heading {
		t.Fatal("rich card bypassed shared premium heading")
	}
	quote := blocks[1].(map[string]any)["text"].([]any)
	code := quote[len(quote)-1].(map[string]any)
	if code["text"] != pretty.String() {
		t.Fatal("rich source JSON changed")
	}
	afterProfile, err := json.Marshal(p.Profile)
	if err != nil {
		t.Fatal(err)
	}
	if p.RawJSON != raw || !bytes.Equal(originalProfile, afterProfile) {
		t.Fatal("premium presentation mutated source data")
	}
}
