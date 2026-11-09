package companion

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestBirthdayGreetingCatalogAndExactRotation(t *testing.T) {
	if birthdayGreetingCatalogError != nil {
		t.Fatal(birthdayGreetingCatalogError)
	}
	if len(birthdayGreetingTemplates) != 156 {
		t.Fatalf("catalog has %d entries", len(birthdayGreetingTemplates))
	}
	birthdays := map[string]string{
		"aries": "2000-04-10", "taurus": "2000-05-10", "gemini": "2000-06-10", "cancer": "2000-07-10",
		"leo": "2000-08-10", "virgo": "2000-09-10", "libra": "2000-10-10", "scorpio": "2000-11-10",
		"sagittarius": "2000-12-10", "capricorn": "2000-01-10", "aquarius": "2000-02-10", "pisces": "2000-03-10",
	}
	for signIndex, sign := range birthdayGreetingSigns {
		for variant := int64(0); variant < 24; variant++ {
			profile := Profile{"age": 25, "name": "Ира😀", "birthday": birthdays[sign]}
			message, index, err := birthdayReactionMessage(profile, "different_owner", variant)
			wantIndex := int64(signIndex*12) + variant%12
			if err != nil || index != wantIndex {
				t.Fatalf("%s variant %d: index=%d want=%d err=%v", sign, variant, index, wantIndex, err)
			}
			want := strings.NewReplacer("{name}", "Ира😀", "{telegram_username}", "different_owner").Replace(birthdayGreetingTemplates[index].Text)
			if message != want || strings.ContainsAny(message, "{}") || !strings.Contains(message, "@different_owner") {
				t.Fatalf("%s variant %d did not render exact catalog text: %q", sign, variant, message)
			}
			if err := validateReactionMessage(message); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, index, err := birthdayReactionMessage(Profile{"age": 18}, "valid_user", math.MaxInt64)
	if err != nil || index != 144+math.MaxInt64%12 {
		t.Fatalf("large variant: index=%d err=%v", index, err)
	}
}

func TestBirthdayGreetingCatalogRejectsBrokenContracts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func([]birthdayGreetingTemplate) []birthdayGreetingTemplate
	}{
		{"missing sign theme", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { return v[:len(v)-1] }},
		{"duplicate id", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { v[1].ID = v[0].ID; return v }},
		{"duplicate text", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { v[1].Text = v[0].Text; return v }},
		{"wrong theme", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { v[0].Theme = "moon"; return v }},
		{"wrong sign", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { v[0].Sign = "unknown"; return v }},
		{"unknown placeholder", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate { v[0].Text += " {birthplace}"; return v }},
		{"repeated username", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate {
			v[0].Text += " {telegram_username}"
			return v
		}},
		{"missing username", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate {
			v[0].Text = strings.ReplaceAll(v[0].Text, "@{telegram_username}", "@hardcoded_owner")
			return v
		}},
		{"too long after substitution", func(v []birthdayGreetingTemplate) []birthdayGreetingTemplate {
			v[0].Text += strings.Repeat("😀", 250)
			return v
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := append([]birthdayGreetingTemplate(nil), birthdayGreetingTemplates...)
			raw, err := json.Marshal(tc.change(copy))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = parseBirthdayGreetingCatalog(raw); err == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
	if _, err := parseBirthdayGreetingCatalog([]byte("not JSON")); err == nil {
		t.Fatal("invalid JSON accepted")
	}
}

func TestBirthdayGreetingCalendarAndBoundaryGuard(t *testing.T) {
	asOf := time.Date(2026, time.November, 10, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		birthday any
		want     string
	}{
		{"2000-02-29", "pisces"}, {"2000-12-31", "capricorn"}, {"2000-01-01", "capricorn"},
		{"2000-03-24T00:30:00+14:00", "aries"}, {"2000-03-24 23:59:59", "aries"},
		{"1999-02-29", "neutral"}, {"2000-04-31", "neutral"}, {"2000-03-24Tgarbage", "neutral"},
		{"2000-03-24ignored", "neutral"}, {"0001-03-24", "neutral"}, {"2026-11-11", "neutral"},
		{"2030-01-01", "neutral"}, {"", "neutral"}, {nil, "neutral"}, {20000324, "neutral"},
	} {
		t.Run(fmt.Sprint(tc.birthday), func(t *testing.T) {
			if got := birthdayGreetingSign(Profile{"birthday": tc.birthday}, asOf); got != tc.want {
				t.Fatalf("sign = %q, want %q", got, tc.want)
			}
		})
	}
	boundaries := []struct {
		date, before, after string
	}{
		{"2000-01-20", "capricorn", "aquarius"}, {"2000-02-19", "aquarius", "pisces"},
		{"2000-03-21", "pisces", "aries"}, {"2000-04-20", "aries", "taurus"},
		{"2000-05-21", "taurus", "gemini"}, {"2000-06-21", "gemini", "cancer"},
		{"2000-07-23", "cancer", "leo"}, {"2000-08-23", "leo", "virgo"},
		{"2000-09-23", "virgo", "libra"}, {"2000-10-23", "libra", "scorpio"},
		{"2000-11-22", "scorpio", "sagittarius"}, {"2000-12-22", "sagittarius", "capricorn"},
	}
	for _, boundary := range boundaries {
		date, err := time.Parse("2006-01-02", boundary.date)
		if err != nil {
			t.Fatal(err)
		}
		for offset := -3; offset <= 3; offset++ {
			birthday := date.AddDate(0, 0, offset).Format("2006-01-02")
			want := "neutral"
			if offset == -3 {
				want = boundary.before
			} else if offset == 3 {
				want = boundary.after
			}
			if got := birthdayGreetingSign(Profile{"birthday": birthday}, asOf); got != want {
				t.Fatalf("boundary %s offset %d: sign=%s want=%s", boundary.date, offset, got, want)
			}
		}
	}
}

func TestBirthdayGreetingDOBContradictionGuard(t *testing.T) {
	for _, tc := range []struct {
		birthday, asOf string
		minor          bool
	}{
		{"2008-11-11", "2026-11-10", true}, {"2008-11-10", "2026-11-10", false},
		{"2008-02-29", "2026-02-28", true}, {"2008-02-29", "2026-03-01", false},
		{"2000-01-01", "2026-11-10", false}, {"2030-01-01", "2026-11-10", false},
		{"2008-02-31", "2026-11-10", false}, {"", "2026-11-10", false},
	} {
		asOf, err := time.Parse("2006-01-02", tc.asOf)
		if err != nil {
			t.Fatal(err)
		}
		if got := birthdayGreetingMinorDOB(Profile{"birthday": tc.birthday}, asOf); got != tc.minor {
			t.Fatalf("birthday %s as of %s: minor=%v want=%v", tc.birthday, tc.asOf, got, tc.minor)
		}
	}
	minor := time.Now().UTC().AddDate(-17, 0, 0).Format("2006-01-02")
	message, index, err := birthdayReactionMessage(Profile{"age": 25, "birthday": minor}, "valid_user", 0)
	if !errors.Is(err, ErrReactionMessageTargetAge) || message != "" || index != -1 {
		t.Fatalf("contradicting minor DOB accepted: message=%q index=%d err=%v", message, index, err)
	}
}

func TestBirthdayGreetingAgeAndInputValidation(t *testing.T) {
	for _, age := range []any{nil, 17, 112, -1, 1000, 18.5, "unknown", json.Number("18.5")} {
		message, index, err := birthdayReactionMessage(Profile{"age": age}, "valid_user", 0)
		if !errors.Is(err, ErrReactionMessageTargetAge) || message != "" || index != -1 {
			t.Fatalf("unconfirmed age %#v: message=%q index=%d err=%v", age, message, index, err)
		}
	}
	for _, age := range []any{18, int64(111), "25", float64(25), json.Number("25")} {
		if _, _, err := birthdayReactionMessage(Profile{"age": age}, "valid_user", 0); err != nil {
			t.Fatalf("valid age %#v: %v", age, err)
		}
	}
	for _, handle := range []string{"", "abcd", "@valid_user", "valid user", "юзернейм", strings.Repeat("a", 33), "a\nbcd"} {
		if _, _, err := birthdayReactionMessage(Profile{"age": 18}, handle, 0); err == nil {
			t.Fatalf("invalid handle accepted: %q", handle)
		}
	}
	if _, _, err := birthdayReactionMessage(Profile{"age": 18}, "valid_user", -1); err == nil {
		t.Fatal("negative variant accepted")
	}
}

func TestBirthdayGreetingNameLengthAndFrozenProfile(t *testing.T) {
	for _, tc := range []struct {
		profile Profile
		prefix  string
	}{
		{Profile{"firstName": "  Ира\t😀  ", "name": "Другое имя"}, "Ира 😀, привет! "},
		{Profile{"firstName": " ", "name": "  李\n华  "}, "李 华, привет! "},
		{Profile{"name": "  "}, "Привет! "}, {Profile{"surName": "Иванова"}, "Привет! "},
		{Profile{"name": "null"}, "Привет! "}, {Profile{"name": string([]byte{0xff})}, "Привет! "},
		{Profile{"name": "{Ира}\x00"}, "Ира, привет! "},
		{Profile{"name": strings.Repeat("😀", 80)}, strings.Repeat("😀", 32) + ", привет! "},
	} {
		tc.profile["age"] = 25
		message, index, err := birthdayReactionMessage(tc.profile, "valid_user", 0)
		if err != nil || index != 144 || !strings.HasPrefix(message, tc.prefix) || !utf8.ValidString(message) || strings.ContainsAny(message, "{}") || strings.Count(strings.ToLower(message), "привет") != 1 {
			t.Fatalf("profile=%#v: message=%q index=%d err=%v", tc.profile, message, index, err)
		}
	}
	profile := Profile{
		"age": 25, "name": "Ира", "birthday": "2000-04-10", "description": "музыка и путешествия", "profession": "designer",
		"photos": []any{map[string]any{"gps": "private", "birthplace": "unknown"}}, "humanDesignType": "untrusted", "moon": "untrusted",
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	message, index, err := birthdayReactionMessage(profile, strings.Repeat("a", 32), 3)
	if err != nil || index != 3 {
		t.Fatalf("generation failed: index=%d err=%v", index, err)
	}
	after, err := json.Marshal(profile)
	if err != nil || !reflect.DeepEqual(raw, after) {
		t.Fatal("frozen raw profile was changed")
	}
	minimal, minimalIndex, err := birthdayReactionMessage(Profile{"age": 25, "name": "Ира", "birthday": "2000-04-10"}, strings.Repeat("a", 32), 3)
	if err != nil || minimal != message || minimalIndex != index {
		t.Fatal("unsupported profile facts affected the greeting")
	}
	for _, template := range birthdayGreetingTemplates {
		rendered := renderBirthdayGreeting(template.Text, strings.Repeat("😀", 32), strings.Repeat("a", 32))
		if err := validateReactionMessage(rendered); err != nil || textUnits(rendered) > 500 {
			t.Fatalf("worst-case UTF-16 limit: %s: %v", template.ID, err)
		}
	}
}
