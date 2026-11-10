package companion

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func profileIndexRenderedRows(t *testing.T, pages []profileIndexPayload, chat int64) []string {
	t.Helper()
	var result []string
	for _, page := range pages {
		if validProfileIndexPayload(page) != nil {
			t.Fatal("index page exceeds Telegram UTF-16/entity bounds")
		}
		quotes := 0
		for _, entity := range page.Entities {
			if entity.Type == "expandable_blockquote" {
				quotes++
				continue
			}
			before, rest := telegramPrefix(page.Text, entity.Offset)
			label, after := telegramPrefix(rest, entity.Length)
			tail := strings.SplitN(after, "\n", 2)[0]
			if entity.Type != "text_link" || !strings.HasSuffix(before, "\n") || strings.Contains(label, "\n") || (tail != " ⭐️" && tail != " ⚪️" && tail != " ❔") {
				t.Fatalf("link does not cover the label with status outside: %+v label=%q tail=%q", entity, label, tail)
			}
			if !strings.HasPrefix(entity.URL, strings.TrimSuffix(forumTopicURL(chat, 1), "1")) {
				t.Fatalf("unexpected index target: %s", entity.URL)
			}
			result = append(result, label)
		}
		if quotes != 1 {
			t.Fatal("index rows must have one expandable quote")
		}
	}
	return result
}

func TestProfileIndexCompatibilitySortKeepsKnownZeroBeforeUnknown(t *testing.T) {
	entries := []profileIndexEntry{
		{ID: 1, Name: "Zero older", Age: 40, AgeKnown: true, Percent: 0, PercentKnown: true},
		{ID: 2, Name: "Unknown younger", Age: 18, AgeKnown: true},
		{ID: 3, Name: "Older", Age: 25, AgeKnown: true, Percent: 90, PercentKnown: true},
		{ID: 4, Name: "Zara", Age: 22, AgeKnown: true, Percent: 90, PercentKnown: true},
		{ID: 5, Name: "Zero younger", Age: 20, AgeKnown: true, Percent: 0, PercentKnown: true},
		{ID: 6, Name: "Anna", Age: 22, AgeKnown: true, Percent: 90, PercentKnown: true},
		{ID: 7, Name: "anna", Age: 22, AgeKnown: true, Percent: 90, PercentKnown: true},
	}
	original := append([]profileIndexEntry(nil), entries...)
	ordered := sortProfileIndex(profileIndexCompatibility, entries)
	var ids []int64
	for _, row := range ordered {
		ids = append(ids, row.ID)
	}
	if !reflect.DeepEqual(ids, []int64{6, 7, 4, 3, 5, 1, 2}) {
		t.Fatalf("score/age/name/id order: %v", ids)
	}
	if !reflect.DeepEqual(entries, original) {
		t.Fatal("sort changed the input snapshot")
	}
}

func TestProfileIndexAgeSortUsesExactDayAndStableFallbacks(t *testing.T) {
	entries := []profileIndexEntry{
		{ID: 1, Name: "June", Age: 22, AgeKnown: true, Birthday: "2004-06-01"},
		{ID: 2, Name: "Zara", Age: 22, AgeKnown: true, Birthday: "2004-12-31"},
		{ID: 3, Name: "Missing birthday", Age: 22, AgeKnown: true},
		{ID: 4, Name: "Younger", Age: 21, AgeKnown: true, Birthday: "2005-02-01"},
		{ID: 5, Name: "Unknown age", Birthday: "2020-01-01"},
		{ID: 6, Name: "Anna", Age: 22, AgeKnown: true, Birthday: "2004-12-31"},
	}
	ordered := sortProfileIndex(profileIndexAge, entries)
	var ids []int64
	for _, row := range ordered {
		ids = append(ids, row.ID)
	}
	if !reflect.DeepEqual(ids, []int64{4, 6, 2, 1, 3, 5}) {
		t.Fatalf("age/birthday/name/id order: %v", ids)
	}
}

func TestProfileIndexCompactNamesZodiacAndSourceStayExact(t *testing.T) {
	profile := Profile{"name": "  Lika\n🙂  ", "surName": "  Bazrova  ", "surname": "Ignored", "familyNames": "Not a surname", "age": 22, "height": 169, "birthday": "2004-07-08T00:00:00"}
	before, _ := json.Marshal(profile)
	name := profileIndexName(profile, 17)
	if name != "Lika 🙂 Bazrova" || profileIndexName(Profile{"name": "Anna", "surName": "  ", "surname": "Ivanova"}, 18) != "Anna Ivanova" {
		t.Fatal("name and actual surname are not normalized correctly")
	}
	if got := profileIndexName(Profile{"familyNames": "Only relatives"}, 18); got != "#18" {
		t.Fatalf("missing name should use ID only: %q", got)
	}
	long := profileIndexName(Profile{"name": strings.Repeat("🙂", 100)}, 19)
	if textUnits(long) > 80 || !utf8.ValidString(long) {
		t.Fatal("long name broke UTF-16 budget or UTF-8")
	}
	now := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	if got := profileIndexZodiac(profile, now); got != "♋️" {
		t.Fatalf("unambiguous calendar sign: %q", got)
	}
	for _, birthday := range []any{nil, "2099-07-08", "2004-02-30", ""} {
		if got := profileIndexZodiac(Profile{"birthday": birthday}, now); got != "❔" {
			t.Fatalf("unknown birthday %v gained a sign: %q", birthday, got)
		}
	}
	row := profileIndexRow(1, profileIndexEntry{ID: 17, Name: name, Age: 22, AgeKnown: true, Percent: 75, PercentKnown: true, Zodiac: "♋️"})
	if row != "1 ♋️ 22, Lika 🙂 Bazrova, 75%" || strings.Contains(row, "169") || strings.Contains(row, "#17") {
		t.Fatalf("non-compact profile row: %q", row)
	}
	if got := profileIndexRow(2, profileIndexEntry{ID: 18, Name: "Anna"}); got != "2 ❔ ?, Anna, —" {
		t.Fatalf("missing fields became invented values: %q", got)
	}
	after, _ := json.Marshal(profile)
	if string(before) != string(after) {
		t.Fatal("name/sign presentation changed raw source fields")
	}
}

func TestProfileIndexPagesLinkWholeUTF16RowsAndKeepGlobalNumbers(t *testing.T) {
	const chat = int64(-1004401653948)
	var entries []profileIndexEntry
	for i := 0; i < 51; i++ {
		entries = append(entries, profileIndexEntry{ID: int64(i + 1), ThreadID: int64(100 + i), CardID: int64(1000 + i), Name: fmt.Sprintf("Name%03d🙂", i), Age: 23, AgeKnown: true, Percent: 75, PercentKnown: true})
	}
	original := append([]profileIndexEntry(nil), entries...)
	pages, err := renderProfileIndexPages(profileIndexAge, entries, chat)
	if err != nil || len(pages) != 1 || len(pages[0].Entities) != 52 || pages[0].Rich == nil {
		t.Fatalf("unexpected arbitrary row pagination: pages=%d err=%v", len(pages), err)
	}
	rows := profileIndexRenderedRows(t, pages, chat)
	if len(rows) != 51 || rows[0] != "1 ❔ 23, Name000🙂, 75%" || rows[50] != "51 ❔ 23, Name050🙂, 75%" {
		t.Fatalf("global numbering or compact labels changed: %v", rows)
	}
	if pages[0].Entities[0].URL != profileIndexCardURL(chat, 100, 1000) || pages[0].Entities[50].URL != profileIndexCardURL(chat, 150, 1050) {
		t.Fatal("row points to an unconfirmed or unrelated card ID")
	}
	if !reflect.DeepEqual(entries, original) {
		t.Fatal("renderer changed its source entries")
	}
	for i := range entries {
		entries[i].Name = strings.Repeat("🙂", 40) + fmt.Sprintf("%03d", i)
	}
	longPages, err := renderProfileIndexPagesMode(profileIndexCompatibility, entries, chat, false)
	if err != nil || len(longPages) < 2 || len(longPages[0].Entities) >= 50 || longPages[0].Rich != nil {
		t.Fatalf("plain UTF-16 budget did not split: %v", err)
	}
	longRows := profileIndexRenderedRows(t, longPages, chat)
	if len(longRows) != len(entries) || !strings.HasPrefix(longRows[len(longRows)-1], "51 ") {
		t.Fatal("UTF-16 pagination lost entries or numbering")
	}
}

func TestProfileIndexEmptyAndInvalidDestinations(t *testing.T) {
	pages, err := renderProfileIndexPages(profileIndexAge, nil, -1004401653948)
	if err != nil || len(pages) != 1 || len(pages[0].Entities) != 1 || pages[0].Text != "🎂 Возраст\nАнкет пока нет." || pages[0].Rich == nil {
		t.Fatalf("empty index: %+v %v", pages, err)
	}
	for _, tc := range []struct {
		kind  string
		chat  int64
		entry profileIndexEntry
	}{
		{"unknown", -1004401653948, profileIndexEntry{ID: 1, ThreadID: 2, CardID: 3}},
		{profileIndexAge, 123, profileIndexEntry{ID: 1, ThreadID: 2, CardID: 3}},
		{profileIndexAge, -1004401653948, profileIndexEntry{ID: 1, ThreadID: 2}},
	} {
		if _, err := renderProfileIndexPages(tc.kind, []profileIndexEntry{tc.entry}, tc.chat); err == nil {
			t.Fatalf("invalid destination accepted: %+v", tc)
		}
	}
}

func TestProfileIndexRichBudgetKeeps834CompactRowsInOneFoldedView(t *testing.T) {
	const chat int64 = -1004401653948
	entries := make([]profileIndexEntry, 834)
	for i := range entries {
		entries[i] = profileIndexEntry{ID: int64(i + 1), ThreadID: int64(i + 100), CardID: int64(i + 1000), Name: "Сабина", Age: 19, AgeKnown: true, Percent: 70, PercentKnown: true, Zodiac: "♑️", PremiumKnown: true, Premium: true}
	}
	pages, err := renderProfileIndexPages(profileIndexCompatibility, entries, chat)
	if err != nil || len(pages) != 1 {
		t.Fatalf("compact catalogue unnecessarily split: pages=%d err=%v", len(pages), err)
	}
	rows := profileIndexRenderedRows(t, pages, chat)
	if len(rows) != len(entries) || !strings.HasPrefix(rows[833], "834 ") || textUnits(pages[0].Text) <= 4096 {
		t.Fatal("rich catalogue lost rows or still used the classic limit")
	}
	blocks := pages[0].Rich["blocks"].([]any)
	if len(blocks) != 2 || blocks[1].(map[string]any)["type"] != "expandable_blockquote" {
		t.Fatal("rich catalogue is not one folded block")
	}
	for i := range entries {
		entries[i].Name = strings.Repeat("🙂", 40)
	}
	unicodePages, err := renderProfileIndexPages(profileIndexAge, entries[:400], chat)
	if err != nil || len(unicodePages) != 1 || textUnits(unicodePages[0].Text) <= profileIndexRichLimit {
		t.Fatal("rich budget incorrectly counted emoji as two characters", err)
	}
	pages, err = renderProfileIndexPages(profileIndexAge, entries, chat)
	if err != nil || len(pages) < 2 || len(profileIndexRenderedRows(t, pages, chat)) != len(entries) {
		t.Fatal("actual rich text budget did not paginate all rows", err)
	}
}

func TestProfileIndexZodiacCalendarBoundariesDoNotUseGreetingGuard(t *testing.T) {
	now := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	for _, boundary := range []struct{ date, before, after string }{
		{"2000-01-20", "♑️", "♒️"}, {"2000-02-19", "♒️", "♓️"},
		{"2000-03-21", "♓️", "♈️"}, {"2000-04-20", "♈️", "♉️"},
		{"2000-05-21", "♉️", "♊️"}, {"2000-06-21", "♊️", "♋️"},
		{"2000-07-23", "♋️", "♌️"}, {"2000-08-23", "♌️", "♍️"},
		{"2000-09-23", "♍️", "♎️"}, {"2000-10-23", "♎️", "♏️"},
		{"2000-11-22", "♏️", "♐️"}, {"2000-12-22", "♐️", "♑️"},
	} {
		date, err := time.Parse("2006-01-02", boundary.date)
		if err != nil {
			t.Fatal(err)
		}
		for offset := -2; offset <= 2; offset++ {
			p := Profile{"birthday": date.AddDate(0, 0, offset).Format("2006-01-02")}
			want := boundary.after
			if offset < 0 {
				want = boundary.before
			}
			if got := profileIndexZodiac(p, now); got != want {
				t.Fatalf("boundary=%s offset=%d got=%s want=%s", boundary.date, offset, got, want)
			}
			if birthdayGreetingSign(p, now) != "neutral" {
				t.Fatal("index presentation changed the existing greeting guard")
			}
		}
	}
	if got := profileIndexZodiac(Profile{"birthday": "2000-02-29T23:30:00-12:00"}, now); got != "♓️" {
		t.Fatalf("leap day or source calendar timezone changed: %s", got)
	}
}

func TestProfileIndexPremiumStrictSourceAndOutsideLink(t *testing.T) {
	for _, tc := range []struct {
		value          any
		premium, known bool
		icon           string
	}{
		{1, true, true, "⭐️"}, {int64(0), false, true, "⚪️"},
		{json.Number("1"), true, true, "⭐️"}, {float64(0), false, true, "⚪️"},
		{nil, false, false, "❔"}, {2, false, false, "❔"}, {"1", false, false, "❔"},
		{true, false, false, "❔"}, {0.5, false, false, "❔"},
	} {
		p := Profile{"type": tc.value, "is_premium": true}
		premium, known := profileIndexPremium(p)
		if premium != tc.premium || known != tc.known {
			t.Fatalf("type=%v: premium=%v known=%v", tc.value, premium, known)
		}
		e := profileIndexEntry{ID: 1, ThreadID: 2, CardID: 3, Name: "Сабина🙂", Age: 19, AgeKnown: true, Percent: 70, PercentKnown: true, Zodiac: "♑️", Premium: premium, PremiumKnown: known}
		if got := profileIndexRow(37, e); got != "37 ♑️ 19, Сабина🙂, 70%" {
			t.Fatalf("wrong horizontal label: %q", got)
		}
		for _, kind := range []string{profileIndexAge, profileIndexCompatibility} {
			pages, err := renderProfileIndexPages(kind, []profileIndexEntry{e}, -1004401653948)
			if err != nil || len(pages) != 1 || len(pages[0].Entities) != 2 {
				t.Fatalf("bad status page: %+v %v", pages, err)
			}
			entity := pages[0].Entities[0]
			_, rest := telegramPrefix(pages[0].Text, entity.Offset)
			label, tail := telegramPrefix(rest, entity.Length)
			if label != "1 ♑️ 19, Сабина🙂, 70%" || tail != " "+tc.icon || validMessageEntities(pages[0].Text, pages[0].Entities) != nil {
				t.Fatalf("Premium included in UTF-16 click span: label=%q tail=%q", label, tail)
			}
		}
	}
}
