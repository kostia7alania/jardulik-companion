package companion

import (
	"strings"
	"testing"
)

func TestBirthdayUsesSourceCalendarDayWithoutTimezoneShift(t *testing.T) {
	for _, tc := range []struct {
		raw  any
		want string
	}{
		{"2000-02-29", "29.02.2000 (вт, ♓️)"}, {"2000-02-29T00:30:00+14:00", "29.02.2000 (вт, ♓️)"}, {"2002-07-20T23:30:00-07:00", "20.07.2002 (сб, ♋️)"},
		{nil, ""}, {"null", ""}, {"0001-01-01T00:00:00Z", ""}, {"0000-01-01", ""}, {"2025-02-29", ""}, {"2000-02-29garbage", ""}, {"2000-02-29Tgarbage", ""}, {"2000-02-29T00:00:00", "29.02.2000 (вт, ♓️)"}, {false, ""},
	} {
		if got := profileBirthday(Profile{"birthday": tc.raw}); got != tc.want {
			t.Fatalf("birthday %v: %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestBirthdayAppearsAfterLocation(t *testing.T) {
	text := profileText(StoredProfile{ID: 10, Profile: Profile{"name": "Ира", "city": "Moscow", "birthday": "2000-02-29T00:30:00+14:00", "goal": "family"}})
	location, birthday, goal := strings.Index(text, "📍"), strings.Index(text, "🎂 29.02.2000 (вт, ♓️)"), strings.Index(text, "🎯")
	if location < 0 || birthday <= location || (goal >= 0 && birthday >= goal) {
		t.Fatalf("birthday placement/weekday changed: %s", text)
	}
}
