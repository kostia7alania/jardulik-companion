package companion

import (
	"strings"
	"time"
)

// A birthday is a calendar date: a timestamp suffix never shifts its day.
func profileBirthdayDate(p Profile) (time.Time, bool) {
	raw, ok := p["birthday"].(string)
	if !ok {
		return time.Time{}, false
	}
	raw = strings.TrimSpace(raw)
	if len(raw) < 10 || (len(raw) > 10 && raw[10] != 'T' && raw[10] != ' ') {
		return time.Time{}, false
	}
	if len(raw) > 10 {
		valid := false
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999"} {
			if _, err := time.Parse(layout, raw); err == nil {
				valid = true
				break
			}
		}
		if !valid {
			return time.Time{}, false
		}
	}
	date, err := time.Parse("2006-01-02", raw[:10])
	if err != nil || date.Year() < 2 {
		return time.Time{}, false
	}
	return date, true
}

func profileZodiac(p Profile, now time.Time) string {
	date, known := profileBirthdayDate(p)
	if !known || date.After(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)) {
		return "❔"
	}
	boundaries := [...]struct {
		start int
		icon  string
	}{
		{120, "♒️"}, {219, "♓️"}, {321, "♈️"}, {420, "♉️"},
		{521, "♊️"}, {621, "♋️"}, {723, "♌️"}, {823, "♍️"},
		{923, "♎️"}, {1023, "♏️"}, {1122, "♐️"}, {1222, "♑️"},
	}
	day := int(date.Month())*100 + date.Day()
	icon := "♑️"
	for _, boundary := range boundaries {
		if day >= boundary.start {
			icon = boundary.icon
		}
	}
	return icon
}

func profileBirthday(p Profile) string {
	date, known := profileBirthdayDate(p)
	if !known {
		return ""
	}
	weekdays := [...]string{"вс", "пн", "вт", "ср", "чт", "пт", "сб"}
	return date.Format("02.01.2006") + " (" + weekdays[date.Weekday()] + ", " + profileZodiac(p, time.Now()) + ")"
}
