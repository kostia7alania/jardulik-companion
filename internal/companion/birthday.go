package companion

import (
	"strings"
	"time"
)

// A birthday is a calendar date: a timestamp suffix never shifts its day.
func profileBirthday(p Profile) string {
	raw, ok := p["birthday"].(string)
	if !ok {
		return ""
	}
	raw = strings.TrimSpace(raw)
	if len(raw) < 10 || (len(raw) > 10 && raw[10] != 'T' && raw[10] != ' ') {
		return ""
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
			return ""
		}
	}
	date, err := time.Parse("2006-01-02", raw[:10])
	if err != nil || date.Year() < 2 {
		return ""
	}
	weekdays := [...]string{"вс", "пн", "вт", "ср", "чт", "пт", "сб"}
	return date.Format("02.01.2006") + " (" + weekdays[date.Weekday()] + ")"
}
