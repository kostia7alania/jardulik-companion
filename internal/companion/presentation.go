package companion

import (
	"strings"
	"time"
)

func displayTime(raw string) string {
	var value time.Time
	var err error
	for _, format := range []string{time.RFC3339Nano, "2006-01-02 15:04:05"} {
		value, err = time.Parse(format, raw)
		if err == nil {
			break
		}
	}
	if err != nil {
		return raw
	}
	value = value.In(time.FixedZone("UTC+3", 3*60*60))
	weekdays := [...]string{"вс", "пн", "вт", "ср", "чт", "пт", "сб"}
	return value.Format("2.01.06, 15:04") + " (" + weekdays[value.Weekday()] + ", UTC+3)"
}

// Telegram entity offsets count UTF-16 units. Keeping this stricter budget also
// handles captions containing flags and other supplementary Unicode symbols.
func telegramPrefix(s string, budget int) (string, string) {
	units := 0
	for i, r := range s {
		n := 1
		if r > 0xffff {
			n = 2
		}
		if units+n > budget {
			return s[:i], s[i:]
		}
		units += n
	}
	return s, ""
}

func profileCaption(text string) string {
	if prefix, rest := telegramPrefix(text, 1024); rest == "" {
		return prefix
	}
	const tail = "\n\nПолный текст по кнопке."
	prefix, _ := telegramPrefix(text, 1024-len([]rune(tail)))
	return strings.TrimRight(prefix, " \n") + tail
}

func profileValue(p Profile, key string) string {
	raw := p.String(key)
	if raw == "" || raw == "null" {
		return ""
	}
	if key == "languages" {
		var names []string
		for _, value := range p.Photos(key) {
			if label := profileLabels[value]; label != "" {
				value = label
			}
			names = append(names, value)
		}
		return strings.Join(names, ", ")
	}
	lookup := raw
	if key == "goal" || key == "worldview" || key == "children" {
		lookup = key + "_" + raw
	}
	if label := profileLabels[lookup]; label != "" {
		return label
	}
	if raw == "dont_tell" {
		return "Не указано"
	}
	if key == "country" {
		if label := countryLabels[raw]; label != "" {
			return label
		}
	}
	if key == "city" {
		if label := map[string]string{"Vladikavkaz": "Владикавказ", "Moscow": "Москва", "Saint Petersburg": "Санкт-Петербург", "Tskhinvali": "Цхинвал", "Tbilisi": "Тбилиси", "Yerevan": "Ереван", "Sochi": "Сочи", "Krasnodar": "Краснодар"}[raw]; label != "" {
			return label
		}
	}
	return raw
}
