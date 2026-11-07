package companion

import (
	"fmt"
	"strconv"
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

func forumTopicURL(chatID, threadID int64) string {
	chat := strconv.FormatInt(chatID, 10)
	if !strings.HasPrefix(chat, "-100") || threadID <= 0 {
		return ""
	}
	channel := strings.TrimPrefix(chat, "-100")
	if channel == "" || channel == "0" {
		return ""
	}
	return fmt.Sprintf("https://t.me/c/%s/%d", channel, threadID)
}

func profileHeading(v Profile) string {
	name:=strings.Join(strings.Fields(v.String("name")+" "+v.String("surName"))," ")
	age,height:=v.String("age"),v.String("height")
	if age=="0" {age=""};if height=="0" {height=""}
	if age!="" || height!="" {name+=", "+strings.Trim(age+"/"+height,"/")}
	return name
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
	return captionWithTail(text, tail, 1024)
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

func meaningfulProfileValue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "null", "нет", "none", "не указано":
		return false
	}
	return true
}

func albumProfileCaption(text string, id int64) string {
	footer := fmt.Sprintf("\n\nТекст: /text_%d\nИзбранное: /fav_%d · /next", id, id)
	return captionWithTail(text, footer, 1024)
}

func textUnits(text string) int {
	n := 0
	for _, r := range text {
		n++
		if r > 0xffff {
			n++
		}
	}
	return n
}

func captionWithTail(text, tail string, limit int) string {
	if _, rest := telegramPrefix(text+tail, limit); rest == "" {
		return text + tail
	}
	body, footer := text, ""
	if i := strings.LastIndex(text, "\n\n"); i >= 0 && strings.Contains(text[i:], "Jardulik:") {
		body, footer = text[:i], text[i:]
	}
	budget := limit - textUnits(footer+tail) - 2
	if budget < 0 {
		prefix, _ := telegramPrefix(text, limit-textUnits(tail))
		return prefix + tail
	}
	prefix, _ := telegramPrefix(body, budget)
	return strings.TrimRight(prefix, " \n") + "…" + footer + tail
}

func missingProfileFields(p Profile) []string {
	var missing []string
	for _, f := range []struct{ key, label string }{{"surName", "фамилия"}, {"age", "возраст"}, {"height", "рост"}, {"country", "страна"}, {"city", "город"}, {"goal", "цель знакомства"}, {"worldview", "мировоззрение"}, {"children", "дети"}, {"profession", "занятие"}, {"languages", "языки"}, {"familyNames", "родственные фамилии"}, {"description", "о себе"}} {
		value, present := p[f.key]
		if !present {
			continue
		}
		raw := strings.ToLower(strings.TrimSpace(valueString(value)))
		empty := raw == "" || raw == "null" || raw == "[]" || raw == "dont_tell"
		if (f.key == "age" || f.key == "height") && raw == "0" {
			empty = true
		}
		if (f.key == "description" || f.key == "familyNames") && !meaningfulProfileValue(raw) {
			empty = true
		}
		if empty {
			missing = append(missing, f.label)
		}
	}
	return missing
}
