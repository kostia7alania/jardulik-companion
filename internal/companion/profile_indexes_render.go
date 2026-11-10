package companion

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const profileIndexCompatibility = "compatibility"
const profileIndexAge = "age"
const profileIndexRowsPerPage = 50

type profileIndexEntry struct {
	ID, ThreadID, CardID   int64
	Name, Birthday, Zodiac string
	Age                    int64
	AgeKnown               bool
	Percent                int
	PercentKnown           bool
	Premium, PremiumKnown  bool
}

type profileIndexPayload struct {
	Text     string          `json:"text"`
	Entities []MessageEntity `json:"entities"`
}

func profileIndexTitle(kind string) string {
	switch kind {
	case profileIndexCompatibility:
		return "🧩 По совместимости"
	case profileIndexAge:
		return "🎂 По возрасту"
	}
	return ""
}

func profileIndexZodiac(p Profile, now time.Time) string {
	date, known := birthdayGreetingDate(p)
	if !known || date.After(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)) {
		return "❔"
	}
	// The same calendar cutoffs as birthdayGreetingSign, without its editorial guard.
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

func profileIndexPremium(p Profile) (premium, known bool) {
	// Accept source numeric values only; strings and missing values are unknown.
	switch p["type"].(type) {
	case int, int64, float64, json.Number:
	default:
		return false, false
	}
	tier, valid := autoInteger(p["type"])
	return tier == 1, valid && (tier == 0 || tier == 1)
}

func profileIndexPremiumIcon(e profileIndexEntry) string {
	if !e.PremiumKnown {
		return "❔"
	}
	if e.Premium {
		return "⭐️"
	}
	return "⚪️"
}

func profileIndexName(p Profile, id int64) string {
	surname := p.String("surName")
	if strings.TrimSpace(surname) == "" {
		surname = p.String("surname")
	}
	name := strings.Join(strings.Fields(p.String("name")+" "+surname), " ")
	if !meaningfulProfileValue(name) {
		return "#" + strconv.FormatInt(id, 10)
	}
	name, _ = telegramPrefix(name, 80)
	return name
}

func sortProfileIndex(kind string, entries []profileIndexEntry) []profileIndexEntry {
	out := append([]profileIndexEntry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if kind == profileIndexCompatibility {
			if a.PercentKnown != b.PercentKnown {
				return a.PercentKnown
			}
			if a.PercentKnown && a.Percent != b.Percent {
				return a.Percent > b.Percent
			}
		}
		if a.AgeKnown != b.AgeKnown {
			return a.AgeKnown
		}
		if a.AgeKnown && a.Age != b.Age {
			return a.Age < b.Age
		}
		if kind == profileIndexAge && a.Birthday != b.Birthday {
			return a.Birthday > b.Birthday
		}
		an, bn := strings.ToLower(a.Name), strings.ToLower(b.Name)
		if an != bn {
			return an < bn
		}
		return a.ID < b.ID
	})
	return out
}

func profileIndexRow(n int, e profileIndexEntry) string {
	age, percent := "?", "—"
	if e.AgeKnown {
		age = strconv.FormatInt(e.Age, 10)
	}
	if e.PercentKnown {
		percent = strconv.Itoa(e.Percent) + "%"
	}
	sign := e.Zodiac
	if sign == "" {
		sign = "❔"
	}
	return fmt.Sprintf("%d %s %s, %s, %s", n, sign, age, e.Name, percent)
}

func renderProfileIndexPages(kind string, entries []profileIndexEntry, chat int64) ([]profileIndexPayload, error) {
	title := profileIndexTitle(kind)
	if title == "" || forumTopicURL(chat, 1) == "" {
		return nil, errors.New("invalid profile index destination or kind")
	}
	ordered := sortProfileIndex(kind, entries)
	if len(ordered) == 0 {
		return []profileIndexPayload{{Text: title + "\n\nАнкет пока нет.", Entities: []MessageEntity{}}}, nil
	}
	type row struct{ label, tail, link string }
	var chunks [][]row
	current := []row{}
	units := 0
	for i, entry := range ordered {
		label := profileIndexRow(i+1, entry)
		tail := " " + profileIndexPremiumIcon(entry)
		link := forumTopicURL(chat, entry.CardID)
		if entry.ID <= 0 || entry.ThreadID <= 0 || entry.CardID <= 0 || link == "" {
			return nil, errors.New("profile index has no confirmed card link")
		}
		// Leave room for the compact heading and page count as the archive grows.
		if len(current) >= profileIndexRowsPerPage || units+1+textUnits(label+tail) > 4096-80 {
			chunks = append(chunks, current)
			current = nil
			units = 0
		}
		current = append(current, row{label, tail, link})
		units += 1 + textUnits(label+tail)
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	pages := make([]profileIndexPayload, 0, len(chunks))
	for i, chunk := range chunks {
		payload := profileIndexPayload{Text: fmt.Sprintf("%s · %d/%d", title, i+1, len(chunks)), Entities: []MessageEntity{}}
		for _, r := range chunk {
			payload.Text += "\n"
			offset := textUnits(payload.Text)
			payload.Text += r.label + r.tail
			payload.Entities = append(payload.Entities, MessageEntity{Type: "text_link", Offset: offset, Length: textUnits(r.label), URL: r.link})
		}
		if textUnits(payload.Text) > 4096 {
			return nil, errors.New("profile index exceeds Telegram message limit")
		}
		if err := validMessageEntities(payload.Text, payload.Entities); err != nil {
			return nil, err
		}
		pages = append(pages, payload)
	}
	return pages, nil
}

func profileIndexPayloadBytes(payload profileIndexPayload) ([]byte, string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}
