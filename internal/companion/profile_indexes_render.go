package companion

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const profileIndexCompatibility = "compatibility"
const profileIndexAge = "age"
const profileIndexRichLimit = 32768

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
	Rich     map[string]any  `json:"rich_message,omitempty"`
	Legacy   bool            `json:"legacy,omitempty"`
}

func profileIndexTitle(kind string) string {
	switch kind {
	case profileIndexCompatibility:
		return "🧩 Совместимость"
	case profileIndexAge:
		return "🎂 Возраст"
	}
	return ""
}

func profileIndexZodiac(p Profile, now time.Time) string {
	return profileZodiac(p, now)
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
	return renderProfileIndexPagesMode(kind, entries, chat, true)
}

func renderProfileIndexPagesMode(kind string, entries []profileIndexEntry, chat int64, rich bool) ([]profileIndexPayload, error) {
	title := profileIndexTitle(kind)
	if title == "" || forumTopicURL(chat, 1) == "" {
		return nil, errors.New("invalid profile index destination or kind")
	}
	ordered := sortProfileIndex(kind, entries)
	limit := 4096
	if rich {
		limit = profileIndexRichLimit
	}
	type row struct{ label, tail, link string }
	var chunks [][]row
	current := []row{}
	units := 0
	for i, entry := range ordered {
		label := profileIndexRow(i+1, entry)
		tail := " " + profileIndexPremiumIcon(entry)
		link := profileIndexCardURL(chat, entry.ThreadID, entry.CardID)
		if entry.ID <= 0 || entry.ThreadID <= 0 || entry.CardID <= 0 || link == "" {
			return nil, errors.New("profile index has no confirmed card link")
		}
		// Reserve room for the heading and an archival pointer if this slot is later retired.
		if len(current) > 0 && units+1+profileIndexBudgetUnits(label+tail, rich) > limit-100 {
			chunks = append(chunks, current)
			current = nil
			units = 0
		}
		current = append(current, row{label, tail, link})
		units += 1 + profileIndexBudgetUnits(label+tail, rich)
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	if len(chunks) == 0 {
		chunks = append(chunks, nil)
	}
	pages := make([]profileIndexPayload, 0, len(chunks))
	for i, chunk := range chunks {
		heading := title
		if len(chunks) > 1 {
			heading = fmt.Sprintf("%s · %d/%d", title, i+1, len(chunks))
		}
		payload := profileIndexPayload{Text: heading, Entities: []MessageEntity{}}
		for _, r := range chunk {
			payload.Text += "\n"
			offset := textUnits(payload.Text)
			payload.Text += r.label + r.tail
			payload.Entities = append(payload.Entities, MessageEntity{Type: "text_link", Offset: offset, Length: textUnits(r.label), URL: r.link})
		}
		if len(chunk) == 0 {
			payload.Text += "\nАнкет пока нет."
		}
		payload.Entities = append(payload.Entities, MessageEntity{Type: "expandable_blockquote", Offset: textUnits(heading) + 1, Length: textUnits(payload.Text) - textUnits(heading) - 1})
		if rich {
			payload.Rich = profileIndexRichContent(payload)
		}
		if err := validProfileIndexPayload(payload); err != nil {
			return nil, err
		}
		pages = append(pages, payload)
	}
	return pages, nil
}

func profileIndexBudgetUnits(text string, rich bool) int {
	if rich {
		return utf8.RuneCountInString(text)
	}
	return textUnits(text)
}

func profileIndexCardURL(chat, thread, message int64) string {
	base := forumTopicURL(chat, thread)
	if base == "" || message <= 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d", base, message)
}

func profileIndexRichContent(payload profileIndexPayload) map[string]any {
	parts := strings.SplitN(payload.Text, "\n", 2)
	if len(parts) != 2 {
		return nil
	}
	headingUnits := textUnits(parts[0])
	text := func(value string, start int) any {
		var links []MessageEntity
		for _, entity := range payload.Entities {
			if entity.Type == "text_link" && entity.Offset >= start && entity.Offset+entity.Length <= start+textUnits(value) {
				entity.Offset -= start
				links = append(links, entity)
			}
		}
		sort.Slice(links, func(i, j int) bool { return links[i].Offset < links[j].Offset })
		var result []any
		cursor := 0
		for _, link := range links {
			_, remaining := telegramPrefix(value, cursor)
			prefix, rest := telegramPrefix(remaining, link.Offset-cursor)
			label, _ := telegramPrefix(rest, link.Length)
			if prefix != "" {
				result = append(result, prefix)
			}
			result = append(result, map[string]any{"type": "url", "text": label, "url": link.URL})
			cursor = link.Offset + link.Length
		}
		_, tail := telegramPrefix(value, cursor)
		if tail != "" || len(result) == 0 {
			result = append(result, tail)
		}
		return result
	}
	return map[string]any{"skip_entity_detection": true, "blocks": []any{
		map[string]any{"type": "paragraph", "text": text(parts[0], 0)},
		map[string]any{"type": "expandable_blockquote", "text": text(parts[1], headingUnits+1)},
	}}
}

func validProfileIndexPayload(payload profileIndexPayload) error {
	limit := 4096
	if payload.Rich != nil {
		limit = profileIndexRichLimit
		want, err := json.Marshal(profileIndexRichContent(payload))
		if err != nil {
			return err
		}
		got, err := json.Marshal(payload.Rich)
		if err != nil || !bytes.Equal(want, got) {
			return errors.New("invalid rich profile index structure")
		}
	}
	if strings.TrimSpace(payload.Text) == "" || profileIndexBudgetUnits(payload.Text, payload.Rich != nil) > limit {
		return errors.New("profile index exceeds Telegram message limit")
	}
	return validMessageEntities(payload.Text, payload.Entities)
}

func profileIndexPayloadBytes(payload profileIndexPayload) ([]byte, string, error) {
	if err := validProfileIndexPayload(payload); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	return raw, digest(raw), nil
}
