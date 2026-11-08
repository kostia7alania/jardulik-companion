package companion

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type photoText struct {
	Text             string
	Entities         []MessageEntity
	Truncated        bool
	SourceJSONInline bool
}

func selectedProfilePhotos(jobs []MediaJob) []MediaJob {
	hasFull := false
	for _, j := range jobs {
		if j.State == "complete" && j.SHA != "" && j.Role == "photos" {
			hasFull = true
		}
	}
	var photos []MediaJob
	for _, j := range jobs {
		if j.State != "complete" || j.SHA == "" || (hasFull && j.Role != "photos") || (!hasFull && j.Role != "miniPhotos") {
			continue
		}
		photos = append(photos, j)
	}
	sort.SliceStable(photos, func(i, j int) bool { return photos[i].Position < photos[j].Position })
	return photos
}

// Source-derived EXIF fields only. Presentation indexes, container dimensions
// and GPS presence belong to the compact card, never to these JSON objects.
func photoExifBlocks(photos []MediaJob) []string {
	type record struct {
		Make           string  `json:"Make,omitempty"`
		Model          string  `json:"Model,omitempty"`
		Lens           string  `json:"LensModel,omitempty"`
		Software       string  `json:"Software,omitempty"`
		DateOriginal   string  `json:"DateTimeOriginal,omitempty"`
		DateDigitized  string  `json:"DateTimeDigitized,omitempty"`
		DateModified   string  `json:"DateTime,omitempty"`
		OffsetOriginal string  `json:"OffsetTimeOriginal,omitempty"`
		Exposure       string  `json:"ExposureTime,omitempty"`
		FNumber        float64 `json:"FNumber,omitempty"`
		FocalLength    float64 `json:"FocalLength,omitempty"`
		ISO            int     `json:"ISO,omitempty"`
		Orientation    int     `json:"Orientation,omitempty"`
	}
	var blocks []string
	for i, photo := range photos {
		m, _ := ReadPhotoMetadata(photo.Path)
		value := record{Make: m.Make, Model: m.Model, Lens: m.Lens, Software: m.Software, DateOriginal: m.DateOriginal, DateDigitized: m.DateDigitized, DateModified: m.DateModified, OffsetOriginal: m.OffsetOriginal, Exposure: m.Exposure, FNumber: m.FNumber, FocalLength: m.FocalLength, ISO: m.ISO, Orientation: m.Orientation}
		data, _ := json.Marshal(value)
		if string(data) == "{}" {
			continue
		}
		blocks = append(blocks, fmt.Sprintf("%d:\n%s", i+1, data))
	}
	return blocks
}

// The main caption always keeps the compact summary, not a synthesized JSON dump.
func photoJSONCardText(text string, photos []MediaJob, tail string, limit int, sourceJSON ...string) photoText {
	value := photoCardText(text, photoMetadataRows(photos), tail, limit)
	if len(sourceJSON) > 0 && sourceJSON[0] != "" && textUnits(value.Text)+textUnits(sourceJSON[0])+1 <= limit {
		for i, e := range value.Entities {
			if e.Type != "expandable_blockquote" {
				continue
			}
			at := e.Offset + e.Length
			prefix, rest := telegramPrefix(value.Text, at)
			added := 1 + textUnits(sourceJSON[0])
			value.Text = prefix + "\n" + sourceJSON[0] + rest
			value.Entities[i].Length += added
			for j := range value.Entities {
				if j != i && value.Entities[j].Offset >= at {
					value.Entities[j].Offset += added
				}
			}
			value.SourceJSONInline = true
			break
		}
	}
	return value
}

func photoMetadataRows(photos []MediaJob) []string {
	var rows []string
	var previous string
	start := 0
	for i, photo := range photos {
		m, _ := ReadPhotoMetadata(photo.Path)
		row := ""
		if m.Width > 0 && m.Height > 0 {
			row = fmt.Sprintf("%d×%d", m.Width, m.Height)
		}
		if m.GPS != "" {
			if row != "" {
				row += " · "
			}
			row += "GPS: " + m.GPS
		}
		if i > 0 && row == previous {
			rows[len(rows)-1] = fmt.Sprintf("%d-%d:%s", start+1, i+1, row)
		} else {
			start = i
			rows = append(rows, fmt.Sprintf("%d:%s", i+1, row))
		}
		previous = row
	}
	return rows
}

func compactPhotoRows(rows []string, budget int) string {
	if len(rows) == 0 || budget <= 0 {
		return ""
	}
	all := strings.Join(rows, "\n")
	if textUnits(all) <= budget {
		return all
	}
	per := (budget - len(rows) + 1) / len(rows)
	if per < 22 {
		summary, _ := telegramPrefix(fmt.Sprintf("1..%d: технические данные по кнопке.", len(rows)), budget)
		return summary
	}
	var compact []string
	for _, row := range rows {
		tail := ""
		if i := strings.LastIndex(row, " · GPS:"); i >= 0 && textUnits(row[i:])+4 <= per {
			tail = row[i:]
			row = row[:i]
		}
		prefix, rest := telegramPrefix(row, per-textUnits(tail)-1)
		if rest != "" {
			prefix = strings.TrimSpace(prefix) + "…"
		}
		compact = append(compact, prefix+tail)
	}
	return strings.Join(compact, "\n")
}

// Only the last generated footer, ending in a numeric Jardulik ID, is split.
// The profile biography remains plain text, including any lookalike labels.
func splitPhotoFooter(text string) (body, details, footer, link string) {
	marker := strings.LastIndex(text, "\nJardulik: ")
	if marker < 0 {
		return text, "", "", ""
	}
	end := strings.IndexByte(text[marker+1:], '\n')
	if end < 0 {
		end = len(text)
	} else {
		end += marker + 1
	}
	idLine := text[marker+1 : end]
	id, err := strconv.ParseInt(strings.TrimPrefix(idLine, "Jardulik: "), 10, 64)
	start := strings.LastIndex(text[:marker+1], "\n\n")
	if err != nil || id <= 0 || start < 0 {
		return text, "", "", ""
	}
	var inside, outside []string
	for _, line := range strings.Split(strings.TrimPrefix(text[start:], "\n\n"), "\n") {
		switch {
		case line == idLine:
			inside = append(inside, line)
		case strings.HasPrefix(line, "Не указано: "):
			inside = append(inside, line)
		case strings.HasPrefix(line, "📍 https://t.me/c/"):
			link = strings.TrimPrefix(line, "📍 ")
		default:
			outside = append(outside, line)
		}
	}
	if len(outside) > 0 {
		footer = "\n\n" + strings.Join(outside, "\n")
	}
	return text[:start], strings.Join(inside, "\n"), footer, link
}

// Offsets are calculated after truncation; generated details keep a fixed
// budget and cannot be displaced by a long biography or photo metadata.
func photoCardText(text string, rows []string, tail string, limit int) photoText {
	body, details, footer, link := splitPhotoFooter(text)
	if validPostURL(link) {
		tail += "\n🔗 пост"
	}
	if len(rows) == 0 && details == "" {
		return withPostLinkEntity(photoText{Text: captionWithTail(body+footer, tail, limit)}, link)
	}
	quoteBudget := 420
	available := limit - textUnits(footer+tail) - 12
	if quoteBudget > available/2 {
		quoteBudget = available / 2
	}
	if quoteBudget < textUnits(details) {
		quoteBudget = textUnits(details)
	}
	rowBudget := quoteBudget - textUnits(details)
	if len(rows) > 0 && details != "" {
		rowBudget--
	}
	quote := compactPhotoRows(rows, rowBudget)
	if details != "" {
		if quote != "" {
			quote += "\n"
		}
		quote += details
	}
	bodyBudget := limit - textUnits("\n\n"+quote+footer+tail)
	prefix, rest := telegramPrefix(body, bodyBudget)
	if rest != "" {
		prefix, _ = telegramPrefix(body, bodyBudget-1)
		prefix = strings.TrimRight(prefix, " \n") + "…"
	}
	prefix = strings.TrimRight(prefix, " \n") + "\n\n"
	result := photoText{Text: prefix + quote + footer + tail, Truncated: rest != ""}
	if quote != "" {
		result.Entities = []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}
	}
	return withPostLinkEntity(result, link)
}

func fullPhotoText(text string, rows []string) photoText {
	body, details, footer, link := splitPhotoFooter(text)
	quote := strings.Join(rows, "\n")
	if details != "" {
		if quote != "" {
			quote += "\n"
		}
		quote += details
	}
	if quote == "" {
		return photoText{Text: text}
	}
	prefix := body + "\n\n"
	result := photoText{Text: prefix + quote + footer, Entities: []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}}
	return appendPostLink(result, link)
}

func photoTextPart(value photoText, limit int) (photoText, photoText) {
	part, rest := telegramPrefix(value.Text, limit)
	units := textUnits(part)
	first, next := photoText{Text: part}, photoText{Text: rest}
	for _, e := range value.Entities {
		end := e.Offset + e.Length
		if e.Offset < units {
			clipped := e
			if end > units {
				clipped.Length = units - e.Offset
			}
			if clipped.Length > 0 {
				first.Entities = append(first.Entities, clipped)
			}
		}
		if end > units {
			clipped := e
			clipped.Offset -= units
			if clipped.Offset < 0 {
				clipped.Offset = 0
			}
			clipped.Length = end - units - clipped.Offset
			if clipped.Length > 0 {
				next.Entities = append(next.Entities, clipped)
			}
		}
	}
	return first, next
}

func photoQuote(entities []MessageEntity) (MessageEntity, bool) {
	for _, e := range entities {
		if e.Type == "expandable_blockquote" {
			return e, true
		}
	}
	return MessageEntity{}, false
}
func withPostLinkEntity(value photoText, link string) photoText {
	if validPostURL(link) && strings.HasSuffix(value.Text, "🔗 пост") {
		value.Entities = append(value.Entities, MessageEntity{Type: "text_link", Offset: textUnits(value.Text) - textUnits("🔗 пост"), Length: textUnits("🔗 пост"), URL: link})
	}
	return value
}
func appendPostLink(value photoText, link string) photoText {
	if validPostURL(link) {
		value.Text += "\n🔗 пост"
		return withPostLinkEntity(value, link)
	}
	return value
}
func privateAlbumActionTail(p StoredProfile, photos int) string {
	if photos < 2 {
		return ""
	}
	return fmt.Sprintf("\n\nТекст: /text_%d\nJSON: /json_%d\n❤️ /like_%d · Избранное: /fav_%d · /next", p.ID, p.VersionID, p.ID, p.ID)
}
func unavailableMediaCount(jobs []MediaJob) int {
	n := 0
	for _, j := range jobs {
		if j.State != "complete" || j.SHA == "" {
			n++
		}
	}
	return n
}

// Both destinations reserve the same body budget. Only private albums append
// their contextual commands; the configured post link remains the final line.
func renderProfileCard(p StoredProfile, photos []MediaJob, missing int, noPhotos bool, link string, limit int, private bool) (photoText, error) {
	pretty, err := prettyProfileJSON(p.RawJSON)
	if err != nil {
		return photoText{}, err
	}
	text := profileText(p)
	if missing > 0 {
		text += fmt.Sprintf("\nНедоступных исходных фото: %d. Подробности в ZIP.", missing)
	}
	if noPhotos {
		text += "\nФотографии пока недоступны."
	}
	actions := privateAlbumActionTail(p, len(photos))
	card := photoJSONCardText(text, photos, "", limit-textUnits(actions+"\n🔗 пост"), pretty)
	if private {
		card.Text += actions
	}
	return appendPostLink(card, link), nil
}
