package companion

import (
	"fmt"
	"sort"
	"strings"
)

type photoText struct {
	Text     string
	Entities []MessageEntity
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

func photoMetadataRows(photos []MediaJob, full ...bool) []string {
	var rows []string
	for i, j := range photos {
		metadata, err := ReadPhotoMetadata(j.Path)
		fields := metadata.compactTechnicalFields()
		if len(full) > 0 && full[0] {
			fields = metadata.TechnicalRows()
		} else {
			for k := range fields {
				fields[k] = strings.Join(strings.Fields(fields[k]), " ")
			}
		}
		if metadata.HasGPS {
			fields = append(fields, "GPS: есть")
		} else if err == nil {
			fields = append(fields, "GPS: нет")
		}
		row := fmt.Sprintf("Фото %d", i+1)
		if len(fields) > 0 {
			row += ": " + strings.Join(fields, " · ")
		}
		rows = append(rows, row)
	}
	return rows
}

func compactPhotoRows(rows []string, budget int) string {
	if len(rows) == 0 {
		return ""
	}
	all := "О фото\n" + strings.Join(rows, "\n")
	if textUnits(all) <= budget {
		return all
	}
	per := (budget - textUnits("О фото\n") - len(rows) + 1) / len(rows)
	if per < 12 {
		return fmt.Sprintf("О фото\nФото 1..%d: полные технические данные по кнопке.", len(rows))
	}
	var compact []string
	for _, row := range rows {
		tail := ""
		for _, gps := range []string{" · GPS: есть", " · GPS: нет"} {
			if strings.HasSuffix(row, gps) {
				tail = gps
				row = strings.TrimSuffix(row, gps)
				break
			}
		}
		prefix, rest := telegramPrefix(row, per-textUnits(tail)-1)
		if rest != "" {
			prefix = strings.TrimSpace(prefix) + "…"
		}
		compact = append(compact, prefix+tail)
	}
	return "О фото\n" + strings.Join(compact, "\n")
}

// Metadata is a generated plain-text block. Entity offsets are calculated only
// after truncation, so profile text can never change the quote boundaries.
func photoCardText(text string, rows []string, tail string, limit int) photoText {
	body, footer := text, ""
	if i := strings.LastIndex(text, "\n\n"); i >= 0 && strings.Contains(text[i:], "Jardulik:") {
		body, footer = text[:i], text[i:]
	}
	link := ""
	if i := strings.LastIndex(footer, "\n📍 https://t.me/c/"); i >= 0 {
		link = footer[i:]
		footer = footer[:i]
	}
	tail += link
	if len(rows) == 0 {
		return photoText{Text: captionWithTail(body+footer, tail, limit)}
	}
	quoteBudget := 420
	available := limit - textUnits(footer+tail) - 12
	if quoteBudget > available/2 {
		quoteBudget = available / 2
	}
	quote := compactPhotoRows(rows, quoteBudget)
	bodyBudget := limit - textUnits("\n\n"+quote+footer+tail)
	prefix, rest := telegramPrefix(body, bodyBudget)
	if rest != "" {
		prefix, _ = telegramPrefix(body, bodyBudget-1)
		prefix = strings.TrimRight(prefix, " \n") + "…"
	}
	prefix = strings.TrimRight(prefix, " \n") + "\n\n"
	result := photoText{Text: prefix + quote + footer + tail}
	result.Entities = []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}
	return result
}

func fullPhotoText(text string, rows []string) photoText {
	if len(rows) == 0 {
		return photoText{Text: text}
	}
	body, footer := text, ""
	if i := strings.LastIndex(text, "\n\n"); i >= 0 && strings.Contains(text[i:], "Jardulik:") {
		body, footer = text[:i], text[i:]
	}
	prefix := body + "\n\n"
	quote := "О фото\n" + strings.Join(rows, "\n")
	return photoText{Text: prefix + quote + footer, Entities: []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}}
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
