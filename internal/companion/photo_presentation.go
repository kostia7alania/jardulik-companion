package companion

import (
	"fmt"
	"sort"
	"strconv"
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
	if len(rows) == 0 || budget <= 0 {
		return ""
	}
	all := strings.Join(rows, "\n")
	if textUnits(all) <= budget {
		return all
	}
	per := (budget - len(rows) + 1) / len(rows)
	if per < 22 {
		summary, _ := telegramPrefix(fmt.Sprintf("Фото 1..%d: полные технические данные по кнопке.", len(rows)), budget)
		return summary
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
			link = "\n" + line
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
	tail += link
	if len(rows) == 0 && details == "" {
		return photoText{Text: captionWithTail(body+footer, tail, limit)}
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
	result := photoText{Text: prefix + quote + footer + tail}
	if quote != "" {
		result.Entities = []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}
	}
	return result
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
	return photoText{Text: prefix + quote + footer + link, Entities: []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(quote)}}}
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
