package companion

import (
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"
)

func analysisPostModel(stored *string, raw json.RawMessage) string {
	model := ""
	if stored != nil {
		model = *stored
	}
	var reported string
	if json.Unmarshal(raw, &reported) == nil && reported != "" {
		model = reported
	}
	if strings.TrimSpace(model) == "" || textUnits(model) > 160 || strings.IndexFunc(model, unicode.IsControl) >= 0 {
		return "Jardulik GPT"
	}
	return model
}

func analysisBoldEscaped(text string, index int) bool {
	slashes := 0
	for i := index - 1; i >= 0 && text[i] == '\\'; i-- {
		slashes++
	}
	return slashes%2 == 1
}
func analysisBoldCanOpen(text string, index int) bool {
	if analysisBoldEscaped(text, index) || index+2 >= len(text) {
		return false
	}
	next, _ := utf8.DecodeRuneInString(text[index+2:])
	if unicode.IsSpace(next) || next == '*' || next == '`' {
		return false
	}
	if index > 0 {
		before, _ := utf8.DecodeLastRuneInString(text[:index])
		if unicode.IsLetter(before) || unicode.IsDigit(before) || before == '_' || before == '*' {
			return false
		}
	}
	return true
}
func analysisBoldCanClose(text string, index int) bool {
	if analysisBoldEscaped(text, index) || index == 0 {
		return false
	}
	before, _ := utf8.DecodeLastRuneInString(text[:index])
	if unicode.IsSpace(before) || before == '*' {
		return false
	}
	if index+2 < len(text) {
		after, _ := utf8.DecodeRuneInString(text[index+2:])
		if unicode.IsLetter(after) || unicode.IsDigit(after) || after == '_' || after == '*' {
			return false
		}
	}
	return true
}

// Interpret only paired strong markers outside code; all other source text stays literal.
func analysisPostBold(text string) (string, []MessageEntity) {
	type span struct{ open, close int }
	var spans []span
	pending, codeRun := -1, 0
	for i := 0; i < len(text); {
		if text[i] == '`' && !analysisBoldEscaped(text, i) {
			end := i + 1
			for end < len(text) && text[end] == '`' {
				end++
			}
			count := end - i
			if codeRun == 0 {
				codeRun = count
			} else if codeRun == count {
				codeRun = 0
			}
			i = end
			continue
		}
		if codeRun == 0 && i+1 < len(text) && text[i:i+2] == "**" {
			if pending >= 0 && analysisBoldCanClose(text, i) {
				spans = append(spans, span{pending, i})
				pending = -1
			} else if analysisBoldCanOpen(text, i) {
				pending = i
			}
			i += 2
			continue
		}
		_, size := utf8.DecodeRuneInString(text[i:])
		i += size
	}
	if len(spans) == 0 {
		return text, nil
	}
	var out strings.Builder
	entities := make([]MessageEntity, 0, len(spans))
	cursor, units := 0, 0
	for _, s := range spans {
		literal := text[cursor:s.open]
		out.WriteString(literal)
		units += textUnits(literal)
		content := text[s.open+2 : s.close]
		length := textUnits(content)
		out.WriteString(content)
		entities = append(entities, MessageEntity{Type: "bold", Offset: units, Length: length})
		units += length
		cursor = s.close + 2
	}
	out.WriteString(text[cursor:])
	return out.String(), entities
}

func analysisPostQuoteEntities(prefix, content string, bold []MessageEntity) []MessageEntity {
	offset, length := textUnits(prefix), textUnits(content)
	entities := []MessageEntity{{Type: "expandable_blockquote", Offset: offset, Length: length}}
	for _, entity := range bold {
		if entity.Offset >= length {
			continue
		}
		entity.Length = min(entity.Length, length-entity.Offset)
		if entity.Length <= 0 {
			continue
		}
		entity.Offset += offset
		entities = append(entities, entity)
	}
	return entities
}
