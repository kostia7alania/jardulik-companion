package companion

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

func analysisFormatEntityText(text string, entity MessageEntity) string {
	units := utf16.Encode([]rune(text))
	return string(utf16.Decode(units[entity.Offset : entity.Offset+entity.Length]))
}

func TestAnalysisPostBoldFormatsOnlyPairedMarkers(t *testing.T) {
	for _, test := range []struct {
		name, raw, display string
		bold               []string
	}{
		{"headings", "😀 **Астрология**/ **Итог**", "😀 Астрология/ Итог", []string{"Астрология", "Итог"}},
		{"unmatched", "Оставить **открытый маркер", "Оставить **открытый маркер", nil},
		{"math", "2 ** 3 ** 4; x**2**; **Итог**", "2 ** 3 ** 4; x**2**; Итог", []string{"Итог"}},
		{"escaped", `\**буквально** и **жирно**`, `\**буквально** и жирно`, []string{"жирно"}},
		{"inline_code", "`**буквально**` и **жирно**", "`**буквально**` и жирно", []string{"жирно"}},
		{"fenced_code", "```text\n**буквально**\n```\n**жирно**", "```text\n**буквально**\n```\nжирно", []string{"жирно"}},
		{"triples", "***буквально*** и ****", "***буквально*** и ****", nil},
		{"multiline", "**А😀\nБ**", "А😀\nБ", []string{"А😀\nБ"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			display, entities := analysisPostBold(test.raw)
			if display != test.display || len(entities) != len(test.bold) {
				t.Fatalf("display=%q entities=%+v", display, entities)
			}
			for i, entity := range entities {
				if entity.Type != "bold" || analysisFormatEntityText(display, entity) != test.bold[i] {
					t.Fatalf("wrong bold range: %+v", entity)
				}
			}
		})
	}
}

func TestAnalysisPostHeaderUsesReportedModelWithoutSourceLine(t *testing.T) {
	f := newAnalysisPostFixture(t, "**Итог**: Оценка совместимости — 75%.")
	original := append([]byte(nil), f.analysis.ResponseBody...)
	payload, err := analysisPostText(f.analysis)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(payload.Text, "🧩 Совместимость: 75% · reported-model\n"+displayTime(f.now.UTC().Format(time.RFC3339Nano))+"\n\n") {
		t.Fatal("wrong model/date header", payload.Text)
	}
	prefix, _ := telegramPrefix(payload.Text, payload.Entities[0].Offset)
	if strings.Contains(prefix, "От:") || strings.Contains(prefix, "Own") || strings.Contains(prefix, "101") {
		t.Fatal("source identity remains in header", prefix)
	}
	if analysisPostQuoted(payload) != "Итог: Оценка совместимости — 75%." {
		t.Fatal("bold markers remained or content changed")
	}
	if len(payload.Entities) != 2 || analysisFormatEntityText(payload.Text, payload.Entities[1]) != "Итог" {
		t.Fatal("missing nested bold entity", payload.Entities)
	}
	if !bytes.Equal(original, f.analysis.ResponseBody) {
		t.Fatal("rendering changed raw response")
	}
	for _, test := range []struct {
		stored *string
		raw    string
		want   string
	}{
		{nil, `"gpt-4o-mini"`, "gpt-4o-mini"},
		{analysisPostString("stored-model"), `"provider-model"`, "provider-model"},
		{analysisPostString("stored-model"), `null`, "stored-model"},
		{nil, `null`, "Jardulik GPT"},
		{analysisPostString(""), `""`, "Jardulik GPT"},
		{nil, `"model\nspoofed"`, "Jardulik GPT"},
	} {
		if got := analysisPostModel(test.stored, json.RawMessage(test.raw)); got != test.want {
			t.Fatalf("model=%q want=%q raw=%s", got, test.want, test.raw)
		}
	}
}

func analysisPostString(s string) *string { return &s }

func TestAnalysisPostClipsNestedBoldAtUTF16QuoteBoundary(t *testing.T) {
	raw := "Начало 😀\n**" + strings.Repeat("Ж😀", 2000) + "**\n**После обрезки**"
	f := newAnalysisPostFixture(t, raw)
	payload, err := analysisPostText(f.analysis)
	if err != nil {
		t.Fatal(err)
	}
	formatted, _ := analysisPostBold(raw)
	quote := payload.Entities[0]
	if textUnits(payload.Text) > 4096 || !strings.HasPrefix(formatted, analysisPostQuoted(payload)) || payload.Keyboard == nil {
		t.Fatal("wrong long-format fallback")
	}
	if len(payload.Entities) != 2 {
		t.Fatal("out-of-range bold entity was not excluded", payload.Entities)
	}
	bold := payload.Entities[1]
	if bold.Type != "bold" || bold.Offset < quote.Offset || bold.Offset+bold.Length != quote.Offset+quote.Length {
		t.Fatalf("bold not clipped within quote: %+v %+v", quote, bold)
	}
	if err := validMessageEntities(payload.Text, payload.Entities); err != nil {
		t.Fatal("invalid UTF16 or nested ranges", err)
	}
	if strings.Contains(analysisPostQuoted(payload), "**") {
		t.Fatal("paired Markdown markers remained after clipping")
	}
}
