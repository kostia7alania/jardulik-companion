package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func assertJardulikAppLink(t *testing.T, markup any) {
	t.Helper()
	raw, err := json.Marshal(markup)
	if err != nil {
		t.Fatal(err)
	}
	var keyboard struct {
		Rows [][]map[string]any `json:"inline_keyboard"`
	}
	if err := json.Unmarshal(raw, &keyboard); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range keyboard.Rows {
		for _, button := range row {
			if button["text"] != "↗️ Открыть Jardulik" {
				continue
			}
			found++
			if button["url"] != "https://t.me/jardulik_bot?startapp" || button["web_app"] != nil || button["callback_data"] != nil {
				t.Fatal("Mini App must launch through its own bot without credentials or profile parameters", button)
			}
		}
	}
	if found != 1 {
		t.Fatalf("expected one app launch link, got %d", found)
	}
}

func TestJardulikDeepLinksUseOfficialBotInAllControls(t *testing.T) {
	for name, markup := range map[string]any{
		"main":    mainButtons(),
		"profile": profileButtons(StoredProfile{ID: 123, VersionID: 456}, true, true),
		"archive": archiveProfileKeyboard(456),
	} {
		t.Run(name, func(t *testing.T) { assertJardulikAppLink(t, markup) })
	}
	t.Run("keyboard menu", func(t *testing.T) {
		s, _ := uxFixture(t)
		var sends int
		s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Markup json.RawMessage `json:"reply_markup"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			assertJardulikAppLink(t, body.Markup)
			sends++
			fmt.Fprint(w, `{"ok":true,"result":{"message_id":123,"chat":{"id":1}}}`)
		})}
		s.showKeyboardMenuLocked(context.Background(), 1)
		if sends != 1 {
			t.Fatalf("menu sent %d messages", sends)
		}
	})
}
