package companion

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAnalysisSourceCallbackExportsExactOwnedResponse(t *testing.T) {
	store, input := analysisFixture(t)
	ctx := context.Background()
	analysis, err := store.SaveAnalysisCapture(ctx, 123, input)
	if err != nil {
		t.Fatal(err)
	}
	cfg := store.cfg
	cfg.OwnerID, cfg.BackupChatID = 123, -100123
	service := &Service{Config: cfg, Store: store, TG: NewTelegram(cfg)}
	documents, answers := 0, 0
	service.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/answerCallbackQuery") {
			answers++
			return albumTestResponse(r, 200, `{"ok":true,"result":true}`), nil
		}
		if !strings.HasSuffix(r.URL.Path, "/sendDocument") {
			t.Fatal("unexpected method", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("chat_id") != "123" {
			t.Fatal("wrong recipient")
		}
		file, header, err := r.FormFile("document")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		raw, err := io.ReadAll(file)
		if err != nil || !bytes.Equal(raw, input.ResponseBody) || !strings.HasSuffix(header.Filename, ".json") {
			t.Fatal("source bytes or extension changed", err)
		}
		documents++
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":71,"chat":{"id":123},"document":{"file_id":"exact-file","file_size":%d}}}`, len(raw))), nil
	})
	callback := Callback{ID: "cb", From: TGUser{ID: 123}, Message: Message{Chat: TGChat{ID: -100123}}, Data: fmt.Sprintf("ga:%d", analysis.ID)}
	service.analysisPostCallback(ctx, callback)
	if documents != 1 || answers != 1 {
		t.Fatal("owned source was not delivered", documents, answers)
	}
	callback.From.ID = 456
	service.analysisPostCallback(ctx, callback)
	callback.From.ID = 123
	callback.Message.Chat.ID = -100456
	service.analysisPostCallback(ctx, callback)
	if documents != 1 || answers != 1 {
		t.Fatal("callback escaped owner/chat boundary")
	}
	callback.Message.Chat.ID = -100123
	if err := store.SaveSession(ctx, 123, Session{Token: "new", UserID: 501, Generation: "new"}); err != nil {
		t.Fatal(err)
	}
	service.analysisPostCallback(ctx, callback)
	if documents != 1 || answers != 2 {
		t.Fatal("old account response was delivered", documents, answers)
	}
}
