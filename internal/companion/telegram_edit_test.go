package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestEditOriginalDocumentInPlace(t *testing.T) {
	c := testConfig(t)
	path := filepath.Join(t.TempDir(), "originals.zip")
	data := []byte("whole ZIP fixture")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	tg := NewTelegram(c)
	calls := 0
	tg.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if filepath.Base(r.URL.Path) != "editMessageMedia" {
			t.Fatal("created a second post")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("chat_id") != "-10042" || r.FormValue("message_id") != "10" {
			t.Fatal("wrong original post")
		}
		var media map[string]string
		if err := json.Unmarshal([]byte(r.FormValue("media")), &media); err != nil {
			t.Fatal(err)
		}
		if media["media"] != "attach://archive" || media["caption"] != "Сохранено" || media["type"] != "document" {
			t.Fatal(media)
		}
		f, _, err := r.FormFile("archive")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil || string(b) != string(data) {
			t.Fatal("original changed")
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":10,"chat":{"id":-10042},"document":{"file_id":"new-whole-zip","file_size":17}}}`)
	})}
	m, err := tg.EditDocument(context.Background(), -10042, 10, path, "Сохранено")
	if err != nil || m.ID != 10 || calls != 1 {
		t.Fatalf("%+v %v %d", m, err, calls)
	}
}
