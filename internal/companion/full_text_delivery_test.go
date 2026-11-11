package companion

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKnownFullTextReusesPersistedReceiptsAndEditsChangedRendering(t *testing.T) {
	s, p := cardFixture(t, 0)
	var methods []string
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, filepath.Base(r.URL.Path))
		_, _ = io.Copy(io.Discard, r.Body)
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":101,"chat":{"id":1}}}`)
	})}
	first := []photoText{{Text: "Описание"}}
	if err := s.sendKnownText(context.Background(), 1, p.VersionID, "full_text", first); err != nil {
		t.Fatal(err)
	}
	// The database receipt, rather than a process-local cooldown, handles restarts.
	copy := &Service{Config: s.Config, Store: s.Store, TG: s.TG}
	if err := copy.sendKnownText(context.Background(), 1, p.VersionID, "full_text", first); err != nil {
		t.Fatal(err)
	}
	if err := copy.sendKnownText(context.Background(), 1, p.VersionID, "full_text", []photoText{{Text: "Описание обновлено"}}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(methods) != "[sendMessage editMessageText]" {
		t.Fatalf("full text duplicated instead of reusing: %v", methods)
	}
	if err := s.Store.DeleteUser(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM text_deliveries").Scan(&count); err != nil || count != 0 {
		t.Fatal("deleted tenant retained text delivery records")
	}
}

func TestUnknownFullTextDeliveryNeverReplays(t *testing.T) {
	for _, unknownPart := range []int{1, 2} {
		t.Run(fmt.Sprint(unknownPart), func(t *testing.T) {
			s, p := cardFixture(t, 0)
			calls := 0
			s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				_, _ = io.Copy(io.Discard, r.Body)
				if calls == unknownPart {
					return nil, errors.New("response interrupted after request")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":101,"chat":{"id":1}}}`)), Request: r}, nil
			})
			parts := []photoText{{Text: "Часть 1"}}
			if unknownPart == 2 {
				parts = append(parts, photoText{Text: "Часть 2"})
			}
			if err := s.sendKnownText(context.Background(), 1, p.VersionID, "full_text", parts); err == nil {
				t.Fatal("unknown delivery accepted")
			}
			if err := s.sendKnownText(context.Background(), 1, p.VersionID, "full_text", parts); err == nil || calls != unknownPart {
				t.Fatalf("unknown send replayed: calls=%d err=%v", calls, err)
			}
			var state string
			if err := s.Store.DB.QueryRow("SELECT state FROM text_deliveries").Scan(&state); err != nil || state != "uncertain" {
				t.Fatal("delivery uncertainty not durable")
			}
		})
	}
}

func TestFullExifChunksNeverSplitTechnicalJSONObject(t *testing.T) {
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	path := photoMetaWrite(t, photoMetaWebP(tiff))
	var photos []MediaJob
	for i := 0; i < 20; i++ {
		photos = append(photos, MediaJob{Path: path})
	}
	parts, err := fullExifTextParts("Описание\n\nJardulik: 99", photos)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) < 2 {
		t.Fatal("fixture did not exercise chunk boundary")
	}
	objects := 0
	for _, part := range parts {
		if textUnits(part.Text) > 3900 {
			t.Fatal("full-text budget exceeded")
		}
		if err := validMessageEntities(part.Text, part.Entities); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(part.Text, "\n") {
			if strings.HasPrefix(line, "{") {
				if !json.Valid([]byte(line)) {
					t.Fatal("technical JSON was cut at a chunk boundary")
				}
				objects++
			}
		}
	}
	if objects != 20 {
		t.Fatalf("technical objects lost: %d", objects)
	}
}

func TestStoredSourceJSONKeepsNumbersKeysAndNeverAddsOurFields(t *testing.T) {
	s, p := cardFixture(t, 0)
	raw := `{"userId":10,"large":9007199254740993,"nil":null,"birthday":"2000-02-29T00:30:00+14:00","custom":[true,1.00]}`
	if _, err := s.Store.DB.Exec("UPDATE versions SET raw_json=? WHERE id=?", raw, p.VersionID); err != nil {
		t.Fatal(err)
	}
	got, err := storedProfileJSON(context.Background(), s.Store, 1, p.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	var expected bytes.Buffer
	if err = json.Indent(&expected, []byte(raw), "", "  "); err != nil {
		t.Fatal(err)
	}
	if got != expected.String() || !strings.Contains(got, "9007199254740993") || !strings.Contains(got, "1.00") {
		t.Fatalf("source JSON was reserialized or decorated: %s", got)
	}
	if _, err = storedProfileJSON(context.Background(), s.Store, 2, p.VersionID); err == nil {
		t.Fatal("another tenant read source JSON")
	}
	card := photoJSONCardText("Ира\n\nJardulik: 10", nil, "", 1024, got)
	if !card.SourceJSONInline || !strings.HasSuffix(photoQuoteText(t, card), got) {
		t.Fatal("short complete source JSON was not appended at quote end")
	}
	long := photoJSONCardText(strings.Repeat("🙂", 700)+"\n\nJardulik: 10", nil, "", 1024, got)
	if long.SourceJSONInline || textUnits(long.Text) > 1024 {
		t.Fatal("oversized source JSON was truncated into main caption")
	}
}

func TestSourceJSONPrivateVersionAndAdminRecipientIsolation(t *testing.T) {
	s, p := cardFixture(t, 0)
	s.Config.BackupChatID = -100123
	if err := s.Store.User(context.Background(), 2, ""); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Store.Archive(context.Background(), 2, []Profile{{"userId": 20, "name": "Чужой источник", "description": "TENANT_TWO_SOURCE"}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	var delivered []struct {
		Chat     int64           `json:"chat_id"`
		Text     string          `json:"text"`
		Entities []MessageEntity `json:"entities"`
	}
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if filepath.Base(r.URL.Path) == "answerCallbackQuery" {
			_, _ = io.Copy(io.Discard, r.Body)
			fmt.Fprint(w, `{"ok":true,"result":true}`)
			return
		}
		var payload struct {
			Chat     int64           `json:"chat_id"`
			Text     string          `json:"text"`
			Entities []MessageEntity `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		delivered = append(delivered, payload)
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":%d}}}`, 100+len(delivered), payload.Chat)
	})}
	uxTap(s, 1, fmt.Sprintf("jv:%d", ids[0]))
	uxSend(s, 1, fmt.Sprintf("/json_%d", ids[0]))
	if len(delivered) != 0 {
		t.Fatal("private source JSON command bypassed tenant version ownership")
	}
	uxTap(s, 2, fmt.Sprintf("jv:%d", p.VersionID))
	if len(delivered) != 0 {
		t.Fatal("other customer received primary source JSON")
	}
	group := func(from int64) {
		s.Handle(context.Background(), Update{Callback: &Callback{ID: "a", From: TGUser{ID: from}, Message: Message{Chat: TGChat{ID: -100123, Type: "supergroup"}}, Data: fmt.Sprintf("aj:%d", ids[0])}})
	}
	group(2)
	if len(delivered) != 0 {
		t.Fatal("non-operator archive callback accepted")
	}
	group(1)
	if len(delivered) != 1 || delivered[0].Chat != 1 || !strings.Contains(delivered[0].Text, "TENANT_TWO_SOURCE") || !json.Valid([]byte(delivered[0].Text)) || len(delivered[0].Entities) != 1 {
		t.Fatalf("admin source/recipient routing failed: %+v", delivered)
	}
	group(1)
	if len(delivered) != 1 {
		t.Fatal("repeated source JSON callback posted twice")
	}
}

func TestLargeSourceJSONIsOneWholeDocumentWithoutDescriptionCopy(t *testing.T) {
	s, p := cardFixture(t, 0)
	raw := `{"userId":10,"description":"` + strings.Repeat("источник ", 1000) + `","large":9007199254740993}`
	if _, err := s.Store.DB.Exec("UPDATE versions SET raw_json=? WHERE id=?", raw, p.VersionID); err != nil {
		t.Fatal(err)
	}
	pretty, err := storedProfileJSON(context.Background(), s.Store, 1, p.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if filepath.Base(r.URL.Path) != "sendDocument" {
			t.Fatalf("large JSON was split or copied with description: %s", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		file, header, err := r.FormFile("document")
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != pretty || !json.Valid(data) || filepath.Ext(header.Filename) != ".json" {
			t.Fatal("whole beautified JSON changed or truncated")
		}
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":101,"chat":{"id":1},"document":{"file_id":"json-file","file_unique_id":"json-unique","file_size":%d}}}`, len(data))
	})}
	s.sourceJSON(context.Background(), 1, 1, p.VersionID)
	s.sourceJSON(context.Background(), 1, 1, p.VersionID)
	if calls != 1 {
		t.Fatalf("source document duplicated: %d", calls)
	}
	files, err := os.ReadDir(filepath.Join(s.Config.DataDir, "json-staging"))
	if err != nil || len(files) != 0 {
		t.Fatal("temporary source JSON not cleaned up")
	}
}

func TestAdminFullTextUsesActualVersionOwnerMetadata(t *testing.T) {
	s, _ := cardFixture(t, 0)
	s.Config.BackupChatID = -100123
	if err := s.Store.User(context.Background(), 2, ""); err != nil {
		t.Fatal(err)
	}
	versions, err := s.Store.Archive(context.Background(), 2, []Profile{{"userId": 20, "name": "Другой", "photos": []string{"https://mini.jardulik.app/a.webp"}}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	data := photoMetaWebP(tiff)
	sha := digest(data)
	path := filepath.Join(s.Config.DataDir, "media", sha)
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.Store.MediaJobs(context.Background(), 2, versions[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.MediaDone(context.Background(), jobs[0], sha, path, "image/webp", int64(len(data))); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if filepath.Base(r.URL.Path) == "answerCallbackQuery" {
			_, _ = io.Copy(io.Discard, r.Body)
			fmt.Fprint(w, `{"ok":true,"result":true}`)
			return
		}
		calls++
		var payload struct {
			Chat int64  `json:"chat_id"`
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.Chat != 1 || !strings.Contains(payload.Text, `"Make":"TestMake"`) || strings.Contains(payload.Text, "ImageWidth") || strings.Contains(payload.Text, `"GPS"`) {
			t.Fatalf("admin EXIF used wrong source tenant or synthetic tags: %+v", payload)
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":101,"chat":{"id":1}}}`)
	})}
	callback := Update{Callback: &Callback{ID: "av", From: TGUser{ID: 1}, Message: Message{Chat: TGChat{ID: -100123, Type: "supergroup"}}, Data: fmt.Sprintf("av:%d", versions[0])}}
	s.Handle(context.Background(), callback)
	s.Handle(context.Background(), callback)
	if calls != 1 {
		t.Fatalf("repeated admin full text posted twice: %d", calls)
	}
}

func TestFullTextCallbackOpensSavedVersionAndKeepsOldRoutes(t *testing.T) {
	s, saved := cardFixture(t, 0)
	ctx := context.Background()
	archiveRevision := func(description string) {
		t.Helper()
		if _, err := s.Store.Archive(ctx, 1, []Profile{{"userId": saved.ID, "name": "Ира", "description": description}}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	archiveRevision("Текст второй версии")
	markup, err := json.Marshal(profileButtons(saved, true, false))
	if err != nil {
		t.Fatal(err)
	}
	versionRoute := fmt.Sprintf("tv:%d", saved.VersionID)
	if !strings.Contains(string(markup), `"callback_data":"`+versionRoute+`"`) {
		t.Fatal("card full-text action does not identify its saved version")
	}
	var texts []string
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch filepath.Base(r.URL.Path) {
		case "answerCallbackQuery":
			fmt.Fprint(w, `{"ok":true,"result":true}`)
		case "sendMessage":
			var body struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			texts = append(texts, body.Text)
			fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":1}}}`, 100+len(texts))
		default:
			t.Fatal("unexpected full-text operation", filepath.Base(r.URL.Path))
		}
	})}
	uxTap(s, 2, versionRoute)
	if len(texts) != 0 {
		t.Fatal("foreign owner opened a stored version")
	}
	uxTap(s, 1, versionRoute)
	uxTap(s, 1, fmt.Sprintf("t:%d", saved.ID))
	archiveRevision("Текст третьей версии")
	m := keyboardMessage(1, 104, fmt.Sprintf("/text_%d", saved.ID))
	s.Handle(ctx, Update{Message: &m})
	want := []string{"Полное описание", "Текст второй версии", "Текст третьей версии"}
	if len(texts) != len(want) {
		t.Fatal("versioned action or legacy route did not deliver full text")
	}
	for i, text := range texts {
		if !strings.Contains(text, want[i]) {
			t.Fatal("full-text route opened the wrong snapshot", i)
		}
	}
}
