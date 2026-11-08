package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
)

func photoPresentationSource(t *testing.T, index, width, height int) MediaJob {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, width, height))); err != nil {
		t.Fatal(err)
	}
	sha := digest(b.Bytes())
	path := filepath.Join(t.TempDir(), sha)
	if err := os.WriteFile(path, b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return MediaJob{Role: "photos", Position: index, State: "complete", SHA: sha, Path: path, Size: int64(b.Len())}
}

func photoQuoteText(t *testing.T, value photoText) string {
	t.Helper()
	if len(value.Entities) != 1 {
		t.Fatalf("expected one collapsed quote: %+v", value.Entities)
	}
	if err := validMessageEntities(value.Text, value.Entities); err != nil {
		t.Fatal(err)
	}
	e := value.Entities[0]
	text := utf16.Encode([]rune(value.Text))
	return string(utf16.Decode(text[e.Offset : e.Offset+e.Length]))
}

func TestPhotoCaptionQuotePreservesFooterAndSafeNumberedOrder(t *testing.T) {
	first := photoPresentationSource(t, 0, 2, 3)
	second := photoPresentationSource(t, 1, 4, 5)
	rows := photoMetadataRows(selectedProfilePhotos([]MediaJob{second, first}))
	if len(rows) != 2 || !strings.Contains(rows[0], "Фото 1: FileType: PNG · ImageWidth: 2 · ImageHeight: 3") || !strings.Contains(rows[1], "Фото 2: FileType: PNG · ImageWidth: 4 · ImageHeight: 5") {
		t.Fatalf("metadata order/format incorrect: %v", rows)
	}
	text := strings.Repeat("🙂<blockquote>О фото</blockquote>", 300) + "\n\n8.11.26, 01:10 (вс, UTC+3)\nJardulik: 99\n📍 https://t.me/c/123/55"
	card := photoCardText(text, rows, "\nТекст: /text_99", 1024)
	quote := photoQuoteText(t, card)
	if textUnits(card.Text) > 1024 || !strings.Contains(quote, "Фото 1:") || !strings.Contains(quote, "Фото 2:") || strings.Contains(quote, "Jardulik:") || !strings.Contains(card.Text, "Jardulik: 99") || !strings.Contains(card.Text, "UTC+3") || !strings.HasSuffix(card.Text, "https://t.me/c/123/55") {
		t.Fatalf("caption/entity/footer invalid: %+v", card)
	}
}

func TestFullPhotoTextChunksClipEntitiesAndKeepAllTechnicalText(t *testing.T) {
	value := fullPhotoText(strings.Repeat("🙂", 2500)+"\n\nJardulik: 99", []string{"Фото 1: " + strings.Repeat("данные ", 700), "Фото 2: FileType: PNG · ImageWidth: 4 · ImageHeight: 5"})
	original := value.Text
	var joined strings.Builder
	quoteParts := 0
	for value.Text != "" {
		part, rest := photoTextPart(value, 3900)
		if textUnits(part.Text) > 3900 {
			t.Fatal("text budget exceeded")
		}
		if err := validMessageEntities(part.Text, part.Entities); err != nil {
			t.Fatal(err)
		}
		quoteParts += len(part.Entities)
		joined.WriteString(part.Text)
		value = rest
	}
	if joined.String() != original || quoteParts < 2 || !strings.Contains(joined.String(), "Фото 2: FileType: PNG") {
		t.Fatal("full metadata or chunk entity range lost")
	}
}

func TestTelegramPhotoEntitiesUsePlainTextAndFirstAlbumItem(t *testing.T) {
	c, _, a, _, _ := archivePresentationFixture(t)
	first := photoPresentationSource(t, 0, 2, 3)
	second := photoPresentationSource(t, 1, 4, 5)
	card := photoCardText("🙂 <script>plain</script>\n\nJardulik: 99", photoMetadataRows([]MediaJob{first, second}), "", 1024)
	calls := 0
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		calls++
		switch call.method {
		case "sendMediaGroup":
			var media []struct {
				Caption  string          `json:"caption"`
				Entities []MessageEntity `json:"caption_entities"`
			}
			if err := json.Unmarshal([]byte(call.fields["media"]), &media); err != nil {
				t.Fatal(err)
			}
			if len(media[0].Entities) != 1 || len(media[1].Entities) != 0 || media[1].Caption != "" || !strings.Contains(media[0].Caption, "<script>plain</script>") {
				t.Fatal("album plain caption/entities misplaced")
			}
			return `{"ok":true,"result":[{"message_id":1,"chat":{"id":-100123}},{"message_id":2,"chat":{"id":-100123}}]}`, nil
		case "sendPhoto":
			var entities []MessageEntity
			if err := json.Unmarshal([]byte(call.fields["caption_entities"]), &entities); err != nil || len(entities) != 1 {
				t.Fatal("photo entities missing")
			}
			if call.fields["parse_mode"] != "" {
				t.Fatal("untrusted profile text uses parse mode")
			}
			return `{"ok":true,"result":{"message_id":3,"chat":{"id":-100123}}}`, nil
		default:
			t.Fatalf("unexpected method %s", call.method)
		}
		return "", nil
	})
	if _, err := a.TG.SendAlbum(context.Background(), c.BackupChatID, 55, []string{first.Path, second.Path}, card.Text, card.Entities); err != nil {
		t.Fatal(err)
	}
	if _, err := a.TG.SendPhotoCard(context.Background(), c.BackupChatID, 55, first.Path, card.Text, nil, card.Entities); err != nil {
		t.Fatal(err)
	}
	before := calls
	if _, err := a.TG.SendPhotoCard(context.Background(), c.BackupChatID, 55, first.Path, "a", nil, []MessageEntity{{Type: "expandable_blockquote", Offset: 1, Length: 2}}); err == nil || calls != before {
		t.Fatal("out-of-range entity reached network")
	}
}

func TestFullTextIncludesMetadataAsExpandableQuote(t *testing.T) {
	s, p := cardFixture(t, 1)
	job := photoPresentationSource(t, 0, 2, 3)
	data, err := os.ReadFile(job.Path)
	if err != nil {
		t.Fatal(err)
	}
	job.Path = filepath.Join(s.Config.DataDir, "media", job.SHA)
	if err = os.WriteFile(job.Path, data, 0600); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.Store.MediaJobs(context.Background(), 1, p.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.MediaDone(context.Background(), jobs[0], job.SHA, job.Path, "image/png", job.Size); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var payload struct {
			Text     string          `json:"text"`
			Entities []MessageEntity `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(payload.Text, "Фото 1: FileType: PNG · ImageWidth: 2 · ImageHeight: 3") || len(payload.Entities) != 1 || filepath.Base(r.URL.Path) != "sendMessage" {
			t.Fatalf("full safe text missing metadata: %+v", payload)
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1,"chat":{"id":1}}}`)
	})}
	s.fullText(context.Background(), 1, p)
	if calls != 1 {
		t.Fatal("full metadata was not delivered")
	}
}
