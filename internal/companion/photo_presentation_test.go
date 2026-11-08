package companion

import (
	"bytes"
	"context"
	"encoding/binary"
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

func TestPrivateAlbumLikeTailSharesBodyBudget(t *testing.T) {
	for _, long := range []bool{false, true} {
		p := StoredProfile{ID: 99, VersionID: 123, RawJSON: `{"userId":99,"name":"Ира🙂","large":9007199254740993}`, Profile: Profile{"name": "Ира🙂"}}
		if long {
			p.Profile["description"] = strings.Repeat("Биография🙂 ", 300)
		}
		photos := []MediaJob{photoPresentationSource(t, 0, 2, 3), photoPresentationSource(t, 1, 2, 3), photoPresentationSource(t, 2, 2, 3)}
		private, err := renderProfileCard(p, photos, 0, false, "https://t.me/c/123/55", 1024, true)
		if err != nil {
			t.Fatal(err)
		}
		archive, err := renderProfileCard(p, photos, 0, false, "https://t.me/c/123/55", 1024, false)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(private.Text, "❤️ /like_99 · Избранное: /fav_99") || !strings.Contains(private.Text, "/json_123") || strings.Contains(archive.Text, "/like_") {
			t.Fatal("album command identity or private scope lost")
		}
		quote := photoQuoteText(t, private)
		if quote != photoQuoteText(t, archive) || private.Truncated != archive.Truncated || private.SourceJSONInline != archive.SourceJSONInline || !strings.Contains(quote, "1-3:2×3") || strings.Contains(quote, "GPS:нет") {
			t.Fatal("album commands changed shared source body")
		}
		for _, card := range []photoText{private, archive} {
			if textUnits(card.Text) > 1024 || !strings.HasSuffix(card.Text, "🔗 пост") {
				t.Fatal("album action displaced caption budget or footer")
			}
			if err := validMessageEntities(card.Text, card.Entities); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestProfileSnapshotDateStableAcrossRepeatedObservations(t *testing.T) {
	p := StoredProfile{ID: 99, VersionID: 123, RawJSON: `{"userId":99,"name":"Ира🙂"}`, Profile: Profile{"name": "Ира🙂"}, FirstSeen: "2026-11-07 22:10:00", LastSeen: "2026-11-07 22:10:00"}
	for _, private := range []bool{false, true} {
		before, err := renderProfileCard(p, nil, 0, true, "https://t.me/c/123/55", 4096, private)
		if err != nil {
			t.Fatal(err)
		}
		observed := p
		observed.LastSeen = "2026-11-10 12:30:00"
		after, err := renderProfileCard(observed, nil, 0, true, "https://t.me/c/123/55", 4096, private)
		if err != nil {
			t.Fatal(err)
		}
		if before.Text != after.Text || !strings.Contains(after.Text, "8.11.26, 01:10 (вс, UTC+3)") {
			t.Fatal("repeated observation changed the snapshot date")
		}
		if observed.LastSeen != "2026-11-10 12:30:00" || observed.RawJSON != p.RawJSON {
			t.Fatal("rendering changed source or observation history")
		}
	}
	p.FirstSeen = ""
	if !strings.Contains(profileText(p), displayTime(p.LastSeen)) {
		t.Fatal("legacy snapshot without FirstSeen lost its date")
	}
}

func photoQuoteText(t *testing.T, value photoText) string {
	t.Helper()
	e, ok := photoQuote(value.Entities)
	if !ok {
		t.Fatalf("expected one collapsed quote: %+v", value.Entities)
	}
	if err := validMessageEntities(value.Text, value.Entities); err != nil {
		t.Fatal(err)
	}
	text := utf16.Encode([]rune(value.Text))
	return string(utf16.Decode(text[e.Offset : e.Offset+e.Length]))
}

func TestPhotoCaptionQuotePreservesFooterAndSafeNumberedOrder(t *testing.T) {
	first := photoPresentationSource(t, 0, 2, 3)
	second := photoPresentationSource(t, 1, 4, 5)
	rows := photoMetadataRows(selectedProfilePhotos([]MediaJob{second, first}))
	if len(rows) != 2 || rows[0] != "1:2×3" || rows[1] != "2:4×5" {
		t.Fatalf("metadata order/format incorrect: %v", rows)
	}
	text := strings.Repeat("🙂<blockquote>О фото</blockquote>", 300) + "\n\n8.11.26, 01:10 (вс, UTC+3)\nJardulik: 99\n📍 https://t.me/c/123/55"
	card := photoCardText(text, rows, "\nТекст: /text_99", 1024)
	quote := photoQuoteText(t, card)
	if textUnits(card.Text) > 1024 || !strings.Contains(quote, "1:2×3") || !strings.Contains(quote, "2:4×5") || !strings.Contains(quote, "Jardulik: 99") || !strings.Contains(card.Text, "Jardulik: 99") || !strings.Contains(card.Text, "UTC+3") || !strings.HasSuffix(card.Text, "🔗 пост") {
		t.Fatalf("caption/entity/footer invalid: %+v", card)
	}
}

func TestPhotoRowsAreCompactAndGPSIsOnlyAFlag(t *testing.T) {
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	first := MediaJob{Path: photoMetaWrite(t, photoMetaWebP(tiff))}
	second := photoPresentationSource(t, 1, 4, 5)
	rows := photoMetadataRows([]MediaJob{first, second})
	if len(rows) != 2 || rows[0] != "1:32×24 · GPS:есть" || rows[1] != "2:4×5" {
		t.Fatalf("compact photo rows incorrect: %v", rows)
	}
}

func TestPhotoRowsCollapseOnlyConsecutiveEqualSummaries(t *testing.T) {
	plain := photoPresentationSource(t, 0, 900, 1200)
	other := photoPresentationSource(t, 0, 4, 5)
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	gps := MediaJob{Path: photoMetaWrite(t, photoMetaWebP(tiff))}
	noGPS := photoPresentationSource(t, 0, 32, 24)
	missing := MediaJob{Path: filepath.Join(t.TempDir(), "missing")}
	for _, test := range []struct {
		name   string
		photos []MediaJob
		want   string
	}{
		{"same three", []MediaJob{plain, plain, plain}, "1-3:900×1200"},
		{"disjoint", []MediaJob{plain, other, plain, plain}, "1:900×1200\n2:4×5\n3-4:900×1200"},
		{"different GPS", []MediaJob{noGPS, gps, gps, noGPS}, "1:32×24\n2-3:32×24 · GPS:есть\n4:32×24"},
		{"one", []MediaJob{plain}, "1:900×1200"},
		{"unavailable metadata", []MediaJob{missing}, "1:"},
		{"no photos", nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := strings.Join(photoMetadataRows(test.photos), "\n"); got != test.want {
				t.Fatalf("photo rows = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPhotoRowsDoNotClaimNoGPSWhenMetadataIsUnavailable(t *testing.T) {
	photos := []MediaJob{{Path: filepath.Join(t.TempDir(), "missing")}, {Path: photoMetaWrite(t, photoMetaWebP([]byte("invalid TIFF")))}}
	rows := photoMetadataRows(photos)
	if len(rows) != 2 || rows[0] != "1:" || !strings.HasPrefix(rows[1], "2:") {
		t.Fatalf("partial metadata lost: %v", rows)
	}
	for _, row := range rows {
		if strings.Contains(row, "GPS") {
			t.Fatal("unknown GPS was invented")
		}
	}
}

func TestPhotoQuoteClippingKeepsGPSAtEndOfEveryRow(t *testing.T) {
	var rows []string
	for i, gps := range []string{"есть", "нет", "есть", "нет"} {
		rows = append(rows, fmt.Sprintf("%d: %s · GPS:%s", i+1, strings.Repeat("Модель🙂 ", 40), gps))
	}
	quote := compactPhotoRows(rows, 180)
	if textUnits(quote) > 180 {
		t.Fatal("compact photo quote exceeded its UTF-16 budget")
	}
	lines := strings.Split(quote, "\n")
	if len(lines) != 4 {
		t.Fatal("photo quote lost a row")
	}
	for i, gps := range []string{"есть", "нет", "есть", "нет"} {
		if !strings.HasPrefix(lines[i], fmt.Sprintf("%d:", i+1)) || !strings.HasSuffix(lines[i], " · GPS:"+gps) {
			t.Fatalf("clipping lost photo number or GPS: %s", lines[i])
		}
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
		if strings.Contains(payload.Text, "ImageWidth") || strings.Contains(payload.Text, "GPS") || strings.Contains(payload.Text, "{}") || !strings.Contains(payload.Text, "Полное описание") || len(payload.Entities) != 1 || filepath.Base(r.URL.Path) != "sendMessage" {
			t.Fatalf("full safe text missing metadata: %+v", payload)
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1,"chat":{"id":1}}}`)
	})}
	s.fullText(context.Background(), 1, p)
	if calls != 1 {
		t.Fatal("full metadata was not delivered")
	}
}

func TestCaptionIsCompactWhileFullEXIFContainsOnlySourceTags(t *testing.T) {
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	first := MediaJob{Role: "photos", Position: 0, State: "complete", SHA: "test", Path: photoMetaWrite(t, photoMetaWebP(tiff))}
	photos := selectedProfilePhotos([]MediaJob{photoPresentationSource(t, 1, 4, 5), first})
	const footer = "\n\nНе указано: рост\n8.11.26, 01:10 (вс, UTC+3)\nJardulik: 99\n📍 https://t.me/c/123/55"
	card := photoJSONCardText("Ира, 25"+footer, photos, "\nТекст: /text_99", 1024)
	quote := photoQuoteText(t, card)
	if !strings.Contains(quote, "1:32×24 · GPS:есть") || !strings.Contains(quote, "2:4×5") || strings.Contains(quote, "DateTimeOriginal") || strings.Contains(quote, "{") {
		t.Fatalf("main caption contains synthesized JSON: %s", quote)
	}
	blocks := photoExifBlocks(photos)
	if len(blocks) != 1 || !strings.HasPrefix(blocks[0], "1:\n{") {
		t.Fatalf("empty EXIF was not omitted: %v", blocks)
	}
	var values map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(blocks[0], "1:\n")), &values); err != nil {
		t.Fatal(err)
	}
	if values["Make"] != "TestMake" || values["DateTimeOriginal"] != "2021:08:09 10:11:12" || values["DateTimeDigitized"] != "2021:08:09 10:11:14" || values["DateTime"] != "2022:09:10 11:12:13" {
		t.Fatalf("actual EXIF values changed: %v", values)
	}
	for _, key := range []string{"Фото", "ImageWidth", "ImageHeight", "GPS", "FileType", "has_exif", "has_icc"} {
		if _, ok := values[key]; ok {
			t.Fatalf("synthetic EXIF key: %s", key)
		}
	}
	long := photoJSONCardText(strings.Repeat("🙂Биография ", 150)+footer, photos, "", 1024)
	if textUnits(long.Text) > 1024 || !strings.HasSuffix(photoQuoteText(t, long), "Jardulik: 99") {
		t.Fatal("caption budget/details lost")
	}
}

func TestPhotoEXIFSkipsEmptyAndUnavailableMetadata(t *testing.T) {
	photos := []MediaJob{photoPresentationSource(t, 0, 4, 5), {Path: filepath.Join(t.TempDir(), "missing")}}
	if blocks := photoExifBlocks(photos); len(blocks) != 0 {
		t.Fatalf("empty technical JSON noise emitted: %v", blocks)
	}
}

func TestPhotoQuoteContainsDetailsWhileSnapshotAndLinkStayOutside(t *testing.T) {
	const snapshot = "8.11.26, 01:10 (вс, UTC+3)"
	const link = "https://t.me/c/123/55"
	text := "Ира🙂\n\nБиография\n\nНе указано: возраст, рост, город\n" + snapshot + "\nJardulik: 99\n📍 " + link
	for _, rows := range [][]string{nil, {"Фото 1:2×3 · GPS: нет", "Фото 2:4×5 · GPS: есть"}} {
		for _, full := range []bool{false, true} {
			card := photoCardText(text, rows, "", 1024)
			if full {
				card = fullPhotoText(text, rows)
			}
			quote := photoQuoteText(t, card)
			if strings.Contains(quote, "О фото") || !strings.HasSuffix(quote, "Не указано: возраст, рост, город\nJardulik: 99") || strings.Contains(quote, snapshot) || strings.Contains(quote, link) || strings.Count(card.Text, "Jardulik: 99") != 1 || strings.Count(card.Text, "Не указано:") != 1 || !strings.HasSuffix(card.Text, "🔗 пост") {
				t.Fatalf("details/outer footer misplaced: %+v", card)
			}
		}
	}
}

func TestPhotoQuoteDetailsSurviveLongBioManyMissingAndPlainLookalikes(t *testing.T) {
	missing := "Не указано: фамилия, возраст, рост, страна, город, цель знакомства, мировоззрение, дети, занятие, языки, родственные фамилии, о себе"
	text := strings.Repeat("🙂Биография\n", 500) + "\n\n" + missing + "\n8.11.26, 01:10 (вс, UTC+3)\nJardulik: 99"
	rows := []string{"Фото 1: " + strings.Repeat("Модель ", 100) + " · GPS: нет", "Фото 2: " + strings.Repeat("Модель ", 100) + " · GPS: есть"}
	card := photoCardText(text, rows, "\n📍 https://t.me/c/123/55", 1024)
	quote := photoQuoteText(t, card)
	if textUnits(card.Text) > 1024 || !strings.Contains(quote, missing) || !strings.HasSuffix(quote, "Jardulik: 99") || !strings.Contains(quote, "Фото 1:") || !strings.Contains(quote, "Фото 2:") {
		t.Fatalf("fixed details budget lost: %+v", card)
	}
	plain := "Ира\n\nНе указано: текст из биографии\nJardulik: 666\n\nБиография продолжается\n\nJardulik: 99"
	card = photoCardText(plain, nil, "", 1024)
	quote = photoQuoteText(t, card)
	if quote != "Jardulik: 99" || !strings.Contains(card.Text, "Не указано: текст из биографии\nJardulik: 666") {
		t.Fatalf("biography lookalike was parsed as generated details: %+v", card)
	}
}

func TestNoPhotoPrivateCardUsesTextEntity(t *testing.T) {
	s, p := cardFixture(t, 0)
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text     string          `json:"text"`
			Entities []MessageEntity `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if filepath.Base(r.URL.Path) != "sendMessage" || !strings.HasPrefix(photoQuoteText(t, photoText{Text: payload.Text, Entities: payload.Entities}), "Jardulik: 10") {
			t.Fatal("no-photo details were not quoted")
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1,"chat":{"id":1}}}`)
	})}
	if _, err := s.sendProfileCard(context.Background(), 1, p); err != nil {
		t.Fatal(err)
	}
}

func TestNoPhotoArchiveCardUsesTextEntityAndRefreshMethod(t *testing.T) {
	c, _, a, p, _ := archivePresentationFixture(t)
	var methods []string
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		methods = append(methods, call.method)
		raw, _ := json.Marshal(call.json["entities"])
		var entities []MessageEntity
		if err := json.Unmarshal(raw, &entities); err != nil {
			t.Fatal(err)
		}
		text, _ := call.json["text"].(string)
		if !strings.Contains(photoQuoteText(t, photoText{Text: text, Entities: entities}), fmt.Sprintf("Jardulik: %d", p.ID)) {
			t.Fatal("archive text details missing")
		}
		return `{"ok":true,"result":{"message_id":101,"chat":{"id":-100123}}}`, nil
	})
	if _, err := a.sendProfileCard(context.Background(), p, nil, 55, func([]int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	card, err := a.profileCardContent(p, 0, 55, 4096, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.editKnownText(context.Background(), 101, card); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(methods) != "[sendMessage editMessageText]" {
		t.Fatalf("no-photo card used caption-only API: %v", methods)
	}
}
