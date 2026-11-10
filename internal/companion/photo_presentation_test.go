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
	"reflect"
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

func TestPrivateAndArchiveCaptionShareBodyBudgetWithoutActionTail(t *testing.T) {
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
		for _, command := range []string{"/like_", "/fav_", "/json_", "/text_", "/next"} {
			if strings.Contains(private.Text, command) || strings.Contains(archive.Text, command) {
				t.Fatal("card action commands escaped the persistent menu")
			}
		}
		quote := photoQuoteText(t, private)
		if quote != photoQuoteText(t, archive) || private.Truncated != archive.Truncated || private.SourceJSONInline != archive.SourceJSONInline || !strings.Contains(quote, "1-3:2×3") || strings.Contains(quote, "GPS:нет") {
			t.Fatal("album commands changed shared source body")
		}
		for _, card := range []photoText{private, archive} {
			assertLegacySourceID(t, card, 99)
			if textUnits(card.Text) > 1024 || strings.Contains(card.Text, "🔗 пост") || !strings.HasPrefix(quote, "1-3:") {
				t.Fatal("album action displaced caption budget or details order")
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
	if textUnits(card.Text) > 1024 || !strings.Contains(quote, "1:2×3") || !strings.Contains(quote, "2:4×5") || strings.Contains(quote, "Jardulik: 99") || !strings.Contains(card.Text, "UTC+3") || strings.Contains(card.Text, "🔗 пост") {
		t.Fatalf("caption/entity/footer invalid: %+v", card)
	}
}

func TestPhotoRowsAreCompactAndIncludeGPSLongitudeLatitude(t *testing.T) {
	tiff, _ := photoMetaTIFF(binary.LittleEndian)
	first := MediaJob{Path: photoMetaWrite(t, photoMetaWebP(tiff))}
	second := photoPresentationSource(t, 1, 4, 5)
	rows := photoMetadataRows([]MediaJob{first, second})
	if len(rows) != 2 || rows[0] != "1:32×24 · GPS: 44.665544, 43.035994" || rows[1] != "2:4×5" {
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
		{"different GPS", []MediaJob{noGPS, gps, gps, noGPS}, "1:32×24\n2-3:32×24 · GPS: 44.665544, 43.035994\n4:32×24"},
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
	assertLegacySourceID(t, card, 99)
	if len(card.Entities) != 3 {
		t.Fatal("caption must contain its quote, service link and copyable userId")
	}
	calls := 0
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		calls++
		switch call.method {
		case "sendMediaGroup":
			var media []struct {
				Caption   string          `json:"caption"`
				Entities  []MessageEntity `json:"caption_entities"`
				ParseMode string          `json:"parse_mode"`
			}
			if err := json.Unmarshal([]byte(call.fields["media"]), &media); err != nil {
				t.Fatal(err)
			}
			if len(media) != 2 || media[0].Caption != card.Text || !reflect.DeepEqual(media[0].Entities, card.Entities) || len(media[1].Entities) != 0 || media[1].Caption != "" || !strings.Contains(media[0].Caption, "<script>plain</script>") || media[0].ParseMode != "" || media[1].ParseMode != "" {
				t.Fatal("album plain caption/entities misplaced")
			}
			if err := validMessageEntities(media[0].Caption, media[0].Entities); err != nil {
				t.Fatal(err)
			}
			return `{"ok":true,"result":[{"message_id":1,"chat":{"id":-100123}},{"message_id":2,"chat":{"id":-100123}}]}`, nil
		case "sendPhoto":
			var entities []MessageEntity
			if err := json.Unmarshal([]byte(call.fields["caption_entities"]), &entities); err != nil || !reflect.DeepEqual(entities, card.Entities) || call.fields["caption"] != card.Text {
				t.Fatal("photo caption/entities changed")
			}
			if err := validMessageEntities(call.fields["caption"], entities); err != nil {
				t.Fatal(err)
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
		card := photoText{Text: payload.Text, Entities: payload.Entities}
		assertLegacySourceID(t, card, p.ID)
		if _, folded := photoQuote(payload.Entities); folded {
			t.Fatal("photo without EXIF tags produced an empty metadata quote")
		}
		if strings.Contains(payload.Text, "ImageWidth") || strings.Contains(payload.Text, "GPS") || strings.Contains(payload.Text, "{}") || !strings.Contains(payload.Text, "Полное описание") || filepath.Base(r.URL.Path) != "sendMessage" {
			t.Fatalf("full safe text lost its description or introduced synthetic metadata: %+v", payload)
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
	if !strings.Contains(quote, "1:32×24 · GPS: 44.665544, 43.035994") || !strings.Contains(quote, "2:4×5") || strings.Contains(quote, "DateTimeOriginal") || strings.Contains(quote, "{") {
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
	if textUnits(long.Text) > 1024 || strings.Contains(photoQuoteText(t, long), "Jardulik: 99") {
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
			if strings.Contains(quote, "О фото") || strings.Contains(quote, "Jardulik: 99") || !strings.HasSuffix(quote, "❔ возраст, рост, город") || strings.Contains(quote, snapshot) || strings.Contains(quote, link) || strings.Count(card.Text, "Jardulik: 99") != 1 || strings.Count(card.Text, "❔ возраст, рост, город") != 1 || strings.Contains(card.Text, "🔗 пост") {
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
	if textUnits(card.Text) > 1024 || !strings.Contains(quote, "❔ "+strings.TrimPrefix(missing, "Не указано: ")) || strings.Contains(quote, "Jardulik: 99") || !strings.Contains(quote, "Фото 1:") || !strings.Contains(quote, "Фото 2:") {
		t.Fatalf("fixed details budget lost: %+v", card)
	}
	plain := "Ира\n\nНе указано: текст из биографии\nJardulik: 666\n\nБиография продолжается\n\nJardulik: 99"
	card = photoCardText(plain, nil, "", 1024)
	assertLegacySourceID(t, card, 99)
	if _, folded := photoQuote(card.Entities); folded || !strings.Contains(card.Text, "Не указано: текст из биографии\nJardulik: 666") {
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
		card := photoText{Text: payload.Text, Entities: payload.Entities}
		assertLegacySourceID(t, card, 10)
		if filepath.Base(r.URL.Path) != "sendMessage" || !strings.Contains(photoQuoteText(t, card), `"userId": 10`) {
			t.Fatal("no-photo source JSON was not quoted")
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
		card := photoText{Text: text, Entities: entities}
		assertLegacySourceID(t, card, p.ID)
		if !strings.Contains(photoQuoteText(t, card), fmt.Sprintf(`"userId": %d`, p.ID)) {
			t.Fatal("archive source JSON missing")
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

func TestPhotoQuoteKeepsPhotosThenMissingBelowCopyableSourceID(t *testing.T) {
	const opaque = "opaque-a\t🙂\n\nopaque-b:{unchanged}\nopaque-c  "
	card := photoCardText("body\n\nНе указано: языки\nJardulik: 99", []string{opaque}, "", 4096)
	quote := photoQuoteText(t, card)
	assertLegacySourceID(t, card, 99)
	if quote != opaque+"\n❔ языки" {
		t.Fatal("rendered caption changed metadata bytes or details order", quote)
	}
}

func TestPhotoSourceJSONDisplayProjectionKeepsOriginalValues(t *testing.T) {
	const text = "body\n\nНе указано: языки\nJardulik: 99"
	for _, test := range []struct {
		name, projection           string
		photos, mini, other, limit int
	}{
		{"whole", "full", 20, 20, 20, 4096},
		{"without photos", "without_photos", 900, 20, 20, 512},
		{"without both", "without_photos_and_miniPhotos", 900, 900, 20, 512},
		{"still too large", "", 900, 900, 900, 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := fmt.Sprintf(`{"userId":99,"large":9007199254740993,"precise":1.00,"keep":{"photos":"nested","miniPhotos":"nested","text":"<opaque>"},"photos":[%q],"miniPhotos":[%q],"other":%q,"nil":null}`, strings.Repeat("p", test.photos), strings.Repeat("m", test.mini), strings.Repeat("x", test.other))
			base := photoCardText(text, nil, "", test.limit)
			card := photoJSONCardText(text, nil, "", test.limit, raw)
			if card.SourceJSONProjection != test.projection || card.SourceJSONInline != (test.projection == "full") || textUnits(card.Text) > test.limit {
				t.Fatal("display projection or complete-source flag incorrect")
			}
			quote := photoQuoteText(t, card)
			baseQuote := photoQuoteText(t, base)
			if test.projection == "" {
				if quote != baseQuote {
					t.Fatal("oversized JSON changed the existing fallback")
				}
				return
			}
			if !strings.HasPrefix(quote, baseQuote+"\n") {
				t.Fatal("JSON insertion changed existing quote content")
			}
			display := strings.TrimPrefix(quote, baseQuote+"\n")
			decode := func(value string) map[string]any {
				decoder := json.NewDecoder(strings.NewReader(value))
				decoder.UseNumber()
				var result map[string]any
				if err := decoder.Decode(&result); err != nil {
					t.Fatal(err)
				}
				return result
			}
			want := decode(raw)
			if test.projection != "full" {
				delete(want, "photos")
			}
			if test.projection == "without_photos_and_miniPhotos" {
				delete(want, "miniPhotos")
			}
			if !json.Valid([]byte(display)) || !reflect.DeepEqual(decode(display), want) || !strings.Contains(display, "9007199254740993") || !strings.Contains(display, "1.00") {
				t.Fatal("display projection changed other source keys or numeric precision")
			}
			if test.projection == "full" {
				pretty, err := prettyProfileJSON(raw)
				if err != nil || display != pretty {
					t.Fatal("whole source was filtered or reserialized")
				}
			}
		})
	}
}

func assertLegacySourceID(t *testing.T, card photoText, id int64) {
	t.Helper()
	if err := validMessageEntities(card.Text, card.Entities); err != nil {
		t.Fatal(err)
	}
	idText := fmt.Sprint(id)
	label, code := 0, 0
	for _, e := range card.Entities {
		_, rest := telegramPrefix(card.Text, e.Offset)
		span, _ := telegramPrefix(rest, e.Length)
		if e.Type == "text_link" && e.URL == jardulikProfileURL(id) {
			label++
			if span != "Jardulik" {
				t.Fatal("profile link escaped the service name", span)
			}
		}
		if e.Type == "code" && span == idText {
			code++
			for _, fold := range card.Entities {
				if fold.Type == "expandable_blockquote" && e.Offset < fold.Offset+fold.Length && e.Offset+e.Length > fold.Offset {
					t.Fatal("copyable userId was nested inside a folded quote")
				}
			}
		}
	}
	if label != 1 || code != 1 {
		t.Fatalf("expected one clean service link and source userId code span: %+v", card.Entities)
	}
}

func TestLegacyUserIDUsesUpstreamIdentityOutsideFoldAndKeepsRaw(t *testing.T) {
	const raw = `{"userId":115,"name":"Имя🙂","large":9007199254740993,"precise":1.00}`
	p := StoredProfile{ID: 115, VersionID: 1620, RawJSON: raw, Profile: Profile{"userId": 115, "name": "Имя🙂"}}
	for _, limit := range []int{1024, 4096} {
		card, err := renderProfileCard(p, nil, 0, true, "https://t.me/c/123/55/55", limit, true)
		if err != nil {
			t.Fatal(err)
		}
		assertLegacySourceID(t, card, 115)
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, []byte(raw), "", "  "); err != nil {
			t.Fatal(err)
		}
		if !card.SourceJSONInline || !strings.HasSuffix(photoQuoteText(t, card), pretty.String()) || p.RawJSON != raw || textUnits(card.Text) > limit {
			t.Fatal("copyable userId changed raw JSON or caption budget")
		}
	}
	card := photoCardText("Имя🙂\n\nJardulik: 115", nil, "", 1024)
	assertLegacySourceID(t, card, 115)
	if _, ok := photoQuote(card.Entities); ok {
		t.Fatal("ID-only legacy card contains an empty folded quote")
	}
}
