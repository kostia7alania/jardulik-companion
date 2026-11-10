package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func richFixture(t *testing.T, n int) (StoredProfile, []MediaJob) {
	t.Helper()
	raw := `{"userId":77,"name":"Имя","familyNames":"Фамилия","languages":"","description":"Полное описание <tag> 🌄","unknown_source":{"keep":true},"photos":["https://example.invalid/1"]}`
	var profile Profile
	_ = json.Unmarshal([]byte(raw), &profile)
	p := StoredProfile{ID: 77, VersionID: 7, RawJSON: raw, Profile: profile, FirstSeen: "2026-11-08 12:00:00"}
	var jobs []MediaJob
	for i := 0; i < n; i++ {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("p%d.jpg", i))
		if err := os.WriteFile(path, []byte("photo-fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, MediaJob{OwnerID: 1, VersionID: 7, ProfileID: 77, Position: i, Role: "photos", State: "complete", SHA: fmt.Sprint(i), Path: path, Size: 13, MIME: "image/jpeg"})
	}
	return p, jobs
}
func richAck(n int) json.RawMessage {
	var photos []any
	for i := 0; i < n; i++ {
		photos = append(photos, map[string]any{"type": "photo", "photo": []any{map[string]any{"file_id": fmt.Sprintf("small%d", i), "width": 1, "height": 1}, map[string]any{"file_id": fmt.Sprintf("large%d", i), "width": 2, "height": 2}}})
	}
	raw, _ := json.Marshal(map[string]any{"blocks": []any{map[string]any{"type": "collage", "blocks": photos}}})
	return raw
}

func richProfileTextValue(t *testing.T, value any) string {
	t.Helper()
	switch value := value.(type) {
	case string:
		return value
	case []any:
		var text strings.Builder
		for _, part := range value {
			text.WriteString(richProfileTextValue(t, part))
		}
		return text.String()
	case map[string]any:
		if value["type"] == "button" {
			return richProfileTextValue(t, value["button"].(map[string]any)["text"])
		}
		return richProfileTextValue(t, value["text"])
	default:
		t.Fatalf("unexpected rich text: %T", value)
		return ""
	}
}

func assertRichProfileCaptions(t *testing.T, content map[string]any, photos int, body string) {
	t.Helper()
	blocks := content["blocks"].([]any)
	if len(blocks) != 3 {
		t.Fatal("profile body duplicated as a separate block")
	}
	head := blocks[0].(map[string]any)
	if photos == 0 {
		if head["type"] != "paragraph" || richProfileTextValue(t, head["text"]) != body {
			t.Fatal("human body differs from shared renderer")
		}
		return
	}
	want := "photo"
	if photos > 1 {
		want = "collage"
	}
	if head["type"] != want || richProfileTextValue(t, head["caption"].(map[string]any)["text"]) != body {
		t.Fatal("missing visible media caption", head["type"])
	}
	media := []any{head}
	if photos > 1 {
		media = head["blocks"].([]any)
	}
	if len(media) != photos {
		t.Fatal("collage lost photos")
	}
	for _, item := range media {
		photo := item.(map[string]any)
		if photo["type"] != "photo" || richProfileTextValue(t, photo["caption"].(map[string]any)["text"]) != body {
			t.Fatal("photo viewer lost its caption")
		}
		if _, nested := photo["photo"].(map[string]any)["caption"]; nested {
			t.Fatal("caption placed in ignored InputMediaPhoto field")
		}
	}
}

func TestRichProfileSourceBodyAndAllMediaShapes(t *testing.T) {
	for _, n := range []int{0, 1, 2, 4, 50} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p, jobs := richFixture(t, n)
			card, err := BuildRichProfileCard(p, jobs, "https://t.me/c/1234/77")
			if err != nil {
				t.Fatal(err)
			}
			blocks := card.Content["blocks"].([]any)
			body, _, _, _ := splitPhotoFooter(profileText(p))
			assertRichProfileCaptions(t, card.Content, n, body)
			quote := blocks[1].(map[string]any)
			parts := quote["text"].([]any)
			if quote["type"] != "expandable_blockquote" || !strings.HasPrefix(richProfileTextValue(t, parts), "Jardulik: 77\n") || !strings.Contains(richProfileTextValue(t, parts), "❔ языки\n") {
				t.Fatal("fold order/shape changed")
			}
			button := parts[2].(map[string]any)["button"].(map[string]any)
			if button["text"] != "77" || button["copy_text"].(map[string]any)["text"] != "77" {
				t.Fatal("source ID lost inline copy action")
			}
			code := parts[len(parts)-1].(map[string]any)
			var pretty bytes.Buffer
			_ = json.Indent(&pretty, []byte(p.RawJSON), "", "  ")
			if code["type"] != "code" || code["text"] != pretty.String() {
				t.Fatal("source JSON omitted/changed fields")
			}
			bodyCopies := 1
			if n > 1 {
				bodyCopies += n
			}
			footerText := richProfileTextValue(t, blocks[2].(map[string]any)["text"])
			if strings.Contains(footerText, "пост") {
				t.Fatal("redundant post footer returned")
			}
			otherText := richProfileTextValue(t, parts) + footerText
			if card.JSONBytes != pretty.Len() || card.JSONCharacters != utf8.RuneCount(pretty.Bytes()) ||
				card.TextBytes != bodyCopies*len(body)+len(otherText) ||
				card.TextCharacters != bodyCopies*utf8.RuneCountInString(body)+utf8.RuneCountInString(otherText) {
				t.Fatal("full text metrics omit captions")
			}
			refs := make([]string, n)
			for i := range refs {
				refs[i] = fmt.Sprintf("confirmed%d", i)
			}
			edit, err := card.WithPhotoFileIDs(refs)
			if err != nil {
				t.Fatal(err)
			}
			assertRichProfileCaptions(t, edit, n, body)
			encoded, _ := json.Marshal(edit)
			original, _ := json.Marshal(card.Content)
			if bytes.Contains(encoded, []byte("attach://p")) || n > 0 && !bytes.Contains(original, []byte("attach://p0")) {
				t.Fatal("edit uploads media or mutated original")
			}
			if n > 0 {
				if _, err = card.WithPhotoFileIDs(refs[:n-1]); err == nil {
					t.Fatal("edit accepted partial media receipt")
				}
			}
		})
	}
}

func TestRichProfileHeadingLinksAndSourceIDCopiesInsideDetails(t *testing.T) {
	p, jobs := richFixture(t, 2)
	p.Profile["gender"], p.Profile["age"], p.Profile["height"] = 2, 28, 167
	p.Profile["birthday"], p.Profile["type"] = "1998-07-04T00:00:00", 1
	raw, _ := json.Marshal(p.Profile)
	p.RawJSON = string(raw)
	const link = "https://t.me/c/1234/77/77"
	card, err := BuildRichProfileCard(p, jobs, link)
	if err != nil {
		t.Fatal(err)
	}
	blocks := card.Content["blocks"].([]any)
	collage := blocks[0].(map[string]any)
	media := append([]any{collage}, collage["blocks"].([]any)...)
	for _, item := range media {
		parts := item.(map[string]any)["caption"].(map[string]any)["text"].([]any)
		heading := parts[0].(map[string]any)
		if heading["type"] != "url" || heading["url"] != link || heading["text"] != "💁‍♀️Имя, 28/167, ♋️ ⭐️" {
			t.Fatal("photo caption did not keep its linked neutral heading", heading)
		}
	}
	quote := blocks[1].(map[string]any)
	parts := quote["text"].([]any)
	serviceLink := parts[0].(map[string]any)
	if serviceLink["type"] != "url" || serviceLink["text"] != "Jardulik" || serviceLink["url"] != jardulikProfileURL(p.ID) || parts[1] != ": " || parts[2].(map[string]any)["type"] != "button" {
		t.Fatal("clean service link and copyable source ID must be the first quoted detail")
	}
	button := parts[2].(map[string]any)["button"].(map[string]any)
	if button["text"] != "77" || button["copy_text"].(map[string]any)["text"] != "77" {
		t.Fatal("inline ID copy changed the source ID")
	}
	quoteText := richProfileTextValue(t, parts)
	if !strings.HasPrefix(quoteText, "Jardulik: 77\n1-2:") || !strings.Contains(quoteText, "\n❔ языки\n") {
		t.Fatal("ID/photo/missing order changed", quoteText)
	}
	var pretty bytes.Buffer
	_ = json.Indent(&pretty, raw, "", "  ")
	if parts[len(parts)-1].(map[string]any)["text"] != pretty.String() || p.RawJSON != string(raw) {
		t.Fatal("display changed source JSON")
	}
	if strings.Contains(richProfileTextValue(t, blocks[2].(map[string]any)["text"]), "пост") {
		t.Fatal("redundant footer link returned")
	}
}

func TestRichProfileLimitsAreTypedBeforeHTTP(t *testing.T) {
	p, jobs := richFixture(t, 0)
	p.Profile["description"] = strings.Repeat("я", 17000)
	raw, _ := json.Marshal(p.Profile)
	p.RawJSON = string(raw)
	if _, err := BuildRichProfileCard(p, jobs, ""); !errors.Is(err, ErrRichCardTooLarge) {
		t.Fatal("total text not bounded", err)
	}
	p, jobs = richFixture(t, 4)
	p.Profile["description"] = strings.Repeat("я", 6000)
	raw, _ = json.Marshal(p.Profile)
	p.RawJSON = string(raw)
	if _, err := BuildRichProfileCard(p, jobs, ""); !errors.Is(err, ErrRichCardTooLarge) {
		t.Fatal("repeated photo captions not bounded", err)
	}
	p, jobs = richFixture(t, 51)
	if _, err := BuildRichProfileCard(p, jobs, ""); !errors.Is(err, ErrRichCardTooLarge) {
		t.Fatal("media count not bounded", err)
	}
	p, jobs = richFixture(t, 1)
	jobs[0].Size = 10 << 20
	if _, err := BuildRichProfileCard(p, jobs, ""); !errors.Is(err, ErrRichCardTooLarge) {
		t.Fatal("photo size not bounded", err)
	}
	jobs[0].Size = 13
	jobs[0].ProfileID = 78
	if _, err := BuildRichProfileCard(p, jobs, ""); err == nil || errors.Is(err, ErrRichCardTooLarge) {
		t.Fatal("bad provenance permits legacy fallback", err)
	}
}
func TestRichProfileOneRequestReceiptAndNoRetry(t *testing.T) {
	for _, n := range []int{0, 1, 2} {
		for _, kind := range []string{"success", "wrongchat", "wrongthread", "partialmedia", "invalidbody", "reject"} {
			t.Run(fmt.Sprintf("%d/%s", n, kind), func(t *testing.T) {
				p, jobs := richFixture(t, n)
				card, err := BuildRichProfileCard(p, jobs, "")
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				tg := &Telegram{Base: "https://api.telegram.org", Token: "fixture", HTTP: &http.Client{Transport: handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					if r.Method != "POST" || r.URL.Path != "/botfixture/sendRichMessage" {
						t.Fatal("unexpected request")
					}
					if n == 0 {
						var payload map[string]any
						if json.NewDecoder(r.Body).Decode(&payload) != nil || payload["chat_id"] != float64(-100123) || payload["message_thread_id"] != float64(9) {
							t.Fatal("wrong JSON target")
						}
					} else {
						form, err := r.MultipartReader()
						if err != nil {
							t.Fatal(err)
						}
						fields := map[string][]byte{}
						for {
							part, err := form.NextPart()
							if err == io.EOF {
								break
							}
							if err != nil {
								t.Fatal(err)
							}
							fields[part.FormName()], _ = io.ReadAll(part)
						}
						if string(fields["chat_id"]) != "-100123" || string(fields["message_thread_id"]) != "9" || !json.Valid(fields["rich_message"]) {
							t.Fatal("wrong multipart target")
						}
						for i := 0; i < n; i++ {
							if string(fields[fmt.Sprintf("p%d", i)]) != "photo-fixture" {
								t.Fatal("photo bytes changed")
							}
						}
					}
					if kind == "invalidbody" {
						fmt.Fprint(w, "interrupted")
						return
					}
					if kind == "reject" {
						fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"fixture"}`)
						return
					}
					chat, thread, count := int64(-100123), int64(9), n
					if kind == "wrongchat" {
						chat = 1
					}
					if kind == "wrongthread" {
						thread = 10
					}
					if kind == "partialmedia" {
						count = n + 1
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 123, "chat": map[string]any{"id": chat}, "message_thread_id": thread, "rich_message": json.RawMessage(richAck(count))}})
				})}}}
				message, err := tg.SendRichProfileCard(context.Background(), -100123, 9, card, nil)
				if calls != 1 {
					t.Fatal("rich send retried")
				}
				if kind == "success" && (err != nil || message.ID != 123 || len(message.Rich) == 0) {
					t.Fatal("valid receipt rejected", err)
				}
				if kind != "success" && err == nil {
					t.Fatal("unconfirmed receipt accepted")
				}
				if (kind == "wrongchat" || kind == "wrongthread" || kind == "partialmedia") && message.ID != 123 {
					t.Fatal("known delivery ID lost")
				}
			})
		}
	}
}
func TestRichCardMarkerValidatedDurableAndExact(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		if err := store.User(ctx, owner, "fixture"); err != nil {
			t.Fatal(err)
		}
	}
	versions, err := store.ArchiveViewed(ctx, 1, []Profile{{"userId": 77, "photos": []any{"https://example.invalid/1", "https://example.invalid/2"}}}, "rich fixture")
	if err != nil {
		t.Fatal(err)
	}
	version := versions[0]
	if marker, err := loadRichCardMarker(ctx, store, -100123, 123); err != nil || marker != nil {
		t.Fatal("absent marker did not permit legacy path")
	}
	if err = recordRichCard(ctx, store, -100123, 123, 1, version, 9, richAck(2)); err == nil {
		t.Fatal("receipt accepted before own source media selection matches")
	}
	if _, err = store.DB.Exec("INSERT INTO media(sha,size,mime,path) VALUES('one',13,'image/jpeg','/fixture')"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("UPDATE media_jobs SET state='complete',sha='one' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if err = recordRichCard(ctx, store, -100123, 123, 1, version, 9, richAck(2)); err != nil {
		t.Fatal(err)
	}
	marker, err := loadRichCardMarker(ctx, store, -100123, 123)
	if err != nil || marker.OwnerID != 1 || marker.VersionID != version || marker.ThreadID != 9 || strings.Join(marker.PhotoFileIDs, ",") != "large0,large1" {
		t.Fatal("marker lost provenance/media receipt", marker, err)
	}
	if err = recordRichCard(ctx, store, -100123, 123, 1, version, 9, richAck(2)); err != nil {
		t.Fatal("idempotent marker rejected", err)
	}
	if err = recordRichCard(ctx, store, -100123, 123, 2, version, 9, richAck(2)); err == nil {
		t.Fatal("cross-owner marker accepted")
	}
	if err = recordRichCard(ctx, store, -100123, 123, 1, version, 10, richAck(2)); err == nil {
		t.Fatal("conflicting marker overwritten")
	}
	if err = store.SetMeta(ctx, richCardKey(-100123, 124), `{"owner_id":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err = loadRichCardMarker(ctx, store, -100123, 124); !errors.Is(err, ErrRichCardMarker) {
		t.Fatal("invalid marker fell back to legacy", err)
	}
	if _, err = store.DB.Exec("DELETE FROM queue WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("DELETE FROM versions WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = loadRichCardMarker(ctx, store, -100123, 123); !errors.Is(err, ErrRichCardMarker) {
		t.Fatal("deleted source marker accepted", err)
	}
}

func TestRichUserIDCopiesSourceIdentityAndLinksCleanProfile(t *testing.T) {
	p, _ := richFixture(t, 0)
	p.ID, p.VersionID = 115, 1620
	p.RawJSON = `{"userId":115,"name":"Имя🙂","large":9007199254740993,"precise":1.00}`
	if err := json.Unmarshal([]byte(p.RawJSON), &p.Profile); err != nil {
		t.Fatal(err)
	}
	original := p.RawJSON
	card, err := BuildRichProfileCard(p, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	parts := card.Content["blocks"].([]any)[1].(map[string]any)["text"].([]any)
	link := parts[0].(map[string]any)
	button := parts[2].(map[string]any)["button"].(map[string]any)
	if link["text"] != "Jardulik" || link["url"] != jardulikProfileURL(115) || parts[1] != ": " || button["text"] != "115" || button["copy_text"].(map[string]any)["text"] != "115" {
		t.Fatal("rich card copied a local version ID or linked a credential-bearing URL")
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(original), "", "  "); err != nil {
		t.Fatal(err)
	}
	if p.RawJSON != original || parts[len(parts)-1].(map[string]any)["text"] != pretty.String() {
		t.Fatal("source identity presentation changed raw JSON")
	}
}
