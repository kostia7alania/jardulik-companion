package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestSharedProfileRendererPrivateArchiveParity(t *testing.T) {
	for _, count := range []int{0, 1, 3} {
		for _, long := range []bool{false, true} {
			t.Run(fmt.Sprintf("photos%d-long%t", count, long), func(t *testing.T) {
				c, store, a, p, _ := archivePresentationFixture(t)
				raw := `{"userId":99,"name":"Ира🙂","birthday":"2000-02-29T00:30:00+14:00","large":9007199254740993,"decimal":1.00,"nil":null}`
				if long {
					raw = strings.Replace(raw, `"nil":null`, `"description":"`+strings.Repeat("🙂Биография ", 300)+`","nil":null`, 1)
				}
				if _, err := store.DB.Exec("UPDATE versions SET raw_json=? WHERE id=?", raw, p.VersionID); err != nil {
					t.Fatal(err)
				}
				var err error
				p, err = store.Version(context.Background(), 123, p.VersionID)
				if err != nil {
					t.Fatal(err)
				}
				if p.RawJSON != raw {
					t.Fatal("scanProfile lost exact source JSON")
				}
				if _, err = store.DB.Exec("INSERT OR REPLACE INTO mirror_topics(profile_id,thread_id) VALUES(?,55)", p.ID); err != nil {
					t.Fatal(err)
				}
				var photos []MediaJob
				for i := 0; i < count; i++ {
					photos = append(photos, photoPresentationSource(t, i, 2+i, 3+i))
				}
				svc := &Service{Config: c, Store: store}
				private, err := svc.renderPrivateCard(context.Background(), 123, p, photos, photos)
				if err != nil {
					t.Fatal(err)
				}
				limit := 1024
				if count == 0 {
					limit = 4096
				}
				archive, err := a.profileCardContent(p, 0, 55, limit, count == 0, photos)
				if err != nil {
					t.Fatal(err)
				}
				for _, card := range []photoText{private, archive} {
					if textUnits(card.Text) > limit {
						t.Fatal("caption budget exceeded")
					}
					if err = validMessageEntities(card.Text, card.Entities); err != nil {
						t.Fatal(err)
					}
					if !strings.HasSuffix(card.Text, "🔗 пост") || strings.Contains(card.Text, "https://") || !strings.Contains(card.Text, "🎂 29.02.2000 (вт)") {
						t.Fatalf("human/footer parity lost: %+v", card)
					}
					quote := photoQuoteText(t, card)
					if strings.Contains(quote, "UTC+3") || strings.Contains(quote, "🔗 пост") {
						t.Fatal("footer included in quote")
					}
					if card.Entities[len(card.Entities)-1].URL != forumTopicURL(c.BackupChatID, 55) {
						t.Fatal("post URL lost")
					}
				}
				if photoQuoteText(t, private) != photoQuoteText(t, archive) || private.Truncated != archive.Truncated || private.SourceJSONInline != archive.SourceJSONInline {
					t.Fatal("archive/private common body differs")
				}
				if !long {
					var pretty bytes.Buffer
					_ = json.Indent(&pretty, []byte(raw), "", "  ")
					if !private.SourceJSONInline || !strings.HasSuffix(photoQuoteText(t, archive), pretty.String()) {
						t.Fatal("whole source JSON missing or reconstructed")
					}
				}
				if long && ((count > 0 && !private.Truncated) || private.SourceJSONInline) {
					t.Fatal("long source must remain available by JSON action")
				}
				other, err := svc.renderPrivateCard(context.Background(), 456, p, photos, photos)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range other.Entities {
					if e.Type == "text_link" {
						t.Fatal("other tenant received operator private topic URL")
					}
				}
				for _, command := range []string{"/text_", "/json_", "/like_", "/fav_", "/next"} {
					if strings.Contains(private.Text, command) || strings.Contains(archive.Text, command) {
						t.Fatalf("command tail returned: %s", command)
					}
				}
			})
		}
	}
}

func TestSharedRendererRejectsMissingInvalidRawJSON(t *testing.T) {
	for _, raw := range []string{"", "{broken"} {
		if _, err := renderProfileCard(StoredProfile{ID: 1, RawJSON: raw, Profile: Profile{"name": "Do not reconstruct"}}, nil, 0, true, "", 4096, true); err == nil {
			t.Fatal("invalid source JSON reconstructed")
		}
	}
}

func TestPostLinkEntitiesSecureAndSurviveJSONInsertionAndChunks(t *testing.T) {
	link := "https://t.me/c/123/55"
	for _, threaded := range []string{"https://t.me/c/123/55/55", "https://t.me/c/123/55/99"} {
		if err := validMessageEntities("пост", []MessageEntity{{Type: "text_link", Length: 4, URL: threaded}}); err != nil {
			t.Fatalf("explicit forum thread rejected: %v", err)
		}
	}
	card := photoJSONCardText("Ира🙂\n\nJardulik: 99\n📍 "+link, nil, "", 1024, `{"large":9007199254740993}`)
	if !card.SourceJSONInline || len(card.Entities) != 2 || !strings.Contains(photoQuoteText(t, card), "9007199254740993") {
		t.Fatal("JSON insertion assumed only one entity")
	}
	if err := validMessageEntities(card.Text, card.Entities); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"http://t.me/c/1/2", "https://example.com/c/1/2", "https://t.me/c/1/2?secret=1", "https://t.me/c/1/2#x", "https://t.me/c/0/2", "https://t.me/c/1/2/extra", "https://t.me/c/1/0/3", "https://t.me/c/1/2/0", "https://t.me/c/1/2/3/4", "https://t.me/c/1/2/3?secret=1", "https://user@t.me/c/1/2", "https://t.me:443/c/1/2"} {
		if validMessageEntities("пост", []MessageEntity{{Type: "text_link", Length: 4, URL: bad}}) == nil {
			t.Fatalf("unsafe post URL accepted: %s", bad)
		}
	}
	if validMessageEntities("🙂пост", []MessageEntity{{Type: "text_link", Offset: 1, Length: 4, URL: link}}) == nil {
		t.Fatal("UTF16 surrogate boundary accepted")
	}
	full := fullPhotoText(strings.Repeat("🙂", 2500)+"\n\nJardulik: 99\n📍 "+link, []string{`1: {"Make":"real"}`})
	links := 0
	var joined strings.Builder
	for full.Text != "" {
		part, rest := photoTextPart(full, 3900)
		if err := validMessageEntities(part.Text, part.Entities); err != nil {
			t.Fatal(err)
		}
		for _, e := range part.Entities {
			if e.Type == "text_link" {
				links++
				if e.URL != link {
					t.Fatal("chunk lost URL")
				}
			}
		}
		joined.WriteString(part.Text)
		full = rest
	}
	if links != 1 || !strings.HasSuffix(joined.String(), "🔗 пост") {
		t.Fatal("fulltext post link lost")
	}
}
