package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProfileLabelsTimeAndCaption(t *testing.T) {
	p := StoredProfile{ID: 10, LastSeen: "2026-11-07 22:10:00", Profile: Profile{"name": "Ира", "goal": "friends", "worldview": "orthodox", "profession": "act_gov_structures", "children": "none", "languages": "ru,en", "city": "Vladikavkaz", "country": "Russia"}}
	text := profileText(p)
	for _, want := range []string{"В поиске друзей", "Православие", "Государственные структуры", "Владикавказ", "Россия", "8.11.26, 01:10 (вс, UTC+3)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %q", want, text)
		}
	}
	if got := displayTime("2026-12-07T20:39:45+04:00"); got != "7.12.26, 19:39 (пн, UTC+3)" {
		t.Fatal(got)
	}
	long := strings.Repeat("🙂", 800) + "FULL-END"
	caption := profileCaption(long)
	if _, rest := telegramPrefix(caption, 1024); rest != "" || !strings.Contains(caption, "Полный текст") {
		t.Fatal("unsafe caption budget")
	}
	p.Profile["profession"] = "future_value"
	if profileValue(p.Profile, "profession") != "future_value" {
		t.Fatal("unknown enum was lost")
	}
}

func TestCompactProfileAndMissingFields(t *testing.T) {
	p := StoredProfile{ID: 7, LastSeen: "2026-11-07 22:10:00", Profile: Profile{"name": "Ира", "surName": "Иванова", "age": 25, "height": 170, "country": "Russia", "city": "Vladikavkaz", "children": "none", "description": "", "languages": "[]", "profession": ""}}
	text := profileText(p)
	if !strings.HasPrefix(text, "Ира Иванова, 25/170\n📍 Россия, Владикавказ") || !strings.HasSuffix(text, "Jardulik: 7") {
		t.Fatal(text)
	}
	if !strings.Contains(text, "Не указано: занятие, языки, о себе") || strings.Contains(text, "Не указано: дети") {
		t.Fatal(text)
	}
	if !strings.Contains(text, "Детей нет") {
		t.Fatal("explicit negative response lost")
	}
	p.Profile["description"] = strings.Repeat("🙂", 800)
	caption := profileCaption(profileText(p))
	if _, rest := telegramPrefix(caption, 1024); rest != "" || !strings.Contains(caption, "Jardulik: 7") || !strings.Contains(caption, "(вс, UTC+3)") || !strings.Contains(caption, "Не указано:") {
		t.Fatal("long biography displaced required footer")
	}
	if got := forumTopicURL(-1004401653948, 5); got != "https://t.me/c/4401653948/5" {
		t.Fatal(got)
	}
}

func TestPrivateArchiveLinkOnlyForOwner(t *testing.T) {
	s, p := cardFixture(t, 1)
	s.Config.OwnerID = 1
	s.Config.BackupChatID = -1004401653948
	if _, err := s.Store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(10,5)"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s.cardText(context.Background(), 1, p), "https://t.me/c/4401653948/5") {
		t.Fatal("owner missing topic link")
	}
	if strings.Contains(s.cardText(context.Background(), 2, p), "https://t.me/c/") {
		t.Fatal("private group locator leaked to customer")
	}
}

func cardFixture(t *testing.T, n int) (*Service, StoredProfile) {
	t.Helper()
	store, c := testStore(t)
	ctx := context.Background()
	if err := store.User(ctx, 1, ""); err != nil {
		t.Fatal(err)
	}
	var urls []any
	for i := 0; i < n; i++ {
		urls = append(urls, fmt.Sprintf("https://mini.jardulik.app/%d.jpg", i))
	}
	ids, err := store.Archive(ctx, 1, []Profile{{"userId": 10, "name": "Ира", "age": 25, "description": "Полное описание", "photos": urls}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(c.DataDir, "media"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("photo-%d", i))
		sha := digest(data)
		name := filepath.Join("media", sha)
		if err = os.WriteFile(filepath.Join(c.DataDir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = store.DB.ExecContext(ctx, "INSERT INTO media(sha,path,size,mime) VALUES(?,?,?,'image/jpeg')", sha, name, len(data)); err != nil {
			t.Fatal(err)
		}
		if _, err = store.DB.ExecContext(ctx, "UPDATE media_jobs SET sha=?,state='complete' WHERE version_id=? AND position=?", sha, ids[0], i); err != nil {
			t.Fatal(err)
		}
	}
	p, err := store.Version(ctx, 1, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	return &Service{Config: c, Store: store, TG: NewTelegram(c)}, p
}

func TestPhotoAndDescriptionOneCard(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s, p := cardFixture(t, n)
			var methods []string
			s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := filepath.Base(r.URL.Path)
				methods = append(methods, method)
				w.Header().Set("Content-Type", "application/json")
				if method == "editMessageReplyMarkup" {
					fmt.Fprint(w, `{"ok":true,"result":true}`)
					return
				}
				if method != "sendPhoto" && method != "sendMediaGroup" {
					t.Fatalf("separate post: %s", method)
				}
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Fatal(err)
				}
				defer r.MultipartForm.RemoveAll()
				if n == 1 {
					if !strings.Contains(r.FormValue("caption"), "Полное описание") || r.FormValue("reply_markup") == "" {
						t.Fatal("missing photo caption/buttons")
					}
					fmt.Fprint(w, `{"ok":true,"result":{"message_id":20,"chat":{"id":1,"type":"private"}}}`)
				} else {
					var media []map[string]string
					if err := json.Unmarshal([]byte(r.FormValue("media")), &media); err != nil {
						t.Fatal(err)
					}
					if len(media) != 3 || !strings.Contains(media[0]["caption"], "Полное описание") || media[1]["caption"] != "" {
						t.Fatal("album caption misplaced")
					}
					fmt.Fprint(w, `{"ok":true,"result":[{"message_id":20,"chat":{"id":1}},{"message_id":21,"chat":{"id":1}},{"message_id":22,"chat":{"id":1}}]}`)
				}
			})}
			ids, err := s.sendProfileCard(context.Background(), 1, p)
			if err != nil || len(ids) != n {
				t.Fatalf("%v %v", ids, err)
			}
			want := []string{"sendPhoto"}
			if n == 3 {
				want = []string{"sendMediaGroup"}
			}
			if !reflect.DeepEqual(methods, want) {
				t.Fatal(methods)
			}
		})
	}
}

func TestPrivateCardRepairConfirmation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s, p := cardFixture(t, 1)
			ctx := context.Background()
			var calls []string
			if _, err := s.Store.DB.ExecContext(ctx, "INSERT INTO deliveries(owner_id,version_id,state,message_ids) VALUES(1,?,'sent','[10,11]')", p.VersionID); err != nil {
				t.Fatal(err)
			}
			s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := filepath.Base(r.URL.Path)
				calls = append(calls, method)
				if method == "sendPhoto" {
					_, _ = io.Copy(io.Discard, r.Body)
					if fail {
						fmt.Fprint(w, `not-json`)
						return
					}
					fmt.Fprint(w, `{"ok":true,"result":{"message_id":30,"chat":{"id":1}}}`)
					return
				}
				var b struct {
					ID int64 `json:"message_id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&b)
				if method != "deleteMessage" || (b.ID != 10 && b.ID != 11) {
					t.Fatalf("unsafe cleanup: %s %d", method, b.ID)
				}
				fmt.Fprint(w, `{"ok":true,"result":true}`)
			})}
			err := s.RepairPrivateCards(ctx)
			if fail {
				if err == nil || !reflect.DeepEqual(calls, []string{"sendPhoto"}) {
					t.Fatalf("unsafe unknown-result repair: %v %v", err, calls)
				}
			} else if err != nil || !reflect.DeepEqual(calls, []string{"sendPhoto", "deleteMessage", "deleteMessage"}) {
				t.Fatalf("%v %v", err, calls)
			}
			before := len(calls)
			err = s.RepairPrivateCards(ctx)
			if (err != nil) != fail {
				t.Fatalf("wrong unresolved-repair status: %v", err)
			}
			if len(calls) != before {
				t.Fatal("blind repair repeat")
			}
		})
	}
}

func TestArchiveCallbackOwnerAndChat(t *testing.T) {
	s, _ := cardFixture(t, 1)
	s.Config.BackupChatID = -10
	s.Config.OwnerID = 1
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("unauthorized archive request") })}
	for _, c := range []Callback{{From: TGUser{ID: 2}, Message: Message{Chat: TGChat{ID: -10}}, Data: "av:1"}, {From: TGUser{ID: 1}, Message: Message{Chat: TGChat{ID: -11}}, Data: "av:1"}} {
		s.archiveCallback(context.Background(), c)
	}
}

func TestAlbumKeepsFullTextAccessibleWithoutMarkup(t *testing.T) {
	svc, p := cardFixture(t, 3)
	p.Profile["description"] = strings.Repeat("Длинное описание ", 180) + "FULL-END"
	calls := 0
	svc.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if filepath.Base(r.URL.Path) != "sendMediaGroup" {
			t.Fatal("unsupported album markup or separate post")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		var media []map[string]string
		if err := json.Unmarshal([]byte(r.FormValue("media")), &media); err != nil {
			t.Fatal(err)
		}
		caption := media[0]["caption"]
		if !strings.Contains(caption, "/text_10") || !strings.Contains(caption, "/fav_10") {
			t.Fatal("full text inaccessible")
		}
		if _, rest := telegramPrefix(caption, 1024); rest != "" {
			t.Fatal("caption too long")
		}
		fmt.Fprint(w, `{"ok":true,"result":[{"message_id":30,"chat":{"id":1}},{"message_id":31,"chat":{"id":1}},{"message_id":32,"chat":{"id":1}}]}`)
	})}
	ids, err := svc.sendProfileCard(context.Background(), 1, p)
	if err != nil || len(ids) != 3 || calls != 1 {
		t.Fatalf("%v %v %d", ids, err, calls)
	}
	if !strings.Contains(profileText(p), "FULL-END") {
		t.Fatal("full original description lost")
	}
}

func TestRepairRetainsCleanupProvenance(t *testing.T) {
	s, p := cardFixture(t, 1)
	ctx := context.Background()
	if _, err := s.Store.DB.ExecContext(ctx, "INSERT INTO deliveries(owner_id,version_id,state,message_ids) VALUES(1,?,'sent','[10,11]')", p.VersionID); err != nil {
		t.Fatal(err)
	}
	var calls int
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = io.Copy(io.Discard, r.Body)
		if filepath.Base(r.URL.Path) == "sendPhoto" {
			fmt.Fprint(w, `{"ok":true,"result":{"message_id":30,"chat":{"id":1}}}`)
			return
		}
		fmt.Fprint(w, `{"ok":false,"error_code":500,"description":"Unavailable"}`)
	})}
	if err := s.RepairPrivateCards(ctx); err == nil {
		t.Fatal("failed cleanup hidden")
	}
	var record privateCardRepair
	if err := json.Unmarshal([]byte(s.Store.Meta(ctx, "repaired_card:1")), &record); err != nil {
		t.Fatal(err)
	}
	if record.State != "deleting" || !reflect.DeepEqual(record.OldIDs, []int64{10, 11}) || !reflect.DeepEqual(record.NewIDs, []int64{30}) {
		t.Fatalf("cleanup provenance lost: %+v", record)
	}
	before := calls
	if err := s.RepairPrivateCards(ctx); err == nil || calls != before {
		t.Fatal("cleanup uncertainty hidden or replayed")
	}
}
