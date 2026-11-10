package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type archiveChangeFixture struct {
	service *Service
	calls   []string
}

func newArchiveChangeFixture(t *testing.T) *archiveChangeFixture {
	t.Helper()
	c, store := archiveTestStore(t)
	tg := NewTelegram(c)
	a := NewArchiver(c, store, tg, nil)
	var tick atomic.Int64
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return start.Add(time.Duration(tick.Add(1)) * 5 * time.Second) }
	s := &Service{Config: c, Store: store, Archiver: a, TG: tg}
	f := &archiveChangeFixture{service: s}
	if err := a.init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.initArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Fatalf("unexpected external method %s", r.URL.Path)
		}
		var body struct {
			Chat   int64  `json:"chat_id"`
			Thread int64  `json:"message_thread_id"`
			Text   string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Chat != c.BackupChatID || body.Thread != 55 {
			t.Fatal("notice escaped known owner topic")
		}
		f.calls = append(f.calls, body.Text)
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":55,"chat":{"id":%d}}}`, 900+len(f.calls), c.BackupChatID)), nil
	})}
	return f
}

func archiveChangePublish(t *testing.T, f *archiveChangeFixture, owner int64, p Profile, at string, thread int64) int64 {
	t.Helper()
	s := f.service.Store
	ctx := context.Background()
	if err := s.User(ctx, owner, "test"); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Archive(ctx, owner, []Profile{p}, "test")
	if err != nil {
		t.Fatal(err)
	}
	v := ids[0]
	if _, err = s.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(?,?) ON CONFLICT(profile_id) DO UPDATE SET thread_id=excluded.thread_id", p.ID(), thread); err != nil {
		t.Fatal(err)
	}
	sha := digest([]byte(fmt.Sprintf("profile-zip:%d", v)))
	if _, err = s.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha) VALUES(?,?,?,?,'sent',?,?,?)`, v, owner, p.ID(), sha, thread, fmt.Sprintf("[%d]", 1000+v), sha); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]',?)`, v, fmt.Sprintf("[%d]", 1000+v)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at) VALUES(?,'profile_version','whole.zip',1,?,?,?,'zip-file','unique',1,?)`, sha, f.service.Config.BackupChatID, thread, 2000+v, at); err != nil {
		t.Fatal(err)
	}
	return v
}

func archiveChangeDrain(t *testing.T, f *archiveChangeFixture) {
	t.Helper()
	for range 20 {
		worked, err := f.service.deliverArchiveChangePost(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
	t.Fatal("change queue did not drain")
}
func archiveChangeState(t *testing.T, f *archiveChangeFixture, v int64) string {
	t.Helper()
	var state string
	if err := f.service.Store.DB.QueryRow("SELECT state FROM archive_change_posts WHERE version_id=?", v).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestArchiveChangePostsLikaRussianPremiumOnceAndRawPreserved(t *testing.T) {
	f := newArchiveChangeFixture(t)
	first := archiveChangePublish(t, f, 123, Profile{"userId": 99, "name": "Лика", "country": "South Ossetia", "city": "Tskhinval", "type": 1}, "2026-11-08 10:00:00", 55)
	if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := watchCount(t, f.service.Store, "archive_change_posts"); n != 0 {
		t.Fatal("first publications clogged the notice queue")
	}
	second := archiveChangePublish(t, f, 123, Profile{"userId": 99, "name": "Лика", "country": "Russia", "city": "Vladikavkaz", "type": 0}, "2026-11-08 11:00:00", 55)
	if _, err := f.service.Store.DB.Exec("UPDATE versions SET first_seen='2026-11-07 22:10:00',last_seen='2026-11-09 09:00:00' WHERE id=?", second); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := f.service.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", second).Scan(&before); err != nil {
		t.Fatal(err)
	}
	archiveChangeDrain(t, f)
	if len(f.calls) != 1 {
		t.Fatalf("notice count %d", len(f.calls))
	}
	for _, want := range []string{"✏️ Изменения анкеты\nСтрана: Южная Осетия → Россия\nГород: Цхинвал → Владикавказ\nPremium: да → нет", "Снимок: " + displayTime("2026-11-07 22:10:00")} {
		if !strings.Contains(f.calls[0], want) {
			t.Fatalf("missing human change explanation %q", want)
		}
	}
	if strings.Contains(f.calls[0], "закончился") {
		t.Fatal("premium status invented an expiration reason")
	}
	var old, message int64
	if err := f.service.Store.DB.QueryRow("SELECT old_version_id,message_id FROM archive_change_posts WHERE version_id=?", second).Scan(&old, &message); err != nil || old != first || message <= 0 {
		t.Fatal("notice receipt or previous publication lost")
	}
	if err := f.service.recoverArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	archiveChangeDrain(t, f)
	var after string
	_ = f.service.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", second).Scan(&after)
	if len(f.calls) != 1 || before != after || archiveChangeState(t, f, second) != "sent" {
		t.Fatal("restart repeated notice or mutated raw JSON")
	}
}

func TestArchiveChangeRowsObserverMissingNullUnknownAndPhotoIdentity(t *testing.T) {
	before := Profile{"name": "Лика", "distance": 1, "reactionView": false, "isFire": false, "fireText": "old"}
	after := Profile{"name": "Лика", "distance": 99, "reactionView": true, "isFire": true, "fireText": "new"}
	oldJobs := []MediaJob{{Role: "photos", Position: 0, SHA: "a", URL: "old"}, {Role: "photos", Position: 1, SHA: "b"}, {Role: "miniPhotos", Position: 0, SHA: "c"}}
	sameJobs := append([]MediaJob(nil), oldJobs...)
	sameJobs[0].URL = "new-url-same-bytes"
	if rows := archiveProfileChangeRows(before, after, oldJobs, sameJobs); len(rows) != 0 {
		t.Fatalf("observer or URL-only spam: %v", rows)
	}
	after["sourceNewField"] = nil
	before["precise"] = json.Number("9007199254740993")
	after["precise"] = json.Number("9007199254740995")
	text := strings.Join(archiveProfileChangeRows(before, after, nil, nil), "\n")
	if !strings.Contains(text, "sourceNewField: (поле отсутствует) → null") || !strings.Contains(text, "precise: 9007199254740993 → 9007199254740995") {
		t.Fatal("unknown values or missing/null distinction lost")
	}
	reordered := append([]MediaJob(nil), oldJobs...)
	reordered[0].SHA, reordered[1].SHA = "b", "a"
	reordered[2].SHA = "d"
	photos := strings.Join(archiveChangePhotoRows(oldJobs, reordered), "\n")
	if photos != "📷 Фото и превью: фото (изменён порядок/позиции); превью (добавлено 1, убрано 1)" {
		t.Fatalf("photo role/order/SHA change lost: %s", photos)
	}
}

func TestArchiveChangePostsReceiptOrderLatestFirstOwnerAndLayout(t *testing.T) {
	f := newArchiveChangeFixture(t)
	highTime := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "Russia"}, "2026-11-08 10:00:02", 55)
	lowTime := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "South Ossetia"}, "2026-11-08 10:00:01", 55)
	tieTime := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "Georgia"}, "2026-11-08 10:00:02", 55)
	unpublished := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "Armenia"}, "2026-11-08 10:00:03", 55)
	if _, err := f.service.Store.DB.Exec("UPDATE archive_profile_repairs SET state='uncertain' WHERE version_id=?", unpublished); err != nil {
		t.Fatal(err)
	}
	archiveChangePublish(t, f, 456, Profile{"userId": 199, "country": "Russia"}, "2026-11-08 10:00:00", 56)
	archiveChangePublish(t, f, 456, Profile{"userId": 199, "country": "Georgia"}, "2026-11-08 11:00:00", 56)
	if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	var previous int64
	if err := f.service.Store.DB.QueryRow("SELECT old_version_id FROM archive_change_posts WHERE version_id=?", highTime).Scan(&previous); err != nil || previous != lowTime {
		t.Fatal("used version ordering instead of receipt time")
	}
	if err := f.service.Store.DB.QueryRow("SELECT old_version_id FROM archive_change_posts WHERE version_id=?", tieTime).Scan(&previous); err != nil || previous != highTime {
		t.Fatal("equal receipt time did not use version order")
	}
	if n := watchCount(t, f.service.Store, "archive_change_posts"); n != 2 {
		t.Fatal("queued tenant or unconfirmed layout or first version")
	}
	if worked, err := f.service.deliverArchiveChangePost(context.Background()); !worked || err != nil {
		t.Fatal(worked, err)
	}
	if archiveChangeState(t, f, tieTime) != "sent" || !strings.Contains(f.calls[0], "Россия → Грузия") {
		t.Fatal("latest published change was not first")
	}
}

func TestArchiveChangePostsObserverOnlyIsSkipped(t *testing.T) {
	f := newArchiveChangeFixture(t)
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "name": "Лика", "distance": 1}, "2026-11-08 10:00:00", 55)
	current := archiveChangePublish(t, f, 123, Profile{"userId": 99, "name": "Лика", "distance": 9}, "2026-11-08 11:00:00", 55)
	archiveChangeDrain(t, f)
	if len(f.calls) != 0 || archiveChangeState(t, f, current) != "skipped" {
		t.Fatal("observer-only update created a post or false sent receipt")
	}
}

func TestArchiveChangePostsPresenceGuardsAndRechecksAtSend(t *testing.T) {
	for _, mode := range []string{"unavailable", "pending", "processing", "during_group_wait"} {
		t.Run(mode, func(t *testing.T) {
			f := newArchiveChangeFixture(t)
			archiveChangePublish(t, f, 123, Profile{"userId": 99, "city": "Tskhinval"}, "2026-11-08 10:00:00", 55)
			current := archiveChangePublish(t, f, 123, Profile{"userId": 99, "city": "Vladikavkaz"}, "2026-11-08 11:00:00", 55)
			if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.Store.DB.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(123,99,'available','2026-11-08','2026-11-08')"); err != nil {
				t.Fatal(err)
			}
			if mode == "unavailable" {
				_, _ = f.service.Store.DB.Exec("UPDATE profile_presence SET availability='unavailable'")
			}
			if mode == "pending" || mode == "processing" {
				if _, err := f.service.Store.DB.Exec("INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,state) VALUES(123,99,1,'unavailable','2026-11-08',?)", mode); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "during_group_wait" {
				var once atomic.Bool
				base := f.service.Archiver.now
				f.service.Archiver.now = func() time.Time {
					if once.CompareAndSwap(false, true) {
						return base()
					}
					_, _ = f.service.Store.DB.Exec("UPDATE profile_presence SET availability='unavailable' WHERE owner_id=123 AND profile_id=99")
					return base()
				}
			}
			worked, err := f.service.deliverArchiveChangePost(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if mode != "during_group_wait" && worked {
				t.Fatal("claimed a presence-blocked change")
			}
			if len(f.calls) != 0 || archiveChangeState(t, f, current) != "pending" {
				t.Fatal("absence was displaced by a change notice")
			}
		})
	}
}

func TestArchiveChangePostsUnconfirmedReceiptNeverRetriesAndRestartIsSafe(t *testing.T) {
	for _, mode := range []string{"bad_json", "wrong_chat", "wrong_thread", "sending_restart", "preparing_restart"} {
		t.Run(mode, func(t *testing.T) {
			f := newArchiveChangeFixture(t)
			archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 0}, "2026-11-08 10:00:00", 55)
			current := archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 1}, "2026-11-08 11:00:00", 55)
			if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
				t.Fatal(err)
			}
			calls := 0
			if strings.HasSuffix(mode, "_restart") {
				state := strings.TrimSuffix(mode, "_restart")
				if _, err := f.service.Store.DB.Exec("UPDATE archive_change_posts SET state=? WHERE version_id=?", state, current); err != nil {
					t.Fatal(err)
				}
				if err := f.service.recoverArchiveChangePosts(context.Background()); err != nil {
					t.Fatal(err)
				}
				archiveChangeDrain(t, f)
				if mode == "sending_restart" {
					if len(f.calls) != 0 || archiveChangeState(t, f, current) != "uncertain" {
						t.Fatal("ambiguous send replayed on restart")
					}
				} else if len(f.calls) != 1 || archiveChangeState(t, f, current) != "sent" {
					t.Fatal("safe preparation did not resume")
				}
				return
			}
			f.service.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				body := `interrupted`
				if mode == "wrong_chat" {
					body = `{"ok":true,"result":{"message_id":999,"message_thread_id":55,"chat":{"id":-100999}}}`
				}
				if mode == "wrong_thread" {
					body = `{"ok":true,"result":{"message_id":999,"message_thread_id":56,"chat":{"id":-100123}}}`
				}
				return albumTestResponse(r, 200, body), nil
			})
			if worked, err := f.service.deliverArchiveChangePost(context.Background()); !worked || err == nil {
				t.Fatal("unconfirmed receipt accepted")
			}
			if archiveChangeState(t, f, current) != "uncertain" {
				t.Fatal("unknown send was made retryable")
			}
			if err := f.service.recoverArchiveChangePosts(context.Background()); err != nil {
				t.Fatal(err)
			}
			if worked, err := f.service.deliverArchiveChangePost(context.Background()); worked || err != nil || calls != 1 {
				t.Fatal("unknown send replayed")
			}
		})
	}
}

func TestArchiveChangePostsRateLimitWaitRemainsPending(t *testing.T) {
	f := newArchiveChangeFixture(t)
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 0}, "2026-11-08 10:00:00", 55)
	current := archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 1}, "2026-11-08 11:00:00", 55)
	calls := 0
	f.service.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"parameters":{"retry_after":3600}}`), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	worked, err := f.service.deliverArchiveChangePost(ctx)
	if !worked || !errors.Is(err, context.DeadlineExceeded) || calls != 1 || archiveChangeState(t, f, current) != "pending" {
		t.Fatalf("429/cancel classified as unknown: %v %v calls%d", worked, err, calls)
	}
	if worked, err = f.service.deliverArchiveChangePost(context.Background()); worked || err != nil || calls != 1 {
		t.Fatal("ignored durable retry_after")
	}
}

func TestArchiveChangeNoticeLongSourceIsBoundedAndHasFullJSONAction(t *testing.T) {
	p := StoredProfile{VersionID: 77, FirstSeen: "2026-11-07 22:10:00"}
	text, entities, keyboard := archiveChangeNotice(p, []string{"unknown: " + strings.Repeat("🙂", 5000)})
	if textUnits(text) > 4096 || !strings.Contains(text, "Показана часть изменений") || !strings.HasSuffix(text, "Снимок: "+displayTime("2026-11-07 22:10:00")) {
		t.Fatal("notice budget or stable source time lost")
	}
	if err := validMessageEntities(text, entities); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(keyboard)
	if !strings.Contains(string(encoded), "aj:77") {
		t.Fatal("full source callback missing")
	}
}

func TestArchiveChangePostsRequireConfirmedCardIDAndSourceReceipt(t *testing.T) {
	for _, bad := range []string{"card_id", "receipt", "other_topic"} {
		t.Run(bad, func(t *testing.T) {
			f := newArchiveChangeFixture(t)
			archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 0}, "2026-11-08 10:00:00", 55)
			v := archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 1}, "2026-11-08 11:00:00", 55)
			switch bad {
			case "card_id":
				_, _ = f.service.Store.DB.Exec("UPDATE archive_versions SET message_ids='[]' WHERE version_id=?", v)
			case "receipt":
				_, _ = f.service.Store.DB.Exec("DELETE FROM archive_receipts WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)", v)
			case "other_topic":
				_, _ = f.service.Store.DB.Exec("UPDATE archive_receipts SET thread_id=56 WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)", v)
			}
			archiveChangeDrain(t, f)
			if len(f.calls) != 0 || watchCount(t, f.service.Store, "archive_change_posts") != 0 {
				t.Fatal("unconfirmed or mismatched publication generated a notice")
			}
		})
	}
}

type archiveChangeCancelBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (b archiveChangeCancelBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.cancel()
	return n, err
}
func (archiveChangeCancelBody) Close() error { return nil }
func TestArchiveChangePostsConfirmedACKSurvivesCancellation(t *testing.T) {
	f := newArchiveChangeFixture(t)
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 0}, "2026-11-08 10:00:00", 55)
	v := archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 1}, "2026-11-08 11:00:00", 55)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.service.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		response := albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":901,"message_thread_id":55,"chat":{"id":-100123}}}`)
		response.Body = archiveChangeCancelBody{strings.NewReader(`{"ok":true,"result":{"message_id":901,"message_thread_id":55,"chat":{"id":-100123}}}`), cancel}
		return response, nil
	})
	if worked, err := f.service.deliverArchiveChangePost(ctx); !worked || err != nil || archiveChangeState(t, f, v) != "sent" {
		t.Fatalf("confirmed ACK was lost on cancellation: %v %v", worked, err)
	}
}

func TestArchiveChangePostsZIPReplacementRetainsOriginalPublicationOrder(t *testing.T) {
	f := newArchiveChangeFixture(t)
	first := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "South Ossetia"}, "2026-11-08 10:00:00", 55)
	second := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "Russia"}, "2026-11-08 11:00:00", 55)
	var oldSHA string
	if err := f.service.Store.DB.QueryRow("SELECT source_sha FROM archive_versions WHERE version_id=?", first).Scan(&oldSHA); err != nil {
		t.Fatal(err)
	}
	newSHA := digest([]byte("replacement ZIP"))
	store := f.service.Store
	if _, err := store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at) VALUES(?,'profile_version','replacement.zip',1,?,55,?,'new-zip','new-unique',1,'2026-11-08 12:00:00')`, newSHA, f.service.Config.BackupChatID, 2000+first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`INSERT INTO archive_zip_repairs(version_id,state,old_source_sha,old_filename,old_message_id,new_source_sha) VALUES(?,'sent',?,'whole.zip',?,?)`, first, oldSHA, 2000+first, newSHA); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("UPDATE archive_versions SET source_sha=? WHERE version_id=?", newSHA, first); err != nil {
		t.Fatal(err)
	}
	archiveChangeDrain(t, f)
	if len(f.calls) != 1 || !strings.Contains(f.calls[0], "Страна: Южная Осетия → Россия") {
		t.Fatal("in-place ZIP replacement reversed actual profile chronology")
	}
	var previous int64
	if err := store.DB.QueryRow("SELECT old_version_id FROM archive_change_posts WHERE version_id=?", second).Scan(&previous); err != nil || previous != first {
		t.Fatal("original receipt was not used")
	}
	if n := watchCount(t, store, "archive_change_posts"); n != 1 {
		t.Fatal("replacement ZIP created a false new profile change")
	}
}

func TestArchiveChangePostsBlockedLatestDoesNotStarveOlderReadyTopic(t *testing.T) {
	f := newArchiveChangeFixture(t)
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 0}, "2026-11-08 12:00:00", 55)
	blocked := archiveChangePublish(t, f, 123, Profile{"userId": 99, "type": 1}, "2026-11-08 13:00:00", 55)
	archiveChangePublish(t, f, 123, Profile{"userId": 100, "type": 0}, "2026-11-08 10:00:00", 56)
	ready := archiveChangePublish(t, f, 123, Profile{"userId": 100, "type": 1}, "2026-11-08 11:00:00", 56)
	if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Store.DB.Exec("UPDATE archive_profile_repairs SET state='uncertain' WHERE version_id=?", blocked); err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.service.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		var body struct {
			Thread int64 `json:"message_thread_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Thread != 56 {
			t.Fatal("posted unconfirmed latest topic")
		}
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":901,"message_thread_id":56,"chat":{"id":-100123}}}`), nil
	})
	if worked, err := f.service.deliverArchiveChangePost(context.Background()); !worked || err != nil || calls != 0 {
		t.Fatal("latest blocked publication was not safely deferred")
	}
	if worked, err := f.service.deliverArchiveChangePost(context.Background()); !worked || err != nil || calls != 1 || archiveChangeState(t, f, ready) != "sent" || archiveChangeState(t, f, blocked) != "pending" {
		t.Fatal("blocked newest publication starved another ready topic")
	}
}

func TestArchiveChangePhotoRowsCombinePhotoAndPreviewWithoutLosingCounts(t *testing.T) {
	cases := []struct {
		name          string
		before, after []MediaJob
		want          string
	}{
		{"same counts", []MediaJob{{Role: "photos", SHA: "old-photo"}, {Role: "miniPhotos", SHA: "old-preview"}}, []MediaJob{{Role: "photos", SHA: "new-photo"}, {Role: "miniPhotos", SHA: "new-preview"}}, "📷 Фото и превью: добавлено 1, убрано 1 (в каждом наборе)"},
		{"different counts", []MediaJob{{Role: "photos", SHA: "a"}, {Role: "photos", Position: 1, SHA: "b"}, {Role: "miniPhotos", SHA: "preview-a"}}, []MediaJob{{Role: "photos", SHA: "a"}, {Role: "photos", Position: 1, SHA: "c"}, {Role: "photos", Position: 2, SHA: "d"}, {Role: "miniPhotos", SHA: "preview-a"}, {Role: "miniPhotos", Position: 1, SHA: "preview-b"}}, "📷 Фото и превью: фото (добавлено 2, убрано 1); превью (добавлено 1, убрано 0)"},
		{"photo only", []MediaJob{{Role: "photos", SHA: "old"}, {Role: "miniPhotos", SHA: "same"}}, []MediaJob{{Role: "photos", SHA: "new"}, {Role: "miniPhotos", SHA: "same"}}, "Фотографии: добавлено 1, убрано 1"},
		{"preview only", []MediaJob{{Role: "photos", SHA: "same"}, {Role: "miniPhotos", SHA: "old"}}, []MediaJob{{Role: "photos", SHA: "same"}, {Role: "miniPhotos", SHA: "new"}}, "Миниатюры: добавлено 1, убрано 1"},
		{"both reordered", []MediaJob{{Role: "photos", SHA: "a"}, {Role: "photos", Position: 1, SHA: "b"}, {Role: "miniPhotos", SHA: "c"}, {Role: "miniPhotos", Position: 1, SHA: "d"}}, []MediaJob{{Role: "photos", SHA: "b"}, {Role: "photos", Position: 1, SHA: "a"}, {Role: "miniPhotos", SHA: "d"}, {Role: "miniPhotos", Position: 1, SHA: "c"}}, "📷 Фото и превью: изменён порядок/позиции (в каждом наборе)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeRaw, _ := json.Marshal(tc.before)
			afterRaw, _ := json.Marshal(tc.after)
			rows := archiveChangePhotoRows(tc.before, tc.after)
			if len(rows) != 1 || rows[0] != tc.want || strings.Contains(rows[0], "\n") {
				t.Fatalf("expected one complete media line, got %v", rows)
			}
			beforeNow, _ := json.Marshal(tc.before)
			afterNow, _ := json.Marshal(tc.after)
			if string(beforeNow) != string(beforeRaw) || string(afterNow) != string(afterRaw) {
				t.Fatal("summary changed source media inventory")
			}
		})
	}
}
