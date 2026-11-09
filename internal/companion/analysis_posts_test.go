package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"
)

type analysisPostFixture struct {
	service  *Service
	now      time.Time
	ticks    atomic.Int64
	analysis ProfileAnalysis
	calls    int
	bodies   []analysisPostPayload
	respond  func(*http.Request) (*http.Response, error)
}

func newAnalysisPostFixture(t *testing.T, content string) *analysisPostFixture {
	t.Helper()
	s, _ := compatibilityFixture(t)
	s.Config.OwnerID = 1
	s.Config.BackupChatID = -100123
	s.Config.ArchivePaused = false
	s.Archiver = NewArchiver(s.Config, s.Store, NewTelegram(s.Config), nil)
	f := &analysisPostFixture{service: s, now: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	s.Archiver.now = func() time.Time { return f.now.Add(time.Duration(f.ticks.Add(1)) * 10 * time.Second) }
	s.Client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("posting saved analysis called Jardulik")
		return nil, nil
	})
	s.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.calls++
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Fatalf("unexpected Telegram operation: %s", r.URL.Path)
		}
		var body struct {
			Chat     int64           `json:"chat_id"`
			Thread   int64           `json:"message_thread_id"`
			Text     string          `json:"text"`
			Entities []MessageEntity `json:"entities"`
			Keyboard any             `json:"reply_markup"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Chat != -100123 || body.Thread != 55 {
			t.Fatalf("wrong destination: %+v", body)
		}
		f.bodies = append(f.bodies, analysisPostPayload{Text: body.Text, Entities: body.Entities, Keyboard: body.Keyboard})
		if f.respond != nil {
			return f.respond(r)
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":55,"chat":{"id":-100123}}}`, 500+f.calls)), nil
	})
	compatibilityAdd(t, s, 1, 7, 23)
	if err := s.initAnalysisPosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Archiver.init(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.analysis = f.save(t, 1, 7, content, f.now)
	return f
}
func (f *analysisPostFixture) save(t *testing.T, owner, target int64, content string, observed time.Time) ProfileAnalysis {
	t.Helper()
	ctx := context.Background()
	session, err := f.service.Store.Session(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := f.service.Store.Profile(ctx, owner, target)
	if err != nil {
		t.Fatal(err)
	}
	response, err := json.Marshal(map[string]string{"content": content, "model": "reported-model"})
	if err != nil {
		t.Fatal(err)
	}
	response = append(response, '\n')
	request, _ := json.Marshal(map[string]int64{"fromUserId": session.UserID, "targetUserId": target})
	a, err := f.service.Store.SaveAnalysisCapture(ctx, owner, AnalysisCaptureInput{UpstreamID: session.UserID, ProfileID: target, VersionID: profile.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: observed.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: session.Generation, RequestBody: request, ResponseBody: response, OwnProfile: session.Account, TargetProfile: []byte(profile.RawJSON)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func (f *analysisPostFixture) ready(t *testing.T) {
	t.Helper()
	s := f.service.Store
	if _, err := s.DB.Exec("INSERT OR REPLACE INTO mirror_topics(profile_id,thread_id) VALUES(7,55)"); err != nil {
		t.Fatal(err)
	}
	_, err := s.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids)
 SELECT v.id,v.owner_id,v.profile_id,v.fingerprint,'sent',55,'[40]' FROM versions v WHERE v.owner_id=1 AND v.profile_id=7
 ON CONFLICT(version_id) DO UPDATE SET state='sent',thread_id=55`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB.Exec(`INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids)
 SELECT id,'sent','[]','[40]' FROM versions WHERE owner_id=1 AND profile_id=7
 ON CONFLICT(version_id) DO UPDATE SET state='sent'`)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *analysisPostFixture) state(t *testing.T, id int64) string {
	t.Helper()
	var state string
	if err := f.service.Store.DB.QueryRow("SELECT state FROM analysis_posts WHERE analysis_id=?", id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}
func analysisPostQuoted(payload analysisPostPayload) string {
	if len(payload.Entities) != 1 {
		return ""
	}
	entity := payload.Entities[0]
	units := utf16.Encode([]rune(payload.Text))
	return string(utf16.Decode(units[entity.Offset : entity.Offset+entity.Length]))
}

func TestAnalysisPostsExactQuoteReceiptAndDedup(t *testing.T) {
	content := "Привет! 😀\n8. **Итог**: Оценка совместимости — 75%.\nБез изменений.\n"
	f := newAnalysisPostFixture(t, content)
	f.ready(t)
	ctx := context.Background()
	before, err := f.service.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	// /pause affects source work, not publication of already saved results.
	f.service.Store.Pause(ctx, 1, true)
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil {
		t.Fatal("post failed", worked, err)
	}
	if f.calls != 1 || f.state(t, f.analysis.ID) != "sent" {
		t.Fatal("wrong delivery state")
	}
	body := f.bodies[0]
	if !strings.HasPrefix(body.Text, "🧩 Совместимость: 75% · Jardulik GPT\nОт: Own · 101\n9.11.26, 15:00 (пн, UTC+3)\n\n") || analysisPostQuoted(body) != content || body.Entities[0].Type != "expandable_blockquote" {
		t.Fatalf("rewritten quote or header: %+v", body)
	}
	var message int64
	var receipt, raw []byte
	var sha string
	if err := f.service.Store.DB.QueryRow("SELECT message_id,receipt_json,payload_json,response_sha FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&message, &receipt, &raw, &sha); err != nil {
		t.Fatal(err)
	}
	if message != 501 || !bytes.Contains(receipt, []byte(`"message_id":501`)) || len(raw) == 0 || sha != digest(f.analysis.ResponseBody) {
		t.Fatal("receipt or source hash not saved")
	}
	if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 1 {
		t.Fatal("post repeated", worked, err)
	}
	after, err := f.service.Store.Profile(ctx, 1, 7)
	if err != nil || after.RawJSON != before.RawJSON {
		t.Fatal("post changed archived profile", err)
	}
	saved, err := f.service.Store.LatestProfileAnalysis(ctx, 1, 7)
	if err != nil || !bytes.Equal(saved.ResponseBody, f.analysis.ResponseBody) {
		t.Fatal("post changed raw GPT response", err)
	}
}

func TestAnalysisPostsWaitForOwnArchiveLayoutAndNewerVersions(t *testing.T) {
	f := newAnalysisPostFixture(t, "saved response")
	ctx := context.Background()
	assertWaiting := func() {
		t.Helper()
		if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 0 {
			t.Fatal("post bypassed archive readiness", worked, err, f.calls)
		}
	}
	assertWaiting()
	if _, err := f.service.Store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(7,55)"); err != nil {
		t.Fatal(err)
	}
	assertWaiting()
	f.ready(t)
	if _, err := f.service.Store.DB.Exec("UPDATE archive_profile_repairs SET state='pending'"); err != nil {
		t.Fatal(err)
	}
	assertWaiting()
	if _, err := f.service.Store.DB.Exec("UPDATE archive_profile_repairs SET state='sent'"); err != nil {
		t.Fatal(err)
	}
	p, err := f.service.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["description"] = "newer pending version"
	if _, err = f.service.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser"); err != nil {
		t.Fatal(err)
	}
	assertWaiting()
	f.ready(t)
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil || f.calls != 1 {
		t.Fatal("ready thread did not publish", worked, err)
	}
}

func TestAnalysisPostsOnlyConfiguredOwnerAndCurrentUpstream(t *testing.T) {
	f := newAnalysisPostFixture(t, "own result")
	f.ready(t)
	ctx := context.Background()
	compatibilityAdd(t, f.service, 2, 7, 23)
	other := f.save(t, 2, 7, "other private result", f.now.Add(time.Hour))
	old, err := f.service.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	next := old
	next.UserID = 999
	next.Generation = "different-account"
	if err = f.service.Store.SaveSession(ctx, 1, next); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 0 {
		t.Fatal("sent old account or foreign owner", worked, err)
	}
	var otherRows int
	if err = f.service.Store.DB.QueryRow("SELECT COUNT(*) FROM analysis_posts WHERE analysis_id=?", other.ID).Scan(&otherRows); err != nil || otherRows != 0 {
		t.Fatal("foreign result was copied into owner outbox", err)
	}
	if err = f.service.Store.SaveSession(ctx, 1, old); err != nil {
		t.Fatal(err)
	}
	f.service.Config.ArchivePaused = true
	if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 0 {
		t.Fatal("ArchivePaused was ignored")
	}
	f.service.Config.ArchivePaused = false
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil || f.calls != 1 {
		t.Fatal("current owner result not published", worked, err)
	}
}

func TestAnalysisPostsLatestBackfillAndFutureCapture(t *testing.T) {
	f := newAnalysisPostFixture(t, "old historical result")
	f.ready(t)
	ctx := context.Background()
	second := f.save(t, 1, 7, "latest historical result", f.now.Add(time.Minute))
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil {
		t.Fatal(worked, err)
	}
	if analysisPostQuoted(f.bodies[0]) != "latest historical result" {
		t.Fatal("old historical result was spammed")
	}
	var oldRows int
	if err := f.service.Store.DB.QueryRow("SELECT COUNT(*) FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&oldRows); err != nil || oldRows != 0 {
		t.Fatal("old historical result entered backfill", err)
	}
	third := f.save(t, 1, 7, "future calculation", f.now.Add(2*time.Minute))
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil || f.calls != 2 {
		t.Fatal("future calculation missing", worked, err)
	}
	if f.state(t, second.ID) != "sent" || f.state(t, third.ID) != "sent" {
		t.Fatal("new calculation overwrote history")
	}
}

func TestAnalysisPostsPreserveUnavailableStatusAsLastPost(t *testing.T) {
	f := newAnalysisPostFixture(t, "saved response")
	f.ready(t)
	ctx := context.Background()
	db := f.service.Store.DB
	if _, err := db.Exec(profilePresenceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(1,7,'unavailable','2026-11-09','2026-11-09')"); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 0 {
		t.Fatal("GPT post displaced unavailable status", worked, err)
	}
	if _, err := db.Exec("UPDATE profile_presence SET availability='available'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,state) VALUES(1,7,1,'available','2026-11-09','pending')"); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"pending", "processing"} {
		if _, err := db.Exec("UPDATE profile_presence_events SET state=?", state); err != nil {
			t.Fatal(err)
		}
		if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 0 {
			t.Fatal("GPT post overtook presence notification", state, worked, err)
		}
	}
	if _, err := db.Exec("UPDATE profile_presence_events SET state='sent'"); err != nil {
		t.Fatal(err)
	}
	// Another account's availability cannot suppress the owner's saved result.
	if _, err := db.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(2,7,'unavailable','2026-11-09','2026-11-09')"); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil || f.calls != 1 {
		t.Fatal("available owner's post stayed blocked", worked, err)
	}
}

func TestAnalysisPostsFailuresAndRestartAreNotBlindlyReplayed(t *testing.T) {
	for _, mode := range []string{"transport", "receipt", "400", "429", "restart_sending", "restart_preparing"} {
		t.Run(mode, func(t *testing.T) {
			f := newAnalysisPostFixture(t, "saved response")
			f.ready(t)
			ctx := context.Background()
			f.respond = func(r *http.Request) (*http.Response, error) {
				switch mode {
				case "transport":
					return nil, errors.New("lost response")
				case "receipt":
					return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":501,"message_thread_id":999,"chat":{"id":-100123}}}`), nil
				case "400":
					return albumTestResponse(r, 400, `{"ok":false,"error_code":400,"description":"invalid payload"}`), nil
				case "429":
					if f.calls == 1 {
						return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"description":"slow down","parameters":{"retry_after":1}}`), nil
					}
				}
				return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":501,"message_thread_id":55,"chat":{"id":-100123}}}`), nil
			}
			if strings.HasPrefix(mode, "restart_") {
				if err := f.service.seedAnalysisPosts(ctx); err != nil {
					t.Fatal(err)
				}
				state := strings.TrimPrefix(mode, "restart_")
				if _, err := f.service.Store.DB.Exec("UPDATE analysis_posts SET state=?", state); err != nil {
					t.Fatal(err)
				}
				if err := f.service.recoverAnalysisPosts(ctx); err != nil {
					t.Fatal(err)
				}
			}
			worked, err := f.service.deliverAnalysisPost(ctx)
			want := "uncertain"
			switch mode {
			case "400":
				want = "error"
			case "429", "restart_preparing":
				want = "sent"
			}
			if f.state(t, f.analysis.ID) != want {
				t.Fatal("wrong outcome", f.state(t, f.analysis.ID), want, worked, err)
			}
			if mode == "429" && f.calls != 2 {
				t.Fatal("confirmed 429 not retried safely", f.calls)
			}
			if mode == "restart_sending" && f.calls != 0 {
				t.Fatal("crashed send replayed")
			}
			before := f.calls
			if worked, err := f.service.deliverAnalysisPost(ctx); worked || err != nil || f.calls != before {
				t.Fatal("completed or unknown delivery repeated", worked, err)
			}
		})
	}
}

func TestAnalysisPostLongContentAndExplicitRating(t *testing.T) {
	f := newAnalysisPostFixture(t, strings.Repeat("😀", 2200)+"\nОценка совместимости: 75%")
	payload, err := analysisPostText(f.analysis)
	if err != nil {
		t.Fatal(err)
	}
	quoted := analysisPostQuoted(payload)
	var original struct {
		Content string `json:"content"`
	}
	if err = json.Unmarshal(f.analysis.ResponseBody, &original); err != nil {
		t.Fatal(err)
	}
	if textUnits(payload.Text) > 4096 || !strings.HasPrefix(original.Content, quoted) || len(quoted) == len(original.Content) || !strings.Contains(payload.Text, "полный JSON по кнопке") || payload.Keyboard == nil {
		t.Fatal("long result lost preservation path")
	}
	markup, _ := json.Marshal(payload.Keyboard)
	if !bytes.Contains(markup, []byte(fmt.Sprintf("ga:%d", f.analysis.ID))) {
		t.Fatal("missing full JSON callback")
	}
	for _, test := range []struct {
		content string
		value   int
		known   bool
	}{
		{"8. **Итог**: Оценка совместимости — 75%.", 75, true},
		{"**Оценка совместимости**: **75%**", 75, true},
		{"Оценка совместимости: 0%", 0, true},
		{"Оценка совместимости: 100%", 100, true},
		{"Оценка совместимости: 101%", 0, false},
		{"Оценка совместимости: -5%", 0, false},
		{"Оценка совместимости: 75%. Оценка совместимости: 75%", 75, true},
		{"Оценка совместимости: 75%. Оценка совместимости: 70%", 0, false},
		{"Наш анализ 75% и 25%", 0, false},
	} {
		value, known := analysisPostRating(test.content)
		if value != test.value || known != test.known {
			t.Fatalf("rating %q = %d,%v", test.content, value, known)
		}
	}
	// Old imports can contain an underage snapshot even if today's stored profile is adult.
	var target Profile
	if err = json.Unmarshal(f.analysis.TargetProfile, &target); err != nil {
		t.Fatal(err)
	}
	target["age"] = 17
	f.analysis.TargetProfile, _ = json.Marshal(target)
	if _, err := analysisPostText(f.analysis); err == nil {
		t.Fatal("legacy underage snapshot was published")
	}
}

func TestAnalysisPostsRecheckBindingAfterConfirmed429(t *testing.T) {
	f := newAnalysisPostFixture(t, "saved response")
	f.ready(t)
	ctx := context.Background()
	original, err := f.service.Store.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.respond = func(r *http.Request) (*http.Response, error) {
		if f.calls == 1 {
			replacement := original
			replacement.UserID = 999
			replacement.Generation = "new-account"
			if err := f.service.Store.SaveSession(ctx, 1, replacement); err != nil {
				t.Fatal(err)
			}
			return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"description":"slow down","parameters":{"retry_after":1}}`), nil
		}
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":502,"message_thread_id":55,"chat":{"id":-100123}}}`), nil
	}
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err == nil || f.calls != 1 || f.state(t, f.analysis.ID) != "pending" {
		t.Fatal("429 retry crossed account binding", worked, err, f.calls)
	}
	if err := f.service.Store.SaveSession(ctx, 1, original); err != nil {
		t.Fatal(err)
	}
	f.ticks.Add(100)
	if worked, err := f.service.deliverAnalysisPost(ctx); !worked || err != nil || f.calls != 2 || f.state(t, f.analysis.ID) != "sent" {
		t.Fatal("known-rejected post did not resume safely", worked, err, f.calls)
	}
}

func TestAnalysisPostsReuseGlobalArchiveDuplicateWithoutReusingForeignGPT(t *testing.T) {
	for _, mode := range []string{"same_public_profile", "different_fingerprint", "different_thread"} {
		t.Run(mode, func(t *testing.T) {
			f := newAnalysisPostFixture(t, "own private GPT result")
			f.ready(t)
			ctx := context.Background()
			db := f.service.Store.DB
			original := compatibilityAdd(t, f.service, 2, 7, 23)
			if _, err := db.Exec("UPDATE archive_versions SET state='duplicate' WHERE version_id=?", f.analysis.VersionID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("DELETE FROM archive_profile_repairs WHERE version_id=?", f.analysis.VersionID); err != nil {
				t.Fatal(err)
			}
			_, err := db.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids)
 SELECT id,owner_id,profile_id,fingerprint,'sent',55,'[99]' FROM versions WHERE id=?`, original.VersionID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]','[99]')", original.VersionID); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "different_fingerprint":
				if _, err = db.Exec("UPDATE archive_versions SET fingerprint='other-public-profile' WHERE version_id=?", original.VersionID); err != nil {
					t.Fatal(err)
				}
			case "different_thread":
				if _, err = db.Exec("UPDATE archive_versions SET thread_id=66 WHERE version_id=?", original.VersionID); err != nil {
					t.Fatal(err)
				}
			}
			worked, err := f.service.deliverAnalysisPost(ctx)
			if err != nil {
				t.Fatal(err)
			}
			allowed := mode == "same_public_profile"
			if worked != allowed || f.calls != map[bool]int{true: 1, false: 0}[allowed] {
				t.Fatal("wrong shared archive resolution", worked, f.calls, allowed)
			}
			if allowed && analysisPostQuoted(f.bodies[0]) != "own private GPT result" {
				t.Fatal("shared archive reused another owner's GPT result")
			}
		})
	}
}
