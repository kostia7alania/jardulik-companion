package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type profileIndexesFixture struct {
	s                       *Service
	now                     time.Time
	ticks                   atomic.Int64
	operations              []string
	bodies                  []map[string]json.RawMessage
	respond                 func(*http.Request, string, map[string]json.RawMessage) (*http.Response, error)
	nextThread, nextMessage int64
}

func newProfileIndexesFixture(t *testing.T) *profileIndexesFixture {
	t.Helper()
	c, store := archiveTestStore(t)
	c.TelegramBase = "https://telegram.test"
	ctx := context.Background()
	if err := store.SaveSession(ctx, 123, Session{Token: "fixture-access", Refresh: "fixture-refresh", UserID: 5696, Generation: "index-generation", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAccount(ctx, 123, Profile{"userId": 5696, "name": "Own", "age": 33}); err != nil {
		t.Fatal(err)
	}
	tg := NewTelegram(c)
	a := NewArchiver(c, store, tg, nil)
	s := &Service{Config: c, Store: store, Archiver: a}
	f := &profileIndexesFixture{s: s, now: time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC), nextThread: 700, nextMessage: 2000}
	a.now = func() time.Time { return f.now.Add(time.Duration(f.ticks.Add(1)) * 10 * time.Second) }
	tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		f.operations = append(f.operations, method)
		f.bodies = append(f.bodies, body)
		if f.respond != nil {
			return f.respond(r, method, body)
		}
		if method == "createForumTopic" {
			f.nextThread++
			return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_thread_id":%d}}`, f.nextThread)), nil
		}
		var thread, message int64
		_ = json.Unmarshal(body["message_thread_id"], &thread)
		_ = json.Unmarshal(body["message_id"], &message)
		if message == 0 {
			f.nextMessage++
			message = f.nextMessage
		} else {
			if err := store.DB.QueryRow("SELECT t.thread_id FROM profile_index_pages p JOIN profile_index_topics t ON t.id=p.topic_id WHERE p.message_id=?", message).Scan(&thread); err != nil {
				t.Fatal(err)
			}
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":%d,"chat":{"id":%d}}}`, message, thread, c.BackupChatID)), nil
	})
	if err := s.initProfileIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *profileIndexesFixture) add(t *testing.T, id, age int64, name, birthday string) StoredProfile {
	t.Helper()
	ctx := context.Background()
	ids, err := f.s.Store.Archive(ctx, 123, []Profile{{"userId": id, "name": name, "age": age, "birthday": birthday, "height": 170}}, "index-fixture")
	if err != nil {
		t.Fatal(err)
	}
	version := ids[0]
	thread := id + 100
	card := id + 1000
	zip := id + 900
	sha := fmt.Sprintf("fixture-source-%d-%d", id, version)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{"INSERT OR REPLACE INTO mirror_topics(profile_id,thread_id) VALUES(?,?)", []any{id, thread}},
		{`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha) VALUES(?,123,?,'fixture','sent',?,?,?)`, []any{version, id, thread, fmt.Sprintf("[%d]", card), sha}},
		{`INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]',?)`, []any{version, fmt.Sprintf("[%d]", card)}},
		{`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size) VALUES(?,'profile_version','profile.zip',10,?,?,?,'fixture-zip','fixture-unique',10)`, []any{sha, f.s.Config.BackupChatID, thread, zip}},
	} {
		if _, err = f.s.Store.DB.Exec(q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	p, err := f.s.Store.Profile(ctx, 123, id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f *profileIndexesFixture) analysis(t *testing.T, target StoredProfile, content string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	ss, err := f.s.Store.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(map[string]int64{"fromUserId": 5696, "targetUserId": target.ID})
	response, _ := json.Marshal(map[string]string{"model": "reported-model", "content": content})
	_, err = f.s.Store.SaveAnalysisCapture(ctx, 123, AnalysisCaptureInput{UpstreamID: 5696, ProfileID: target.ID, VersionID: target.VersionID, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: at.UnixMilli(), HTTPStatus: 200, State: "response_received", Generation: ss.Generation, RequestBody: request, ResponseBody: response, OwnProfile: ss.Account, TargetProfile: []byte(target.RawJSON)})
	if err != nil {
		t.Fatal(err)
	}
}
func (f *profileIndexesFixture) step(t *testing.T) {
	t.Helper()
	if worked, err := f.s.deliverProfileIndex(context.Background()); !worked || err != nil {
		t.Fatalf("index delivery %v %v", worked, err)
	}
}
func (f *profileIndexesFixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.s.Store.DB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestProfileIndexesCurrentOwnerConfirmedArchiveAndLatestRating(t *testing.T) {
	f := newProfileIndexesFixture(t)
	ctx := context.Background()
	a := f.add(t, 10, 23, "CurrentA", "2003-03-01")
	b := f.add(t, 11, 24, "CurrentB", "2002-03-01")
	f.analysis(t, a, "Оценка совместимости: 90%", f.now)
	f.analysis(t, a, "Оценка совместимости: 75%. Оценка совместимости: 80%", f.now.Add(time.Minute))
	f.analysis(t, b, "Оценка совместимости: 0%", f.now)
	// Retain the last confirmed card while a fresh version is pending/unavailable.
	if _, err := f.s.Store.Archive(ctx, 123, []Profile{{"userId": 10, "name": "Latest name", "age": 23, "birthday": "2003-03-01"}}, "index-new"); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Store.User(ctx, 456, "foreign"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.Archive(ctx, 456, []Profile{{"userId": 10, "name": "Foreign", "age": 18}}, "index-foreign"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(profilePresenceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(`INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(123,10,'unavailable','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.Archive(ctx, 123, []Profile{{"userId": 12, "name": "Not published", "age": 18}}, "no-card"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(12,112)"); err != nil {
		t.Fatal(err)
	}
	entries, err := f.s.profileIndexEntries(ctx, 123, 5696, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("wrong confirmed owner entries: %+v", entries)
	}
	byID := map[int64]profileIndexEntry{}
	for _, e := range entries {
		byID[e.ID] = e
	}
	if byID[10].Name != "Latest name" || byID[10].CardID != 1010 || byID[10].PercentKnown {
		t.Fatalf("stale, foreign, unavailable or ambiguous source handling wrong: %+v", byID[10])
	}
	if !byID[11].PercentKnown || byID[11].Percent != 0 {
		t.Fatal("known zero lost")
	}
	foreignSource, err := f.s.profileIndexScores(ctx, 123, 777)
	if err != nil || len(foreignSource) != 0 {
		t.Fatal("source score leakage", err)
	}
}

func TestProfileIndexesCreateTwoTopicsSendOnceAndEditExistingPages(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 10, 23, "Name🙂", "2003-03-01")
	f.analysis(t, p, "Оценка совместимости: 75%", f.now)
	for range 4 {
		f.step(t)
	}
	if f.count(t, "SELECT count(*) FROM profile_index_topics WHERE state='sent'") != 2 || f.count(t, "SELECT count(*) FROM profile_index_pages WHERE state='sent' AND message_id>0") != 2 {
		t.Fatal("missing topics/pages")
	}
	oldIDs := []int64{}
	rows, err := f.s.Store.DB.Query("SELECT message_id FROM profile_index_pages ORDER BY topic_id,page_no")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		oldIDs = append(oldIDs, id)
	}
	rows.Close()
	before := len(f.operations)
	if worked, err := f.s.deliverProfileIndex(context.Background()); worked || err != nil {
		t.Fatal("unchanged index made another post", worked, err)
	}
	if len(f.operations) != before {
		t.Fatal("unchanged external request")
	}
	// A later recalculation changes both payloads but never their message IDs.
	f.analysis(t, p, "Оценка совместимости: 81%", f.now.Add(2*time.Minute))
	f.now = f.now.Add(10 * time.Minute)
	f.step(t)
	f.step(t)
	gotIDs := []int64{}
	rows, err = f.s.Store.DB.Query("SELECT message_id FROM profile_index_pages ORDER BY topic_id,page_no")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		gotIDs = append(gotIDs, id)
	}
	rows.Close()
	if !reflect.DeepEqual(oldIDs, gotIDs) {
		t.Fatal("index edit created replacement posts", oldIDs, gotIDs)
	}
	if strings.Join(f.operations, ",") != "createForumTopic,createForumTopic,sendMessage,sendMessage,editMessageText,editMessageText" {
		t.Fatal(f.operations)
	}
	for _, b := range f.bodies[4:] {
		var text string
		_ = json.Unmarshal(b["text"], &text)
		if !strings.Contains(text, "81%") {
			t.Fatal("new score missing", text)
		}
	}
}

func TestProfileIndexesUnknownCreateOrSendNeverBlindlyRepeats(t *testing.T) {
	for _, failMethod := range []string{"createForumTopic", "sendMessage"} {
		t.Run(failMethod, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 10, 23, "Name", "2003-03-01")
			failed := false
			f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
				if method == failMethod && !failed {
					failed = true
					return nil, io.ErrUnexpectedEOF
				}
				if method == "createForumTopic" {
					f.nextThread++
					return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_thread_id":%d}}`, f.nextThread)), nil
				}
				var thread int64
				_ = json.Unmarshal(b["message_thread_id"], &thread)
				f.nextMessage++
				return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":%d,"chat":{"id":-100123}}}`, f.nextMessage, thread)), nil
			}
			for range 6 {
				_, _ = f.s.deliverProfileIndex(context.Background())
			}
			if failMethod == "createForumTopic" && f.count(t, "SELECT count(*) FROM profile_index_topics WHERE state='uncertain'") != 1 {
				t.Fatal("unknown topic not preserved")
			}
			if failMethod == "sendMessage" && f.count(t, "SELECT count(*) FROM profile_index_pages WHERE state='uncertain' AND message_id=0") != 1 {
				t.Fatal("unknown new post not preserved")
			}
			before := len(f.operations)
			if err := f.s.recoverProfileIndexes(context.Background()); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				_, _ = f.s.deliverProfileIndex(context.Background())
			}
			if len(f.operations) != before {
				t.Fatal("unknown effect replayed on restart", f.operations)
			}
		})
	}
}

func TestProfileIndexesKnownEditRetryAndNotModified(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 10, 23, "Name", "2003-03-01")
	for range 4 {
		f.step(t)
	}
	f.analysis(t, p, "Оценка совместимости: 75%", f.now)
	f.now = f.now.Add(10 * time.Minute)
	failures := 0
	f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
		if method != "editMessageText" {
			t.Fatal("edit became send", method)
		}
		failures++
		if failures == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		if failures == 2 {
			return albumTestResponse(r, 500, `{"ok":false,"error_code":500,"description":"Internal error"}`), nil
		}
		return albumTestResponse(r, 400, `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`), nil
	}
	if worked, err := f.s.deliverProfileIndex(context.Background()); !worked || err == nil {
		t.Fatal("missing retryable transport failure")
	}
	for range 3 {
		_, _ = f.s.deliverProfileIndex(context.Background())
	}
	if f.count(t, "SELECT count(*) FROM profile_index_pages WHERE state='sent' AND message_id>0") != 2 {
		t.Fatal("known edits did not recover")
	}
}

func TestProfileIndexesSourceFenceAndReceiptValidation(t *testing.T) {
	f := newProfileIndexesFixture(t)
	f.add(t, 10, 23, "Name", "2003-03-01")
	ctx := context.Background()
	ss, err := f.s.Store.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.seedProfileIndexTopics(ctx, ss); err != nil {
		t.Fatal(err)
	}
	if err = f.s.refreshProfileIndexPlan(ctx, ss); err != nil {
		t.Fatal(err)
	}
	op, claimed, err := f.s.claimProfileIndexOperation(ctx, ss)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	next := ss
	next.Generation = "new-generation"
	if err = f.s.Store.SaveSession(ctx, 123, next); err != nil {
		t.Fatal(err)
	}
	if err = f.s.markProfileIndexSending(ctx, op, ss); !errors.Is(err, errProfileIndexBinding) {
		t.Fatal("stale source was allowed", err)
	}
	if len(f.operations) != 0 {
		t.Fatal("source fence sent HTTP")
	}
	f = newProfileIndexesFixture(t)
	f.add(t, 10, 23, "Name", "2003-03-01")
	f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_thread_id":7}}`), nil
	}
	if worked, err := f.s.deliverProfileIndex(ctx); !worked || err == nil {
		t.Fatal("invalid topic receipt accepted")
	}
	if f.count(t, "SELECT count(*) FROM profile_index_topics WHERE state='uncertain' AND thread_id=0") != 1 {
		t.Fatal("invalid topic receipt retried")
	}
}

func TestProfileIndexesUnsentPageReturnsAfterShrinkAndGrow(t *testing.T) {
	f := newProfileIndexesFixture(t)
	ctx := context.Background()
	// Direct staged payloads reproduce page2 shrinking before its first send.
	ss, err := f.s.Store.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.seedProfileIndexTopics(ctx, ss); err != nil {
		t.Fatal(err)
	}
	var topic int64
	if err = f.s.Store.DB.QueryRow("SELECT id FROM profile_index_topics ORDER BY id LIMIT 1").Scan(&topic); err != nil {
		t.Fatal(err)
	}
	payload := profileIndexPayload{Text: "second page", Entities: []MessageEntity{}}
	tx, err := f.s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.stageProfileIndexPageTx(ctx, tx, topic, 2, payload); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE profile_index_pages SET state='error',error='unpublished_page_superseded' WHERE topic_id=? AND page_no=2", topic); err != nil {
		t.Fatal(err)
	}
	tx, err = f.s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.stageProfileIndexPageTx(ctx, tx, topic, 2, payload); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if f.count(t, "SELECT count(*) FROM profile_index_pages WHERE topic_id=? AND page_no=2 AND state='pending'", topic) != 1 {
		t.Fatal("same pending page was never revived")
	}
}

func TestProfileIndexesWrongEditAckKeepsKnownMessageID(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 10, 23, "Name", "2003-03-01")
	for range 4 {
		f.step(t)
	}
	var topic, known, thread int64
	if err := f.s.Store.DB.QueryRow(`SELECT t.id,p.message_id,t.thread_id FROM profile_index_topics t JOIN profile_index_pages p ON p.topic_id=t.id WHERE t.kind='compatibility' AND p.page_no=1`).Scan(&topic, &known, &thread); err != nil {
		t.Fatal(err)
	}
	f.analysis(t, p, "Оценка совместимости: 75%", f.now)
	f.now = f.now.Add(10 * time.Minute)
	wrong := int64(99999)
	f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
		if method != "editMessageText" {
			t.Fatal("update unexpectedly created a message", method)
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":%d,"chat":{"id":-100123}}}`, wrong, thread)), nil
	}
	if worked, err := f.s.deliverProfileIndex(context.Background()); !worked || !errors.Is(err, errProfileIndexReceipt) {
		t.Fatalf("invalid edit ACK accepted: worked=%v err=%v", worked, err)
	}
	var stored int64
	var state string
	if err := f.s.Store.DB.QueryRow("SELECT message_id,state FROM profile_index_pages WHERE topic_id=? AND page_no=1", topic).Scan(&stored, &state); err != nil {
		t.Fatal(err)
	}
	if stored != known || state != "uncertain" {
		t.Fatalf("bad ACK replaced known message ID: known=%d stored=%d state=%s", known, stored, state)
	}
	f.analysis(t, p, "Оценка совместимости: 81%", f.now.Add(time.Minute))
	f.now = f.now.Add(10 * time.Minute)
	var edited int64
	f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
		if method != "editMessageText" {
			t.Fatal("known update became new send", method)
		}
		if err := json.Unmarshal(b["message_id"], &edited); err != nil {
			t.Fatal(err)
		}
		if edited != known {
			t.Fatalf("later update targeted unconfirmed ID: %d, want %d", edited, known)
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":%d,"chat":{"id":-100123}}}`, known, thread)), nil
	}
	f.step(t)
	if edited != known {
		t.Fatal("known message was not edited")
	}
	if err := f.s.Store.DB.QueryRow("SELECT message_id,state FROM profile_index_pages WHERE topic_id=? AND page_no=1", topic).Scan(&stored, &state); err != nil {
		t.Fatal(err)
	}
	if stored != known || state != "sent" {
		t.Fatalf("recovered edit changed receipt: %d %s", stored, state)
	}
}

func TestProfileIndexesServerErrorNewEffectNeverRepeatsAfterChangeAndRestart(t *testing.T) {
	for _, failMethod := range []string{"createForumTopic", "sendMessage"} {
		t.Run(failMethod, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			p := f.add(t, 10, 23, "Name", "2003-03-01")
			failed := false
			var failedThread int64
			f.respond = func(r *http.Request, method string, b map[string]json.RawMessage) (*http.Response, error) {
				if method == failMethod && !failed {
					failed = true
					_ = json.Unmarshal(b["message_thread_id"], &failedThread)
					return albumTestResponse(r, 500, `{"ok":false,"error_code":500,"description":"Internal error"}`), nil
				}
				if method == "createForumTopic" {
					f.nextThread++
					return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_thread_id":%d}}`, f.nextThread)), nil
				}
				var thread, message int64
				_ = json.Unmarshal(b["message_thread_id"], &thread)
				_ = json.Unmarshal(b["message_id"], &message)
				if message == 0 {
					f.nextMessage++
					message = f.nextMessage
				} else if err := f.s.Store.DB.QueryRow("SELECT t.thread_id FROM profile_index_pages p JOIN profile_index_topics t ON t.id=p.topic_id WHERE p.message_id=?", message).Scan(&thread); err != nil {
					t.Fatal(err)
				}
				return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":%d,"chat":{"id":-100123}}}`, message, thread)), nil
			}
			for range 6 {
				_, _ = f.s.deliverProfileIndex(context.Background())
			}
			f.analysis(t, p, "Оценка совместимости: 81%", f.now)
			f.now = f.now.Add(10 * time.Minute)
			if err := f.s.recoverProfileIndexes(context.Background()); err != nil {
				t.Fatal(err)
			}
			for range 6 {
				_, _ = f.s.deliverProfileIndex(context.Background())
			}
			if failMethod == "createForumTopic" {
				creates := 0
				for _, method := range f.operations {
					if method == "createForumTopic" {
						creates++
					}
				}
				if creates != 2 || f.count(t, "SELECT count(*) FROM profile_index_topics WHERE state='uncertain' AND thread_id=0") != 1 {
					t.Fatal("unknown server-side topic effect was repeated", f.operations)
				}
			} else {
				sends := 0
				for i, method := range f.operations {
					var thread int64
					_ = json.Unmarshal(f.bodies[i]["message_thread_id"], &thread)
					if method == "sendMessage" && thread == failedThread {
						sends++
					}
				}
				if sends != 1 || f.count(t, "SELECT count(*) FROM profile_index_pages WHERE state='uncertain' AND message_id=0") != 1 {
					t.Fatal("unknown server-side new post effect was repeated", f.operations)
				}
			}
		})
	}
}

func TestProfileIndexesOwnDuplicateUsesConfirmedOriginalLinkOnly(t *testing.T) {
	f := newProfileIndexesFixture(t)
	ctx := context.Background()
	own := f.add(t, 10, 23, "Own name", "2003-03-01")
	f.analysis(t, own, "Оценка совместимости: 75%", f.now)
	if _, err := f.s.Store.DB.Exec("UPDATE archive_versions SET state='duplicate',message_ids='[]',source_sha='' WHERE version_id=?", own.VersionID); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Store.User(ctx, 456, "foreign"); err != nil {
		t.Fatal(err)
	}
	versions, err := f.s.Store.Archive(ctx, 456, []Profile{{"userId": 10, "name": "Foreign name", "age": 45, "birthday": "1981-01-01"}}, "foreign-original")
	if err != nil {
		t.Fatal(err)
	}
	foreign := versions[0]
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha) VALUES(?,456,10,'fixture','sent',110,'[3333]','foreign-confirmed')`, []any{foreign}},
		{`INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]','[3333]')`, []any{foreign}},
		{`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size) VALUES('foreign-confirmed','profile_version','profile.zip',10,?,110,3332,'foreign-zip','foreign-unique',10)`, []any{f.s.Config.BackupChatID}},
	} {
		if _, err = f.s.Store.DB.Exec(q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := f.s.profileIndexEntries(ctx, 123, 5696, f.now)
	if err != nil || len(entries) != 1 {
		t.Fatalf("confirmed duplicate omitted: %+v, %v", entries, err)
	}
	got := entries[0]
	if got.CardID != 3333 || got.Name != "Own name" || got.Age != 23 || got.Birthday != "2003-03-01" || !got.PercentKnown || got.Percent != 75 {
		t.Fatalf("foreign source fields leaked or original card lost: %+v", got)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE archive_versions SET fingerprint='different' WHERE version_id=?", foreign); err != nil {
		t.Fatal(err)
	}
	entries, err = f.s.profileIndexEntries(ctx, 123, 5696, f.now)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unrelated foreign publication linked: %+v, %v", entries, err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE archive_versions SET fingerprint='' WHERE profile_id=10"); err != nil {
		t.Fatal(err)
	}
	entries, err = f.s.profileIndexEntries(ctx, 123, 5696, f.now)
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty fingerprint treated as confirmed duplicate: %+v, %v", entries, err)
	}
}

func TestProfileIndexesZipRepairDoesNotReorderPublishedCards(t *testing.T) {
	f := newProfileIndexesFixture(t)
	first := f.add(t, 10, 23, "First", "2003-03-01")
	second := f.add(t, 10, 23, "Second", "2003-03-01")
	firstSHA := fmt.Sprintf("fixture-source-10-%d", first.VersionID)
	secondSHA := fmt.Sprintf("fixture-source-10-%d", second.VersionID)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{"UPDATE archive_receipts SET sent_at='2026-11-01 01:00:00' WHERE source_sha=?", []any{firstSHA}},
		{"UPDATE archive_receipts SET sent_at='2026-11-02 01:00:00' WHERE source_sha=?", []any{secondSHA}},
		{"UPDATE archive_versions SET message_ids='[2020]' WHERE version_id=?", []any{second.VersionID}},
		{"UPDATE archive_profile_repairs SET new_message_ids='[2020]' WHERE version_id=?", []any{second.VersionID}},
		{`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at) SELECT 'repaired-first',kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,'2026-11-03 01:00:00' FROM archive_receipts WHERE source_sha=?`, []any{firstSHA}},
		{`INSERT INTO archive_zip_repairs(version_id,state,old_source_sha,old_filename,old_message_id,new_source_sha) VALUES(?,'sent',?,'profile.zip',910,'repaired-first')`, []any{first.VersionID, firstSHA}},
		{"UPDATE archive_versions SET source_sha='repaired-first' WHERE version_id=?", []any{first.VersionID}},
	} {
		if _, err := f.s.Store.DB.Exec(q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := f.s.profileIndexEntries(context.Background(), 123, 5696, f.now)
	if err != nil || len(entries) != 1 || entries[0].CardID != 2020 {
		t.Fatalf("repaired ZIP replaced newer confirmed card link: %+v, %v", entries, err)
	}
}
