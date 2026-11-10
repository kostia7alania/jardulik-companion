package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type operationsFixture struct {
	s       *Service
	now     time.Time
	calls   []string
	bodies  []map[string]json.RawMessage
	respond func(*http.Request, string, map[string]json.RawMessage) (*http.Response, error)
}

func newOperationsFixture(t *testing.T) *operationsFixture {
	t.Helper()
	c, store := archiveTestStore(t)
	c.ArchivePaused = true
	tg := NewTelegram(c)
	f := &operationsFixture{s: &Service{Config: c, Store: store}, now: time.Date(2026, 11, 10, 20, 0, 0, 0, time.UTC)}
	f.s.Archiver = NewArchiver(c, store, tg, nil)
	f.s.Archiver.now = func() time.Time { return f.now }
	tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		method := filepath.Base(r.URL.Path)
		if method != "createForumTopic" && method != "sendMessage" && method != "editMessageText" && method != "pinChatMessage" {
			t.Fatal("unexpected external action", method)
		}
		var input map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		f.calls = append(f.calls, method)
		f.bodies = append(f.bodies, input)
		var chat int64
		_ = json.Unmarshal(input["chat_id"], &chat)
		if chat != c.BackupChatID {
			t.Fatal("foreign chat", chat)
		}
		probe, cancel := context.WithTimeout(r.Context(), time.Second)
		var one int
		err := store.DB.QueryRowContext(probe, "SELECT 1").Scan(&one)
		cancel()
		if err != nil {
			t.Fatal("HTTP held SQLite transaction", err)
		}
		if f.respond != nil {
			return f.respond(r, method, input)
		}
		return f.response(r, method, input), nil
	})
	if err := f.s.initOperationsChannel(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *operationsFixture) response(r *http.Request, method string, input map[string]json.RawMessage) *http.Response {
	if method == "createForumTopic" {
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_thread_id":811}}`)
	}
	if method == "pinChatMessage" {
		return albumTestResponse(r, 200, `{"ok":true,"result":true}`)
	}
	var message int64
	_ = json.Unmarshal(input["message_id"], &message)
	if message == 0 {
		message = 900 + int64(len(f.calls))
	}
	return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":811,"chat":{"id":-100123}}}`, message))
}

func (f *operationsFixture) step(t *testing.T) (bool, error) {
	t.Helper()
	if err := f.s.Store.SetMeta(context.Background(), "archive_next_send_ns", "0"); err != nil {
		t.Fatal(err)
	}
	return f.s.deliverOperations(context.Background())
}
func (f *operationsFixture) ready(t *testing.T) {
	t.Helper()
	for range 3 {
		if did, err := f.step(t); !did || err != nil {
			t.Fatal(did, err)
		}
	}
	if fmt.Sprint(f.calls) != "[createForumTopic sendMessage pinChatMessage]" {
		t.Fatal(f.calls)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE operations_channel SET status_next=?", f.now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
}
func (f *operationsFixture) addEvent(t *testing.T) {
	t.Helper()
	_, err := f.s.Store.DB.Exec(`INSERT INTO operations_events(owner_id,chat_id,fingerprint,role,level,message,fields_json,first_at_ms,last_at_ms,occurrence_count)
 VALUES(123,-100123,'fixture','poster','ERROR','publication unavailable','{"kind":"operation_failed","target":7}',?,?,1)`, f.now.UnixMilli(), f.now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
}

func TestOperationsStatusSurvivesDisconnectedPausedSourceAndEditsSamePost(t *testing.T) {
	f := newOperationsFixture(t)
	f.ready(t)
	var before int64
	if err := f.s.Store.DB.QueryRow("SELECT status_message_id FROM operations_channel").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if did, err := f.step(t); did || err != nil {
		t.Fatal("unchanged snapshot created message", did, err)
	}
	f.now = f.now.Add(2 * time.Hour)
	if did, err := f.step(t); !did || err != nil {
		t.Fatal(did, err)
	}
	if f.calls[len(f.calls)-1] != "editMessageText" {
		t.Fatal("status was resent", f.calls)
	}
	var after int64
	if err := f.s.Store.DB.QueryRow("SELECT status_message_id FROM operations_channel").Scan(&after); err != nil || before != after {
		t.Fatal(before, after, err)
	}
	var text string
	_ = json.Unmarshal(f.bodies[1]["text"], &text)
	if !strings.Contains(text, "не подключён") || !strings.Contains(text, "UTC+3") || !strings.Contains(text, "(вт,") {
		t.Fatal("status lost state/time", text)
	}
}

func TestOperationsUnknownNewTopicAndStatusNeverResend(t *testing.T) {
	for _, kind := range []string{"topic", "status"} {
		t.Run(kind, func(t *testing.T) {
			f := newOperationsFixture(t)
			if kind == "status" {
				if did, err := f.step(t); !did || err != nil {
					t.Fatal(did, err)
				}
			}
			f.respond = func(*http.Request, string, map[string]json.RawMessage) (*http.Response, error) {
				return nil, errors.New("response lost")
			}
			if did, err := f.step(t); !did || err == nil {
				t.Fatal(did, err)
			}
			before := len(f.calls)
			if err := f.s.recoverOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(time.Hour)
			if did, err := f.step(t); did || err != nil || len(f.calls) != before {
				t.Fatal("unknown send repeated", did, err, f.calls)
			}
		})
	}
}

func TestOperationsEventCountsEditKnownMessageAndKeepConcurrentOccurrences(t *testing.T) {
	f := newOperationsFixture(t)
	f.ready(t)
	f.addEvent(t)
	f.respond = func(r *http.Request, method string, input map[string]json.RawMessage) (*http.Response, error) {
		if method == "sendMessage" {
			if _, err := f.s.Store.DB.Exec("UPDATE operations_events SET occurrence_count=2,last_at_ms=?", f.now.Add(time.Second).UnixMilli()); err != nil {
				t.Fatal(err)
			}
		}
		return f.response(r, method, input), nil
	}
	if did, err := f.step(t); !did || err != nil {
		t.Fatal(did, err)
	}
	var state string
	var posted, message int64
	if err := f.s.Store.DB.QueryRow("SELECT state,posted_count,message_id FROM operations_events").Scan(&state, &posted, &message); err != nil || state != "pending" || posted != 1 || message <= 0 {
		t.Fatal(state, posted, message, err)
	}
	if did, err := f.step(t); !did || err != nil {
		t.Fatal(did, err)
	}
	if f.calls[len(f.calls)-1] != "editMessageText" {
		t.Fatal("repeat occurrence resent", f.calls)
	}
	if err := f.s.Store.DB.QueryRow("SELECT state,posted_count,message_id FROM operations_events").Scan(&state, &posted, &message); err != nil || state != "sent" || posted != 2 {
		t.Fatal(state, posted, message, err)
	}
	var text string
	_ = json.Unmarshal(f.bodies[len(f.bodies)-1]["text"], &text)
	if !strings.Contains(text, "×2") || !strings.Contains(text, "UTC+3") {
		t.Fatal(text)
	}
}

func TestOperationsNewEventAcceptedButACKFailureNeverResends(t *testing.T) {
	f := newOperationsFixture(t)
	f.ready(t)
	f.addEvent(t)
	if _, err := f.s.Store.DB.Exec(`CREATE TRIGGER fail_operations_ack BEFORE UPDATE OF message_id ON operations_events WHEN NEW.message_id>0 BEGIN SELECT RAISE(FAIL,'ack unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if did, err := f.step(t); !did || err == nil {
		t.Fatal(did, err)
	}
	before := len(f.calls)
	if _, err := f.s.Store.DB.Exec("UPDATE operations_events SET occurrence_count=2"); err != nil {
		t.Fatal(err)
	}
	if err := f.s.recoverOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if did, err := f.step(t); did || err != nil || len(f.calls) != before {
		t.Fatal("accepted unknown event repeated", did, err, f.calls)
	}
	var state string
	if err := f.s.Store.DB.QueryRow("SELECT state FROM operations_events").Scan(&state); err != nil || state != "uncertain" {
		t.Fatal(state, err)
	}
}

func TestOperationsCannotPublishToChangedOwnerOrChat(t *testing.T) {
	for _, field := range []string{"owner", "chat"} {
		t.Run(field, func(t *testing.T) {
			f := newOperationsFixture(t)
			if field == "owner" {
				f.s.Config.OwnerID++
			} else {
				f.s.Config.BackupChatID--
			}
			if did, err := f.step(t); did || err != nil || len(f.calls) != 0 {
				t.Fatal("changed binding published", did, err, f.calls)
			}
		})
	}
}

func TestOperationsRejectsForeignTopicAndMessageReceipts(t *testing.T) {
	for _, kind := range []string{"topic", "message"} {
		t.Run(kind, func(t *testing.T) {
			f := newOperationsFixture(t)
			if kind == "topic" {
				if _, err := f.s.Store.DB.Exec(`INSERT INTO operations_channel(owner_id,chat_id,thread_id,topic_state) VALUES(999,-100123,811,'sent')`); err != nil {
					t.Fatal(err)
				}
			} else {
				if did, err := f.step(t); !did || err != nil {
					t.Fatal(did, err)
				}
				f.respond = func(r *http.Request, _ string, _ map[string]json.RawMessage) (*http.Response, error) {
					return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":1000,"message_thread_id":777,"chat":{"id":-100123}}}`), nil
				}
			}
			if did, err := f.step(t); !did || err == nil {
				t.Fatal("foreign receipt accepted", did, err)
			}
			before := len(f.calls)
			if err := f.s.recoverOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			if did, err := f.step(t); did || err != nil || len(f.calls) != before {
				t.Fatal("foreign unconfirmed action repeated", did, err, f.calls)
			}
		})
	}
}

func TestOperationsStatusReportsFailedCaptionInsteadOfHidingBlocker(t *testing.T) {
	f := newOperationsFixture(t)
	if _, err := f.s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(id,revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id,state,error) VALUES(71,?,'archive',1,123,1,'[5]',-100123,811,'failed','receipt_missing_or_changed')`, captionDesignRevision); err != nil {
		t.Fatal(err)
	}
	text, err := f.s.OperationsStatus(context.Background())
	if err != nil || !strings.Contains(text, "Оформление: 0/1 · ⚠️ 1") || !strings.Contains(text, "Правка #71") || !strings.Contains(text, "receipt_missing_or_changed") || len(f.calls) != 0 {
		t.Fatal(text, err, f.calls)
	}
}

func TestOperationsStatusReadsSearchProgressWithoutMovingCursor(t *testing.T) {
	for _, state := range []string{"running", "done", "paused"} {
		t.Run(state, func(t *testing.T) {
			f := newOperationsFixture(t)
			if _, err := f.s.Store.DB.Exec(crawlSchema); err != nil {
				t.Fatal(err)
			}
			reason := ""
			if state == "paused" {
				reason = "source_paused"
			}
			next := f.now.Add(time.Minute).UnixMilli()
			if _, err := f.s.Store.DB.Exec(`INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,include_locations,unfiltered,mode,page,next_request,state,reason,catalog_profiles,hydrated,unavailable,started_at,updated_at)
 VALUES(123,'status-source',5696,'source','{}',1,1,1,3,742,?,?,?,741,735,6,1,1)`, next, state, reason); err != nil {
				t.Fatal(err)
			}
			if _, err := f.s.Store.DB.Exec(`CREATE TRIGGER forbid_status_search_update BEFORE UPDATE ON feed_crawls BEGIN SELECT RAISE(FAIL,'status must not update source cursor');END`); err != nil {
				t.Fatal(err)
			}
			text, err := f.s.OperationsStatus(context.Background())
			if err != nil || len(f.calls) != 0 {
				t.Fatal(text, err, f.calls)
			}
			switch state {
			case "done":
				if !strings.Contains(text, "Поиск: завершён · 741 анкет") {
					t.Fatal(text)
				}
			case "running":
				if !strings.Contains(text, "mode=3 · страница 742 · полных 735/741") || !strings.Contains(text, "следующая: 🕒") {
					t.Fatal(text)
				}
			case "paused":
				if !strings.Contains(text, "source_paused") {
					t.Fatal(text)
				}
			}
			var page, after int64
			if err := f.s.Store.DB.QueryRow("SELECT page,next_request FROM feed_crawls WHERE owner_id=123").Scan(&page, &after); err != nil || page != 742 || after != next {
				t.Fatal(page, after, err)
			}
		})
	}
}

func TestOperationsStatusIncludesOpeningTimelineWhileArchivePaused(t *testing.T) {
	f := newOperationsFixture(t)
	if _, err := f.s.Store.DB.Exec(openingTimelineSchema); err != nil {
		t.Fatal(err)
	}
	for i, state := range []string{"sent", "pending", "error"} {
		action, err := f.s.Store.DB.Exec(`INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(123,?,'Like','succeeded','{}')`, 10+i)
		if err != nil {
			t.Fatal(err)
		}
		id, err := action.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		chat := f.s.Config.BackupChatID
		if i == 2 {
			chat = -100999
		}
		if _, err = f.s.Store.DB.Exec(`INSERT INTO opening_timeline_posts(owner_id,upstream_id,action_id,profile_id,chat_id,thread_id,attempted_at_ms,source_json,source_sha,payload_json,payload_sha,state)
 VALUES(123,5696,?,?,?,811,1,'{}','source','{}','payload',?)`, id, 10+i, chat, state); err != nil {
			t.Fatal(err)
		}
	}
	text, err := f.s.OperationsStatus(context.Background())
	if err != nil || !strings.Contains(text, "💬 Открывашки в анкетах: 1/2") || strings.Contains(text, "💬 Открывашки в анкетах: 1/3") || len(f.calls) != 0 {
		t.Fatal(text, err, f.calls)
	}
}
