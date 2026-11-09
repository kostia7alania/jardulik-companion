package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func threadNoticeRowHash(t *testing.T, s *Store, query string, args ...any) string {
	t.Helper()
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing notice fixture row")
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err = rows.Scan(pointers...); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	return digest(raw)
}

func threadChangeMaintenanceFixture(t *testing.T) (*Service, *captionMaintenanceJob) {
	t.Helper()
	f := newArchiveChangeFixture(t)
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "South Ossetia", "city": "Tskhinval"}, "2026-11-08 10:00:00", 55)
	v := archiveChangePublish(t, f, 123, Profile{"userId": 99, "country": "Russia", "city": "Vladikavkaz"}, "2026-11-08 11:00:00", 55)
	if err := f.service.enqueueArchiveChangePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Store.DB.Exec("UPDATE archive_change_posts SET state='sent',message_id=901,notice_text='old notice' WHERE version_id=?", v); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Store.DB.Exec("UPDATE versions SET first_seen='2026-11-07 22:10:00',last_seen='2026-11-09 12:00:00' WHERE id=?", v); err != nil {
		t.Fatal(err)
	}
	return f.service, &captionMaintenanceJob{kind: "change", receipt: v, version: v, owner: 123, chat: f.service.Config.BackupChatID, thread: 55, inventory: "[901]"}
}

func threadPresenceMaintenanceFixture(t *testing.T, availability, reason, changes string) (*Service, *captionMaintenanceJob) {
	t.Helper()
	f := newArchiveChangeFixture(t)
	s := f.service
	if _, err := s.Store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(99,55)"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(123,99,'unavailable','2026-11-07','2026-11-07')"); err != nil {
		t.Fatal(err)
	}
	r, err := s.Store.DB.Exec(`INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,state,chat_id,thread_id,message_id,reason,version_id,changes_text)
 VALUES(123,99,1,?,'2026-11-07T22:10:00Z','sent',?,55,902,?,0,?)`, availability, s.Config.BackupChatID, reason, changes)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return s, &captionMaintenanceJob{kind: "presence", receipt: id, version: 0, owner: 123, chat: s.Config.BackupChatID, thread: 55, inventory: "[902]"}
}

func threadNoticeConfirmedEdit(t *testing.T, s *Service, j *captionMaintenanceJob, method string, payload map[string]any) {
	t.Helper()
	calls := 0
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if !strings.HasSuffix(r.URL.Path, "/editMessageText") {
			t.Fatal("notice maintenance attempted send/upload/delete or another edit")
		}
		var body struct {
			Chat    int64  `json:"chat_id"`
			Message int64  `json:"message_id"`
			Text    string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Chat != j.chat || body.Message != payload["message_id"].(int64) || body.Text != payload["text"] {
			t.Fatal("notice target/text changed")
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d},"message_thread_id":%d}}`, body.Message, body.Chat, j.thread)), nil
	})
	var ack SentMessage
	if err := s.TG.Call(context.Background(), method, payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.ID != payload["message_id"] || ack.Chat.ID != j.chat || ack.ThreadID != j.thread {
		t.Fatal("edit ACK is unconfirmed")
	}
	if err := s.saveThreadNoticeEdit(context.Background(), j, payload); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("save sent another Telegram request")
	}
}

func TestThreadNoticeChangeMaintenanceOnlyEditsKnownIDAndKeepsSource(t *testing.T) {
	s, j := threadChangeMaintenanceFixture(t)
	ctx := context.Background()
	sourceBefore := threadNoticeRowHash(t, s.Store, "SELECT * FROM versions WHERE id=?", j.version)
	method, payload, err := s.prepareThreadNoticeEdit(ctx, j, []int64{901})
	if err != nil {
		t.Fatal(err)
	}
	text := payload["text"].(string)
	if method != "editMessageText" || payload["message_id"] != int64(901) || !strings.Contains(text, "Страна: Южная Осетия → Россия") || !strings.Contains(text, "Снимок: 🕒 8.11.26, 01:10 (вс, UTC+3)") || strings.Count(text, "🕒") != 1 {
		t.Fatal("change notice did not use original snapshot time/current clock formatter")
	}
	if payload["link_preview_options"].(map[string]bool)["is_disabled"] != true {
		t.Fatal("edit enabled link previews")
	}
	markup, _ := json.Marshal(payload["reply_markup"])
	if string(markup) != `{"inline_keyboard":[]}` {
		t.Fatal("short notice did not clear obsolete keyboard")
	}
	threadNoticeConfirmedEdit(t, s, j, method, payload)
	var stored string
	var message int64
	if err = s.Store.DB.QueryRow("SELECT notice_text,message_id FROM archive_change_posts WHERE version_id=?", j.receipt).Scan(&stored, &message); err != nil {
		t.Fatal(err)
	}
	if stored != text || message != 901 || sourceBefore != threadNoticeRowHash(t, s.Store, "SELECT * FROM versions WHERE id=?", j.version) {
		t.Fatal("change edit altered identity/raw source instead of only cached notice")
	}
}

func TestThreadNoticePresenceSharedPureTextAndNoEventMutation(t *testing.T) {
	for _, tc := range []struct{ availability, reason, changes, title string }{
		{"unavailable", "direct_unavailable", "", "Анкета недоступна"},
		{"unavailable", "feed_absent", "", "Анкета в этом обходе не обнаружена"},
		{"available", "direct_unavailable", "country: old → new", "Анкета снова доступна"},
	} {
		t.Run(tc.title, func(t *testing.T) {
			s, j := threadPresenceMaintenanceFixture(t, tc.availability, tc.reason, tc.changes)
			before := threadNoticeRowHash(t, s.Store, "SELECT * FROM profile_presence_events WHERE id=?", j.receipt)
			method, payload, err := s.prepareThreadNoticeEdit(context.Background(), j, []int64{902})
			if err != nil {
				t.Fatal(err)
			}
			text := payload["text"].(string)
			if method != "editMessageText" || text != profilePresenceText(99, tc.availability, "2026-11-07T22:10:00Z", tc.reason, tc.changes) || !strings.HasPrefix(text, tc.title+"\n") || !strings.Contains(text, "Обнаружено: 🕒 8.11.26, 01:10 (вс, UTC+3)") {
				t.Fatal("presence delivery/edit format diverged")
			}
			threadNoticeConfirmedEdit(t, s, j, method, payload)
			if before != threadNoticeRowHash(t, s.Store, "SELECT * FROM profile_presence_events WHERE id=?", j.receipt) {
				t.Fatal("edit mutated historical presence event")
			}
		})
	}
}

func TestThreadNoticeMaintenanceRejectsWrongOwnerReceiptAndPayload(t *testing.T) {
	for _, kind := range []string{"change", "presence"} {
		for _, bad := range []string{"owner", "chat", "thread", "service_topic", "message", "inventory", "version", "state", "mirror"} {
			t.Run(kind+"/"+bad, func(t *testing.T) {
				var s *Service
				var j *captionMaintenanceJob
				id := int64(901)
				if kind == "change" {
					s, j = threadChangeMaintenanceFixture(t)
				} else {
					s, j = threadPresenceMaintenanceFixture(t, "unavailable", "direct_unavailable", "")
					id = 902
				}
				ids := []int64{id}
				switch bad {
				case "owner":
					j.owner = 456
				case "chat":
					j.chat = -100999
				case "thread":
					j.thread = 56
				case "service_topic":
					j.thread = s.Config.BackupThreadID
				case "message":
					ids[0]++
					j.inventory = fmt.Sprintf("[%d]", ids[0])
				case "inventory":
					j.inventory = "[0]"
				case "version":
					j.version = 999999
				case "state":
					if kind == "change" {
						_, _ = s.Store.DB.Exec("UPDATE archive_change_posts SET state='uncertain'")
					} else {
						_, _ = s.Store.DB.Exec("UPDATE profile_presence_events SET state='uncertain'")
					}
				case "mirror":
					_, _ = s.Store.DB.Exec("UPDATE mirror_topics SET thread_id=56")
				}
				if _, _, err := s.prepareThreadNoticeEdit(context.Background(), j, ids); !errors.Is(err, errCaptionMaintenanceReceipt) {
					t.Fatalf("invalid live notice receipt accepted: %v", err)
				}
			})
		}
	}
	s, j := threadChangeMaintenanceFixture(t)
	_, payload, err := s.prepareThreadNoticeEdit(context.Background(), j, []int64{901})
	if err != nil {
		t.Fatal(err)
	}
	payload["message_id"] = int64(999)
	if err = s.saveThreadNoticeEdit(context.Background(), j, payload); !errors.Is(err, errCaptionMaintenanceReceipt) {
		t.Fatal("save accepted a different message")
	}
}

func TestThreadNoticeSaveRejectsReceiptChangedAfterPrepare(t *testing.T) {
	for _, kind := range []string{"change", "presence"} {
		t.Run(kind, func(t *testing.T) {
			var s *Service
			var j *captionMaintenanceJob
			id := int64(901)
			if kind == "change" {
				s, j = threadChangeMaintenanceFixture(t)
			} else {
				s, j = threadPresenceMaintenanceFixture(t, "unavailable", "direct_unavailable", "")
				id = 902
			}
			_, payload, err := s.prepareThreadNoticeEdit(context.Background(), j, []int64{id})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "change" {
				_, err = s.Store.DB.Exec("UPDATE archive_change_posts SET message_id=999 WHERE version_id=?", j.receipt)
			} else {
				_, err = s.Store.DB.Exec("UPDATE profile_presence_events SET message_id=999 WHERE id=?", j.receipt)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = s.saveThreadNoticeEdit(context.Background(), j, payload); !errors.Is(err, errCaptionMaintenanceReceipt) {
				t.Fatal("saved against a changed delivery receipt")
			}
		})
	}
}
