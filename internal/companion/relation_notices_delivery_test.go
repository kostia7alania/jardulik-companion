package companion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func newRelationNoticeDeliveryFixture(t *testing.T) *profileIndexesFixture {
	t.Helper()
	f := newProfileIndexesFixture(t)
	f.s.Config.RelationNoticesEnabled = true
	if err := f.s.initRelationNotices(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f
}

func addRelationNoticeDeliveryEvent(t *testing.T, f *profileIndexesFixture, owner, upstream, profile int64, kind string) int64 {
	t.Helper()
	result, err := f.s.Store.DB.Exec(`INSERT INTO relation_notice_events(owner_id,upstream_id,profile_id,kind,observed_at_ms) VALUES(?,?,?,?,?)`, owner, upstream, profile, kind, f.now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func readRelationNoticeDeliveryReceipt(t *testing.T, f *profileIndexesFixture, id int64) (state string, chat, thread, message int64) {
	t.Helper()
	if err := f.s.Store.DB.QueryRow(`SELECT state,chat_id,thread_id,message_id FROM relation_notice_events WHERE id=?`, id).Scan(&state, &chat, &thread, &message); err != nil {
		t.Fatal(err)
	}
	return
}

func TestRelationNoticeDeliveryUsesOnlyConfirmedOwnedThread(t *testing.T) {
	ctx := context.Background()
	f := newRelationNoticeDeliveryFixture(t)
	if err := f.s.Store.User(ctx, 124, "another tenant"); err != nil {
		t.Fatal(err)
	}
	f.add(t, 91, 31, "Archived", "")
	missing := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 90, "incoming")
	foreign := addRelationNoticeDeliveryEvent(t, f, 124, 5696, 91, "mutual")
	differentAccount := addRelationNoticeDeliveryEvent(t, f, 123, 5697, 91, "mutual")
	mutual := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
	incoming := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "incoming")

	for i, title := range []string{"💘 У вас взаимная симпатия!", "❤️ Вам поставили лайк"} {
		did, err := f.s.deliverRelationNotice(ctx)
		if err != nil || !did {
			t.Fatalf("deliver = %v, %v", did, err)
		}
		var body string
		var entities []MessageEntity
		if err := json.Unmarshal(f.bodies[i]["text"], &body); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(f.bodies[i]["entities"], &entities); err != nil {
			t.Fatal(err)
		}
		prefix := title + "\n" + displayTime(f.now.Format(time.RFC3339)) + "\n"
		if body != prefix+"Анкета" || len(entities) != 2 || entities[0].Type != "bold" || entities[1] != (MessageEntity{
			Type: "text_link", Offset: textUnits(prefix), Length: textUnits("Анкета"), URL: "https://t.me/c/123/191/1091",
		}) || validMessageEntities(body, entities) != nil {
			t.Fatalf("body = %q, entities = %#v", body, entities)
		}
	}
	if did, err := f.s.deliverRelationNotice(ctx); err != nil || did {
		t.Fatalf("unexpected repeat = %v, %v", did, err)
	}
	if !reflect.DeepEqual(f.operations, []string{"sendMessage", "sendMessage"}) {
		t.Fatalf("Telegram calls = %v", f.operations)
	}
	for _, id := range []int64{missing, foreign, differentAccount} {
		state, chat, thread, message := readRelationNoticeDeliveryReceipt(t, f, id)
		if state != "pending" || chat != 0 || thread != 0 || message != 0 {
			t.Fatalf("unready event %d = %q/%d/%d/%d", id, state, chat, thread, message)
		}
	}
	for i, id := range []int64{mutual, incoming} {
		state, chat, thread, message := readRelationNoticeDeliveryReceipt(t, f, id)
		if state != "sent" || chat != f.s.Config.BackupChatID || thread != 191 || message != 2001+int64(i) {
			t.Fatalf("receipt %d = %q/%d/%d/%d", id, state, chat, thread, message)
		}
	}
}

func TestRelationNoticeDeliveryRechecksBindingAndThread(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		err  error
	}{
		{"disconnected", "UPDATE sessions SET state='auth_required' WHERE owner_id=123", errOpeningPostBinding},
		{"different upstream", "UPDATE sessions SET upstream_id=5697 WHERE owner_id=123", nil},
		{"thread changed", "UPDATE mirror_topics SET thread_id=777 WHERE profile_id=91", errOpeningPostSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newRelationNoticeDeliveryFixture(t)
			f.add(t, 91, 31, "Archived", "")
			addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
			session, err := f.s.Store.Session(ctx, 123)
			if err != nil {
				t.Fatal(err)
			}
			post, claimed, err := f.s.claimRelationNotice(ctx, session)
			if err != nil || !claimed {
				t.Fatalf("claim = %v, %v", claimed, err)
			}
			if _, err := f.s.Store.DB.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			link, version, err := f.s.markRelationNoticeSending(ctx, post, session)
			if err == nil || (tc.err != nil && !errors.Is(err, tc.err)) || link != "" || version != 0 || len(f.operations) != 0 {
				t.Fatalf("unexpectedly ready to send, link=%q, err=%v, calls=%v", link, err, f.operations)
			}
		})
	}
}

func TestRelationNoticeDeliveryDoesNotReplayUncertain(t *testing.T) {
	ctx := context.Background()
	f := newRelationNoticeDeliveryFixture(t)
	f.add(t, 91, 31, "Archived", "")
	unknown := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
	interrupted := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "incoming")
	f.respond = func(*http.Request, string, map[string]json.RawMessage) (*http.Response, error) {
		return nil, errors.New("acknowledgement lost")
	}
	did, err := f.s.deliverRelationNotice(ctx)
	if !did || err == nil {
		t.Fatalf("unknown outcome = %v, %v", did, err)
	}
	if _, err := f.s.Store.DB.Exec(`UPDATE relation_notice_events SET state='sending',chat_id=?,thread_id=191 WHERE id=?`, f.s.Config.BackupChatID, interrupted); err != nil {
		t.Fatal(err)
	}
	if err := f.s.recoverRelationNoticeDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{unknown, interrupted} {
		state, chat, thread, message := readRelationNoticeDeliveryReceipt(t, f, id)
		if state != "uncertain" || chat != f.s.Config.BackupChatID || thread != 191 || message != 0 {
			t.Fatalf("unknown receipt %d = %q/%d/%d/%d", id, state, chat, thread, message)
		}
	}
	if did, err := f.s.deliverRelationNotice(ctx); did || err != nil || !reflect.DeepEqual(f.operations, []string{"sendMessage"}) {
		t.Fatalf("unknown outcome replayed: %v, %v, %v", did, err, f.operations)
	}
}

func TestRelationNoticeDeliveryHonorsPause(t *testing.T) {
	f := newRelationNoticeDeliveryFixture(t)
	f.add(t, 91, 31, "Archived", "")
	id := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
	f.s.Config.ArchivePaused = true
	if did, err := f.s.deliverRelationNotice(context.Background()); did || err != nil || len(f.operations) != 0 {
		t.Fatalf("paused delivery: %v, %v, %v", did, err, f.operations)
	}
	state, _, _, _ := readRelationNoticeDeliveryReceipt(t, f, id)
	if state != "pending" {
		t.Fatalf("paused event state = %q", state)
	}
}

func TestRelationNoticeDeliveryRejectsWrongTelegramThread(t *testing.T) {
	f := newRelationNoticeDeliveryFixture(t)
	f.add(t, 91, 31, "Archived", "")
	id := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
	f.respond = func(r *http.Request, method string, _ map[string]json.RawMessage) (*http.Response, error) {
		if method != "sendMessage" {
			t.Fatalf("Telegram method = %q", method)
		}
		return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":2001,"message_thread_id":999,"chat":{"id":-100123}}}`), nil
	}
	if did, err := f.s.deliverRelationNotice(context.Background()); !did || !errors.Is(err, errOpeningPostReceipt) {
		t.Fatalf("wrong Telegram thread = %v, %v", did, err)
	}
	state, _, _, message := readRelationNoticeDeliveryReceipt(t, f, id)
	if state != "uncertain" || message != 0 {
		t.Fatalf("wrong Telegram thread stored as %q/%d", state, message)
	}
}

func TestRelationNoticeDeliveryRetriesConfirmedRateLimit(t *testing.T) {
	f := newRelationNoticeDeliveryFixture(t)
	f.add(t, 91, 31, "Archived", "")
	id := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
	f.respond = func(r *http.Request, _ string, _ map[string]json.RawMessage) (*http.Response, error) {
		return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":5}}`), nil
	}
	if did, err := f.s.deliverRelationNotice(context.Background()); !did || err == nil {
		t.Fatalf("confirmed rate limit = %v, %v", did, err)
	}
	state, chat, thread, message := readRelationNoticeDeliveryReceipt(t, f, id)
	if state != "pending" || chat != f.s.Config.BackupChatID || thread != 191 || message != 0 || len(f.operations) != 3 {
		t.Fatalf("rate-limited receipt = %q/%d/%d/%d, calls=%v", state, chat, thread, message, f.operations)
	}
	f.respond = nil
	if did, err := f.s.deliverRelationNotice(context.Background()); !did || err != nil {
		t.Fatalf("retry after rate limit = %v, %v", did, err)
	}
	state, _, _, message = readRelationNoticeDeliveryReceipt(t, f, id)
	if state != "sent" || message != 2001 || len(f.operations) != 4 {
		t.Fatalf("retried receipt = %q/%d, calls=%v", state, message, f.operations)
	}
}

func TestRelationNoticeDeliveryUsesVerifiedPhotoFromConfirmedVersion(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "confirmed"
		if unknown {
			name = "unknown photo acknowledgement"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newRelationNoticeDeliveryFixture(t)
			p := f.add(t, 91, 31, "Archived", "")
			id := addRelationNoticeDeliveryEvent(t, f, 123, 5696, 91, "mutual")
			dir := filepath.Join(f.s.Config.DataDir, "media")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			addPhoto := func(version int64, position int, expected, actual string) {
				t.Helper()
				result, err := f.s.Store.DB.Exec(`INSERT INTO media_jobs(owner_id,version_id,position,role,url) VALUES(123,?,?,'photos','https://example.test/photo.jpg')`, version, position)
				if err != nil {
					t.Fatal(err)
				}
				jobID, err := result.LastInsertId()
				if err != nil {
					t.Fatal(err)
				}
				hash := digest([]byte(expected))
				path := filepath.Join(dir, hash)
				if err := os.WriteFile(path, []byte(actual), 0600); err != nil {
					t.Fatal(err)
				}
				if err := f.s.Store.MediaDone(ctx, MediaJob{ID: jobID}, hash, path, "image/jpeg", int64(len(expected))); err != nil {
					t.Fatal(err)
				}
			}
			addPhoto(p.VersionID, 0, "corrupted", "different")
			addPhoto(p.VersionID, 1, "confirmed-photo", "confirmed-photo")
			versions, err := f.s.Store.Archive(ctx, 123, []Profile{{"userId": p.ID, "name": "Not published yet"}}, "notice-fixture")
			if err != nil {
				t.Fatal(err)
			}
			addPhoto(versions[0], 0, "unpublished-photo", "unpublished-photo")

			calls := 0
			f.s.Archiver.TG = archiveFixtureTelegram(f.s.Config, func(call archiveTelegramCall) (string, error) {
				calls++
				prefix := "💘 У вас взаимная симпатия!\n" + displayTime(f.now.Format(time.RFC3339)) + "\n"
				if call.method != "sendPhoto" || call.fields["message_thread_id"] != "191" ||
					call.fields["photo"] != "confirmed-photo" || call.fields["caption"] != prefix+"Анкета" {
					t.Fatalf("wrong photo post: %#v", call)
				}
				var entities []MessageEntity
				if err := json.Unmarshal([]byte(call.fields["caption_entities"]), &entities); err != nil {
					t.Fatal(err)
				}
				if len(entities) != 2 || entities[1].URL != "https://t.me/c/123/191/1091" ||
					validMessageEntities(call.fields["caption"], entities) != nil {
					t.Fatalf("wrong caption entities: %#v", entities)
				}
				if unknown {
					return "", errors.New("photo acknowledgement lost")
				}
				return `{"ok":true,"result":{"message_id":2001,"message_thread_id":191,"chat":{"id":-100123}}}`, nil
			})
			if did, err := f.s.deliverRelationNotice(ctx); !did || (err != nil) != unknown {
				t.Fatalf("photo delivery = %v, %v", did, err)
			}
			state, _, _, message := readRelationNoticeDeliveryReceipt(t, f, id)
			if unknown && (state != "uncertain" || message != 0) || !unknown && (state != "sent" || message != 2001) {
				t.Fatalf("photo receipt = %q/%d", state, message)
			}
			if did, err := f.s.deliverRelationNotice(ctx); did || err != nil || calls != 1 {
				t.Fatalf("unexpected fallback or repeat = %v, %v, calls=%d", did, err, calls)
			}
		})
	}
}
