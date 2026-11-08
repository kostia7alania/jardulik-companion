package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func captionRefreshFixture(t *testing.T) (*Service, StoredProfile, []MediaJob, string) {
	t.Helper()
	c, store, a, p, jobs := archivePresentationFixture(t)
	c.ArchivePaused = true
	archiveLegacyPresentation(t, store, a, p, jobs)
	if _, err := store.DB.Exec("UPDATE archive_versions SET message_ids='[101,102]' WHERE version_id=?", p.VersionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]','[101,102]')", p.VersionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("INSERT INTO deliveries(owner_id,version_id,state,message_ids) VALUES(123,?,'sent','[201,202]')", p.VersionID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.DataDir, "preserved-source.zip")
	if err := os.WriteFile(path, []byte("whole source ZIP must remain unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Config: c, Store: store, Archiver: a}
	return svc, p, jobs, path
}

func TestRefreshCaptionsOnlyEditsKnownCardsPreservingSources(t *testing.T) {
	svc, p, jobs, zipPath := captionRefreshFixture(t)
	before := map[string]string{}
	for _, path := range append([]string{zipPath}, jobs[0].Path, jobs[1].Path, jobs[2].Path) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = digest(b)
	}
	var rawBefore string
	if err := svc.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&rawBefore); err != nil {
		t.Fatal(err)
	}
	var targets []int64
	tg := NewTelegram(svc.Config)
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		method := filepath.Base(r.URL.Path)
		if method != "editMessageCaption" && method != "editMessageReplyMarkup" {
			t.Fatalf("caption maintenance attempted forbidden method %s", method)
		}
		var input struct {
			Chat     int64           `json:"chat_id"`
			Message  int64           `json:"message_id"`
			Caption  string          `json:"caption"`
			Entities []MessageEntity `json:"caption_entities"`
			Markup   struct {
				Rows [][]map[string]string `json:"inline_keyboard"`
			} `json:"reply_markup"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if method == "editMessageReplyMarkup" {
			if input.Chat != svc.Config.BackupChatID || input.Message != 11 {
				t.Fatalf("edited controls outside known ZIP: %+v", input)
			}
			var callbacks []string
			for _, row := range input.Markup.Rows {
				for _, button := range row {
					callbacks = append(callbacks, button["callback_data"])
				}
			}
			for _, prefix := range []string{"av:", "aj:", "az:"} {
				want := fmt.Sprintf("%s%d", prefix, p.VersionID)
				if !strings.Contains(strings.Join(callbacks, ","), want) {
					t.Fatalf("missing source ZIP control: %s", want)
				}
			}
			targets = append(targets, input.Message)
			body := fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d},"message_thread_id":55}}`, input.Message, input.Chat)
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		}
		if (input.Chat == svc.Config.BackupChatID && input.Message != 101) || (input.Chat == 123 && input.Message != 201) || (input.Chat != svc.Config.BackupChatID && input.Chat != 123) {
			t.Fatalf("edited a source or unknown card: %+v", input)
		}
		if !strings.Contains(input.Caption, "1-2:") || strings.Contains(input.Caption, `[{"Фото":1}`) || len(input.Entities) != 2 {
			t.Fatalf("existing card did not receive the compact expandable summary: %+v", input)
		}
		targets = append(targets, input.Message)
		body := fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d}}}`, input.Message, input.Chat)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	svc.TG, svc.Archiver.TG = tg, tg
	result, err := svc.RefreshCaptions(context.Background())
	if err != nil || result.ArchiveCards != 1 || result.PrivateCards != 1 || result.ArchiveControls != 1 || fmt.Sprint(targets) != "[101 11 201]" {
		t.Fatalf("refresh failed: %+v %v %v", result, targets, err)
	}
	for path, sha := range before {
		b, err := os.ReadFile(path)
		if err != nil || digest(b) != sha {
			t.Fatalf("source bytes changed: %s %v", filepath.Base(path), err)
		}
	}
	var rawAfter string
	if err := svc.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&rawAfter); err != nil || rawBefore != rawAfter {
		t.Fatal("raw profile changed")
	}
	if watchCount(t, svc.Store, "archive_backups") != 0 || watchCount(t, svc.Store, "archive_zip_repairs") != 0 {
		t.Fatal("caption refresh requested new backup or ZIP work")
	}
}

func TestRefreshCaptionsRequiresPauseAndExclusiveArchiveLock(t *testing.T) {
	svc, _, _, _ := captionRefreshFixture(t)
	svc.Config.ArchivePaused = false
	if _, err := svc.RefreshCaptions(context.Background()); err == nil {
		t.Fatal("unpaused maintenance accepted")
	}
	svc.Config.ArchivePaused = true
	lock, err := os.OpenFile(filepath.Join(svc.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if _, err = svc.RefreshCaptions(context.Background()); err == nil {
		t.Fatal("maintenance bypassed running archive worker")
	}
}

func TestRefreshCaptionsRejectsZIPOrServiceTopicTargets(t *testing.T) {
	for _, invalid := range []string{"zip", "service-topic"} {
		t.Run(invalid, func(t *testing.T) {
			svc, p, _, _ := captionRefreshFixture(t)
			statement := "UPDATE archive_versions SET message_ids='[11,102]' WHERE version_id=?"
			if invalid == "service-topic" {
				statement = fmt.Sprintf("UPDATE archive_versions SET thread_id=%d WHERE version_id=?", svc.Config.BackupThreadID)
			}
			if _, err := svc.Store.DB.Exec(statement, p.VersionID); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.RefreshCaptions(context.Background()); err == nil {
				t.Fatal("unsafe caption target accepted")
			}
		})
	}
}

func TestRefreshCaptionsRejectsUnconfirmedZIPReceiptsBeforeMutating(t *testing.T) {
	for _, statement := range []string{
		"UPDATE archive_receipts SET message_id=0",
		"UPDATE archive_receipts SET kind='service_snapshot'",
		"UPDATE archive_receipts SET chat_id=-100999",
		"UPDATE archive_receipts SET thread_id=7",
		"UPDATE archive_receipts SET file_id=''",
		"DELETE FROM archive_receipts",
	} {
		t.Run(statement, func(t *testing.T) {
			svc, _, _, _ := captionRefreshFixture(t)
			if _, err := svc.Store.DB.Exec(statement); err != nil {
				t.Fatal(err)
			}
			tg := NewTelegram(svc.Config)
			tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				t.Fatalf("invalid ZIP receipt caused a mutation: %s", r.URL.Path)
				return nil, nil
			})
			svc.TG, svc.Archiver.TG = tg, tg
			result, err := svc.RefreshCaptions(context.Background())
			if err == nil || result.ArchiveCards != 0 || result.ArchiveControls != 0 {
				t.Fatalf("invalid ZIP receipt accepted: %+v %v", result, err)
			}
		})
	}
}

func TestRefreshZIPControlsRequiresConfirmedIDChatAndThread(t *testing.T) {
	for _, mode := range []string{"not-modified", "wrong-id", "wrong-chat", "wrong-thread", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			svc, _, _, _ := captionRefreshFixture(t)
			var methods []string
			tg := NewTelegram(svc.Config)
			tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				method := filepath.Base(r.URL.Path)
				methods = append(methods, method)
				var input struct {
					Chat    int64 `json:"chat_id"`
					Message int64 `json:"message_id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Fatal(err)
				}
				if method != "editMessageCaption" && method != "editMessageReplyMarkup" {
					t.Fatalf("forbidden refresh action %s", method)
				}
				if method == "editMessageReplyMarkup" && (input.Chat != svc.Config.BackupChatID || input.Message != 11) {
					t.Fatal("guessed a ZIP target")
				}
				body := fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d},"message_thread_id":55}}`, input.Message, input.Chat)
				if method == "editMessageReplyMarkup" {
					switch mode {
					case "not-modified":
						body = `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`
					case "wrong-id":
						body = fmt.Sprintf(`{"ok":true,"result":{"message_id":999,"chat":{"id":%d},"message_thread_id":55}}`, input.Chat)
					case "wrong-chat":
						body = `{"ok":true,"result":{"message_id":11,"chat":{"id":-100999},"message_thread_id":55}}`
					case "wrong-thread":
						body = fmt.Sprintf(`{"ok":true,"result":{"message_id":11,"chat":{"id":%d},"message_thread_id":999}}`, input.Chat)
					case "unknown":
						body = `interrupted`
					}
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			svc.TG, svc.Archiver.TG = tg, tg
			result, err := svc.RefreshCaptions(context.Background())
			if mode == "not-modified" {
				if err != nil || result.ArchiveControls != 1 || result.PrivateCards != 1 {
					t.Fatalf("idempotent controls rejected: %+v %v", result, err)
				}
			} else {
				if err == nil || result.ArchiveCards != 1 || result.ArchiveControls != 0 || fmt.Sprint(methods) != "[editMessageCaption editMessageReplyMarkup]" {
					t.Fatalf("unconfirmed control receipt accepted/retried: %+v %v %v", result, methods, err)
				}
			}
		})
	}
}

func TestRefreshCardsOnlyEditsConfirmedCardsWithoutZIPControls(t *testing.T) {
	for _, repairState := range []string{"sent", "uncertain"} {
		t.Run("repair="+repairState, func(t *testing.T) {
			svc, p, _, _ := captionRefreshFixture(t)
			if _, err := svc.Store.DB.Exec("DELETE FROM deliveries"); err != nil {
				t.Fatal(err)
			}
			if repairState != "sent" {
				if _, err := svc.Store.DB.Exec("UPDATE archive_profile_repairs SET state=? WHERE version_id=?", repairState, p.VersionID); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			tg := NewTelegram(svc.Config)
			tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if filepath.Base(r.URL.Path) != "editMessageCaption" {
					t.Fatalf("cards-only attempted non-caption mutation: %s", r.URL.Path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["message_id"] != float64(101) || body["chat_id"] != float64(svc.Config.BackupChatID) || body["reply_markup"] != nil {
					t.Fatal("cards-only edited ZIP or markup")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`)), Request: r}, nil
			})}
			svc.TG = tg
			svc.Archiver.TG = tg
			result, err := svc.RefreshCaptions(context.Background(), true)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if repairState != "sent" {
				want = 0
			}
			if calls != want || result.ArchiveCards != want || result.ArchiveControls != 0 || result.PrivateCards != 0 {
				t.Fatalf("normal/excluded selection or cards-only count wrong: %+v calls%d", result, calls)
			}
		})
	}
}
