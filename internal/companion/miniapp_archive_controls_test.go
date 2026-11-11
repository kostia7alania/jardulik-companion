package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func archiveViewerControlFixture(t *testing.T, syncEnabled bool) (*Service, StoredProfile, string) {
	t.Helper()
	s, p, _, zip := captionRefreshFixture(t)
	s.Config.MiniAppListen = "127.0.0.1:8790"
	s.Config.MiniAppPublicOrigin = "https://companion.example"
	s.Config.SyncCycleEnabled = syncEnabled
	s.Config.ArchivePaused = !syncEnabled
	s.Archiver.Config = s.Config
	s.Store.cfg.SyncCycleEnabled = syncEnabled
	return s, p, zip
}

func TestRefreshArchiveViewerControlOnlyEditsConfirmedZIPMarkup(t *testing.T) {
	for _, mode := range []string{"confirmed", "not-modified", "wrong-id", "wrong-chat", "wrong-thread", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, p, zip := archiveViewerControlFixture(t, true)
			wantMarkup, err := json.Marshal(s.Archiver.archiveProfileKeyboard(s.Config.OwnerID, p.VersionID))
			if err != nil || !strings.Contains(string(wantMarkup), fmt.Sprintf("%s%d", archiveMiniAppAction, p.VersionID)) {
				t.Fatalf("viewer action missing from configured keyboard: %s %v", wantMarkup, err)
			}
			source, err := os.ReadFile(zip)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			s.Archiver.TG = archiveFixtureTelegram(s.Config, func(call archiveTelegramCall) (string, error) {
				calls++
				markup, err := json.Marshal(call.json["reply_markup"])
				if err != nil || call.method != "editMessageReplyMarkup" || len(call.json) != 3 ||
					call.json["chat_id"] != float64(s.Config.BackupChatID) || call.json["message_id"] != float64(11) ||
					string(markup) != string(wantMarkup) {
					t.Fatalf("unexpected archive edit: %+v %v", call, err)
				}
				unlock, acquired, err := s.Store.trySyncPublicationLock()
				if acquired {
					unlock()
				}
				if err != nil || acquired || s.Store.Meta(ctx, "archive_next_send_ns") == "" {
					t.Fatalf("controls did not hold the publication lock and reserve a paced slot: %v %v", acquired, err)
				}
				thread, message, chat := int64(55), int64(11), s.Config.BackupChatID
				switch mode {
				case "not-modified":
					return `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`, nil
				case "wrong-id":
					message++
				case "wrong-chat":
					chat++
				case "wrong-thread":
					thread++
				case "unknown":
					return "interrupted", nil
				}
				return fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d},"message_thread_id":%d}}`, message, chat, thread), nil
			})
			got, err := s.RefreshArchiveViewerControl(ctx, p.VersionID)
			if calls != 1 {
				t.Fatalf("archive edit attempted %d times", calls)
			}
			if mode == "confirmed" || mode == "not-modified" {
				want := ArchiveViewerControlResult{VersionID: p.VersionID, ThreadID: 55, MessageID: 11, Unchanged: mode == "not-modified"}
				if err != nil || got != want {
					t.Fatalf("confirmed edit failed: %+v %v", got, err)
				}
			} else if err == nil || got != (ArchiveViewerControlResult{}) {
				t.Fatalf("accepted unconfirmed edit: %+v %v", got, err)
			}
			after, err := os.ReadFile(zip)
			if err != nil || string(after) != string(source) {
				t.Fatal("archive source file changed", err)
			}
		})
	}
}

func TestRefreshArchiveViewerControlRejectsBusyOrUnknownZIP(t *testing.T) {
	ctx := context.Background()
	s, p, _ := archiveViewerControlFixture(t, true)
	s.Archiver.TG = archiveFixtureTelegram(s.Config, func(call archiveTelegramCall) (string, error) {
		t.Fatalf("unexpected Telegram operation %s", call.method)
		return "", nil
	})
	unlock, acquired, err := s.Store.trySyncPublicationLock()
	if err != nil || !acquired {
		t.Fatal("could not hold fixture publication lock", err)
	}
	_, busyErr := s.RefreshArchiveViewerControl(ctx, p.VersionID)
	unlock()
	if busyErr == nil || !strings.Contains(busyErr.Error(), "sync publication is busy") {
		t.Fatal("did not report a busy publication lock", busyErr)
	}

	const foreign = 124
	if err := s.Store.User(ctx, foreign, "fixture"); err != nil {
		t.Fatal(err)
	}
	versions, err := s.Store.ArchiveViewed(ctx, foreign, []Profile{{"userId": p.ID}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RefreshArchiveViewerControl(ctx, versions[0]); err == nil {
		t.Fatal("foreign archive version accepted")
	}
	if _, err := s.Store.DB.Exec("DELETE FROM archive_receipts"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RefreshArchiveViewerControl(ctx, p.VersionID); err == nil {
		t.Fatal("missing ZIP receipt accepted")
	}
}

func TestRefreshArchiveViewerControlRequiresLegacyArchivePauseAndLock(t *testing.T) {
	ctx := context.Background()
	s, p, _ := archiveViewerControlFixture(t, false)
	s.Archiver.TG = archiveFixtureTelegram(s.Config, func(call archiveTelegramCall) (string, error) {
		t.Fatalf("unexpected Telegram operation %s", call.method)
		return "", nil
	})
	s.Config.ArchivePaused = false
	if _, err := s.RefreshArchiveViewerControl(ctx, p.VersionID); err == nil || !strings.Contains(err.Error(), "ARCHIVE_WORKER_PAUSED=true") {
		t.Fatal("unpaused legacy controls refresh accepted", err)
	}
	s.Config.ArchivePaused = true
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RefreshArchiveViewerControl(ctx, p.VersionID); err == nil || !strings.Contains(err.Error(), "lock archive worker") {
		t.Fatal("legacy controls refresh bypassed worker lock", err)
	}
}
