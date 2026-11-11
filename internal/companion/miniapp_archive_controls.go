package companion

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type ArchiveViewerControlResult struct {
	VersionID int64 `json:"version_id"`
	ThreadID  int64 `json:"thread_id"`
	MessageID int64 `json:"message_id"`
	Unchanged bool  `json:"unchanged"`
}

// RefreshArchiveViewerControl edits the keyboard of one confirmed primary-owner ZIP.
func (s *Service) RefreshArchiveViewerControl(ctx context.Context, versionID int64) (ArchiveViewerControlResult, error) {
	var result ArchiveViewerControlResult
	a := s.Archiver
	owner, chat := s.Config.OwnerID, s.Config.BackupChatID
	origin := s.Config.MiniAppPublicOrigin
	if versionID <= 0 || owner <= 0 || chat == 0 || a == nil || a.TG == nil {
		return result, errors.New("archive viewer controls require a version ID, primary owner and private archive")
	}
	if a.TG.Token == "" {
		return result, errors.New("TELEGRAM_BOT_TOKEN is required to refresh archive viewer controls")
	}
	if origin == "" || a.Config.MiniAppPublicOrigin != origin || a.Config.OwnerID != owner {
		return result, errors.New("archive viewer Mini App origin and owner are not configured")
	}
	if err := validateMiniAppEndpoint(s.Config.MiniAppListen, origin); err != nil {
		return result, err
	}

	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if !a.mu.TryLock() {
		return result, errors.New("archive worker is busy; retry the controls refresh")
	}
	defer a.mu.Unlock()

	if s.Config.SyncCycleEnabled {
		unlock, acquired, err := s.Store.trySyncPublicationLock()
		if err != nil {
			return result, fmt.Errorf("acquire sync publication lock: %w", err)
		}
		if !acquired {
			return result, errors.New("sync publication is busy; retry the controls refresh")
		}
		defer unlock()
	} else {
		if !s.Config.ArchivePaused {
			return result, errors.New("pause the archive worker with ARCHIVE_WORKER_PAUSED=true before refreshing controls")
		}
		lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return result, err
		}
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return result, fmt.Errorf("lock archive worker for controls refresh: %w", err)
		}
	}

	p, err := s.Store.Version(ctx, owner, versionID)
	if err != nil {
		return result, fmt.Errorf("load primary-owner archive version: %w", err)
	}
	thread, message, err := s.miniAppArchiveZIP(ctx, p)
	if err != nil {
		return result, fmt.Errorf("load confirmed archive ZIP receipt: %w", err)
	}
	var updated SentMessage
	err = a.groupCall(ctx, func() error {
		confirmedThread, confirmedMessage, err := s.miniAppArchiveZIP(ctx, p)
		if err != nil {
			return err
		}
		if confirmedThread != thread || confirmedMessage != message {
			return errors.New("archive ZIP receipt changed while waiting to refresh controls")
		}
		return a.TG.Call(ctx, "editMessageReplyMarkup", map[string]any{
			"chat_id": chat, "message_id": message, "reply_markup": a.archiveProfileKeyboard(owner, p.VersionID),
		}, &updated)
	})
	var telegramErr *TelegramError
	unchanged := errors.As(err, &telegramErr) && telegramErr.Code == 400 && strings.Contains(strings.ToLower(telegramErr.Description), "message is not modified")
	if !unchanged {
		if err != nil {
			return result, fmt.Errorf("archive viewer controls refresh: %w", err)
		}
		if updated.ID != message || updated.Chat.ID != chat || updated.ThreadID != thread {
			return result, errors.New("archive viewer controls edit is unconfirmed")
		}
	}
	return ArchiveViewerControlResult{VersionID: versionID, ThreadID: thread, MessageID: message, Unchanged: unchanged}, nil
}
