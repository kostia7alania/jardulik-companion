package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type CaptionRefreshResult struct {
	ArchiveCards    int `json:"archive_cards_confirmed"`
	PrivateCards    int `json:"private_cards_confirmed"`
	ArchiveControls int `json:"archive_controls_confirmed"`
}

// RefreshCaptions edits confirmed cards and their known source ZIP controls.
// It never uploads, deletes, renames topics, or changes profile/media records.
func (s *Service) RefreshCaptions(ctx context.Context) (result CaptionRefreshResult, err error) {
	if !s.Config.ArchivePaused {
		return result, errors.New("pause the archive worker with ARCHIVE_WORKER_PAUSED=true before refreshing captions")
	}
	if s.Config.BackupChatID == 0 {
		return result, errors.New("private archive chat is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Minute)
	defer cancel()
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return result, errors.New("archive worker is active; pause it before refreshing captions")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	s.Archiver.mu.Lock()
	defer s.Archiver.mu.Unlock()
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT av.version_id,av.owner_id,av.thread_id,av.message_ids,COALESCE(r.message_id,0),COALESCE(r.chat_id,0),COALESCE(r.thread_id,0),COALESCE(r.kind,''),COALESCE(r.file_id,'')
 FROM archive_versions av JOIN archive_profile_repairs layout ON layout.version_id=av.version_id
 LEFT JOIN archive_receipts r ON r.source_sha=av.source_sha
 WHERE av.state IN ('sent','partial') AND layout.state='sent' ORDER BY av.version_id`)
	if err != nil {
		return result, err
	}
	type target struct {
		version, owner, thread, zip int64
		receiptChat, receiptThread  int64
		ids                         string
		receiptKind, fileID         string
	}
	var targets []target
	for rows.Next() {
		var item target
		if err = rows.Scan(&item.version, &item.owner, &item.thread, &item.ids, &item.zip, &item.receiptChat, &item.receiptThread, &item.receiptKind, &item.fileID); err != nil {
			rows.Close()
			return result, err
		}
		targets = append(targets, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	for _, item := range targets {
		if item.thread <= 0 || item.thread == s.Config.BackupThreadID {
			return result, errors.New("card receipt has an invalid profile topic")
		}
		if item.zip <= 0 || item.receiptKind != "profile_version" || item.receiptChat != s.Config.BackupChatID || item.receiptThread != item.thread || item.fileID == "" {
			return result, errors.New("profile ZIP receipt is not confirmed for this archive topic")
		}

		var ids []int64
		if json.Unmarshal([]byte(item.ids), &ids) != nil || len(ids) == 0 {
			return result, errors.New("card receipt has no confirmed message inventory")
		}
		for _, id := range ids {
			if id <= 0 || id == item.zip {
				return result, errors.New("card receipt conflicts with a source ZIP or invalid message")
			}
		}
		if err = s.Archiver.refreshConfirmedProfileCaption(ctx, item.version, item.owner, item.thread); err != nil {
			return result, fmt.Errorf("archive caption refresh: %w", err)
		}
		result.ArchiveCards++
		var updated SentMessage
		err = s.Archiver.groupCall(ctx, func() error {
			return s.Archiver.TG.Call(ctx, "editMessageReplyMarkup", map[string]any{"chat_id": s.Config.BackupChatID, "message_id": item.zip, "reply_markup": archiveProfileKeyboard(item.version)}, &updated)
		})
		var unchanged *TelegramError
		notModified := errors.As(err, &unchanged) && unchanged.Code == 400 && strings.Contains(strings.ToLower(unchanged.Description), "message is not modified")
		if !notModified {
			if err != nil {
				return result, fmt.Errorf("archive controls refresh: %w", err)
			}
			if updated.ID != item.zip || updated.Chat.ID != s.Config.BackupChatID || updated.ThreadID != item.thread {
				return result, errors.New("profile ZIP controls edit is unconfirmed")
			}
		}
		result.ArchiveControls++
		if result.ArchiveCards%25 == 0 {
			slog.Info("caption refresh progress", "archive_cards_confirmed", result.ArchiveCards, "archive_controls_confirmed", result.ArchiveControls)
		}
	}
	var private int
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM deliveries WHERE state='sent'").Scan(&private); err != nil {
		return result, err
	}
	if err = s.RefreshCardCaptions(ctx); err != nil {
		return result, err
	}
	result.PrivateCards = private
	return result, nil
}
