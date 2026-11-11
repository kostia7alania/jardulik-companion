package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Shared by initial presence delivery and edits of its confirmed receipt.
func profilePresenceText(profile int64, availability, observed, reason, changes string) string {
	title := "Анкета недоступна"
	if availability == "available" && reason == "feed_returned" {
		title = "Анкета снова в выдаче"
	} else if availability == "available" {
		title = "Анкета снова доступна"
	} else if reason == "feed_absent" {
		title = "Анкета в этом обходе не обнаружена"
	}
	text := fmt.Sprintf("%s\nJardulik: %d\nОбнаружено: %s", title, profile, displayTime(observed))
	if availability == "available" && changes != "" {
		summary, rest := telegramPrefix(changes, 3400)
		if rest != "" {
			summary += "…"
		}
		text += "\n\nИзменения:\n" + summary
	}
	return text
}

func threadNoticeMessage(j *captionMaintenanceJob, ids []int64) (int64, error) {
	if len(ids) != 1 || ids[0] <= 0 {
		return 0, errCaptionMaintenanceReceipt
	}
	var saved []int64
	if json.Unmarshal([]byte(j.inventory), &saved) != nil || len(saved) != 1 || saved[0] != ids[0] {
		return 0, errCaptionMaintenanceReceipt
	}
	return ids[0], nil
}

func (s *Service) prepareThreadNoticeEdit(ctx context.Context, j *captionMaintenanceJob, ids []int64) (string, map[string]any, error) {
	if j == nil || j.owner != s.Config.OwnerID || j.owner <= 0 || j.chat != s.Config.BackupChatID || j.chat == 0 || j.thread <= 0 || j.thread == s.Config.BackupThreadID || j.receipt <= 0 {
		return "", nil, errCaptionMaintenanceReceipt
	}
	message, err := threadNoticeMessage(j, ids)
	if err != nil {
		return "", nil, err
	}
	var text string
	var entities []MessageEntity
	var keyboard any
	switch j.kind {
	case "change":
		if j.version != j.receipt {
			return "", nil, errCaptionMaintenanceReceipt
		}
		var profile, oldVersion int64
		err = s.Store.DB.QueryRowContext(ctx, `SELECT n.profile_id,n.old_version_id FROM archive_change_posts n
 JOIN mirror_topics t ON t.profile_id=n.profile_id AND t.thread_id=n.thread_id
 JOIN archive_versions av ON av.version_id=n.version_id AND av.owner_id=n.owner_id AND av.profile_id=n.profile_id AND av.thread_id=n.thread_id
 JOIN archive_receipts r ON r.source_sha=av.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=av.thread_id AND r.message_id>0 AND r.file_id<>''
 WHERE n.version_id=? AND n.owner_id=? AND n.state='sent' AND n.thread_id=? AND n.message_id=? AND n.old_version_id>0`, j.chat, j.receipt, j.owner, j.thread, message).Scan(&profile, &oldVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, errCaptionMaintenanceReceipt
		}
		if err != nil {
			return "", nil, err
		}
		before, e := s.archiveChangeSnapshot(ctx, oldVersion)
		if e != nil {
			if errors.Is(e, sql.ErrNoRows) {
				return "", nil, errCaptionMaintenanceReceipt
			}
			return "", nil, e
		}
		after, e := s.archiveChangeSnapshot(ctx, j.receipt)
		if e != nil {
			if errors.Is(e, sql.ErrNoRows) {
				return "", nil, errCaptionMaintenanceReceipt
			}
			return "", nil, e
		}
		if before.ID != profile || after.ID != profile {
			return "", nil, errCaptionMaintenanceReceipt
		}
		oldPhotos, e := s.Store.MediaJobs(ctx, j.owner, before.VersionID)
		if e != nil {
			return "", nil, e
		}
		newPhotos, e := s.Store.MediaJobs(ctx, j.owner, after.VersionID)
		if e != nil {
			return "", nil, e
		}
		rows := archiveProfileChangeRows(before.Profile, after.Profile, oldPhotos, newPhotos)
		if len(rows) == 0 {
			return "", nil, errCaptionMaintenanceReceipt
		}
		text, entities, keyboard = archiveChangeNotice(after, rows)
	case "presence":
		var profile, version int64
		var availability, observed, reason, changes string
		err = s.Store.DB.QueryRowContext(ctx, `SELECT e.profile_id,e.version_id,e.availability,e.observed_at,e.reason,e.changes_text
 FROM profile_presence_events e JOIN mirror_topics t ON t.profile_id=e.profile_id AND t.thread_id=e.thread_id
 WHERE e.id=? AND e.owner_id=? AND e.state='sent' AND e.chat_id=? AND e.thread_id=? AND e.message_id=?`, j.receipt, j.owner, j.chat, j.thread, message).Scan(&profile, &version, &availability, &observed, &reason, &changes)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, errCaptionMaintenanceReceipt
		}
		if err != nil {
			return "", nil, err
		}
		if (j.version != 0 && j.version != version) || (availability != "available" && availability != "unavailable") {
			return "", nil, errCaptionMaintenanceReceipt
		}
		text = profilePresenceText(profile, availability, observed, reason, changes)
	default:
		return "", nil, errCaptionMaintenanceReceipt
	}
	if entities == nil {
		entities = []MessageEntity{}
	}
	if keyboard == nil {
		keyboard = map[string]any{"inline_keyboard": [][]map[string]string{}}
	}
	if textUnits(text) > 4096 {
		return "", nil, errors.New("thread notice exceeds Telegram text budget")
	}
	if err = validMessageEntities(text, entities); err != nil {
		return "", nil, err
	}
	return "editMessageText", map[string]any{"chat_id": j.chat, "message_id": message, "text": text, "entities": entities, "reply_markup": keyboard, "link_preview_options": map[string]bool{"is_disabled": true}}, nil
}

// The worker has already confirmed Telegram's ACK. Revalidate the frozen target
// before changing only our cached notice; presence event facts stay untouched.
func (s *Service) saveThreadNoticeEdit(ctx context.Context, j *captionMaintenanceJob, payload map[string]any) error {
	if j == nil {
		return errCaptionMaintenanceReceipt
	}
	var ids []int64
	if json.Unmarshal([]byte(j.inventory), &ids) != nil {
		return errCaptionMaintenanceReceipt
	}
	_, expected, err := s.prepareThreadNoticeEdit(ctx, j, ids)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	wanted, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, wanted) {
		return errCaptionMaintenanceReceipt
	}
	if j.kind == "presence" {
		return nil
	}
	text, ok := payload["text"].(string)
	if !ok {
		return errCaptionMaintenanceReceipt
	}
	result, err := s.Store.DB.ExecContext(ctx, `UPDATE archive_change_posts SET notice_text=?
 WHERE version_id=? AND owner_id=? AND state='sent' AND thread_id=? AND message_id=?
 AND EXISTS(SELECT 1 FROM mirror_topics t WHERE t.profile_id=archive_change_posts.profile_id AND t.thread_id=archive_change_posts.thread_id)`, text, j.receipt, j.owner, j.thread, ids[0])
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errCaptionMaintenanceReceipt
	}
	return nil
}
