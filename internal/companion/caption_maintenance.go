package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const captionDesignRevision = "2026-11-10-model-bold-clock-v3"
const captionMaintenanceSchema = `
CREATE TABLE IF NOT EXISTS caption_maintenance_jobs (
 id INTEGER PRIMARY KEY AUTOINCREMENT, revision TEXT NOT NULL, kind TEXT NOT NULL,
 receipt_id INTEGER NOT NULL, owner_id INTEGER NOT NULL, version_id INTEGER NOT NULL,
 inventory TEXT NOT NULL, source_sha TEXT NOT NULL DEFAULT '', chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(revision,kind,receipt_id)
);
CREATE INDEX IF NOT EXISTS caption_maintenance_due ON caption_maintenance_jobs(revision,state,next_attempt,id);`

// ScheduleCaptionMaintenance snapshots confirmed receipts once for this design.
// New publications already use the current renderer and do not need this pass.
func (s *Service) ScheduleCaptionMaintenance(ctx context.Context) error {
	if _, err := s.Store.DB.ExecContext(ctx, archiveSchema+captionMaintenanceSchema); err != nil {
		return err
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO meta(key,value) VALUES('caption_design_revision','')"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='caption_design_revision'"); err != nil {
		return err
	}
	var revision string
	if err = tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='caption_design_revision'").Scan(&revision); err != nil {
		return err
	}
	if revision == captionDesignRevision {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, "UPDATE caption_maintenance_jobs SET state='superseded',error='newer_design_revision',updated_at=CURRENT_TIMESTAMP WHERE revision<>? AND state IN ('pending','retry')", captionDesignRevision); err != nil {
		return err
	}
	var analysisPosts bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='analysis_posts')").Scan(&analysisPosts); err != nil {
		return err
	}
	if analysisPosts {
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id)
 SELECT ?,'analysis',p.analysis_id,p.owner_id,COALESCE(a.version_id,0),json_array(p.message_id),p.response_sha,p.chat_id,p.thread_id
 FROM analysis_posts p JOIN profile_analyses a ON a.id=p.analysis_id AND a.owner_id=p.owner_id AND a.upstream_id=p.upstream_id AND a.profile_id=p.profile_id
 JOIN sessions se ON se.owner_id=a.owner_id AND se.upstream_id=a.upstream_id
 WHERE p.owner_id=? AND p.state='sent' AND p.message_id>0 AND p.chat_id=? AND p.thread_id>0`, captionDesignRevision, s.Config.OwnerID, s.Config.BackupChatID); err != nil {
			return err
		}
	}
	for _, notice := range []struct{ table, kind, receipt, version string }{
		{"archive_change_posts", "change", "version_id", "version_id"},
		{"profile_presence_events", "presence", "id", "version_id"},
	} {
		var exists bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)", notice.table).Scan(&exists); err != nil {
			return err
		}
		if exists {
			query := `INSERT OR IGNORE INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id)
 SELECT ?,?,` + notice.receipt + `,owner_id,` + notice.version + `,json_array(message_id),?,thread_id FROM ` + notice.table + ` WHERE owner_id=? AND state='sent' AND message_id>0 AND thread_id>0`
			if _, err = tx.ExecContext(ctx, query, captionDesignRevision, notice.kind, s.Config.BackupChatID, s.Config.OwnerID); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id)
 SELECT ?,'archive',version_id,owner_id,version_id,message_ids,source_sha,?,thread_id FROM archive_versions WHERE state IN ('sent','partial')`, captionDesignRevision, s.Config.BackupChatID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id)
 SELECT ?,'private',id,owner_id,version_id,message_ids,owner_id,0 FROM deliveries WHERE state='sent'`, captionDesignRevision); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key='caption_design_revision'", captionDesignRevision); err != nil {
		return err
	}
	return tx.Commit()
}

type captionMaintenanceWorker struct {
	service *Service
	now     func() time.Time
}
type captionMaintenanceJob struct {
	id, receipt, owner, version, chat, thread int64
	kind, inventory, source                   string
	attempts                                  int
}

var errCaptionMaintenanceReceipt = errors.New("caption receipt is missing or changed")

// RunCaptionMaintenance never takes a private-chat lock or calls the source API.
// Repeating a known edit after a restart is idempotent; no message is resent.
func (s *Service) RunCaptionMaintenance(ctx context.Context) {
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "caption-maintenance.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = s.ScheduleCaptionMaintenance(ctx); err != nil {
		return
	}
	w := captionMaintenanceWorker{s, time.Now}
	if err = w.recover(ctx); err != nil {
		return
	}
	for ctx.Err() == nil {
		_, err = w.step(ctx)
		delay := time.Second
		if err != nil {
			delay = 5 * time.Second
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}
func (w *captionMaintenanceWorker) recover(ctx context.Context) error {
	_, err := w.service.Store.DB.ExecContext(ctx, "UPDATE caption_maintenance_jobs SET state='retry',next_attempt=?,error='worker_restarted',updated_at=CURRENT_TIMESTAMP WHERE revision=? AND state='processing'", w.now().Add(4*time.Second).UnixMilli(), captionDesignRevision)
	return err
}
func (w *captionMaintenanceWorker) claim(ctx context.Context) (*captionMaintenanceJob, error) {
	tx, err := w.service.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO meta(key,value) VALUES('archive_next_send_ns','0')"); err != nil {
		return nil, err
	}
	var raw string
	if err = tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='archive_next_send_ns'").Scan(&raw); err != nil {
		return nil, err
	}
	next, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, errors.New("invalid archive pacing checkpoint")
	}
	if next > w.now().UnixNano() {
		return nil, nil
	}
	var j captionMaintenanceJob
	err = tx.QueryRowContext(ctx, `SELECT id,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id,attempts FROM caption_maintenance_jobs
 WHERE revision=? AND state IN ('pending','retry') AND next_attempt<=? ORDER BY CASE WHEN kind='analysis' THEN 0 WHEN kind IN ('change','presence') THEN 1 WHEN EXISTS(SELECT 1 FROM versions v WHERE v.id=caption_maintenance_jobs.version_id AND CASE WHEN json_valid(v.raw_json) THEN json_extract(v.raw_json,'$.type')=1 ELSE 0 END) THEN 2 ELSE 3 END,CASE WHEN kind='analysis' THEN -receipt_id ELSE id END LIMIT 1`, captionDesignRevision, w.now().UnixMilli()).Scan(&j.id, &j.kind, &j.receipt, &j.owner, &j.version, &j.inventory, &j.source, &j.chat, &j.thread, &j.attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE caption_maintenance_jobs SET state='processing',attempts=attempts+1,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state IN ('pending','retry')", j.id); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key='archive_next_send_ns'", strconv.FormatInt(w.now().Add(4*time.Second).UnixNano(), 10)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	j.attempts++
	return &j, nil
}
func (w *captionMaintenanceWorker) finish(ctx context.Context, j *captionMaintenanceJob, state, reason string, next time.Time) error {
	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := w.service.Store.DB.ExecContext(finish, "UPDATE caption_maintenance_jobs SET state=?,error=?,next_attempt=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='processing'", state, reason, next.UnixMilli(), j.id)
	return err
}
func (w *captionMaintenanceWorker) prepare(ctx context.Context, j *captionMaintenanceJob) (string, map[string]any, error) {
	s := w.service
	var ids []int64
	if j.owner <= 0 || (j.version <= 0 && j.kind != "analysis" && j.kind != "presence") || json.Unmarshal([]byte(j.inventory), &ids) != nil || len(ids) == 0 {
		return "", nil, errCaptionMaintenanceReceipt
	}
	seen := map[int64]bool{}
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return "", nil, errCaptionMaintenanceReceipt
		}
		seen[id] = true
	}
	if j.kind == "analysis" {
		return s.prepareAnalysisPostEdit(ctx, j, ids)
	}
	if j.kind == "change" || j.kind == "presence" {
		return s.prepareThreadNoticeEdit(ctx, j, ids)
	}
	if j.kind == "archive" {
		var owner, thread, chat, receiptThread, zip int64
		var inventory, source, state, layout, kind, file string
		err := s.Store.DB.QueryRowContext(ctx, `SELECT av.owner_id,av.thread_id,av.message_ids,av.source_sha,av.state,COALESCE(l.state,''),COALESCE(r.chat_id,0),COALESCE(r.thread_id,0),COALESCE(r.message_id,0),COALESCE(r.kind,''),COALESCE(r.file_id,'')
 FROM archive_versions av LEFT JOIN archive_profile_repairs l ON l.version_id=av.version_id LEFT JOIN archive_receipts r ON r.source_sha=av.source_sha WHERE av.version_id=?`, j.receipt).Scan(&owner, &thread, &inventory, &source, &state, &layout, &chat, &receiptThread, &zip, &kind, &file)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, errCaptionMaintenanceReceipt
		}
		if err != nil {
			return "", nil, err
		}
		if j.receipt != j.version || owner != j.owner || inventory != j.inventory || source != j.source || (state != "sent" && state != "partial") || layout != "sent" || j.chat == 0 || j.chat != s.Config.BackupChatID || chat != j.chat || thread != j.thread || thread <= 0 || thread == s.Config.BackupThreadID || receiptThread != thread || zip <= 0 || seen[zip] || kind != "profile_version" || file == "" {
			return "", nil, errCaptionMaintenanceReceipt
		}
	} else if j.kind == "private" {
		var owner, version int64
		var inventory, state string
		err := s.Store.DB.QueryRowContext(ctx, "SELECT owner_id,version_id,message_ids,state FROM deliveries WHERE id=?", j.receipt).Scan(&owner, &version, &inventory, &state)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, errCaptionMaintenanceReceipt
		}
		if err != nil {
			return "", nil, err
		}
		if owner != j.owner || version != j.version || inventory != j.inventory || state != "sent" || j.chat != j.owner || j.thread != 0 {
			return "", nil, errCaptionMaintenanceReceipt
		}
	} else {
		return "", nil, errCaptionMaintenanceReceipt
	}
	p, err := s.Store.Version(ctx, j.owner, j.version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, errCaptionMaintenanceReceipt
	}
	if err != nil {
		return "", nil, err
	}
	jobs, err := s.Store.MediaJobs(ctx, j.owner, j.version)
	if err != nil {
		return "", nil, err
	}
	marker, err := loadRichCardMarker(ctx, s.Store, j.chat, ids[0])
	if errors.Is(err, ErrRichCardMarker) {
		return "", nil, errCaptionMaintenanceReceipt
	}
	if err != nil {
		return "", nil, err
	}
	if marker != nil {
		if len(ids) != 1 || marker.OwnerID != j.owner || marker.VersionID != j.version || marker.ThreadID != j.thread {
			return "", nil, errCaptionMaintenanceReceipt
		}
		link := s.profilePostURL(ctx, j.owner, p)
		if j.kind == "archive" {
			link = forumTopicURL(s.Config.BackupChatID, j.thread)
		}
		card, err := BuildRichProfileCard(p, jobs, link)
		if err != nil {
			return "", nil, err
		}
		if len(marker.PhotoFileIDs) != len(card.Photos) {
			return "", nil, errCaptionMaintenanceReceipt
		}
		rich, err := card.WithPhotoFileIDs(marker.PhotoFileIDs)
		if err != nil {
			return "", nil, errCaptionMaintenanceReceipt
		}
		return "editMessageText", map[string]any{"chat_id": j.chat, "message_id": ids[0], "rich_message": rich}, nil
	}
	photos := selectedProfilePhotos(jobs)
	if (len(photos) == 0 && len(ids) != 1) || (len(photos) > 0 && len(ids) != len(photos)) {
		return "", nil, errCaptionMaintenanceReceipt
	}
	var card photoText
	if j.kind == "private" {
		card, err = s.renderPrivateCard(ctx, j.owner, p, jobs, photos)
	} else {
		limit := 1024
		if len(photos) == 0 {
			limit = 4096
		}
		card, err = s.Archiver.profileCardContent(p, unavailableMediaCount(jobs), j.thread, limit, len(photos) == 0, photos)
	}
	if err != nil {
		return "", nil, err
	}
	if err = validMessageEntities(card.Text, card.Entities); err != nil {
		return "", nil, err
	}
	payload := map[string]any{"chat_id": j.chat, "message_id": ids[0]}
	if len(photos) == 0 {
		payload["text"] = card.Text
		payload["entities"] = card.Entities
		payload["link_preview_options"] = map[string]bool{"is_disabled": true}
		return "editMessageText", payload, nil
	}
	payload["caption"] = card.Text
	payload["caption_entities"] = card.Entities
	return "editMessageCaption", payload, nil
}
func (w *captionMaintenanceWorker) step(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s := w.service
	// TryLock avoids waiting behind the poster; no bot/user lock is ever held.
	if s.Archiver == nil || !s.Archiver.mu.TryLock() {
		return false, nil
	}
	defer s.Archiver.mu.Unlock()
	j, err := w.claim(ctx)
	if err != nil || j == nil {
		return false, err
	}
	method, payload, err := w.prepare(ctx, j)
	if errors.Is(err, errCaptionMaintenanceReceipt) {
		return true, w.finish(ctx, j, "failed", "receipt_missing_or_changed", time.Time{})
	}
	if err != nil {
		return true, w.retry(ctx, j, "render_failed", 30*time.Second)
	}
	done := func() error {
		if j.kind == "analysis" {
			if err := s.saveAnalysisPostEdit(ctx, j, payload); err != nil {
				return w.retry(ctx, j, "analysis_receipt_update_failed", 30*time.Second)
			}
		}
		if j.kind == "change" || j.kind == "presence" {
			if err := s.saveThreadNoticeEdit(ctx, j, payload); err != nil {
				return w.retry(ctx, j, "notice_receipt_update_failed", 30*time.Second)
			}
		}
		return w.finish(ctx, j, "done", "", time.Time{})
	}
	var out SentMessage
	err = s.TG.Call(ctx, method, payload, &out)
	var te *TelegramError
	if errors.As(err, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
		return true, done()
	}
	if err == nil {
		if out.ID != payload["message_id"].(int64) || out.Chat.ID != j.chat || (j.kind != "private" && out.ThreadID != j.thread) {
			return true, w.finish(ctx, j, "failed", "edit_receipt_unconfirmed", time.Time{})
		}
		return true, done()
	}
	if errors.As(err, &te) {
		if te.Code == 429 {
			if te.RetryAfter > 365*24*60*60 {
				return true, w.finish(ctx, j, "failed", "invalid_retry_after", time.Time{})
			}
			delay := time.Duration(te.RetryAfter) * time.Second
			if delay < 4*time.Second {
				delay = 4 * time.Second
			}
			next := w.now().Add(delay)
			_, saveErr := s.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE meta SET value=MAX(CAST(value AS INTEGER),?) WHERE key='archive_next_send_ns'", next.UnixNano())
			if saveErr != nil {
				return true, saveErr
			}
			return true, w.finish(ctx, j, "retry", "telegram_429", next)
		}
		if te.Code < 500 {
			return true, w.finish(ctx, j, "failed", fmt.Sprintf("telegram_%d", te.Code), time.Time{})
		}
	}
	return true, w.retry(ctx, j, "edit_transport_retry", 30*time.Second)
}
func (w *captionMaintenanceWorker) retry(ctx context.Context, j *captionMaintenanceJob, reason string, delay time.Duration) error {
	if j.attempts >= 8 {
		return w.finish(ctx, j, "failed", reason, time.Time{})
	}
	return w.finish(ctx, j, "retry", reason, w.now().Add(delay))
}
