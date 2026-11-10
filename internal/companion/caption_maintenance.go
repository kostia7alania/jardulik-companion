package companion

import (
	"bytes"
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

const captionDesignRevision = "2026-11-11-profile-copy-button-v8"
const captionNoticeRevision = "2026-11-10-rating-short-photo-v1"
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
		if err = s.scheduleCaptionNoticesTx(ctx, tx); err != nil {
			return err
		}
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
	if err = s.scheduleCaptionNoticesTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// A scoped pass uses existing v3 jobs; archive/private/presence snapshots stay as-is.
func (s *Service) scheduleCaptionNoticesTx(ctx context.Context, tx *sql.Tx) error {
	const key = "caption_notice_revision"
	if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO meta(key,value) VALUES(?,'')", key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key=?", key); err != nil {
		return err
	}
	var revision string
	if err := tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", key).Scan(&revision); err != nil {
		return err
	}
	if revision == captionNoticeRevision {
		return nil
	}
	var targets []captionMaintenanceJob
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='analysis_posts')").Scan(&exists); err != nil {
		return err
	}
	if exists {
		rows, err := tx.QueryContext(ctx, `SELECT p.analysis_id,p.owner_id,COALESCE(a.version_id,0),json_array(p.message_id),p.response_sha,p.chat_id,p.thread_id,a.response_body
 FROM analysis_posts p JOIN profile_analyses a ON a.id=p.analysis_id AND a.owner_id=p.owner_id AND a.upstream_id=p.upstream_id AND a.profile_id=p.profile_id
 JOIN sessions se ON se.owner_id=a.owner_id AND se.upstream_id=a.upstream_id
 JOIN mirror_topics t ON t.profile_id=p.profile_id AND t.thread_id=p.thread_id
 WHERE p.owner_id=? AND p.state='sent' AND p.message_id>0 AND p.chat_id=? AND p.thread_id>0 AND p.thread_id<>? AND `+analysisPostSuccessSQL("a"), s.Config.OwnerID, s.Config.BackupChatID, s.Config.BackupThreadID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var j captionMaintenanceJob
			var raw []byte
			if err = rows.Scan(&j.receipt, &j.owner, &j.version, &j.inventory, &j.source, &j.chat, &j.thread, &raw); err != nil {
				rows.Close()
				return err
			}
			if digest(raw) == j.source {
				j.kind = "analysis"
				targets = append(targets, j)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive_change_posts')").Scan(&exists); err != nil {
		return err
	}
	if exists {
		rows, err := tx.QueryContext(ctx, `SELECT n.version_id,n.owner_id,json_array(n.message_id),n.thread_id
 FROM archive_change_posts n JOIN versions v ON v.id=n.version_id AND v.owner_id=n.owner_id AND v.profile_id=n.profile_id
 JOIN versions old ON old.id=n.old_version_id AND old.owner_id=n.owner_id AND old.profile_id=n.profile_id
 JOIN mirror_topics t ON t.profile_id=n.profile_id AND t.thread_id=n.thread_id
 JOIN archive_versions av ON av.version_id=n.version_id AND av.owner_id=n.owner_id AND av.profile_id=n.profile_id AND av.thread_id=n.thread_id
 JOIN archive_receipts r ON r.source_sha=av.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=n.thread_id AND r.message_id>0 AND r.file_id<>''
 WHERE n.owner_id=? AND n.state='sent' AND n.message_id>0 AND n.thread_id>0 AND n.thread_id<>?`, s.Config.BackupChatID, s.Config.OwnerID, s.Config.BackupThreadID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var j captionMaintenanceJob
			if err = rows.Scan(&j.receipt, &j.owner, &j.inventory, &j.thread); err != nil {
				rows.Close()
				return err
			}
			j.version = j.receipt
			j.kind = "change"
			j.chat = s.Config.BackupChatID
			targets = append(targets, j)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	for _, j := range targets {
		if _, err := tx.ExecContext(ctx, `INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id)
 VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(revision,kind,receipt_id) DO UPDATE SET state='pending',attempts=0,next_attempt=0,error='',updated_at=CURRENT_TIMESTAMP
 WHERE caption_maintenance_jobs.owner_id=excluded.owner_id AND caption_maintenance_jobs.version_id=excluded.version_id
 AND caption_maintenance_jobs.inventory=excluded.inventory AND caption_maintenance_jobs.source_sha=excluded.source_sha
 AND caption_maintenance_jobs.chat_id=excluded.chat_id AND caption_maintenance_jobs.thread_id=excluded.thread_id`, captionDesignRevision, j.kind, j.receipt, j.owner, j.version, j.inventory, j.source, j.chat, j.thread); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key=?", captionNoticeRevision, key)
	return err
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
	return w.claimProfile(ctx, 0)
}
func (w *captionMaintenanceWorker) claimProfile(ctx context.Context, profile int64) (*captionMaintenanceJob, error) {
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
	if _, err = strconv.ParseInt(raw, 10, 64); err != nil {
		return nil, errors.New("invalid archive pacing checkpoint")
	}
	var j captionMaintenanceJob
	err = tx.QueryRowContext(ctx, `SELECT id,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id,attempts FROM caption_maintenance_jobs
 WHERE revision=? AND state IN ('pending','retry') AND next_attempt<=?
 AND EXISTS(SELECT 1 FROM users u WHERE u.id=caption_maintenance_jobs.owner_id AND u.paused=0)
 AND (?=0 OR (kind='archive' AND owner_id=? AND EXISTS(SELECT 1 FROM versions target WHERE target.id=caption_maintenance_jobs.version_id AND target.owner_id=caption_maintenance_jobs.owner_id AND target.profile_id=?)))
 ORDER BY CASE WHEN ?>0 THEN -version_id ELSE 0 END,CASE WHEN kind='archive' AND NOT EXISTS(
 SELECT 1 FROM archive_versions newer JOIN versions current ON current.id=caption_maintenance_jobs.version_id
 WHERE newer.owner_id=caption_maintenance_jobs.owner_id AND newer.profile_id=current.profile_id
 AND newer.state IN ('sent','partial') AND newer.version_id>caption_maintenance_jobs.version_id) THEN 0
 WHEN kind='analysis' THEN 1 WHEN kind IN ('change','presence') THEN 2 WHEN EXISTS(SELECT 1 FROM versions v WHERE v.id=caption_maintenance_jobs.version_id AND CASE WHEN json_valid(v.raw_json) THEN json_extract(v.raw_json,'$.type')=1 ELSE 0 END) THEN 3 ELSE 4 END,CASE WHEN kind='analysis' THEN -receipt_id ELSE id END LIMIT 1`, captionDesignRevision, w.now().UnixMilli(), profile, w.service.Config.OwnerID, profile, profile).Scan(&j.id, &j.kind, &j.receipt, &j.owner, &j.version, &j.inventory, &j.source, &j.chat, &j.thread, &j.attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE caption_maintenance_jobs SET state='processing',updated_at=CURRENT_TIMESTAMP WHERE id=? AND state IN ('pending','retry')", j.id); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &j, nil
}
func (w *captionMaintenanceWorker) finish(ctx context.Context, j *captionMaintenanceJob, state, reason string, next time.Time) error {
	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := w.service.Store.DB.ExecContext(finish, "UPDATE caption_maintenance_jobs SET state=?,error=?,next_attempt=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='processing'", state, reason, next.UnixMilli(), j.id)
	return err
}
func (w *captionMaintenanceWorker) beginAttempt(ctx context.Context, j *captionMaintenanceJob) error {
	r, err := w.service.Store.DB.ExecContext(ctx, "UPDATE caption_maintenance_jobs SET attempts=attempts+1,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='processing'", j.id)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errCaptionMaintenanceReceipt
	}
	j.attempts++
	return nil
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
			link = forumThreadURL(s.Config.BackupChatID, j.thread)
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
	if j.kind == "archive" {
		payload["reply_markup"] = legacyProfileCopyMarkup(p.ID)
	}
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

// RefreshProfileCaption edits one pending/retry archive card using the normal worker guards.
func (s *Service) RefreshProfileCaption(ctx context.Context, profile int64) (bool, error) {
	if profile <= 0 || s.Config.OwnerID <= 0 {
		return false, errors.New("invalid profile caption target")
	}
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		return false, err
	}
	w := captionMaintenanceWorker{s, time.Now}
	return w.stepProfile(ctx, profile)
}

func (w *captionMaintenanceWorker) step(ctx context.Context) (bool, error) {
	return w.stepProfile(ctx, 0)
}
func (w *captionMaintenanceWorker) stepProfile(ctx context.Context, profile int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	s := w.service
	// Join the normal group slot after acquiring mu, including its future window.
	// No bot/user lock or SQL transaction is held while waiting.
	if s.Archiver == nil || !s.Archiver.mu.TryLock() {
		return false, nil
	}
	defer s.Archiver.mu.Unlock()

	unlock, locked, lockErr := s.Store.trySyncPublicationLock()
	if lockErr != nil {
		return false, lockErr
	}
	if !locked {
		return false, nil
	}
	defer unlock()
	j, err := w.claimProfile(ctx, profile)
	if err != nil || j == nil {
		return false, err
	}
	expected, err := s.savedMessageEditSession(ctx, j.owner)
	if err != nil {
		return true, w.finish(ctx, j, "retry", "source_binding_deferred", w.now().Add(4*time.Second))
	}
	if j.kind == "analysis" {
		if reused, e := w.reuseConfirmedAnalysis(ctx, j, expected); e != nil {
			return true, w.finish(ctx, j, "retry", "analysis_reuse_check_failed", w.now().Add(4*time.Second))
		} else if reused {
			return true, nil
		}
	}
	if err = s.Archiver.groupSlot(ctx); err != nil {
		return true, w.finish(ctx, j, "retry", "pacing_wait", w.now().Add(4*time.Second))
	}
	current, err := s.savedMessageEditSession(ctx, j.owner)
	if err != nil || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return true, w.finish(ctx, j, "retry", "source_binding_deferred", w.now().Add(4*time.Second))
	}
	// Waiting, including a restart during pacing, does not consume an attempt.
	if err = w.beginAttempt(ctx, j); err != nil {
		return true, errors.Join(err, w.retry(ctx, j, "attempt_checkpoint_failed", 4*time.Second))
	}
	// Receipts and sessions may change while the shared slot is pending.
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

// The saved payload was written after a confirmed send/edit. An absent keyboard
// and an explicit empty keyboard both mean the confirmed message has no buttons.
func confirmedAnalysisPayloadEqual(raw []byte, desired map[string]any) bool {
	var saved map[string]any
	if json.Unmarshal(raw, &saved) != nil || saved == nil {
		return false
	}
	if _, ok := saved["text"].(string); !ok {
		return false
	}
	if saved["reply_markup"] == nil {
		saved["reply_markup"] = map[string]any{"inline_keyboard": [][]map[string]string{}}
	}
	want, err := json.Marshal(map[string]any{"text": desired["text"], "entities": desired["entities"], "reply_markup": desired["reply_markup"]})
	if err != nil {
		return false
	}
	var normalized any
	if json.Unmarshal(want, &normalized) != nil {
		return false
	}
	want, err = json.Marshal(normalized)
	if err != nil {
		return false
	}
	have, err := json.Marshal(saved)
	return err == nil && bytes.Equal(want, have)
}

func (w *captionMaintenanceWorker) reuseConfirmedAnalysis(ctx context.Context, j *captionMaintenanceJob, expected Session) (bool, error) {
	method, payload, err := w.prepare(ctx, j)
	if errors.Is(err, errCaptionMaintenanceReceipt) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if method != "editMessageText" {
		return false, nil
	}
	var saved, receipt, source, encrypted []byte
	var upstream int64
	var sessionState string
	err = w.service.Store.DB.QueryRowContext(ctx, `SELECT p.payload_json,p.receipt_json,a.response_body,se.encrypted,p.upstream_id,se.state
 FROM analysis_posts p JOIN profile_analyses a ON a.id=p.analysis_id AND a.owner_id=p.owner_id AND a.upstream_id=p.upstream_id AND a.profile_id=p.profile_id
 JOIN sessions se ON se.owner_id=p.owner_id AND se.upstream_id=p.upstream_id
 JOIN mirror_topics t ON t.profile_id=p.profile_id AND t.thread_id=p.thread_id
 WHERE p.analysis_id=? AND p.owner_id=? AND p.state='sent' AND p.chat_id=? AND p.thread_id=? AND p.message_id=? AND p.response_sha=?`, j.receipt, j.owner, j.chat, j.thread, payload["message_id"], j.source).Scan(&saved, &receipt, &source, &encrypted, &upstream, &sessionState)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ack SentMessage
	if json.Unmarshal(receipt, &ack) != nil || ack.ID <= 0 || ack.ID != payload["message_id"].(int64) || ack.Chat.ID != j.chat || ack.ThreadID != j.thread || digest(source) != j.source || !confirmedAnalysisPayloadEqual(saved, payload) {
		return false, nil
	}
	current, err := w.service.savedMessageEditSession(ctx, j.owner)
	if err != nil || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return false, nil
	}
	cycleGuard := ""
	args := []any{j.id, captionDesignRevision, j.receipt, j.owner, j.version, j.inventory, j.source, j.chat, j.thread, j.receipt, j.owner, upstream, j.chat, j.thread, ack.ID, j.source, saved, receipt, source, encrypted, sessionState}
	if w.service.Config.SyncCycleEnabled {
		cycleGuard = ` AND NOT EXISTS(SELECT 1 FROM sync_cycles c WHERE c.owner_id=caption_maintenance_jobs.owner_id
 AND c.id=(SELECT latest.id FROM sync_cycles latest WHERE latest.owner_id=c.owner_id ORDER BY latest.started_at_ms DESC,latest.id DESC LIMIT 1)
 AND (c.stage='blocked' OR c.upstream_id<>? OR c.generation<>?))`
		args = append(args, expected.UserID, expected.Generation)
	}
	result, err := w.service.Store.DB.ExecContext(ctx, `UPDATE caption_maintenance_jobs SET state='done',error='',next_attempt=0,updated_at=CURRENT_TIMESTAMP
 WHERE id=? AND revision=? AND kind='analysis' AND state='processing' AND receipt_id=? AND owner_id=? AND version_id=? AND inventory=? AND source_sha=? AND chat_id=? AND thread_id=?
 AND EXISTS(SELECT 1 FROM users u WHERE u.id=caption_maintenance_jobs.owner_id AND u.paused=0)
 AND EXISTS(SELECT 1 FROM analysis_posts p JOIN profile_analyses a ON a.id=p.analysis_id AND a.owner_id=p.owner_id AND a.upstream_id=p.upstream_id AND a.profile_id=p.profile_id
 JOIN sessions se ON se.owner_id=p.owner_id AND se.upstream_id=p.upstream_id
 JOIN mirror_topics t ON t.profile_id=p.profile_id AND t.thread_id=p.thread_id
 WHERE p.analysis_id=? AND p.owner_id=? AND p.upstream_id=? AND p.state='sent' AND p.chat_id=? AND p.thread_id=? AND p.message_id=? AND p.response_sha=?
 AND p.payload_json=? AND p.receipt_json=? AND a.response_body=? AND se.encrypted=? AND se.state=?)`+cycleGuard, args...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// Formatting edits use an existing receipt and immutable snapshot, so collection
// can continue. A pause, blocked source or changed session still defers the edit.
func (s *Service) savedMessageEditSession(ctx context.Context, owner int64) (Session, error) {
	var paused bool
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&paused); err != nil {
		return Session{}, err
	}
	// ArchivePaused stops new archival publication; this separately scheduled
	// formatter edits only already confirmed message IDs and source snapshots.
	if paused {
		return Session{}, errSyncPublicationDeferred
	}
	if !s.Config.SyncCycleEnabled {
		return Session{}, nil
	}
	ss, err := s.Store.Session(ctx, owner)
	if err != nil {
		return Session{}, err
	}
	if ss.State != "connected" {
		return Session{}, errSyncPublicationDeferred
	}
	cycle, err := s.Store.SyncCycle(ctx, owner)
	if err != nil {
		return Session{}, err
	}
	if cycle != nil && (cycle.Stage == "blocked" || cycle.UserID != ss.UserID || cycle.Generation != ss.Generation) {
		return Session{}, errSyncPublicationDeferred
	}
	return ss, nil
}
