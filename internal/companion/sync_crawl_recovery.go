package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ResumeStoppedCrawl permits one explicit replay of a legacy unclassified GET.
// The existing source, cursor, observations and completed pages stay together.
func (s *Service) ResumeStoppedCrawl(ctx context.Context, owner int64, cycleID, runID string, eventID int64) (*SyncCycle, error) {
	if !s.Config.SyncCycleEnabled || owner <= 0 || owner != s.Config.OwnerID || cycleID == "" || runID == "" || eventID <= 0 {
		return nil, errors.New("invalid crawl recovery request")
	}
	if err := s.Store.InitSyncCycles(ctx); err != nil {
		return nil, err
	}
	return s.resumeStoppedCrawl(ctx, owner, cycleID, runID, eventID)
}

func (s *Service) resumeStoppedCrawl(ctx context.Context, owner int64, cycleID, runID string, eventID int64) (*SyncCycle, error) {
	reason := "request_failed"
	if eventID == 0 {
		reason = "disk_reserve"
		if err := s.Media.CheckDisk(); err != nil {
			return nil, nil
		}
	}
	now := s.Client.Now().UnixMilli()
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return nil, err
	}
	cycle, err := scanSyncCycle(tx.QueryRowContext(ctx, "SELECT "+syncCycleColumns+" FROM sync_cycles WHERE owner_id=? ORDER BY started_at_ms DESC,id DESC LIMIT 1", owner))
	if err != nil {
		return nil, err
	}
	if cycle == nil || cycle.ID != cycleID || cycle.SearchRunID != runID || cycle.Stage != "blocked" || cycle.Reason != "search_incomplete" || cycle.CollectedAt != 0 {
		return nil, errors.New("latest stopped search does not match the recovery request")
	}
	if err = syncCycleBindingTx(ctx, tx, s.Store, *cycle); err != nil {
		return nil, err
	}
	job, err := scanCrawl(tx.QueryRowContext(ctx, "SELECT "+crawlColumns+" FROM feed_crawls WHERE owner_id=?", owner))
	if err != nil {
		return nil, err
	}
	if !syncBroadRun(job, Session{UserID: cycle.UserID, Generation: cycle.Generation}) || job.RunID != runID || job.State != "stopped" || job.Reason != reason {
		return nil, errors.New("stopped crawl does not match the recovery request")
	}
	var unfinished bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM crawl_presence_runs WHERE owner_id=? AND run_id=? AND finished_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM sync_refresh_items WHERE cycle_id=?)
 AND NOT EXISTS(SELECT 1 FROM sync_publication_manifests WHERE cycle_id=?)`, owner, runID, cycleID, cycleID).Scan(&unfinished); err != nil {
		return nil, err
	}
	if !unfinished {
		return nil, errors.New("crawl collection is already closed")
	}
	receiptKey := "sync_crawl_resume:" + cycleID
	if eventID > 0 {
		if err = verifyLegacyCrawlRead(ctx, tx, owner, eventID, job); err != nil {
			return nil, err
		}
	} else {
		receiptKey = fmt.Sprintf("sync_crawl_disk_resume:%s:%d", cycleID, job.UpdatedAt)
	}
	log, err := json.Marshal(map[string]any{"event_id": eventID, "reason": reason, "mode": job.Mode, "page": job.Page, "stopped_at_ms": job.UpdatedAt, "resumed_at_ms": now})
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?)", receiptKey, string(log)); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE feed_crawls SET state='queued',reason='',updated_at=?,
 next_request=MAX(next_request,?,(SELECT CAST(value AS INTEGER) FROM meta WHERE key='feed_crawl_next')) WHERE owner_id=? AND run_id=?`, now, now, owner, runID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_cycles SET stage='search',reason='',updated_at_ms=? WHERE id=? AND owner_id=?", now, cycleID, owner); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	cycle.Stage, cycle.Reason, cycle.UpdatedAt = "search", "", now
	return cycle, nil
}

func verifyLegacyCrawlRead(ctx context.Context, tx *sql.Tx, owner, eventID int64, job *CrawlJob) error {
	var raw []byte
	var first, last, count int64
	if err := tx.QueryRowContext(ctx, `SELECT fields_json,first_at_ms,last_at_ms,occurrence_count FROM operations_events
 WHERE id=? AND owner_id=? AND role='bot-parser' AND level='WARN' AND message='API operation failed'`, eventID, owner).Scan(&raw, &first, &last, &count); err != nil {
		return err
	}
	var event struct {
		Source     string `json:"source"`
		Stage      string `json:"stage"`
		Method     string `json:"method"`
		Kind       string `json:"kind"`
		Tenant     int64  `json:"tenant"`
		Status     int    `json:"http_status"`
		RetryAfter int64  `json:"retry_after"`
	}
	if json.Unmarshal(raw, &event) != nil || count != 1 || first != last || first < job.StartedAt || first < job.UpdatedAt-5000 || first > job.UpdatedAt ||
		event.Source != "Jardulik" || event.Method != http.MethodGet || event.Tenant != owner || event.Kind != "operation_failed" || event.Status != 0 || event.RetryAfter != 0 || !crawlReadEndpoint(event.Stage) {
		return errors.New("stored event does not match the legacy GET stop")
	}
	return nil
}
