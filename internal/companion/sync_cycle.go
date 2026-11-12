package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const syncFullReadsSchema = `CREATE TABLE IF NOT EXISTS sync_full_reads (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, upstream_id INTEGER NOT NULL,
 generation TEXT NOT NULL, profile_id INTEGER NOT NULL, version_id INTEGER NOT NULL, observed_at_ms INTEGER NOT NULL,
 PRIMARY KEY(owner_id,upstream_id,generation,profile_id)
);
`

const syncCycleBindingChanged = "source_binding_or_pause_changed"

const syncCycleActiveIndex = `CREATE UNIQUE INDEX IF NOT EXISTS sync_cycle_active_owner
 ON sync_cycles(owner_id) WHERE stage NOT IN ('complete', 'superseded');`

const syncCycleSchema = `
CREATE TABLE IF NOT EXISTS sync_cycles (
 id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL, stage TEXT NOT NULL,
 search_run_id TEXT NOT NULL, started_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 next_cycle_ms INTEGER NOT NULL, reason TEXT NOT NULL DEFAULT '', last_tick_ms INTEGER NOT NULL DEFAULT 0
);
` + syncCycleActiveIndex + `
CREATE TABLE IF NOT EXISTS sync_refresh_items (
 cycle_id TEXT NOT NULL REFERENCES sync_cycles(id) ON DELETE CASCADE, profile_id INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', version_id INTEGER NOT NULL DEFAULT 0,
 observed_at_ms INTEGER NOT NULL DEFAULT 0, next_attempt_ms INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(cycle_id,profile_id)
);
INSERT OR IGNORE INTO meta(key,value) VALUES('sync_cycle_writer','0');`

// SyncCycle pins a daily operation to one authenticated source. Milliseconds are UTC.
type SyncCycle struct {
	ID          string `json:"id"`
	OwnerID     int64  `json:"owner_id"`
	UserID      int64  `json:"upstream_id"`
	Generation  string `json:"-"`
	Stage       string `json:"stage"`
	SearchRunID string `json:"search_run_id"`
	StartedAt   int64  `json:"started_at_ms"`
	UpdatedAt   int64  `json:"updated_at_ms"`
	NextCycle   int64  `json:"next_cycle_ms"`
	Reason      string `json:"reason,omitempty"`
}

const syncCycleColumns = "id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms,reason"

func scanSyncCycle(row interface{ Scan(...any) error }) (*SyncCycle, error) {
	var c SyncCycle
	err := row.Scan(&c.ID, &c.OwnerID, &c.UserID, &c.Generation, &c.Stage, &c.SearchRunID, &c.StartedAt, &c.UpdatedAt, &c.NextCycle, &c.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &c, err
}

func (s *Store) InitSyncCycles(ctx context.Context) error {
	if _, err := s.DB.ExecContext(ctx, syncCycleSchema+syncFullReadsSchema+syncPublicationSchema+watchSchema+profilePresenceSchema); err != nil {
		return err
	}
	var indexSQL string
	if err := s.DB.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='index' AND name='sync_cycle_active_owner'").Scan(&indexSQL); err != nil {
		return err
	}
	if strings.Contains(indexSQL, "'superseded'") {
		return nil
	}
	// Keep the name for older processes that still run their IF NOT EXISTS schema.
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DROP INDEX sync_cycle_active_owner;"+syncCycleActiveIndex); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SyncCycle(ctx context.Context, owner int64) (*SyncCycle, error) {
	exists, err := snapshotHasTable(ctx, s.DB, "sync_cycles")
	if err != nil || !exists {
		return nil, err
	}
	return scanSyncCycle(s.DB.QueryRowContext(ctx, "SELECT "+syncCycleColumns+" FROM sync_cycles WHERE owner_id=? ORDER BY started_at_ms DESC,id DESC LIMIT 1", owner))
}

// Publishers fail closed while the source branch and finite compatibility frontier run.
func (s *Store) SyncPublicationAllowed(ctx context.Context, owner int64) (bool, error) {
	c, err := s.SyncCycle(ctx, owner)
	if err != nil {
		return false, err
	}
	return c != nil && (c.Stage == "publishing" || c.Stage == "complete"), nil
}

func syncCycleBindingTx(ctx context.Context, tx *sql.Tx, s *Store, c SyncCycle) error {
	ss, err := s.businessSession(ctx, tx, c.OwnerID)
	if err != nil {
		return err
	}
	var paused, blocked bool
	if err = tx.QueryRowContext(ctx, "SELECT paused,EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1') FROM users WHERE id=?", dailyCrawlBlockKey(c.OwnerID), c.OwnerID).Scan(&paused, &blocked); err != nil {
		return err
	}
	if paused || blocked || ss.State != "connected" || ss.UserID != c.UserID || ss.Generation != c.Generation {
		return &crawlLifecycleError{state: "session_changed"}
	}
	return nil
}

func (s *Store) syncCycleTransition(ctx context.Context, c SyncCycle, stage, reason string, now time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return err
	}
	if stage != "blocked" {
		if err = syncCycleBindingTx(ctx, tx, s, c); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE sync_cycles SET stage=?,reason=?,updated_at_ms=? WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND stage=?", stage, reason, now.UnixMilli(), c.ID, c.OwnerID, c.UserID, c.Generation, c.Stage)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return errors.New("sync cycle stage changed")
	}
	return tx.Commit()
}

func syncBroadRun(j *CrawlJob, ss Session) bool {
	return j != nil && j.UserID == ss.UserID && j.Generation == ss.Generation && j.Unfiltered && j.IncludeOthers && j.IncludeLocations && j.Filters.AgeMin == broadCrawlAgeMin && j.Filters.AgeMax == broadCrawlAgeMax
}

// startSyncCycle is DB-only. It adopts an existing compatible run without moving its cursor.
func (s *Service) startSyncCycle(ctx context.Context, owner int64, now time.Time) (bool, error) {
	if !s.Crawler.mu.TryLock() {
		return false, nil
	}
	defer s.Crawler.mu.Unlock()
	mu := s.userLock(owner)
	if !mu.TryLock() {
		return false, nil
	}
	defer mu.Unlock()
	if s.Archiver != nil {
		if !s.Archiver.mu.TryLock() {
			return false, nil
		}
		defer s.Archiver.mu.Unlock()
	}
	unlockPublication, acquired, err := s.Store.trySyncPublicationLock()
	if err != nil || !acquired {
		return false, err
	}
	defer unlockPublication()
	sessionMu := s.Store.SessionMutex(owner)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	ss, err := s.Store.Session(ctx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var paused, blocked bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT paused,EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1') FROM users WHERE id=?", dailyCrawlBlockKey(owner), owner).Scan(&paused, &blocked); err != nil {
		return false, err
	}
	if paused || blocked || ss.State != "connected" || ss.Generation == "" || ss.UserID <= 0 {
		return false, nil
	}
	previous, err := s.Store.SyncCycle(ctx, owner)
	if err != nil {
		return false, err
	}
	supersede := false
	if previous != nil {
		if now.UnixMilli() < previous.NextCycle {
			return false, nil
		}
		if previous.Stage != "complete" {
			if previous.Stage != "blocked" || previous.Reason != syncCycleBindingChanged ||
				previous.UserID != ss.UserID || previous.Generation == ss.Generation {
				return false, nil
			}
			// A frozen manifest proves the old cycle reached publication preparation.
			// Search/refresh blocks require their original operator recovery.
			if err = s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_publication_manifests
 WHERE cycle_id=? AND owner_id=? AND upstream_id=? AND generation=?)`,
				previous.ID, owner, previous.UserID, previous.Generation).Scan(&supersede); err != nil {
				return false, err
			}
			if !supersede {
				return false, nil
			}
		}
	}
	j, err := s.Store.CrawlStatus(ctx, owner)
	if err != nil {
		return false, err
	}
	// A stopped job is an operator decision or unresolved failure, never permission to restart it.
	if j != nil && !j.active() && !(j.State == "done" && j.Reason == "empty_page") {
		return false, nil
	}
	adopt := false
	if syncBroadRun(j, ss) && (!supersede || j.StartedAt >= previous.NextCycle) {
		recent := j.StartedAt <= now.UnixMilli() && now.UnixMilli() < j.StartedAt+(24*time.Hour).Milliseconds()
		// A retry can adopt a fresh run that finished before the replacement cycle was committed.
		adopt = j.active() || (recent && (previous == nil || supersede))
	}
	if j != nil && j.active() && !adopt {
		return false, nil
	}
	if !adopt {
		j, err = s.Crawler.requestLocked(context.WithValue(ctx, dailyCrawlRequestKey{}, dailyCrawlIdentity{UserID: ss.UserID, Generation: ss.Generation}), owner, true, true)
		if err != nil {
			var lifecycle *crawlLifecycleError
			if errors.As(err, &lifecycle) {
				return false, nil
			}
			return false, err
		}
	}
	id, err := filterNonce()
	if err != nil {
		return false, err
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return false, err
	}
	cycle := SyncCycle{ID: id, OwnerID: owner, UserID: ss.UserID, Generation: ss.Generation}
	if err = syncCycleBindingTx(ctx, tx, s.Store, cycle); err != nil {
		return false, err
	}
	if supersede {
		result, err := tx.ExecContext(ctx, `UPDATE sync_cycles SET stage='superseded',updated_at_ms=?
 WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND stage='blocked' AND reason=? AND next_cycle_ms<=?`,
			now.UnixMilli(), previous.ID, owner, previous.UserID, previous.Generation, syncCycleBindingChanged, now.UnixMilli())
		if err != nil {
			return false, err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return false, err
		}
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_cycles WHERE owner_id=? AND (stage NOT IN ('complete', 'superseded') OR next_cycle_ms>?))", owner, now.UnixMilli()).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	var sameRun bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=? AND run_id=? AND upstream_id=? AND generation=? AND state IN ('queued','running','done'))", owner, j.RunID, ss.UserID, ss.Generation).Scan(&sameRun); err != nil {
		return false, err
	}
	if !sameRun {
		return false, errors.New("sync search run changed")
	}
	if err = insertSyncSearchTx(ctx, tx, &cycle, j, now); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func insertSyncSearchTx(ctx context.Context, tx *sql.Tx, cycle *SyncCycle, job *CrawlJob, now time.Time) error {
	cycle.Stage, cycle.SearchRunID, cycle.StartedAt = "search", job.RunID, job.StartedAt
	cycle.UpdatedAt = now.UnixMilli()
	cycle.NextCycle = job.StartedAt + (24 * time.Hour).Milliseconds()
	result, err := tx.ExecContext(ctx, `INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms)
 VALUES(?,?,?,?,?,?,?,?,?)`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation, cycle.Stage, cycle.SearchRunID, cycle.StartedAt, cycle.UpdatedAt, cycle.NextCycle)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("sync search cycle was not inserted")
	}
	return nil
}

// RecoverAuthStoppedCycle is an explicit operator action backed by a stored API error.
func (s *Service) RecoverAuthStoppedCycle(ctx context.Context, owner int64, cycleID, runID string, receiptID int64) (*SyncCycle, error) {
	if !s.Config.SyncCycleEnabled || owner <= 0 || owner != s.Config.OwnerID || cycleID == "" || runID == "" || receiptID <= 0 {
		return nil, errors.New("invalid sync auth recovery request")
	}
	if err := s.Store.InitSyncCycles(ctx); err != nil {
		return nil, err
	}
	if err := s.Crawler.init(ctx); err != nil {
		return nil, err
	}
	unlock, acquired, err := s.Store.trySyncPublicationLock()
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, errors.New("sync publication is busy")
	}
	defer unlock()
	id, err := filterNonce()
	if err != nil {
		return nil, err
	}
	now := s.Client.Now()
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return nil, err
	}
	previous, err := scanSyncCycle(tx.QueryRowContext(ctx, "SELECT "+syncCycleColumns+" FROM sync_cycles WHERE owner_id=? ORDER BY started_at_ms DESC,id DESC LIMIT 1", owner))
	if err != nil {
		return nil, err
	}
	if previous == nil || previous.ID != cycleID || previous.SearchRunID != runID || previous.Stage != "blocked" || previous.Reason != syncCycleBindingChanged {
		return nil, errors.New("blocked sync cycle does not match recovery request")
	}
	job, err := scanCrawl(tx.QueryRowContext(ctx, "SELECT "+crawlColumns+" FROM feed_crawls WHERE owner_id=?", owner))
	if err != nil {
		return nil, err
	}
	if job.RunID != runID || !syncBroadRun(job, Session{UserID: previous.UserID, Generation: previous.Generation}) || job.State != "stopped" || job.Reason != "auth_required" ||
		job.Mode != 0 || job.Page != 1 || job.Pages != 0 || job.Received != 0 || job.CatalogPages != 0 || job.CatalogReceived != 0 || job.CatalogProfiles != 0 || job.Hydrated != 0 || job.Unavailable != 0 {
		return nil, errors.New("crawl did not stop at the first authentication check")
	}
	var untouched bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM crawl_presence_runs WHERE owner_id=? AND run_id=? AND finished_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM crawl_presence_profiles WHERE owner_id=? AND run_id=? AND seen<>0)
 AND NOT EXISTS(SELECT 1 FROM feed_crawl_pages WHERE owner_id=? AND run_id=?)
 AND NOT EXISTS(SELECT 1 FROM crawl_location_results WHERE owner_id=? AND run_id=?)
 AND NOT EXISTS(SELECT 1 FROM sync_refresh_items WHERE cycle_id=?)
 AND NOT EXISTS(SELECT 1 FROM sync_publication_manifests WHERE cycle_id=?)`, owner, runID, owner, runID, owner, runID, owner, runID, cycleID, cycleID).Scan(&untouched)
	if err != nil {
		return nil, err
	}
	if !untouched {
		return nil, errors.New("crawl recovery would replace recorded progress")
	}
	session, err := s.Store.businessSession(ctx, tx, owner)
	if err != nil {
		return nil, err
	}
	if session.UserID != previous.UserID || session.Generation == "" || session.Generation == previous.Generation || session.State != "connected" {
		return nil, errors.New("a fresh connection to the same Jardulik account is required")
	}
	cycle := &SyncCycle{ID: id, OwnerID: owner, UserID: session.UserID, Generation: session.Generation}
	if err = syncCycleBindingTx(ctx, tx, s.Store, *cycle); err != nil {
		return nil, err
	}
	var reconnected bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE owner_id=? AND kind='connect_success'
 AND datetime(created_at)>=datetime(?/1000.0,'unixepoch'))`, owner, job.UpdatedAt).Scan(&reconnected)
	if err != nil {
		return nil, err
	}
	if !reconnected {
		return nil, errors.New("no successful reconnection after the stopped crawl")
	}
	if err = verifySyncAuthRecoveryReceipt(ctx, tx, owner, receiptID, job); err != nil {
		return nil, err
	}
	requestCtx := context.WithValue(ctx, dailyCrawlRequestKey{}, dailyCrawlIdentity{UserID: session.UserID, Generation: session.Generation})
	fresh, err := s.Crawler.requestRunTx(requestCtx, tx, owner, true, true, nil)
	if err != nil {
		return nil, err
	}
	if fresh.StartedAt <= previous.StartedAt {
		return nil, errors.New("replacement crawl must start after the old cycle")
	}
	history, err := json.Marshal(struct {
		Crawl       *CrawlJob `json:"crawl"`
		ReceiptID   int64     `json:"operations_event_id"`
		NextCycleID string    `json:"next_cycle_id"`
	}{job, receiptID, cycle.ID})
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?)", "sync_auth_recovery:"+previous.ID, string(history)); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_cycles SET stage='superseded',updated_at_ms=? WHERE id=? AND owner_id=?", now.UnixMilli(), previous.ID, owner); err != nil {
		return nil, err
	}
	if err = insertSyncSearchTx(ctx, tx, cycle, fresh, now); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return cycle, nil
}

func verifySyncAuthRecoveryReceipt(ctx context.Context, tx *sql.Tx, owner, receiptID int64, job *CrawlJob) error {
	var raw []byte
	var first, last, count int64
	err := tx.QueryRowContext(ctx, `SELECT fields_json,first_at_ms,last_at_ms,occurrence_count FROM operations_events
 WHERE id=? AND owner_id=? AND role='bot-parser' AND level='WARN' AND message='API operation failed'`, receiptID, owner).Scan(&raw, &first, &last, &count)
	if err != nil {
		return err
	}
	var fields struct {
		Source         string `json:"source"`
		Stage          string `json:"stage"`
		RequestedStage string `json:"requested_stage"`
		Method         string `json:"method"`
		Kind           string `json:"kind"`
		Tenant         int64  `json:"tenant"`
		Status         int    `json:"http_status"`
		RetryAfter     int64  `json:"retry_after"`
	}
	if json.Unmarshal(raw, &fields) != nil || count != 1 || first != last || first < job.StartedAt || first < job.UpdatedAt-5000 || first > job.UpdatedAt ||
		fields.Source != "Jardulik" || fields.Tenant != owner || fields.Status != 400 || fields.Kind != "Jardulik: request_rejected (HTTP 400)" || fields.RetryAfter != 0 {
		return errors.New("stored API error does not prove this authentication stop")
	}
	refresh := fields.Stage == "/appuser/refresh" && fields.RequestedStage == "/user/getMe" && fields.Method == "PUT"
	// Earlier logs attributed the nested refresh failure to its outer GET.
	legacy := fields.Stage == "/user/getMe" && fields.RequestedStage == "" && fields.Method == "GET"
	if !refresh && !legacy {
		return errors.New("stored API error does not match the first account check")
	}
	return nil
}

func (s *Service) RunSyncCycles(ctx context.Context) {
	if !s.Config.SyncCycleEnabled {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "sync-cycle.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	for ctx.Err() == nil {
		if _, err = s.SyncCycleStep(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("sync cycle step deferred", "kind", safeError(err))
		}
		if !sleep(ctx, time.Second) {
			return
		}
	}
}

// SyncCycleStep does at most one source refresh. Compatibility planning is bounded and runs alongside search.
func (s *Service) SyncCycleStep(ctx context.Context) (bool, error) {
	if !s.Config.SyncCycleEnabled || s.foregroundPending() {
		return false, nil
	}
	if err := s.Store.InitSyncCycles(ctx); err != nil {
		return false, err
	}
	rows, err := s.Store.DB.QueryContext(ctx, "SELECT u.id FROM users u JOIN sessions se ON se.owner_id=u.id WHERE u.paused=0 AND se.state='connected' ORDER BY u.id")
	if err != nil {
		return false, err
	}
	var owners []int64
	for rows.Next() {
		var owner int64
		if err = rows.Scan(&owner); err != nil {
			rows.Close()
			return false, err
		}
		owners = append(owners, owner)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, owner := range owners {
		if _, err = s.startSyncCycle(ctx, owner, s.Client.Now()); err != nil {
			return false, err
		}
	}
	c, err := scanSyncCycle(s.Store.DB.QueryRowContext(ctx, "SELECT "+syncCycleColumns+" FROM sync_cycles WHERE stage IN ('search','refresh','draining','publishing') ORDER BY last_tick_ms,owner_id LIMIT 1"))
	if err != nil || c == nil {
		return false, err
	}
	now := s.Client.Now()
	if _, err = s.Store.DB.ExecContext(ctx, "UPDATE sync_cycles SET last_tick_ms=? WHERE id=?", now.UnixMilli(), c.ID); err != nil {
		return false, err
	}
	mu := s.userLock(c.OwnerID)
	if !mu.TryLock() {
		return false, nil
	}
	defer mu.Unlock()
	session, err := s.Store.Session(ctx, c.OwnerID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var paused, blocked bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT paused,EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1') FROM users WHERE id=?", dailyCrawlBlockKey(c.OwnerID), c.OwnerID).Scan(&paused, &blocked); err != nil {
		return false, err
	}
	if paused || blocked || session.UserID != c.UserID || session.Generation != c.Generation {
		return false, s.Store.syncCycleTransition(ctx, *c, "blocked", syncCycleBindingChanged, now)
	}
	// A challenge/source pause requires normal recovery; no automatic unpause or replacement.
	if session.State != "connected" {
		return false, nil
	}
	if s.Config.CompatibilityEnabled && (c.Stage == "search" || c.Stage == "refresh") {
		if _, err = s.PlanSyncCompatibility(ctx, *c, false); err != nil && !errors.Is(err, ErrCompatibilityUnavailable) {
			return false, err
		}
	}
	switch c.Stage {
	case "search":
		j, e := s.Store.CrawlStatus(ctx, c.OwnerID)
		if e != nil {
			return false, e
		}
		if !syncBroadRun(j, session) || j.RunID != c.SearchRunID {
			return false, s.Store.syncCycleTransition(ctx, *c, "blocked", "search_run_changed", now)
		}
		if j.active() {
			return false, nil
		}
		if j.State != "done" || j.Reason != "empty_page" {
			return false, s.Store.syncCycleTransition(ctx, *c, "blocked", "search_incomplete", now)
		}
		return true, s.Store.seedSyncRefresh(ctx, *c, now)
	case "refresh":
		return s.syncRefreshOne(ctx, *c, session, now)
	case "draining":
		ready := true
		if s.Config.CompatibilityEnabled {
			ready, err = s.PlanSyncCompatibility(ctx, *c, true)
			if err != nil {
				return false, err
			}
		}
		if !ready {
			return false, nil
		}
		if err = s.Store.FreezeSyncPublication(ctx, *c); err != nil {
			return false, err
		}
		return true, s.Store.syncCycleTransition(ctx, *c, "publishing", "", now)
	case "publishing":
		ready, e := s.syncCyclePublicationComplete(ctx, *c)
		if e != nil || !ready {
			return false, e
		}
		return true, s.Store.syncCycleTransition(ctx, *c, "complete", "", now)
	}
	return false, nil
}
