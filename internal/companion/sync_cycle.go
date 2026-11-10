package companion

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const syncFullReadsSchema = `CREATE TABLE IF NOT EXISTS sync_full_reads (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, upstream_id INTEGER NOT NULL,
 generation TEXT NOT NULL, profile_id INTEGER NOT NULL, version_id INTEGER NOT NULL, observed_at_ms INTEGER NOT NULL,
 PRIMARY KEY(owner_id,upstream_id,generation,profile_id)
);
`

const syncCycleSchema = `
CREATE TABLE IF NOT EXISTS sync_cycles (
 id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL, stage TEXT NOT NULL,
 search_run_id TEXT NOT NULL, started_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 next_cycle_ms INTEGER NOT NULL, reason TEXT NOT NULL DEFAULT '', last_tick_ms INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS sync_cycle_active_owner ON sync_cycles(owner_id) WHERE stage<>'complete';
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
	_, err := s.DB.ExecContext(ctx, syncCycleSchema+syncFullReadsSchema+syncPublicationSchema+watchSchema+profilePresenceSchema)
	return err
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
	if previous != nil && (previous.Stage != "complete" || now.UnixMilli() < previous.NextCycle) {
		return false, nil
	}
	j, err := s.Store.CrawlStatus(ctx, owner)
	if err != nil {
		return false, err
	}
	// A stopped job is an operator decision or unresolved failure, never permission to restart it.
	if j != nil && !j.active() && !(j.State == "done" && j.Reason == "empty_page") {
		return false, nil
	}
	adopt := syncBroadRun(j, ss) && (j.active() || (previous == nil && j.StartedAt <= now.UnixMilli() && now.UnixMilli() < j.StartedAt+(24*time.Hour).Milliseconds()))
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
	started := j.StartedAt
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return false, err
	}
	cycle := SyncCycle{ID: id, OwnerID: owner, UserID: ss.UserID, Generation: ss.Generation, Stage: "search", SearchRunID: j.RunID, StartedAt: started}
	if err = syncCycleBindingTx(ctx, tx, s.Store, cycle); err != nil {
		return false, err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_cycles WHERE owner_id=? AND (stage<>'complete' OR next_cycle_ms>?))", owner, now.UnixMilli()).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, tx.Commit()
	}
	var sameRun bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=? AND run_id=? AND upstream_id=? AND generation=? AND state IN ('queued','running','done'))", owner, j.RunID, ss.UserID, ss.Generation).Scan(&sameRun); err != nil {
		return false, err
	}
	if !sameRun {
		return false, errors.New("sync search run changed")
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES(?,?,?,?,?,?,?,?,?)", id, owner, ss.UserID, ss.Generation, "search", j.RunID, started, now.UnixMilli(), started+(24*time.Hour).Milliseconds())
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
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
		return false, s.Store.syncCycleTransition(ctx, *c, "blocked", "source_binding_or_pause_changed", now)
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
