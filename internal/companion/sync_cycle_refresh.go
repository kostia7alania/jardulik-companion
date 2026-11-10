package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// RecordSyncFullCapture records actual successful HTTP evidence, never a feed preview.
// The caller already holds SessionMutex while archiving and checking the binding.
func (s *Store) RecordSyncFullCapture(ctx context.Context, owner int64, expected Session, profile, version int64, observed time.Time) error {
	if owner <= 0 || profile <= 0 || version <= 0 || observed.IsZero() {
		return errors.New("invalid full read receipt")
	}
	if err := s.InitSyncCycles(ctx); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return err
	}
	c := SyncCycle{OwnerID: owner, UserID: expected.UserID, Generation: expected.Generation}
	if err = syncCycleBindingTx(ctx, tx, s, c); err != nil {
		return err
	}
	if err = recordSyncFullCaptureTx(ctx, tx, c, profile, version, observed); err != nil {
		return err
	}
	return tx.Commit()
}

func recordSyncFullCaptureTx(ctx context.Context, tx *sql.Tx, c SyncCycle, profile, version int64, observed time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO sync_full_reads(owner_id,upstream_id,generation,profile_id,version_id,observed_at_ms)
 VALUES(?,?,?,?,?,?) ON CONFLICT(owner_id,upstream_id,generation,profile_id) DO UPDATE SET version_id=excluded.version_id,observed_at_ms=excluded.observed_at_ms
 WHERE excluded.observed_at_ms>=sync_full_reads.observed_at_ms`, c.OwnerID, c.UserID, c.Generation, profile, version, observed.UnixMilli())
	return err
}

// The search ledger proves every reused hydration belongs to this exact run/source.
func (s *Store) seedSyncRefresh(ctx context.Context, c SyncCycle, now time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return err
	}
	if err = syncCycleBindingTx(ctx, tx, s, c); err != nil {
		return err
	}
	var stage string
	if err = tx.QueryRowContext(ctx, "SELECT stage FROM sync_cycles WHERE id=? AND owner_id=? AND upstream_id=? AND generation=?", c.ID, c.OwnerID, c.UserID, c.Generation).Scan(&stage); err != nil {
		return err
	}
	if stage != "search" {
		return tx.Commit()
	}
	var complete bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM feed_crawls j JOIN crawl_presence_runs p ON p.owner_id=j.owner_id AND p.run_id=j.run_id
 WHERE j.owner_id=? AND j.run_id=? AND j.upstream_id=? AND j.generation=? AND j.state='done' AND j.reason='empty_page'
 AND j.include_others=1 AND j.include_locations=1 AND j.unfiltered=1 AND j.mode=3 AND p.finished_at IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM crawl_location_results r WHERE r.owner_id=j.owner_id AND r.run_id=j.run_id AND r.state='pending'))`, c.OwnerID, c.SearchRunID, c.UserID, c.Generation).Scan(&complete)
	if err != nil {
		return err
	}
	if !complete {
		return errors.New("sync search completion not proven")
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO sync_refresh_items(cycle_id,profile_id,version_id)
 SELECT ?,q.profile_id,q.version_id FROM queue q WHERE q.owner_id=?`, c.ID, c.OwnerID); err != nil {
		return err
	}
	// Reuse source-bound full GETs made by the accepted broad search, including before coordinator adoption.
	if _, err = tx.ExecContext(ctx, `UPDATE sync_refresh_items AS item SET state='available',
 version_id=(SELECT q.version_id FROM queue q WHERE q.owner_id=? AND q.profile_id=item.profile_id),
 observed_at_ms=(SELECT r.completed_at FROM crawl_location_results r WHERE r.owner_id=? AND r.run_id=? AND r.profile_id=item.profile_id),reason='search_full_capture'
 WHERE item.cycle_id=? AND item.state='pending' AND NOT EXISTS(SELECT 1 FROM profile_presence p WHERE p.owner_id=? AND p.profile_id=item.profile_id AND p.availability='unavailable') AND EXISTS(
 SELECT 1 FROM crawl_location_results r JOIN location_batches b ON b.id=r.first_batch_id AND b.owner_id=r.owner_id AND b.run_id=r.run_id
 JOIN queue q ON q.owner_id=r.owner_id AND q.profile_id=r.profile_id AND q.version_id=r.version_id
 WHERE r.owner_id=? AND r.run_id=? AND r.profile_id=item.profile_id AND r.state='hydrated' AND r.version_id>0 AND b.upstream_id=? AND b.generation=?)`, c.OwnerID, c.OwnerID, c.SearchRunID, c.ID, c.OwnerID, c.OwnerID, c.SearchRunID, c.UserID, c.Generation); err != nil {
		return err
	}
	// Only this exact run's confirmed GET failures are reusable; feed absence is not evidence.
	if _, err = tx.ExecContext(ctx, `UPDATE sync_refresh_items AS item SET state='unavailable',reason='search_direct_unavailable',
 observed_at_ms=(SELECT r.completed_at FROM crawl_location_results r WHERE r.owner_id=? AND r.run_id=? AND r.profile_id=item.profile_id)
 WHERE item.cycle_id=? AND item.state='pending' AND EXISTS(
 SELECT 1 FROM crawl_location_results r JOIN location_batches b ON b.id=r.first_batch_id AND b.owner_id=r.owner_id AND b.run_id=r.run_id
 JOIN profile_presence p ON p.owner_id=r.owner_id AND p.profile_id=r.profile_id AND p.availability='unavailable'
 WHERE r.owner_id=? AND r.run_id=? AND r.profile_id=item.profile_id AND r.state='unavailable' AND b.upstream_id=? AND b.generation=?)`,
		c.OwnerID, c.SearchRunID, c.ID, c.OwnerID, c.SearchRunID, c.UserID, c.Generation); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_cycles SET stage='refresh',updated_at_ms=? WHERE id=? AND stage='search'", now.UnixMilli(), c.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) reuseSyncRefresh(ctx context.Context, c SyncCycle) error {
	// Full-read receipts are exact, generation-bound, and must still reference the current version.
	_, err := s.DB.ExecContext(ctx, `UPDATE sync_refresh_items AS item SET state='available',
 version_id=(SELECT r.version_id FROM sync_full_reads r WHERE r.owner_id=? AND r.upstream_id=? AND r.generation=? AND r.profile_id=item.profile_id),
 observed_at_ms=(SELECT r.observed_at_ms FROM sync_full_reads r WHERE r.owner_id=? AND r.upstream_id=? AND r.generation=? AND r.profile_id=item.profile_id),reason='fresh_full_capture'
 WHERE item.cycle_id=? AND item.state='pending' AND NOT EXISTS(SELECT 1 FROM profile_presence p WHERE p.owner_id=? AND p.profile_id=item.profile_id AND p.availability='unavailable') AND EXISTS(
 SELECT 1 FROM sync_full_reads r JOIN queue q ON q.owner_id=r.owner_id AND q.profile_id=r.profile_id AND q.version_id=r.version_id
 WHERE r.owner_id=? AND r.upstream_id=? AND r.generation=? AND r.profile_id=item.profile_id AND r.observed_at_ms>=?)`, c.OwnerID, c.UserID, c.Generation, c.OwnerID, c.UserID, c.Generation, c.ID, c.OwnerID, c.OwnerID, c.UserID, c.Generation, c.StartedAt)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) claimSyncRefresh(ctx context.Context, c SyncCycle, now time.Time) (int64, error) {
	w := s.Watcher
	gap, limit := 20*time.Second, 2000
	if w != nil {
		gap, limit = w.Config.WatchGap, w.Config.WatchDailyLimit
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE profile_watch_budget SET used=used WHERE id=1"); err != nil {
		return 0, err
	}
	if err = syncCycleBindingTx(ctx, tx, s.Store, c); err != nil {
		return 0, err
	}
	var active bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_cycles WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND stage='refresh')", c.ID, c.OwnerID, c.UserID, c.Generation).Scan(&active); err != nil {
		return 0, err
	}
	if !active {
		return 0, errors.New("sync refresh stage changed")
	}
	var day string
	var used int
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT day,used,next_request FROM profile_watch_budget WHERE id=1").Scan(&day, &used, &next); err != nil {
		return 0, err
	}
	today := now.In(time.FixedZone("UTC+3", 3*3600)).Format("2006-01-02")
	if day != today {
		used = 0
	}
	if next > now.UnixMilli() || used >= limit {
		return 0, tx.Commit()
	}
	var profile int64
	err = tx.QueryRowContext(ctx, "SELECT profile_id FROM sync_refresh_items WHERE cycle_id=? AND state='pending' AND next_attempt_ms<=? ORDER BY next_attempt_ms,profile_id LIMIT 1", c.ID, now.UnixMilli()).Scan(&profile)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, tx.Commit()
	}
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE profile_watch_budget SET day=?,used=?,next_request=? WHERE id=1", today, used+1, now.Add(gap).UnixMilli()); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_refresh_items SET next_attempt_ms=? WHERE cycle_id=? AND profile_id=? AND state='pending'", now.Add(gap).UnixMilli(), c.ID, profile); err != nil {
		return 0, err
	}
	return profile, tx.Commit()
}

func (s *Service) syncRefreshOne(ctx context.Context, c SyncCycle, ss Session, now time.Time) (bool, error) {
	sessionMu := s.Store.SessionMutex(c.OwnerID)
	sessionMu.Lock()
	err := s.Client.debugBinding(ctx, c.OwnerID, ss)
	if err == nil {
		err = s.Store.reuseSyncRefresh(ctx, c)
	}
	sessionMu.Unlock()
	if err != nil {
		return false, err
	}
	var pending bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_refresh_items WHERE cycle_id=? AND state='pending')", c.ID).Scan(&pending); err != nil {
		return false, err
	}
	if !pending {
		return true, s.Store.syncCycleTransition(ctx, c, "draining", "", now)
	}
	if s.foregroundPending() {
		return false, nil
	}
	free, err := FreeBytes(s.Config.DataDir)
	if err != nil || free < s.Config.MinFreeBytes {
		return false, err
	}
	target, err := s.claimSyncRefresh(ctx, c, now)
	if err != nil || target == 0 {
		return false, err
	}
	p, _, observed, requestErr := s.Client.checkedProfileObserved(withCrawlPriority(ctx, s), c.OwnerID, ss, target)
	finished := s.Client.Now()
	gap := 20 * time.Second
	if s.Watcher != nil {
		gap = s.Watcher.Config.WatchGap
	}
	if _, err = s.Store.DB.ExecContext(ctx, "UPDATE profile_watch_budget SET next_request=MAX(next_request,?) WHERE id=1", finished.Add(gap).UnixMilli()); err != nil {
		return true, errors.Join(requestErr, err)
	}
	if err = ctx.Err(); err != nil {
		return true, err
	}
	sessionMu.Lock()
	defer sessionMu.Unlock()
	if err = s.Client.debugBinding(ctx, c.OwnerID, ss); err != nil {
		return true, err
	}
	state, version := "unavailable", int64(0)
	if requestErr == nil {
		stored, e := s.captureCheckedProfileLocked(ctx, c.OwnerID, ss, p, observed)
		if e != nil {
			return true, e
		}
		version = stored.VersionID
		if queueErr := s.queueCheckedCompatibility(ctx, c.OwnerID, ss, stored); queueErr != nil {
			slog.Warn("compatibility enqueue deferred", "kind", safeError(queueErr))
		}
		state = "available"
	} else if !errors.Is(requestErr, ErrProfileUnavailable) {
		return true, s.deferSyncRefreshError(ctx, c, target, requestErr, finished)
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return true, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return true, err
	}
	if err = syncCycleBindingTx(ctx, tx, s.Store, c); err != nil {
		return true, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE sync_refresh_items SET state=?,version_id=?,observed_at_ms=?,reason='' WHERE cycle_id=? AND profile_id=? AND state='pending'
 AND EXISTS(SELECT 1 FROM sync_cycles WHERE id=? AND stage='refresh')`, state, version, observed.UnixMilli(), c.ID, target, c.ID)
	if err != nil {
		return true, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return true, err
	}
	if n != 1 {
		return true, fmt.Errorf("sync refresh cursor changed")
	}
	return true, tx.Commit()
}

// Failed reads remain pending, with a longer retry delay so one broken ID cannot starve the sweep.
func (s *Service) deferSyncRefreshError(ctx context.Context, c SyncCycle, target int64, requestErr error, now time.Time) error {
	if errors.Is(requestErr, errCrawlYield) {
		return nil
	}
	retry := 5 * time.Minute
	var upstream *UpstreamError
	if errors.As(requestErr, &upstream) && upstream.RetryAfter > retry {
		retry = upstream.RetryAfter
	}
	_, err := s.Store.DB.ExecContext(ctx, "UPDATE sync_refresh_items SET reason=?,next_attempt_ms=? WHERE cycle_id=? AND profile_id=? AND state='pending'", watchErrorCode(requestErr), now.Add(retry).UnixMilli(), c.ID, target)
	return errors.Join(requestErr, err)
}
