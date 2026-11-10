package companion

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const compatibilityArchiveScope = "archive-adult-v1"

const compatibilityCycleSchema = `
CREATE TABLE IF NOT EXISTS compatibility_cycle_scans (
 cycle_id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL, own_hash TEXT NOT NULL,
 last_profile_id INTEGER NOT NULL DEFAULT 0, next_plan_ms INTEGER NOT NULL DEFAULT 0,
 final_pass INTEGER NOT NULL DEFAULT 0, sealed INTEGER NOT NULL DEFAULT 0,
 updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS compatibility_cycle_targets (
 cycle_id TEXT NOT NULL REFERENCES compatibility_cycle_scans(cycle_id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, version_id INTEGER NOT NULL DEFAULT 0,
 input_hash TEXT NOT NULL DEFAULT '', job_id INTEGER REFERENCES compatibility_jobs(id) ON DELETE SET NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','succeeded','unknown','rejected','unavailable','excluded')),
 reason TEXT NOT NULL DEFAULT '', final_member INTEGER NOT NULL DEFAULT 0,
 updated_at_ms INTEGER NOT NULL, PRIMARY KEY(cycle_id,profile_id)
);
CREATE INDEX IF NOT EXISTS compatibility_cycle_pending ON compatibility_cycle_targets(cycle_id,final_member,state,profile_id);`

// The cycle and its exact directed input must still be active at dispatch time.
const compatibilityCycleScopeSQL = `EXISTS(
 SELECT 1 FROM sync_cycles sc JOIN compatibility_cycle_scans cs ON cs.cycle_id=sc.id
 JOIN compatibility_cycle_targets ct ON ct.cycle_id=sc.id
 WHERE sc.owner_id=p.owner_id AND sc.upstream_id=p.from_user_id AND sc.generation=j.generation
 AND sc.stage IN ('search','refresh','draining')
 AND NOT EXISTS(SELECT 1 FROM meta block WHERE block.key='daily_crawl_block:'||p.owner_id AND block.value='1')
 AND cs.owner_id=p.owner_id
 AND cs.upstream_id=p.from_user_id AND cs.generation=j.generation
 AND ct.profile_id=p.target_user_id AND ct.input_hash=j.input_hash AND ct.state='pending'
 AND (sc.stage='search' OR EXISTS(SELECT 1 FROM sync_refresh_items r WHERE r.cycle_id=sc.id AND r.profile_id=ct.profile_id)))`

func compatibilityAdult(p Profile) bool {
	age, known := autoInteger(p["age"])
	return known && age >= 18 && age <= 111
}

func (s *Store) compatibilityCycleAllows(ctx context.Context, q reactionQuerier, j CompatibilityJob) (bool, error) {
	var allowed bool
	err := q.QueryRowContext(ctx, `SELECT `+compatibilityCycleScopeSQL+`
 FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id WHERE j.id=?
 AND p.owner_id=? AND p.from_user_id=? AND p.target_user_id=? AND j.generation=? AND j.input_hash=?`,
		j.ID, j.OwnerID, j.FromUserID, j.TargetUserID, j.Generation, j.InputHash).Scan(&allowed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return allowed, err
}

func (s *Service) compatibilityEligible(ctx context.Context, j CompatibilityJob, target Profile) (bool, error) {
	if !compatibilityAdult(target) {
		return false, nil
	}
	if s.Config.SyncCycleEnabled {
		return s.Store.compatibilityCycleAllows(ctx, s.Store.DB, j)
	}
	if j.FilterHash == compatibilityArchiveScope {
		return false, nil
	}
	filters, err := s.Autoliker.filters(ctx, j.OwnerID)
	if err != nil {
		return false, err
	}
	return compatibilityMatchesFilters(target, filters), nil
}

// PlanSyncCompatibility does no source HTTP. Its cursor is independent of personal
// filters. The final pass freezes a finite target cohort after source refresh ends.
func (s *Service) PlanSyncCompatibility(ctx context.Context, cycle SyncCycle, final bool) (bool, error) {
	if !s.Config.CompatibilityEnabled {
		return true, nil
	}
	if err := s.Store.InitCompatibility(ctx); err != nil {
		return false, err
	}
	current, err := s.Store.SyncCycle(ctx, cycle.OwnerID)
	if err != nil {
		return false, err
	}
	if current == nil || current.ID != cycle.ID || current.UserID != cycle.UserID || current.Generation != cycle.Generation || (current.Stage != "search" && current.Stage != "refresh" && current.Stage != "draining") {
		return false, ErrCompatibilityUnavailable
	}
	session, err := s.Store.Session(ctx, cycle.OwnerID)
	if err != nil {
		return false, err
	}
	own, err := debugProfile(session.Account)
	frozen, knownFrozen := own["freezeState"].(bool)
	if err != nil || session.State != "connected" || session.UserID != cycle.UserID || session.Generation != cycle.Generation || s.Store.Paused(ctx, cycle.OwnerID) || own.ID() != cycle.UserID || !compatibilityAdult(own) || !knownFrozen || frozen {
		return false, ErrCompatibilityUnavailable
	}
	now := s.Client.Now()
	ownHash := compatibilityProfileHash(own)
	var cursor, next int64
	var savedHash string
	var finalPass, sealed bool
	err = s.Store.DB.QueryRowContext(ctx, `SELECT own_hash,last_profile_id,next_plan_ms,final_pass,sealed FROM compatibility_cycle_scans WHERE cycle_id=? AND owner_id=? AND upstream_id=? AND generation=?`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation).Scan(&savedHash, &cursor, &next, &finalPass, &sealed)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		_, err = s.Store.DB.ExecContext(ctx, `INSERT INTO compatibility_cycle_scans(cycle_id,owner_id,upstream_id,generation,own_hash,updated_at_ms) VALUES(?,?,?,?,?,?)`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation, ownHash, now.UnixMilli())
		if err != nil {
			return false, err
		}
	}
	if savedHash != ownHash {
		cursor, next, sealed = 0, 0, false
	}
	if final && !finalPass {
		// Snapshot membership once. New foreground discoveries belong to a later cycle.
		tx, e := s.Store.DB.BeginTx(ctx, nil)
		if e != nil {
			return false, e
		}
		defer tx.Rollback()
		_, e = tx.ExecContext(ctx, `INSERT INTO compatibility_cycle_targets(cycle_id,profile_id,final_member,updated_at_ms)
 SELECT ?,q.profile_id,1,? FROM queue q JOIN sync_refresh_items r ON r.profile_id=q.profile_id AND r.cycle_id=?
 WHERE q.owner_id=? AND r.state IN ('available','unavailable')
 ON CONFLICT(cycle_id,profile_id) DO UPDATE SET final_member=1`, cycle.ID, now.UnixMilli(), cycle.ID, cycle.OwnerID)
		if e == nil {
			_, e = tx.ExecContext(ctx, `UPDATE compatibility_cycle_scans SET final_pass=1,sealed=0,last_profile_id=0,next_plan_ms=0 WHERE cycle_id=?`, cycle.ID)
		}
		if e != nil {
			return false, e
		}
		if e = tx.Commit(); e != nil {
			return false, e
		}
		cursor, next, finalPass, sealed = 0, 0, true, false
	}
	if next > now.UnixMilli() {
		return s.syncCompatibilityReady(ctx, cycle.ID, finalPass, sealed)
	}
	var candidates []StoredProfile
	query := `SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id WHERE q.owner_id=? AND q.profile_id>?`
	args := []any{cycle.OwnerID, cursor}
	if !finalPass && current.Stage == "refresh" {
		query += ` AND EXISTS(SELECT 1 FROM sync_refresh_items r WHERE r.cycle_id=? AND r.profile_id=q.profile_id)`
		args = append(args, cycle.ID)
	}
	if finalPass {
		query += ` AND EXISTS(SELECT 1 FROM compatibility_cycle_targets ct WHERE ct.cycle_id=? AND ct.profile_id=q.profile_id AND ct.final_member=1`
		args = append(args, cycle.ID)
		if sealed {
			query += ` AND ct.state='pending'`
		}
		query += `)`
	}
	query += ` ORDER BY q.profile_id LIMIT ?`
	args = append(args, compatibilityPlanBatch)
	rows, err := s.Store.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		p, e := scanProfile(rows)
		if e != nil {
			rows.Close()
			return false, e
		}
		candidates = append(candidates, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, target := range candidates {
		cursor = target.ID
		if err = s.planSyncCompatibilityTarget(ctx, cycle, session, ownHash, target, now); err != nil {
			return false, err
		}
	}
	delay := time.Second
	if len(candidates) < compatibilityPlanBatch {
		cursor = 0
		if finalPass {
			sealed = true
		} else {
			delay = 15 * time.Second
		}
	}
	_, err = s.Store.DB.ExecContext(ctx, `UPDATE compatibility_cycle_scans SET own_hash=?,last_profile_id=?,next_plan_ms=?,sealed=?,updated_at_ms=? WHERE cycle_id=? AND owner_id=? AND upstream_id=? AND generation=?`, ownHash, cursor, now.Add(delay).UnixMilli(), sealed, now.UnixMilli(), cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation)
	if err != nil {
		return false, err
	}
	return s.syncCompatibilityReady(ctx, cycle.ID, finalPass, sealed)
}

func (s *Service) syncCompatibilityReady(ctx context.Context, cycleID string, final, sealed bool) (bool, error) {
	if !final || !sealed {
		return false, nil
	}
	var pending int
	err := s.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM compatibility_cycle_targets WHERE cycle_id=? AND final_member=1 AND state='pending'`, cycleID).Scan(&pending)
	return pending == 0, err
}

func (s *Service) planSyncCompatibilityTarget(ctx context.Context, cycle SyncCycle, session Session, ownHash string, target StoredProfile, now time.Time) error {
	state, reason := "pending", ""
	if target.ID == cycle.UserID || !compatibilityAdult(target.Profile) {
		state, reason = "excluded", "not_adult_pair"
	}
	full, err := s.Store.ProfileHasFullCapture(ctx, cycle.OwnerID, target.ID, target.VersionID)
	if err != nil {
		return err
	}
	if !full {
		state, reason = "excluded", "full_capture_unavailable"
	}
	inputHash := compatibilityInputHash(ownHash, compatibilityProfileHash(target.Profile))
	var previousInput, previousState string
	err = s.Store.DB.QueryRowContext(ctx, `SELECT input_hash,state FROM compatibility_cycle_targets WHERE cycle_id=? AND profile_id=?`, cycle.ID, target.ID).Scan(&previousInput, &previousState)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if state == "pending" {
		unavailable, e := profileKnownUnavailable(ctx, s.Store.DB, cycle.OwnerID, target.ID)
		if e != nil {
			return e
		}
		if unavailable {
			state, reason = "unavailable", "target_unavailable"
		}
	}
	// Keep the outcome and original analysis snapshot; only advance the current
	// profile version when a new photo or another non-input field changes.
	if previousInput == inputHash && previousState != "pending" && previousState != "excluded" && previousState != "unavailable" && state == "pending" {
		_, err = s.Store.DB.ExecContext(ctx, "UPDATE compatibility_cycle_targets SET version_id=?,updated_at_ms=? WHERE cycle_id=? AND profile_id=? AND input_hash=?", target.VersionID, now.UnixMilli(), cycle.ID, target.ID, inputHash)
		return err
	}
	_, err = s.Store.DB.ExecContext(ctx, `INSERT INTO compatibility_cycle_targets(cycle_id,profile_id,version_id,input_hash,state,reason,updated_at_ms) VALUES(?,?,?,?,?,?,?)
 ON CONFLICT(cycle_id,profile_id) DO UPDATE SET version_id=excluded.version_id,input_hash=excluded.input_hash,state=excluded.state,reason=excluded.reason,updated_at_ms=excluded.updated_at_ms`, cycle.ID, target.ID, target.VersionID, inputHash, state, reason, now.UnixMilli())
	if err != nil || state != "pending" {
		return err
	}
	j := CompatibilityJob{OwnerID: cycle.OwnerID, FromUserID: cycle.UserID, TargetUserID: target.ID, VersionID: target.VersionID, Generation: cycle.Generation, OwnHash: ownHash, TargetHash: compatibilityProfileHash(target.Profile), InputHash: inputHash, FilterHash: compatibilityArchiveScope, OwnProfile: append([]byte(nil), session.Account...), TargetProfile: []byte(target.RawJSON)}
	if err = s.Store.enqueueCompatibility(ctx, j, now); err != nil {
		return err
	}
	var jobID int64
	var jobState, jobReason string
	err = s.Store.DB.QueryRowContext(ctx, `SELECT j.id,j.state,j.reason FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id
 WHERE p.owner_id=? AND p.from_user_id=? AND p.target_user_id=? AND (j.input_hash=? OR j.state IN ('unknown','sending'))
 ORDER BY CASE j.state WHEN 'unknown' THEN 0 WHEN 'sending' THEN 1 ELSE 2 END,j.id DESC LIMIT 1`, cycle.OwnerID, cycle.UserID, target.ID, inputHash).Scan(&jobID, &jobState, &jobReason)
	if err != nil {
		return err
	}
	switch jobState {
	case "succeeded", "unknown", "rejected":
		state = jobState
	case "cancelled":
		if jobReason == "target_unavailable" {
			state = "unavailable"
		}
	}
	_, err = s.Store.DB.ExecContext(ctx, `UPDATE compatibility_cycle_targets SET job_id=?,state=?,reason=?,updated_at_ms=? WHERE cycle_id=? AND profile_id=? AND input_hash=?`, jobID, state, jobReason, now.UnixMilli(), cycle.ID, target.ID, inputHash)
	return err
}
