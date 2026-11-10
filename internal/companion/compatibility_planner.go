package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"
)

const compatibilityPlanBatch = 64

func compatibilityMatchesFilters(p Profile, f FeedFilters) bool {
	if f.validate() != nil {
		return false
	}
	age, ok := autoInteger(p["age"])
	if !ok || age < 18 || age > 111 || age < f.AgeMin || age > f.AgeMax {
		return false
	}
	height, ok := autoInteger(p["height"])
	if !ok || height < f.HeightMin || height > f.HeightMax {
		return false
	}
	for key, wanted := range map[string]string{"goal": f.Goal, "children": f.Children, "worldview": f.Worldview, "country": f.Country, "city": f.City} {
		if wanted == "" {
			continue
		}
		actual, ok := p[key].(string)
		if !ok || !strings.EqualFold(strings.TrimSpace(actual), strings.TrimSpace(wanted)) {
			return false
		}
	}
	return true
}

// A separate process plans work; the bot/parser's shared Client executes HTTP.
func (s *Service) runCompatibilityPlanner(ctx context.Context) {
	if !s.Config.CompatibilityEnabled {
		return
	}
	if err := s.Store.InitCompatibility(ctx); err != nil {
		slog.Error("compatibility planner unavailable", "kind", safeError(err))
		return
	}
	for ctx.Err() == nil {
		if err := s.planCompatibility(ctx, time.Now()); err != nil && ctx.Err() == nil {
			slog.Warn("compatibility planning failed", "kind", safeError(err))
		}
		if !sleep(ctx, time.Second) {
			return
		}
	}
}

func (s *Service) planCompatibility(ctx context.Context, now time.Time) error {
	if err := s.Store.InitCompatibility(ctx); err != nil {
		return err
	}
	var owner int64
	err := s.Store.DB.QueryRowContext(ctx, `SELECT u.id FROM users u JOIN sessions se ON se.owner_id=u.id
 LEFT JOIN compatibility_schedule c ON c.owner_id=u.id
 WHERE u.paused=0 AND se.state='connected' AND COALESCE(c.next_plan_ms,0)<=?
 ORDER BY COALESCE(c.next_plan_ms,0),u.id LIMIT 1`, now.UnixMilli()).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if s.Config.SyncCycleEnabled {
		cycle, cycleErr := s.Store.SyncCycle(ctx, owner)
		if cycleErr != nil {
			return cycleErr
		}
		if cycle != nil && cycle.Stage != "complete" {
			return s.deferCompatibilityPlan(ctx, owner, cycle.UserID, now)
		}
		// Daily cycle owns all compatibility planning when enabled.
		return s.deferCompatibilityPlan(ctx, owner, 0, now)
	}
	session, err := s.Store.Session(ctx, owner)
	if err != nil {
		return err
	}
	var own Profile
	if json.Unmarshal(session.Account, &own) != nil || own.ID() != session.UserID {
		return s.deferCompatibilityPlan(ctx, owner, session.UserID, now)
	}
	age, known := autoInteger(own["age"])
	frozen, knownFrozen := own["freezeState"].(bool)
	if !known || age < 18 || age > 111 || !knownFrozen || frozen || session.Generation == "" {
		return s.deferCompatibilityPlan(ctx, owner, session.UserID, now)
	}
	filters, err := s.Autoliker.filters(ctx, owner)
	if err != nil {
		return s.deferCompatibilityPlan(ctx, owner, session.UserID, now)
	}
	ownHash, filterHash := compatibilityProfileHash(own), autoFilterHash(filters)
	var cursor, previousFrom int64
	var previousOwn, previousFilter string
	err = s.Store.DB.QueryRowContext(ctx, "SELECT upstream_id,own_hash,filter_hash,last_profile_id FROM compatibility_schedule WHERE owner_id=?", owner).Scan(&previousFrom, &previousOwn, &previousFilter, &cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if previousFrom != session.UserID || previousOwn != ownHash || previousFilter != filterHash {
		cursor = 0
	}
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen
 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id
 WHERE q.owner_id=? AND q.profile_id>? ORDER BY q.profile_id LIMIT ?`, owner, cursor, compatibilityPlanBatch)
	if err != nil {
		return err
	}
	var candidates []StoredProfile
	for rows.Next() {
		p, e := scanProfile(rows)
		if e != nil {
			rows.Close()
			return e
		}
		candidates = append(candidates, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, target := range candidates {
		cursor = target.ID
		if target.ID == session.UserID || !compatibilityMatchesFilters(target.Profile, filters) {
			continue
		}
		full, e := s.Store.ProfileHasFullCapture(ctx, owner, target.ID, target.VersionID)
		if e != nil {
			return e
		}
		if !full {
			continue
		}
		job := CompatibilityJob{OwnerID: owner, FromUserID: session.UserID, TargetUserID: target.ID, VersionID: target.VersionID, Generation: session.Generation,
			OwnHash: ownHash, TargetHash: compatibilityProfileHash(target.Profile), FilterHash: filterHash, OwnProfile: append([]byte(nil), session.Account...), TargetProfile: []byte(target.RawJSON)}
		job.InputHash = compatibilityInputHash(job.OwnHash, job.TargetHash)
		if e = s.Store.enqueueCompatibility(ctx, job, now); e != nil {
			if errors.Is(e, ErrCompatibilityUnavailable) {
				return nil
			}
			return e
		}
	}
	next := now.Add(5 * time.Second)
	if len(candidates) < compatibilityPlanBatch {
		cursor = 0
		next = now.Add(time.Minute)
	}
	_, err = s.Store.DB.ExecContext(ctx, `INSERT INTO compatibility_schedule(owner_id,upstream_id,own_hash,filter_hash,last_profile_id,next_plan_ms)
 VALUES(?,?,?,?,?,?) ON CONFLICT(owner_id) DO UPDATE SET upstream_id=excluded.upstream_id,own_hash=excluded.own_hash,
 filter_hash=excluded.filter_hash,last_profile_id=excluded.last_profile_id,next_plan_ms=excluded.next_plan_ms`, owner, session.UserID, ownHash, filterHash, cursor, next.UnixMilli())
	return err
}

func (s *Service) deferCompatibilityPlan(ctx context.Context, owner, from int64, now time.Time) error {
	_, err := s.Store.DB.ExecContext(ctx, `INSERT INTO compatibility_schedule(owner_id,upstream_id,own_hash,filter_hash,next_plan_ms)
 VALUES(?,?,'','',?) ON CONFLICT(owner_id) DO UPDATE SET next_plan_ms=excluded.next_plan_ms`, owner, from, now.Add(time.Minute).UnixMilli())
	return err
}

func (s *Store) enqueueCompatibility(ctx context.Context, j CompatibilityJob, now time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='compatibility_next_at_ms'"); err != nil {
		return err
	}
	session, err := s.businessSession(ctx, tx, j.OwnerID)
	if err != nil {
		return err
	}
	var paused bool
	if err = tx.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", j.OwnerID).Scan(&paused); err != nil {
		return err
	}
	if paused || session.State != "connected" || session.UserID != j.FromUserID || session.Generation != j.Generation {
		return ErrCompatibilityUnavailable
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO compatibility_pairs(owner_id,from_user_id,target_user_id,created_at_ms,updated_at_ms)
 VALUES(?,?,?,?,?) ON CONFLICT(owner_id,from_user_id,target_user_id) DO NOTHING`, j.OwnerID, j.FromUserID, j.TargetUserID, now.UnixMilli(), now.UnixMilli()); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT id FROM compatibility_pairs WHERE owner_id=? AND from_user_id=? AND target_user_id=?", j.OwnerID, j.FromUserID, j.TargetUserID).Scan(&j.PairID); err != nil {
		return err
	}
	var blocked bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM compatibility_jobs WHERE pair_id=? AND state IN ('sending','unknown'))", j.PairID).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return tx.Commit()
	}
	var existing int64
	var state, reason string
	var nextAt, updatedAt int64
	var savedAnalysis sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT id,state,analysis_id,reason,next_at_ms,updated_at_ms FROM compatibility_jobs WHERE pair_id=? AND input_hash=?", j.PairID, j.InputHash).Scan(&existing, &state, &savedAnalysis, &reason, &nextAt, &updatedAt)
	if err == nil {
		// An unavailable profile is checked again by the daily cycle, not every planner tick.
		if state == "cancelled" && reason == "target_unavailable" && nextAt > now.UnixMilli() {
			returned := false
			if j.FilterHash == compatibilityArchiveScope {
				// A successful later source read can prove a same-content profile
				// returned during this cycle. This job has never sent a GPT POST.
				err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_full_reads r
 WHERE r.owner_id=? AND r.upstream_id=? AND r.generation=? AND r.profile_id=? AND r.observed_at_ms>?
 AND EXISTS(SELECT 1 FROM profile_presence p WHERE p.owner_id=r.owner_id AND p.profile_id=r.profile_id AND p.availability='available'))`, j.OwnerID, j.FromUserID, j.Generation, j.TargetUserID, updatedAt).Scan(&returned)
				if err != nil {
					return err
				}
			}
			if !returned {
				return tx.Commit()
			}
		}
		if state == "succeeded" && savedAnalysis.Valid {
			_, err = tx.ExecContext(ctx, "UPDATE compatibility_pairs SET latest_analysis_id=?,updated_at_ms=? WHERE id=? AND (latest_analysis_id IS NULL OR latest_analysis_id<>?)", savedAnalysis.Int64, now.UnixMilli(), j.PairID, savedAnalysis.Int64)
		} else if state == "queued" || state == "cancelled" {
			_, err = tx.ExecContext(ctx, `UPDATE compatibility_jobs SET generation=?,version_id=?,filter_hash=?,own_profile=?,target_profile=?,state='queued',reason=CASE WHEN state='cancelled' THEN '' ELSE reason END,next_at_ms=CASE WHEN state='cancelled' THEN 0 ELSE next_at_ms END,updated_at_ms=?
 WHERE id=? AND state IN ('queued','cancelled') AND length(request_body)=0`, j.Generation, j.VersionID, j.FilterHash, j.OwnProfile, j.TargetProfile, now.UnixMilli(), existing)
		}
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	analysisID, err := s.reusableCompatibilityAnalysis(ctx, tx, j)
	if err != nil {
		return err
	}
	state = "queued"
	var analysis any
	if analysisID > 0 {
		state, reason = "succeeded", "cached_analysis"
		analysis = analysisID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO compatibility_jobs(pair_id,generation,version_id,own_hash,target_hash,input_hash,filter_hash,own_profile,target_profile,state,analysis_id,reason,created_at_ms,updated_at_ms)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, j.PairID, j.Generation, j.VersionID, j.OwnHash, j.TargetHash, j.InputHash, j.FilterHash, j.OwnProfile, j.TargetProfile, state, analysis, reason, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	if analysisID > 0 {
		if _, err = tx.ExecContext(ctx, "UPDATE compatibility_pairs SET latest_analysis_id=?,updated_at_ms=? WHERE id=?", analysisID, now.UnixMilli(), j.PairID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) reusableCompatibilityAnalysis(ctx context.Context, tx *sql.Tx, j CompatibilityJob) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,own_profile,target_profile,response_body FROM profile_analyses
 WHERE owner_id=? AND upstream_id=? AND profile_id=? AND state='response_received' AND http_status>=200 AND http_status<300
 ORDER BY observed_at_ms DESC,id DESC`, j.OwnerID, j.FromUserID, j.TargetUserID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var ownRaw, targetRaw, response []byte
		if err = rows.Scan(&id, &ownRaw, &targetRaw, &response); err != nil {
			return 0, err
		}
		own, oe := debugProfile(ownRaw)
		target, te := debugProfile(targetRaw)
		if oe == nil && te == nil && compatibilityGPTSuccess(response) && compatibilityProfileHash(own) == j.OwnHash && compatibilityProfileHash(target) == j.TargetHash {
			return id, nil
		}
	}
	return 0, rows.Err()
}
