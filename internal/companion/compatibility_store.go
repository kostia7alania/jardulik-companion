package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrCompatibilityInputChanged = errors.New("compatibility inputs changed")
	ErrCompatibilityUnavailable  = errors.New("compatibility account unavailable")
	ErrCompatibilityBusy         = errors.New("compatibility cooldown active")
	ErrCompatibilityCached       = errors.New("compatibility resolved from saved analysis")
)

const compatibilitySchema = `
CREATE TABLE IF NOT EXISTS compatibility_pairs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 from_user_id INTEGER NOT NULL, target_user_id INTEGER NOT NULL,
 latest_analysis_id INTEGER REFERENCES profile_analyses(id) ON DELETE SET NULL,
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 CHECK(from_user_id<>target_user_id), UNIQUE(owner_id,from_user_id,target_user_id)
);
CREATE TABLE IF NOT EXISTS compatibility_jobs (
 id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id INTEGER NOT NULL REFERENCES compatibility_pairs(id) ON DELETE CASCADE,
 generation TEXT NOT NULL, version_id INTEGER REFERENCES versions(id) ON DELETE SET NULL,
 own_hash TEXT NOT NULL, target_hash TEXT NOT NULL, input_hash TEXT NOT NULL, filter_hash TEXT NOT NULL,
 own_profile BLOB NOT NULL, target_profile BLOB NOT NULL,
 request_body BLOB NOT NULL DEFAULT X'', response_body BLOB NOT NULL DEFAULT X'', http_status INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('queued','preparing','sending','succeeded','rejected','cancelled','unknown')),
 reason TEXT NOT NULL DEFAULT '', next_at_ms INTEGER NOT NULL DEFAULT 0,
 analysis_id INTEGER REFERENCES profile_analyses(id) ON DELETE SET NULL,
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, UNIQUE(pair_id,input_hash)
);
CREATE INDEX IF NOT EXISTS compatibility_jobs_pending ON compatibility_jobs(state,next_at_ms,id);
CREATE TABLE IF NOT EXISTS compatibility_schedule (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, own_hash TEXT NOT NULL, filter_hash TEXT NOT NULL,
 last_profile_id INTEGER NOT NULL DEFAULT 0, next_plan_ms INTEGER NOT NULL DEFAULT 0,
 last_dispatch_ms INTEGER NOT NULL DEFAULT 0, next_dispatch_ms INTEGER NOT NULL DEFAULT 0
);`

type CompatibilityJob struct {
	ID, PairID, OwnerID, FromUserID, TargetUserID, VersionID              int64
	Generation, OwnHash, TargetHash, InputHash, FilterHash, State, Reason string
	OwnProfile, TargetProfile, RequestBody, ResponseBody                  []byte
	HTTPStatus                                                            int
	AnalysisID                                                            int64
}

const compatibilityJobColumns = `j.id,j.pair_id,p.owner_id,p.from_user_id,p.target_user_id,COALESCE(j.version_id,0),j.generation,j.own_hash,j.target_hash,j.input_hash,j.filter_hash,j.state,j.reason,j.own_profile,j.target_profile,j.request_body,j.response_body,j.http_status,COALESCE(j.analysis_id,0)`

func scanCompatibilityJob(row interface{ Scan(...any) error }) (*CompatibilityJob, error) {
	j := new(CompatibilityJob)
	err := row.Scan(&j.ID, &j.PairID, &j.OwnerID, &j.FromUserID, &j.TargetUserID, &j.VersionID, &j.Generation, &j.OwnHash, &j.TargetHash, &j.InputHash, &j.FilterHash, &j.State, &j.Reason, &j.OwnProfile, &j.TargetProfile, &j.RequestBody, &j.ResponseBody, &j.HTTPStatus, &j.AnalysisID)
	return j, err
}

func compatibilityBoundJob(ctx context.Context, tx *sql.Tx, j CompatibilityJob) error {
	stored, err := scanCompatibilityJob(tx.QueryRowContext(ctx, "SELECT "+compatibilityJobColumns+" FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id WHERE j.id=? AND j.pair_id=?", j.ID, j.PairID))
	if err != nil {
		return err
	}
	if stored.OwnerID != j.OwnerID || stored.FromUserID != j.FromUserID || stored.TargetUserID != j.TargetUserID || stored.Generation != j.Generation || stored.OwnHash != j.OwnHash || stored.TargetHash != j.TargetHash || stored.InputHash != j.InputHash || j.InputHash != compatibilityInputHash(j.OwnHash, j.TargetHash) {
		return errors.New("compatibility job identity mismatch")
	}
	return nil
}

func (s *Store) InitCompatibility(ctx context.Context) error {
	if _, err := s.DB.ExecContext(ctx, analysisSchema); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, compatibilitySchema)
	return err
}

// Exclude balances, subscription dates and other account counters from cache identity.
func compatibilityProfileHash(p Profile) string {
	fields := Profile{}
	for _, key := range []string{"userId", "name", "surName", "birthday", "age", "gender", "country", "city", "worldview", "description", "profession", "familyNames", "children", "goal", "height", "language", "languages", "fileName", "miniFileName", "photos", "miniPhotos"} {
		if value, ok := p[key]; ok {
			fields[key] = value
		}
	}
	raw, _ := json.Marshal(fields)
	return digest(append([]byte("jardulik-compatibility-profile-v1\n"), raw...))
}
func compatibilityInputHash(ownHash, targetHash string) string {
	return digest([]byte("jardulik-compatibility-pair-v1\n" + ownHash + "\n" + targetHash))
}

func (s *Store) RecoverCompatibilityJobs(ctx context.Context, now time.Time) error {
	if err := s.InitCompatibility(ctx); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE compatibility_jobs SET state='unknown',reason='process_restarted',updated_at_ms=? WHERE state='sending'", now.UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE compatibility_jobs SET state='queued',reason='preflight_restarted',next_at_ms=?,updated_at_ms=? WHERE state='preparing'", now.UnixMilli(), now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func compatibilityCooldown(ctx context.Context, tx *sql.Tx, now time.Time) error {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='compatibility_next_at_ms'").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	next, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return err
	}
	if next > now.UnixMilli() {
		return ErrCompatibilityBusy
	}
	return nil
}

func (s *Store) ClaimCompatibilityJob(ctx context.Context, now time.Time) (*CompatibilityJob, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='compatibility_next_at_ms'"); err != nil {
		return nil, err
	}
	if err = compatibilityCooldown(ctx, tx, now); errors.Is(err, ErrCompatibilityBusy) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	j, err := scanCompatibilityJob(tx.QueryRowContext(ctx, `SELECT `+compatibilityJobColumns+` FROM compatibility_jobs j
 JOIN compatibility_pairs p ON p.id=j.pair_id JOIN users u ON u.id=p.owner_id
 JOIN sessions s ON s.owner_id=p.owner_id AND s.upstream_id=p.from_user_id
 LEFT JOIN compatibility_schedule c ON c.owner_id=p.owner_id
 WHERE j.state='queued' AND j.next_at_ms<=? AND COALESCE(c.next_dispatch_ms,0)<=? AND u.paused=0 AND s.state='connected'
 AND json_valid(s.account_json) AND COALESCE(json_extract(s.account_json,'$.freezeState'),1)=0 AND json_extract(s.account_json,'$.age') BETWEEN 18 AND 111
 AND NOT EXISTS(SELECT 1 FROM compatibility_jobs blocked WHERE blocked.pair_id=j.pair_id AND blocked.state IN ('sending','unknown'))
 ORDER BY COALESCE(c.last_dispatch_ms,0),j.id LIMIT 1`, now.UnixMilli(), now.UnixMilli()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	session, err := s.businessSession(ctx, tx, j.OwnerID)
	if err != nil {
		return nil, err
	}
	if session.UserID != j.FromUserID || session.State != "connected" || session.Generation == "" {
		return nil, ErrCompatibilityUnavailable
	}
	// This job has never sent HTTP, so a login to the same account can rebind it.
	j.Generation = session.Generation
	result, err := tx.ExecContext(ctx, "UPDATE compatibility_jobs SET state='preparing',generation=?,reason='',updated_at_ms=? WHERE id=? AND state='queued'", j.Generation, now.UnixMilli(), j.ID)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return nil, errors.New("compatibility claim lost")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO compatibility_schedule(owner_id,upstream_id,own_hash,filter_hash,last_dispatch_ms)
 VALUES(?,?,?,?,?) ON CONFLICT(owner_id) DO UPDATE SET last_dispatch_ms=excluded.last_dispatch_ms`, j.OwnerID, j.FromUserID, j.OwnHash, j.FilterHash, now.UnixMilli()); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	j.State = "preparing"
	return j, nil
}

func (s *Store) ReleaseCompatibilityJob(ctx context.Context, j CompatibilityJob, now time.Time, reason string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='compatibility_next_at_ms'"); err != nil {
		return err
	}
	if err = compatibilityBoundJob(ctx, tx, j); err != nil {
		return err
	}
	delay := 5 * time.Second
	if reason == "preflight_unavailable" {
		delay = time.Minute
	}
	result, err := tx.ExecContext(ctx, "UPDATE compatibility_jobs SET state='queued',reason=?,next_at_ms=?,updated_at_ms=? WHERE id=? AND pair_id=? AND state='preparing'", reason, now.Add(delay).UnixMilli(), now.UnixMilli(), j.ID, j.PairID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("compatibility release lost")
	}
	if reason == "preflight_unavailable" {
		if _, err = tx.ExecContext(ctx, "UPDATE compatibility_schedule SET next_dispatch_ms=? WHERE owner_id=?", now.Add(delay).UnixMilli(), j.OwnerID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) StartCompatibilityJob(ctx context.Context, j CompatibilityJob, ownRaw, targetRaw, request []byte, now time.Time, gap time.Duration) error {
	own, err := debugProfile(ownRaw)
	if err != nil {
		return err
	}
	target, err := debugProfile(targetRaw)
	if err != nil {
		return err
	}
	if own.ID() != j.FromUserID || target.ID() != j.TargetUserID || compatibilityProfileHash(own) != j.OwnHash || compatibilityProfileHash(target) != j.TargetHash {
		return ErrCompatibilityInputChanged
	}
	var sent struct {
		From   int64 `json:"fromUserId"`
		Target int64 `json:"targetUserId"`
	}
	if json.Unmarshal(request, &sent) != nil || sent.From != j.FromUserID || sent.Target != j.TargetUserID || gap <= 0 {
		return errors.New("invalid compatibility request")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='compatibility_next_at_ms'"); err != nil {
		return err
	}
	if err = compatibilityBoundJob(ctx, tx, j); err != nil {
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
	var blocked bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM compatibility_jobs WHERE pair_id=? AND state IN ('sending','unknown'))", j.PairID).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return ErrCompatibilityUnavailable
	}
	// A manual calculation may have completed after this job was planned or claimed.
	analysisID, err := s.reusableCompatibilityAnalysis(ctx, tx, j)
	if err != nil {
		return err
	}
	if analysisID > 0 {
		var savedOwn, savedTarget, savedRequest, savedResponse []byte
		var savedStatus int
		if err = tx.QueryRowContext(ctx, "SELECT own_profile,target_profile,request_body,response_body,http_status FROM profile_analyses WHERE id=?", analysisID).Scan(&savedOwn, &savedTarget, &savedRequest, &savedResponse, &savedStatus); err != nil {
			return err
		}
		result, updateErr := tx.ExecContext(ctx, `UPDATE compatibility_jobs SET state='succeeded',analysis_id=?,http_status=?,own_profile=?,target_profile=?,request_body=?,response_body=?,reason='cached_analysis',updated_at_ms=?
 WHERE id=? AND pair_id=? AND state='preparing'`, analysisID, savedStatus, savedOwn, savedTarget, savedRequest, savedResponse, now.UnixMilli(), j.ID, j.PairID)
		if updateErr != nil {
			return updateErr
		}
		n, updateErr := result.RowsAffected()
		if updateErr != nil {
			return updateErr
		}
		if n != 1 {
			return errors.New("compatibility cache resolution lost")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE compatibility_pairs SET latest_analysis_id=?,updated_at_ms=? WHERE id=?", analysisID, now.UnixMilli(), j.PairID); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		return ErrCompatibilityCached
	}
	if err = compatibilityCooldown(ctx, tx, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE compatibility_jobs SET state='sending',own_profile=?,target_profile=?,request_body=?,reason='',updated_at_ms=?
 WHERE id=? AND pair_id=? AND generation=? AND input_hash=? AND state='preparing'`, ownRaw, targetRaw, request, now.UnixMilli(), j.ID, j.PairID, j.Generation, j.InputHash)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("compatibility start lost")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('compatibility_next_at_ms',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", strconv.FormatInt(now.Add(gap).UnixMilli(), 10)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE compatibility_schedule SET last_dispatch_ms=? WHERE owner_id=?", now.UnixMilli(), j.OwnerID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) FinishCompatibilityJob(ctx context.Context, j CompatibilityJob, state string, status int, response []byte, analysisID int64, reason string, now time.Time) error {
	if state != "succeeded" && state != "rejected" && state != "unknown" && state != "cancelled" {
		return errors.New("invalid compatibility outcome")
	}
	if response == nil {
		response = []byte{}
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='compatibility_next_at_ms'"); err != nil {
		return err
	}
	if err = compatibilityBoundJob(ctx, tx, j); err != nil {
		return err
	}
	var analysis any
	if analysisID > 0 {
		var owner, from, target int64
		var ownRaw, targetRaw, analysisResponse []byte
		var httpStatus int
		var outcome string
		err = tx.QueryRowContext(ctx, "SELECT owner_id,upstream_id,profile_id,own_profile,target_profile,http_status,state,response_body FROM profile_analyses WHERE id=?", analysisID).Scan(&owner, &from, &target, &ownRaw, &targetRaw, &httpStatus, &outcome, &analysisResponse)
		if err != nil {
			return err
		}
		own, oe := debugProfile(ownRaw)
		targetProfile, te := debugProfile(targetRaw)
		if owner != j.OwnerID || from != j.FromUserID || target != j.TargetUserID || oe != nil || te != nil || compatibilityProfileHash(own) != j.OwnHash || compatibilityProfileHash(targetProfile) != j.TargetHash {
			return errors.New("compatibility analysis identity mismatch")
		}
		if httpStatus != status || !bytes.Equal(analysisResponse, response) {
			return errors.New("compatibility analysis response mismatch")
		}
		if state == "succeeded" && (httpStatus < 200 || httpStatus >= 300 || outcome != "response_received" || !compatibilityGPTSuccess(analysisResponse)) {
			return errors.New("compatibility analysis did not succeed")
		}
		analysis = analysisID
	}
	if state == "succeeded" && analysisID <= 0 {
		return errors.New("successful compatibility requires saved analysis")
	}
	previous := "sending"
	if state == "cancelled" {
		previous = "preparing"
	}
	unavailable := state == "cancelled" && reason == "target_unavailable"
	result, err := tx.ExecContext(ctx, "UPDATE compatibility_jobs SET state=?,http_status=?,response_body=?,analysis_id=?,reason=?,next_at_ms=CASE WHEN ? THEN ? ELSE next_at_ms END,updated_at_ms=? WHERE id=? AND pair_id=? AND state=?", state, status, response, analysis, reason, unavailable, now.Add(24*time.Hour).UnixMilli(), now.UnixMilli(), j.ID, j.PairID, previous)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("compatibility finish lost")
	}
	if state == "succeeded" {
		if _, err = tx.ExecContext(ctx, "UPDATE compatibility_pairs SET latest_analysis_id=?,updated_at_ms=? WHERE id=? AND owner_id=? AND from_user_id=? AND target_user_id=?", analysisID, now.UnixMilli(), j.PairID, j.OwnerID, j.FromUserID, j.TargetUserID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) CompatibilityStatus(ctx context.Context, owner int64) (map[string]any, error) {
	if err := s.InitCompatibility(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, "SELECT j.state,COUNT(*) FROM compatibility_jobs j JOIN compatibility_pairs p ON p.id=j.pair_id WHERE p.owner_id=? GROUP BY j.state", owner)
	if err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	for rows.Next() {
		var state string
		var count int64
		if err = rows.Scan(&state, &count); err != nil {
			rows.Close()
			return nil, err
		}
		counts[state] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var pairs int64
	if err = s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM compatibility_pairs WHERE owner_id=?", owner).Scan(&pairs); err != nil {
		return nil, err
	}
	return map[string]any{"owner_id": owner, "pairs": pairs, "jobs": counts, "next_post_ms": s.Meta(ctx, "compatibility_next_at_ms")}, nil
}

func compatibilityGPTSuccess(raw []byte) bool {
	var response struct {
		Content string `json:"content"`
	}
	return json.Unmarshal(raw, &response) == nil && strings.TrimSpace(response.Content) != ""
}
