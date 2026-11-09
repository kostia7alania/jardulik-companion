package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const contactJobsSchema = `
CREATE TABLE IF NOT EXISTS contact_requests (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, generation TEXT NOT NULL,
 mutual_batch_id INTEGER REFERENCES relation_batches(id) ON DELETE SET NULL,
 proof_at_ms INTEGER NOT NULL, retry_after_ms INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('queued','sending','awaiting_receipt','received','unknown','rejected','cancelled')),
 request_json TEXT NOT NULL DEFAULT '', response_body BLOB NOT NULL DEFAULT X'',
 http_status INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
 attempted_at_ms INTEGER NOT NULL DEFAULT 0, completed_at_ms INTEGER NOT NULL DEFAULT 0,
 created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
 UNIQUE(owner_id,upstream_id,profile_id)
);
CREATE INDEX IF NOT EXISTS contact_requests_pending ON contact_requests(state,id);
`

type ContactJob struct {
	ID, OwnerID, UpstreamID, ProfileID, MutualBatchID int64
	Generation, State                                 string
}

func (s *Store) initContactJobs(ctx context.Context) error {
	for _, init := range []func(context.Context) error{s.initRelations, s.initContactEvidence, s.initBusinessContacts} {
		if err := init(ctx); err != nil {
			return err
		}
	}
	_, err := s.DB.ExecContext(ctx, contactJobsSchema)
	return err
}

func (s *Store) knownContactTx(ctx context.Context, tx *sql.Tx, owner, upstream, profile int64) (bool, error) {
	var known bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM profile_contact_evidence WHERE owner_id=? AND upstream_id=? AND profile_id=? AND source_bot_id=? AND source_peer_id=?
 UNION ALL SELECT 1 FROM profile_business_contacts WHERE owner_id=? AND upstream_id=? AND profile_id=? AND source_bot_id=?)`,
		owner, upstream, profile, s.cfg.SourceBotID, s.cfg.SourceBotID,
		owner, upstream, profile, s.cfg.SourceBotID).Scan(&known)
	return known, err
}

func (s *Store) contactBindingTx(ctx context.Context, tx *sql.Tx, owner int64, expected Session) (bool, error) {
	if s.cfg.SourceBotID <= 0 {
		return false, nil
	}
	current, err := s.businessSession(ctx, tx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if expected.UserID <= 0 || expected.Generation == "" || current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return false, nil
	}
	var paused bool
	var account string
	if err = tx.QueryRowContext(ctx, "SELECT u.paused,s.account_json FROM users u JOIN sessions s ON s.owner_id=u.id WHERE u.id=?", owner).Scan(&paused, &account); err != nil {
		return false, err
	}
	if paused {
		return false, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(account), &fields) != nil {
		return false, nil
	}
	if freeze, ok := fields["freezeState"]; ok && !bytes.Equal(bytes.TrimSpace(freeze), []byte("null")) && !bytes.Equal(bytes.TrimSpace(freeze), []byte("false")) {
		return false, nil
	}
	var bound bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM business_contact_connections WHERE owner_id=? AND upstream_id=? AND session_generation=? AND enabled=1)`, owner, expected.UserID, expected.Generation).Scan(&bound)
	return bound, err
}

// Called only while saving a freshly fetched mutual page, never for an old batch.
func (s *Store) queueMutualContactsTx(ctx context.Context, tx *sql.Tx, owner int64, session Session, batch int64, profiles []Profile, now time.Time) error {
	bound, err := s.contactBindingTx(ctx, tx, owner, session)
	if err != nil || !bound {
		return err
	}
	for _, profile := range profiles {
		id := profile.ID()
		if id <= 0 {
			return errors.New("invalid mutual contact profile")
		}
		known, err := s.knownContactTx(ctx, tx, owner, session.UserID, id)
		if err != nil {
			return err
		}
		if known {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO contact_requests(owner_id,upstream_id,profile_id,generation,mutual_batch_id,proof_at_ms,state,created_at_ms,updated_at_ms)
 VALUES(?,?,?,?,?,?,'queued',?,?) ON CONFLICT(owner_id,upstream_id,profile_id) DO UPDATE SET generation=excluded.generation,mutual_batch_id=excluded.mutual_batch_id,proof_at_ms=excluded.proof_at_ms,retry_after_ms=0,updated_at_ms=excluded.updated_at_ms
 WHERE contact_requests.state='queued' AND contact_requests.attempted_at_ms=0`, owner, session.UserID, id, session.Generation, batch, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return err
		}
	}
	return nil
}

const contactJobColumns = "id,owner_id,upstream_id,profile_id,COALESCE(mutual_batch_id,0),generation,state"

func scanContactJob(row interface{ Scan(...any) error }) (ContactJob, error) {
	var job ContactJob
	err := row.Scan(&job.ID, &job.OwnerID, &job.UpstreamID, &job.ProfileID, &job.MutualBatchID, &job.Generation, &job.State)
	return job, err
}

func (s *Store) contactJobReadyTx(ctx context.Context, tx *sql.Tx, job ContactJob, now time.Time) (bool, error) {
	var proof, attempted, retry int64
	current, err := scanContactJob(tx.QueryRowContext(ctx, "SELECT "+contactJobColumns+" FROM contact_requests WHERE id=?", job.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current != job || current.State != "queued" || current.MutualBatchID <= 0 {
		return false, nil
	}
	if err = tx.QueryRowContext(ctx, "SELECT proof_at_ms,attempted_at_ms,retry_after_ms FROM contact_requests WHERE id=?", job.ID).Scan(&proof, &attempted, &retry); err != nil {
		return false, err
	}
	if retry > now.UnixMilli() || attempted != 0 || proof > now.UnixMilli() || proof < now.Add(-24*time.Hour).UnixMilli() {
		return false, nil
	}
	var mutual bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM relation_batches b JOIN relation_profiles p ON p.batch_id=b.id AND p.owner_id=b.owner_id WHERE b.id=? AND b.owner_id=? AND b.mutual=1 AND p.profile_id=?)`, current.MutualBatchID, current.OwnerID, current.ProfileID).Scan(&mutual); err != nil || !mutual {
		return false, err
	}
	bound, err := s.contactBindingTx(ctx, tx, current.OwnerID, Session{UserID: current.UpstreamID, Generation: current.Generation})
	if err != nil || !bound {
		return false, err
	}
	known, err := s.knownContactTx(ctx, tx, current.OwnerID, current.UpstreamID, current.ProfileID)
	return !known, err
}

// ContactJobReady is a read-only guard before any token refresh or account HTTP.
func (s *Store) ContactJobReady(ctx context.Context, job ContactJob, now time.Time) (bool, error) {
	if job.ID <= 0 || now.IsZero() {
		return false, errors.New("invalid contact job readiness")
	}
	if err := s.initContactJobs(ctx); err != nil {
		return false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	return s.contactJobReadyTx(ctx, tx, job, now)
}

func (s *Store) NextContactJob(ctx context.Context) (ContactJob, error) {
	var empty ContactJob
	if err := s.initContactJobs(ctx); err != nil {
		return empty, err
	}
	now := time.Now().UTC()
	// An operator may have imported a contact after its job was queued.
	if _, err := s.DB.ExecContext(ctx, `UPDATE contact_requests AS j SET state='received',completed_at_ms=?,updated_at_ms=? WHERE j.state='queued' AND (
 EXISTS(SELECT 1 FROM profile_contact_evidence e WHERE e.owner_id=j.owner_id AND e.upstream_id=j.upstream_id AND e.profile_id=j.profile_id AND e.source_bot_id=? AND e.source_peer_id=?) OR
 EXISTS(SELECT 1 FROM profile_business_contacts e WHERE e.owner_id=j.owner_id AND e.upstream_id=j.upstream_id AND e.profile_id=j.profile_id AND e.source_bot_id=?))`, now.UnixMilli(), now.UnixMilli(), s.cfg.SourceBotID, s.cfg.SourceBotID, s.cfg.SourceBotID); err != nil {
		return empty, err
	}
	var after int64
	for {
		rows, err := s.DB.QueryContext(ctx, `SELECT j.id,j.owner_id,j.upstream_id,j.profile_id,COALESCE(j.mutual_batch_id,0),j.generation,j.state FROM contact_requests j
 JOIN users u ON u.id=j.owner_id JOIN sessions s ON s.owner_id=j.owner_id AND s.upstream_id=j.upstream_id
 WHERE j.id>? AND j.state='queued' AND j.retry_after_ms<=? AND j.proof_at_ms>=? AND j.proof_at_ms<=? AND u.paused=0 AND s.state='connected'
 AND EXISTS(SELECT 1 FROM business_contact_connections b WHERE b.owner_id=j.owner_id AND b.upstream_id=j.upstream_id AND b.session_generation=j.generation AND b.enabled=1)
 ORDER BY j.id LIMIT 64`, after, now.UnixMilli(), now.Add(-24*time.Hour).UnixMilli(), now.UnixMilli())
		if err != nil {
			return empty, err
		}
		var jobs []ContactJob
		for rows.Next() {
			job, e := scanContactJob(rows)
			if e != nil {
				rows.Close()
				return empty, e
			}
			jobs = append(jobs, job)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return empty, err
		}
		if len(jobs) == 0 {
			return empty, sql.ErrNoRows
		}
		for _, job := range jobs {
			after = job.ID
			ready, err := s.ContactJobReady(ctx, job, now)
			if err != nil {
				return empty, err
			}
			if ready {
				return job, nil
			}
		}
	}
}

// The caller holds SessionMutex. The SQLite writer also protects other Store instances.
func (s *Store) ClaimContactJob(ctx context.Context, id int64, expected Session, request []byte, now time.Time) (bool, error) {
	if id <= 0 || now.IsZero() || len(request) > 1024 {
		return false, errors.New("invalid contact job claim")
	}
	var payload map[string]json.RawMessage
	var from, target int64
	if json.Unmarshal(request, &payload) != nil || len(payload) != 2 || json.Unmarshal(payload["fromUserId"], &from) != nil || json.Unmarshal(payload["targetUserId"], &target) != nil {
		return false, errors.New("invalid contact request body")
	}
	if err := s.initContactJobs(ctx); err != nil {
		return false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_requests'"); err != nil {
		return false, err
	}
	job, err := scanContactJob(tx.QueryRowContext(ctx, "SELECT "+contactJobColumns+" FROM contact_requests WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if job.UpstreamID != expected.UserID || job.Generation != expected.Generation || from != job.UpstreamID || target != job.ProfileID {
		return false, nil
	}
	known, err := s.knownContactTx(ctx, tx, job.OwnerID, job.UpstreamID, job.ProfileID)
	if err != nil {
		return false, err
	}
	if known && job.State == "queued" {
		if _, err = tx.ExecContext(ctx, "UPDATE contact_requests SET state='received',completed_at_ms=?,updated_at_ms=? WHERE id=? AND state='queued'", now.UnixMilli(), now.UnixMilli(), id); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	ready, err := s.contactJobReadyTx(ctx, tx, job, now)
	if err != nil || !ready {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE contact_requests SET state='sending',request_json=?,attempted_at_ms=?,updated_at_ms=? WHERE id=? AND state='queued' AND attempted_at_ms=0`, string(request), now.UnixMilli(), now.UnixMilli(), id)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (s *Store) FinishContactJob(ctx context.Context, id int64, state string, httpStatus int, body []byte, reason string, now time.Time) error {
	if id <= 0 || now.IsZero() || httpStatus < 0 || httpStatus > 599 || len(body) > 16<<20 || len(reason) > 1024 {
		return errors.New("invalid contact job result")
	}
	switch state {
	case "awaiting_receipt", "unknown", "rejected", "cancelled":
	default:
		return errors.New("invalid contact job result state")
	}
	if body == nil {
		body = []byte{}
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE contact_requests SET state=CASE WHEN state='sending' THEN ? ELSE state END,http_status=?,response_body=?,reason=?,completed_at_ms=?,updated_at_ms=?
 WHERE id=? AND attempted_at_ms>0 AND state IN ('sending','received')`, state, httpStatus, body, reason, now.UnixMilli(), now.UnixMilli(), id)
	return err
}

func (s *Store) DeferContactJob(ctx context.Context, id int64, reason string, retryAt time.Time) error {
	if id <= 0 || retryAt.IsZero() || len(reason) > 1024 {
		return errors.New("invalid contact job backoff")
	}
	_, err := s.DB.ExecContext(ctx, "UPDATE contact_requests SET retry_after_ms=?,reason=?,updated_at_ms=? WHERE id=? AND state='queued' AND attempted_at_ms=0", retryAt.UnixMilli(), reason, time.Now().UTC().UnixMilli(), id)
	return err
}

func (s *Store) RecoverContactJobs(ctx context.Context) error {
	if err := s.initContactJobs(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, "UPDATE contact_requests SET state='unknown',reason='interrupted_before_result',updated_at_ms=? WHERE state='sending'", time.Now().UTC().UnixMilli())
	return err
}

func (s *Store) receiveContactJobTx(ctx context.Context, tx *sql.Tx, owner, upstream, profile int64, now time.Time) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='contact_requests')").Scan(&exists); err != nil || !exists {
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE contact_requests SET state='received',completed_at_ms=?,updated_at_ms=? WHERE owner_id=? AND upstream_id=? AND profile_id=? AND state<>'received'", now.UnixMilli(), now.UnixMilli(), owner, upstream, profile)
	return err
}
