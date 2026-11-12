package companion

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

const (
	relationNoticePollGap  = 5 * time.Minute
	relationNoticeFirstGap = 30 * time.Second
)

const relationNoticeSchema = `
CREATE TABLE IF NOT EXISTS relation_notice_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL CHECK(upstream_id>0), profile_id INTEGER NOT NULL CHECK(profile_id>0),
 kind TEXT NOT NULL CHECK(kind IN ('incoming','mutual')), observed_at_ms INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','uncertain','error')),
 chat_id INTEGER NOT NULL DEFAULT 0, thread_id INTEGER NOT NULL DEFAULT 0, message_id INTEGER NOT NULL DEFAULT 0,
 next_attempt_ms INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 UNIQUE(owner_id,upstream_id,profile_id,kind)
);
CREATE INDEX IF NOT EXISTS relation_notice_events_due ON relation_notice_events(state,next_attempt_ms,id);
CREATE TABLE IF NOT EXISTS relation_notice_seen (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL CHECK(upstream_id>0), profile_id INTEGER NOT NULL CHECK(profile_id>0),
 kind TEXT NOT NULL CHECK(kind IN ('incoming','mutual')),
 PRIMARY KEY(owner_id,upstream_id,profile_id,kind)
);
CREATE TABLE IF NOT EXISTS relation_notice_poll_state (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL CHECK(upstream_id>0),
 next_kind TEXT NOT NULL DEFAULT 'incoming' CHECK(next_kind IN ('incoming','mutual')),
 next_attempt_ms INTEGER NOT NULL DEFAULT 0,
 incoming_baseline INTEGER NOT NULL DEFAULT 0 CHECK(incoming_baseline IN (0,1)),
 mutual_baseline INTEGER NOT NULL DEFAULT 0 CHECK(mutual_baseline IN (0,1)),
 PRIMARY KEY(owner_id,upstream_id)
);`

const relationNoticeSourceReady = `s.state='connected' AND s.upstream_id>0 AND u.paused=0
 AND json_valid(s.account_json)
 AND COALESCE(json_type(s.account_json,'$.freezeState') IN ('null','false'),1)`

func (s *Service) initRelationNotices(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, relationNoticeSchema)
	return err
}

func (s *Service) RunRelationNoticePoll(ctx context.Context) {
	if !s.Config.RelationNoticesEnabled || s.Config.OwnerID <= 0 {
		return
	}
	if err := s.initRelationNotices(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Warn("relationship notice initialization failed", "kind", safeError(err))
		}
		return
	}
	for ctx.Err() == nil {
		_, err := s.relationNoticePollStep(ctx, s.Client.Now())
		if err != nil && ctx.Err() == nil && !errors.Is(err, errCrawlYield) {
			slog.Warn("relationship notice poll failed", "kind", safeError(err))
		}
		if !sleep(ctx, 10*time.Second) {
			return
		}
	}
}

func (s *Service) relationNoticePollStep(ctx context.Context, now time.Time) (bool, error) {
	owner := s.Config.OwnerID
	if !s.Config.RelationNoticesEnabled || owner <= 0 || s.foregroundPending() {
		return false, nil
	}
	var upstream int64
	err := s.Store.DB.QueryRowContext(ctx, `SELECT s.upstream_id FROM sessions s JOIN users u ON u.id=s.owner_id
 LEFT JOIN relation_notice_poll_state p ON p.owner_id=s.owner_id AND p.upstream_id=s.upstream_id
 WHERE s.owner_id=? AND `+relationNoticeSourceReady+` AND COALESCE(p.next_attempt_ms,0)<=?`, owner, now.UnixMilli()).Scan(&upstream)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	due := now.Add(relationNoticePollGap).UnixMilli()
	session, kind, err := s.claimRelationNoticePoll(ctx, owner, upstream, now.UnixMilli(), due)
	if err != nil || kind == "" || s.foregroundPending() {
		return false, err
	}
	// Notices only need a relationship snapshot. Contact jobs have their own worker.
	page, err := s.Client.reactUsers(withCrawlPriority(ctx, s), owner, &session, kind == "mutual", 1, false)
	if errors.Is(err, errCrawlYield) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	// A full page may have a successor. Only a complete first page can
	// establish or advance the seen set.
	if page.Profiles == nil || len(page.Profiles) >= 1000 {
		return true, nil
	}
	return true, s.recordRelationNoticePage(ctx, owner, session, page, kind, s.Client.Now(), due)
}

func (s *Service) claimRelationNoticePoll(ctx context.Context, owner, upstream, now, due int64) (Session, string, error) {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, "", err
	}
	defer tx.Rollback()
	// Obtain the write lock before checking the session and persisted cadence.
	if _, err = tx.ExecContext(ctx, `INSERT INTO relation_notice_poll_state(owner_id,upstream_id)
 SELECT s.owner_id,s.upstream_id FROM sessions s JOIN users u ON u.id=s.owner_id
 WHERE s.owner_id=? AND s.upstream_id=? AND `+relationNoticeSourceReady+`
 ON CONFLICT(owner_id,upstream_id) DO NOTHING`, owner, upstream); err != nil {
		return Session{}, "", err
	}
	var kind string
	err = tx.QueryRowContext(ctx, `SELECT p.next_kind FROM relation_notice_poll_state p
 JOIN sessions s ON s.owner_id=p.owner_id AND s.upstream_id=p.upstream_id JOIN users u ON u.id=s.owner_id
 WHERE p.owner_id=? AND p.upstream_id=? AND p.next_attempt_ms<=? AND `+relationNoticeSourceReady, owner, upstream, now).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, "", nil
	}
	if err != nil {
		return Session{}, "", err
	}
	session, err := s.Store.businessSession(ctx, tx, owner)
	if err != nil || session.Generation == "" {
		return Session{}, "", err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE relation_notice_poll_state SET
 next_kind=CASE WHEN next_kind='incoming' THEN 'mutual' ELSE 'incoming' END, next_attempt_ms=?
 WHERE owner_id=? AND upstream_id=?`, due, owner, upstream); err != nil {
		return Session{}, "", err
	}
	return session, kind, tx.Commit()
}

func (s *Service) recordRelationNoticePage(ctx context.Context, owner int64, expected Session, page RelationPage, kind string, observedAt time.Time, due int64) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	claim, err := tx.ExecContext(ctx, `UPDATE relation_notice_poll_state SET next_attempt_ms=next_attempt_ms
 WHERE owner_id=? AND upstream_id=? AND next_attempt_ms=? AND next_kind<>?
 AND EXISTS(SELECT 1 FROM sessions s JOIN users u ON u.id=s.owner_id
 WHERE s.owner_id=? AND s.upstream_id=? AND `+relationNoticeSourceReady+`)`, owner, expected.UserID, due, kind, owner, expected.UserID)
	if err != nil {
		return err
	}
	if affected, e := claim.RowsAffected(); e != nil || affected == 0 {
		return e
	}
	current, err := s.Store.businessSession(ctx, tx, owner)
	if err != nil || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return err
	}
	var incomingBaseline, mutualBaseline bool
	if err = tx.QueryRowContext(ctx, `SELECT incoming_baseline,mutual_baseline FROM relation_notice_poll_state WHERE owner_id=? AND upstream_id=?`,
		owner, expected.UserID).Scan(&incomingBaseline, &mutualBaseline); err != nil {
		return err
	}
	if (kind == "incoming" && incomingBaseline) || (kind == "mutual" && mutualBaseline) {
		_, err = tx.ExecContext(ctx, `INSERT INTO relation_notice_events(owner_id,upstream_id,profile_id,kind,observed_at_ms)
 SELECT DISTINCT ?,?,p.profile_id,?,? FROM relation_profiles p WHERE p.batch_id=? AND p.owner_id=?
 AND NOT EXISTS(SELECT 1 FROM relation_notice_seen WHERE owner_id=? AND upstream_id=? AND profile_id=p.profile_id AND kind=?)
 ON CONFLICT(owner_id,upstream_id,profile_id,kind) DO NOTHING`,
			owner, expected.UserID, kind, observedAt.UnixMilli(), page.ID, owner, owner, expected.UserID, kind)
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO relation_notice_seen(owner_id,upstream_id,profile_id,kind)
 SELECT ?,?,profile_id,? FROM relation_profiles WHERE batch_id=? AND owner_id=?
 ON CONFLICT(owner_id,upstream_id,profile_id,kind) DO NOTHING`, owner, expected.UserID, kind, page.ID, owner); err != nil {
		return err
	}
	if !incomingBaseline && !mutualBaseline {
		due = observedAt.Add(relationNoticeFirstGap).UnixMilli()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE relation_notice_poll_state SET next_attempt_ms=?,
 incoming_baseline=incoming_baseline OR ?='incoming', mutual_baseline=mutual_baseline OR ?='mutual'
 WHERE owner_id=? AND upstream_id=?`, due, kind, kind, owner, expected.UserID); err != nil {
		return err
	}
	return tx.Commit()
}
