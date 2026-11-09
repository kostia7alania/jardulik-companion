package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const contactSyncSchema = `
CREATE TABLE IF NOT EXISTS contact_sync_state (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 page INTEGER NOT NULL CHECK(page>=1), next_at_ms INTEGER NOT NULL,
 last_error TEXT NOT NULL DEFAULT '', updated_at_ms INTEGER NOT NULL
);`

type contactSyncJob struct {
	owner, updated int64
	page           int
	session        Session
}

func (s *Store) initContactSync(ctx context.Context) error {
	if err := s.initContactJobs(ctx); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, contactSyncSchema)
	return err
}

// Daily reads may refresh a cached freeze flag, unlike contact POST eligibility.
func (s *Store) contactSyncBoundTx(ctx context.Context, tx *sql.Tx, owner int64, expected Session) (bool, error) {
	if s.cfg.SourceBotID <= 0 || expected.UserID <= 0 || expected.Generation == "" {
		return false, nil
	}
	current, err := s.businessSession(ctx, tx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return false, nil
	}
	var bound bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM users u JOIN business_contact_connections b ON b.owner_id=u.id
 WHERE u.id=? AND u.paused=0 AND b.upstream_id=? AND b.session_generation=? AND b.enabled=1)`, owner, expected.UserID, expected.Generation).Scan(&bound)
	return bound, err
}

// RequestContactSync is called with the caller's session lock already held.
func (s *Store) RequestContactSync(ctx context.Context, owner int64, expected Session, now time.Time) error {
	if owner <= 0 || now.IsZero() {
		return errors.New("invalid contact sync request")
	}
	if err := s.initContactSync(ctx); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_sync_state'"); err != nil {
		return err
	}
	bound, err := s.contactSyncBoundTx(ctx, tx, owner, expected)
	if err != nil || !bound {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO contact_sync_state(owner_id,upstream_id,generation,page,next_at_ms,last_error,updated_at_ms)
 VALUES(?,?,?,1,?,'',?) ON CONFLICT(owner_id) DO UPDATE SET upstream_id=excluded.upstream_id,generation=excluded.generation,page=1,
 next_at_ms=excluded.next_at_ms,last_error='',updated_at_ms=MAX(contact_sync_state.updated_at_ms+1,excluded.updated_at_ms)`, owner, expected.UserID, expected.Generation, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) pinContactSync(ctx context.Context, owner int64, session Session, now time.Time) (*contactSyncJob, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_sync_state'"); err != nil {
		return nil, err
	}
	bound, err := s.contactSyncBoundTx(ctx, tx, owner, session)
	if err != nil || !bound {
		return nil, err
	}
	job := &contactSyncJob{owner: owner, session: session}
	var upstream, next int64
	var generation string
	err = tx.QueryRowContext(ctx, "SELECT upstream_id,generation,page,next_at_ms,updated_at_ms FROM contact_sync_state WHERE owner_id=?", owner).Scan(&upstream, &generation, &job.page, &next, &job.updated)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) || upstream != session.UserID || generation != session.Generation {
		_, err = tx.ExecContext(ctx, `INSERT INTO contact_sync_state(owner_id,upstream_id,generation,page,next_at_ms,last_error,updated_at_ms)
 VALUES(?,?,?,1,?,'',?) ON CONFLICT(owner_id) DO UPDATE SET upstream_id=excluded.upstream_id,generation=excluded.generation,page=1,
 next_at_ms=excluded.next_at_ms,last_error='',updated_at_ms=MAX(contact_sync_state.updated_at_ms+1,excluded.updated_at_ms)`, owner, session.UserID, session.Generation, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return nil, err
		}
		if err = tx.QueryRowContext(ctx, "SELECT page,next_at_ms,updated_at_ms FROM contact_sync_state WHERE owner_id=?", owner).Scan(&job.page, &next, &job.updated); err != nil {
			return nil, err
		}
	}
	if next > now.UnixMilli() {
		return nil, nil
	}
	return job, tx.Commit()
}

func (s *Store) nextContactSync(ctx context.Context, now time.Time) (*contactSyncJob, error) {
	if err := s.initContactSync(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT b.owner_id,b.upstream_id,b.session_generation FROM business_contact_connections b
 JOIN users u ON u.id=b.owner_id JOIN sessions s ON s.owner_id=b.owner_id AND s.upstream_id=b.upstream_id
 LEFT JOIN contact_sync_state c ON c.owner_id=b.owner_id
 WHERE b.enabled=1 AND u.paused=0 AND s.state='connected' AND (c.owner_id IS NULL OR c.upstream_id<>b.upstream_id OR c.generation<>b.session_generation OR c.next_at_ms<=?)
 ORDER BY COALESCE(c.next_at_ms,0),b.owner_id`, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	type binding struct {
		owner, upstream int64
		generation      string
	}
	var candidates []binding
	for rows.Next() {
		var candidate binding
		if err = rows.Scan(&candidate.owner, &candidate.upstream, &candidate.generation); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// SQLite has one connection: release the result set before decrypting sessions.
	for _, candidate := range candidates {
		session, err := s.Session(ctx, candidate.owner)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if session.UserID != candidate.upstream || session.Generation != candidate.generation || session.State != "connected" {
			continue
		}
		job, err := s.pinContactSync(ctx, candidate.owner, session, now)
		if err != nil || job != nil {
			return job, err
		}
	}
	return nil, nil
}

func (s *Store) finishContactSync(ctx context.Context, job contactSyncJob, page int, next time.Time, reason string, now time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_sync_state'"); err != nil {
		return err
	}
	// Never move a new account's cursor, or overwrite a newer explicit sync request.
	current, err := s.businessSession(ctx, tx, job.owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.UserID != job.session.UserID || current.Generation != job.session.Generation {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE contact_sync_state SET page=?,next_at_ms=?,last_error=?,updated_at_ms=MAX(updated_at_ms+1,?)
 WHERE owner_id=? AND upstream_id=? AND generation=? AND page=? AND updated_at_ms=?`, page, next.UnixMilli(), reason, now.UnixMilli(), job.owner, job.session.UserID, job.session.Generation, job.page, job.updated)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) contactGap() time.Duration {
	if s.Config.WatchGap > 0 {
		return s.Config.WatchGap
	}
	return 20 * time.Second
}

func (s *Service) runContacts(ctx context.Context) {
	if err := s.Store.RecoverContactJobs(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Warn("contact worker recovery failed", "kind", safeError(err))
		}
		return
	}
	for ctx.Err() == nil {
		worked, err := s.ContactStep(ctx, s.Client.Now())
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("contact worker stopped", "kind", safeError(err))
			}
			return
		}
		delay := time.Second
		if worked {
			delay = s.contactGap()
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}

// ContactStep performs one contact request or one page of the daily mutual check.
func (s *Service) ContactStep(ctx context.Context, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.foregroundPending() {
		return false, nil
	}
	job, err := s.Store.NextContactJob(ctx)
	if err == nil {
		if s.foregroundPending() {
			return false, nil
		}
		err = s.Client.RequestContact(ctx, job)
		if errors.Is(err, ErrContactPersistence) {
			return true, err
		}
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, contactPersistence(err)
	}
	syncJob, err := s.Store.nextContactSync(ctx, now)
	if err != nil || syncJob == nil {
		return false, contactPersistence(err)
	}
	finish := func(page int, next time.Time, reason string) error {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return contactPersistence(s.Store.finishContactSync(writeCtx, *syncJob, page, next, reason, now))
	}
	if s.foregroundPending() {
		return false, nil
	}
	if syncJob.page == 1 {
		account, err := s.Client.ContactAccount(ctx, syncJob.owner, syncJob.session)
		if err != nil {
			if errors.Is(err, ErrContactPersistence) {
				return true, err
			}
			return true, finish(1, now.Add(24*time.Hour), safeError(err))
		}
		if value, present := account["freezeState"]; present && value != nil {
			frozen, ok := value.(bool)
			if !ok || frozen {
				reason := "account_frozen"
				if !ok {
					reason = "account_state_unknown"
				}
				return true, finish(1, now.Add(24*time.Hour), reason)
			}
		}
		if s.foregroundPending() {
			return true, nil
		}
		fresh, err := s.Store.pinContactSync(ctx, syncJob.owner, syncJob.session, now)
		if err != nil {
			return true, contactPersistence(err)
		}
		if fresh == nil || fresh.updated != syncJob.updated || fresh.page != syncJob.page || fresh.session.UserID != syncJob.session.UserID || fresh.session.Generation != syncJob.session.Generation {
			return true, nil
		}
	}
	page, err := s.Client.ReactUsersFrozen(ctx, syncJob.owner, syncJob.session, true, syncJob.page)
	if err != nil {
		if errors.Is(err, ErrContactPersistence) || contactSQLError(err) {
			return true, contactPersistence(err)
		}
		return true, finish(syncJob.page, now.Add(24*time.Hour), safeError(err))
	}
	if len(page.Profiles) >= 1000 {
		return true, finish(syncJob.page+1, now.Add(s.contactGap()), "")
	}
	return true, finish(1, now.Add(24*time.Hour), "")
}

func contactSQLError(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, sql.ErrTxDone)
}

func (s *Service) ContactStatusText(ctx context.Context, owner int64) string {
	if err := s.Store.initContactSync(ctx); err != nil {
		return "Telegram: статус недоступен"
	}
	session, err := s.Store.Session(ctx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "Telegram: сначала подключи Jardulik"
	}
	if err != nil {
		return "Telegram: статус недоступен"
	}
	var saved, queued, waiting, unknown int
	if err = s.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
 SELECT profile_id FROM profile_contact_evidence WHERE owner_id=? AND upstream_id=? AND source_bot_id=? AND source_peer_id=?
 UNION SELECT profile_id FROM profile_business_contacts WHERE owner_id=? AND upstream_id=? AND source_bot_id=?)`, owner, session.UserID, s.Config.SourceBotID, s.Config.SourceBotID, owner, session.UserID, s.Config.SourceBotID).Scan(&saved); err != nil {
		return "Telegram: статус недоступен"
	}
	if err = s.Store.DB.QueryRowContext(ctx, `SELECT COUNT(CASE WHEN state='queued' THEN 1 END),
 COUNT(CASE WHEN state IN ('sending','awaiting_receipt') THEN 1 END),COUNT(CASE WHEN state='unknown' THEN 1 END)
 FROM contact_requests WHERE owner_id=? AND upstream_id=?`, owner, session.UserID).Scan(&queued, &waiting, &unknown); err != nil {
		return "Telegram: статус недоступен"
	}
	parts := []string{fmt.Sprintf("Telegram: сохранено %d", saved)}
	for _, count := range []struct {
		label string
		n     int
	}{{"в очереди", queued}, {"ждём карточку", waiting}, {"нужна проверка", unknown}} {
		if count.n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", count.label, count.n))
		}
	}
	if session.State != "connected" {
		state := "сбор на паузе"
		if session.State == "reconnect_required" {
			state = "нужно переподключить /connect"
		} else if session.State == "paused" {
			var reason string
			if err = s.Store.DB.QueryRowContext(ctx, "SELECT last_error FROM sessions WHERE owner_id=?", owner).Scan(&reason); err != nil {
				return "Telegram: статус недоступен"
			}
			switch reason {
			case "unexpected_response_or_challenge":
				state += " (неожиданный ответ Jardulik)"
			case "rate_limited":
				state += " (лимит Jardulik)"
			case "access_denied":
				state += " (отказ Jardulik)"
			}
		}
		parts = append(parts, state)
	} else if s.Store.Paused(ctx, owner) {
		parts = append(parts, "сбор на паузе")
	} else {
		var bound bool
		if err = s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM business_contact_connections WHERE owner_id=? AND upstream_id=? AND session_generation=? AND enabled=1)`, owner, session.UserID, session.Generation).Scan(&bound); err != nil {
			return "Telegram: статус недоступен"
		}
		if !bound {
			parts = append(parts, "чат Jardulik не подключён")
		} else {
			var account map[string]json.RawMessage
			if json.Unmarshal(session.Account, &account) == nil {
				if flag, exists := account["freezeState"]; exists && !bytes.Equal(bytes.TrimSpace(flag), []byte("null")) && !bytes.Equal(bytes.TrimSpace(flag), []byte("false")) {
					state := "состояние аккаунта не подтверждено"
					if bytes.Equal(bytes.TrimSpace(flag), []byte("true")) {
						state = "анкета заморожена"
					}
					parts = append(parts, state)
				}
			}
		}
	}
	return strings.Join(parts, " · ")
}
