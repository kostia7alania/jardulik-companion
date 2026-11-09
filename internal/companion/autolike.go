package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const autoSchema = `
CREATE TABLE IF NOT EXISTS autolike_settings(
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 mode TEXT NOT NULL DEFAULT 'off', interval_seconds INTEGER NOT NULL DEFAULT 30,
 series_count INTEGER NOT NULL DEFAULT 10,run_limit INTEGER NOT NULL DEFAULT 10,daily_cap INTEGER NOT NULL DEFAULT 20,
 nonce TEXT NOT NULL,run_id TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'off',reason TEXT NOT NULL DEFAULT '',
 generation TEXT NOT NULL DEFAULT '',upstream_id INTEGER NOT NULL DEFAULT 0,
 filters_json TEXT NOT NULL DEFAULT '{}',filter_hash TEXT NOT NULL DEFAULT '',
 page INTEGER NOT NULL DEFAULT 1,sent INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS autolike_candidates(
 owner_id INTEGER NOT NULL REFERENCES autolike_settings(owner_id) ON DELETE CASCADE,
 run_id TEXT NOT NULL,profile_id INTEGER NOT NULL,source TEXT NOT NULL,filter_hash TEXT NOT NULL,
 generation TEXT NOT NULL,page INTEGER NOT NULL,fetched_at INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
 PRIMARY KEY(owner_id,run_id,profile_id)
);
CREATE TABLE IF NOT EXISTS autolike_pages(
 owner_id INTEGER NOT NULL REFERENCES autolike_settings(owner_id) ON DELETE CASCADE,
 run_id TEXT NOT NULL,filter_hash TEXT NOT NULL,fingerprint TEXT NOT NULL,
 PRIMARY KEY(owner_id,run_id,filter_hash,fingerprint)
);`

const autoColumns = `owner_id,mode,interval_seconds,series_count,run_limit,daily_cap,nonce,run_id,state,reason,generation,upstream_id,filters_json,filter_hash,page,sent,next_attempt`

type AutoSettings struct {
	Owner                                   int64
	Mode                                    string
	Interval, Count, Limit, DailyCap        int
	Nonce, RunID, State, Reason, Generation string
	UserID                                  int64
	Filters                                 FeedFilters
	FilterHash                              string
	Page, Sent                              int
	NextAttempt                             int64
}

type Autoliker struct {
	Service *Service
	mu      sync.Mutex
	now     func() time.Time
}

func NewAutoliker(s *Service) *Autoliker { return &Autoliker{Service: s, now: time.Now} }
func (a *Autoliker) init(ctx context.Context) error {
	_, err := a.Service.Store.DB.ExecContext(ctx, autoSchema)
	return err
}
func scanAuto(row interface{ Scan(...any) error }) (*AutoSettings, error) {
	var j AutoSettings
	var raw string
	err := row.Scan(&j.Owner, &j.Mode, &j.Interval, &j.Count, &j.Limit, &j.DailyCap, &j.Nonce, &j.RunID, &j.State, &j.Reason, &j.Generation, &j.UserID, &raw, &j.FilterHash, &j.Page, &j.Sent, &j.NextAttempt)
	if err == nil {
		err = json.Unmarshal([]byte(raw), &j.Filters)
	}
	return &j, err
}
func (a *Autoliker) settings(ctx context.Context, owner int64) (*AutoSettings, error) {
	if err := a.init(ctx); err != nil {
		return nil, err
	}
	nonce, err := filterNonce()
	if err != nil {
		return nil, err
	}
	if _, err = a.Service.Store.DB.ExecContext(ctx, "INSERT OR IGNORE INTO autolike_settings(owner_id,nonce) VALUES(?,?)", owner, nonce); err != nil {
		return nil, err
	}
	return scanAuto(a.Service.Store.DB.QueryRowContext(ctx, "SELECT "+autoColumns+" FROM autolike_settings WHERE owner_id=?", owner))
}
func (a *Autoliker) filters(ctx context.Context, owner int64) (FeedFilters, error) {
	state, err := a.Service.Store.filterState(ctx, owner)
	if err != nil {
		return FeedFilters{}, err
	}
	if state.Applied != nil {
		return *state.Applied, state.Applied.validate()
	}
	session, err := a.Service.Store.Session(ctx, owner)
	if err != nil {
		return FeedFilters{}, err
	}
	var account Profile
	if len(session.Account) > 0 {
		if err = json.Unmarshal(session.Account, &account); err != nil {
			return FeedFilters{}, err
		}
	}
	f := existingFeedFilters(account)
	return f, f.validate()
}
func autoFilterHash(f FeedFilters) string { b, _ := json.Marshal(f); return digest(b) }

// Start is an explicit owner action. UI handlers already hold userLock and use startLocked.
func (a *Autoliker) Start(ctx context.Context, owner int64, mode string, count int) error {
	mu := a.Service.userLock(owner)
	mu.Lock()
	defer mu.Unlock()
	return a.startLocked(ctx, owner, mode, count, "")
}
func (a *Autoliker) startLocked(ctx context.Context, owner int64, mode string, count int, nonce string) error {
	if mode != "series" && mode != "automatic" {
		return errors.New("неверный режим")
	}
	if count < 1 || count > 50 {
		return errors.New("размер серии: от 1 до 50")
	}
	s := a.Service
	if !s.Config.AllowReactions || !s.Store.cfg.AllowReactions {
		return errors.New("лайки сейчас выключены")
	}
	j, err := a.settings(ctx, owner)
	if err != nil {
		return err
	}
	if nonce != "" && nonce != j.Nonce {
		return errors.New("меню устарело; открой /settings")
	}
	if j.Mode != "off" {
		return errors.New("автолайкер уже включён; сначала выключи его")
	}
	if s.Store.Paused(ctx, owner) {
		return errors.New("сначала /resume, затем включи режим в /settings")
	}
	session, err := s.Store.Session(ctx, owner)
	if err != nil || session.State != "connected" {
		return errors.New("нужно подключение Jardulik")
	}
	f, err := a.filters(ctx, owner)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(f)
	run, err := filterNonce()
	if err != nil {
		return err
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE autolike_settings SET mode=?,series_count=?,run_limit=?,nonce=?,run_id=?,state='active',reason='',generation=?,upstream_id=?,filters_json=?,filter_hash=?,page=1,sent=0,next_attempt=? WHERE owner_id=? AND nonce=?`, mode, count, count, run, run, session.Generation, session.UserID, string(raw), autoFilterHash(f), a.now().UnixMilli(), owner, j.Nonce)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("меню устарело; открой /settings")
	}
	for _, table := range []string{"autolike_candidates", "autolike_pages"} {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE owner_id=?", owner); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DisableAutolike is DB-only and deliberately takes no userLock, so /pause can interrupt HTTP preflight.
func (s *Service) DisableAutolike(ctx context.Context, owner int64, reason string) error {
	a := NewAutoliker(s)
	if err := a.init(ctx); err != nil {
		return err
	}
	nonce, err := filterNonce()
	if err != nil {
		return err
	}
	_, err = s.Store.DB.ExecContext(ctx, "UPDATE autolike_settings SET mode='off',state='off',reason=?,nonce=? WHERE owner_id=?", reason, nonce, owner)
	return err
}
func (s *Service) AutolikeStatus(ctx context.Context, owner int64) string {
	j, err := NewAutoliker(s).settings(ctx, owner)
	if err != nil {
		return "Автолайкер: состояние недоступно."
	}
	text := fmt.Sprintf("Автолайкер: %s; отправлено %d; интервал %d с; мой лимит %d /24ч.", map[string]string{"off": "выключен", "series": "серия", "automatic": "авто"}[j.Mode], j.Sent, j.Interval, j.DailyCap)
	if j.NextAttempt > time.Now().UnixMilli() && j.Mode != "off" {
		text += " Следующая попытка: " + displayTime(time.UnixMilli(j.NextAttempt).UTC().Format(time.RFC3339)) + "."
	}
	if j.Reason != "" {
		text += " " + autoReason(j.Reason)
	}
	return text
}
func autoReason(reason string) string {
	labels := map[string]string{"paused": "Пауза.", "stopped": "Остановлен.", "user_off": "Выключен вручную.", "series_done": "Серия завершена.", "quota": "Ожидание срока сервиса.", "own_cap": "Достигнут мой лимит /24ч.", "feed_empty": "Выдача закончилась.", "uncertain": "Результат неизвестен; повтор выключен.", "disconnected": "Нужно переподключение.", "reaction_failed": "Отправка остановлена.", "source_failed": "Источник недоступен.", "disk": "Недостаточно свободного места."}
	more := map[string]string{"manual_off": "Выключен вручную.", "disabled": "Лайки сейчас выключены.", "filters_unavailable": "Не удалось прочитать фильтры.", "settings_changed": "Настройки или сессия изменились; включи режим заново.", "page_cap": "Достигнут предел страниц.", "invalid_profiles": "Формат выдачи изменился.", "archive_failed": "Не удалось сохранить выдачу.", "repeated_page": "Источник повторяет страницу.", "invalid_settings": "Настройки недоступны; открой /settings."}
	if text := labels[reason]; text != "" {
		return text
	}
	if text := more[reason]; text != "" {
		return text
	}
	return "Остановлен."
}
func (a *Autoliker) Run(ctx context.Context) {
	lock, err := os.OpenFile(filepath.Join(a.Service.Config.DataDir, "autolike.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	for ctx.Err() == nil {
		if _, err = a.Step(ctx); err != nil {
			return
		}
		if !sleep(ctx, time.Second) {
			return
		}
	}
}

var errAutoChanged = errors.New("автолайкер или фильтры изменились")

func (a *Autoliker) guard(ctx context.Context, j *AutoSettings) error {
	s := a.Service
	if !s.Config.AllowReactions || !s.Store.cfg.AllowReactions || s.Store.Paused(ctx, j.Owner) {
		return errAutoChanged
	}
	current, err := scanAuto(s.Store.DB.QueryRowContext(ctx, "SELECT "+autoColumns+" FROM autolike_settings WHERE owner_id=?", j.Owner))
	if err != nil {
		return err
	}
	if current.Mode == "off" || current.RunID != j.RunID {
		return errAutoChanged
	}
	session, err := s.Store.Session(ctx, j.Owner)
	if err != nil || session.State != "connected" || session.Generation != j.Generation || session.UserID != j.UserID {
		return errAutoChanged
	}
	f, err := a.filters(ctx, j.Owner)
	if err != nil {
		return err
	}
	if autoFilterHash(f) != j.FilterHash {
		return errAutoChanged
	}
	return s.ManualLikeBudgetGuard(ctx, j.Owner)
}

var ErrOwnLikeLimit = errors.New("достигнут мой лимит бота /24ч")

type OwnLikeLimitError struct {
	Limit   int
	RetryAt time.Time
}

func (e *OwnLikeLimitError) Error() string {
	return "Достигнут мой лимит бота /24ч. Настройки: /settings."
}
func (e *OwnLikeLimitError) Unwrap() error { return ErrOwnLikeLimit }

// ManualLikeBudgetGuard is shared by explicit manual and automatic reactions.
// It is DB-only, takes no userLock and imposes no auto mode or filter condition.
func (s *Service) ManualLikeBudgetGuard(ctx context.Context, owner int64) error {
	a := NewAutoliker(s)
	if s.Client != nil && s.Client.Now != nil {
		a.now = s.Client.Now
	}
	j, err := a.settings(ctx, owner)
	if err != nil {
		return err
	}
	count, next, err := a.budget(ctx, owner)
	if err != nil {
		return err
	}
	// The post-claim context excludes only the committed reservation supplied by Client.
	if count >= j.DailyCap {
		return &OwnLikeLimitError{Limit: j.DailyCap, RetryAt: next}
	}
	return nil
}

func (a *Autoliker) budget(ctx context.Context, owner int64) (int, time.Time, error) {
	var count int
	var oldest sql.NullInt64
	err := a.Service.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*),MIN(CAST(strftime('%s',created_at) AS INTEGER)) FROM actions WHERE owner_id=? AND kind='Like' AND state IN ('pending','processing','succeeded','unknown') AND datetime(created_at)>datetime(?,'-24 hours') AND id<>?`, owner, a.now().UTC().Format("2006-01-02 15:04:05"), ClaimedLikeActionID(ctx)).Scan(&count, &oldest)
	return count, time.Unix(oldest.Int64, 0).Add(24*time.Hour + time.Second), err
}
func (a *Autoliker) wait(ctx context.Context, j *AutoSettings, reason string, next time.Time) error {
	_, err := a.Service.Store.DB.ExecContext(ctx, "UPDATE autolike_settings SET state='waiting',reason=?,next_attempt=? WHERE owner_id=? AND run_id=? AND mode<>'off'", reason, next.UnixMilli(), j.Owner, j.RunID)
	return err
}
func (a *Autoliker) candidateState(ctx context.Context, j *AutoSettings, id int64, state string) error {
	_, err := a.Service.Store.DB.ExecContext(ctx, "UPDATE autolike_candidates SET state=? WHERE owner_id=? AND run_id=? AND profile_id=?", state, j.Owner, j.RunID, id)
	return err
}
func (a *Autoliker) resetFeed(ctx context.Context, j *AutoSettings, f FeedFilters) error {
	tx, err := a.Service.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM autolike_candidates WHERE owner_id=? AND source='feed' AND state='pending'", j.Owner); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM autolike_pages WHERE owner_id=?", j.Owner); err != nil {
		return err
	}
	raw, _ := json.Marshal(f)
	if _, err = tx.ExecContext(ctx, "UPDATE autolike_settings SET filters_json=?,filter_hash=?,page=1 WHERE owner_id=? AND run_id=?", string(raw), autoFilterHash(f), j.Owner, j.RunID); err != nil {
		return err
	}
	j.Filters, j.FilterHash, j.Page = f, autoFilterHash(f), 1
	return tx.Commit()
}

type autoCandidate struct {
	id      int64
	source  string
	fetched int64
}

func (a *Autoliker) choose(ctx context.Context, j *AutoSettings) (autoCandidate, error) {
	s := a.Service
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT f.profile_id FROM favorites f WHERE f.owner_id=? AND NOT EXISTS(SELECT 1 FROM autolike_candidates c WHERE c.owner_id=f.owner_id AND c.run_id=? AND c.profile_id=f.profile_id AND c.state<>'pending') ORDER BY f.created_at,f.profile_id`, j.Owner, j.RunID)
	if err != nil {
		return autoCandidate{}, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return autoCandidate{}, err
	}
	for _, id := range ids {
		ok, e := s.Store.CanAttemptLike(ctx, j.Owner, id, a.now())
		if !ok {
			var blocked *ActionBlockedError
			if errors.As(e, &blocked) && blocked.State == "quota_waiting" {
				return autoCandidate{}, e
			}
			if e != nil && !errors.Is(e, ErrActionBlocked) {
				return autoCandidate{}, e
			}
			// Retain the favorite; only this run's candidate is skipped.
			if _, err = s.Store.DB.ExecContext(ctx, `INSERT INTO autolike_candidates(owner_id,run_id,profile_id,source,filter_hash,generation,page,fetched_at,state) VALUES(?,?,?,'favorite','',?,0,?,'skipped') ON CONFLICT(owner_id,run_id,profile_id) DO UPDATE SET state='skipped'`, j.Owner, j.RunID, id, j.Generation, a.now().UnixMilli()); err != nil {
				return autoCandidate{}, err
			}
			continue
		}
		_, err = s.Store.DB.ExecContext(ctx, `INSERT INTO autolike_candidates(owner_id,run_id,profile_id,source,filter_hash,generation,page,fetched_at) VALUES(?,?,?,'favorite','',?,0,?) ON CONFLICT(owner_id,run_id,profile_id) DO UPDATE SET source='favorite'`, j.Owner, j.RunID, id, j.Generation, a.now().UnixMilli())
		return autoCandidate{id: id, source: "favorite"}, err
	}
	var c autoCandidate
	err = s.Store.DB.QueryRowContext(ctx, `SELECT profile_id,source,fetched_at FROM autolike_candidates WHERE owner_id=? AND run_id=? AND state='pending' AND source='feed' AND filter_hash=? AND generation=? ORDER BY page,profile_id LIMIT 1`, j.Owner, j.RunID, j.FilterHash, j.Generation).Scan(&c.id, &c.source, &c.fetched)
	if errors.Is(err, sql.ErrNoRows) {
		return autoCandidate{}, nil
	}
	return c, err
}

// Step performs at most one discovery page or one guarded reaction, never Store.Next.
func (a *Autoliker) Step(ctx context.Context) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := a.init(ctx); err != nil {
		return false, err
	}
	s := a.Service
	j, err := scanAuto(s.Store.DB.QueryRowContext(ctx, "SELECT "+autoColumns+" FROM autolike_settings WHERE mode<>'off' AND next_attempt<=? ORDER BY next_attempt,owner_id LIMIT 1", a.now().UnixMilli()))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	mu := s.userLock(j.Owner)
	mu.Lock()
	defer mu.Unlock()
	j, err = scanAuto(s.Store.DB.QueryRowContext(ctx, "SELECT "+autoColumns+" FROM autolike_settings WHERE owner_id=?", j.Owner))
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if j.Mode == "off" || j.NextAttempt > a.now().UnixMilli() {
		return false, nil
	}
	stop := func(reason string) (bool, error) {
		return true, s.DisableAutolike(context.WithoutCancel(ctx), j.Owner, reason)
	}
	if (j.Mode != "series" && j.Mode != "automatic") || (j.Interval != 10 && j.Interval != 30 && j.Interval != 60) || j.Count < 1 || j.Count > 50 || j.Limit < 1 || j.Limit > 50 || j.DailyCap < 1 || j.DailyCap > 500 {
		return stop("invalid_settings")
	}
	if !s.Config.AllowReactions || !s.Store.cfg.AllowReactions {
		return stop("disabled")
	}
	if s.Store.Paused(ctx, j.Owner) {
		return stop("paused")
	}
	session, err := s.Store.Session(ctx, j.Owner)
	if err != nil || session.State != "connected" || session.Generation != j.Generation || session.UserID != j.UserID {
		return stop("disconnected")
	}
	var uncertain int
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM autolike_candidates WHERE owner_id=? AND run_id=? AND state='sending'", j.Owner, j.RunID).Scan(&uncertain); err != nil {
		return true, err
	}
	if uncertain > 0 {
		return stop("uncertain")
	}
	if err = s.Media.CheckDisk(); err != nil {
		return stop("disk")
	}
	f, err := a.filters(ctx, j.Owner)
	if err != nil {
		return stop("filters_unavailable")
	}
	if autoFilterHash(f) != j.FilterHash {
		if err = a.resetFeed(ctx, j, f); err != nil {
			return true, err
		}
	}
	if count, next, e := a.budget(ctx, j.Owner); e != nil {
		return true, e
	} else if count >= j.DailyCap {
		return true, a.wait(ctx, j, "own_cap", next)
	}
	c, err := a.choose(ctx, j)
	if err != nil {
		var blocked *ActionBlockedError
		if errors.As(err, &blocked) && blocked.State == "quota_waiting" && blocked.RetryAt.After(a.now()) {
			return true, a.wait(ctx, j, "quota", blocked.RetryAt.Add(time.Second))
		}
		return stop("reaction_failed")
	}
	if c.id == 0 {
		return a.loadPage(ctx, j, session)
	}
	if c.source == "feed" && a.now().Sub(time.UnixMilli(c.fetched)) > 15*time.Minute {
		if err = a.resetFeed(ctx, j, j.Filters); err != nil {
			return true, err
		}
		return true, nil
	}
	if ok, e := s.Store.CanAttemptLike(ctx, j.Owner, c.id, a.now()); !ok {
		var blocked *ActionBlockedError
		if errors.As(e, &blocked) && blocked.State == "quota_waiting" && blocked.RetryAt.After(a.now()) {
			return true, a.wait(ctx, j, "quota", blocked.RetryAt.Add(time.Second))
		}
		if e != nil && !errors.Is(e, ErrActionBlocked) {
			return stop("reaction_failed")
		}
		return true, a.candidateState(ctx, j, c.id, "skipped")
	}
	full, err := s.Client.FullProfile(ctx, j.Owner, c.id)
	if errors.Is(err, ErrProfileUnavailable) {
		return true, a.candidateState(ctx, j, c.id, "skipped")
	}
	if err != nil {
		return stop("source_failed")
	}
	// The selected age range and adult age apply to every automatic reaction, including favorites.
	age, validAge := autoInteger(full["age"])
	if !validAge || age < 18 || age < j.Filters.AgeMin || age > j.Filters.AgeMax || c.source != "favorite" && !profileMatchesAutoFilters(full, j.Filters) {
		return true, a.candidateState(ctx, j, c.id, "skipped")
	}
	if err = a.guard(ctx, j); err != nil {
		return stop("settings_changed")
	}
	sessionMu := s.Store.SessionMutex(j.Owner)
	sessionMu.Lock()
	err = a.guard(ctx, j)
	if err == nil {
		_, err = s.Store.ArchiveViewed(ctx, j.Owner, []Profile{full}, "GET /user/getUser (autolike)")
	}
	sessionMu.Unlock()
	if err != nil {
		return stop("source_failed")
	}
	if err = a.candidateState(ctx, j, c.id, "sending"); err != nil {
		return true, err
	}
	result, err := s.Client.LikeGuarded(ctx, j.Owner, c.id, func(ctx context.Context) error { return a.guard(ctx, j) })
	if err != nil {
		_ = a.candidateState(context.WithoutCancel(ctx), j, c.id, "blocked")
		return stop("reaction_failed")
	}
	if result.Success {
		// Candidate completion and series cursor commit together. A crash before
		// commit leaves 'sending', which disables rather than blindly resuming.
		tx, e := s.Store.DB.BeginTx(ctx, nil)
		if e != nil {
			return true, e
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(ctx, "UPDATE autolike_candidates SET state='done' WHERE owner_id=? AND run_id=? AND profile_id=?", j.Owner, j.RunID, c.id); e != nil {
			return true, e
		}
		_, e = tx.ExecContext(ctx, "UPDATE autolike_settings SET sent=sent+1,page=1,state=CASE WHEN mode='off' THEN state ELSE 'active' END,reason=CASE WHEN mode='off' THEN reason ELSE '' END,next_attempt=? WHERE owner_id=? AND run_id=?", a.now().Add(time.Duration(j.Interval)*time.Second).UnixMilli(), j.Owner, j.RunID)
		if e != nil {
			return true, e
		}
		if e = tx.Commit(); e != nil {
			return true, e
		}
		if j.Mode == "series" && j.Sent+1 >= j.Limit {
			return stop("series_done")
		}
		return true, nil
	}
	if result.Decision == ReactionQuota && result.RetryAt.After(a.now()) {
		if err = a.candidateState(ctx, j, c.id, "pending"); err != nil {
			return true, err
		}
		return true, a.wait(ctx, j, "quota", result.RetryAt.Add(time.Second))
	}
	if result.Decision == ReactionExhausted {
		if err = a.candidateState(ctx, j, c.id, "blocked"); err != nil {
			return true, err
		}
		if j.Mode == "series" {
			return stop("feed_empty")
		}
		if err = a.resetFeed(ctx, j, j.Filters); err != nil {
			return true, err
		}
		return true, a.wait(ctx, j, "feed_empty", a.now().Add(24*time.Hour))
	}
	_ = a.candidateState(ctx, j, c.id, "blocked")
	return stop("reaction_failed")
}
func (a *Autoliker) loadPage(ctx context.Context, j *AutoSettings, session Session) (bool, error) {
	s := a.Service
	stop := func(reason string) (bool, error) {
		return true, s.DisableAutolike(context.WithoutCancel(ctx), j.Owner, reason)
	}
	if j.Page > 1000 {
		return stop("page_cap")
	}
	profiles, err := s.Client.UsersFrozen(ctx, j.Owner, session, j.Filters, j.Page, false)
	if err != nil {
		return stop("source_failed")
	}
	if err = a.guard(ctx, j); err != nil {
		return stop("settings_changed")
	}
	sessionMu := s.Store.SessionMutex(j.Owner)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	if err = a.guard(ctx, j); err != nil {
		return stop("settings_changed")
	}
	if err = s.Media.CheckDisk(); err != nil {
		return stop("disk")
	}
	// Archive the whole response, including non-matches, before recording candidates/cursor.
	if _, err = s.Store.ArchiveViewed(ctx, j.Owner, profiles, "GET /user/getUsers (autolike)"); err != nil {
		return stop("archive_failed")
	}
	if len(profiles) == 0 {
		if j.Mode == "series" {
			return stop("feed_empty")
		}
		if err = a.resetFeed(ctx, j, j.Filters); err != nil {
			return true, err
		}
		return true, a.wait(ctx, j, "feed_empty", a.now().Add(24*time.Hour))
	}
	fingerprint, err := crawlFingerprint(profiles)
	if err != nil {
		return stop("invalid_profiles")
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return true, err
	}
	defer tx.Rollback()
	var seen bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM autolike_pages WHERE owner_id=? AND run_id=? AND filter_hash=? AND fingerprint=?)", j.Owner, j.RunID, j.FilterHash, fingerprint).Scan(&seen); err != nil {
		return true, err
	}
	if seen {
		tx.Rollback()
		return stop("repeated_page")
	}
	for _, p := range profiles {
		if p.ID() <= 0 || !profileMatchesAutoFilters(p, j.Filters) {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO autolike_candidates(owner_id,run_id,profile_id,source,filter_hash,generation,page,fetched_at) VALUES(?,?,?,'feed',?,?,?,?)`, j.Owner, j.RunID, p.ID(), j.FilterHash, j.Generation, j.Page, a.now().UnixMilli()); err != nil {
			return true, err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO autolike_pages(owner_id,run_id,filter_hash,fingerprint) VALUES(?,?,?,?)", j.Owner, j.RunID, j.FilterHash, fingerprint); err != nil {
		return true, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE autolike_settings SET page=page+1,state='active',reason='',next_attempt=? WHERE owner_id=? AND run_id=? AND mode<>'off'", a.now().Add(time.Duration(j.Interval)*time.Second).UnixMilli(), j.Owner, j.RunID); err != nil {
		return true, err
	}
	return true, tx.Commit()
}
