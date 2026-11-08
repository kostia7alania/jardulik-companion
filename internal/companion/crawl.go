package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const crawlGap = 20 * time.Second
const crawlPageCap = 1000

const crawlSchema = `
CREATE TABLE IF NOT EXISTS feed_crawls (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 filters_json TEXT NOT NULL, include_others INTEGER NOT NULL, mode INTEGER NOT NULL DEFAULT 0,
 page INTEGER NOT NULL DEFAULT 1, next_request INTEGER NOT NULL, state TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '', pages INTEGER NOT NULL DEFAULT 0, received INTEGER NOT NULL DEFAULT 0,
 started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS feed_crawl_pages (
 owner_id INTEGER NOT NULL REFERENCES feed_crawls(owner_id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, mode INTEGER NOT NULL, fingerprint TEXT NOT NULL, page INTEGER NOT NULL,
 PRIMARY KEY(owner_id,run_id,mode,fingerprint)
);
INSERT OR IGNORE INTO meta(key,value) VALUES('feed_crawl_next','0');`

type CrawlJob struct {
	Owner         int64       `json:"owner"`
	RunID         string      `json:"run_id"`
	UserID        int64       `json:"user_id"`
	Generation    string      `json:"-"`
	Filters       FeedFilters `json:"filters"`
	IncludeOthers bool        `json:"include_others"`
	Mode          int         `json:"all_mode"`
	Page          int         `json:"next_page"`
	NextRequest   int64       `json:"next_request"`
	State         string      `json:"state"`
	Reason        string      `json:"reason,omitempty"`
	Pages         int         `json:"pages_archived"`
	Received      int         `json:"records_received"`
	StartedAt     int64       `json:"started_at"`
	UpdatedAt     int64       `json:"updated_at"`
}

func (j CrawlJob) active() bool { return j.State == "queued" || j.State == "running" }

const crawlColumns = "owner_id,run_id,upstream_id,generation,filters_json,include_others,mode,page,next_request,state,reason,pages,received,started_at,updated_at"

func scanCrawl(row interface{ Scan(...any) error }) (*CrawlJob, error) {
	var j CrawlJob
	var raw string
	err := row.Scan(&j.Owner, &j.RunID, &j.UserID, &j.Generation, &raw, &j.IncludeOthers, &j.Mode, &j.Page, &j.NextRequest, &j.State, &j.Reason, &j.Pages, &j.Received, &j.StartedAt, &j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal([]byte(raw), &j.Filters); err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) hasCrawlTable(ctx context.Context) (bool, error) {
	var exists bool
	err := s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_crawls')").Scan(&exists)
	return exists, err
}

// CrawlStatus reads local state only; it never calls Jardulik.
func (s *Store) CrawlStatus(ctx context.Context, owner int64) (*CrawlJob, error) {
	exists, err := s.hasCrawlTable(ctx)
	if err != nil || !exists {
		return nil, err
	}
	j, err := scanCrawl(s.DB.QueryRowContext(ctx, "SELECT "+crawlColumns+" FROM feed_crawls WHERE owner_id=?", owner))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

func (s *Store) StopCrawl(ctx context.Context, owner int64, reason string) error {
	exists, err := s.hasCrawlTable(ctx)
	if err != nil || !exists {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "UPDATE feed_crawls SET state='stopped',reason=?,updated_at=? WHERE owner_id=? AND state IN ('queued','running')", reason, time.Now().UnixMilli(), owner)
	return err
}

func stopCrawlTx(ctx context.Context, tx *sql.Tx, owner int64, reason string) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_crawls')").Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err := tx.ExecContext(ctx, "UPDATE feed_crawls SET state='stopped',reason=?,updated_at=? WHERE owner_id=? AND state IN ('queued','running')", reason, time.Now().UnixMilli(), owner)
	return err
}

type FeedCrawler struct {
	Service *Service
	mu      sync.Mutex
	now     func() time.Time
}

func NewFeedCrawler(s *Service) *FeedCrawler {
	return &FeedCrawler{Service: s, now: time.Now}
}

func (c *FeedCrawler) init(ctx context.Context) error {
	_, err := c.Service.Store.DB.ExecContext(ctx, filterSchema+crawlSchema)
	return err
}

func crawlSession(ctx context.Context, tx *sql.Tx, store *Store, owner int64) (Session, error) {
	var b []byte
	var state string
	var session Session
	err := tx.QueryRowContext(ctx, "SELECT encrypted,state FROM sessions WHERE owner_id=?", owner).Scan(&b, &state)
	if err != nil {
		return session, err
	}
	b, err = unseal(store.cfg.SessionKey, b, sessionPurpose(owner))
	if err != nil {
		return session, err
	}
	if err = json.Unmarshal(b, &session); err != nil {
		return session, err
	}
	session.State = state
	return session, nil
}

// Request is DB-only. Source entitlement is freshly verified by each worker page.
func (c *FeedCrawler) Request(ctx context.Context, owner int64, includeOthers bool) (*CrawlJob, error) {
	s := c.Service
	mu := s.userLock(owner)
	mu.Lock()
	defer mu.Unlock()
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	filters := DefaultFeedFilters()
	filters.AgeMin, filters.AgeMax = 0, 100
	if err := filters.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	run, err := filterNonce()
	if err != nil {
		return nil, err
	}
	s.cancelRun(owner)
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var paused bool
	if err = tx.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&paused); err != nil {
		return nil, err
	}
	if paused {
		return nil, errors.New("аккаунт на паузе; сначала /resume, затем новый запрос обхода")
	}
	session, err := crawlSession(ctx, tx, s.Store, owner)
	if err != nil || session.State != "connected" || session.Generation == "" {
		return nil, errors.New("для обхода нужна подключённая сессия Jardulik")
	}
	var oldState string
	err = tx.QueryRowContext(ctx, "SELECT state FROM feed_crawls WHERE owner_id=?", owner).Scan(&oldState)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if oldState == "queued" || oldState == "running" {
		return nil, errors.New("обход уже выполняется; смотри crawl-status")
	}
	now := c.now().UnixMilli()
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='feed_crawl_next'").Scan(&next); err != nil {
		return nil, err
	}
	if next < now {
		next = now
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO feed_filters(owner_id,applied_json,draft_json,include_others,others_id) VALUES(?,?,NULL,0,'')
	 ON CONFLICT(owner_id) DO UPDATE SET applied_json=excluded.applied_json,draft_json=NULL,include_others=0,others_id=''`, owner, string(raw))
	if err != nil {
		return nil, err
	}
	if err = resetFilterQueueTx(ctx, tx, owner); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM feed_crawl_pages WHERE owner_id=?", owner); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,next_request,state,started_at,updated_at)
	 VALUES(?,?,?,?,?,?,?,'queued',?,?)
	 ON CONFLICT(owner_id) DO UPDATE SET run_id=excluded.run_id,upstream_id=excluded.upstream_id,generation=excluded.generation,
	 filters_json=excluded.filters_json,include_others=excluded.include_others,mode=0,page=1,next_request=excluded.next_request,
	 state='queued',reason='',pages=0,received=0,started_at=excluded.started_at,updated_at=excluded.updated_at`,
		owner, run, session.UserID, session.Generation, string(raw), includeOthers, next, now, now)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return s.Store.CrawlStatus(ctx, owner)
}

func (c *FeedCrawler) Run(ctx context.Context) {
	lock, err := os.OpenFile(filepath.Join(c.Service.Config.DataDir, "feed-crawl.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	for ctx.Err() == nil {
		if _, err := c.Step(ctx); err != nil {
			if ctx.Err() == nil {
				slog.Warn("feed crawl worker stopped", "kind", safeError(err))
			}
			return
		}
		if !sleep(ctx, time.Second) {
			return
		}
	}
}

func (c *FeedCrawler) stop(ctx context.Context, j *CrawlJob, reason string) error {
	_, err := c.Service.Store.DB.ExecContext(ctx, "UPDATE feed_crawls SET state='stopped',reason=?,updated_at=? WHERE owner_id=? AND run_id=? AND state IN ('queued','running')", reason, c.now().UnixMilli(), j.Owner, j.RunID)
	return err
}

func (c *FeedCrawler) invalid(ctx context.Context, j *CrawlJob) (string, error) {
	current, err := c.Service.Store.CrawlStatus(ctx, j.Owner)
	if err != nil {
		return "", err
	}
	if current == nil || current.RunID != j.RunID || !current.active() {
		return "job_stopped", nil
	}
	session, err := c.Service.Store.Session(ctx, j.Owner)
	if err != nil {
		return "disconnected", nil
	}
	if session.State != "connected" {
		return "session_unavailable", nil
	}
	if session.UserID != j.UserID || session.Generation != j.Generation {
		return "session_changed", nil
	}
	if c.Service.Store.Paused(ctx, j.Owner) {
		return "paused", nil
	}
	filters, err := c.Service.Store.filterState(ctx, j.Owner)
	if err != nil {
		return "", err
	}
	if filters.Applied == nil || *filters.Applied != j.Filters || filters.Others {
		return "filters_changed", nil
	}
	return "", nil
}

func crawlFingerprint(profiles []Profile) (string, error) {
	seen := make(map[int64]bool, len(profiles))
	ids := make([]int64, 0, len(profiles))
	for _, p := range profiles {
		id := p.ID()
		if id <= 0 {
			return "", errors.New("profile without ID")
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var values []string
	for _, id := range ids {
		values = append(values, strconv.FormatInt(id, 10))
	}
	return digest([]byte(strings.Join(values, ","))), nil
}

func crawlErrorReason(err error) string {
	if errors.Is(err, ErrFiltersPremium) {
		return "premium_required"
	}
	var upstream *UpstreamError
	if errors.As(err, &upstream) {
		if upstream.Endpoint == "/appuser/refresh" || upstream.Status == 401 {
			return "auth_required"
		}
		if upstream.Kind == "unexpected_response_or_challenge" {
			return "challenge"
		}
		return fmt.Sprintf("http_%d", upstream.Status)
	}
	return "request_failed"
}

// Step handles at most one page across all owners, without reactions or ID scans.
func (c *FeedCrawler) Step(ctx context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := c.init(ctx); err != nil {
		return false, err
	}
	now := c.now().UnixMilli()
	s := c.Service
	var budget int64
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='feed_crawl_next'").Scan(&budget); err != nil {
		return false, err
	}
	if budget > now {
		return false, nil
	}
	j, err := scanCrawl(s.Store.DB.QueryRowContext(ctx, "SELECT "+crawlColumns+" FROM feed_crawls WHERE state IN ('queued','running') AND next_request<=? ORDER BY next_request,owner_id LIMIT 1", now))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ownerMu := s.userLock(j.Owner)
	ownerMu.Lock()
	defer ownerMu.Unlock()
	if reason, e := c.invalid(ctx, j); e != nil {
		return true, e
	} else if reason != "" {
		return true, c.stop(ctx, j, reason)
	}
	if j.Page > crawlPageCap {
		return true, c.stop(ctx, j, "page_cap")
	}
	if err := s.Media.CheckDisk(); err != nil {
		return true, c.stop(ctx, j, "disk_reserve")
	}
	// Reserve a durable gap before HTTP, then extend it from completion in defer.
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='feed_crawl_next'").Scan(&budget)
	if err != nil {
		return false, err
	}
	if budget > now {
		return false, nil
	}
	res, err := tx.ExecContext(ctx, "UPDATE feed_crawls SET state='running',next_request=?,updated_at=? WHERE owner_id=? AND run_id=? AND state IN ('queued','running') AND page=? AND mode=? AND next_request<=?", now+crawlGap.Milliseconds(), now, j.Owner, j.RunID, j.Page, j.Mode, now)
	if err != nil {
		return false, err
	}
	if n, e := res.RowsAffected(); e != nil || n != 1 {
		return false, e
	}
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key='feed_crawl_next'", now+crawlGap.Milliseconds()); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	defer func() {
		gapCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		next := c.now().Add(crawlGap).UnixMilli()
		_, _ = s.Store.DB.ExecContext(gapCtx, "UPDATE meta SET value=MAX(CAST(value AS INTEGER),?) WHERE key='feed_crawl_next'", next)
		_, _ = s.Store.DB.ExecContext(gapCtx, "UPDATE feed_crawls SET next_request=MAX(next_request,?) WHERE owner_id=? AND run_id=?", next, j.Owner, j.RunID)
	}()
	profiles, requestErr := s.Client.UsersFrozen(ctx, j.Owner, Session{UserID: j.UserID, Generation: j.Generation}, j.Filters, j.Page, j.Mode == 1)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	// No session mutex is held during HTTP. Guard the archived response afterwards.
	sessionMu := s.Client.lock(j.Owner)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	if reason, e := c.invalid(ctx, j); e != nil {
		return true, e
	} else if reason != "" {
		if reason == "session_unavailable" && requestErr != nil {
			reason = crawlErrorReason(requestErr)
		}
		return true, c.stop(ctx, j, reason)
	}
	if requestErr != nil {
		return true, c.stop(ctx, j, crawlErrorReason(requestErr))
	}
	if err := s.Media.CheckDisk(); err != nil {
		return true, c.stop(ctx, j, "disk_reserve")
	}
	fingerprint, err := crawlFingerprint(profiles)
	if err != nil {
		return true, c.stop(ctx, j, "invalid_profiles")
	}
	if _, err = s.Store.Archive(ctx, j.Owner, profiles, "GET /user/getUsers (crawl)"); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, c.stop(ctx, j, "archive_failed")
	}
	if err = s.Store.RestoreFilteredBatch(ctx, j.Owner, profiles); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, c.stop(ctx, j, "archive_failed")
	}
	if err = c.checkpoint(ctx, j, fingerprint, len(profiles)); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, c.stop(ctx, j, "checkpoint_failed")
	}
	return true, nil
}

func (c *FeedCrawler) checkpoint(ctx context.Context, j *CrawlJob, fingerprint string, count int) error {
	tx, err := c.Service.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, run string
	if err = tx.QueryRowContext(ctx, "SELECT run_id,state FROM feed_crawls WHERE owner_id=?", j.Owner).Scan(&run, &state); err != nil {
		return err
	}
	if run != j.RunID || (state != "queued" && state != "running") {
		return nil
	}
	nextPage, nextMode := j.Page, j.Mode
	reason, nextState := "", "running"
	pages, received := j.Pages, j.Received
	if count == 0 {
		if j.Mode == 0 && j.IncludeOthers {
			nextMode, nextPage = 1, 1
		} else {
			nextState, reason = "done", "empty_page"
		}
	} else {
		pages++
		received += count
		var seen bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawl_pages WHERE owner_id=? AND run_id=? AND mode=? AND fingerprint=?)", j.Owner, j.RunID, j.Mode, fingerprint).Scan(&seen); err != nil {
			return err
		}
		if seen {
			nextState, reason = "stopped", "repeated_page"
		} else {
			if _, err = tx.ExecContext(ctx, "INSERT INTO feed_crawl_pages(owner_id,run_id,mode,fingerprint,page) VALUES(?,?,?,?,?)", j.Owner, j.RunID, j.Mode, fingerprint, j.Page); err != nil {
				return err
			}
			nextPage++
			if nextPage > crawlPageCap {
				nextState, reason = "stopped", "page_cap"
			}
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE feed_crawls SET state=?,reason=?,mode=?,page=?,next_request=?,pages=?,received=?,updated_at=? WHERE owner_id=? AND run_id=?", nextState, reason, nextMode, nextPage, c.now().Add(crawlGap).UnixMilli(), pages, received, c.now().UnixMilli(), j.Owner, j.RunID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (j CrawlJob) shortStatus() string {
	state := map[string]string{"queued": "ожидает", "running": "идёт", "done": "завершён", "stopped": "остановлен"}[j.State]
	mode := "основная выдача"
	if j.Mode == 1 {
		mode = "дополнительная выдача"
	}
	text := fmt.Sprintf("Обход: %s; %s, страница %d; пачек %d, получено записей %d.", state, mode, j.Page, j.Pages, j.Received)
	if j.Reason != "" && j.Reason != "empty_page" {
		reason := map[string]string{
			"paused": "пауза", "disconnected": "сессия отключена", "session_changed": "сессия изменилась",
			"session_unavailable": "сессия недоступна", "auth_required": "нужно повторное подключение",
			"filters_changed": "фильтры изменены", "premium_required": "Premium недоступен",
			"repeated_page": "сервис повторяет уже полученную страницу", "page_cap": "лимит 1000 страниц",
			"disk_reserve": "недостаточно свободного места", "challenge": "сервис запросил дополнительную проверку",
			"archive_failed": "не удалось сохранить пачку", "request_failed": "не удалось получить страницу",
			"checkpoint_failed": "не удалось сохранить прогресс обхода",
		}[j.Reason]
		if reason == "" {
			reason = j.Reason
		}
		text += " Причина: " + reason + "."
	}
	return text
}
