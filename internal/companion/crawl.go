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
 filters_json TEXT NOT NULL, include_others INTEGER NOT NULL, include_locations INTEGER NOT NULL DEFAULT 0, unfiltered INTEGER NOT NULL DEFAULT 0, mode INTEGER NOT NULL DEFAULT 0,
 page INTEGER NOT NULL DEFAULT 1, next_request INTEGER NOT NULL, state TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '', pages INTEGER NOT NULL DEFAULT 0, received INTEGER NOT NULL DEFAULT 0,
 catalog_pages INTEGER NOT NULL DEFAULT 0, catalog_received INTEGER NOT NULL DEFAULT 0, catalog_profiles INTEGER NOT NULL DEFAULT 0,
 hydrated INTEGER NOT NULL DEFAULT 0, unavailable INTEGER NOT NULL DEFAULT 0,
 started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS feed_crawl_pages (
 owner_id INTEGER NOT NULL REFERENCES feed_crawls(owner_id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, mode INTEGER NOT NULL, fingerprint TEXT NOT NULL, page INTEGER NOT NULL,
 PRIMARY KEY(owner_id,run_id,mode,fingerprint)
);
INSERT OR IGNORE INTO meta(key,value) VALUES('feed_crawl_next','0');`

type CrawlJob struct {
	Owner            int64       `json:"owner"`
	RunID            string      `json:"run_id"`
	UserID           int64       `json:"user_id"`
	Generation       string      `json:"-"`
	Filters          FeedFilters `json:"filters"`
	IncludeOthers    bool        `json:"include_others"`
	IncludeLocations bool        `json:"include_locations"`
	Unfiltered       bool        `json:"unfiltered"`
	Mode             int         `json:"all_mode"`
	Page             int         `json:"next_page"`
	NextRequest      int64       `json:"next_request"`
	State            string      `json:"state"`
	Reason           string      `json:"reason,omitempty"`
	Pages            int         `json:"pages_archived"`
	Received         int         `json:"records_received"`
	CatalogPages     int         `json:"catalog_pages"`
	CatalogReceived  int         `json:"catalog_records_received"`
	CatalogProfiles  int         `json:"catalog_unique_profiles"`
	Hydrated         int         `json:"profiles_hydrated"`
	Unavailable      int         `json:"profiles_unavailable"`
	StartedAt        int64       `json:"started_at"`
	UpdatedAt        int64       `json:"updated_at"`
}

func (j CrawlJob) active() bool { return j.State == "queued" || j.State == "running" }

const crawlColumns = "owner_id,run_id,upstream_id,generation,filters_json,include_others,include_locations,unfiltered,mode,page,next_request,state,reason,pages,received,catalog_pages,catalog_received,catalog_profiles,hydrated,unavailable,started_at,updated_at"

func scanCrawl(row interface{ Scan(...any) error }) (*CrawlJob, error) {
	var j CrawlJob
	var raw string
	err := row.Scan(&j.Owner, &j.RunID, &j.UserID, &j.Generation, &raw, &j.IncludeOthers, &j.IncludeLocations, &j.Unfiltered, &j.Mode, &j.Page, &j.NextRequest, &j.State, &j.Reason, &j.Pages, &j.Received, &j.CatalogPages, &j.CatalogReceived, &j.CatalogProfiles, &j.Hydrated, &j.Unavailable, &j.StartedAt, &j.UpdatedAt)
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
	if err = s.migrateCrawl(ctx); err != nil {
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

type FeedCrawler struct {
	Service *Service
	mu      sync.Mutex
	now     func() time.Time
}

func NewFeedCrawler(s *Service) *FeedCrawler {
	return &FeedCrawler{Service: s, now: time.Now}
}

func (s *Store) migrateCrawl(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Serialize schema inspection with other processes before taking a WAL read snapshot.
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='feed_crawl_next'"); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info(feed_crawls)")
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		columns[name] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range []string{"include_locations", "unfiltered", "catalog_pages", "catalog_received", "catalog_profiles", "hydrated", "unavailable"} {
		if !columns[name] {
			if _, err = tx.ExecContext(ctx, "ALTER TABLE feed_crawls ADD COLUMN "+name+" INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (c *FeedCrawler) init(ctx context.Context) error {
	_, err := c.Service.Store.DB.ExecContext(ctx, filterSchema+crawlSchema+locationSchema+crawlPresenceSchema)
	if err != nil {
		return err
	}
	return c.Service.Store.migrateCrawl(ctx)
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
func (c *FeedCrawler) Request(ctx context.Context, owner int64, includeOthers bool, includeLocations ...bool) (*CrawlJob, error) {
	if len(includeLocations) > 1 {
		return nil, errors.New("invalid crawl options")
	}
	mu := c.Service.userLock(owner)
	mu.Lock()
	defer mu.Unlock()
	return c.requestLocked(ctx, owner, includeOthers, includeLocations...)
}

type crawlLifecycleError struct{ state string }

func (e *crawlLifecycleError) Error() string {
	switch e.state {
	case "paused":
		return "аккаунт на паузе; сначала /resume, затем новый запрос обхода"
	case "active":
		return "обход уже выполняется; смотри crawl-status"
	default:
		return "для обхода нужна подключённая сессия Jardulik"
	}
}

// Caller holds userLock to serialize explicit requests and daily scheduling.
func (c *FeedCrawler) requestLocked(ctx context.Context, owner int64, includeOthers bool, includeLocations ...bool) (*CrawlJob, error) {
	locations := len(includeLocations) == 1 && includeLocations[0]
	s := c.Service
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
		return nil, &crawlLifecycleError{state: "paused"}
	}
	expectedDaily, daily := ctx.Value(dailyCrawlRequestKey{}).(dailyCrawlIdentity)
	if daily {
		var blocked bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1')", dailyCrawlBlockKey(owner)).Scan(&blocked); err != nil {
			return nil, err
		}
		if blocked {
			return nil, &crawlLifecycleError{state: "paused"}
		}
	}
	session, err := crawlSession(ctx, tx, s.Store, owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) || session.State != "connected" || session.Generation == "" {
		return nil, &crawlLifecycleError{state: "session_unavailable"}
	}
	if daily && (session.UserID != expectedDaily.UserID || session.Generation != expectedDaily.Generation) {
		return nil, &crawlLifecycleError{state: "session_changed"}
	}
	var oldState string
	err = tx.QueryRowContext(ctx, "SELECT state FROM feed_crawls WHERE owner_id=?", owner).Scan(&oldState)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if oldState == "queued" || oldState == "running" {
		return nil, &crawlLifecycleError{state: "active"}
	}
	now := c.now().UnixMilli()
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='feed_crawl_next'").Scan(&next); err != nil {
		return nil, err
	}
	if next < now {
		next = now
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM feed_crawl_pages WHERE owner_id=?", owner); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM meta WHERE key=?", crawlRetryKey(owner)); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO feed_crawls(owner_id,run_id,upstream_id,generation,filters_json,include_others,include_locations,unfiltered,next_request,state,started_at,updated_at)
	 VALUES(?,?,?,?,?,?,?,1,?,'queued',?,?)
	 ON CONFLICT(owner_id) DO UPDATE SET run_id=excluded.run_id,upstream_id=excluded.upstream_id,generation=excluded.generation,
	 filters_json=excluded.filters_json,include_others=excluded.include_others,include_locations=excluded.include_locations,unfiltered=excluded.unfiltered,mode=0,page=1,next_request=excluded.next_request,
	 state='queued',reason='',pages=0,received=0,catalog_pages=0,catalog_received=0,catalog_profiles=0,hydrated=0,unavailable=0,started_at=excluded.started_at,updated_at=excluded.updated_at`,
		owner, run, session.UserID, session.Generation, string(raw), includeOthers, locations, next, now, now)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM meta WHERE key=?", dailyCrawlBlockKey(owner)); err != nil {
		return nil, err
	}
	if err = initCrawlPresenceTx(ctx, tx, owner, run); err != nil {
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
		if _, err := c.ScheduleDaily(ctx); err != nil {
			if ctx.Err() == nil {
				slog.Warn("daily crawl scheduler stopped", "kind", safeError(err))
			}
			return
		}
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
	if errors.Is(err, ErrAccountFrozen) {
		return "account_frozen"
	}
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

var errCrawlRetryObsolete = errors.New("crawl retry superseded")

type crawlRetry struct {
	RunID    string `json:"run_id"`
	Mode     int    `json:"mode"`
	Page     int    `json:"page"`
	Target   int64  `json:"target"`
	Failures int    `json:"failures"`
}

func crawlRetryKey(owner int64) string { return "feed_crawl_retry:" + strconv.FormatInt(owner, 10) }

func crawlRetryableGET(err error) (*UpstreamError, bool) {
	var upstream *UpstreamError
	if !errors.As(err, &upstream) || upstream.Kind != "request_rejected" {
		return nil, false
	}
	switch upstream.Status {
	case 500, 502, 503, 504:
	default:
		return nil, false
	}
	switch upstream.Endpoint {
	case "/user/getMe", "/user/getUsers", "/user/getUsersLocations", "/user/getUser":
		return upstream, true
	default:
		return nil, false
	}
}

// Only confirmed transient GET failures count; cancellation retains the normal resume path.
func (c *FeedCrawler) retryGET(ctx context.Context, j *CrawlJob, target int64, upstream *UpstreamError) error {
	tx, err := c.Service.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='feed_crawl_next'"); err != nil {
		return err
	}
	var run, state string
	var mode, page int
	var paused bool
	if err = tx.QueryRowContext(ctx, `SELECT j.run_id,j.state,j.mode,j.page,u.paused FROM feed_crawls j JOIN users u ON u.id=j.owner_id WHERE j.owner_id=?`, j.Owner).Scan(&run, &state, &mode, &page, &paused); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errCrawlRetryObsolete
		}
		return err
	}
	if run != j.RunID || (state != "queued" && state != "running") || mode != j.Mode || page != j.Page {
		return errCrawlRetryObsolete
	}
	session, err := crawlSession(ctx, tx, c.Service.Store, j.Owner)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errCrawlRetryObsolete
		}
		return err
	}
	if paused || session.State != "connected" || session.Generation != j.Generation || session.UserID != j.UserID {
		return errCrawlRetryObsolete
	}
	retry := crawlRetry{RunID: j.RunID, Mode: j.Mode, Page: j.Page, Target: target}
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", crawlRetryKey(j.Owner)).Scan(&raw)
	if err == nil {
		var previous crawlRetry
		if json.Unmarshal([]byte(raw), &previous) != nil || previous.Failures < 1 || previous.Failures > 3 {
			return errors.New("invalid crawl retry checkpoint")
		}
		if previous.RunID == retry.RunID && previous.Mode == retry.Mode && previous.Page == retry.Page && previous.Target == retry.Target {
			retry.Failures = previous.Failures
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	retry.Failures++
	if retry.Failures > 3 {
		retry.Failures = 3
	}
	encoded, err := json.Marshal(retry)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", crawlRetryKey(j.Owner), string(encoded)); err != nil {
		return err
	}
	if retry.Failures == 3 {
		_, err = tx.ExecContext(ctx, "UPDATE feed_crawls SET state='stopped',reason=?,updated_at=? WHERE owner_id=? AND run_id=?", crawlErrorReason(upstream), c.now().UnixMilli(), j.Owner, j.RunID)
	} else {
		delay := time.Minute
		if retry.Failures == 2 {
			delay = 5 * time.Minute
		}
		if upstream.RetryAfter > delay {
			delay = upstream.RetryAfter
		}
		_, err = tx.ExecContext(ctx, "UPDATE feed_crawls SET state='queued',reason=?,next_request=MAX(next_request,?),updated_at=? WHERE owner_id=? AND run_id=?", fmt.Sprintf("retry_http_%d", upstream.Status), c.now().Add(delay).UnixMilli(), c.now().UnixMilli(), j.Owner, j.RunID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Step handles at most one source page or catalog-proven full profile across all owners.
func (c *FeedCrawler) Step(ctx context.Context) (bool, error) {
	if c.Service.foregroundPending() {
		return false, nil
	}
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
	if !ownerMu.TryLock() {
		return false, nil
	}
	defer ownerMu.Unlock()
	if s.foregroundPending() {
		return false, nil
	}
	if reason, e := c.invalid(ctx, j); e != nil {
		return true, e
	} else if reason != "" {
		return true, c.stop(ctx, j, reason)
	}
	if j.Mode < 0 || j.Mode > 3 || (j.Mode >= 2 && !j.IncludeLocations) {
		return true, c.stop(ctx, j, "invalid_phase")
	}
	if j.Mode != 3 && j.Page > crawlPageCap {
		return true, c.stop(ctx, j, "page_cap")
	}
	if err := s.Media.CheckDisk(); err != nil {
		return true, c.stop(ctx, j, "disk_reserve")
	}
	var target int64
	if j.Mode == 3 {
		target, err = s.Store.nextCrawlLocation(ctx, j)
		if errors.Is(err, sql.ErrNoRows) {
			sessionMu := s.Client.lock(j.Owner)
			sessionMu.Lock()
			defer sessionMu.Unlock()
			if reason, guardErr := c.invalid(ctx, j); guardErr != nil {
				return true, guardErr
			} else if reason != "" {
				return true, c.stop(ctx, j, reason)
			}
			if err = c.checkpointHydration(ctx, j, 0, nil, false); err != nil {
				return true, c.stop(ctx, j, "checkpoint_failed")
			}
			return true, nil
		}
		if err != nil {
			return true, err
		}
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
	var profiles []Profile
	var locations LocationPage
	var requestErr error
	expected := Session{UserID: j.UserID, Generation: j.Generation}
	requestCtx := withCrawlPriority(ctx, s)
	switch j.Mode {
	case 0, 1:
		if j.Unfiltered {
			profiles, requestErr = s.Client.UsersUnfilteredFrozen(requestCtx, j.Owner, expected, j.Page, j.Mode == 1)
		} else {
			profiles, requestErr = s.Client.UsersFrozen(requestCtx, j.Owner, expected, j.Filters, j.Page, j.Mode == 1)
		}
	case 2:
		locations, requestErr = s.Client.LocationsFrozen(requestCtx, j.Owner, expected, "", "", j.Page)
	case 3:
		var profile Profile
		profile, requestErr = s.Client.FullProfileFrozen(requestCtx, j.Owner, expected, target)
		if requestErr == nil {
			profiles = []Profile{profile}
		}
	}
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
	if errors.Is(requestErr, errCrawlYield) {
		return true, nil
	}
	if requestErr != nil && !(j.Mode == 3 && errors.Is(requestErr, ErrProfileUnavailable)) {
		if upstream, retryable := crawlRetryableGET(requestErr); retryable {
			if retryErr := c.retryGET(ctx, j, target, upstream); retryErr != nil {
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				if errors.Is(retryErr, errCrawlRetryObsolete) {
					return true, nil
				}
				return true, errors.Join(retryErr, c.stop(ctx, j, "checkpoint_failed"))
			}
			return true, nil
		}
		return true, c.stop(ctx, j, crawlErrorReason(requestErr))
	}
	if err := s.Media.CheckDisk(); err != nil {
		return true, c.stop(ctx, j, "disk_reserve")
	}
	if j.Mode == 2 {
		if err = s.Store.saveCrawlLocations(ctx, j, locations, c.now()); err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, c.stop(ctx, j, "archive_failed")
		}
		if err = c.checkpointLocations(ctx, j, locations); err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, c.stop(ctx, j, "checkpoint_failed")
		}
		return true, nil
	}
	if j.Mode == 3 && errors.Is(requestErr, ErrProfileUnavailable) {
		if err = c.checkpointHydration(ctx, j, target, nil, true); err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, c.stop(ctx, j, "checkpoint_failed")
		}
		return true, nil
	}
	fingerprint, err := crawlFingerprint(profiles)
	if err != nil {
		return true, c.stop(ctx, j, "invalid_profiles")
	}
	source := "GET /user/getUsers (crawl)"
	if j.Mode == 3 {
		source = "GET /user/getUser (crawl catalog)"
	}
	if _, err = s.Store.ArchiveViewed(ctx, j.Owner, profiles, source); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, c.stop(ctx, j, "archive_failed")
	}
	if j.Mode == 3 {
		err = c.checkpointHydration(ctx, j, target, profiles, false)
	} else {
		err = c.checkpoint(ctx, j, fingerprint, len(profiles), profiles)
	}
	if err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, c.stop(ctx, j, "checkpoint_failed")
	}
	return true, nil
}

func (c *FeedCrawler) checkpointTx(ctx context.Context, j *CrawlJob, advance func(*sql.Tx, *CrawlJob) error) error {
	tx, err := c.Service.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, run string
	var mode, page int
	if err = tx.QueryRowContext(ctx, "SELECT run_id,state,mode,page FROM feed_crawls WHERE owner_id=?", j.Owner).Scan(&run, &state, &mode, &page); err != nil {
		return err
	}
	if run != j.RunID || (state != "queued" && state != "running") || mode != j.Mode || page != j.Page {
		return nil
	}
	next := *j
	next.State, next.Reason = "running", ""
	if err = advance(tx, &next); err != nil {
		return err
	}
	next.NextRequest, next.UpdatedAt = c.now().Add(crawlGap).UnixMilli(), c.now().UnixMilli()
	_, err = tx.ExecContext(ctx, `UPDATE feed_crawls SET state=?,reason=?,mode=?,page=?,next_request=?,pages=?,received=?,catalog_pages=?,catalog_received=?,catalog_profiles=?,hydrated=?,unavailable=?,updated_at=? WHERE owner_id=? AND run_id=?`,
		next.State, next.Reason, next.Mode, next.Page, next.NextRequest, next.Pages, next.Received, next.CatalogPages, next.CatalogReceived, next.CatalogProfiles, next.Hydrated, next.Unavailable, next.UpdatedAt, j.Owner, j.RunID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM meta WHERE key=?", crawlRetryKey(j.Owner)); err != nil {
		return err
	}
	if next.State == "done" {
		if err = finishCrawlPresenceTx(ctx, tx, &next, c.now()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func crawlPageSeenTx(ctx context.Context, tx *sql.Tx, j *CrawlJob, fingerprint string) (bool, error) {
	var seen bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawl_pages WHERE owner_id=? AND run_id=? AND mode=? AND fingerprint=?)", j.Owner, j.RunID, j.Mode, fingerprint).Scan(&seen)
	if err != nil || seen {
		return seen, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO feed_crawl_pages(owner_id,run_id,mode,fingerprint,page) VALUES(?,?,?,?,?)", j.Owner, j.RunID, j.Mode, fingerprint, j.Page)
	return false, err
}

func (c *FeedCrawler) checkpoint(ctx context.Context, j *CrawlJob, fingerprint string, count int, batches ...[]Profile) error {
	return c.checkpointTx(ctx, j, func(tx *sql.Tx, next *CrawlJob) error {
		if len(batches) > 0 {
			if err := recordCrawlSeenTx(ctx, tx, j, batches[0], c.now()); err != nil {
				return err
			}
		}
		if count == 0 {
			switch {
			case j.Mode == 0 && j.IncludeOthers:
				next.Mode, next.Page = 1, 1
			case j.IncludeLocations:
				next.Mode, next.Page = 2, 1
			default:
				next.State, next.Reason = "done", "empty_page"
			}
			return nil
		}
		next.Pages++
		next.Received += count
		seen, err := crawlPageSeenTx(ctx, tx, j, fingerprint)
		if err != nil {
			return err
		}
		if seen {
			next.State, next.Reason = "stopped", "repeated_page"
			return nil
		}
		next.Page++
		if next.Page > crawlPageCap {
			next.State, next.Reason = "stopped", "page_cap"
		}
		return nil
	})
}

func (c *FeedCrawler) checkpointLocations(ctx context.Context, j *CrawlJob, page LocationPage) error {
	return c.checkpointTx(ctx, j, func(tx *sql.Tx, next *CrawlJob) error {
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM crawl_location_results WHERE owner_id=? AND run_id=?", j.Owner, j.RunID).Scan(&next.CatalogProfiles); err != nil {
			return err
		}
		count := len(page.Entries)
		if count > 0 {
			next.CatalogPages++
			next.CatalogReceived += count
			seen, err := crawlPageSeenTx(ctx, tx, j, page.Fingerprint)
			if err != nil {
				return err
			}
			if seen {
				next.State, next.Reason = "stopped", "repeated_page"
				return nil
			}
		}
		if count == 0 || (int64(j.Page)*locationPageSize >= page.Total) {
			next.Mode, next.Page = 3, 1
		} else {
			next.Page++
			if next.Page > crawlPageCap {
				next.State, next.Reason = "stopped", "page_cap"
			}
		}
		return nil
	})
}

func (c *FeedCrawler) checkpointHydration(ctx context.Context, j *CrawlJob, target int64, profiles []Profile, unavailable bool) error {
	return c.checkpointTx(ctx, j, func(tx *sql.Tx, next *CrawlJob) error {
		if target != 0 {
			state, version := "unavailable", int64(0)
			if !unavailable {
				if len(profiles) != 1 || profiles[0].ID() != target {
					return errors.New("invalid hydrated profile")
				}
				if err := recordCrawlSeenTx(ctx, tx, j, profiles, c.now()); err != nil {
					return err
				}
				if err := tx.QueryRowContext(ctx, "SELECT version_id FROM queue WHERE owner_id=? AND profile_id=?", j.Owner, target).Scan(&version); err != nil {
					return err
				}
				state = "hydrated"
			}
			result, err := tx.ExecContext(ctx, "UPDATE crawl_location_results SET state=?,version_id=?,completed_at=? WHERE owner_id=? AND run_id=? AND profile_id=? AND state='pending'", state, version, c.now().UnixMilli(), j.Owner, j.RunID, target)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil {
				return err
			} else if n != 1 {
				return errors.New("hydration cursor changed")
			}
			if unavailable {
				known, err := presenceVersionTx(ctx, tx, j.Owner, target)
				if err != nil {
					return err
				}
				if known > 0 {
					if _, err = recordProfilePresenceTx(ctx, tx, j.Owner, target, false, "direct_unavailable", known, known, c.now()); err != nil {
						return err
					}
				}
				next.Unavailable++
			} else {
				next.Hydrated++
			}
			next.Page++
		}
		var pending bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM crawl_location_results WHERE owner_id=? AND run_id=? AND state='pending')", j.Owner, j.RunID).Scan(&pending); err != nil {
			return err
		}
		if target == 0 && pending {
			return errors.New("pending profile lacks source provenance")
		}
		if !pending {
			next.State, next.Reason = "done", "empty_page"
		}
		return nil
	})
}

func (j CrawlJob) shortStatus() string {
	state := map[string]string{"queued": "ожидает", "running": "идёт", "done": "завершён", "stopped": "остановлен"}[j.State]
	mode := "основная выдача"
	switch j.Mode {
	case 1:
		mode = "дополнительная выдача"
	case 2:
		mode = "каталог локаций"
	case 3:
		mode = "полные анкеты из каталога"
	}
	position := fmt.Sprintf("страница %d", j.Page)
	if j.Mode == 3 {
		position = fmt.Sprintf("обработано ID %d", j.Hydrated+j.Unavailable)
	}
	text := fmt.Sprintf("Обход доступной выдачи: %s; %s, %s; пачек выдачи %d, записей %d.", state, mode, position, j.Pages, j.Received)
	if j.IncludeLocations {
		text += fmt.Sprintf(" Каталог: страниц %d, записей %d, разных ID %d; полных анкет %d, недоступных %d.", j.CatalogPages, j.CatalogReceived, j.CatalogProfiles, j.Hydrated, j.Unavailable)
	}
	if j.State == "queued" && (j.Reason == "retry_http_500" || j.Reason == "retry_http_502" || j.Reason == "retry_http_503" || j.Reason == "retry_http_504") {
		return text + " Временная ошибка GET HTTP " + strings.TrimPrefix(j.Reason, "retry_http_") + "; повтор по нашему расписанию не раньше " + displayTime(time.UnixMilli(j.NextRequest).UTC().Format(time.RFC3339)) + "."
	}
	if j.Reason != "" && j.Reason != "empty_page" {
		reason := map[string]string{
			"paused": "пауза", "disconnected": "сессия отключена", "session_changed": "сессия изменилась",
			"session_unavailable": "сессия недоступна", "auth_required": "нужно повторное подключение", "account_frozen": "анкета заморожена в Jardulik",
			"filters_changed": "фильтры изменены", "premium_required": "Premium недоступен",
			"repeated_page": "сервис повторяет уже полученную страницу", "page_cap": "лимит 1000 страниц",
			"disk_reserve": "недостаточно свободного места", "challenge": "сервис запросил дополнительную проверку",
			"archive_failed": "не удалось сохранить пачку", "request_failed": "не удалось получить страницу",
			"checkpoint_failed": "не удалось сохранить прогресс обхода", "invalid_phase": "неизвестная фаза обхода",
		}[j.Reason]
		if reason == "" {
			reason = j.Reason
		}
		text += " Причина: " + reason + "."
	}
	return text
}
