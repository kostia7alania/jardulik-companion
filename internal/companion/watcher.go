package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const watchSchema = `
CREATE TABLE IF NOT EXISTS profile_watch (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, next_check INTEGER NOT NULL, last_checked INTEGER NOT NULL DEFAULT 0,
 last_result TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(owner_id,profile_id),
 FOREIGN KEY(owner_id) REFERENCES sessions(owner_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS profile_watch_budget (
 id INTEGER PRIMARY KEY CHECK(id=1), day TEXT NOT NULL DEFAULT '', used INTEGER NOT NULL DEFAULT 0,
 next_request INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO profile_watch_budget(id) VALUES(1);`

type ProfileWatcher struct {
	Config   Config
	Store    *Store
	Client   *Client
	Archiver *Archiver

	mu          sync.Mutex
	initialized bool
	now         func() time.Time
}

func NewProfileWatcher(c Config, s *Store, client *Client, archive ...*Archiver) *ProfileWatcher {
	if c.WatchInterval <= 0 {
		c.WatchInterval = 24 * time.Hour
	}
	if c.WatchGap <= 0 {
		c.WatchGap = 20 * time.Second
	}
	if c.WatchDailyLimit <= 0 {
		c.WatchDailyLimit = 300
	}
	w := &ProfileWatcher{Config: c, Store: s, Client: client, now: time.Now}
	if len(archive) > 0 {
		w.Archiver = archive[0]
	}
	return w
}

func (w *ProfileWatcher) init(ctx context.Context) error {
	if !w.initialized {
		if _, err := w.Store.DB.ExecContext(ctx, watchSchema+profilePresenceSchema); err != nil {
			return err
		}
		w.initialized = true
	}
	return nil
}

func (w *ProfileWatcher) Run(ctx context.Context) {
	lock, err := os.OpenFile(filepath.Join(w.Config.DataDir, "profile-watch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	w.mu.Lock()
	err = w.init(ctx)
	w.mu.Unlock()
	if err != nil || w.recoverProfilePresence(ctx) != nil {
		return
	}
	presenceCtx, cancelPresence := context.WithCancel(ctx)
	presenceDone := make(chan struct{})
	go func() {
		defer close(presenceDone)
		w.runProfilePresence(presenceCtx)
	}()
	defer func() { cancelPresence(); <-presenceDone }()
	for ctx.Err() == nil {
		if _, err = w.CheckOne(ctx); err != nil && ctx.Err() == nil {
			_ = w.Store.SetMeta(ctx, "watch_last_error", watchErrorCode(err))
		}
		timer := time.NewTimer(w.Config.WatchGap)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// CheckOne consumes at most one due check, including failed or disconnected responses.
// It never waits for a future slot and never sends a reaction.
func (w *ProfileWatcher) CheckOne(ctx context.Context) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := w.init(ctx); err != nil {
		return false, err
	}
	now := w.now()
	tx, err := w.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = seedProfilePresence(ctx, tx); err != nil {
		return false, err
	}
	interval := w.Config.WatchInterval.Milliseconds()
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO profile_watch(owner_id,profile_id,next_check)
	 SELECT q.owner_id,q.profile_id,COALESCE(unixepoch(v.last_seen)*1000,?)+?
	 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id
	 JOIN sessions s ON s.owner_id=q.owner_id JOIN users u ON u.id=q.owner_id
	 WHERE s.state='connected' AND u.paused=0`, now.UnixMilli(), interval)
	if err != nil {
		return false, err
	}
	var day string
	var used int
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT day,used,next_request FROM profile_watch_budget WHERE id=1").Scan(&day, &used, &next); err != nil {
		return false, err
	}
	today := now.In(time.FixedZone("MSK", 3*60*60)).Format("2006-01-02")
	if day != today {
		used = 0
	}
	if next > now.UnixMilli() || used >= w.Config.WatchDailyLimit {
		return false, tx.Commit()
	}
	var owner, profile, account int64
	err = tx.QueryRowContext(ctx, `WITH owner_turn AS (SELECT owner_id,MAX(last_checked) AS last_turn FROM profile_watch GROUP BY owner_id)
	 SELECT w.owner_id,w.profile_id,s.upstream_id
	 FROM profile_watch w JOIN sessions s ON s.owner_id=w.owner_id JOIN users u ON u.id=w.owner_id
	 JOIN owner_turn turn ON turn.owner_id=w.owner_id
	 JOIN queue q ON q.owner_id=w.owner_id AND q.profile_id=w.profile_id
	 JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id
	 WHERE s.state='connected' AND u.paused=0 AND w.next_check<=?
	 AND COALESCE(unixepoch(v.last_seen)*1000,?)+?<=?
	 ORDER BY turn.last_turn,w.next_check,w.owner_id,w.profile_id LIMIT 1`, now.UnixMilli(), now.UnixMilli(), interval, now.UnixMilli()).Scan(&owner, &profile, &account)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE profile_watch SET last_checked=?,next_check=?,last_result='checking' WHERE owner_id=? AND profile_id=?`, now.UnixMilli(), now.Add(w.Config.WatchInterval).UnixMilli(), owner, profile); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE profile_watch_budget SET day=?,used=?,next_request=? WHERE id=1", today, used+1, now.Add(w.Config.WatchGap).UnixMilli()); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}

	mu := w.Store.SessionMutex(owner)
	mu.Lock()
	session, err := w.Store.Session(ctx, owner)
	active := err == nil && session.State == "connected" && session.UserID == account && !w.Store.Paused(ctx, owner)
	mu.Unlock()
	if !active {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return true, err
		}
		return true, ctx.Err()
	}
	p, requestErr := w.Client.FullProfile(ctx, owner, profile)
	finished := w.now()
	if err = ctx.Err(); err != nil {
		return true, err
	}
	if _, err = w.Store.DB.ExecContext(ctx, "UPDATE profile_watch_budget SET next_request=MAX(next_request,?) WHERE id=1", finished.Add(w.Config.WatchGap).UnixMilli()); err != nil {
		return true, errors.Join(requestErr, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if err = ctx.Err(); err != nil {
		return true, err
	}
	// Disconnect cascades the claim away, even when the same account reconnects.
	var claim int64
	session, err = w.Store.Session(ctx, owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return true, errors.Join(requestErr, err)
	}
	if errors.Is(err, sql.ErrNoRows) || session.UserID != account {
		return true, nil
	}
	err = w.Store.DB.QueryRowContext(ctx, "SELECT last_checked FROM profile_watch WHERE owner_id=? AND profile_id=?", owner, profile).Scan(&claim)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && claim != now.UnixMilli()) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	result := "discarded"
	if session.State == "connected" && !w.Store.Paused(ctx, owner) && errors.Is(requestErr, ErrProfileUnavailable) {
		_, requestErr = w.recordProfilePresence(ctx, owner, profile, false, finished)
		result = "unavailable"
	} else if requestErr == nil && session.State == "connected" && !w.Store.Paused(ctx, owner) {
		result = "unchanged"
		var previous StoredProfile
		previous, requestErr = w.Store.Profile(ctx, owner, profile)
		if requestErr == nil {
			var oldHash, newHash string
			oldHash, requestErr = watchFingerprint(previous.Profile)
			if requestErr == nil {
				newHash, requestErr = watchFingerprint(p)
			}
			if requestErr == nil && oldHash != newHash {
				_, requestErr = w.Store.Archive(ctx, owner, []Profile{p}, "profile_watch")
				result = "changed"
			}
		}
		if requestErr == nil {
			var returned bool
			returned, requestErr = w.recordProfilePresence(ctx, owner, profile, true, finished)
			if returned {
				result = "returned"
			}
		}
	}
	retry := w.Config.WatchInterval
	if requestErr != nil {
		result = watchErrorCode(requestErr)
		var upstream *UpstreamError
		if errors.As(requestErr, &upstream) && upstream.RetryAfter > retry {
			retry = upstream.RetryAfter
		}
	}
	_, err = w.Store.DB.ExecContext(ctx, "UPDATE profile_watch SET next_check=?,last_result=? WHERE owner_id=? AND profile_id=?", finished.Add(retry).UnixMilli(), result, owner, profile)
	if requestErr != nil {
		return true, requestErr
	}
	return true, err
}

func watchFingerprint(p Profile) (string, error) {
	// shortcut: compare source filenames; add byte checks if the service reuses image URLs.
	var jobs []MediaJob
	for _, role := range []string{"photos", "miniPhotos"} {
		for position, source := range p.Photos(role) {
			jobs = append(jobs, MediaJob{Role: role, Position: position, URL: source})
		}
	}
	return mirrorFingerprint(p, jobs)
}

func watchErrorCode(err error) string {
	var upstream *UpstreamError
	if errors.As(err, &upstream) {
		return fmt.Sprintf("http_%d", upstream.Status)
	}
	return "check_failed"
}

func (w *ProfileWatcher) Status(ctx context.Context, owner int64) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.init(ctx) != nil {
		return "Проверка анкет: состояние недоступно"
	}
	var checked int64
	if w.Store.DB.QueryRowContext(ctx, "SELECT COALESCE(MAX(last_checked),0) FROM profile_watch WHERE owner_id=?", owner).Scan(&checked) != nil {
		return "Проверка анкет: состояние недоступно"
	}
	if checked == 0 {
		return "Проверка сохранённых анкет: раз в сутки; проверок ещё не было"
	}
	return "Проверка сохранённых анкет: раз в сутки; последняя попытка " + displayTime(time.UnixMilli(checked).Format(time.RFC3339))
}
