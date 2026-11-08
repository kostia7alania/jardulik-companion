package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type dailyCrawlRequestKey struct{}
type dailyCrawlIdentity struct {
	UserID     int64
	Generation string
}

type dailyCrawlMarker struct {
	RunID      string `json:"run_id"`
	Generation string `json:"generation"`
	UserID     int64  `json:"upstream_id"`
	NextAt     int64  `json:"next_at"`
	Blocked    bool   `json:"blocked,omitempty"`
}

func dailyCrawlKey(owner int64) string      { return fmt.Sprintf("daily_crawl:%d", owner) }
func dailyCrawlBlockKey(owner int64) string { return fmt.Sprintf("daily_crawl_block:%d", owner) }

func (c *FeedCrawler) saveDailyCrawlMarker(ctx context.Context, owner int64, marker dailyCrawlMarker) error {
	raw, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	_, err = c.Service.Store.DB.ExecContext(ctx, `INSERT INTO meta(key,value)
 SELECT ?,? WHERE EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=? AND run_id=?)
 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, dailyCrawlKey(owner), string(raw), owner, marker.RunID)
	return err
}

// ScheduleDaily only queues the configured owner's run. Each worker request checks entitlement.
func (c *FeedCrawler) ScheduleDaily(ctx context.Context) (bool, error) {
	s := c.Service
	owner := s.Config.OwnerID
	if !s.Config.DailyCrawl || owner <= 0 || s.foregroundPending() {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	mu := s.userLock(owner)
	if !mu.TryLock() {
		return false, nil
	}
	defer mu.Unlock()
	if s.foregroundPending() {
		return false, nil
	}
	if err := c.init(ctx); err != nil {
		return false, err
	}
	j, err := s.Store.CrawlStatus(ctx, owner)
	if err != nil {
		return false, err
	}
	var raw string
	err = s.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", dailyCrawlKey(owner)).Scan(&raw)
	var marker dailyCrawlMarker
	hasMarker := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if hasMarker {
		if err = json.Unmarshal([]byte(raw), &marker); err != nil {
			return false, err
		}
		if marker.RunID == "" || marker.Generation == "" || marker.UserID <= 0 || marker.NextAt <= 0 {
			return false, errors.New("invalid daily crawl marker")
		}
	}
	// A missing job after initial activation is not permission to recreate a deleted/stopped run.
	if j == nil && hasMarker {
		return false, nil
	}
	changed := false
	if j != nil && (!hasMarker || marker.RunID != j.RunID) {
		marker = dailyCrawlMarker{RunID: j.RunID, Generation: j.Generation, UserID: j.UserID, NextAt: j.StartedAt + (24 * time.Hour).Milliseconds()}
		changed = true
	}
	var paused bool
	err = s.Store.DB.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&paused)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	session, err := s.Store.Session(ctx, owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	connected := err == nil && session.State == "connected" && session.Generation != "" && session.UserID > 0
	if j != nil && (paused || !connected || session.Generation != marker.Generation || session.UserID != marker.UserID) && !marker.Blocked {
		marker.Blocked = true
		changed = true
	}
	if changed {
		if err = c.saveDailyCrawlMarker(ctx, owner, marker); err != nil {
			return false, err
		}
	}
	if paused || !connected {
		return false, nil
	}
	if j != nil {
		if marker.Blocked || j.active() || j.State != "done" || j.Reason != "empty_page" || c.now().UnixMilli() < marker.NextAt {
			return false, nil
		}
	}
	var blocked bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1')", dailyCrawlBlockKey(owner)).Scan(&blocked); err != nil {
		return false, err
	}
	if blocked {
		return false, nil
	}
	next, err := c.requestLocked(context.WithValue(ctx, dailyCrawlRequestKey{}, dailyCrawlIdentity{UserID: session.UserID, Generation: session.Generation}), owner, true, true)
	if err != nil {
		var lifecycle *crawlLifecycleError
		if !errors.As(err, &lifecycle) {
			return false, err
		}
		if j != nil {
			marker.Blocked = true
			if err = c.saveDailyCrawlMarker(ctx, owner, marker); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	marker = dailyCrawlMarker{RunID: next.RunID, Generation: next.Generation, UserID: next.UserID, NextAt: next.StartedAt + (24 * time.Hour).Milliseconds()}
	return true, c.saveDailyCrawlMarker(ctx, owner, marker)
}
