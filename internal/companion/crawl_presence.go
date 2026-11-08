package companion

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const crawlPresenceSchema = profilePresenceSchema + `
CREATE TABLE IF NOT EXISTS crawl_presence_runs (
 owner_id INTEGER NOT NULL REFERENCES sessions(owner_id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, initialized_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TEXT,
 PRIMARY KEY(owner_id,run_id)
);
CREATE TABLE IF NOT EXISTS crawl_presence_profiles (
 owner_id INTEGER NOT NULL, run_id TEXT NOT NULL, profile_id INTEGER NOT NULL,
 baseline_version_id INTEGER NOT NULL, is_baseline INTEGER NOT NULL, seen INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(owner_id,run_id,profile_id),
 FOREIGN KEY(owner_id,run_id) REFERENCES crawl_presence_runs(owner_id,run_id) ON DELETE CASCADE
);`

// Request snapshots only this owner's known profiles; disconnect removes the ledger.
func initCrawlPresenceTx(ctx context.Context, tx *sql.Tx, owner int64, runID string) error {
	if _, err := tx.ExecContext(ctx, crawlPresenceSchema); err != nil {
		return err
	}
	if err := migrateProfilePresenceTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM crawl_presence_runs WHERE owner_id=?", owner); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO crawl_presence_runs(owner_id,run_id) VALUES(?,?)", owner, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crawl_presence_profiles(owner_id,run_id,profile_id,baseline_version_id,is_baseline)
 SELECT q.owner_id,?,q.profile_id,v.id,1 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=?`, runID, owner); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at)
 SELECT q.owner_id,q.profile_id,'available',v.last_seen,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=?`, owner)
	return err
}

// The caller has archived this batch and records seen IDs in its checkpoint TX.
func recordCrawlSeenTx(ctx context.Context, tx *sql.Tx, job *CrawlJob, profiles []Profile, observed ...time.Time) error {
	var initialized bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM crawl_presence_runs WHERE owner_id=? AND run_id=?)", job.Owner, job.RunID).Scan(&initialized)
	if err != nil || !initialized {
		return err
	}
	var active bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=? AND run_id=? AND state IN ('queued','running'))", job.Owner, job.RunID).Scan(&active); err != nil || !active {
		return err
	}
	at := time.Now()
	if len(observed) > 0 {
		at = observed[0]
	}
	seen := map[int64]bool{}
	for _, profile := range profiles {
		id := profile.ID()
		if id <= 0 {
			return errors.New("crawl presence received profile without ID")
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		version, err := presenceVersionTx(ctx, tx, job.Owner, id)
		if err != nil {
			return err
		}
		if version == 0 {
			return errors.New("crawl presence profile was not archived")
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO crawl_presence_profiles(owner_id,run_id,profile_id,baseline_version_id,is_baseline,seen) VALUES(?,?,?,0,0,1)
 ON CONFLICT(owner_id,run_id,profile_id) DO UPDATE SET seen=1`, job.Owner, job.RunID, id); err != nil {
			return err
		}
		if _, err = recordProfilePresenceTx(ctx, tx, job.Owner, id, true, "", 0, version, at); err != nil {
			return err
		}
	}
	return nil
}

// Call after the final job UPDATE to done/empty_page, in that same checkpoint TX.
// A stopped, partial, old or single-mode run can never declare profiles absent.
func finishCrawlPresenceTx(ctx context.Context, tx *sql.Tx, job *CrawlJob, observed time.Time) error {
	var complete bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM feed_crawls j JOIN crawl_presence_runs r ON r.owner_id=j.owner_id AND r.run_id=j.run_id
 WHERE j.owner_id=? AND j.run_id=? AND j.state='done' AND j.reason='empty_page' AND j.include_others=1 AND r.finished_at IS NULL)`, job.Owner, job.RunID).Scan(&complete)
	if err != nil || !complete {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT profile_id,baseline_version_id FROM crawl_presence_profiles WHERE owner_id=? AND run_id=? AND is_baseline=1 AND seen=0 ORDER BY profile_id", job.Owner, job.RunID)
	if err != nil {
		return err
	}
	type missingProfile struct{ profile, baseline int64 }
	var missing []missingProfile
	for rows.Next() {
		var m missingProfile
		if err = rows.Scan(&m.profile, &m.baseline); err != nil {
			rows.Close()
			return err
		}
		missing = append(missing, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, m := range missing {
		current, err := presenceVersionTx(ctx, tx, job.Owner, m.profile)
		if err != nil {
			return err
		}
		if _, err = recordProfilePresenceTx(ctx, tx, job.Owner, m.profile, false, "feed_absent", m.baseline, current, observed); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE crawl_presence_runs SET finished_at=? WHERE owner_id=? AND run_id=?", observed.UTC().Format(time.RFC3339Nano), job.Owner, job.RunID)
	return err
}
