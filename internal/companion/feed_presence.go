package companion

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

const feedAbsenceSchema = `
CREATE TABLE IF NOT EXISTS profile_feed_absences (
 owner_id INTEGER NOT NULL, upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 run_id TEXT NOT NULL, observed_at TEXT NOT NULL, baseline_version_id INTEGER NOT NULL,
 PRIMARY KEY(owner_id,upstream_id,profile_id),
 FOREIGN KEY(owner_id,profile_id) REFERENCES profile_presence(owner_id,profile_id) ON DELETE CASCADE
);`

// Membership in either discovery source is independent of the detail GET result.
func feedSeenSQL(ctx context.Context, tx *sql.Tx) (string, error) {
	var catalog bool
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*)=2 FROM sqlite_master WHERE type='table' AND name IN ('crawl_location_results','location_batches')`).Scan(&catalog)
	if err != nil || !catalog {
		return "p.seen<>0", err
	}
	return `p.seen<>0 OR EXISTS(SELECT 1 FROM crawl_location_results r
 JOIN location_batches b ON b.id=r.first_batch_id AND b.owner_id=r.owner_id AND b.run_id=r.run_id
 WHERE r.owner_id=p.owner_id AND r.run_id=p.run_id AND r.profile_id=p.profile_id
 AND b.upstream_id=j.upstream_id AND b.generation=j.generation)`, nil
}

func migrateFeedAbsencesTx(ctx context.Context, tx *sql.Tx) error {
	marker, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO meta(key,value) VALUES('profile_feed_absences_migrated','1')")
	if err != nil {
		return err
	}
	n, err := marker.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	var ledger, refresh, broadColumn, catalog bool
	if err = tx.QueryRowContext(ctx, `SELECT
 COUNT(CASE WHEN name IN ('feed_crawls','crawl_presence_runs','crawl_presence_profiles') THEN 1 END)=3,
 COUNT(CASE WHEN name IN ('sync_cycles','sync_refresh_items') THEN 1 END)=2,
 EXISTS(SELECT 1 FROM pragma_table_info('feed_crawls') WHERE name='unfiltered'),
 COUNT(CASE WHEN name IN ('crawl_location_results','location_batches') THEN 1 END)=2
 FROM sqlite_master WHERE type='table'`).Scan(&ledger, &refresh, &broadColumn, &catalog); err != nil {
		return err
	}
	if ledger && broadColumn {
		seen, err := feedSeenSQL(ctx, tx)
		if err != nil {
			return err
		}
		// One current run per owner. Seed only from a completed broad pass.
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO profile_feed_absences(owner_id,upstream_id,profile_id,run_id,observed_at,baseline_version_id)
 SELECT j.owner_id,j.upstream_id,p.profile_id,j.run_id,l.finished_at,p.baseline_version_id
 FROM feed_crawls j JOIN crawl_presence_runs l ON l.owner_id=j.owner_id AND l.run_id=j.run_id
 JOIN sessions se ON se.owner_id=j.owner_id AND se.upstream_id=j.upstream_id
 JOIN crawl_presence_profiles p ON p.owner_id=j.owner_id AND p.run_id=j.run_id
 JOIN profile_presence pp ON pp.owner_id=p.owner_id AND pp.profile_id=p.profile_id
 WHERE j.state='done' AND j.reason='empty_page' AND j.include_others=1 AND j.unfiltered=1 AND l.finished_at IS NOT NULL
 AND p.is_baseline=1 AND NOT (`+seen+`)`); err != nil {
			return err
		}
	}
	var negatives []string
	if refresh {
		negatives = append(negatives, `SELECT c.owner_id,r.profile_id,c.upstream_id,c.generation,r.observed_at_ms AS observed_ms
 FROM sync_cycles c JOIN sync_refresh_items r ON r.cycle_id=c.id WHERE r.state='unavailable' AND r.observed_at_ms>0`)
	}
	if catalog {
		negatives = append(negatives, `SELECT r.owner_id,r.profile_id,b.upstream_id,b.generation,r.completed_at AS observed_ms
 FROM crawl_location_results r JOIN location_batches b ON b.id=r.first_batch_id AND b.owner_id=r.owner_id AND b.run_id=r.run_id
 JOIN feed_crawls j ON j.owner_id=r.owner_id AND j.run_id=r.run_id
 WHERE r.state='unavailable' AND r.completed_at>0`)
	}
	if ledger && len(negatives) > 0 {
		// Recover verified negative GETs hidden by an old feed_absent reason.
		if _, err = tx.ExecContext(ctx, `WITH negative_reads AS (`+strings.Join(negatives, " UNION ALL ")+`), receipts AS (
 SELECT p.owner_id,p.profile_id,MAX(r.observed_ms) AS observed_ms
 FROM profile_presence p JOIN feed_crawls j ON j.owner_id=p.owner_id
 JOIN sessions se ON se.owner_id=j.owner_id AND se.upstream_id=j.upstream_id
 JOIN negative_reads r ON r.owner_id=j.owner_id AND r.upstream_id=j.upstream_id AND r.generation=j.generation AND r.profile_id=p.profile_id
 WHERE p.availability='unavailable' AND p.reason='feed_absent'
 AND julianday(r.observed_ms/1000.0,'unixepoch')>=julianday(p.changed_at)
 GROUP BY p.owner_id,p.profile_id)
 UPDATE profile_presence AS p SET reason='direct_unavailable',
 changed_at=strftime('%Y-%m-%dT%H:%M:%fZ',r.observed_ms/1000.0,'unixepoch'),
 checked_at=strftime('%Y-%m-%dT%H:%M:%fZ',r.observed_ms/1000.0,'unixepoch')
 FROM receipts r WHERE p.owner_id=r.owner_id AND p.profile_id=r.profile_id`); err != nil {
			return err
		}
	}
	// An unverified legacy absence is eligible for a fresh direct check.
	_, err = tx.ExecContext(ctx, `UPDATE profile_presence SET availability='available',reason='',baseline_version_id=0,changed_at='',checked_at=''
 WHERE availability='unavailable' AND reason='feed_absent'`)
	return err
}

func finishFeedAbsencesTx(ctx context.Context, tx *sql.Tx, job *CrawlJob, upstream int64, observed time.Time) error {
	seen, err := feedSeenSQL(ctx, tx)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `WITH observed AS (
 SELECT p.profile_id,p.baseline_version_id,p.is_baseline,(`+seen+`) AS seen
 FROM crawl_presence_profiles p JOIN feed_crawls j ON j.owner_id=p.owner_id AND j.run_id=p.run_id
 WHERE p.owner_id=? AND p.run_id=?)
 SELECT p.profile_id,COALESCE(a.baseline_version_id,p.baseline_version_id),p.seen
 FROM observed p LEFT JOIN profile_feed_absences a ON a.owner_id=? AND a.upstream_id=? AND a.profile_id=p.profile_id
 WHERE (p.seen AND a.profile_id IS NOT NULL) OR (NOT p.seen AND p.is_baseline=1 AND a.profile_id IS NULL)
 ORDER BY p.profile_id`, job.Owner, job.RunID, job.Owner, upstream)
	if err != nil {
		return err
	}
	type change struct {
		profile, baseline int64
		returned          bool
	}
	var changes []change
	for rows.Next() {
		var c change
		if err = rows.Scan(&c.profile, &c.baseline, &c.returned); err != nil {
			rows.Close()
			return err
		}
		changes = append(changes, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	at := observed.UTC().Format(time.RFC3339Nano)
	for _, c := range changes {
		current, err := presenceVersionTx(ctx, tx, job.Owner, c.profile)
		if err != nil {
			return err
		}
		if current == 0 {
			current = c.baseline
		}
		availability, reason, summary := "unavailable", "feed_absent", ""
		if c.returned {
			availability, reason = "available", "feed_returned"
			summary, err = profilePresenceChangesTx(ctx, tx, job.Owner, c.profile, c.baseline, current)
			if err == nil {
				_, err = tx.ExecContext(ctx, "DELETE FROM profile_feed_absences WHERE owner_id=? AND upstream_id=? AND profile_id=?", job.Owner, upstream, c.profile)
			}
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO profile_feed_absences(owner_id,upstream_id,profile_id,run_id,observed_at,baseline_version_id)
 VALUES(?,?,?,?,?,?)`, job.Owner, upstream, c.profile, job.RunID, at, c.baseline)
		}
		if err != nil {
			return err
		}
		var transition int64
		if err = tx.QueryRowContext(ctx, `UPDATE profile_presence SET transition=transition+1
 WHERE owner_id=? AND profile_id=? RETURNING transition`, job.Owner, c.profile).Scan(&transition); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,reason,old_version_id,version_id,changes_text)
 VALUES(?,?,?,?,?,?,?,?,?)`, job.Owner, c.profile, transition, availability, at, reason, c.baseline, current, summary); err != nil {
			return err
		}
	}
	return nil
}
