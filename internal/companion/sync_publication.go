package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const syncPublicationSchema = `
CREATE TABLE IF NOT EXISTS sync_publication_manifests (
 cycle_id TEXT PRIMARY KEY REFERENCES sync_cycles(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL, upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 frozen_at_ms INTEGER NOT NULL, indexes_frozen INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sync_publication_items (
 cycle_id TEXT NOT NULL REFERENCES sync_publication_manifests(cycle_id) ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK(kind IN ('version','analysis','presence','change')),
 item_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, source_sha TEXT NOT NULL,
 disposition TEXT NOT NULL DEFAULT 'required' CHECK(disposition IN ('required','ignored')),
 reason TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(cycle_id,kind,item_id)
);
CREATE TABLE IF NOT EXISTS sync_publication_indexes (
 cycle_id TEXT NOT NULL REFERENCES sync_publication_manifests(cycle_id) ON DELETE CASCADE,
 topic_id INTEGER NOT NULL, page_no INTEGER NOT NULL, payload_sha TEXT NOT NULL,
 PRIMARY KEY(cycle_id,topic_id,page_no)
);`

var errSyncPublicationDeferred = errors.New("sync publication is not ready")

const syncPresenceSource = `json_array(e.owner_id,e.profile_id,e.transition,e.availability,e.observed_at,e.reason,e.old_version_id,e.version_id,e.changes_text)`

type syncPublicationOwnerKey struct{}

// The source coordinator and poster role run in separate processes.
func (s *Store) trySyncPublicationLock() (func(), bool, error) {
	if !s.cfg.SyncCycleEnabled {
		return func() {}, true, nil
	}
	f, err := os.OpenFile(filepath.Join(s.cfg.DataDir, "sync-publication.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, true, nil
}

func (s *Store) requireSyncPublication(ctx context.Context, owner int64) error {
	if !s.cfg.SyncCycleEnabled {
		return nil
	}
	allowed, err := s.SyncPublicationAllowed(ctx, owner)
	if err != nil {
		return err
	}
	if !allowed {
		return errSyncPublicationDeferred
	}
	c, err := s.SyncCycle(ctx, owner)
	if err != nil {
		return err
	}
	if c == nil || (c.Stage != "publishing" && c.Stage != "complete") {
		return errSyncPublicationDeferred
	}
	ss, err := s.Session(ctx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return errSyncPublicationDeferred
	}
	if err != nil {
		return err
	}
	if ss.UserID != c.UserID || ss.Generation != c.Generation {
		return errSyncPublicationDeferred
	}
	var frozen bool
	if err = s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_publication_manifests WHERE cycle_id=? AND owner_id=? AND upstream_id=? AND generation=?)", c.ID, c.OwnerID, c.UserID, c.Generation).Scan(&frozen); err != nil {
		return err
	}
	if !frozen {
		return errSyncPublicationDeferred
	}
	return nil
}

// Only trusted column names and kind literals from the poster queries enter this SQL.
func (s *Store) syncPublicationCandidates(owner, item, kind string) string {
	if !s.cfg.SyncCycleEnabled {
		return ""
	}
	return fmt.Sprintf(` AND EXISTS(SELECT 1 FROM sync_cycles sc
 WHERE sc.owner_id=%s AND sc.id=(SELECT latest.id FROM sync_cycles latest WHERE latest.owner_id=sc.owner_id ORDER BY latest.started_at_ms DESC,latest.id DESC LIMIT 1)
 AND (sc.stage='complete' OR (sc.stage='publishing' AND EXISTS(SELECT 1 FROM sync_publication_items pi
 JOIN sync_publication_manifests pm ON pm.cycle_id=pi.cycle_id AND pm.owner_id=sc.owner_id AND pm.upstream_id=sc.upstream_id AND pm.generation=sc.generation
 WHERE pi.cycle_id=sc.id AND pi.kind='%s' AND pi.item_id=%s AND pi.disposition='required')))) `, owner, kind, item)
}

func (s *Store) syncPublicationPhaseCandidates(owner string) string {
	if !s.cfg.SyncCycleEnabled {
		return ""
	}
	return fmt.Sprintf(` AND EXISTS(SELECT 1 FROM sync_cycles sc JOIN sync_publication_manifests pm ON pm.cycle_id=sc.id
 WHERE sc.owner_id=%s AND pm.owner_id=sc.owner_id AND pm.upstream_id=sc.upstream_id AND pm.generation=sc.generation
 AND sc.id=(SELECT latest.id FROM sync_cycles latest WHERE latest.owner_id=sc.owner_id ORDER BY latest.started_at_ms DESC,latest.id DESC LIMIT 1)
 AND sc.stage IN ('publishing','complete')) `, owner)
}

// Readiness must use the frozen snapshot, not a later foreground queue pointer.
func (s *Store) syncAnalysisPostReady() string {
	if !s.cfg.SyncCycleEnabled {
		return analysisPostReady
	}
	version := `CASE WHEN EXISTS(SELECT 1 FROM sync_cycles sc WHERE sc.owner_id=q.owner_id AND sc.stage='publishing') THEN
 (SELECT MAX(pi.item_id) FROM sync_publication_items pi JOIN sync_cycles sc ON sc.id=pi.cycle_id
 WHERE sc.owner_id=q.owner_id AND sc.stage='publishing' AND pi.kind='version' AND pi.profile_id=q.profile_id)
 ELSE q.version_id END`
	ready := strings.Replace(analysisPostReady, "av.version_id=q.version_id", "av.version_id=("+version+")", 1)
	return strings.Replace(ready, "WHERE v.owner_id=p.owner_id AND v.profile_id=p.profile_id AND (", "WHERE v.owner_id=p.owner_id AND v.profile_id=p.profile_id "+s.syncPublicationCandidates("v.owner_id", "v.id", "version")+" AND (", 1)
}

// FreezeSyncPublication commits an immutable finite frontier, including an empty one.
func (s *Store) FreezeSyncPublication(ctx context.Context, cycle SyncCycle) error {
	if _, err := s.DB.ExecContext(ctx, archiveSchema+analysisSchema+analysisPostsSchema+profilePresenceSchema+archiveChangePostsSchema+syncPublicationSchema); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='sync_cycle_writer'"); err != nil {
		return err
	}
	if err = syncCycleBindingTx(ctx, tx, s, cycle); err != nil {
		return err
	}
	var stage string
	if err = tx.QueryRowContext(ctx, "SELECT stage FROM sync_cycles WHERE id=? AND owner_id=? AND upstream_id=? AND generation=?", cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation).Scan(&stage); err != nil {
		return err
	}
	if stage != "draining" && stage != "publishing" && stage != "complete" {
		return errSyncPublicationDeferred
	}
	var frozenOwner, frozenUpstream int64
	var frozenGeneration string
	err = tx.QueryRowContext(ctx, "SELECT owner_id,upstream_id,generation FROM sync_publication_manifests WHERE cycle_id=?", cycle.ID).Scan(&frozenOwner, &frozenUpstream, &frozenGeneration)
	if err == nil {
		if frozenOwner != cycle.OwnerID || frozenUpstream != cycle.UserID || frozenGeneration != cycle.Generation {
			return errSyncPublicationDeferred
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var pending bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_refresh_items WHERE cycle_id=? AND state NOT IN ('available','unavailable'))", cycle.ID).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return errSyncPublicationDeferred
	}
	if s.cfg.CompatibilityEnabled {
		var frontierReady bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM compatibility_cycle_scans cs WHERE cs.cycle_id=? AND cs.owner_id=? AND cs.upstream_id=? AND cs.generation=? AND cs.sealed=1 AND cs.final_pass=1
 AND NOT EXISTS(SELECT 1 FROM compatibility_cycle_targets ct WHERE ct.cycle_id=cs.cycle_id AND ct.final_member=1 AND ct.state='pending'))`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation).Scan(&frontierReady); err != nil {
			return err
		}
		if !frontierReady {
			return errSyncPublicationDeferred
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO sync_publication_manifests(cycle_id,owner_id,upstream_id,generation,frozen_at_ms) VALUES(?,?,?,?,?)", cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation, time.Now().UnixMilli()); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT v.id,v.profile_id,v.raw_json FROM versions v JOIN sync_refresh_items r ON r.profile_id=v.profile_id AND r.cycle_id=?
 WHERE v.owner_id=? AND (v.id=r.version_id OR julianday(v.first_seen)>=julianday(?)) ORDER BY v.id`, cycle.ID, cycle.OwnerID, time.UnixMilli(cycle.StartedAt).UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	type version struct {
		id, profile int64
		raw         string
	}
	var versions []version
	for rows.Next() {
		var v version
		if err = rows.Scan(&v.id, &v.profile, &v.raw); err != nil {
			rows.Close()
			return err
		}
		versions = append(versions, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range versions {
		p, e := decodeProfile([]byte(v.raw))
		if e != nil || p.ID() != v.profile {
			return errors.New("invalid frozen profile source")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO sync_publication_items(cycle_id,kind,item_id,profile_id,source_sha) VALUES(?,'version',?,?,?)", cycle.ID, v.id, v.profile, digest([]byte(v.raw))); err != nil {
			return err
		}
		if cycle.OwnerID == s.cfg.OwnerID {
			if _, err = tx.ExecContext(ctx, `INSERT INTO sync_publication_items(cycle_id,kind,item_id,profile_id,source_sha,disposition)
 VALUES(?,'change',?,?,?,CASE WHEN EXISTS(SELECT 1 FROM profile_presence WHERE owner_id=? AND profile_id=? AND availability='unavailable') THEN 'ignored' ELSE 'required' END)`, cycle.ID, v.id, v.profile, digest([]byte(v.raw)), cycle.OwnerID, v.profile); err != nil {
				return err
			}
		}
	}
	rows, err = tx.QueryContext(ctx, `SELECT e.id,e.profile_id,`+syncPresenceSource+` FROM profile_presence_events e JOIN sync_refresh_items r ON r.cycle_id=? AND r.profile_id=e.profile_id
 WHERE e.owner_id=? AND (e.state<>'sent' OR julianday(e.observed_at)>=julianday(?))
 AND (EXISTS(SELECT 1 FROM mirror_topics t WHERE t.profile_id=e.profile_id AND t.thread_id>0) OR EXISTS(SELECT 1 FROM sync_publication_items v WHERE v.cycle_id=? AND v.kind='version' AND v.profile_id=e.profile_id))`, cycle.ID, cycle.OwnerID, time.UnixMilli(cycle.StartedAt).UTC().Format(time.RFC3339Nano), cycle.ID)
	if err != nil {
		return err
	}
	type presence struct {
		id, profile int64
		source      string
	}
	var events []presence
	for rows.Next() {
		var e presence
		if err = rows.Scan(&e.id, &e.profile, &e.source); err != nil {
			rows.Close()
			return err
		}
		events = append(events, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, e := range events {
		if _, err = tx.ExecContext(ctx, "INSERT INTO sync_publication_items(cycle_id,kind,item_id,profile_id,source_sha) VALUES(?,'presence',?,?,?)", cycle.ID, e.id, e.profile, digest([]byte(e.source))); err != nil {
			return err
		}
	}

	if cycle.OwnerID == s.cfg.OwnerID {
		var has bool
		e := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='compatibility_cycle_targets')").Scan(&has)
		if e != nil {
			return e
		}
		if has {
			rows, err = tx.QueryContext(ctx, `SELECT DISTINCT a.id,a.profile_id,a.response_body FROM compatibility_cycle_targets ct
 JOIN compatibility_cycle_scans cs ON cs.cycle_id=ct.cycle_id JOIN compatibility_jobs j ON j.id=ct.job_id AND j.input_hash=ct.input_hash AND j.state='succeeded' JOIN profile_analyses a ON a.id=j.analysis_id
 JOIN sync_refresh_items cohort ON cohort.cycle_id=ct.cycle_id AND cohort.profile_id=ct.profile_id
 WHERE ct.cycle_id=? AND cs.owner_id=? AND cs.upstream_id=? AND cs.generation=? AND ct.final_member=1 AND ct.state='succeeded'
 AND a.owner_id=cs.owner_id AND a.upstream_id=cs.upstream_id AND a.profile_id=ct.profile_id AND `+analysisPostSuccessSQL("a")+`
 AND NOT EXISTS(SELECT 1 FROM profile_presence pp WHERE pp.owner_id=a.owner_id AND pp.profile_id=a.profile_id AND pp.availability='unavailable')`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation)
			if err != nil {
				return err
			}
			type analysis struct {
				id, profile int64
				raw         []byte
			}
			var analyses []analysis
			for rows.Next() {
				var a analysis
				if err = rows.Scan(&a.id, &a.profile, &a.raw); err != nil {
					rows.Close()
					return err
				}
				analyses = append(analyses, a)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, a := range analyses {
				capture, e := scanProfileAnalysis(tx.QueryRowContext(ctx, "SELECT "+analysisColumns+" FROM profile_analyses a WHERE a.id=? AND a.owner_id=? AND a.upstream_id=?", a.id, cycle.OwnerID, cycle.UserID))
				if e != nil {
					return e
				}
				if _, e = analysisPostText(capture); e != nil {
					return e
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO sync_publication_items(cycle_id,kind,item_id,profile_id,source_sha) VALUES(?,'analysis',?,?,?)", cycle.ID, a.id, a.profile, digest(a.raw)); err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO analysis_posts(analysis_id,owner_id,upstream_id,profile_id,state) VALUES(?,?,?,?,'pending')`, a.id, cycle.OwnerID, cycle.UserID, a.profile); err != nil {
					return err
				}
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sync_publication_items SET reason='source_unavailable_at_freeze' WHERE cycle_id=? AND disposition='ignored' AND reason=''", cycle.ID); err != nil {
		return err
	}
	return tx.Commit()
}

type syncPublicationItem struct {
	kind, sha, disposition string
	id, profile            int64
}

// syncConfirmedVersion requires an actual whole-ZIP receipt and exact confirmed layout.
// A global duplicate is complete only through the matching original's receipts.
func (s *Service) syncConfirmedVersion(ctx context.Context, version, owner, profile int64) (bool, error) {
	var state, fp string
	var thread int64
	err := s.Store.DB.QueryRowContext(ctx, "SELECT state,fingerprint,thread_id FROM archive_versions WHERE version_id=? AND owner_id=? AND profile_id=?", version, owner, profile).Scan(&state, &fp, &thread)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state == "duplicate" {
		rows, e := s.Store.DB.QueryContext(ctx, `SELECT version_id,owner_id FROM archive_versions WHERE fingerprint=? AND fingerprint<>'' AND profile_id=? AND thread_id=? AND state IN ('sent','partial') ORDER BY version_id`, fp, profile, thread)
		if e != nil {
			return false, e
		}
		var originals [][2]int64
		for rows.Next() {
			var o [2]int64
			if e = rows.Scan(&o[0], &o[1]); e != nil {
				rows.Close()
				return false, e
			}
			originals = append(originals, o)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return false, e
		}
		for _, o := range originals {
			ok, e := s.syncConfirmedVersion(ctx, o[0], o[1], profile)
			if e != nil || ok {
				return ok, e
			}
		}
		return false, nil
	}
	if state != "sent" && state != "partial" {
		return false, nil
	}
	var idsRaw, layoutRaw, layoutState, file, kind string
	var chat, receiptThread, zip, topic int64
	err = s.Store.DB.QueryRowContext(ctx, `SELECT av.message_ids,l.new_message_ids,l.state,r.chat_id,r.thread_id,r.message_id,r.file_id,r.kind,t.thread_id
 FROM archive_versions av JOIN archive_profile_repairs l ON l.version_id=av.version_id JOIN archive_receipts r ON r.source_sha=av.source_sha JOIN mirror_topics t ON t.profile_id=av.profile_id
 WHERE av.version_id=? AND julianday(r.sent_at) IS NOT NULL`, version).Scan(&idsRaw, &layoutRaw, &layoutState, &chat, &receiptThread, &zip, &file, &kind, &topic)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ids []int64
	if layoutState != "sent" || idsRaw != layoutRaw || chat != s.Config.BackupChatID || thread <= 0 || thread == s.Config.BackupThreadID || receiptThread != thread || topic != thread || zip <= 0 || file == "" || kind != "profile_version" || json.Unmarshal([]byte(idsRaw), &ids) != nil || len(ids) == 0 {
		return false, nil
	}
	seen := map[int64]bool{zip: true}
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return false, nil
		}
		seen[id] = true
	}
	var photos, mini int
	if err = s.Store.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(role='photos'),0),COALESCE(SUM(role='miniPhotos'),0) FROM media_jobs WHERE owner_id=? AND version_id=? AND state='complete' AND sha IS NOT NULL AND sha<>''`, owner, version).Scan(&photos, &mini); err != nil {
		return false, err
	}
	expected := photos
	if expected == 0 {
		expected = mini
	}
	if expected == 0 {
		expected = 1
	}
	var markerRaw string
	err = s.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", richCardKey(chat, ids[0])).Scan(&markerRaw)
	if err == nil {
		var m richCardMarker
		if json.Unmarshal([]byte(markerRaw), &m) != nil || !validRichCardMarker(m) || m.OwnerID != owner || m.VersionID != version || m.ThreadID != thread {
			return false, nil
		}
		return len(ids) == 1, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return len(ids) == expected, nil
}

func (s *Service) syncCyclePublicationComplete(ctx context.Context, cycle SyncCycle) (bool, error) {
	if s.Archiver == nil || !s.Archiver.mu.TryLock() {
		return false, nil
	}
	defer s.Archiver.mu.Unlock()
	unlock, locked, err := s.Store.trySyncPublicationLock()
	if err != nil || !locked {
		return false, err
	}
	defer unlock()
	if err := s.Store.requireSyncPublication(ctx, cycle.OwnerID); err != nil {
		if errors.Is(err, errSyncPublicationDeferred) {
			return false, nil
		}
		return false, err
	}
	current, err := s.Store.SyncCycle(ctx, cycle.OwnerID)
	if err != nil {
		return false, err
	}
	if current == nil || current.ID != cycle.ID || current.Stage != "publishing" {
		return false, nil
	}
	var owner, upstream int64
	var generation string
	var indexesFrozen bool
	err = s.Store.DB.QueryRowContext(ctx, "SELECT owner_id,upstream_id,generation,indexes_frozen FROM sync_publication_manifests WHERE cycle_id=?", cycle.ID).Scan(&owner, &upstream, &generation, &indexesFrozen)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner != cycle.OwnerID || upstream != cycle.UserID || generation != cycle.Generation {
		return false, nil
	}
	if err = s.ignoreLateSyncNotices(ctx, cycle); err != nil {
		return false, err
	}
	rows, err := s.Store.DB.QueryContext(ctx, "SELECT kind,item_id,profile_id,source_sha,disposition FROM sync_publication_items WHERE cycle_id=? ORDER BY CASE kind WHEN 'version' THEN 0 WHEN 'presence' THEN 1 WHEN 'analysis' THEN 2 ELSE 3 END,item_id", cycle.ID)
	if err != nil {
		return false, err
	}
	var items []syncPublicationItem
	for rows.Next() {
		var i syncPublicationItem
		if err = rows.Scan(&i.kind, &i.id, &i.profile, &i.sha, &i.disposition); err != nil {
			rows.Close()
			return false, err
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	// Changes are enqueued from confirmed publication chronology, before absence can mean initial/no-op.
	if cycle.OwnerID == s.Config.OwnerID {
		if err = s.enqueueArchiveChangePosts(ctx); err != nil {
			return false, err
		}
	}
	for _, i := range items {
		if i.disposition == "ignored" {
			continue
		}
		ready := false
		switch i.kind {
		case "version", "change":
			var raw string
			err = s.Store.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=? AND owner_id=? AND profile_id=?", i.id, cycle.OwnerID, i.profile).Scan(&raw)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if digest([]byte(raw)) != i.sha {
				return false, nil
			}
			ready, err = s.syncConfirmedVersion(ctx, i.id, cycle.OwnerID, i.profile)
			if err != nil || !ready {
				return false, err
			}
			if i.kind == "change" {
				var state, reason string
				var message, thread, noticeOwner, noticeProfile int64
				err = s.Store.DB.QueryRowContext(ctx, "SELECT state,message_id,thread_id,owner_id,profile_id,error FROM archive_change_posts WHERE version_id=?", i.id).Scan(&state, &message, &thread, &noticeOwner, &noticeProfile, &reason)
				if errors.Is(err, sql.ErrNoRows) {
					// Confirmed initial publications and global duplicates have no change post.
					_, err = s.Store.DB.ExecContext(ctx, "UPDATE sync_publication_items SET disposition='ignored',reason='initial_or_duplicate_publication' WHERE cycle_id=? AND kind='change' AND item_id=?", cycle.ID, i.id)
					if err != nil {
						return false, err
					}
					continue
				}
				if err != nil {
					return false, err
				}
				if noticeOwner != cycle.OwnerID || noticeProfile != i.profile {
					return false, nil
				}
				if state == "skipped" {
					ready = message == 0 && (reason == "no_profile_changes" || reason == "initial_published_version")
				} else {
					var match bool
					err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM mirror_topics WHERE profile_id=? AND thread_id=? AND thread_id>0)", i.profile, thread).Scan(&match)
					ready = state == "sent" && message > 0 && match
				}
			}
		case "analysis":
			var response []byte
			var sha, state string
			var chat, thread, message int64
			var receipt []byte
			err = s.Store.DB.QueryRowContext(ctx, `SELECT a.response_body,p.response_sha,p.state,p.chat_id,p.thread_id,p.message_id,p.receipt_json FROM profile_analyses a JOIN analysis_posts p ON p.analysis_id=a.id
 WHERE a.id=? AND a.owner_id=? AND a.upstream_id=? AND a.profile_id=? AND p.owner_id=a.owner_id AND p.upstream_id=a.upstream_id AND p.profile_id=a.profile_id`, i.id, cycle.OwnerID, cycle.UserID, i.profile).Scan(&response, &sha, &state, &chat, &thread, &message, &receipt)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			var ack SentMessage
			var topic int64
			err = s.Store.DB.QueryRowContext(ctx, "SELECT thread_id FROM mirror_topics WHERE profile_id=?", i.profile).Scan(&topic)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			ready = state == "sent" && digest(response) == i.sha && sha == i.sha && chat == s.Config.BackupChatID && thread == topic && thread > 0 && thread != s.Config.BackupThreadID && message > 0 && json.Unmarshal(receipt, &ack) == nil && ack.ID == message && ack.Chat.ID == chat && ack.ThreadID == thread
		case "presence":
			var state, source string
			var chat, thread, message, topic int64
			err = s.Store.DB.QueryRowContext(ctx, `SELECT e.state,e.chat_id,e.thread_id,e.message_id,t.thread_id,`+syncPresenceSource+` FROM profile_presence_events e JOIN mirror_topics t ON t.profile_id=e.profile_id WHERE e.id=? AND e.owner_id=? AND e.profile_id=?`, i.id, cycle.OwnerID, i.profile).Scan(&state, &chat, &thread, &message, &topic, &source)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			ready = digest([]byte(source)) == i.sha && state == "sent" && message > 0 && chat == s.Config.BackupChatID && thread == topic && thread > 0 && thread != s.Config.BackupThreadID
		}
		if err != nil || !ready {
			return false, err
		}
	}
	if cycle.OwnerID != s.Config.OwnerID || s.Config.BackupChatID == 0 {
		return true, nil
	}
	if ready, err := s.syncPublicationCaptionsComplete(ctx, cycle); err != nil || !ready {
		return false, err
	}
	return s.syncPublicationIndexesComplete(ctx, cycle, indexesFrozen)
}
