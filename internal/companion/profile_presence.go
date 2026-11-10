package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const profilePresenceSchema = `
CREATE TABLE IF NOT EXISTS profile_presence (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, availability TEXT NOT NULL CHECK(availability IN ('available','unavailable')),
 transition INTEGER NOT NULL DEFAULT 0, changed_at TEXT NOT NULL, checked_at TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '', baseline_version_id INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(owner_id,profile_id)
);
CREATE TABLE IF NOT EXISTS profile_presence_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, transition INTEGER NOT NULL,
 availability TEXT NOT NULL CHECK(availability IN ('available','unavailable')), observed_at TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', next_attempt INTEGER NOT NULL DEFAULT 0,
 chat_id INTEGER NOT NULL DEFAULT 0, thread_id INTEGER NOT NULL DEFAULT 0,
 message_id INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 reason TEXT NOT NULL DEFAULT '', old_version_id INTEGER NOT NULL DEFAULT 0,
 version_id INTEGER NOT NULL DEFAULT 0, changes_text TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,profile_id,transition),
 FOREIGN KEY(owner_id,profile_id) REFERENCES profile_presence(owner_id,profile_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS profile_presence_pending ON profile_presence_events(state,next_attempt,id);
`

func migrateProfilePresenceTx(ctx context.Context, tx *sql.Tx) error {
	// Serialize schema inspection and ALTER across startup workers and connections.
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_presence_schema'"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, profilePresenceSchema); err != nil {
		return err
	}
	for _, table := range []struct {
		name    string
		columns []struct{ name, definition string }
	}{
		{"profile_presence", []struct{ name, definition string }{{"reason", "TEXT NOT NULL DEFAULT ''"}, {"baseline_version_id", "INTEGER NOT NULL DEFAULT 0"}}},
		{"profile_presence_events", []struct{ name, definition string }{{"reason", "TEXT NOT NULL DEFAULT ''"}, {"old_version_id", "INTEGER NOT NULL DEFAULT 0"}, {"version_id", "INTEGER NOT NULL DEFAULT 0"}, {"changes_text", "TEXT NOT NULL DEFAULT ''"}}},
	} {
		rows, err := tx.QueryContext(ctx, "PRAGMA table_info("+table.name+")")
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
		for _, column := range table.columns {
			if !columns[column.name] {
				if _, err = tx.ExecContext(ctx, "ALTER TABLE "+table.name+" ADD COLUMN "+column.name+" "+column.definition); err != nil {
					return err
				}
			}
		}
	}
	_, err := tx.ExecContext(ctx, `
 UPDATE profile_presence SET reason='direct_unavailable' WHERE availability='unavailable' AND reason='';
 UPDATE profile_presence SET baseline_version_id=COALESCE((
   SELECT v.id FROM versions v WHERE v.owner_id=profile_presence.owner_id AND v.profile_id=profile_presence.profile_id
   AND julianday(v.first_seen)<=julianday(profile_presence.changed_at) ORDER BY v.first_seen DESC,v.id DESC LIMIT 1),0)
 WHERE availability='unavailable' AND baseline_version_id=0;
 UPDATE profile_presence_events SET reason='direct_unavailable' WHERE availability='unavailable' AND reason='';
 UPDATE profile_presence_events SET version_id=COALESCE((SELECT q.version_id FROM queue q WHERE q.owner_id=profile_presence_events.owner_id AND q.profile_id=profile_presence_events.profile_id),0)
 WHERE version_id=0;`)
	return err
}

func seedProfilePresence(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at)
 SELECT q.owner_id,q.profile_id,'available',v.last_seen,v.last_seen
 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id`)
	return err
}

func presenceVersionTx(ctx context.Context, tx *sql.Tx, owner, profile int64) (int64, error) {
	var version int64
	err := tx.QueryRowContext(ctx, "SELECT v.id FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?", owner, profile).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (w *ProfileWatcher) recordProfilePresence(ctx context.Context, owner, profile int64, available bool, observed time.Time) (bool, error) {
	return w.Store.recordProfilePresence(ctx, owner, profile, available, observed)
}

func (s *Store) recordProfilePresence(ctx context.Context, owner, profile int64, available bool, observed time.Time) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = migrateProfilePresenceTx(ctx, tx); err != nil {
		return false, err
	}
	version, err := presenceVersionTx(ctx, tx, owner, profile)
	if err != nil {
		return false, err
	}
	changed, err := recordProfilePresenceTx(ctx, tx, owner, profile, available, "direct_unavailable", version, version, observed)
	if err != nil {
		return false, err
	}
	return changed, tx.Commit()
}

func recordProfilePresenceTx(ctx context.Context, tx *sql.Tx, owner, profile int64, available bool, reason string, baseline, current int64, observed time.Time) (bool, error) {
	availability := "unavailable"
	if available {
		availability = "available"
	}
	at := observed.UTC().Format(time.RFC3339Nano)
	var previous, oldReason, checked string
	var transition, oldBaseline int64
	err := tx.QueryRowContext(ctx, "SELECT availability,transition,reason,baseline_version_id,checked_at FROM profile_presence WHERE owner_id=? AND profile_id=?", owner, profile).Scan(&previous, &transition, &oldReason, &oldBaseline, &checked)
	if errors.Is(err, sql.ErrNoRows) {
		if available {
			reason = ""
			baseline = 0
		}
		if !available && baseline > 0 {
			transition = 1
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at,reason,baseline_version_id) VALUES(?,?,?,?,?,?,?,?)", owner, profile, availability, transition, at, at, reason, baseline)
		if err == nil && transition > 0 {
			_, err = tx.ExecContext(ctx, "INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,reason,old_version_id,version_id) VALUES(?,?,?,?,?,?,?,?)", owner, profile, transition, availability, at, reason, baseline, current)
		}
		return transition > 0 && err == nil, err
	}
	if err != nil {
		return false, err
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05"} {
		if last, err := time.Parse(layout, checked); err == nil {
			if observed.Before(last) {
				return false, nil
			}
			break
		}
	}
	if previous == availability {
		_, err = tx.ExecContext(ctx, "UPDATE profile_presence SET checked_at=? WHERE owner_id=? AND profile_id=?", at, owner, profile)
		return false, err
	}
	oldVersion := baseline
	changes := ""
	eventReason := reason
	if available {
		oldVersion = oldBaseline
		eventReason = oldReason
		changes, err = profilePresenceChangesTx(ctx, tx, owner, profile, oldVersion, current)
		if err != nil {
			return false, err
		}
		reason = ""
		baseline = 0
	}
	if current == 0 {
		current = oldVersion
	}
	transition++
	if _, err = tx.ExecContext(ctx, "UPDATE profile_presence SET availability=?,transition=?,changed_at=?,checked_at=?,reason=?,baseline_version_id=? WHERE owner_id=? AND profile_id=?", availability, transition, at, at, reason, baseline, owner, profile); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,reason,old_version_id,version_id,changes_text) VALUES(?,?,?,?,?,?,?,?,?)", owner, profile, transition, availability, at, eventReason, oldVersion, current, changes)
	return err == nil, err
}

func presenceValue(value any, exists bool) string {
	if !exists {
		return "(поле отсутствует)"
	}
	raw, _ := json.Marshal(value)
	text := string(raw)
	if len([]rune(text)) > 180 {
		text = string([]rune(text)[:179]) + "…"
	}
	return text
}

func profilePresenceChangesTx(ctx context.Context, tx *sql.Tx, owner, profile, oldVersion, newVersion int64) (string, error) {
	if oldVersion == 0 || newVersion == 0 || oldVersion == newVersion {
		return "", nil
	}
	var before, after Profile
	for i, id := range []int64{oldVersion, newVersion} {
		var raw string
		if err := tx.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=? AND owner_id=? AND profile_id=?", id, owner, profile).Scan(&raw); err != nil {
			return "", err
		}
		p, err := decodeProfile([]byte(raw))
		if err != nil {
			return "", err
		}
		if i == 0 {
			before = p
		} else {
			after = p
		}
	}
	keys := map[string]bool{}
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		keys[key] = true
	}
	var sorted []string
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var changes []string
	for _, key := range sorted {
		if key == "distance" || key == "reactionView" || key == "isFire" || key == "fireText" {
			continue
		}
		if key == "photos" || key == "miniPhotos" {
			old, new := before.Photos(key), after.Photos(key)
			if strings.Join(old, "\x00") == strings.Join(new, "\x00") {
				continue
			}
			oldSet, newSet := map[string]bool{}, map[string]bool{}
			for _, v := range old {
				oldSet[v] = true
			}
			for _, v := range new {
				newSet[v] = true
			}
			added, removed := 0, 0
			for v := range newSet {
				if !oldSet[v] {
					added++
				}
			}
			for v := range oldSet {
				if !newSet[v] {
					removed++
				}
			}
			if added == 0 && removed == 0 {
				if len(old) != len(new) {
					changes = append(changes, fmt.Sprintf("%s: количество ссылок %d → %d", key, len(old), len(new)))
				} else {
					changes = append(changes, key+": изменён порядок ссылок")
				}
			} else {
				changes = append(changes, fmt.Sprintf("%s: ссылок добавлено %d, убрано %d", key, added, removed))
			}
			continue
		}
		old, oldExists := before[key]
		new, newExists := after[key]
		oldJSON, _ := json.Marshal(old)
		newJSON, _ := json.Marshal(new)
		if oldExists == newExists && string(oldJSON) == string(newJSON) {
			continue
		}
		changes = append(changes, key+": "+presenceValue(old, oldExists)+" → "+presenceValue(new, newExists))
	}
	return strings.Join(changes, "\n"), nil
}

func (w *ProfileWatcher) recoverProfilePresence(ctx context.Context) error {
	_, err := w.Store.DB.ExecContext(ctx, "UPDATE profile_presence_events SET state='uncertain',error='worker_restarted_during_request',updated_at=CURRENT_TIMESTAMP WHERE state='processing'")
	return err
}

func (w *ProfileWatcher) runProfilePresence(ctx context.Context) {
	for ctx.Err() == nil {
		if _, err := w.deliverProfilePresence(ctx); err != nil && ctx.Err() == nil {
			_ = w.Store.SetMeta(ctx, "profile_presence_last_error", archiveErrorCode(err))
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

// Reuse the sole archive sender and its uncertain-send policy; create no topics.
func (w *ProfileWatcher) deliverProfilePresence(ctx context.Context) (bool, error) {
	a := w.Archiver
	if a == nil || a.TG == nil || a.Config.BackupChatID == 0 || a.Config.ArchivePaused {
		return false, nil
	}
	sendCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !a.mu.TryLock() {
		if !sleep(sendCtx, 25*time.Millisecond) {
			return false, sendCtx.Err()
		}
	}
	defer a.mu.Unlock()

	unlock, locked, lockErr := w.Store.trySyncPublicationLock()
	if lockErr != nil {
		return false, lockErr
	}
	if !locked {
		return false, nil
	}
	defer unlock()
	if err := a.init(sendCtx); err != nil {
		return false, err
	}
	tx, err := w.Store.DB.BeginTx(sendCtx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id, owner, profile, thread int64
	var availability, observed, reason, changes string
	err = tx.QueryRowContext(sendCtx, `SELECT e.id,e.owner_id,e.profile_id,t.thread_id,e.availability,e.observed_at,e.reason,e.changes_text
 FROM profile_presence_events e JOIN mirror_topics t ON t.profile_id=e.profile_id
 WHERE e.state='pending' AND e.next_attempt<=? AND t.thread_id>0 `+w.Store.syncPublicationCandidates("e.owner_id", "e.id", "presence")+`
 AND NOT EXISTS(SELECT 1 FROM profile_presence_events prior WHERE prior.owner_id=e.owner_id AND prior.profile_id=e.profile_id AND prior.id<e.id AND prior.state IN ('pending','processing'))
 AND (e.version_id=0 OR EXISTS(
  SELECT 1 FROM archive_versions av WHERE av.version_id=e.version_id AND av.owner_id=e.owner_id AND av.profile_id=e.profile_id AND av.thread_id=t.thread_id AND (
   (av.state IN ('sent','partial') AND EXISTS(SELECT 1 FROM archive_profile_repairs layout WHERE layout.version_id=av.version_id AND layout.state='sent'))
   OR (av.state='duplicate' AND EXISTS(SELECT 1 FROM archive_versions original JOIN archive_profile_repairs layout ON layout.version_id=original.version_id
       WHERE original.fingerprint=av.fingerprint AND original.profile_id=av.profile_id AND original.thread_id=t.thread_id AND original.state IN ('sent','partial') AND layout.state='sent'))
  )
 ))
 AND NOT EXISTS(SELECT 1 FROM versions v LEFT JOIN archive_versions av ON av.version_id=v.id WHERE v.owner_id=e.owner_id AND v.profile_id=e.profile_id `+w.Store.syncPublicationCandidates("v.owner_id", "v.id", "version")+` AND (
   av.version_id IS NULL OR av.state IN ('pending','processing')
   OR (av.state IN ('sent','partial') AND NOT EXISTS(SELECT 1 FROM archive_profile_repairs layout WHERE layout.version_id=v.id AND layout.state='sent'))
 ))
 ORDER BY e.id LIMIT 1`, w.now().Unix()).Scan(&id, &owner, &profile, &thread, &availability, &observed, &reason, &changes)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	chat := a.Config.BackupChatID
	if _, err = tx.ExecContext(sendCtx, "UPDATE profile_presence_events SET state='processing',chat_id=?,thread_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='pending'", chat, thread, id); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	text := profilePresenceText(profile, availability, observed, reason, changes)

	var message SentMessage
	var lastSendErr error
	attempted := false
	err = a.groupCall(sendCtx, func() error {
		if err := w.Store.requireSyncPublication(sendCtx, owner); err != nil {
			return err
		}
		attempted = true
		message, lastSendErr = a.TG.SendText(sendCtx, chat, thread, text, nil)
		return lastSendErr
	})
	state, detail := "sent", ""
	next := int64(0)
	if err == nil && (message.ID <= 0 || message.Chat.ID != chat || message.ThreadID != thread) {
		err = errors.New("profile presence receipt unconfirmed")
	}
	if err != nil {
		classification := err
		var lastRejected *TelegramError
		if errors.As(lastSendErr, &lastRejected) && lastRejected.Code == 429 {
			// The deadline may expire in the next group-slot wait, after a
			// confirmed rejection and before any further HTTP attempt.
			classification = lastSendErr
		}
		state, detail = "uncertain", archiveErrorCode(classification)
		var rejected *TelegramError
		if !attempted {
			state = "pending"
		} else if errors.As(classification, &rejected) {
			state = "error"
			if rejected.Code == 429 {
				state = "pending"
				next = w.now().Add(max(4*time.Second, time.Duration(rejected.RetryAfter)*time.Second)).Unix()
			}
		}
	}
	// A canceled or ambiguous HTTP send must still get a durable outcome marker.
	writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	_, writeErr := w.Store.DB.ExecContext(writeCtx, "UPDATE profile_presence_events SET state=?,message_id=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", state, message.ID, next, detail, id)
	return true, errors.Join(err, writeErr)
}
