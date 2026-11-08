package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const profilePresenceSchema = `
CREATE TABLE IF NOT EXISTS profile_presence (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, availability TEXT NOT NULL CHECK(availability IN ('available','unavailable')),
 transition INTEGER NOT NULL DEFAULT 0, changed_at TEXT NOT NULL, checked_at TEXT NOT NULL,
 PRIMARY KEY(owner_id,profile_id)
);
CREATE TABLE IF NOT EXISTS profile_presence_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, transition INTEGER NOT NULL,
 availability TEXT NOT NULL CHECK(availability IN ('available','unavailable')), observed_at TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', next_attempt INTEGER NOT NULL DEFAULT 0,
 chat_id INTEGER NOT NULL DEFAULT 0, thread_id INTEGER NOT NULL DEFAULT 0,
 message_id INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,profile_id,transition),
 FOREIGN KEY(owner_id,profile_id) REFERENCES profile_presence(owner_id,profile_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS profile_presence_pending ON profile_presence_events(state,next_attempt,id);
`

func seedProfilePresence(ctx context.Context, tx *sql.Tx) error {
	// A previously archived profile is evidence that this viewer could see it.
	// It also lets a first missing check produce a transition without an initial
	// "available" notification for every existing archive.
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at)
 SELECT q.owner_id,q.profile_id,'available',v.last_seen,v.last_seen
 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id`)
	return err
}

func (w *ProfileWatcher) recordProfilePresence(ctx context.Context, owner, profile int64, available bool, observed time.Time) (bool, error) {
	availability := "unavailable"
	if available {
		availability = "available"
	}
	at := observed.UTC().Format(time.RFC3339Nano)
	tx, err := w.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var previous string
	var transition int64
	err = tx.QueryRowContext(ctx, "SELECT availability,transition FROM profile_presence WHERE owner_id=? AND profile_id=?", owner, profile).Scan(&previous, &transition)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, "INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(?,?,?,?,?)", owner, profile, availability, at, at)
		if err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if previous == availability {
		if _, err = tx.ExecContext(ctx, "UPDATE profile_presence SET checked_at=? WHERE owner_id=? AND profile_id=?", at, owner, profile); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	transition++
	if _, err = tx.ExecContext(ctx, "UPDATE profile_presence SET availability=?,transition=?,changed_at=?,checked_at=? WHERE owner_id=? AND profile_id=?", availability, transition, at, at, owner, profile); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at) VALUES(?,?,?,?,?)", owner, profile, transition, availability, at); err != nil {
		return false, err
	}
	return true, tx.Commit()
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

// Delivery has its own worker, so a busy archive cannot stall daily HTTP checks.
// It shares the actual Archiver mutex and persistent group gate with all other
// sends to this private group. No topic is ever created by this worker.
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
	tx, err := w.Store.DB.BeginTx(sendCtx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id, profile, thread int64
	var availability, observed string
	err = tx.QueryRowContext(sendCtx, `SELECT e.id,e.profile_id,t.thread_id,e.availability,e.observed_at
 FROM profile_presence_events e JOIN mirror_topics t ON t.profile_id=e.profile_id
 WHERE e.state='pending' AND e.next_attempt<=? AND t.thread_id>0 ORDER BY e.id LIMIT 1`, w.now().Unix()).Scan(&id, &profile, &thread, &availability, &observed)
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
	title := "Анкета недоступна"
	if availability == "available" {
		title = "Анкета снова доступна"
	}
	text := fmt.Sprintf("%s\nJardulik: %d\n%s", title, profile, displayTime(observed))
	var message SentMessage
	var lastSendErr error
	attempted := false
	err = a.groupCall(sendCtx, func() error {
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
