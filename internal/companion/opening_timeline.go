package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const openingTimelineSchema = `CREATE TABLE IF NOT EXISTS opening_timeline_posts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, action_id INTEGER NOT NULL REFERENCES actions(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL,
 attempted_at_ms INTEGER NOT NULL, source_json BLOB NOT NULL, source_sha TEXT NOT NULL,
 payload_json BLOB NOT NULL, payload_sha TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 message_id INTEGER NOT NULL DEFAULT 0, receipt_json BLOB NOT NULL DEFAULT X'', next_attempt INTEGER NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,upstream_id,action_id,chat_id)
);`

type openingTimelineSource struct {
	Action, Owner, Upstream, Profile, Reaction, Attempted int64
	Generation, Message                                   string
	Request, Response                                     []byte
}

type openingTimelineOperation struct {
	ID, Chat, Thread int64
	Source           openingTimelineSource
	SourceSHA, SHA   string
	Payload          openingPostPayload
}

func (s *Service) openingTimelineAllowed() bool {
	return s.Store != nil && s.Archiver != nil && s.Archiver.TG != nil && s.Config.OwnerID > 0 &&
		s.Config.OwnerID == s.Archiver.Config.OwnerID && s.Config.BackupChatID != 0 &&
		s.Config.BackupChatID == s.Archiver.Config.BackupChatID && s.Config.BackupThreadID == s.Archiver.Config.BackupThreadID
}

func (s *Service) initOpeningTimeline(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, openingTimelineSchema)
	return err
}

// RunOpeningTimeline records confirmed historical sends. It never sends reactions,
// regenerates openers, or contacts Jardulik. Collection/publication phases do not gate these facts.
func (s *Service) RunOpeningTimeline(ctx context.Context) {
	if !s.openingTimelineAllowed() {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "opening-timeline.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = s.initOpeningTimeline(ctx); err == nil {
		err = s.recoverOpeningTimeline(ctx)
	}
	if err != nil {
		_ = s.Store.SetMeta(ctx, "opening_timeline_last_error", archiveErrorCode(err))
		return
	}
	for ctx.Err() == nil {
		_, err = s.deliverOpeningTimeline(ctx)
		delay := 2 * time.Second
		if err != nil {
			_ = s.Store.SetMeta(ctx, "opening_timeline_last_error", archiveErrorCode(err))
			delay = 5 * time.Second
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}

func (s *Service) recoverOpeningTimeline(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, `UPDATE opening_timeline_posts
 SET state=CASE state WHEN 'sending' THEN 'uncertain' ELSE 'pending' END,
 error=CASE state WHEN 'sending' THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE owner_id=? AND chat_id=? AND state IN ('preparing','sending')`, s.Config.OwnerID, s.Config.BackupChatID)
	return err
}

func (s *Service) openingTimelineBindingTx(ctx context.Context, tx *sql.Tx, expected Session) error {
	if !s.openingTimelineAllowed() || expected.UserID <= 0 || expected.Generation == "" {
		return errOpeningPostBinding
	}
	current, err := s.Store.businessSession(ctx, tx, s.Config.OwnerID)
	if err != nil {
		return err
	}
	if current.UserID != expected.UserID || current.Generation != expected.Generation {
		return errOpeningPostBinding
	}
	return nil
}

const openingTimelineSourceColumns = `i.action_id,i.owner_id,i.upstream_id,i.profile_id,i.reaction_type,i.attempted_at_ms,i.generation,i.message,i.request_body,a.response`
const openingTimelineSourceJoin = ` FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id AND a.owner_id=i.owner_id AND a.profile_id=i.profile_id
 WHERE i.owner_id=? AND i.upstream_id=? AND a.kind='Like' AND a.state='succeeded'
 AND i.message<>'' AND i.attempted_at_ms>0 AND i.reaction_type IN (1,3)`

func scanOpeningTimelineSource(row interface{ Scan(...any) error }) (openingTimelineSource, error) {
	var source openingTimelineSource
	err := row.Scan(&source.Action, &source.Owner, &source.Upstream, &source.Profile, &source.Reaction, &source.Attempted, &source.Generation, &source.Message, &source.Request, &source.Response)
	return source, err
}

func openingTimelineSourceValid(source openingTimelineSource) bool {
	if source.Action <= 0 || source.Owner <= 0 || source.Upstream <= 0 || source.Profile <= 0 || source.Attempted <= 0 ||
		!openingFrozenRequest(source.Request, source.Upstream, source.Profile, source.Reaction, source.Message) {
		return false
	}
	result, err := decodeReactionResult(source.Response, time.UnixMilli(source.Attempted))
	return err == nil && result.Decision == ReactionSuccess
}

func openingTimelinePayload(source openingTimelineSource) openingPostPayload {
	reaction := "❤️"
	if source.Reaction == 3 {
		reaction = "🔥"
	}
	header := "💌 Отправлено · " + reaction
	prefix := header + "\n" + displayTime(time.UnixMilli(source.Attempted).UTC().Format(time.RFC3339)) + "\n\n"
	return openingPostPayload{Text: prefix + source.Message, Entities: []MessageEntity{
		{Type: "bold", Offset: 0, Length: textUnits(header)},
		{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(source.Message)},
	}}
}

func (s *Service) openingTimelineThreadTx(ctx context.Context, tx *sql.Tx, profile int64) (int64, error) {
	var thread int64
	err := tx.QueryRowContext(ctx, `SELECT thread_id FROM (`+archiveChangePublishedSQL("?", "?", "?")+`)
 WHERE profile_id=? ORDER BY julianday(sent_at) DESC,version_id DESC LIMIT 1`, s.Config.BackupChatID, s.Config.OwnerID, s.Config.BackupThreadID, profile).Scan(&thread)
	return thread, err
}

func (s *Service) seedOpeningTimelineTx(ctx context.Context, tx *sql.Tx, expected Session) error {
	for _, table := range []string{"reaction_message_intents", "archive_versions", "archive_profile_repairs", "archive_receipts", "archive_zip_repairs", "mirror_topics"} {
		present, err := openingTableTx(ctx, tx, table)
		if err != nil || !present {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+openingTimelineSourceColumns+openingTimelineSourceJoin+`
 AND NOT EXISTS(SELECT 1 FROM opening_timeline_posts p WHERE p.owner_id=i.owner_id AND p.upstream_id=i.upstream_id AND p.action_id=i.action_id AND p.chat_id=?)
 ORDER BY i.attempted_at_ms,i.action_id`, s.Config.OwnerID, expected.UserID, s.Config.BackupChatID)
	if err != nil {
		return err
	}
	var sources []openingTimelineSource
	for rows.Next() {
		source, e := scanOpeningTimelineSource(rows)
		if e != nil {
			rows.Close()
			return e
		}
		sources = append(sources, source)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for _, source := range sources {
		if !openingTimelineSourceValid(source) {
			continue
		}
		thread, e := s.openingTimelineThreadTx(ctx, tx, source.Profile)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return e
		}
		raw, e := json.Marshal(source)
		if e != nil {
			return e
		}
		payload, e := openingPostBytes(openingTimelinePayload(source))
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO opening_timeline_posts(owner_id,upstream_id,action_id,profile_id,chat_id,thread_id,attempted_at_ms,source_json,source_sha,payload_json,payload_sha) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, source.Owner, source.Upstream, source.Action, source.Profile, s.Config.BackupChatID, thread, source.Attempted, raw, digest(raw), payload, digest(payload))
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *Service) claimOpeningTimeline(ctx context.Context, expected Session) (openingTimelineOperation, bool, error) {
	var op openingTimelineOperation
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return op, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_timeline'"); err != nil {
		return op, false, err
	}
	if err = s.openingTimelineBindingTx(ctx, tx, expected); err != nil {
		return op, false, err
	}
	if err = s.seedOpeningTimelineTx(ctx, tx, expected); err != nil {
		return op, false, err
	}
	var source, payload []byte
	err = tx.QueryRowContext(ctx, `SELECT id,chat_id,thread_id,source_json,source_sha,payload_json,payload_sha FROM opening_timeline_posts
 WHERE owner_id=? AND upstream_id=? AND chat_id=? AND state='pending' AND next_attempt<=?
 ORDER BY attempted_at_ms,action_id LIMIT 1`, s.Config.OwnerID, expected.UserID, s.Config.BackupChatID, s.Archiver.now().UnixMilli()).Scan(&op.ID, &op.Chat, &op.Thread, &source, &op.SourceSHA, &payload, &op.SHA)
	if errors.Is(err, sql.ErrNoRows) {
		return op, false, tx.Commit()
	}
	if err != nil {
		return op, false, err
	}
	if json.Unmarshal(source, &op.Source) != nil || json.Unmarshal(payload, &op.Payload) != nil || digest(source) != op.SourceSHA || digest(payload) != op.SHA || !openingTimelineSourceValid(op.Source) {
		return op, false, errOpeningPostSource
	}
	if _, err = openingPostBytes(op.Payload); err != nil {
		return op, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE opening_timeline_posts SET state='preparing',error='' WHERE id=? AND state='pending'", op.ID); err != nil {
		return op, false, err
	}
	return op, true, tx.Commit()
}

func (s *Service) markOpeningTimelineSending(ctx context.Context, op openingTimelineOperation, expected Session) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_timeline'"); err != nil {
		return err
	}
	if err = s.openingTimelineBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	var allowed bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM opening_timeline_posts WHERE id=? AND owner_id=? AND upstream_id=? AND action_id=? AND profile_id=? AND chat_id=? AND thread_id=? AND source_sha=? AND payload_sha=? AND message_id=0 AND state IN ('preparing','sending'))`, op.ID, s.Config.OwnerID, expected.UserID, op.Source.Action, op.Source.Profile, s.Config.BackupChatID, op.Thread, op.SourceSHA, op.SHA).Scan(&allowed)
	if err != nil {
		return err
	}
	source, err := scanOpeningTimelineSource(tx.QueryRowContext(ctx, `SELECT `+openingTimelineSourceColumns+openingTimelineSourceJoin+` AND i.action_id=?`, s.Config.OwnerID, expected.UserID, op.Source.Action))
	if errors.Is(err, sql.ErrNoRows) {
		return errOpeningPostSource
	}
	if err != nil {
		return err
	}
	thread, err := s.openingTimelineThreadTx(ctx, tx, op.Source.Profile)
	if errors.Is(err, sql.ErrNoRows) {
		return errOpeningPostSource
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	payload, err := openingPostBytes(openingTimelinePayload(source))
	if err != nil {
		return err
	}
	if !allowed || thread != op.Thread || !openingTimelineSourceValid(source) || digest(raw) != op.SourceSHA || digest(payload) != op.SHA {
		return errOpeningPostSource
	}
	if _, err = tx.ExecContext(ctx, "UPDATE opening_timeline_posts SET state='sending' WHERE id=?", op.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) finishOpeningTimeline(ctx context.Context, op openingTimelineOperation, state string, next int64, detail string, receipt []byte, message int64) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if receipt == nil {
		receipt = []byte{}
	}
	result, err := s.Store.DB.ExecContext(write, `UPDATE opening_timeline_posts SET state=?,message_id=?,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP
 WHERE id=? AND owner_id=? AND upstream_id=? AND source_sha=? AND payload_sha=? AND state IN ('preparing','sending')`, state, message, receipt, next, detail, op.ID, op.Source.Owner, op.Source.Upstream, op.SourceSHA, op.SHA)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("opening timeline receipt not committed")
	}
	return err
}

func (s *Service) deliverOpeningTimeline(ctx context.Context) (bool, error) {
	if !s.openingTimelineAllowed() {
		return false, nil
	}
	a := s.Archiver
	send, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !a.mu.TryLock() {
		if !sleep(send, 25*time.Millisecond) {
			return false, send.Err()
		}
	}
	defer a.mu.Unlock()
	if err := s.initOpeningTimeline(send); err != nil {
		return false, err
	}
	expected, err := s.Store.Session(send, s.Config.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || expected.UserID <= 0 || expected.Generation == "" {
		return false, err
	}
	op, claimed, err := s.claimOpeningTimeline(send, expected)
	if err != nil || !claimed {
		return false, err
	}
	attempted := false
	var last error
	var receipt []byte
	var message int64
	err = a.groupCall(send, func() error {
		if e := s.markOpeningTimelineSending(send, op, expected); e != nil {
			return e
		}
		attempted = true
		var result SentMessage
		result, last = a.TG.SendText(send, op.Chat, op.Thread, op.Payload.Text, nil, op.Payload.Entities)
		if last == nil {
			receipt, _ = json.Marshal(result)
			if result.ID <= 0 || result.Chat.ID != op.Chat || result.ThreadID != op.Thread {
				return errOpeningPostReceipt
			}
			message = result.ID
		}
		return last
	})
	state, detail, next := "sent", "", int64(0)
	if err != nil {
		state, detail, next = openingPostFailure(err, last, attempted, false, a.now())
	}
	return true, errors.Join(err, s.finishOpeningTimeline(ctx, op, state, next, detail, receipt, message))
}
