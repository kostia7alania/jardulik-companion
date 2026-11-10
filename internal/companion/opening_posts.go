package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const openingAuditTitle = "💌 Открывашки"
const openingPostsSchema = `
CREATE TABLE IF NOT EXISTS opening_audit_topics (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL, chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 next_attempt INTEGER NOT NULL DEFAULT 0, next_refresh INTEGER NOT NULL DEFAULT 0,
 receipt_json BLOB NOT NULL DEFAULT X'', error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,upstream_id,chat_id)
);
CREATE TABLE IF NOT EXISTS opening_audit_posts (
 topic_id INTEGER NOT NULL REFERENCES opening_audit_topics(id) ON DELETE CASCADE, item_key TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('formula','draft','intent','debug')), profile_id INTEGER NOT NULL DEFAULT 0,
 message_id INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 payload_json BLOB NOT NULL, desired_sha TEXT NOT NULL, sent_sha TEXT NOT NULL DEFAULT '',
 source_json BLOB NOT NULL, source_sha TEXT NOT NULL, receipt_json BLOB NOT NULL DEFAULT X'',
 next_attempt INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(topic_id,item_key)
);`

type openingPostPayload struct {
	Text     string          `json:"text"`
	Entities []MessageEntity `json:"entities"`
}
type openingAuditTopic struct {
	ID, Owner, Upstream, Chat, Thread int64
	Generation, State                 string
	NextRefresh                       int64
}
type openingPostItem struct {
	Key, Kind string
	Profile   int64
	Payload   openingPostPayload
	Source    []byte
}
type openingPostOperation struct {
	Topic   openingAuditTopic
	Item    openingPostItem
	Message int64
	SHA     string
}

var errOpeningPostBinding = errors.New("opening audit source or receipt changed")
var errOpeningPostReceipt = errors.New("opening audit Telegram receipt unconfirmed")
var errOpeningPostSource = errors.New("opening audit source unavailable")

func (s *Service) openingPostsAllowed() bool {
	return s.Archiver != nil && s.Archiver.TG != nil && s.Config.OwnerID > 0 && s.Config.OwnerID == s.Archiver.Config.OwnerID && s.Config.BackupChatID != 0 && s.Config.BackupChatID == s.Archiver.Config.BackupChatID && s.Config.BackupThreadID == s.Archiver.Config.BackupThreadID && !s.Config.ArchivePaused && !s.Archiver.Config.ArchivePaused
}

func (s *Service) initOpeningPosts(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, openingPostsSchema)
	return err
}

// RunOpeningPosts publishes only saved operator data; it never sends a reaction,
// calls Jardulik/LLM, pins topics, or changes the message-selection cursor.
func (s *Service) RunOpeningPosts(ctx context.Context) {
	if !s.openingPostsAllowed() {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "opening-posts.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = s.initOpeningPosts(ctx); err != nil {
		return
	}
	if err = s.recoverOpeningPosts(ctx); err != nil {
		return
	}
	for ctx.Err() == nil {
		_, err = s.deliverOpeningPost(ctx)
		delay := 2 * time.Second
		if err != nil {
			_ = s.Store.SetMeta(ctx, "opening_posts_last_error", archiveErrorCode(err))
			delay = 5 * time.Second
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}

func (s *Service) recoverOpeningPosts(ctx context.Context) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_posts'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE opening_audit_topics SET state=CASE state WHEN 'sending' THEN 'uncertain' ELSE 'pending' END,
 error=CASE state WHEN 'sending' THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE owner_id=? AND state IN ('preparing','sending')`, s.Config.OwnerID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE opening_audit_posts SET state=CASE WHEN state='sending' AND message_id=0 THEN 'uncertain' ELSE 'pending' END,
 error=CASE WHEN state='sending' AND message_id=0 THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE topic_id IN (SELECT id FROM opening_audit_topics WHERE owner_id=?) AND state IN ('preparing','sending')`, s.Config.OwnerID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) openingPostBindingTx(ctx context.Context, tx *sql.Tx, expected Session) error {
	if !s.openingPostsAllowed() || expected.UserID <= 0 || expected.Generation == "" {
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

const openingTopicColumns = "id,owner_id,upstream_id,generation,chat_id,thread_id,state,next_refresh"

func scanOpeningTopic(row interface{ Scan(...any) error }) (openingAuditTopic, error) {
	var t openingAuditTopic
	err := row.Scan(&t.ID, &t.Owner, &t.Upstream, &t.Generation, &t.Chat, &t.Thread, &t.State, &t.NextRefresh)
	return t, err
}

func (s *Service) refreshOpeningPosts(ctx context.Context, expected Session) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_posts'"); err != nil {
		return err
	}
	if err = s.openingPostBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO opening_audit_topics(owner_id,upstream_id,generation,chat_id) VALUES(?,?,?,?)
 ON CONFLICT(owner_id,upstream_id,chat_id) DO UPDATE SET generation=excluded.generation,next_refresh=0,updated_at=CURRENT_TIMESTAMP
 WHERE opening_audit_topics.generation<>excluded.generation`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID); err != nil {
		return err
	}
	topic, err := scanOpeningTopic(tx.QueryRowContext(ctx, "SELECT "+openingTopicColumns+" FROM opening_audit_topics WHERE owner_id=? AND upstream_id=? AND chat_id=?", s.Config.OwnerID, expected.UserID, s.Config.BackupChatID))
	if err != nil {
		return err
	}
	if topic.NextRefresh > s.Archiver.now().UnixMilli() {
		return tx.Commit()
	}
	keys, err := openingPostKeysTx(ctx, tx, topic.Owner, topic.Upstream)
	if err != nil {
		return err
	}
	for _, key := range keys {
		item, err := s.openingPostItemTx(ctx, tx, expected, key)
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errOpeningPostSource) {
			if _, e := tx.ExecContext(ctx, "UPDATE opening_audit_posts SET state='error',error='source_unavailable',updated_at=CURRENT_TIMESTAMP WHERE topic_id=? AND item_key=? AND NOT(state IN ('uncertain','sending') AND message_id=0)", topic.ID, key); e != nil {
				return e
			}
			continue
		}
		if err != nil {
			return err
		}
		raw, err := openingPostBytes(item.Payload)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO opening_audit_posts(topic_id,item_key,kind,profile_id,payload_json,desired_sha,source_json,source_sha) VALUES(?,?,?,?,?,?,?,?)
 ON CONFLICT(topic_id,item_key) DO UPDATE SET kind=excluded.kind,profile_id=excluded.profile_id,payload_json=excluded.payload_json,desired_sha=excluded.desired_sha,
 source_json=excluded.source_json,source_sha=excluded.source_sha,state=CASE WHEN opening_audit_posts.sent_sha=excluded.desired_sha THEN 'sent' ELSE 'pending' END,
 next_attempt=0,error='',updated_at=CURRENT_TIMESTAMP
 WHERE (opening_audit_posts.desired_sha<>excluded.desired_sha OR opening_audit_posts.source_sha<>excluded.source_sha OR (opening_audit_posts.state='error' AND opening_audit_posts.error='source_unavailable'))
 AND NOT(opening_audit_posts.state IN ('uncertain','sending') AND opening_audit_posts.message_id=0)`, topic.ID, item.Key, item.Kind, item.Profile, raw, digest(raw), item.Source, digest(item.Source))
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE opening_audit_topics SET next_refresh=? WHERE id=?", s.Archiver.now().Add(time.Minute).UnixMilli(), topic.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func openingPostBytes(payload openingPostPayload) ([]byte, error) {
	if strings.TrimSpace(payload.Text) == "" || textUnits(payload.Text) > 4096 {
		return nil, errors.New("invalid opening audit text length")
	}
	if err := validMessageEntities(payload.Text, payload.Entities); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

func (s *Service) claimOpeningPost(ctx context.Context, expected Session) (openingPostOperation, bool, error) {
	var op openingPostOperation
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return op, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_posts'"); err != nil {
		return op, false, err
	}
	if err = s.openingPostBindingTx(ctx, tx, expected); err != nil {
		return op, false, err
	}
	op.Topic, err = scanOpeningTopic(tx.QueryRowContext(ctx, "SELECT "+openingTopicColumns+` FROM opening_audit_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND state='pending' AND thread_id=0 AND next_attempt<=? ORDER BY id LIMIT 1`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, s.Archiver.now().UnixMilli()))
	if err == nil {
		_, err = tx.ExecContext(ctx, "UPDATE opening_audit_topics SET state='preparing',error='' WHERE id=? AND state='pending'", op.Topic.ID)
		if err != nil {
			return op, false, err
		}
		return op, true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return op, false, err
	}
	var topic int64
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT p.topic_id,p.item_key,p.kind,p.profile_id,p.message_id,p.payload_json,p.desired_sha,p.source_json FROM opening_audit_posts p JOIN opening_audit_topics t ON t.id=p.topic_id
 WHERE t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=? AND t.state='sent' AND t.thread_id>0 AND p.state='pending' AND p.next_attempt<=?
 ORDER BY CASE p.kind WHEN 'formula' THEN 0 WHEN 'intent' THEN 1 WHEN 'debug' THEN 2 ELSE 3 END,p.profile_id,p.item_key LIMIT 1`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, s.Archiver.now().UnixMilli()).Scan(&topic, &op.Item.Key, &op.Item.Kind, &op.Item.Profile, &op.Message, &raw, &op.SHA, &op.Item.Source)
	if errors.Is(err, sql.ErrNoRows) {
		return op, false, tx.Commit()
	}
	if err != nil {
		return op, false, err
	}
	op.Topic, err = scanOpeningTopic(tx.QueryRowContext(ctx, "SELECT "+openingTopicColumns+" FROM opening_audit_topics WHERE id=?", topic))
	if err != nil {
		return op, false, err
	}
	if json.Unmarshal(raw, &op.Item.Payload) != nil || digest(raw) != op.SHA {
		return op, false, errors.New("invalid saved opening audit payload")
	}
	if _, err = openingPostBytes(op.Item.Payload); err != nil {
		return op, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE opening_audit_posts SET state='preparing',error='' WHERE topic_id=? AND item_key=? AND state='pending'", topic, op.Item.Key); err != nil {
		return op, false, err
	}
	return op, true, tx.Commit()
}

func (s *Service) markOpeningPostSending(ctx context.Context, op openingPostOperation, expected Session) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='opening_posts'"); err != nil {
		return err
	}
	if err = s.openingPostBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	var allowed bool
	invalidSource := false
	if op.Item.Key == "" {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM opening_audit_topics WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND thread_id=0 AND state IN ('preparing','sending'))`, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation, op.Topic.Chat).Scan(&allowed)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM opening_audit_posts p JOIN opening_audit_topics t ON t.id=p.topic_id WHERE t.id=? AND t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=? AND t.thread_id=? AND t.state='sent' AND p.item_key=? AND p.message_id=? AND p.desired_sha=? AND p.state IN ('preparing','sending'))`, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation, op.Topic.Chat, op.Topic.Thread, op.Item.Key, op.Message, op.SHA).Scan(&allowed)
		if err == nil && allowed {
			var item openingPostItem
			item, err = s.openingPostItemTx(ctx, tx, expected, op.Item.Key)
			invalidSource = errors.Is(err, sql.ErrNoRows) || errors.Is(err, errOpeningPostSource)
			if err == nil {
				var raw []byte
				raw, err = openingPostBytes(item.Payload)
				allowed = err == nil && digest(raw) == op.SHA && digest(item.Source) == digest(op.Item.Source)
			}
		}
	}
	if err != nil || !allowed {
		if _, e := tx.ExecContext(ctx, "UPDATE opening_audit_topics SET next_refresh=0 WHERE id=?", op.Topic.ID); e != nil {
			return e
		}
		if e := tx.Commit(); e != nil {
			return e
		}
		if invalidSource {
			return errOpeningPostSource
		}
		return errOpeningPostBinding
	}
	if op.Item.Key == "" {
		_, err = tx.ExecContext(ctx, "UPDATE opening_audit_topics SET state='sending' WHERE id=?", op.Topic.ID)
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE opening_audit_posts SET state='sending' WHERE topic_id=? AND item_key=?", op.Topic.ID, op.Item.Key)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func openingPostFailure(err, last error, attempted, editing bool, now time.Time) (string, string, int64) {
	if errors.Is(err, errOpeningPostSource) {
		return "error", "source_unavailable", 0
	}
	classification := err
	var rejected *TelegramError
	if errors.As(last, &rejected) && rejected.Code == 429 {
		classification = last
	}
	if !attempted {
		return "pending", archiveErrorCode(classification), now.Add(5 * time.Second).UnixMilli()
	}
	if errors.As(classification, &rejected) {
		if rejected.Code == 429 {
			return "pending", archiveErrorCode(classification), now.Add(max(4*time.Second, time.Duration(rejected.RetryAfter)*time.Second)).UnixMilli()
		}
		if rejected.Code < 500 {
			return "error", archiveErrorCode(classification), 0
		}
	}
	if errors.Is(classification, errOpeningPostReceipt) || !editing {
		return "uncertain", archiveErrorCode(classification), 0
	}
	return "pending", archiveErrorCode(classification), now.Add(5 * time.Second).UnixMilli()
}

func (s *Service) finishOpeningPost(ctx context.Context, op openingPostOperation, state string, next int64, detail string, receipt []byte, thread, message int64) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if receipt == nil {
		receipt = []byte{}
	}
	var result sql.Result
	var err error
	if op.Item.Key == "" {
		result, err = s.Store.DB.ExecContext(write, `UPDATE opening_audit_topics SET state=?,thread_id=CASE WHEN ?>0 THEN ? ELSE thread_id END,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND state IN ('preparing','sending')`, state, thread, thread, receipt, next, detail, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation)
	} else {
		result, err = s.Store.DB.ExecContext(write, `UPDATE opening_audit_posts SET state=?,message_id=CASE WHEN ?>0 THEN ? ELSE message_id END,sent_sha=CASE WHEN ?='sent' THEN ? ELSE sent_sha END,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE topic_id=? AND item_key=? AND desired_sha=? AND state IN ('preparing','sending')`, state, message, message, state, op.SHA, receipt, next, detail, op.Topic.ID, op.Item.Key, op.SHA)
	}
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("opening audit receipt not committed")
	}
	return nil
}

func (s *Service) openingTopicCollision(ctx context.Context, topic openingAuditTopic, thread int64) (bool, error) {
	if thread <= 0 || thread == s.Config.BackupThreadID {
		return true, nil
	}
	var collision bool
	if err := s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM mirror_topics WHERE thread_id=? UNION ALL SELECT 1 FROM opening_audit_topics WHERE chat_id=? AND thread_id=? AND id<>?)`, thread, topic.Chat, thread, topic.ID).Scan(&collision); err != nil || collision {
		return collision, err
	}
	present, err := snapshotHasTable(ctx, s.Store.DB, "profile_index_topics")
	if err != nil || !present {
		return false, err
	}
	err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM profile_index_topics WHERE chat_id=? AND thread_id=?)", topic.Chat, thread).Scan(&collision)
	return collision, err
}

func (s *Service) deliverOpeningPost(ctx context.Context) (bool, error) {
	if !s.openingPostsAllowed() {
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
	if err := s.initOpeningPosts(send); err != nil {
		return false, err
	}
	expected, err := s.Store.Session(send, s.Config.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || expected.UserID <= 0 || expected.Generation == "" {
		return false, err
	}
	if err = s.refreshOpeningPosts(send, expected); err != nil {
		return false, err
	}
	op, claimed, err := s.claimOpeningPost(send, expected)
	if err != nil || !claimed {
		return false, err
	}
	attempted := false
	var last error
	var receipt []byte
	var thread, message int64
	err = a.groupCall(send, func() error {
		if e := s.markOpeningPostSending(send, op, expected); e != nil {
			return e
		}
		attempted = true
		if op.Item.Key == "" {
			var result struct {
				Thread int64 `json:"message_thread_id"`
			}
			last = a.TG.Call(send, "createForumTopic", map[string]any{"chat_id": op.Topic.Chat, "name": openingAuditTitle}, &result)
			if last == nil {
				thread = result.Thread
				receipt, _ = json.Marshal(result)
			}
			return last
		}
		var result SentMessage
		if op.Message == 0 {
			result, last = a.TG.SendText(send, op.Topic.Chat, op.Topic.Thread, op.Item.Payload.Text, nil, op.Item.Payload.Entities)
		} else {
			last = a.TG.Call(send, "editMessageText", map[string]any{"chat_id": op.Topic.Chat, "message_id": op.Message, "text": op.Item.Payload.Text, "entities": op.Item.Payload.Entities, "link_preview_options": map[string]bool{"is_disabled": true}}, &result)
			var te *TelegramError
			if errors.As(last, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
				message = op.Message
				receipt, _ = json.Marshal(map[string]any{"not_modified": true, "message_id": op.Message})
				last = nil
				return nil
			}
		}
		if last == nil {
			receipt, _ = json.Marshal(result)
			if result.ID <= 0 || result.Chat.ID != op.Topic.Chat || result.ThreadID != op.Topic.Thread || op.Message > 0 && result.ID != op.Message {
				return errOpeningPostReceipt
			}
			message = result.ID
		}
		return last
	})
	if err == nil && op.Item.Key == "" {
		collision, e := s.openingTopicCollision(send, op.Topic, thread)
		if e != nil {
			err = e
		} else if collision {
			err = errOpeningPostReceipt
		}
	}
	state, detail, next := "sent", "", int64(0)
	if err != nil {
		state, detail, next = openingPostFailure(err, last, attempted, op.Message > 0, a.now())
		if op.Item.Key == "" {
			thread = 0
		}
		message = op.Message
	}
	return true, errors.Join(err, s.finishOpeningPost(ctx, op, state, next, detail, receipt, thread, message))
}
