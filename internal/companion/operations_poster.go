package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const operationsTitle = "🖥 Сервер · логи"
const operationsChannelSchema = `CREATE TABLE IF NOT EXISTS operations_channel (
 owner_id INTEGER NOT NULL,chat_id INTEGER NOT NULL,thread_id INTEGER NOT NULL DEFAULT 0,
 topic_state TEXT NOT NULL DEFAULT 'pending',next_attempt INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',topic_receipt BLOB NOT NULL DEFAULT X'',
 status_message_id INTEGER NOT NULL DEFAULT 0,status_state TEXT NOT NULL DEFAULT 'pending',status_sha TEXT NOT NULL DEFAULT '',
 status_next INTEGER NOT NULL DEFAULT 0,status_receipt BLOB NOT NULL DEFAULT X'',status_error TEXT NOT NULL DEFAULT '',status_pinned INTEGER NOT NULL DEFAULT 0,pin_error TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(owner_id,chat_id)
);`

type operationsOperation struct {
	Kind, Text, SHA                                         string
	Owner, Chat, Thread, Message, Event, Count, First, Last int64
	Role, Level, Detail                                     string
}

var errOperationsReceipt = errors.New("operations Telegram receipt unconfirmed")

func (s *Service) operationsAllowed() bool {
	return s.Archiver != nil && s.Archiver.TG != nil && s.Config.OwnerID > 0 && s.Config.OwnerID == s.Archiver.Config.OwnerID && s.Config.BackupChatID == s.Archiver.Config.BackupChatID && forumTopicURL(s.Config.BackupChatID, 1) != ""
}

func (s *Service) initOperationsChannel(ctx context.Context) error {
	if err := s.Store.InitOperations(ctx); err != nil {
		return err
	}
	if _, err := s.Store.DB.ExecContext(ctx, operationsChannelSchema); err != nil {
		return err
	}
	_, err := s.Store.DB.ExecContext(ctx, `INSERT OR IGNORE INTO operations_channel(owner_id,chat_id) VALUES(?,?)`, s.Config.OwnerID, s.Config.BackupChatID)
	return err
}

func (s *Service) recoverOperations(ctx context.Context) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE operations_channel SET topic_state=CASE WHEN topic_state='sending' THEN 'uncertain' ELSE 'pending' END,error=CASE WHEN topic_state='sending' THEN 'worker_restarted_during_request' ELSE '' END WHERE owner_id=? AND chat_id=? AND topic_state IN ('preparing','sending')`, s.Config.OwnerID, s.Config.BackupChatID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE operations_channel SET status_state=CASE WHEN status_state='sending' AND status_message_id=0 THEN 'uncertain' ELSE 'pending' END,status_error=CASE WHEN status_state='sending' AND status_message_id=0 THEN 'worker_restarted_during_request' ELSE '' END WHERE owner_id=? AND chat_id=? AND status_state IN ('preparing','sending')`, s.Config.OwnerID, s.Config.BackupChatID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE operations_events SET state=CASE WHEN state='sending' AND message_id=0 THEN 'uncertain' ELSE 'pending' END,error=CASE WHEN state='sending' AND message_id=0 THEN 'worker_restarted_during_request' ELSE '' END WHERE owner_id=? AND chat_id=? AND state IN ('preparing','sending')`, s.Config.OwnerID, s.Config.BackupChatID); err != nil {
		return err
	}
	return tx.Commit()
}

// The monitor stays alive when source sessions or archival publication are paused.
func (s *Service) RunOperations(ctx context.Context) {
	if !s.operationsAllowed() {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "operations-poster.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	// An initialization failure must not silently kill the monitoring goroutine.
	for ctx.Err() == nil {
		if err = s.initOperationsChannel(ctx); err == nil {
			err = s.recoverOperations(ctx)
		}
		if err == nil {
			break
		}
		if !sleep(ctx, 5*time.Second) {
			return
		}
	}
	lastCollect := time.Time{}
	for ctx.Err() == nil {
		now := time.Now()
		if now.Sub(lastCollect) >= 10*time.Second {
			if err = s.collectOperationsLogs(ctx); err != nil {
				_ = s.Store.SetMeta(ctx, "operations_last_error", archiveErrorCode(err))
			}
			lastCollect = now
		}
		_, err = s.deliverOperations(ctx)
		delay := time.Second
		if err != nil {
			_ = s.Store.SetMeta(ctx, "operations_last_error", archiveErrorCode(err))
			delay = 5 * time.Second
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}

func (s *Service) claimOperations(ctx context.Context) (operationsOperation, bool, error) {
	op := operationsOperation{Owner: s.Config.OwnerID, Chat: s.Config.BackupChatID}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return op, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE operations_channel SET topic_state=topic_state WHERE owner_id=? AND chat_id=?`, op.Owner, op.Chat); err != nil {
		return op, false, err
	}
	var topicState, statusState string
	var topicNext, statusNext, pinned int64
	err = tx.QueryRowContext(ctx, `SELECT thread_id,topic_state,next_attempt,status_message_id,status_state,status_next,status_pinned FROM operations_channel WHERE owner_id=? AND chat_id=?`, op.Owner, op.Chat).Scan(&op.Thread, &topicState, &topicNext, &op.Message, &statusState, &statusNext, &pinned)
	if err != nil {
		return op, false, err
	}
	now := s.Archiver.now().UnixMilli()
	if topicState == "pending" && topicNext <= now && op.Thread == 0 {
		op.Kind = "topic"
		_, err = tx.ExecContext(ctx, `UPDATE operations_channel SET topic_state='preparing' WHERE owner_id=? AND chat_id=? AND topic_state='pending' AND thread_id=0`, op.Owner, op.Chat)
	} else if topicState == "sent" && op.Thread > 0 && op.Thread != s.Config.BackupThreadID && (statusState == "pending" || statusState == "sent") && statusNext <= now {
		op.Kind = "status"
		_, err = tx.ExecContext(ctx, `UPDATE operations_channel SET status_state='preparing' WHERE owner_id=? AND chat_id=?`, op.Owner, op.Chat)
	} else if topicState == "sent" && op.Thread > 0 && op.Message > 0 && statusState == "sent" && pinned == 0 {
		op.Kind = "pin"
	} else if topicState == "sent" && op.Thread > 0 && op.Thread != s.Config.BackupThreadID {
		op.Message = 0
		err = tx.QueryRowContext(ctx, `SELECT id,message_id,role,level,message,CAST(fields_json AS TEXT),first_at_ms,last_at_ms,occurrence_count FROM operations_events WHERE owner_id=? AND chat_id=? AND state='pending' AND next_attempt<=? ORDER BY CASE level WHEN 'ERROR' THEN 0 ELSE 1 END,id LIMIT 1`, op.Owner, op.Chat, now).Scan(&op.Event, &op.Message, &op.Role, &op.Level, &op.Text, &op.Detail, &op.First, &op.Last, &op.Count)
		if errors.Is(err, sql.ErrNoRows) {
			return op, false, tx.Commit()
		}
		if err != nil {
			return op, false, err
		}
		op.Kind = "event"
		_, err = tx.ExecContext(ctx, `UPDATE operations_events SET state='preparing' WHERE id=? AND owner_id=? AND chat_id=? AND state='pending'`, op.Event, op.Owner, op.Chat)
	} else {
		return op, false, tx.Commit()
	}
	if err != nil {
		return op, false, err
	}
	return op, true, tx.Commit()
}

func (s *Service) markOperationsSending(ctx context.Context, op operationsOperation) error {
	if !s.operationsAllowed() || op.Owner != s.Config.OwnerID || op.Chat != s.Config.BackupChatID {
		return errOperationsReceipt
	}
	var result sql.Result
	var err error
	switch op.Kind {
	case "topic":
		result, err = s.Store.DB.ExecContext(ctx, `UPDATE operations_channel SET topic_state='sending' WHERE owner_id=? AND chat_id=? AND thread_id=0 AND topic_state IN ('preparing','sending')`, op.Owner, op.Chat)
	case "status":
		result, err = s.Store.DB.ExecContext(ctx, `UPDATE operations_channel SET status_state='sending' WHERE owner_id=? AND chat_id=? AND thread_id=? AND topic_state='sent' AND status_message_id=? AND status_state IN ('preparing','sending')`, op.Owner, op.Chat, op.Thread, op.Message)
	case "event":
		result, err = s.Store.DB.ExecContext(ctx, `UPDATE operations_events SET state='sending' WHERE id=? AND owner_id=? AND chat_id=? AND message_id=? AND state IN ('preparing','sending') AND EXISTS(SELECT 1 FROM operations_channel c WHERE c.owner_id=operations_events.owner_id AND c.chat_id=operations_events.chat_id AND c.thread_id=? AND c.topic_state='sent')`, op.Event, op.Owner, op.Chat, op.Message, op.Thread)
	case "pin":
		var ok bool
		err = s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations_channel WHERE owner_id=? AND chat_id=? AND thread_id=? AND status_message_id=? AND status_state='sent')`, op.Owner, op.Chat, op.Thread, op.Message).Scan(&ok)
		if err == nil && !ok {
			return errOperationsReceipt
		}
		return err
	default:
		return errOperationsReceipt
	}
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errOperationsReceipt
	}
	return err
}

func operationsEventText(op operationsOperation, botToken string) (string, []MessageEntity) {
	icon := "⚠️"
	if op.Level == "ERROR" {
		icon = "❌"
	} else if op.Level == "INFO" {
		icon = "ℹ️"
	}
	redact := func(value string) string {
		if botToken != "" {
			value = strings.ReplaceAll(value, botToken, "[redacted]")
			value = strings.ReplaceAll(value, url.QueryEscape(botToken), "[redacted]")
		}
		return value
	}
	fields := map[string]any{}
	var saved map[string]any
	decoder := json.NewDecoder(strings.NewReader(op.Detail))
	decoder.UseNumber()
	if decoder.Decode(&saved) == nil {
		for key, value := range saved {
			if v, ok := value.(string); ok {
				value = redact(v)
			}
			operationsSafeAttr(fields, slog.Any(key, value))
		}
	}
	heading := fmt.Sprintf("%s %s · %s", icon, operationsSafeText(redact(op.Role), 40), operationsSafeText(redact(op.Text), 180))
	var request string
	var reconnect bool
	if op.Text == "API operation failed" {
		source, _ := fields["source"].(string)
		if source == "Jardulik" || source == "Telegram" {
			heading = fmt.Sprintf("%s Ошибка запроса к %s", icon, source)
			status, _ := fields["http_status"].(int64)
			if status >= 100 && status <= 599 {
				label := "HTTP"
				if source == "Telegram" {
					label = "код"
				}
				heading += fmt.Sprintf(" · %s %d", label, status)
			}
			method, _ := fields["method"].(string)
			reconnect = source == "Jardulik" && method == "PUT" && fields["stage"] == "/appuser/refresh" && status == 400
			switch method {
			case "GET", "POST", "PUT", "PATCH", "DELETE":
				if requestURL, ok := fields["request_url"].(string); ok && source == "Jardulik" {
					request = fmt.Sprintf("%s: '%s'", method, requestURL)
				}
			}
		}
	}
	if op.Count > 1 {
		heading += fmt.Sprintf(" · ×%d", op.Count)
	}
	text := heading + "\n" + displayTime(time.UnixMilli(op.Last).UTC().Format(time.RFC3339))
	if op.Count > 1 {
		text += "\nПервый: " + displayTime(time.UnixMilli(op.First).UTC().Format(time.RFC3339))
	}
	var entities []MessageEntity
	if request != "" {
		text += "\n"
		entities = append(entities, MessageEntity{Type: "code", Offset: textUnits(text), Length: textUnits(request)})
		text += request
		if fields["query_filtered"] == true {
			text += "\nЧасть параметров URL скрыта."
		}
	}
	if reconnect {
		text += "\nJardulik отклонил обновление сессии. Переподключи аккаунт через /connect."
	}
	if len(fields) == 0 {
		return text, entities
	}
	raw, _ := json.MarshalIndent(fields, "", "  ")
	details := string(raw)
	var remainder string
	details, remainder = telegramPrefix(details, 3200)
	if remainder != "" {
		details += "\n…"
	}
	text += "\n\n"
	entities = append(entities, MessageEntity{Type: "expandable_blockquote", Offset: textUnits(text), Length: textUnits(details)})
	return text + details, entities
}

func (s *Service) operationsTopicCollision(ctx context.Context, thread int64) (bool, error) {
	if thread <= 0 || thread == s.Config.BackupThreadID {
		return true, nil
	}
	for _, check := range []struct{ table, query string }{
		{"operations_channel", "SELECT EXISTS(SELECT 1 FROM operations_channel WHERE thread_id=?)"},
		{"mirror_topics", "SELECT EXISTS(SELECT 1 FROM mirror_topics WHERE thread_id=?)"},
		{"profile_index_topics", "SELECT EXISTS(SELECT 1 FROM profile_index_topics WHERE thread_id=?)"},
		{"opening_audit_topics", "SELECT EXISTS(SELECT 1 FROM opening_audit_topics WHERE thread_id=?)"},
	} {
		exists, err := snapshotHasTable(ctx, s.Store.DB, check.table)
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		var collision bool
		if err = s.Store.DB.QueryRowContext(ctx, check.query, thread).Scan(&collision); err != nil || collision {
			return collision, err
		}
	}
	return false, nil
}

func (s *Service) finishOperations(ctx context.Context, op operationsOperation, state string, next int64, reason string, receipt []byte, thread, message int64) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if receipt == nil {
		receipt = []byte{}
	}
	var result sql.Result
	var err error
	switch op.Kind {
	case "topic":
		result, err = s.Store.DB.ExecContext(write, `UPDATE operations_channel SET topic_state=?,thread_id=CASE WHEN ?>0 THEN ? ELSE thread_id END,next_attempt=?,error=?,topic_receipt=? WHERE owner_id=? AND chat_id=? AND topic_state IN ('preparing','sending')`, state, thread, thread, next, reason, receipt, op.Owner, op.Chat)
	case "status":
		if state == "sent" {
			next = s.Archiver.now().Add(time.Minute).UnixMilli()
		}
		result, err = s.Store.DB.ExecContext(write, `UPDATE operations_channel SET status_state=?,status_message_id=CASE WHEN ?>0 THEN ? ELSE status_message_id END,status_sha=CASE WHEN ?='sent' THEN ? ELSE status_sha END,status_next=?,status_error=?,status_receipt=? WHERE owner_id=? AND chat_id=? AND thread_id=? AND status_state IN ('preparing','sending')`, state, message, message, state, op.SHA, next, reason, receipt, op.Owner, op.Chat, op.Thread)
	case "event":
		result, err = s.Store.DB.ExecContext(write, `UPDATE operations_events SET state=CASE WHEN ?='sent' AND occurrence_count>? THEN 'pending' ELSE ? END,message_id=CASE WHEN ?>0 THEN ? ELSE message_id END,posted_count=CASE WHEN ?='sent' THEN ? ELSE posted_count END,next_attempt=?,error=?,receipt_json=? WHERE id=? AND owner_id=? AND chat_id=? AND state IN ('preparing','sending')`, state, op.Count, state, message, message, state, op.Count, next, reason, receipt, op.Event, op.Owner, op.Chat)
	case "pin":
		value := int64(1)
		if state != "sent" {
			value = 0
			if state == "error" {
				value = -1
			}
		}
		result, err = s.Store.DB.ExecContext(write, `UPDATE operations_channel SET status_pinned=?,pin_error=? WHERE owner_id=? AND chat_id=? AND thread_id=? AND status_message_id=?`, value, reason, op.Owner, op.Chat, op.Thread, op.Message)
	default:
		return errOperationsReceipt
	}
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errOperationsReceipt
	}
	return err
}

func (s *Service) deliverOperations(ctx context.Context) (bool, error) {
	if !s.operationsAllowed() {
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
	op, claimed, err := s.claimOperations(send)
	if err != nil || !claimed {
		return false, err
	}
	var entities []MessageEntity
	if op.Kind == "status" {
		op.Text, err = s.OperationsStatus(send)
		op.SHA = digest([]byte(op.Text))
	} else if op.Kind == "event" {
		op.Text, entities = operationsEventText(op, s.Config.Token)
	}
	if err != nil {
		return true, errors.Join(err, s.finishOperations(ctx, op, "pending", a.now().Add(5*time.Second).UnixMilli(), "status_read_failed", nil, 0, op.Message))
	}
	attempted := false
	var last error
	var receipt []byte
	var thread, message int64
	err = a.groupCall(send, func() error {
		if err := s.markOperationsSending(send, op); err != nil {
			return err
		}
		attempted = true
		if op.Kind == "topic" {
			var result struct {
				Thread int64 `json:"message_thread_id"`
			}
			last = a.TG.Call(send, "createForumTopic", map[string]any{"chat_id": op.Chat, "name": operationsTitle}, &result)
			if last == nil {
				thread = result.Thread
				receipt, _ = json.Marshal(result)
			}
			return last
		}
		if op.Kind == "pin" {
			var pinned bool
			last = a.TG.Call(send, "pinChatMessage", map[string]any{"chat_id": op.Chat, "message_id": op.Message, "disable_notification": true}, &pinned)
			if last == nil && !pinned {
				return errOperationsReceipt
			}
			return last
		}
		var result SentMessage
		if op.Message == 0 {
			result, last = a.TG.SendText(send, op.Chat, op.Thread, op.Text, nil, entities)
		} else {
			last = a.TG.Call(send, "editMessageText", map[string]any{"chat_id": op.Chat, "message_id": op.Message, "text": op.Text, "entities": entities, "link_preview_options": map[string]bool{"is_disabled": true}}, &result)
			var te *TelegramError
			if errors.As(last, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
				message = op.Message
				receipt, _ = json.Marshal(map[string]any{"message_id": message, "not_modified": true})
				last = nil
				return nil
			}
		}
		if last == nil {
			if result.ID <= 0 || result.Chat.ID != op.Chat || result.ThreadID != op.Thread || op.Message > 0 && result.ID != op.Message {
				return errOperationsReceipt
			}
			message = result.ID
			receipt, _ = json.Marshal(result)
		}
		return last
	})
	if err == nil && op.Kind == "topic" {
		collision, e := s.operationsTopicCollision(send, thread)
		if e != nil {
			err = e
		} else if collision {
			err = errOperationsReceipt
		}
	}
	state, reason, next := "sent", "", int64(0)
	if err != nil {
		state, reason, next = profileIndexFailure(err, last, attempted, op.Message > 0, a.now())
		thread = 0
		message = op.Message
	}
	return true, errors.Join(err, s.finishOperations(ctx, op, state, next, reason, receipt, thread, message))
}
