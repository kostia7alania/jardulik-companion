package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"syscall"
	"time"
)

const operationsStoreSchema = `
CREATE TABLE IF NOT EXISTS operations_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, role TEXT NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL,
 fields_json BLOB NOT NULL, first_at_ms INTEGER NOT NULL, last_at_ms INTEGER NOT NULL,
 occurrence_count INTEGER NOT NULL DEFAULT 1, message_id INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 posted_count INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '', receipt_json BLOB NOT NULL DEFAULT X'',
 UNIQUE(owner_id,chat_id,fingerprint)
);
CREATE INDEX IF NOT EXISTS operations_events_due ON operations_events(owner_id,chat_id,state,next_attempt,id);
CREATE TABLE IF NOT EXISTS operations_log_cursors (
 owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, role TEXT NOT NULL,
 file_identity TEXT NOT NULL, offset_bytes INTEGER NOT NULL DEFAULT 0, discarding INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(owner_id,chat_id,role)
);
CREATE TABLE IF NOT EXISTS operations_log_records (
 owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, record_id TEXT NOT NULL,
 PRIMARY KEY(owner_id,chat_id,record_id)
);
INSERT OR IGNORE INTO meta(key,value) VALUES('operations_writer','');`

func (s *Store) InitOperations(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, operationsStoreSchema)
	return err
}

func (s *Service) collectOperationsLogs(ctx context.Context) error {
	if err := s.Store.InitOperations(ctx); err != nil {
		return err
	}
	remaining, records := 64<<10, 256
	for _, role := range operationsRoles {
		if remaining == 0 || records == 0 {
			break
		}
		// Reserve capacity for durable worker errors even during a log flood.
		consumed, count, err := s.collectOperationsFile(ctx, role, min(remaining, operationsRecordLimit), min(records, 48))
		if err != nil {
			return err
		}
		remaining -= consumed
		records -= count
	}
	if records > 0 {
		return s.collectOperationsStateErrors(ctx, min(records, 64))
	}
	return nil
}

type operationsLogCursor struct {
	identity string
	offset   int64
	discard  bool
}

func (s *Service) operationsCursor(ctx context.Context, q reactionQuerier, role RuntimeRole) (operationsLogCursor, error) {
	var cursor operationsLogCursor
	err := q.QueryRowContext(ctx, `SELECT file_identity,offset_bytes,discarding FROM operations_log_cursors WHERE owner_id=? AND chat_id=? AND role=?`, s.Config.OwnerID, s.Config.BackupChatID, role).Scan(&cursor.identity, &cursor.offset, &cursor.discard)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return cursor, err
}

func (s *Service) collectOperationsFile(ctx context.Context, role RuntimeRole, budget, limit int) (int, int, error) {
	if budget < operationsRecordLimit {
		return 0, 0, nil
	}
	path := operationsLogPath(s.Config.DataDir, role)
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return 0, 0, errors.New("operations input is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("operations file identity unavailable")
	}
	identity := fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
	original, err := s.operationsCursor(ctx, s.Store.DB, role)
	if err != nil {
		return 0, 0, err
	}
	cursor := original
	if cursor.identity != identity || info.Size() < cursor.offset {
		cursor = operationsLogCursor{identity: identity}
	}
	data := make([]byte, budget)
	n, readErr := f.ReadAt(data, cursor.offset)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return 0, 0, readErr
	}
	data = data[:n]
	if n == 0 {
		return 0, 0, nil
	}
	var collected []operationsLogRecord
	consumed := 0
	if cursor.discard {
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			consumed = len(data)
		} else {
			consumed, cursor.discard = end+1, false
		}
	}
	for !cursor.discard && consumed < len(data) && len(collected) < limit {
		end := bytes.IndexByte(data[consumed:], '\n')
		if end < 0 {
			if len(data)-consumed >= operationsRecordLimit {
				collected = append(collected, operationsMalformedRecord(role, cursor.offset+int64(consumed), data[consumed:], "oversized_record"))
				consumed, cursor.discard = len(data), true
			}
			break
		}
		line := data[consumed : consumed+end]
		var record operationsLogRecord
		if json.Unmarshal(line, &record) != nil || record.ID == "" || len(record.ID) > 160 || record.Role != string(role) || record.At <= 0 || record.At > time.Now().Add(5*time.Minute).UnixMilli() {
			record = operationsMalformedRecord(role, cursor.offset+int64(consumed), line, "invalid_record")
		}
		collected = append(collected, record)
		consumed += end + 1
	}
	if consumed == 0 {
		return n, 0, nil
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return n, 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='operations_writer'"); err != nil {
		return n, 0, err
	}
	current, err := s.operationsCursor(ctx, tx, role)
	if err != nil {
		return n, 0, err
	}
	if current != original {
		return n, 0, nil
	}
	for _, record := range collected {
		if err = s.appendOperationsRecordTx(ctx, tx, record); err != nil {
			return n, 0, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operations_log_cursors(owner_id,chat_id,role,file_identity,offset_bytes,discarding) VALUES(?,?,?,?,?,?)
 ON CONFLICT(owner_id,chat_id,role) DO UPDATE SET file_identity=excluded.file_identity,offset_bytes=excluded.offset_bytes,discarding=excluded.discarding`, s.Config.OwnerID, s.Config.BackupChatID, role, identity, cursor.offset+int64(consumed), cursor.discard)
	if err != nil {
		return n, 0, err
	}
	return n, len(collected), tx.Commit()
}

func operationsMalformedRecord(role RuntimeRole, offset int64, raw []byte, kind string) operationsLogRecord {
	return operationsLogRecord{ID: "invalid:" + string(role) + ":" + strconv.FormatInt(offset, 10) + ":" + digest(raw), At: time.Now().UnixMilli(), Role: string(role), Level: "WARN", Message: "Повреждена строка журнала", Fields: map[string]any{"kind": kind}}
}

func (s *Service) appendOperationsRecordTx(ctx context.Context, tx *sql.Tx, record operationsLogRecord) error {
	result, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO operations_log_records(owner_id,chat_id,record_id) VALUES(?,?,?)", s.Config.OwnerID, s.Config.BackupChatID, record.ID)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 0 {
		return err
	}
	fields := map[string]any{}
	for key, value := range record.Fields {
		operationsSafeAttr(fields, slog.Any(key, value))
		// JSON numeric values arrive as float64, unlike direct slog integers.
		if number, ok := value.(float64); ok && number == float64(int64(number)) {
			operationsSafeAttr(fields, slog.Int64(key, int64(number)))
		}
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	fingerprintFields := map[string]any{}
	for key, value := range fields {
		if key != "pid" {
			fingerprintFields[key] = value
		}
	}
	identity, _ := json.Marshal([]any{record.Role, record.Level, operationsSafeText(record.Message, 300), fingerprintFields})
	_, err = tx.ExecContext(ctx, `INSERT INTO operations_events(owner_id,chat_id,fingerprint,role,level,message,fields_json,first_at_ms,last_at_ms) VALUES(?,?,?,?,?,?,?,?,?)
 ON CONFLICT(owner_id,chat_id,fingerprint) DO UPDATE SET occurrence_count=operations_events.occurrence_count+1,last_at_ms=MAX(operations_events.last_at_ms,excluded.last_at_ms),first_at_ms=MIN(operations_events.first_at_ms,excluded.first_at_ms),fields_json=excluded.fields_json,
 state=CASE WHEN operations_events.state='sent' AND operations_events.message_id>0 THEN 'pending' ELSE operations_events.state END,
 next_attempt=CASE WHEN operations_events.state='sent' AND operations_events.message_id>0 THEN 0 ELSE operations_events.next_attempt END`, s.Config.OwnerID, s.Config.BackupChatID, digest(identity), record.Role, record.Level, operationsSafeText(record.Message, 300), raw, record.At, record.At)
	return err
}

type operationsErrorSource struct {
	table, role, from, tenant, reason, stamp string
}

// A number of workers persist their errors without emitting slog records.
var operationsErrorSources = []operationsErrorSource{
	{"caption_maintenance_jobs", "poster", "caption_maintenance_jobs t", "t.owner_id", "t.error", "t.updated_at"},
	{"archive_versions", "poster", "archive_versions t", "t.owner_id", "t.error", "t.updated_at"},
	{"archive_profile_repairs", "poster", "archive_profile_repairs t JOIN versions v ON v.id=t.version_id", "v.owner_id", "t.error", "t.updated_at"},
	{"archive_zip_repairs", "poster", "archive_zip_repairs t JOIN versions v ON v.id=t.version_id", "v.owner_id", "t.error", "t.updated_at"},
	{"archive_backups", "poster", "archive_backups t", "0", "t.error", "t.created_at"},
	{"full_backup_jobs", "poster", "full_backup_jobs t", "0", "t.error", "t.updated_at"},
	{"analysis_posts", "poster", "analysis_posts t", "t.owner_id", "t.error", "t.updated_at"},
	{"archive_change_posts", "poster", "archive_change_posts t", "t.owner_id", "t.error", "t.updated_at"},
	{"profile_presence_events", "poster", "profile_presence_events t", "t.owner_id", "t.error", "t.updated_at"},
	{"profile_index_topics", "poster", "profile_index_topics t", "t.owner_id", "t.error", "t.updated_at"},
	{"profile_index_pages", "poster", "profile_index_pages t JOIN profile_index_topics topic ON topic.id=t.topic_id", "topic.owner_id", "t.error", "t.updated_at"},
	{"opening_audit_topics", "poster", "opening_audit_topics t", "t.owner_id", "t.error", "t.updated_at"},
	{"opening_audit_posts", "poster", "opening_audit_posts t JOIN opening_audit_topics topic ON topic.id=t.topic_id", "topic.owner_id", "t.error", "t.updated_at"},
	{"opening_timeline_posts", "poster", "opening_timeline_posts t", "t.owner_id", "t.error", "t.updated_at"},
	{"compatibility_jobs", "bot-parser", "compatibility_jobs t JOIN compatibility_pairs pair ON pair.id=t.pair_id", "pair.owner_id", "t.reason", "CAST(t.updated_at_ms AS TEXT)"},
	{"contact_requests", "bot-parser", "contact_requests t", "t.owner_id", "t.reason", "CAST(t.updated_at_ms AS TEXT)"},
	{"media_jobs", "bot-parser", "media_jobs t", "t.owner_id", "t.error", "CAST(t.attempts AS TEXT)"},
}

func (s *Service) collectOperationsStateErrors(ctx context.Context, budget int) error {
	metaBudget := min(8, budget)
	budget -= metaBudget
	perSource := max(1, budget/len(operationsErrorSources))
	for _, source := range operationsErrorSources {
		if budget <= 0 {
			break
		}
		exists, err := snapshotHasTable(ctx, s.Store.DB, source.table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		key := "operations_error_cursor:" + strconv.FormatInt(s.Config.OwnerID, 10) + ":" + strconv.FormatInt(s.Config.BackupChatID, 10) + ":" + source.table
		original := s.Store.Meta(ctx, key)
		cursor, _ := strconv.ParseInt(original, 10, 64)
		limit := min(perSource, budget)
		rows, err := s.Store.DB.QueryContext(ctx, `SELECT t.rowid,`+source.tenant+`,t.state,`+source.reason+`,`+source.stamp+` FROM `+source.from+`
 WHERE t.rowid>? AND t.state IN ('failed','error','uncertain','unknown','rejected') ORDER BY t.rowid LIMIT ?`, cursor, limit)
		if err != nil {
			return err
		}
		var collected []operationsLogRecord
		last := cursor
		for rows.Next() {
			var id, tenant int64
			var state, reason, stamp string
			if err = rows.Scan(&id, &tenant, &state, &reason, &stamp); err != nil {
				rows.Close()
				return err
			}
			last = id
			if reason == "" {
				reason = state
			}
			recordID := "state:" + source.table + ":" + strconv.FormatInt(id, 10) + ":" + digest([]byte(state+"\n"+reason+"\n"+stamp))
			collected = append(collected, operationsLogRecord{ID: recordID, At: time.Now().UnixMilli(), Role: source.role, Level: "WARN", Message: "Ошибка фонового задания", Fields: map[string]any{"component": source.table, "job_id": id, "tenant": tenant, "kind": operationsSafeText(reason, 180), "state": state}})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(collected) < limit {
			last = 0
		}
		budget -= len(collected)
		tx, err := s.Store.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err = s.saveOperationsStateBatch(ctx, tx, key, original, last, collected); err == nil {
			err = tx.Commit()
		}
		tx.Rollback()
		if err != nil {
			return err
		}
	}
	return s.collectOperationsMetaErrors(ctx, metaBudget)
}

func (s *Service) saveOperationsStateBatch(ctx context.Context, tx *sql.Tx, key, expected string, cursor int64, records []operationsLogRecord) error {
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='operations_writer'"); err != nil {
		return err
	}
	var current string
	err := tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", key).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if current != expected {
		return nil
	}
	for _, record := range records {
		if err = s.appendOperationsRecordTx(ctx, tx, record); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, strconv.FormatInt(cursor, 10))
	return err
}

func (s *Service) collectOperationsMetaErrors(ctx context.Context, budget int) error {
	if budget <= 0 {
		return nil
	}
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT key,value FROM meta WHERE key IN ('archive_last_error','watch_last_error','profile_presence_last_error','profile_indexes_last_error','opening_posts_last_error','opening_timeline_last_error','archive_change_posts_last_error','operations_last_error') AND value<>'' LIMIT ?`, min(8, budget))
	if err != nil {
		return err
	}
	var records []operationsLogRecord
	for rows.Next() {
		var key, value string
		if err = rows.Scan(&key, &value); err != nil {
			rows.Close()
			return err
		}
		records = append(records, operationsLogRecord{ID: "meta:" + key + ":" + digest([]byte(value)), At: time.Now().UnixMilli(), Role: "poster", Level: "WARN", Message: "Последняя ошибка воркера", Fields: map[string]any{"component": key, "kind": operationsSafeText(value, 180)}})
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(records) == 0 {
		return err
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='operations_writer'"); err != nil {
		return err
	}
	for _, record := range records {
		if err = s.appendOperationsRecordTx(ctx, tx, record); err != nil {
			return err
		}
	}
	return tx.Commit()
}
