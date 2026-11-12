package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func operationsTestService(t *testing.T) *Service {
	t.Helper()
	c, store := archiveTestStore(t)
	if err := store.InitOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &Service{Config: c, Store: store}
}

func operationsTestRecord(id string, at int64) operationsLogRecord {
	return operationsLogRecord{ID: id, At: at, Role: string(RuntimePoster), Level: "WARN", Message: "caption edit unavailable", Fields: map[string]any{"kind": "telegram_429", "tenant": int64(123)}}
}

func operationsWriteRecords(t *testing.T, s *Service, records ...operationsLogRecord) {
	t.Helper()
	f, err := os.OpenFile(operationsLogPath(s.Config.DataDir, RuntimePoster), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	for _, record := range records {
		if err = encoder.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOperationsLoggingInsideSQLiteWriterKeepsStderrAndOmitsCredentials(t *testing.T) {
	s := operationsTestService(t)
	tx, err := s.Store.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("INSERT INTO meta(key,value) VALUES('writer-held','1')"); err != nil {
		t.Fatal(err)
	}
	var stderr, file bytes.Buffer
	handler := &operationsLogHandler{base: slog.NewJSONHandler(&stderr, nil), sink: &operationsLogSink{writer: &file, role: RuntimePoster, boot: "fixture"}}
	logger := slog.New(handler).With("tenant", 123, "authorization", "Bearer secret-token").WithGroup("worker")
	done := make(chan struct{})
	go func() {
		logger.Warn("caption edit unavailable", "kind", "telegram_429", "request_body", "private-profile", "error", "https://api.telegram.org/botsecret-token/getMe")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("logging re-entered SQLite while a transaction was held")
	}
	if !strings.Contains(stderr.String(), "secret-token") || !strings.Contains(stderr.String(), "caption edit unavailable") {
		t.Fatal("original stderr handler changed")
	}
	for _, secret := range []string{"secret-token", "private-profile", "authorization", "request_body", "api.telegram"} {
		if strings.Contains(file.String(), secret) {
			t.Fatal("operations spool exposed an unrelated payload", secret)
		}
	}
	var record operationsLogRecord
	if json.Unmarshal(bytes.TrimSpace(file.Bytes()), &record) != nil || record.Role != "poster" || record.Fields["tenant"] != float64(123) || record.Fields["kind"] != "telegram_429" {
		t.Fatal("safe diagnostic fields were lost", file.String())
	}
}

func TestOperationsAutoResultReachesCollapsedQuote(t *testing.T) {
	var spool bytes.Buffer
	logger := slog.New(&operationsLogHandler{base: slog.NewJSONHandler(io.Discard, nil), sink: &operationsLogSink{writer: &spool, role: RuntimeBotParser, boot: "auto"}})
	logger.Info("ordinary diagnostic", "state", "ready")
	logger.Info(autoLogResult, "reaction_type", 3, "action_id", 12, "state", "succeeded",
		"mutual", false, "message_body", "private-message")
	var record operationsLogRecord
	if err := json.Unmarshal(spool.Bytes(), &record); err != nil || record.Message != autoLogResult {
		t.Fatalf("unexpected spool: %s, %v", spool.Bytes(), err)
	}
	details, err := json.Marshal(record.Fields)
	if err != nil {
		t.Fatal(err)
	}
	body, entities := operationsEventText(operationsOperation{Role: record.Role, Level: record.Level, Text: record.Message, Detail: string(details), Last: record.At}, "")
	if len(entities) != 1 || entities[0].Type != "expandable_blockquote" || validMessageEntities(body, entities) != nil ||
		!strings.Contains(body, `"reaction_type": 3`) || !strings.Contains(body, `"action_id": 12`) ||
		!strings.Contains(body, `"mutual": false`) || strings.Contains(body, "private-message") {
		t.Fatalf("unexpected Telegram event: %s %+v", body, entities)
	}
}

func TestOperationsCollectorPartialLineRestartAndRecordDedup(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	one, _ := json.Marshal(operationsTestRecord("one", now))
	two, _ := json.Marshal(operationsTestRecord("two", now+1))
	path := operationsLogPath(s.Config.DataDir, RuntimePoster)
	if err := os.WriteFile(path, append(append(one, '\n'), two[:len(two)/2]...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	var count, offset int64
	if err := s.Store.DB.QueryRow("SELECT occurrence_count FROM operations_events").Scan(&count); err != nil || count != 1 {
		t.Fatal("partial line was consumed", count, err)
	}
	if err := s.Store.DB.QueryRow("SELECT offset_bytes FROM operations_log_cursors WHERE role='poster'").Scan(&offset); err != nil || offset != int64(len(one)+1) {
		t.Fatal("cursor advanced beyond newline", offset, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write(append(two[len(two)/2:], '\n'))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	// A replacement collector has no in-memory state.
	s = &Service{Config: s.Config, Store: s.Store}
	if err = s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.DB.QueryRow("SELECT occurrence_count FROM operations_events").Scan(&count); err != nil || count != 2 {
		t.Fatal("restart duplicated or dropped a record", count, err)
	}
	// Replaying a preserved file after replacing its inode must not count its IDs twice.
	if err = os.Rename(path, path+".preserved"); err != nil {
		t.Fatal(err)
	}
	operationsWriteRecords(t, s, operationsTestRecord("one", now), operationsTestRecord("two", now+1), operationsTestRecord("three", now+2))
	if err = s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.DB.QueryRow("SELECT occurrence_count FROM operations_events").Scan(&count); err != nil || count != 3 {
		t.Fatal("file replay bypassed durable record IDs", count, err)
	}
}

func TestOperationsRepeatNeverRequeuesUnknownSend(t *testing.T) {
	for _, state := range []string{"preparing", "sending", "uncertain", "error", "sent"} {
		t.Run(state, func(t *testing.T) {
			s := operationsTestService(t)
			ctx := context.Background()
			now := time.Now().UnixMilli()
			operationsWriteRecords(t, s, operationsTestRecord("one", now))
			if err := s.collectOperationsLogs(ctx); err != nil {
				t.Fatal(err)
			}
			message := 0
			if state == "sent" {
				message = 101
			}
			if _, err := s.Store.DB.Exec("UPDATE operations_events SET state=?,message_id=?,posted_count=1", state, message); err != nil {
				t.Fatal(err)
			}
			operationsWriteRecords(t, s, operationsTestRecord("two", now+1))
			if err := s.collectOperationsLogs(ctx); err != nil {
				t.Fatal(err)
			}
			var saved string
			var count, posted int
			if err := s.Store.DB.QueryRow("SELECT state,occurrence_count,posted_count FROM operations_events").Scan(&saved, &count, &posted); err != nil {
				t.Fatal(err)
			}
			want := state
			if state == "sent" {
				want = "pending"
			}
			if saved != want || count != 2 || posted != 1 {
				t.Fatal("aggregation reset an uncertain effect or lost its count", saved, count, posted)
			}
		})
	}
}

func TestOperationsCollectorRecordsDurableErrorsWithoutRepeatedPollSpam(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	if _, err := s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id,state,error) VALUES('fixture','private',1,123,0,'[1]',123,0,'failed','receipt_missing_or_changed')`); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := s.collectOperationsLogs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	var fields []byte
	if err := s.Store.DB.QueryRow("SELECT occurrence_count,fields_json FROM operations_events").Scan(&count, &fields); err != nil || count != 1 || !bytes.Contains(fields, []byte("caption_maintenance_jobs")) {
		t.Fatal("persistent error was missed or emitted on every poll", count, string(fields), err)
	}
}

func TestOperationsCollectorBoundsOnePassAndHeartbeat(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	for i := range 300 {
		operationsWriteRecords(t, s, operationsTestRecord(strconv.Itoa(i), now))
	}
	if err := s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT occurrence_count FROM operations_events").Scan(&count); err != nil || count != 38 {
		t.Fatal("unbounded collector pass", count, err)
	}
	for range 7 {
		if err := s.collectOperationsLogs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Store.DB.QueryRow("SELECT occurrence_count FROM operations_events").Scan(&count); err != nil || count != 300 {
		t.Fatal("bounded pass dropped remaining lines", count, err)
	}
	if err := s.Store.UpdateOperationsHeartbeat(ctx, RuntimePoster); err != nil {
		t.Fatal(err)
	}
	var heartbeat map[string]int64
	if json.Unmarshal([]byte(s.Store.Meta(ctx, "operations_heartbeat:poster")), &heartbeat) != nil || heartbeat["at_ms"] <= 0 || heartbeat["started_at_ms"] <= 0 || heartbeat["pid"] != int64(os.Getpid()) {
		t.Fatal("invalid heartbeat", heartbeat)
	}
}

func TestOperationsCollectorSmallBudgetNeverDiscardsValidRecord(t *testing.T) {
	s := operationsTestService(t)
	operationsWriteRecords(t, s, operationsTestRecord("valid-longer-than-remainder", time.Now().UnixMilli()))
	read, count, err := s.collectOperationsFile(context.Background(), RuntimePoster, 36, 64)
	if err != nil || read != 0 || count != 0 {
		t.Fatal("small remaining budget consumed a partial record", read, count, err)
	}
	if err = s.collectOperationsLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	var message string
	if err = s.Store.DB.QueryRow("SELECT message FROM operations_events").Scan(&message); err != nil || message != "caption edit unavailable" {
		t.Fatal("valid record was mistaken for an oversized line", message, err)
	}
}

func TestOperationsCollectorRealWorkerSchemas(t *testing.T) {
	s := operationsTestService(t)
	if _, err := s.Store.DB.Exec(archiveSchema + captionMaintenanceSchema + fullTransferSchema + analysisSchema + analysisPostsSchema + archiveChangePostsSchema + profilePresenceSchema + profileIndexesSchema + openingPostsSchema + openingTimelineSchema + compatibilitySchema + contactJobsSchema); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.collectOperationsLogs(context.Background()); err != nil {
			t.Fatal("real worker table shape rejected", err)
		}
	}
}

func TestOperationsCollectorOpeningTimelineAndAllMetaErrors(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	if _, err := s.Store.DB.Exec(openingTimelineSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("INSERT INTO actions(id,owner_id,profile_id,kind,state) VALUES(1,123,7,'like','unknown')"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO opening_timeline_posts(owner_id,upstream_id,action_id,profile_id,chat_id,thread_id,attempted_at_ms,source_json,source_sha,payload_json,payload_sha,state,error)
 VALUES(123,5696,1,7,?,55,1,'{}','fixture','{}','fixture','uncertain','worker_restarted_during_request')`, s.Config.BackupChatID); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"archive_last_error", "watch_last_error", "profile_presence_last_error", "profile_indexes_last_error", "opening_posts_last_error", "opening_timeline_last_error", "archive_change_posts_last_error", "operations_last_error"} {
		if err := s.Store.SetMeta(ctx, key, "fixture_error"); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := s.collectOperationsLogs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT SUM(occurrence_count) FROM operations_events").Scan(&count); err != nil || count != 9 {
		t.Fatal("timeline or metadata errors were missed or repeated", count, err)
	}
}

func TestOperationsTypedErrorKeepsCodesWithoutRawCredentialString(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(&operationsLogHandler{base: slog.NewJSONHandler(io.Discard, nil), sink: &operationsLogSink{writer: &output, role: RuntimeBotParser, boot: "typed"}})
	logger.Error("command failed", "error", &TelegramError{Code: 429, RetryAfter: 17, Description: "Bearer secret-token"})
	if bytes.Contains(output.Bytes(), []byte("secret-token")) || !bytes.Contains(output.Bytes(), []byte(`"http_status":429`)) || !bytes.Contains(output.Bytes(), []byte(`"retry_after":17`)) {
		t.Fatal("typed error lost its diagnostics or exposed raw response", output.String())
	}
}

func TestOperationsFloodStillCollectsDurableStateAndMetaErrors(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	if _, err := s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id,state,error) VALUES('fixture','private',1,123,0,'[1]',123,0,'failed','receipt_missing_or_changed')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetMeta(ctx, "archive_last_error", "telegram_429"); err != nil {
		t.Fatal(err)
	}
	for _, role := range operationsRoles {
		f, err := os.OpenFile(operationsLogPath(s.Config.DataDir, role), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 80 {
			record := operationsTestRecord(fmt.Sprintf("%s:%d", role, i), time.Now().UnixMilli())
			record.Role = string(role)
			if err = json.NewEncoder(f).Encode(record); err != nil {
				f.Close()
				t.Fatal(err)
			}
		}
		f.Close()
	}
	if err := s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM operations_events WHERE message IN ('Ошибка фонового задания','Последняя ошибка воркера')").Scan(&count); err != nil || count != 2 {
		t.Fatal("continuous logs starved the state or metadata channel", count, err)
	}
	if err := s.Store.DB.QueryRow("SELECT SUM(occurrence_count) FROM operations_events WHERE message='caption edit unavailable'").Scan(&count); err != nil || count < 152 || count > 190 {
		t.Fatal("roles did not receive their bounded share", count, err)
	}
	if err := s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(DISTINCT role) FROM operations_events WHERE message='caption edit unavailable'").Scan(&count); err != nil || count != 5 {
		t.Fatal("rotation starved the fifth role", count, err)
	}
}

func TestOperationsMiniAppRoleLoggerAndHeartbeat(t *testing.T) {
	s := operationsTestService(t)
	s.Config.Token = "fixture-token"
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer slog.SetDefault(previous)
	if err := InstallOperationsLogger(s.Config, RuntimeMiniApp); err != nil {
		t.Fatal(err)
	}
	handler := slog.Default().Handler().(*operationsLogHandler)
	defer handler.sink.writer.(*os.File).Close()
	slog.Warn("miniapp unavailable", "kind", "fixture_failure")
	if err := s.Store.UpdateOperationsHeartbeat(context.Background(), RuntimeMiniApp); err != nil {
		t.Fatal(err)
	}
	if err := s.collectOperationsLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM operations_events WHERE role='miniapp'").Scan(&count); err != nil || count != 2 {
		t.Fatal("miniapp startup or warning was missed", count, err)
	}
	var heartbeat map[string]int64
	if json.Unmarshal([]byte(s.Store.Meta(context.Background(), "operations_heartbeat:miniapp")), &heartbeat) != nil || heartbeat["pid"] != int64(os.Getpid()) {
		t.Fatal("standalone miniapp heartbeat was missed", heartbeat)
	}
}

func TestOperationsLoggerInstallationPrivateAppendOnly(t *testing.T) {
	s := operationsTestService(t)
	s.Config.Token = "fixture-token"
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer slog.SetDefault(previous)
	if err := InstallOperationsLogger(s.Config, RuntimePoster); err != nil {
		t.Fatal(err)
	}
	handler := slog.Default().Handler().(*operationsLogHandler)
	defer handler.sink.writer.(*os.File).Close()
	slog.Info("quiet ordinary polling")
	slog.Warn("test diagnostic", "kind", "telegram_429")
	path := operationsLogPath(s.Config.DataDir, RuntimePoster)
	raw, err := os.ReadFile(path)
	if err != nil || bytes.Count(raw, []byte{'\n'}) != 2 || bytes.Contains(raw, []byte("quiet ordinary polling")) {
		t.Fatal("logger did not keep startup and warning only", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("operations spool permissions changed", err)
	}
}
