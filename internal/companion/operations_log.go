package companion

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
)

var operationsRoles = []RuntimeRole{RuntimeAll, RuntimeBotParser, RuntimePoster, RuntimeCompatibility, RuntimeMiniApp}
var operationsProcessStarted = time.Now().UnixMilli()

const operationsRecordLimit = 16 << 10

type operationsLogRecord struct {
	ID      string         `json:"id"`
	At      int64          `json:"at_ms"`
	Role    string         `json:"role"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

type operationsLogSink struct {
	mu     sync.Mutex
	writer io.Writer
	role   RuntimeRole
	boot   string
	seq    atomic.Uint64
}

type operationsLogHandler struct {
	base  slog.Handler
	sink  *operationsLogSink
	attrs []slog.Attr
}

// This process-lifetime descriptor remains open for main's final fatal log.
// The handler never accesses SQLite, including when its caller holds a writer.
func InstallOperationsLogger(c Config, role RuntimeRole) error {
	if c.OwnerID <= 0 || c.BackupChatID == 0 || c.Token == "" {
		return nil
	}
	if !operationsRoleKnown(role) {
		return errors.New("invalid operations runtime role")
	}
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(operationsLogPath(c.DataDir, role), os.O_WRONLY|os.O_CREATE|os.O_APPEND|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return errors.New("operations log is not a regular file")
	}
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	var random [12]byte
	if _, err = rand.Read(random[:]); err != nil {
		f.Close()
		return err
	}
	sink := &operationsLogSink{writer: f, role: role, boot: hex.EncodeToString(random[:])}
	slog.SetDefault(slog.New(&operationsLogHandler{base: slog.Default().Handler(), sink: sink}))
	return sink.append(operationsLogRecord{At: time.Now().UnixMilli(), Level: "INFO", Message: "Сервис запущен", Fields: map[string]any{"pid": os.Getpid()}})
}

func operationsRoleKnown(role RuntimeRole) bool {
	for _, valid := range operationsRoles {
		if role == valid {
			return true
		}
	}
	return false
}

func operationsLogPath(dir string, role RuntimeRole) string {
	return filepath.Join(dir, "operations-"+string(role)+".jsonl")
}

func (h *operationsLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn || h.base.Enabled(ctx, level)
}

func (h *operationsLogHandler) Handle(ctx context.Context, record slog.Record) error {
	var baseErr error
	if h.base.Enabled(ctx, record.Level) {
		baseErr = h.base.Handle(ctx, record)
	}
	if record.Level < slog.LevelWarn {
		return baseErr
	}
	fields := map[string]any{}
	for _, attr := range h.attrs {
		operationsSafeAttr(fields, attr)
	}
	record.Attrs(func(attr slog.Attr) bool { operationsSafeAttr(fields, attr); return true })
	err := h.sink.append(operationsLogRecord{At: record.Time.UnixMilli(), Level: record.Level.String(), Message: operationsSafeText(record.Message, 300), Fields: fields})
	return errors.Join(baseErr, err)
}

func (h *operationsLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &operationsLogHandler{base: h.base.WithAttrs(attrs), sink: h.sink, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *operationsLogHandler) WithGroup(name string) slog.Handler {
	return &operationsLogHandler{base: h.base.WithGroup(name), sink: h.sink, attrs: append([]slog.Attr{}, h.attrs...)}
}

func (s *operationsLogSink) append(record operationsLogRecord) error {
	record.ID = fmt.Sprintf("%s:%d", s.boot, s.seq.Add(1))
	record.Role = string(s.role)
	if record.At <= 0 {
		record.At = time.Now().UnixMilli()
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw)+1 > operationsRecordLimit {
		// Keep the full original on stderr; the notification retains its identity.
		record.Fields = map[string]any{"kind": "diagnostic_too_large"}
		raw, err = json.Marshal(record)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw = append(raw, '\n')
	n, err := s.writer.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	if err == nil {
		if durable, ok := s.writer.(interface{ Sync() error }); ok {
			err = durable.Sync()
		}
	}
	return err
}

func operationsSafeAttr(fields map[string]any, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Key == "error" && attr.Value.Kind() == slog.KindAny {
		if cause, ok := attr.Value.Any().(error); ok && cause != nil {
			fields["kind"] = safeError(cause)
			var code interface{ Code() int }
			if errors.As(cause, &code) {
				fields["sqlite_code"] = int64(code.Code())
			}
			var te *TelegramError
			if errors.As(cause, &te) {
				fields["http_status"], fields["retry_after"] = int64(te.Code), int64(te.RetryAfter)
			}
			var ue *UpstreamError
			if errors.As(cause, &ue) {
				fields["http_status"] = int64(ue.Status)
			}
			var network net.Error
			switch {
			case errors.Is(cause, context.DeadlineExceeded):
				fields["kind"] = "context_deadline"
			case errors.Is(cause, context.Canceled):
				fields["kind"] = "context_cancelled"
			case errors.As(cause, &network) && network.Timeout():
				fields["kind"] = "network_timeout"
			}
		}
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			operationsSafeAttr(fields, child)
		}
		return
	}
	if attr.Key == "request_url" {
		if attr.Value.Kind() == slog.KindString {
			if safeURL, filtered := operationsUpstreamURL(attr.Value.String()); safeURL != "" {
				fields[attr.Key] = safeURL
				if filtered {
					fields["query_filtered"] = true
				}
			}
		}
		return
	}
	if attr.Key == "query_filtered" {
		if attr.Value.Kind() == slog.KindBool && attr.Value.Bool() {
			fields[attr.Key] = true
		}
		return
	}
	switch attr.Key {
	case "kind", "stage", "requested_stage", "tenant", "owner", "owner_id", "upstream_id", "profile_id", "target_user_id", "run_id", "mode", "page", "sqlite_code", "fatal", "http_status", "retry_after", "component", "source", "job_id", "state", "pid", "method", "description":
	default:
		return
	}
	switch attr.Value.Kind() {
	case slog.KindString:
		value := attr.Value.String()
		if attr.Key == "stage" || attr.Key == "requested_stage" {
			value, _, _ = strings.Cut(value, "?")
		}
		fields[attr.Key] = operationsSafeText(value, 180)
	case slog.KindInt64:
		fields[attr.Key] = attr.Value.Int64()
	case slog.KindUint64:
		fields[attr.Key] = attr.Value.Uint64()
	case slog.KindBool:
		fields[attr.Key] = attr.Value.Bool()
	case slog.KindAny:
		if number, ok := attr.Value.Any().(json.Number); ok {
			if n, err := number.Int64(); err == nil {
				fields[attr.Key] = n
			}
		}
	}
}

var operationsTelegramCredential = regexp.MustCompile(`(?i)[0-9]{5,}(?::|%3a)[a-z0-9_-]{16,}`)

func operationsSafeText(value string, limit int) string {
	if operationsTelegramCredential.MatchString(value) {
		return "details_available_locally"
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "tgwebappdata", "accesstoken", "refreshtoken", "authorization", "initdata", "access_token", "refresh_token", "token=", "https://", "http://"} {
		if strings.Contains(lower, marker) {
			return "details_available_locally"
		}
	}
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value, _ = telegramPrefix(strings.TrimSpace(value), limit)
	return value
}

func (s *Store) UpdateOperationsHeartbeat(ctx context.Context, role RuntimeRole) error {
	if !operationsRoleKnown(role) {
		return errors.New("invalid operations heartbeat role")
	}
	raw, _ := json.Marshal(map[string]int64{"at_ms": time.Now().UnixMilli(), "started_at_ms": operationsProcessStarted, "pid": int64(os.Getpid())})
	return s.SetMeta(ctx, "operations_heartbeat:"+string(role), string(raw))
}
