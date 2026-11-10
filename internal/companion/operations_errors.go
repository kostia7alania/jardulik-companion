package companion

import (
	"context"
	"errors"
	"log/slog"
	"strings"
)

func logOperationsFailure(ctx context.Context, source, method, path string, owner int64, err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrProfileUnavailable) {
		return
	}
	var te *TelegramError
	if errors.As(err, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "not modified") {
		return
	}
	path, _, _ = strings.Cut(path, "?")
	attrs := []any{"source", source, "method", operationsSafeText(method, 60), "stage", operationsSafeText(path, 120), "kind", safeError(err)}
	if owner > 0 {
		attrs = append(attrs, "tenant", owner)
	}
	if te != nil {
		attrs = append(attrs, "http_status", te.Code, "retry_after", te.RetryAfter, "description", operationsSafeText(te.Description, 240))
	}
	var upstream *UpstreamError
	if errors.As(err, &upstream) {
		attrs = append(attrs, "http_status", upstream.Status, "retry_after", int64(upstream.RetryAfter.Seconds()))
	}
	slog.WarnContext(ctx, "API operation failed", attrs...)
}
