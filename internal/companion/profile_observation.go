package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Observe missing targets once, regardless of which worker requested the profile.
func (c *Client) observeProfileUnavailable(ctx context.Context, owner int64, expected Session, method, path string, raw []byte, requestErr error, observed time.Time) error {
	parts := strings.SplitN(path, "?", 2)
	if method != http.MethodGet || parts[0] != "/user/getUser" || len(parts) != 2 {
		return requestErr
	}
	missing := errors.Is(requestErr, ErrProfileUnavailable)
	var upstream *UpstreamError
	if errors.As(requestErr, &upstream) && upstream.Endpoint == parts[0] && (upstream.Status == http.StatusNotFound || upstream.Status == http.StatusGone) {
		missing = true
	}
	if requestErr == nil {
		var entries []json.RawMessage
		missing = json.Unmarshal(raw, &entries) == nil && len(entries) == 0
	}
	if !missing {
		return requestErr
	}
	query, err := url.ParseQuery(parts[1])
	if err != nil || len(query["viewerUserId"]) != 1 || len(query["targetUserId"]) != 1 {
		return requestErr
	}
	viewer, err := strconv.ParseInt(query.Get("viewerUserId"), 10, 64)
	if err != nil || viewer != expected.UserID {
		return requestErr
	}
	target, err := strconv.ParseInt(query.Get("targetUserId"), 10, 64)
	if err != nil || target <= 0 {
		return requestErr
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	if err = c.debugBinding(ctx, owner, expected); err != nil {
		return err
	}
	if _, err = c.Store.recordProfilePresence(ctx, owner, target, false, observed); err != nil {
		// Do not expose the missing sentinel until its durable event has committed.
		slog.Warn("profile availability not saved", "kind", safeError(err))
		return fmt.Errorf("save profile availability: %w", err)
	}
	return ErrProfileUnavailable
}
