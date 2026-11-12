package companion

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
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
	requestedStage := path
	var upstream *UpstreamError
	if errors.As(err, &upstream) {
		if upstream.Endpoint != "" {
			path, _, _ = strings.Cut(upstream.Endpoint, "?")
		}
		if upstream.Method != "" {
			method = upstream.Method
		}
	}
	attrs := []any{"source", source, "method", operationsSafeText(method, 60), "stage", operationsSafeText(path, 120), "kind", safeError(err)}
	if owner > 0 {
		attrs = append(attrs, "tenant", owner)
	}
	if te != nil {
		attrs = append(attrs, "http_status", te.Code, "retry_after", te.RetryAfter, "description", operationsSafeText(te.Description, 240))
	}
	if upstream != nil {
		attrs = append(attrs, "http_status", upstream.Status, "retry_after", int64(upstream.RetryAfter.Seconds()))
		if requestedStage != path {
			attrs = append(attrs, "requested_stage", operationsSafeText(requestedStage, 120))
		}
		if safeURL, filtered := operationsUpstreamURL(upstream.RequestURL); safeURL != "" {
			attrs = append(attrs, "request_url", safeURL)
			if upstream.QueryFiltered || filtered {
				attrs = append(attrs, "query_filtered", true)
			}
		}
	}
	slog.WarnContext(ctx, "API operation failed", attrs...)
}

// Only these Jardulik API routes and typed, non-secret query values enter logs.
func operationsUpstreamURL(raw string) (string, bool) {
	if raw == "" || len(raw) > 4096 {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "api.jardulik.app" || u.User != nil || u.RawPath != "" || u.Fragment != "" {
		return "", false
	}
	switch u.Path {
	case "/api/user/getMe", "/api/user/getUser", "/api/user/getUsers", "/api/user/getUsersLocations", "/api/user/getReactUsers",
		"/api/appuser/refresh", "/api/appuser/login/telegram", "/api/GPT/checkCompatibility", "/api/reactions/addNew", "/api/telegram/getTG":
	default:
		return "", false
	}
	query := make(url.Values)
	parsed, err := url.ParseQuery(u.RawQuery)
	if err == nil {
		for key, values := range parsed {
			if len(values) == 1 && operationsSafeQueryValue(key, values[0]) {
				query.Set(key, values[0])
			}
		}
	}
	u.RawQuery = query.Encode()
	u.ForceQuery = false
	return u.String(), err != nil || len(query) != len(parsed)
}

func operationsSafeQueryValue(key, value string) bool {
	switch key {
	case "all":
		return value == "0" || value == "1"
	case "isMutual":
		return value == "true" || value == "false"
	case "isMan":
		return value == "true" || value == "false" || value == "0" || value == "1"
	case "goal", "children", "worldview":
		return value == "" || slices.Contains(filterOptions[key], value)
	case "country":
		_, known := countryLabels[value]
		return value == "" || known
	case "city":
		// City input is free text even when the filter itself is valid.
		return value == ""
	case "userId", "viewerUserId", "targetUserId", "pageNumber", "pageSize", "ageMin", "ageMax", "heightMin", "heightMax":
		if value == "" || len(value) > 19 || strings.Trim(value, "0123456789") != "" {
			return false
		}
		_, err := strconv.ParseUint(value, 10, 63)
		return err == nil
	default:
		return false
	}
}
