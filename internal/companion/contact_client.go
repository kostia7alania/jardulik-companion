package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const contactResponseLimit = 16 << 20

var ErrContactPersistence = errors.New("contact persistence unavailable")

func contactPersistence(err error) error {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return errors.Join(ErrContactPersistence, err)
}

// RequestContact asks for one official card; only its incoming receipt proves a contact.
func (c *Client) RequestContact(ctx context.Context, job ContactJob) error {
	ready, err := c.Store.ContactJobReady(ctx, job, c.Now())
	if err != nil || !ready {
		return contactPersistence(err)
	}
	initial, err := c.Store.Session(ctx, job.OwnerID)
	if err != nil {
		return contactPersistence(err)
	}
	if initial.State != "connected" || initial.UserID != job.UpstreamID || initial.Generation != job.Generation || c.Store.Paused(ctx, job.OwnerID) {
		return ErrReactionSessionChanged
	}
	account, err := c.ContactAccount(ctx, job.OwnerID, initial)
	if err != nil {
		return c.deferContactPreflight(ctx, job, err)
	}
	if value, exists := account["freezeState"]; exists && value != nil {
		frozen, ok := value.(bool)
		if !ok {
			return errors.New("неизвестное состояние аккаунта")
		}
		if frozen {
			return ErrAccountFrozen
		}
	}
	// Any needed refresh finishes before the side effect; a rejected POST is never replayed.
	if _, err = c.token(ctx, job.OwnerID, ""); err != nil {
		return c.deferContactPreflight(ctx, job, err)
	}
	requestBody, err := json.Marshal(struct {
		FromUserID   int64 `json:"fromUserId"`
		TargetUserID int64 `json:"targetUserId"`
	}{job.UpstreamID, job.ProfileID})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/telegram/getTG", bytes.NewReader(requestBody))
	if err != nil {
		return errors.New("invalid contact request")
	}
	mu := c.lock(job.OwnerID)
	mu.Lock()
	defer mu.Unlock()
	current, err := c.Store.Session(ctx, job.OwnerID)
	if err != nil {
		return contactPersistence(err)
	}
	if current.State != "connected" || current.UserID != initial.UserID || current.Generation != initial.Generation || current.UserID != job.UpstreamID || current.Generation != job.Generation {
		return ErrReactionSessionChanged
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	claimed, err := c.Store.ClaimContactJob(ctx, job.ID, current, requestBody, c.Now())
	if err != nil || !claimed {
		return contactPersistence(err)
	}
	latest, checkErr := c.Store.Session(ctx, job.OwnerID)
	if checkErr != nil || latest.State != "connected" || latest.UserID != current.UserID || latest.Generation != current.Generation || latest.Token != current.Token || c.Store.Paused(ctx, job.OwnerID) || ctx.Err() != nil {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return errors.Join(ErrReactionSessionChanged, contactPersistence(checkErr), contactPersistence(c.Store.FinishContactJob(writeCtx, job.ID, "cancelled", 0, nil, "lifecycle_changed_before_send", c.Now())))
	}
	upstreamHeaders(request, c.UserAgent, false)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+current.Token)
	status, body, reason, sendErr := c.sendContact(request)
	state := "awaiting_receipt"
	var upstream *UpstreamError
	if sendErr != nil {
		state = "unknown"
		if errors.As(sendErr, &upstream) && upstream.Status >= 400 && upstream.Status < 500 && upstream.Kind != "unexpected_response_or_challenge" && upstream.Status != http.StatusUnauthorized {
			state = "rejected"
		}
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var sessionErr error
	if errors.As(sendErr, &upstream) {
		if upstream.Status == http.StatusUnauthorized {
			sessionErr = c.Store.failContactSession(writeCtx, job.OwnerID, current, "reconnect_required", "contact_auth_not_confirmed")
		} else if upstream.Status == http.StatusTooManyRequests || upstream.Status == http.StatusForbidden && upstream.Kind != "recipient_restricted" || upstream.Kind == "unexpected_response_or_challenge" {
			sessionErr = c.Store.failContactSession(writeCtx, job.OwnerID, current, "paused", upstream.Kind)
		}
	}
	return errors.Join(sendErr, contactPersistence(sessionErr), contactPersistence(c.Store.FinishContactJob(writeCtx, job.ID, state, status, body, reason, c.Now())))
}

// ContactAccount keeps a read-only preflight pinned while allowing a daily freeze-state refresh.
func (c *Client) ContactAccount(ctx context.Context, owner int64, expected Session) (Profile, error) {
	current, err := c.Store.Session(ctx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrReactionSessionChanged
	}
	if err != nil {
		return nil, contactPersistence(err)
	}
	if current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation || c.Store.Paused(ctx, owner) {
		return nil, ErrReactionSessionChanged
	}
	raw, err := c.requestSession(ctx, owner, http.MethodGet, "/user/getMe?userId="+strconv.FormatInt(expected.UserID, 10), nil, &expected)
	if err != nil {
		var sqliteErr interface{ Code() int }
		if errors.As(err, &sqliteErr) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, sql.ErrTxDone) {
			err = contactPersistence(err)
		}
		return nil, err
	}
	account, err := decodeProfile(raw)
	if err != nil || account.ID() != expected.UserID {
		latest, bindingErr := c.Store.Session(ctx, owner)
		if bindingErr == nil && latest.UserID == expected.UserID && latest.Generation == expected.Generation {
			bindingErr = c.Store.failContactSession(ctx, owner, latest, "paused", "contact_account_identity_not_confirmed")
		}
		return nil, errors.Join(&UpstreamError{Status: http.StatusOK, Kind: "unexpected_response_or_challenge", Endpoint: "/user/getMe"}, contactPersistence(bindingErr))
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	tx, err := c.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, contactPersistence(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_requests'"); err != nil {
		return nil, contactPersistence(err)
	}
	current, err = c.Store.businessSession(ctx, tx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrReactionSessionChanged
	}
	if err != nil {
		return nil, contactPersistence(err)
	}
	if current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return nil, ErrReactionSessionChanged
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sessions SET account_json=? WHERE owner_id=?", string(raw), owner); err != nil {
		return nil, contactPersistence(err)
	}
	if err = tx.Commit(); err != nil {
		return nil, contactPersistence(err)
	}
	return account, nil
}

func (c *Client) deferContactPreflight(ctx context.Context, job ContactJob, cause error) error {
	var sqliteErr interface{ Code() int }
	if errors.As(cause, &sqliteErr) || errors.Is(cause, sql.ErrConnDone) || errors.Is(cause, sql.ErrTxDone) {
		return contactPersistence(cause)
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return errors.Join(cause, contactPersistence(c.Store.DeferContactJob(writeCtx, job.ID, "preflight_unavailable", c.Now().Add(24*time.Hour))))
}

func (s *Store) failContactSession(ctx context.Context, owner int64, expected Session, state, reason string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='contact_requests'"); err != nil {
		return err
	}
	current, err := s.businessSession(ctx, tx, owner)
	if err != nil {
		return err
	}
	if current.UserID != expected.UserID || current.Generation != expected.Generation || current.Token != expected.Token || current.State != "connected" {
		return nil
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sessions SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=?", state, reason, owner); err != nil {
		return err
	}
	return tx.Commit()
}

func (c *Client) sendContact(request *http.Request) (status int, body []byte, reason string, err error) {
	response, err := c.HTTP.Do(request)
	if err != nil {
		return 0, nil, "transport_unknown", fmt.Errorf("Jardulik contact transport: %w", redactedTransportError(err))
	}
	defer response.Body.Close()
	status = response.StatusCode
	body, err = io.ReadAll(io.LimitReader(response.Body, contactResponseLimit+1))
	if err != nil {
		return status, body, "response_interrupted", errors.New("Jardulik contact response interrupted")
	}
	if len(body) > contactResponseLimit {
		return status, body[:contactResponseLimit], "response_too_large", &UpstreamError{Status: status, Kind: "unexpected_response_or_challenge", Endpoint: "/telegram/getTG"}
	}
	prefix := bytes.TrimSpace(body)
	if len(prefix) > 32 {
		prefix = prefix[:32]
	}
	prefix = bytes.ToLower(prefix)
	html := strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/html") || bytes.HasPrefix(prefix, []byte("<!doctype html")) || bytes.HasPrefix(prefix, []byte("<html"))
	if html || status >= 300 && status < 400 {
		return status, body, "unexpected_response_or_challenge", &UpstreamError{Status: status, Kind: "unexpected_response_or_challenge", Endpoint: "/telegram/getTG"}
	}
	if status >= 200 && status < 300 {
		return status, body, "", nil
	}
	kind := "request_rejected"
	switch status {
	case http.StatusUnauthorized:
		kind = "unauthorized"
	case http.StatusForbidden:
		kind = "access_denied"
		if recipientRestricted(body) {
			kind = "recipient_restricted"
		}
	case http.StatusTooManyRequests:
		kind = "rate_limited"
	}
	var retry time.Duration
	if seconds, _ := strconv.ParseInt(response.Header.Get("Retry-After"), 10, 32); seconds > 0 {
		retry = time.Duration(seconds) * time.Second
	}
	return status, body, kind, &UpstreamError{Status: status, Kind: kind, RetryAfter: retry, Endpoint: "/telegram/getTG"}
}

func recipientRestricted(body []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	found := false
	for _, key := range []string{"code", "Code"} {
		raw, exists := fields[key]
		if !exists {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil || value != "recipient_restricted" {
			return false
		}
		found = true
	}
	return found
}
