package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const debugAuditSchema = `CREATE TABLE IF NOT EXISTS debug_api_calls (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 request_id TEXT NOT NULL, upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 path TEXT NOT NULL, target_id INTEGER NOT NULL, reaction_type INTEGER NOT NULL,
 request_body BLOB NOT NULL, response_body BLOB NOT NULL DEFAULT X'', http_status INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('sending','received','rejected','cancelled','unknown')),
 error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(owner_id,request_id)
);`

type debugCall struct {
	ID                int64
	Upstream          int64
	Generation, Path  string
	Request, Response []byte
	Status            int
	State             string
}

func (d *debugHTTP) claim(ctx context.Context, ss Session, requestID, path string, body []byte, target, reaction int64) (debugCall, bool, error) {
	var out debugCall
	s := d.service.Store
	owner := d.service.Config.OwnerID
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return out, false, e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='debug_api_calls'"); e != nil {
		return out, false, e
	}
	e = tx.QueryRowContext(ctx, "SELECT id,upstream_id,generation,path,request_body,response_body,http_status,state FROM debug_api_calls WHERE owner_id=? AND request_id=?", owner, requestID).Scan(&out.ID, &out.Upstream, &out.Generation, &out.Path, &out.Request, &out.Response, &out.Status, &out.State)
	if e == nil {
		if out.Upstream != ss.UserID || out.Generation != ss.Generation || out.Path != path || !bytes.Equal(out.Request, body) {
			return out, false, errors.New("request_id_conflict")
		}
		return out, false, tx.Commit()
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return out, false, e
	}
	var blocked bool
	e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM debug_api_calls WHERE owner_id=? AND upstream_id=? AND path=? AND target_id=? AND reaction_type=? AND state IN ('sending','unknown'))", owner, ss.UserID, path, target, reaction).Scan(&blocked)
	if e != nil {
		return out, false, e
	}
	if blocked {
		return out, false, errors.New("previous_result_unconfirmed")
	}
	var currentID int64
	var state string
	if e = tx.QueryRowContext(ctx, "SELECT upstream_id,state FROM sessions WHERE owner_id=?", owner).Scan(&currentID, &state); e != nil || currentID != ss.UserID || state != "connected" {
		return out, false, ErrReactionSessionChanged
	}
	r, e := tx.ExecContext(ctx, "INSERT INTO debug_api_calls(owner_id,request_id,upstream_id,generation,path,target_id,reaction_type,request_body,state) VALUES(?,?,?,?,?,?,?,?,'sending')", owner, requestID, ss.UserID, ss.Generation, path, target, reaction, body)
	if e != nil {
		return out, false, e
	}
	out.ID, e = r.LastInsertId()
	if e != nil {
		return out, false, e
	}
	out.Upstream, out.Generation, out.Path, out.Request, out.State = ss.UserID, ss.Generation, path, body, "sending"
	return out, true, tx.Commit()
}
func (d *debugHTTP) finish(ctx context.Context, id int64, state string, status int, body []byte, reason string) error {
	if body == nil {
		body = []byte{}
	}
	result, e := d.service.Store.DB.ExecContext(ctx, "UPDATE debug_api_calls SET state=?,http_status=?,response_body=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='sending'", state, status, body, reason, id)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return errors.New("debug_result_not_committed")
	}
	return nil
}

func (c *Client) debugBinding(ctx context.Context, owner int64, ss Session) error {
	current, e := c.Store.Session(ctx, owner)
	if e != nil || current.State != "connected" || current.Generation != ss.Generation || current.UserID != ss.UserID || c.Store.Paused(ctx, owner) {
		return ErrReactionSessionChanged
	}
	return nil
}
func (c *Client) debugExchange(ctx context.Context, ss Session, method, path string, body []byte) (int, []byte, error) {
	req, e := http.NewRequestWithContext(ctx, method, c.Base+path, bytes.NewReader(body))
	if e != nil {
		return 0, nil, errors.New("invalid_debug_request")
	}
	upstreamHeaders(req, c.UserAgent, false)
	req.Header.Set("Authorization", "Bearer "+ss.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := *c.HTTP
	client.Timeout = 90 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, e := client.Do(req)
	if e != nil {
		return 0, nil, errors.New("debug_transport_unconfirmed")
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, debugResponseLimit+1))
	if e != nil || len(raw) > debugResponseLimit {
		return response.StatusCode, nil, errors.New("debug_response_unconfirmed")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		kind := "request_rejected"
		switch response.StatusCode {
		case 401:
			kind = "unauthorized"
		case 403:
			kind = "access_denied"
		case 429:
			kind = "rate_limited"
		}
		wait, _ := strconv.Atoi(response.Header.Get("Retry-After"))
		return response.StatusCode, raw, &UpstreamError{Status: response.StatusCode, Kind: kind, RetryAfter: time.Duration(wait) * time.Second}
	}
	if strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/html") || !json.Valid(raw) {
		return response.StatusCode, raw, &UpstreamError{Status: response.StatusCode, Kind: "unexpected_response_or_challenge"}
	}
	return response.StatusCode, raw, nil
}
func (c *Client) debugFailure(ctx context.Context, owner int64, ss Session, err error) {
	var upstream *UpstreamError
	if !errors.As(err, &upstream) {
		return
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	current, e := c.Store.Session(ctx, owner)
	if e != nil || current.Generation != ss.Generation || current.Token != ss.Token {
		return
	}
	if upstream.Status == 401 {
		c.Store.SessionState(ctx, owner, "reconnect_required", "debug_auth_not_confirmed")
	} else if upstream.Status == 403 || upstream.Status == 429 || upstream.Kind == "unexpected_response_or_challenge" {
		c.Store.SessionState(ctx, owner, "paused", upstream.Kind)
	}
}
func (c *Client) debugRead(ctx context.Context, owner int64, expected Session, path string) (int, []byte, error) {
	current, e := c.token(ctx, owner, "")
	if e != nil {
		return 0, nil, e
	}
	if current.UserID != expected.UserID || current.Generation != expected.Generation {
		return 0, nil, ErrReactionSessionChanged
	}
	status, body, e := c.debugExchange(ctx, current, http.MethodGet, path, nil)
	var upstream *UpstreamError
	if errors.As(e, &upstream) && upstream.Status == 401 {
		current, e = c.token(ctx, owner, current.Token, current.Generation)
		if e == nil {
			status, body, e = c.debugExchange(ctx, current, http.MethodGet, path, nil)
		}
	}
	if bind := c.debugBinding(ctx, owner, current); bind != nil {
		return 0, nil, bind
	}
	c.debugFailure(ctx, owner, current, e)
	return status, body, e
}
func (c *Client) debugPost(ctx context.Context, owner int64, expected Session, path string, body []byte) (int, []byte, error) {
	if _, e := c.token(ctx, owner, ""); e != nil {
		return 0, nil, e
	}
	mu := c.lock(owner)
	mu.Lock()
	if e := c.debugBinding(ctx, owner, expected); e != nil {
		mu.Unlock()
		return 0, nil, e
	}
	current, e := c.Store.Session(ctx, owner)
	if e != nil {
		mu.Unlock()
		return 0, nil, e
	}
	if path == "/reactions/addNew" && !c.Store.cfg.AllowReactions {
		mu.Unlock()
		return 0, nil, ErrReactionDisabled
	}
	status, response, e := c.debugExchange(ctx, current, http.MethodPost, path, body)
	mu.Unlock()
	c.debugFailure(context.WithoutCancel(ctx), owner, current, e)
	return status, response, e
}
func (d *debugHTTP) hasSecret(body []byte, initial Session) bool {
	secrets := []string{initial.Token, initial.Refresh, d.service.Config.Token}
	if current, e := d.service.Store.Session(context.Background(), d.service.Config.OwnerID); e == nil {
		secrets = append(secrets, current.Token, current.Refresh)
	}
	for _, secret := range secrets {
		if len(secret) >= 8 && bytes.Contains(body, []byte(secret)) {
			return true
		}
	}
	return false
}
func debugProfile(raw []byte) (Profile, error) {
	body := bytes.TrimSpace(raw)
	if len(body) > 0 && body[0] == '[' {
		var profiles []json.RawMessage
		if json.Unmarshal(body, &profiles) != nil || len(profiles) != 1 {
			return nil, errors.New("profile_unavailable")
		}
		body = profiles[0]
	}
	return decodeProfile(body)
}
func (d *debugHTTP) write(w http.ResponseWriter, r *http.Request, ss Session, requestID, path string, body []byte, target, reaction int64) {
	s := d.service
	owner := s.Config.OwnerID
	call, fresh, e := d.claim(r.Context(), ss, requestID, path, body, target, reaction)
	if e != nil {
		debugError(w, 409, "request_conflict_or_unconfirmed")
		return
	}
	if !fresh {
		if call.State == "received" || call.State == "rejected" {
			w.Header().Set("X-Jardulik-Debug-Replay", "true")
			d.reply(w, call.Status, call.Response, ss)
		} else {
			debugError(w, 409, "previous_result_unconfirmed")
		}
		return
	}
	finish := func(state string, status int, response []byte, reason string) bool {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if e = d.finish(ctx, call.ID, state, status, response, reason); e != nil {
			s.Store.Pause(ctx, owner, true)
			debugError(w, 503, "debug_result_not_committed")
			return false
		}
		return true
	}
	status, ownRaw, e := s.Client.debugRead(r.Context(), owner, ss, "/user/getMe?userId="+strconv.FormatInt(ss.UserID, 10))
	own, decodeErr := debugProfile(ownRaw)
	frozen, knownFrozen := own["freezeState"].(bool)
	if e != nil || status != 200 || decodeErr != nil || own.ID() != ss.UserID || !knownFrozen || frozen {
		if finish("cancelled", 0, nil, "account_preflight_failed") {
			debugError(w, 409, "account_preflight_failed")
		}
		return
	}
	status, targetRaw, e := s.Client.debugRead(r.Context(), owner, ss, "/user/getUser?viewerUserId="+strconv.FormatInt(ss.UserID, 10)+"&targetUserId="+strconv.FormatInt(target, 10))
	targetProfile, decodeErr := debugProfile(targetRaw)
	age, adult := autoInteger(targetProfile["age"])
	if e != nil || status != 200 || decodeErr != nil || targetProfile.ID() != target || !adult || age < 18 {
		if finish("cancelled", 0, nil, "target_preflight_failed") {
			debugError(w, 409, "target_preflight_failed")
		}
		return
	}
	if reaction == 1 {
		mu := s.Store.SessionMutex(owner)
		mu.Lock()
		archiveErr := s.Client.debugBinding(r.Context(), owner, ss)
		if archiveErr == nil {
			_, archiveErr = s.Store.ArchiveViewed(r.Context(), owner, []Profile{targetProfile}, "GET /user/getUser (debug like)")
		}
		mu.Unlock()
		if archiveErr != nil {
			if finish("cancelled", 0, nil, "target_archive_failed") {
				debugError(w, 409, "target_archive_failed")
			}
			return
		}
		// Keep the existing shared action ledger, account budget and unknown-result stop.
		result, err := s.Client.LikeGuarded(r.Context(), owner, target, func(ctx context.Context) error {
			if e := s.Client.debugBinding(ctx, owner, ss); e != nil {
				return e
			}
			return s.ManualLikeBudgetGuard(ctx, owner)
		})
		status, body, e = 200, result.Raw, err
		if len(body) == 0 {
			status = 0
		}
	} else {
		status, body, e = s.Client.debugPost(r.Context(), owner, ss, path, body)
	}
	if d.hasSecret(body, ss) {
		body = nil
		e = errors.New("credential_in_upstream_response")
		status = 0
	}
	state, reason := "received", ""
	if e != nil {
		state, reason = "unknown", "upstream_result_unconfirmed"
		var upstream *UpstreamError
		if errors.As(e, &upstream) && upstream.Status >= 400 && upstream.Status < 500 && upstream.Status != 401 {
			state = "rejected"
			reason = upstream.Kind
		}
		if errors.Is(e, ErrActionBlocked) || errors.Is(e, ErrOwnLikeLimit) || errors.Is(e, ErrReactionPaused) || errors.Is(e, ErrReactionDisabled) || errors.Is(e, ErrReactionSessionChanged) || errors.Is(e, ErrAccountFrozen) {
			state = "cancelled"
			reason = "action_preflight_rejected"
		}
	} else if reaction != 0 {
		result, decodeErr := decodeReactionResult(body, s.Client.Now())
		if decodeErr != nil {
			state, reason = "unknown", "reaction_result_unconfirmed"
		} else if result.Decision != ReactionSuccess {
			state, reason = "rejected", string(result.Decision)
		}
	}
	if state == "unknown" {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		s.Store.Pause(ctx, owner, true)
		cancel()
	}
	if !finish(state, status, body, reason) {
		return
	}
	if reaction == 0 && state != "cancelled" {
		input := AnalysisCaptureInput{UpstreamID: ss.UserID, ProfileID: target, Method: http.MethodPost, Path: path, ObservedAtMillis: s.Client.Now().UnixMilli(), HTTPStatus: status, State: "response_received", Generation: ss.Generation, RequestBody: call.Request, ResponseBody: body, OwnProfile: ownRaw, TargetProfile: targetRaw}
		if status == 0 {
			input.State = "transport_unknown"
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		_, saveErr := s.Store.SaveAnalysisCapture(ctx, owner, input)
		cancel()
		if saveErr != nil {
			w.Header().Set("X-Jardulik-Analysis-State", "not_saved")
		} else {
			w.Header().Set("X-Jardulik-Analysis-State", "saved")
		}
	}
	w.Header().Set("X-Jardulik-Debug-State", state)
	if status == 0 {
		if state == "cancelled" {
			debugError(w, 409, "action_not_sent")
			return
		}
		debugError(w, 502, "upstream_result_unconfirmed")
		return
	}
	d.reply(w, status, body, ss)
}
