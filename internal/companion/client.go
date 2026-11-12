package companion

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrProfileUnavailable = errors.New("Jardulik profile unavailable")

type UpstreamError struct {
	Status        int
	Kind          string
	RetryAfter    time.Duration
	Endpoint      string
	Method        string
	RequestURL    string
	QueryFiltered bool
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("Jardulik: %s (HTTP %d)", e.Kind, e.Status)
}

type Client struct {
	Store     *Store
	Base      string
	UserAgent string
	HTTP      *http.Client
	locks     sync.Map
	Now       func() time.Time
	cities    filterCityCache
}

func NewClient(s *Store, c Config) *Client {
	return &Client{Store: s, Base: strings.TrimRight(c.APIBase, "/"), UserAgent: c.UserAgent, HTTP: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}, Now: time.Now}
}
func (c *Client) lock(owner int64) *sync.Mutex {
	return c.Store.SessionMutex(owner)
}

// Keep this file in place: unlinking it would let another process lock a different inode.
func (c *Client) sessionFileLock(ctx context.Context, owner int64) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(c.Store.cfg.DataDir, fmt.Sprintf("session-%d.lock", owner)), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("session lock is not a regular file")
	}
	if err == nil {
		err = f.Chmod(0600)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) exchange(ctx context.Context, method, path, token string, payload any) ([]byte, error) {
	if method == http.MethodGet && crawlShouldYield(ctx) {
		return nil, errCrawlYield
	}
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return nil, errors.New("invalid upstream request")
	}
	upstreamHeaders(r, c.UserAgent, false)
	if payload != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := c.HTTP.Do(r)
	if err != nil {
		return nil, fmt.Errorf("Jardulik transport: %w", redactedTransportError(err))
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return nil, errors.New("Jardulik response interrupted")
	}
	failure := func(kind string, retryAfter time.Duration) *UpstreamError {
		requestURL, filtered := operationsUpstreamURL(r.URL.String())
		return &UpstreamError{Status: res.StatusCode, Kind: kind, RetryAfter: retryAfter, Endpoint: strings.SplitN(path, "?", 2)[0], Method: r.Method, RequestURL: requestURL, QueryFiltered: filtered}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		kind := "request_rejected"
		if res.StatusCode == 401 {
			kind = "unauthorized"
		}
		if res.StatusCode == 403 {
			kind = "access_denied"
		}
		if res.StatusCode == 429 {
			kind = "rate_limited"
		}
		d := time.Duration(0)
		if n, _ := strconv.Atoi(res.Header.Get("Retry-After")); n > 0 {
			d = time.Duration(n) * time.Second
		}
		return nil, failure(kind, d)
	}
	if profileNoContent(method, path, res.StatusCode, res.Header.Get("Content-Type"), b) {
		return nil, ErrProfileUnavailable
	}
	if strings.Contains(strings.ToLower(res.Header.Get("Content-Type")), "text/html") || !json.Valid(b) {
		return nil, failure("unexpected_response_or_challenge", 0)
	}
	return b, nil
}

func profileNoContent(method, path string, status int, contentType string, body []byte) bool {
	return method == http.MethodGet && strings.SplitN(path, "?", 2)[0] == "/user/getUser" &&
		status == http.StatusNoContent && len(bytes.TrimSpace(body)) == 0 &&
		!strings.Contains(strings.ToLower(contentType), "text/html")
}

// URL errors can contain credentials in paths. Never propagate them into chat or logs.
func redactedTransportError(err error) error {
	var u *url.Error
	if errors.As(err, &u) {
		return u.Err
	}
	return err
}

func (c *Client) Login(ctx context.Context, owner int64, raw string) error {
	_, err := c.LoginAccount(ctx, owner, raw)
	return err
}

// LoginAccount keeps the previous session until the new identity is verified and committed.
func (c *Client) LoginAccount(ctx context.Context, owner int64, raw string) (Profile, error) {
	if err := ValidateInitData(raw, c.Store.cfg.SourceBotID, owner, c.Now(), c.Store.cfg.InitMaxAge); err != nil {
		return nil, err
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	fileLock, err := c.sessionFileLock(ctx, owner)
	if err != nil {
		return nil, err
	}
	defer fileLock.Close()
	var previous []byte
	var upstream int64
	var state, reason string
	err = c.Store.DB.QueryRowContext(ctx, "SELECT encrypted,upstream_id,state,last_error FROM sessions WHERE owner_id=?", owner).Scan(&previous, &upstream, &state, &reason)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return nil, err
	}
	b, err := c.exchange(ctx, http.MethodPost, "/appuser/login/telegram", "", map[string]string{"initData": raw})
	if err != nil {
		return nil, err
	}
	session, err := decodeLoginResponse(b)
	if err != nil {
		return nil, err
	}
	accountRaw, err := c.exchange(ctx, http.MethodGet, "/user/getMe?userId="+strconv.FormatInt(session.UserID, 10), session.Token, nil)
	if err != nil {
		return nil, err
	}
	account, err := decodeProfile(accountRaw)
	if err != nil {
		return nil, err
	}
	if account.ID() != session.UserID {
		return nil, errors.New("Jardulik account identity changed")
	}
	var generation [16]byte
	if _, err = rand.Read(generation[:]); err != nil {
		return nil, err
	}
	session.Generation = hex.EncodeToString(generation[:])
	encoded, err := json.Marshal(session)
	if err != nil {
		return nil, err
	}
	encoded, err = seal(c.Store.cfg.SessionKey, encoded, sessionPurpose(owner))
	if err != nil {
		return nil, err
	}
	tx, err := c.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var result sql.Result
	if missing {
		result, err = tx.ExecContext(ctx, `INSERT INTO sessions(owner_id,upstream_id,encrypted,expires_at,state,account_json) VALUES(?,?,?,?,'connected',?) ON CONFLICT(owner_id) DO NOTHING`, owner, session.UserID, encoded, session.Expires, string(accountRaw))
	} else {
		// A second process may have paused, replaced or disconnected this binding during HTTP.
		result, err = tx.ExecContext(ctx, `UPDATE sessions SET upstream_id=?,encrypted=?,expires_at=?,state='connected',last_error='',account_json=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND encrypted=? AND upstream_id=? AND state=? AND last_error=?`, session.UserID, encoded, session.Expires, string(accountRaw), owner, previous, upstream, state, reason)
	}
	if err != nil {
		return nil, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if changed != 1 {
		return nil, errors.New("сессия изменилась; подключение отменено")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO events(owner_id,kind) VALUES(?,'connect_success')", owner); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return account, nil
}

func decodeLoginResponse(b []byte) (Session, error) {
	var response struct {
		Token, RefreshToken string
		UserID              int64 `json:"userId"`
		Code                int   `json:"code"`
		IsReged             *bool `json:"isReged"`
	}
	err := json.Unmarshal(b, &response)
	// The live API returns 201 + isReged=true + "Login successful" for an
	// existing registered account. Other onboarding/moderation codes stay blocked.
	allowed := response.Code == 200 || (response.Code == 201 && response.IsReged != nil && *response.IsReged)
	if err != nil || response.Token == "" || response.RefreshToken == "" || response.UserID <= 0 || !allowed || (response.IsReged != nil && !*response.IsReged) {
		return Session{}, errors.New("Jardulik не подтвердил вход: закончи регистрацию или проверку в официальном приложении")
	}
	return Session{Token: response.Token, Refresh: response.RefreshToken, UserID: response.UserID, Expires: tokenExpiry(response.Token)}, nil
}

func (c *Client) refreshLocked(ctx context.Context, owner int64, s Session) (Session, error) {
	var previous []byte
	var upstream int64
	var state, reason string
	err := c.Store.DB.QueryRowContext(ctx, "SELECT encrypted,upstream_id,state,last_error FROM sessions WHERE owner_id=?", owner).Scan(&previous, &upstream, &state, &reason)
	if err != nil {
		return s, err
	}
	if state != "connected" || upstream != s.UserID || !bytes.Equal(previous, s.encrypted) {
		return s, errors.New("сессия изменилась; старый запрос отменён")
	}
	// Other actors may pause or replace the session during HTTP; only this exact snapshot may change.
	pause := func(failureReason string) {
		_, _ = c.Store.DB.ExecContext(ctx, `UPDATE sessions SET state='reconnect_required',last_error=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND encrypted=? AND upstream_id=? AND state='connected' AND last_error=?`, failureReason, owner, previous, upstream, reason)
	}
	b, err := c.exchange(ctx, "PUT", "/appuser/refresh", "", map[string]string{"token": s.Token, "refreshToken": s.Refresh})
	if err != nil {
		pause("refresh_not_confirmed")
		return s, err
	}
	var r struct{ Token, RefreshToken string }
	if json.Unmarshal(b, &r) != nil || r.Token == "" || r.RefreshToken == "" {
		pause("refresh_format_changed")
		return s, errors.New("invalid refresh response")
	}
	s.Token = r.Token
	s.Refresh = r.RefreshToken
	s.Expires = tokenExpiry(r.Token)
	encoded, err := json.Marshal(s)
	if err != nil {
		return s, err
	}
	encoded, err = seal(c.Store.cfg.SessionKey, encoded, sessionPurpose(owner))
	if err != nil {
		return s, err
	}
	result, err := c.Store.DB.ExecContext(ctx, `UPDATE sessions SET encrypted=?,expires_at=?,state='connected',last_error='',updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND encrypted=? AND upstream_id=? AND state='connected' AND last_error=?`, encoded, s.Expires, owner, previous, upstream, reason)
	if err != nil {
		return s, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return s, err
	}
	if changed != 1 {
		return s, errors.New("сессия изменилась; старый запрос отменён")
	}
	s.encrypted = encoded
	return s, nil
}

func (c *Client) token(ctx context.Context, owner int64, after401 string, generation ...string) (Session, error) {
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	fileLock, err := c.sessionFileLock(ctx, owner)
	if err != nil {
		return Session{}, err
	}
	defer fileLock.Close()
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return s, errors.New("сначала подключи свой Jardulik через /connect")
	}
	if s.State != "connected" {
		return s, errors.New("сессия приостановлена; нужно повторное подключение /connect")
	}
	if after401 != "" && len(generation) > 0 && s.Generation != generation[0] {
		return s, errors.New("сессия изменилась; старый запрос отменён")
	}
	if after401 != "" && s.Token != after401 {
		return s, nil
	}
	if after401 != "" || (s.Expires > 0 && s.Expires <= c.Now().Add(30*time.Second).Unix()) {
		return c.refreshLocked(ctx, owner, s)
	}
	return s, nil
}

func (c *Client) Request(ctx context.Context, owner int64, method, path string, payload any) ([]byte, error) {
	return c.requestSession(ctx, owner, method, path, payload, nil)
}

func (c *Client) requestSession(ctx context.Context, owner int64, method, path string, payload any, expected *Session) ([]byte, error) {
	raw, _, err := c.requestSessionObserved(ctx, owner, method, path, payload, expected)
	return raw, err
}

func (c *Client) requestSessionObserved(ctx context.Context, owner int64, method, path string, payload any, expected *Session) (raw []byte, observedAt time.Time, requestErr error) {
	defer func() { logOperationsFailure(ctx, "Jardulik", method, path, owner, requestErr) }()
	s, err := c.token(ctx, owner, "")
	if err != nil {
		return nil, time.Time{}, err
	}
	if expected != nil && (s.Generation != expected.Generation || s.UserID != expected.UserID) {
		return nil, time.Time{}, errors.New("сессия изменилась; старый запрос отменён")
	}
	b, err := c.exchange(ctx, method, path, s.Token, payload)
	var ue *UpstreamError
	if errors.As(err, &ue) && ue.Status == 401 {
		s, err = c.token(ctx, owner, s.Token, s.Generation)
		if err != nil {
			return nil, time.Time{}, err
		}
		b, err = c.exchange(ctx, method, path, s.Token, payload)
	}
	observed := c.Now()
	if errors.As(err, &ue) && (ue.Status == 403 || ue.Status == 429 || ue.Kind == "unexpected_response_or_challenge") {
		mu := c.lock(owner)
		mu.Lock()
		c.Store.setSessionStateIfCurrent(ctx, owner, s, "paused", ue.Kind)
		mu.Unlock()
	}
	return b, observed, c.observeProfileUnavailable(ctx, owner, s, method, path, b, err, observed)
}
func decodeProfile(b []byte) (Profile, error) {
	var p Profile
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	err := d.Decode(&p)
	return p, err
}
func (c *Client) Me(ctx context.Context, owner int64) (Profile, error) {
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, errors.New("сначала /connect")
	}
	return c.meForSession(ctx, owner, s)
}

func (c *Client) meForSession(ctx context.Context, owner int64, s Session) (Profile, error) {
	b, err := c.requestSession(ctx, owner, "GET", "/user/getMe?userId="+strconv.FormatInt(s.UserID, 10), nil, &s)
	if err != nil {
		return nil, err
	}
	p, err := decodeProfile(b)
	if err != nil {
		return nil, err
	}
	if p.ID() != s.UserID {
		return nil, errors.New("Jardulik account identity changed")
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	current, e := c.Store.Session(ctx, owner)
	if e != nil || current.Generation != s.Generation || current.UserID != s.UserID {
		return nil, errors.New("сессия изменилась; старый ответ отменён")
	}
	err = c.Store.SaveAccount(ctx, owner, p)
	return p, err
}
func (c *Client) Users(ctx context.Context, owner int64, page int) ([]Profile, error) {
	if page < 1 {
		return nil, errors.New("invalid discovery page")
	}
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, err
	}
	state, err := c.Store.filterState(ctx, owner)
	if err != nil {
		return nil, err
	}
	account, err := c.Me(ctx, owner)
	if err != nil {
		return nil, err
	}
	filters := existingFeedFilters(account)
	if state.Applied != nil {
		if number(account["type"]) != 1 {
			return nil, ErrFiltersPremium
		}
		filters = *state.Applied
	}
	return c.usersPage(ctx, owner, s, filters, page, state.Others)
}

// UsersFrozen never substitutes mutable account preferences or UI filters.
func (c *Client) UsersFrozen(ctx context.Context, owner int64, expected Session, filters FeedFilters, page int, others bool) ([]Profile, error) {
	if page < 1 {
		return nil, errors.New("invalid discovery page")
	}
	if err := filters.validate(); err != nil {
		return nil, err
	}
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, err
	}
	if s.UserID != expected.UserID || s.Generation != expected.Generation {
		return nil, errors.New("сессия изменилась; обход остановлен")
	}
	account, err := c.Me(ctx, owner)
	if err != nil {
		return nil, err
	}
	if number(account["type"]) != 1 {
		return nil, ErrFiltersPremium
	}
	if c.Store.Paused(ctx, owner) {
		return nil, errors.New("аккаунт на паузе; обход остановлен")
	}
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return nil, errors.New("сессия изменилась; обход остановлен")
	}
	return c.usersPage(ctx, owner, expected, filters, page, others)
}

func (c *Client) usersPage(ctx context.Context, owner int64, expected Session, filters FeedFilters, page int, others bool) ([]Profile, error) {
	if page < 1 {
		return nil, errors.New("invalid discovery page")
	}
	if err := filters.validate(); err != nil {
		return nil, err
	}
	q := filters.query(expected.UserID, page, others)
	b, err := c.requestSession(ctx, owner, "GET", "/user/getUsers?"+q.Encode(), nil, &expected)
	if err != nil {
		return nil, err
	}
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.Generation != expected.Generation || current.UserID != expected.UserID || current.State != "connected" {
		return nil, errors.New("сессия изменилась; старый ответ отменён")
	}
	if trimmed := bytes.TrimSpace(b); len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, errors.New("Jardulik profile list format changed")
	}
	var profiles []Profile
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&profiles); err != nil {
		return nil, errors.New("Jardulik profile list format changed")
	}
	return profiles, nil
}
func (c *Client) FullProfile(ctx context.Context, owner, id int64) (Profile, error) {
	profile, _, err := c.fullProfileObserved(ctx, owner, id)
	return profile, err
}

func (c *Client) fullProfileObserved(ctx context.Context, owner, id int64) (Profile, time.Time, error) {
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, time.Time{}, err
	}
	p, _, observed, err := c.checkedProfileObserved(ctx, owner, s, id)
	return p, observed, err
}

type ReactionResult struct {
	Success, Mutual, Last        bool
	Kind, HoursLeft, MinutesLeft int64
	Raw                          []byte
	Decision                     ReactionDecision
	Wait                         time.Duration
	RetryAt                      time.Time
	Exhausted                    bool
}

type ReactionDecision string

const (
	ReactionSuccess         ReactionDecision = "success"
	ReactionQuota           ReactionDecision = "quota"
	ReactionPremiumRequired ReactionDecision = "premium_required"
	ReactionFireUnavailable ReactionDecision = "fire_unavailable"
	ReactionExhausted       ReactionDecision = "exhausted"
	ReactionUnknown         ReactionDecision = "unknown"
)

var (
	ErrReactionDisabled       = errors.New("лайки выключены администратором")
	ErrReactionPaused         = errors.New("действия приостановлены: /resume")
	ErrAccountFrozen          = errors.New("анкета заморожена в Jardulik")
	ErrActionBlocked          = errors.New("реакция уже отправлялась или её повтор запрещён")
	ErrReactionSessionChanged = errors.New("сессия изменилась или требует подключения")
)

type claimedLikeContextKey struct{}

// ClaimedLikeActionID lets a post-claim DB guard exclude its own reservation
// from an account budget. No caller may claim or release this action itself.
func ClaimedLikeActionID(ctx context.Context) int64 {
	id, _ := ctx.Value(claimedLikeContextKey{}).(int64)
	return id
}

func decodeReactionResult(body []byte, now time.Time) (ReactionResult, error) {
	out := ReactionResult{Decision: ReactionUnknown, Raw: body}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return out, errors.New("неизвестный формат ответа реакции")
	}
	conflict := false
	get := func(keys ...string) json.RawMessage {
		var found json.RawMessage
		for _, key := range keys {
			if raw, ok := fields[key]; ok {
				if found != nil && !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(found)) {
					conflict = true
				}
				found = raw
			}
		}
		return found
	}
	readBool := func(raw json.RawMessage, required bool) (bool, error) {
		raw = bytes.TrimSpace(raw)
		if !required && (raw == nil || bytes.Equal(raw, []byte("null"))) {
			return false, nil
		}
		if !bytes.Equal(raw, []byte("true")) && !bytes.Equal(raw, []byte("false")) {
			return false, errors.New("неизвестный флаг ответа реакции")
		}
		return bytes.Equal(raw, []byte("true")), nil
	}
	integer := func(raw json.RawMessage) (int64, bool) {
		if raw == nil {
			return 0, true
		}
		value := string(raw)
		if len(raw) > 0 && raw[0] == '"' {
			if json.Unmarshal(raw, &value) != nil {
				return 0, false
			}
		}
		n, err := strconv.ParseInt(value, 10, 64)
		return n, err == nil
	}
	var err error
	if out.Success, err = readBool(get("IsSuccess", "Success", "success"), true); err != nil {
		return out, err
	}
	if out.Mutual, err = readBool(get("IsMutual", "Mutual", "mutual"), false); err != nil {
		return out, err
	}
	if out.Last, err = readBool(get("IsLast", "Last", "last"), false); err != nil {
		return out, err
	}
	var kindValid bool
	out.Kind, kindValid = integer(get("MessageKind", "Kind", "kind"))
	if conflict || !out.Success && out.Mutual {
		return out, errors.New("противоречивый ответ реакции")
	}
	if out.Success {
		out.Decision = ReactionSuccess
		return out, nil // The frontend interprets Last only inside the failure branch.
	}
	out.Exhausted = out.Last
	if !kindValid {
		return out, errors.New("неизвестный код ответа реакции")
	}
	switch out.Kind {
	case -1:
		out.Decision = ReactionQuota
		hours, minutes := get("HoursLeft", "hoursLeft"), get("MinutesLeft", "minutesLeft")
		if conflict {
			out.Decision = ReactionUnknown
			return out, errors.New("противоречивый срок квоты")
		}
		var hOK, mOK bool
		out.HoursLeft, hOK = integer(hours)
		out.MinutesLeft, mOK = integer(minutes)
		if (hours != nil || minutes != nil) && hOK && mOK && out.HoursLeft >= 0 && out.MinutesLeft >= 0 && out.MinutesLeft < 60 && out.HoursLeft <= int64((1<<63-1)/int64(time.Hour)) {
			wait := time.Duration(out.HoursLeft) * time.Hour
			mins := time.Duration(out.MinutesLeft) * time.Minute
			if wait <= time.Duration(1<<63-1)-mins && wait+mins > 0 {
				out.Wait = wait + mins
				out.RetryAt = now.Add(out.Wait).UTC()
			}
		}
	case -2:
		out.Decision = ReactionFireUnavailable
	case -3:
		out.Decision = ReactionPremiumRequired
	default:
		if out.Exhausted {
			out.Decision = ReactionExhausted
		}
	}
	if out.Decision == ReactionUnknown {
		return out, errors.New("неизвестный результат реакции")
	}
	return out, nil
}

func (c *Client) Like(ctx context.Context, owner, id int64) (ReactionResult, error) {
	return c.LikeGuarded(ctx, owner, id, nil)
}

func (c *Client) LikeGuarded(ctx context.Context, owner, id int64, guard func(context.Context) error) (ReactionResult, error) {
	return c.PositiveReactionGuarded(ctx, owner, id, PositiveLike, nil, guard)
}

// An explicit message, including an empty string, never consumes the owner's rotation.
func (c *Client) LikeWithMessageGuarded(ctx context.Context, owner, id int64, message string, guard func(context.Context) error) (ReactionResult, error) {
	return c.PositiveReactionGuarded(ctx, owner, id, PositiveLike, &message, guard)
}

func (c *Client) PositiveReactionGuarded(ctx context.Context, owner, id int64, kind PositiveReactionType, explicit *string, guard func(context.Context) error) (ReactionResult, error) {
	if kind != PositiveLike && kind != PositiveSuperLike {
		return ReactionResult{Decision: ReactionUnknown}, errors.New("invalid positive reaction")
	}
	return c.positiveGuarded(ctx, owner, id, kind, false, explicit, guard)
}

func (c *Client) AutomaticLikeGuarded(ctx context.Context, owner, id int64, guard func(context.Context) error) (ReactionResult, error) {
	return c.positiveGuarded(ctx, owner, id, PositiveLike, true, nil, guard)
}

func (c *Client) positiveGuarded(ctx context.Context, owner, id int64, kind PositiveReactionType, automatic bool, explicit *string, guard func(context.Context) error) (ReactionResult, error) {
	out := ReactionResult{Decision: ReactionUnknown}
	if kind == PositiveSuperLike {
		ctx = context.WithValue(ctx, positiveSpacingContextKey{}, true)
	}
	extraGuard := guard
	guard = func(checkCtx context.Context) error {
		if e := c.Store.checkPositiveBudget(checkCtx, owner, c.Now(), false); e != nil {
			return e
		}
		if extraGuard != nil {
			return extraGuard(checkCtx)
		}
		return nil
	}
	if explicit != nil {
		if err := validateReactionMessage(*explicit); err != nil {
			return out, err
		}
	}
	if err := c.Store.checkReactionAction(ctx, c.Store.DB, owner, id, c.Now()); err != nil {
		return out, err
	}
	initial, err := c.Store.Session(ctx, owner)
	if err != nil {
		return out, err
	}
	if guard != nil {
		if err = guard(ctx); err != nil {
			return out, err
		}
	}
	account, err := c.Me(ctx, owner)
	if err != nil {
		return out, err
	}
	if value, exists := account["freezeState"]; exists && value != nil {
		frozen, ok := value.(bool)
		if !ok {
			return out, errors.New("неизвестное состояние аккаунта")
		}
		if frozen {
			return out, ErrAccountFrozen
		}
	}
	// Refresh is allowed for this read-only preflight, never as a replay of POST.
	if _, err = c.token(ctx, owner, ""); err != nil {
		return out, err
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	bound := func() (Session, error) {
		if err := c.Store.checkReactionAction(ctx, c.Store.DB, owner, id, c.Now()); err != nil {
			return Session{}, err
		}
		current, err := c.Store.Session(ctx, owner)
		if err != nil || current.Generation != initial.Generation || current.UserID != initial.UserID {
			return Session{}, ErrReactionSessionChanged
		}
		return current, nil
	}
	if _, err = bound(); err != nil {
		return out, err
	}
	if guard != nil {
		if err = guard(ctx); err != nil {
			return out, err
		}
	}
	action, err := c.Store.ClaimOwnerAction(ctx, owner, id, c.Now())
	if err != nil {
		return out, err
	}
	release := func(cause error) (ReactionResult, error) {
		writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancelWrite()
		return out, errors.Join(cause, c.Store.releaseUnsentReaction(writeCtx, action))
	}
	if guard != nil {
		if err = guard(context.WithValue(ctx, claimedLikeContextKey{}, action)); err != nil {
			return release(err)
		}
	}
	// The new pending row is our own claim, so recheck lifecycle independently.
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != initial.Generation || current.UserID != initial.UserID {
		return release(ErrReactionSessionChanged)
	}
	if !c.Store.cfg.AllowReactions || c.Store.Paused(ctx, owner) {
		cause := ErrReactionPaused
		if !c.Store.cfg.AllowReactions {
			cause = ErrReactionDisabled
		}
		return release(cause)
	}
	target, err := c.Store.Profile(ctx, owner, id)
	if err != nil {
		return release(err)
	}
	if target.ID != id {
		return release(errors.New("анкета изменилась; действие отменено"))
	}
	if err = ctx.Err(); err != nil {
		return release(err)
	}
	request, err := c.Store.preparePositiveMessage(ctx, owner, action, target, current, explicit, kind, automatic)
	if err != nil {
		return release(err)
	}
	// Preparing the durable intent may have waited for another SQLite writer.
	if guard != nil {
		if err = guard(context.WithValue(ctx, claimedLikeContextKey{}, action)); err != nil {
			return release(err)
		}
	}
	if !c.Store.cfg.AllowReactions {
		return release(ErrReactionDisabled)
	}
	if c.Store.Paused(ctx, owner) {
		return release(ErrReactionPaused)
	}
	latest, bindingErr := c.Store.Session(ctx, owner)
	if bindingErr != nil || latest.State != "connected" || latest.Generation != current.Generation || latest.UserID != current.UserID {
		return release(ErrReactionSessionChanged)
	}
	if err = ctx.Err(); err != nil {
		return release(err)
	}
	if err = c.Store.markPositiveAttempt(ctx, owner, action, current, automatic, c.Now); err != nil {
		return release(err)
	}
	body, err := c.exchange(ctx, http.MethodPost, "/reactions/addNew", current.Token, json.RawMessage(request))
	if err == nil {
		out, err = decodeReactionResult(body, c.Now())
	}
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelWrite()
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) {
			if upstream.Status == 401 {
				c.Store.setSessionStateIfCurrent(writeCtx, owner, current, "reconnect_required", "reaction_auth_not_confirmed")
			} else if upstream.Status == 403 || upstream.Status == 429 || upstream.Kind == "unexpected_response_or_challenge" {
				c.Store.setSessionStateIfCurrent(writeCtx, owner, current, "paused", upstream.Kind)
			}
		}
		persistErr := c.Store.finishReactionAction(writeCtx, action, "unknown", body, time.Time{})
		c.Store.Pause(writeCtx, owner, true)
		return out, errors.Join(errors.New("результат реакции неизвестен; повторная отправка запрещена"), persistErr)
	}
	state := "rejected"
	if out.Decision == ReactionSuccess {
		state = "succeeded"
	} else if out.Decision == ReactionQuota {
		state = "quota_waiting"
	} else if out.Decision == ReactionFireUnavailable {
		var wire struct {
			Kind int64 `json:"reactionType"`
		}
		_ = json.Unmarshal(request, &wire)
		if wire.Kind == 3 {
			state = "fire_waiting"
		}
	}
	if err = c.Store.finishReactionAction(writeCtx, action, state, body, out.RetryAt); err != nil {
		c.Store.Pause(writeCtx, owner, true)
		return out, err
	}
	if out.Decision == ReactionSuccess && out.Mutual {
		if err := c.Store.RequestContactSync(writeCtx, owner, current, c.Now()); err != nil {
			slog.Warn("contact sync scheduling failed", "kind", safeError(err))
		}
	}
	return out, nil
}
