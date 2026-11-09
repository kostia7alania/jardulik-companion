package companion

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrProfileUnavailable = errors.New("Jardulik profile unavailable")

type UpstreamError struct {
	Status     int
	Kind       string
	RetryAfter time.Duration
	Endpoint   string
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
		return nil, &UpstreamError{Status: res.StatusCode, Kind: kind, RetryAfter: d, Endpoint: strings.SplitN(path, "?", 2)[0]}
	}
	if strings.Contains(strings.ToLower(res.Header.Get("Content-Type")), "text/html") || !json.Valid(b) {
		return nil, &UpstreamError{Status: res.StatusCode, Kind: "unexpected_response_or_challenge", Endpoint: strings.SplitN(path, "?", 2)[0]}
	}
	return b, nil
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
	if err := ValidateInitData(raw, c.Store.cfg.SourceBotID, owner, c.Now(), c.Store.cfg.InitMaxAge); err != nil {
		return err
	}
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
	b, err := c.exchange(ctx, "POST", "/appuser/login/telegram", "", map[string]string{"initData": raw})
	if err != nil {
		return err
	}
	session, err := decodeLoginResponse(b)
	if err != nil {
		return err
	}
	var generation [16]byte
	if _, err = rand.Read(generation[:]); err != nil {
		return err
	}
	session.Generation = hex.EncodeToString(generation[:])
	if err = c.Store.SaveSession(ctx, owner, session); err != nil {
		return err
	}
	c.Store.Event(ctx, owner, "connect_success")
	return nil
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
	b, err := c.exchange(ctx, "PUT", "/appuser/refresh", "", map[string]string{"token": s.Token, "refreshToken": s.Refresh})
	if err != nil {
		c.Store.SessionState(ctx, owner, "reconnect_required", "refresh_not_confirmed")
		return s, err
	}
	var r struct{ Token, RefreshToken string }
	if json.Unmarshal(b, &r) != nil || r.Token == "" || r.RefreshToken == "" {
		c.Store.SessionState(ctx, owner, "reconnect_required", "refresh_format_changed")
		return s, errors.New("invalid refresh response")
	}
	s.Token = r.Token
	s.Refresh = r.RefreshToken
	s.Expires = tokenExpiry(r.Token)
	if err = c.Store.SaveSession(ctx, owner, s); err != nil {
		return s, err
	}
	return s, nil
}

func (c *Client) token(ctx context.Context, owner int64, after401 string, generation ...string) (Session, error) {
	mu := c.lock(owner)
	mu.Lock()
	defer mu.Unlock()
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
	s, err := c.token(ctx, owner, "")
	if err != nil {
		return nil, err
	}
	if expected != nil && (s.Generation != expected.Generation || s.UserID != expected.UserID) {
		return nil, errors.New("сессия изменилась; старый запрос отменён")
	}
	b, err := c.exchange(ctx, method, path, s.Token, payload)
	var ue *UpstreamError
	if errors.As(err, &ue) && ue.Status == 401 {
		s, err = c.token(ctx, owner, s.Token, s.Generation)
		if err != nil {
			return nil, err
		}
		b, err = c.exchange(ctx, method, path, s.Token, payload)
	}
	if errors.As(err, &ue) && (ue.Status == 403 || ue.Status == 429 || ue.Kind == "unexpected_response_or_challenge") {
		mu := c.lock(owner)
		mu.Lock()
		if current, e := c.Store.Session(ctx, owner); e == nil && current.Generation == s.Generation && current.Token == s.Token {
			c.Store.SessionState(ctx, owner, "paused", ue.Kind)
		}
		mu.Unlock()
	}
	return b, err
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
	if err := c.Store.KnownProfile(ctx, owner, id); err != nil {
		return nil, errors.New("анкета недоступна этому аккаунту")
	}
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, err
	}
	q := url.Values{"viewerUserId": {strconv.FormatInt(s.UserID, 10)}, "targetUserId": {strconv.FormatInt(id, 10)}}
	b, err := c.requestSession(ctx, owner, "GET", "/user/getUser?"+q.Encode(), nil, &s)
	current, currentErr := c.Store.Session(ctx, owner)
	if currentErr != nil || current.Generation != s.Generation || current.UserID != s.UserID {
		return nil, errors.New("сессия изменилась; старый ответ отменён")
	}
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) && upstream.Endpoint == "/user/getUser" && (upstream.Status == http.StatusNotFound || upstream.Status == http.StatusGone) {
			return nil, ErrProfileUnavailable
		}
		return nil, err
	}
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) || bytes.Equal(b, []byte("[]")) {
		return nil, ErrProfileUnavailable
	}
	if len(b) > 0 && b[0] == '[' {
		var a []json.RawMessage
		if json.Unmarshal(b, &a) != nil {
			return nil, errors.New("Jardulik profile format changed")
		}
		if len(a) == 0 {
			return nil, ErrProfileUnavailable
		}
		if len(a) != 1 {
			return nil, errors.New("Jardulik profile format changed")
		}
		b = a[0]
	}
	p, err := decodeProfile(b)
	if err != nil || p == nil || p.ID() != id {
		return nil, errors.New("Jardulik profile identity or format changed")
	}
	return p, nil
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
	out := ReactionResult{Decision: ReactionUnknown}
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
	body, err := c.exchange(ctx, http.MethodPost, "/reactions/addNew", current.Token, map[string]any{"reactionType": 1, "isDeleted": false, "userId": id, "fromUserId": current.UserID, "message": ""})
	if err == nil {
		out, err = decodeReactionResult(body, c.Now())
	}
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelWrite()
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) {
			if upstream.Status == 401 {
				c.Store.SessionState(writeCtx, owner, "reconnect_required", "reaction_auth_not_confirmed")
			} else if upstream.Status == 403 || upstream.Status == 429 || upstream.Kind == "unexpected_response_or_challenge" {
				c.Store.SessionState(writeCtx, owner, "paused", upstream.Kind)
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
