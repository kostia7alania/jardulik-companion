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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type UpstreamError struct {
	Status     int
	Kind       string
	RetryAfter time.Duration
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
}

func NewClient(s *Store, c Config) *Client {
	return &Client{Store: s, Base: strings.TrimRight(c.APIBase, "/"), UserAgent: c.UserAgent, HTTP: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}, Now: time.Now}
}
func (c *Client) lock(owner int64) *sync.Mutex {
	return c.Store.SessionMutex(owner)
}

func (c *Client) exchange(ctx context.Context, method, path, token string, payload any) ([]byte, error) {
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
		return nil, &UpstreamError{res.StatusCode, kind, d}
	}
	if strings.Contains(strings.ToLower(res.Header.Get("Content-Type")), "text/html") || !json.Valid(b) {
		return nil, &UpstreamError{res.StatusCode, "unexpected_response_or_challenge", 0}
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
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, err
	}
	q := url.Values{"userId": {strconv.FormatInt(s.UserID, 10)}, "pageNumber": {strconv.Itoa(page)}, "pageSize": {"150"}, "all": {"0"}, "ageMin": {"18"}, "ageMax": {"80"}, "heightMin": {"100"}, "heightMax": {"250"}}
	for _, key := range []string{"goal", "children", "worldview", "country", "city"} {
		q.Set(key, "")
	}
	var account Profile
	_ = json.Unmarshal(s.Account, &account)
	if n := number(account["minAge"]); n >= 18 {
		q.Set("ageMin", strconv.FormatInt(n, 10))
	}
	if n := number(account["maxAge"]); n >= 18 {
		q.Set("ageMax", strconv.FormatInt(n, 10))
	}
	b, err := c.requestSession(ctx, owner, "GET", "/user/getUsers?"+q.Encode(), nil, &s)
	if err != nil {
		return nil, err
	}
	var p []Profile
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err = d.Decode(&p); err != nil {
		return nil, errors.New("Jardulik profile list format changed")
	}
	return p, nil
}
func (c *Client) FullProfile(ctx context.Context, owner, id int64) (Profile, error) {
	if _, err := c.Store.Profile(ctx, owner, id); err != nil {
		return nil, errors.New("анкета недоступна этому аккаунту")
	}
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return nil, err
	}
	q := url.Values{"viewerUserId": {strconv.FormatInt(s.UserID, 10)}, "targetUserId": {strconv.FormatInt(id, 10)}}
	b, err := c.requestSession(ctx, owner, "GET", "/user/getUser?"+q.Encode(), nil, &s)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(b)) > 0 && bytes.TrimSpace(b)[0] == '[' {
		var a []json.RawMessage
		if json.Unmarshal(b, &a) != nil || len(a) != 1 {
			return nil, errors.New("profile unavailable")
		}
		b = a[0]
	}
	p, err := decodeProfile(b)
	if err != nil || p.ID() != id {
		return nil, errors.New("profile unavailable")
	}
	return p, nil
}

type ReactionResult struct {
	Success, Mutual, Last        bool
	Kind, HoursLeft, MinutesLeft int64
	Raw                          []byte
}

func (c *Client) Like(ctx context.Context, owner, id int64) (ReactionResult, error) {
	var out ReactionResult
	if !c.Store.cfg.AllowReactions {
		return out, errors.New("автолайки выключены администратором")
	}
	if c.Store.Paused(ctx, owner) {
		return out, errors.New("действия приостановлены: /resume")
	}
	if _, err := c.Store.Profile(ctx, owner, id); err != nil {
		return out, errors.New("анкета недоступна")
	}
	s, err := c.Store.Session(ctx, owner)
	if err != nil {
		return out, err
	}
	action, err := c.Store.ClaimAction(ctx, owner, id)
	if err != nil {
		return out, errors.New("реакция уже отправлялась или её результат неизвестен")
	}
	b, err := c.Request(ctx, owner, "POST", "/reactions/addNew", map[string]any{"reactionType": 1, "isDeleted": false, "userId": id, "fromUserId": s.UserID, "message": ""})
	if err != nil {
		c.Store.FinishAction(ctx, action, "unknown", nil)
		c.Store.Pause(ctx, owner, true)
		return out, errors.New("результат реакции неизвестен; серия остановлена без повторной отправки")
	}
	var p Profile
	if json.Unmarshal(b, &p) != nil {
		c.Store.FinishAction(ctx, action, "unknown", b)
		c.Store.Pause(ctx, owner, true)
		return out, errors.New("неизвестный ответ реакции")
	}
	get := func(keys ...string) any {
		for _, k := range keys {
			if v, ok := p[k]; ok {
				return v
			}
		}
		return nil
	}
	out.Success, _ = get("IsSuccess", "Success", "success").(bool)
	out.Mutual, _ = get("IsMutual", "Mutual", "mutual").(bool)
	out.Last, _ = get("IsLast", "Last", "last").(bool)
	out.Kind = number(get("MessageKind", "Kind", "kind"))
	out.HoursLeft = number(get("HoursLeft", "hoursLeft"))
	out.MinutesLeft = number(get("MinutesLeft", "minutesLeft"))
	out.Raw = b
	state := "failed"
	if out.Success {
		state = "succeeded"
	}
	c.Store.FinishAction(ctx, action, state, b)
	if !out.Success || out.Last {
		c.Store.Pause(ctx, owner, true)
	}
	return out, nil
}
