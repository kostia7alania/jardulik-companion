package companion

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const miniAppAudience = "jardulik-owner-miniapp"
const miniAppLifetime = 10 * time.Minute
const miniAppJWTHeader = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"

type miniAppHTTP struct {
	service *Service
	origin  string
	assets  http.Handler
	key     [32]byte
	now     func() time.Time
}

type miniAppClaims struct {
	Audience    string `json:"aud"`
	Owner       int64  `json:"owner"`
	Upstream    int64  `json:"upstream,omitempty"`
	Generation  string `json:"generation,omitempty"`
	Target      int64  `json:"target"`
	Version     int64  `json:"version"`
	Issued      int64  `json:"iat"`
	Expires     int64  `json:"exp"`
	ArchiveOnly bool   `json:"archiveOnly,omitempty"`
}

// NewMiniAppHandler serves public shells, but requires owner Telegram authentication for all data.
func NewMiniAppHandler(s *Service, publicOrigin string, assets http.Handler) (http.Handler, error) {
	u, err := url.Parse(publicOrigin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || publicOrigin != "https://"+u.Host || strings.ContainsAny(publicOrigin, "\r\n") {
		return nil, errors.New("mini app requires an explicit HTTPS origin")
	}
	if s == nil || s.Store == nil || s.Client == nil || s.Config.OwnerID <= 0 || s.Config.Token == "" || s.Client.Base != "https://api.jardulik.app/api" {
		return nil, errors.New("mini app requires the owner bot and Jardulik HTTPS source")
	}
	if assets == nil {
		return nil, errors.New("mini app assets are required")
	}
	m := &miniAppHTTP{service: s, origin: publicOrigin, assets: assets, now: time.Now}
	if _, err = rand.Read(m.key[:]); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *miniAppHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline' https://telegram.org; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob: https:; media-src 'self' blob: https:; font-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'self' https://web.telegram.org https://*.telegram.org https://telegram.org")
	if !strings.HasPrefix(r.URL.Path, "/miniapp/") {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			debugError(w, 405, "method_not_allowed")
			return
		}
		m.assets.ServeHTTP(w, r)
		return
	}
	if len(r.Header.Values("Origin")) > 1 || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != m.origin) || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		debugError(w, 403, "same_origin_required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if r.Method == http.MethodPost && (r.URL.Path == "/miniapp/auth" || r.URL.Path == "/miniapp/api/appuser/login/telegram") {
		m.authenticate(w, r)
		return
	}
	claims, ss, err := m.authorize(r)
	if err != nil {
		debugError(w, 401, "miniapp_auth_required")
		return
	}
	if r.Method != http.MethodGet {
		debugError(w, 403, "read_only_endpoint")
		return
	}
	switch {
	case r.URL.Path == "/miniapp/profile":
		m.profile(w, r, claims, ss)
	case strings.HasPrefix(r.URL.Path, "/miniapp/media/"):
		m.media(w, r, claims, ss)
	case strings.HasPrefix(r.URL.Path, "/miniapp/api/"):
		if claims.ArchiveOnly {
			debugError(w, 403, "archive_view_only")
			return
		}
		m.api(w, r, claims, ss)
	default:
		debugError(w, 404, "not_found")
	}
}

func miniAppValidateInitData(raw, botToken string, owner int64, now time.Time) error {
	if raw == "" || len(raw) > 32<<10 || botToken == "" || owner <= 0 {
		return errors.New("invalid_launch")
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return errors.New("invalid_launch")
	}
	for _, values := range q {
		if len(values) != 1 {
			return errors.New("ambiguous_launch")
		}
	}
	got, err := hex.DecodeString(q.Get("hash"))
	if err != nil || len(got) != sha256.Size {
		return errors.New("invalid_launch_hash")
	}
	keys := make([]string, 0, len(q))
	for key := range q {
		if key != "hash" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+q.Get(key))
	}
	secret := hmac.New(sha256.New, []byte("WebAppData"))
	secret.Write([]byte(botToken))
	mac := hmac.New(sha256.New, secret.Sum(nil))
	mac.Write([]byte(strings.Join(parts, "\n")))
	if !hmac.Equal(got, mac.Sum(nil)) {
		return errors.New("invalid_launch_signature")
	}
	date, err := strconv.ParseInt(q.Get("auth_date"), 10, 64)
	if err != nil || date <= 0 || date > now.Add(30*time.Second).Unix() || date < now.Add(-miniAppLifetime).Unix() {
		return errors.New("expired_launch")
	}
	var user struct {
		ID int64 `json:"id"`
	}
	if json.Unmarshal([]byte(q.Get("user")), &user) != nil || user.ID != owner {
		return errors.New("wrong_launch_owner")
	}
	return nil
}

func (m *miniAppHTTP) current(ctx context.Context) (Session, error) {
	s := m.service
	ss, err := s.Store.Session(ctx, s.Config.OwnerID)
	if err != nil || ss.State != "connected" || ss.UserID <= 0 || ss.Generation == "" {
		return ss, errors.New("source_session_unavailable")
	}
	var paused bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", s.Config.OwnerID).Scan(&paused); err != nil || paused {
		return ss, errors.New("source_session_paused")
	}
	var account Profile
	if json.Unmarshal(ss.Account, &account) == nil {
		if frozen, ok := account["freezeState"].(bool); ok && frozen {
			return ss, ErrAccountFrozen
		}
	}
	return ss, nil
}

func (m *miniAppHTTP) binding(ctx context.Context, expected Session) error {
	ss, err := m.current(ctx)
	if err != nil || ss.Generation != expected.Generation || ss.UserID != expected.UserID {
		return ErrReactionSessionChanged
	}
	return nil
}

func (m *miniAppHTTP) own(ctx context.Context, ss Session) ([]byte, error) {
	status, raw, err := m.service.Client.debugRead(ctx, m.service.Config.OwnerID, ss, "/user/getMe?userId="+strconv.FormatInt(ss.UserID, 10))
	if err != nil || status != 200 {
		return nil, errors.New("source_account_unavailable")
	}
	p, err := debugProfile(raw)
	frozen, known := p["freezeState"].(bool)
	if err != nil || p.ID() != ss.UserID || !known || frozen {
		return nil, errors.New("source_account_frozen_or_unconfirmed")
	}
	if m.binding(ctx, ss) != nil || m.hasSecret(raw, ss) {
		return nil, errors.New("source_account_unavailable")
	}
	return raw, nil
}

func (m *miniAppHTTP) authenticate(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		debugError(w, 400, "json_body_required")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 40<<10))
	if err != nil {
		debugError(w, 413, "body_too_large")
		return
	}
	allowed := []string{"initData", "targetUserId"}
	if r.URL.Path == "/miniapp/auth" {
		allowed = append(allowed, "versionId")
	}
	fields, err := debugObject(raw, allowed...)
	if err != nil {
		debugError(w, 400, "invalid_request")
		return
	}
	var initData string
	if json.Unmarshal(fields["initData"], &initData) != nil || miniAppValidateInitData(initData, m.service.Config.Token, m.service.Config.OwnerID, m.now()) != nil {
		debugError(w, 401, "invalid_telegram_launch")
		return
	}
	target, err := debugJSONID(fields, "targetUserId", false)
	if err != nil {
		debugError(w, 400, "invalid_target")
		return
	}
	version, err := debugJSONID(fields, "versionId", true)
	if err != nil {
		debugError(w, 400, "invalid_version")
		return
	}
	s := m.service
	var p StoredProfile
	var ss Session
	if version > 0 {
		p, err = s.Store.Version(r.Context(), s.Config.OwnerID, version)
	} else {
		ss, err = m.current(r.Context())
		if err != nil {
			debugError(w, 409, "source_session_unavailable")
			return
		}
		p, err = s.Store.Profile(r.Context(), s.Config.OwnerID, target)
	}
	if err != nil || p.ID != target {
		debugError(w, 404, "stored_profile_not_found")
		return
	}
	if version == 0 {
		s.beginForeground()
		defer s.endForeground()
		if _, err = m.own(r.Context(), ss); err != nil {
			debugError(w, 409, "source_account_unavailable")
			return
		}
	}
	now := m.now().Unix()
	claims := miniAppClaims{
		Audience: miniAppAudience, Owner: s.Config.OwnerID, Upstream: ss.UserID, Generation: ss.Generation,
		Target: target, Version: p.VersionID, Issued: now, Expires: now + int64(miniAppLifetime/time.Second),
		ArchiveOnly: version > 0,
	}
	token, err := m.sign(claims)
	if err != nil {
		debugError(w, 500, "token_unavailable")
		return
	}
	if claims.ArchiveOnly {
		m.json(w, 200, map[string]any{"token": token, "expiresAt": claims.Expires}, ss)
		return
	}
	m.json(w, 200, map[string]any{"code": 200, "isReged": true, "userId": ss.UserID, "targetUserId": target, "token": token, "refreshToken": token, "expiresAt": claims.Expires, "adapter": "owner-read-only-bridge"}, ss)
}

func (m *miniAppHTTP) sign(claims miniAppClaims) (string, error) {
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := miniAppJWTHeader + "." + base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, m.key[:])
	mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (m *miniAppHTTP) authorize(r *http.Request) (miniAppClaims, Session, error) {
	var claims miniAppClaims
	var ss Session
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) > 4096 {
		return claims, ss, errors.New("invalid_bearer")
	}
	parts := strings.Split(strings.TrimPrefix(values[0], "Bearer "), ".")
	if len(parts) != 3 || parts[0] != miniAppJWTHeader {
		return claims, ss, errors.New("invalid_token")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	mac := hmac.New(sha256.New, m.key[:])
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return claims, ss, errors.New("invalid_token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(raw, &claims) != nil {
		return claims, ss, errors.New("invalid_claims")
	}
	now := m.now().Unix()
	if claims.Audience != miniAppAudience || claims.Owner != m.service.Config.OwnerID || claims.Target <= 0 || claims.Version <= 0 || claims.Issued > now+30 || claims.Issued <= 0 || claims.Expires <= now || claims.Expires <= claims.Issued || claims.Expires-claims.Issued > int64(miniAppLifetime/time.Second) {
		return claims, ss, errors.New("expired_token")
	}
	if claims.ArchiveOnly {
		if claims.Upstream != 0 || claims.Generation != "" {
			return claims, ss, errors.New("invalid_archive_binding")
		}
	} else {
		ss, err = m.current(r.Context())
		if err != nil || ss.UserID != claims.Upstream || ss.Generation != claims.Generation {
			return claims, ss, errors.New("source_binding_changed")
		}
	}
	p, err := m.service.Store.Version(r.Context(), claims.Owner, claims.Version)
	if err != nil || p.ID != claims.Target {
		return claims, ss, errors.New("target_binding_changed")
	}
	return claims, ss, nil
}

func (m *miniAppHTTP) hasSecret(raw []byte, ss Session, previous ...Session) bool {
	secrets := []string{m.service.Config.Token}
	sessions := append([]Session{ss}, previous...)
	if current, err := m.service.Store.Session(context.Background(), m.service.Config.OwnerID); err == nil {
		sessions = append(sessions, current)
	}
	for _, session := range sessions {
		secrets = append(secrets, session.Token, session.Refresh)
	}
	containsSecret := func(value string) bool {
		for _, secret := range secrets {
			if secret != "" && strings.Contains(value, secret) {
				return true
			}
		}
		return false
	}
	if containsSecret(string(raw)) {
		return true
	}
	// Inspect decoded strings and every key, including duplicate keys, without rewriting accepted source bytes.
	nodes := 0
	var scanJSON func([]byte, int) bool
	scanJSON = func(body []byte, depth int) bool {
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		var walk func(int, bool) bool
		walk = func(level int, credential bool) bool {
			nodes++
			if level > 64 || nodes > 100000 {
				return true
			}
			token, err := decoder.Token()
			if err != nil {
				return true
			}
			if credential {
				value, ok := token.(string)
				if !ok || !m.isBridgeCredential(value) {
					return true
				}
			}
			switch value := token.(type) {
			case string:
				if containsSecret(value) {
					return true
				}
				// Some source fields contain JSON text that the frontend parses again.
				if json.Valid([]byte(value)) && scanJSON([]byte(value), level+1) {
					return true
				}
			case json.Delim:
				switch value {
				case '{':
					for decoder.More() {
						key, err := decoder.Token()
						name, ok := key.(string)
						if err != nil || !ok || containsSecret(name) || walk(level+1, miniAppCredentialKey(name)) {
							return true
						}
					}
					end, err := decoder.Token()
					return err != nil || end != json.Delim('}')
				case '[':
					for decoder.More() {
						if walk(level+1, false) {
							return true
						}
					}
					end, err := decoder.Token()
					return err != nil || end != json.Delim(']')
				default:
					return true
				}
			}
			return false
		}
		if walk(depth, false) {
			return true
		}
		_, err := decoder.Token()
		return err != io.EOF
	}
	return scanJSON(raw, 0)
}

func miniAppCredentialKey(name string) bool {
	name = strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(name))
	switch name {
	case "token", "accesstoken", "refreshtoken", "bottoken", "telegrambottoken", "authorization", "apikey", "password", "clientsecret", "sessionkey", "sessiontoken", "idtoken":
		return true
	}
	return false
}

func (m *miniAppHTTP) isBridgeCredential(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 3 || parts[0] != miniAppJWTHeader {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	mac := hmac.New(sha256.New, m.key[:])
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	var claims miniAppClaims
	return err == nil && json.Unmarshal(raw, &claims) == nil && claims.Audience == miniAppAudience && claims.Owner == m.service.Config.OwnerID
}

func (m *miniAppHTTP) json(w http.ResponseWriter, status int, value any, ss Session) {
	raw, err := json.Marshal(value)
	if err != nil || m.hasSecret(raw, ss) {
		debugError(w, 502, "credential_or_invalid_response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (m *miniAppHTTP) profile(w http.ResponseWriter, r *http.Request, c miniAppClaims, ss Session) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || !debugQueryKeys(q, "id") || q.Get("id") != strconv.FormatInt(c.Target, 10) {
		debugError(w, 400, "invalid_target")
		return
	}
	p, err := m.service.Store.Version(r.Context(), c.Owner, c.Version)
	if err != nil || p.ID != c.Target {
		debugError(w, 404, "stored_profile_not_found")
		return
	}
	jobs, err := m.service.Store.MediaJobs(r.Context(), c.Owner, c.Version)
	if err != nil {
		debugError(w, 500, "media_unavailable")
		return
	}
	media := make([]map[string]any, 0)
	for _, job := range selectedProfilePhotos(jobs) {
		if !miniAppSafeMedia(job) {
			continue
		}
		media = append(media, map[string]any{"id": job.ID, "role": job.Role, "position": job.Position, "mime": job.MIME, "size": job.Size, "url": "/miniapp/media/" + strconv.FormatInt(job.ID, 10)})
	}
	if !c.ArchiveOnly && m.binding(r.Context(), ss) != nil {
		debugError(w, 409, "source_binding_changed")
		return
	}
	result := map[string]any{"id": p.ID, "versionId": p.VersionID, "firstSeen": p.FirstSeen, "lastSeen": p.LastSeen, "formattedText": profileText(p), "media": media}
	if c.ArchiveOnly {
		// The browser renders the original JSON text without rounding its numbers.
		result["rawJSON"] = p.RawJSON
	} else {
		result["raw"] = json.RawMessage(p.RawJSON)
	}
	m.json(w, 200, result, ss)
}

func miniAppSafeMedia(j MediaJob) bool {
	decoded, err := hex.DecodeString(j.SHA)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == j.SHA && j.State == "complete" && j.Size > 0 && (j.MIME == "image/jpeg" || j.MIME == "image/png" || j.MIME == "image/webp" || j.MIME == "image/gif")
}

func (m *miniAppHTTP) media(w http.ResponseWriter, r *http.Request, c miniAppClaims, ss Session) {
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/miniapp/media/"), 10, 64)
	if err != nil || id <= 0 || r.URL.RawQuery != "" {
		debugError(w, 400, "invalid_media")
		return
	}
	jobs, err := m.service.Store.MediaJobs(r.Context(), c.Owner, c.Version)
	if err != nil {
		debugError(w, 404, "media_not_found")
		return
	}
	for _, job := range selectedProfilePhotos(jobs) {
		if job.ID != id || job.ProfileID != c.Target || !miniAppSafeMedia(job) {
			continue
		}
		path := filepath.Join(m.service.Config.DataDir, "media", job.SHA)
		// OpenRoot prevents stored paths and symlinks from escaping the media directory.
		root, e := os.OpenRoot(filepath.Dir(path))
		if e != nil {
			break
		}
		defer root.Close()
		info, e := root.Lstat(job.SHA)
		if e != nil || !info.Mode().IsRegular() || info.Size() != job.Size {
			break
		}
		file, e := root.Open(job.SHA)
		if e != nil {
			break
		}
		defer file.Close()
		opened, e := file.Stat()
		if e != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Size() != job.Size {
			break
		}
		if !c.ArchiveOnly && m.binding(r.Context(), ss) != nil {
			debugError(w, 409, "source_binding_changed")
			return
		}
		w.Header().Set("Content-Type", job.MIME)
		w.Header().Set("Content-Disposition", "inline")
		http.ServeContent(w, r, "profile-image", opened.ModTime(), file)
		return
	}
	debugError(w, 404, "media_not_found")
}

func (m *miniAppHTTP) api(w http.ResponseWriter, r *http.Request, c miniAppClaims, ss Session) {
	path := strings.TrimPrefix(r.URL.Path, "/miniapp/api")
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		debugError(w, 400, "invalid_query")
		return
	}
	switch path {
	case "/user/getMe":
		if !debugQueryKeys(q, "userId") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
	case "/user/getUser":
		if !debugQueryKeys(q, "viewerUserId", "targetUserId") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "viewerUserId", ss.UserID)
		if err == nil && q.Get("targetUserId") != strconv.FormatInt(c.Target, 10) {
			err = errors.New("target_mismatch")
		}
	case "/user/getReactUsers":
		if !debugQueryKeys(q, "userId", "pageNumber", "pageSize", "isMutual") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
		for key, limit := range map[string]int{"pageNumber": 1000, "pageSize": 100} {
			if q.Get(key) == "" {
				q.Set(key, "1")
			}
			n, e := strconv.Atoi(q.Get(key))
			if e != nil || n < 1 || n > limit {
				err = errors.New("invalid_page")
			}
		}
		if q.Get("isMutual") != "true" && q.Get("isMutual") != "false" {
			err = errors.New("invalid_relationship_kind")
		}
	default:
		if path != "/photos/user/"+strconv.FormatInt(ss.UserID, 10) || len(q) != 0 {
			debugError(w, 403, "read_only_endpoint")
			return
		}
	}
	if err != nil {
		debugError(w, 400, "invalid_owner_or_target")
		return
	}
	s := m.service
	s.beginForeground()
	defer s.endForeground()
	own, err := m.own(r.Context(), ss)
	if err != nil {
		debugError(w, 409, "source_account_unavailable")
		return
	}
	if path == "/user/getMe" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(own)
		return
	}
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	// The account preflight may have refreshed credentials; retain both snapshots across the next read.
	beforeRead, snapshotErr := m.current(r.Context())
	if snapshotErr != nil || beforeRead.Generation != ss.Generation || beforeRead.UserID != ss.UserID {
		debugError(w, 409, "source_binding_changed")
		return
	}
	status, raw, err := s.Client.debugRead(r.Context(), c.Owner, ss, path)
	if errors.Is(err, ErrProfileUnavailable) && status == http.StatusNoContent {
		if m.binding(r.Context(), ss) != nil {
			debugError(w, 409, "source_binding_changed")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil && (status < 400 || status > 599) {
		debugError(w, 502, "upstream_unavailable")
		return
	}
	if err == nil && strings.HasPrefix(path, "/user/getUser?") {
		p, e := debugProfile(raw)
		if e != nil || p.ID() != c.Target {
			debugError(w, 502, "unexpected_profile_identity")
			return
		}
	}
	if err == nil && strings.HasPrefix(path, "/photos/user/") {
		var photos []struct {
			UserID int64 `json:"userId"`
		}
		if json.Unmarshal(raw, &photos) != nil {
			debugError(w, 502, "unexpected_photo_response")
			return
		}
		for _, photo := range photos {
			if photo.UserID != ss.UserID {
				debugError(w, 502, "unexpected_photo_identity")
				return
			}
		}
	}
	if m.binding(r.Context(), ss) != nil {
		debugError(w, 409, "source_binding_changed")
		return
	}
	if !json.Valid(raw) || m.hasSecret(raw, ss, beforeRead) {
		debugError(w, 502, "credential_or_invalid_response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}
