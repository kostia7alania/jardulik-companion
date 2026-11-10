package companion

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type miniAppFixture struct {
	*debugFixture
	h         *miniAppHTTP
	frozen    bool
	ownID     int64
	ownBody   string
	getStatus int
	getBody   string
}

func newMiniAppFixture(t *testing.T) *miniAppFixture {
	t.Helper()
	f := &miniAppFixture{debugFixture: newDebugFixture(t), ownID: 101}
	f.service.Config.Token = "123456:miniapp-test-bot-token"
	transport := f.service.Client.HTTP.Transport
	f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err != nil {
			return response, err
		}
		if r.URL.Path == "/api/user/getMe" {
			response.Body.Close()
			body := f.ownBody
			if body == "" {
				body = fmt.Sprintf(`{"userId":%d,"type":0,"freezeState":%t}`, f.ownID, f.frozen)
			}
			response.Body = io.NopCloser(strings.NewReader(body))
		}
		if r.URL.Path == "/api/user/getUser" && f.getStatus != 0 {
			response.Body.Close()
			response.StatusCode = f.getStatus
			response.Body = io.NopCloser(strings.NewReader(f.getBody))
		}
		return response, nil
	})
	h, err := NewMiniAppHandler(f.service, "https://miniapp.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "public shell") }))
	if err != nil {
		t.Fatal(err)
	}
	f.h = h.(*miniAppHTTP)
	f.h.now = f.service.Client.Now
	return f
}

func miniAppLaunch(token string, owner int64, date time.Time) string {
	q := url.Values{"auth_date": {strconv.FormatInt(date.Unix(), 10)}, "user": {fmt.Sprintf(`{"id":%d,"first_name":"fixture"}`, owner)}, "query_id": {"fixture"}, "signature": {"telegram-third-party-signature"}}
	keys := make([]string, 0, len(q))
	for key := range q {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+q.Get(key))
	}
	secret := hmac.New(sha256.New, []byte("WebAppData"))
	secret.Write([]byte(token))
	mac := hmac.New(sha256.New, secret.Sum(nil))
	mac.Write([]byte(strings.Join(parts, "\n")))
	q.Set("hash", hex.EncodeToString(mac.Sum(nil)))
	return q.Encode()
}

func (f *miniAppFixture) request(method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "https://miniapp.test"+path, strings.NewReader(body))
	r.Header.Set("Origin", "https://miniapp.test")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	return w
}

func (f *miniAppFixture) login(t *testing.T, path string) string {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"initData": miniAppLaunch(f.service.Config.Token, 1, f.h.now()), "targetUserId": 7})
	w := f.request("POST", path, string(raw), "")
	if w.Code != 200 {
		t.Fatalf("login HTTP%d %s", w.Code, w.Body.String())
	}
	var result struct {
		Token, RefreshToken, Adapter    string
		UserID, TargetUserID, ExpiresAt int64
		Code                            int
	}
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Token == "" || result.Token != result.RefreshToken || result.Adapter != "owner-read-only-bridge" || result.Code != 200 || result.UserID != 101 || result.TargetUserID != 7 || result.ExpiresAt != f.h.now().Add(miniAppLifetime).Unix() {
		t.Fatalf("invalid adapter login %s", w.Body.String())
	}
	for _, secret := range []string{"source-access-token", "source-refresh-token", f.service.Config.Token} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("source credential exposed")
		}
	}
	return result.Token
}

func TestMiniAppTelegramLaunchValidation(t *testing.T) {
	now := time.Unix(1800000000, 0)
	const token = "123456:fixture-token"
	good := miniAppLaunch(token, 1, now)
	if err := miniAppValidateInitData(good, token, 1, now); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"wrong-owner":    miniAppLaunch(token, 2, now),
		"wrong-bot":      miniAppLaunch("different-token", 1, now),
		"expired":        miniAppLaunch(token, 1, now.Add(-10*time.Minute-time.Second)),
		"future":         miniAppLaunch(token, 1, now.Add(time.Minute)),
		"duplicate-date": good + "&auth_date=" + strconv.FormatInt(now.Unix(), 10),
		"duplicate-hash": good + "&hash=abc",
		"bad-encoding":   good + "&x=%GG",
		"missing-hash":   "user=%7B%22id%22%3A1%7D",
	} {
		t.Run(name, func(t *testing.T) {
			if miniAppValidateInitData(raw, token, 1, now) == nil {
				t.Fatal("accepted invalid launch")
			}
		})
	}
	if miniAppValidateInitData(miniAppLaunch(token, 1, now.Add(-10*time.Minute)), token, 1, now) != nil {
		t.Fatal("rejected 10min boundary")
	}
}

func TestMiniAppShellAndAuthBoundary(t *testing.T) {
	f := newMiniAppFixture(t)
	for _, path := range []string{"/viewer?id=7", "/mutuals?targetUserId=7&noCheck=1", "/assets/app.js"} {
		w := f.request("GET", path, "", "")
		if w.Code != 200 || w.Body.String() != "public shell" || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "connect-src 'self'") {
			t.Fatalf("public shell: %d %s", w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/miniapp/profile?id=7", "/miniapp/media/1", "/miniapp/api/user/getMe"} {
		w := f.request("GET", path, "", "")
		if w.Code != 401 || w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("data accessible without auth: %d", w.Code)
		}
	}
	if len(f.calls) != 0 {
		t.Fatal("public/unauthorized requests contacted source")
	}
	for name, mutate := range map[string]func(*http.Request){
		"wrong-origin":  func(r *http.Request) { r.Header.Set("Origin", "https://evil.test") },
		"double-origin": func(r *http.Request) { r.Header.Add("Origin", "https://miniapp.test") },
		"cross-site":    func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "https://miniapp.test/miniapp/profile?id=7", nil)
			r.Header.Set("Origin", "https://miniapp.test")
			mutate(r)
			w := httptest.NewRecorder()
			f.h.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatalf("origin HTTP%d", w.Code)
			}
		})
	}
	f.login(t, "/miniapp/api/appuser/login/telegram")
	if len(f.calls) != 1 || f.calls[0] != "GET /api/user/getMe" {
		t.Fatalf("bootstrap called unexpected source %v", f.calls)
	}
}

func TestMiniAppInvalidAuthNeverCallsSource(t *testing.T) {
	f := newMiniAppFixture(t)
	launch := miniAppLaunch(f.service.Config.Token, 1, f.h.now())
	for name, body := range map[string]string{
		"bad-launch":       `{"initData":"bad","targetUserId":7}`,
		"unknown-target":   fmt.Sprintf(`{"initData":%q,"targetUserId":999}`, launch),
		"duplicate-target": fmt.Sprintf(`{"initData":%q,"targetUserId":7,"targetUserId":8}`, launch),
		"missing-target":   fmt.Sprintf(`{"initData":%q}`, launch),
		"oversized":        strings.Repeat(" ", 41<<10),
	} {
		t.Run(name, func(t *testing.T) {
			w := f.request("POST", "/miniapp/auth", body, "")
			if w.Code < 400 {
				t.Fatalf("accepted invalid auth %s", w.Body.String())
			}
		})
	}
	if len(f.calls) != 0 {
		t.Fatalf("invalid auth source calls %v", f.calls)
	}
}

func TestMiniAppAuthRequiresActualOwnUnfrozenAccount(t *testing.T) {
	for _, kind := range []string{"frozen", "identity", "paused", "source-state"} {
		t.Run(kind, func(t *testing.T) {
			f := newMiniAppFixture(t)
			switch kind {
			case "frozen":
				f.frozen = true
			case "identity":
				f.ownID = 999
			case "paused":
				f.service.Store.Pause(context.Background(), 1, true)
			case "source-state":
				f.service.Store.SessionState(context.Background(), 1, "paused", "test")
			}
			raw, _ := json.Marshal(map[string]any{"initData": miniAppLaunch(f.service.Config.Token, 1, f.h.now()), "targetUserId": 7})
			w := f.request("POST", "/miniapp/auth", string(raw), "")
			if w.Code != 409 {
				t.Fatalf("unsafe auth HTTP%d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestMiniAppJWTBoundToOwnerTargetGenerationExpiryAndHandler(t *testing.T) {
	f := newMiniAppFixture(t)
	token := f.login(t, "/miniapp/auth")
	for _, path := range []string{"/miniapp/profile?id=8", "/miniapp/profile?id=7&id=8", "/miniapp/api/user/getUser?viewerUserId=101&targetUserId=8", "/miniapp/api/user/getMe?userId=102", "/miniapp/api/user/getReactUsers?userId=101&isMutual=true&pageSize=101"} {
		w := f.request("GET", path, "", token)
		if w.Code != 400 {
			t.Fatalf("target boundary %s HTTP%d %s", path, w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest("GET", "https://miniapp.test/miniapp/profile?id=7", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	claims, _, err := f.h.authorize(r)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*miniAppClaims){
		"audience":   func(c *miniAppClaims) { c.Audience = "different" },
		"owner":      func(c *miniAppClaims) { c.Owner = 2 },
		"upstream":   func(c *miniAppClaims) { c.Upstream = 102 },
		"generation": func(c *miniAppClaims) { c.Generation = "different" },
		"version": func(c *miniAppClaims) {
			p, _ := f.service.Store.Profile(context.Background(), 1, 8)
			c.Version = p.VersionID
		},
		"expired":  func(c *miniAppClaims) { c.Expires = f.h.now().Unix() },
		"too-long": func(c *miniAppClaims) { c.Expires = c.Issued + 601 },
		"future":   func(c *miniAppClaims) { c.Issued = f.h.now().Add(time.Minute).Unix() },
	} {
		t.Run(name, func(t *testing.T) {
			c := claims
			mutate(&c)
			bad, _ := f.h.sign(c)
			w := f.request("GET", "/miniapp/profile?id=7", "", bad)
			if w.Code != 401 {
				t.Fatalf("invalid JWT HTTP%d", w.Code)
			}
		})
	}
	other, err := NewMiniAppHandler(f.service, "https://miniapp.test", f.h.assets)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	other.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("JWT survived unrelated handler key")
	}
	ss, _ := f.service.Store.Session(context.Background(), 1)
	ss.Generation = "new-generation"
	if err = f.service.Store.SaveSession(context.Background(), 1, ss); err != nil {
		t.Fatal(err)
	}
	if w = f.request("GET", "/miniapp/profile?id=7", "", token); w.Code != 401 {
		t.Fatal("JWT survived replaced session")
	}
}

func TestMiniAppReadOnlyUpstreamAndFrozenGate(t *testing.T) {
	f := newMiniAppFixture(t)
	token := f.login(t, "/miniapp/auth")
	before := len(f.calls)
	for _, request := range [][2]string{{"POST", "/miniapp/api/reactions/addNew"}, {"POST", "/miniapp/api/GPT/checkCompatibility"}, {"POST", "/miniapp/api/telegram/getTG"}, {"PUT", "/miniapp/api/user/updateLang"}, {"PUT", "/miniapp/api/appuser/refresh"}, {"DELETE", "/miniapp/api/appuser/delete"}, {"GET", "/miniapp/api/user/getUsers"}, {"GET", "/miniapp/api/orders/type?userId=101&type=1"}} {
		w := f.request(request[0], request[1], `{}`, token)
		if w.Code != 403 {
			t.Fatalf("write denied HTTP%d %s", w.Code, w.Body.String())
		}
	}
	if len(f.calls) != before {
		t.Fatal("denied endpoint reached upstream")
	}
	w := f.request("GET", "/miniapp/api/user/getMe?userId=101", "", token)
	if w.Code != 200 || w.Body.String() != `{"userId":101,"type":0,"freezeState":false}` {
		t.Fatalf("own account changed: HTTP%d %s", w.Code, w.Body.String())
	}
	w = f.request("GET", "/miniapp/api/user/getUser?viewerUserId=101&targetUserId=7", "", token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"userId":7`) {
		t.Fatalf("target HTTP%d %s", w.Code, w.Body.String())
	}
	w = f.request("GET", "/miniapp/api/user/getReactUsers?userId=101&isMutual=true&pageNumber=1&pageSize=20", "", token)
	if w.Code != 200 || w.Body.String() != `[]` {
		t.Fatalf("relationships HTTP%d %s", w.Code, w.Body.String())
	}
	f.frozen = true
	before = len(f.calls)
	w = f.request("GET", "/miniapp/api/user/getUser?targetUserId=7", "", token)
	if w.Code != 409 || len(f.calls) != before+1 || f.calls[len(f.calls)-1] != "GET /api/user/getMe" {
		t.Fatalf("frozen account reached target %d %v", w.Code, f.calls)
	}
}

func TestMiniAppPreservesUpstreamErrorsNoContentAndRejectsSecrets(t *testing.T) {
	f := newMiniAppFixture(t)
	token := f.login(t, "/miniapp/auth")
	for _, tc := range []struct {
		status int
		body   string
		want   int
	}{
		{404, `{"message":"profile absent"}`, 404},
		{204, "", 204},
		{200, `{"userId":7,"echo":"source-access-token"}`, 502},
		{200, `{"userId":8}`, 502},
	} {
		f.getStatus, f.getBody = tc.status, tc.body
		w := f.request("GET", "/miniapp/api/user/getUser?targetUserId=7", "", token)
		if w.Code != tc.want || strings.Contains(w.Body.String(), "source-access-token") {
			t.Fatalf("upstream status %d: HTTP%d %s", tc.status, w.Code, w.Body.String())
		}
	}
}

func miniAppEscapedString(value string) string {
	var out strings.Builder
	for _, r := range value {
		fmt.Fprintf(&out, `\u%04x`, r)
	}
	return out.String()
}

func TestMiniAppRejectsDecodedCredentialStringsKeysAndFields(t *testing.T) {
	f := newMiniAppFixture(t)
	token := f.login(t, "/miniapp/auth")
	for name, secret := range map[string]string{"access": "source-access-token", "refresh": "source-refresh-token", "bot": f.service.Config.Token} {
		for _, key := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-key-%t", name, key), func(t *testing.T) {
				escaped := miniAppEscapedString(secret)
				body := `{"userId":7,"nested":[{"echo":"` + escaped + `"}]}`
				if key {
					body = `{"userId":7,"nested":[{"` + escaped + `":"safe"}]}`
				}
				f.getStatus, f.getBody = 200, body
				w := f.request("GET", "/miniapp/api/user/getUser?targetUserId=7", "", token)
				if w.Code != 502 {
					t.Fatalf("decoded source credential escaped: HTTP%d %s", w.Code, w.Body.String())
				}
				if _, err := f.service.Store.DB.Exec("UPDATE versions SET raw_json=? WHERE owner_id=1 AND profile_id=7", body); err != nil {
					t.Fatal(err)
				}
				w = f.request("GET", "/miniapp/profile?id=7", "", token)
				if w.Code != 502 {
					t.Fatalf("decoded stored credential escaped: HTTP%d %s", w.Code, w.Body.String())
				}
			})
		}
	}
	for _, body := range []string{
		`{"userId":7,"access\u0054oken":"unknown-rotated-credential"}`,
		`{"userId":7,"nested":[{"REFRESH_token":"unknown-rotated-credential"}]}`,
		`{"userId":7,"authorization":"Bearer unknown-rotated-credential"}`,
		`{"userId":7,"echo":"{\"accessToken\":\"unknown-rotated-credential\"}"}`,
	} {
		f.getStatus, f.getBody = 200, body
		w := f.request("GET", "/miniapp/api/user/getUser?targetUserId=7", "", token)
		if w.Code != 502 {
			t.Fatalf("source credential field escaped: HTTP%d %s", w.Code, w.Body.String())
		}
	}
}

func TestMiniAppAcceptedUpstreamJSONRemainsExact(t *testing.T) {
	f := newMiniAppFixture(t)
	token := f.login(t, "/miniapp/auth")
	f.ownBody = " {\n  \"userId\": 101, \"freezeState\": false, \"type\": 0, \"description\": \"safe\\u0020text\"\n}\n"
	f.getStatus = 200
	f.getBody = " {\n  \"userId\": 7, \"description\": \"safe\\u0020text\"\n}\n"
	for _, tc := range []struct{ path, body string }{{"/miniapp/api/user/getMe", f.ownBody}, {"/miniapp/api/user/getUser?targetUserId=7", f.getBody}} {
		w := f.request("GET", tc.path, "", token)
		if w.Code != 200 || w.Body.String() != tc.body {
			t.Fatalf("accepted source changed: HTTP%d %q", w.Code, w.Body.String())
		}
	}
}

func TestMiniAppRejectsCredentialsBeforeAndAfterSourceRefresh(t *testing.T) {
	for _, secret := range []string{"source-access-token", "source-refresh-token", "rotated-access-1", "rotated-refresh-1", "rotated-access-2", "rotated-refresh-2"} {
		t.Run(secret, func(t *testing.T) {
			f := newMiniAppFixture(t)
			token := f.login(t, "/miniapp/auth")
			ss, err := f.service.Store.Session(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			ss.Expires = f.h.now().Add(-time.Minute).Unix()
			if err = f.service.Store.SaveSession(context.Background(), 1, ss); err != nil {
				t.Fatal(err)
			}
			refreshes := 0
			f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				status, body := 200, ""
				switch r.URL.Path {
				case "/api/appuser/refresh":
					if r.Method != "PUT" {
						t.Fatal("unexpected refresh method")
					}
					refreshes++
					body = fmt.Sprintf(`{"token":"rotated-access-%d","refreshToken":"rotated-refresh-%d"}`, refreshes, refreshes)
				case "/api/user/getMe":
					body = `{"userId":101,"freezeState":false,"type":0}`
				case "/api/user/getUser":
					if refreshes == 1 {
						status, body = 401, `{"error":"expired"}`
					} else {
						body = `{"userId":7,"nested":[{"echo":"` + miniAppEscapedString(secret) + `"}]}`
					}
				default:
					t.Fatalf("unexpected route %s", r.URL.Path)
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			w := f.request("GET", "/miniapp/api/user/getUser?targetUserId=7", "", token)
			if refreshes != 2 || w.Code != 502 {
				t.Fatalf("rotated source credential escaped: refreshes=%d HTTP%d %s", refreshes, w.Code, w.Body.String())
			}
		})
	}
}

func TestMiniAppStoredProfileMediaIsAuthenticatedAndConstrained(t *testing.T) {
	f := newMiniAppFixture(t)
	ctx := context.Background()
	p, err := f.service.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("fixture image bytes")
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	dir := filepath.Join(f.service.Config.DataDir, "media")
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, sha), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.Store.DB.ExecContext(ctx, "INSERT INTO media(sha,path,mime,size) VALUES(?,?,?,?)", sha, "/arbitrary/stored/path", "image/jpeg", len(data)); err != nil {
		t.Fatal(err)
	}
	result, err := f.service.Store.DB.ExecContext(ctx, "INSERT INTO media_jobs(owner_id,version_id,role,position,url,state,sha) VALUES(1,?,'photos',0,'https://api.jardulik.app/photo','complete',?)", p.VersionID, sha)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := result.LastInsertId()
	if _, err = f.service.Store.DB.ExecContext(ctx, "INSERT INTO media_jobs(owner_id,version_id,role,position,url,state,sha) VALUES(1,?,'miniPhotos',0,'https://api.jardulik.app/thumb','complete',?)", p.VersionID, sha); err != nil {
		t.Fatal(err)
	}
	token := f.login(t, "/miniapp/auth")
	w := f.request("GET", "/miniapp/profile?id=7", "", token)
	var profile struct {
		ID, VersionID int64
		Raw           Profile
		FormattedText string
		Media         []struct {
			ID        int64
			Role, URL string
		}
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &profile) != nil || profile.ID != 7 || profile.VersionID != p.VersionID || profile.Raw.ID() != 7 || profile.FormattedText != profileText(p) || len(profile.Media) != 1 || profile.Media[0].Role != "photos" || profile.Media[0].ID != id {
		t.Fatalf("stored profile HTTP%d %s", w.Code, w.Body.String())
	}
	path := profile.Media[0].URL
	if w = f.request("GET", path, "", ""); w.Code != 401 {
		t.Fatal("public media exposed")
	}
	if w = f.request("GET", path, "", token); w.Code != 200 || w.Body.String() != string(data) || w.Header().Get("Content-Type") != "image/jpeg" {
		t.Fatalf("media HTTP%d %s", w.Code, w.Body.String())
	}
	if w = f.request("GET", path+"?url=/etc/passwd", "", token); w.Code != 400 {
		t.Fatal("arbitrary media query accepted")
	}
	if err = os.Remove(filepath.Join(dir, sha)); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err = os.WriteFile(outside, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(dir, sha)); err != nil {
		t.Fatal(err)
	}
	if w = f.request("GET", path, "", token); w.Code != 404 {
		t.Fatal("media symlink followed")
	}
	f.service.Store.Pause(ctx, 1, true)
	if w = f.request("GET", "/miniapp/profile?id=7", "", token); w.Code != 401 {
		t.Fatal("paused owner read succeeded")
	}
}
