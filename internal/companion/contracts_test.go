package companion

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type handlerTransport struct{ handler http.Handler }

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	t.handler.ServeHTTP(w, r)
	res := w.Result()
	res.Request = r
	return res, nil
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{DataDir: t.TempDir(), OwnerID: 1, SourceBotID: 42, SessionKey: make([]byte, 32), BackupKey: make([]byte, 32), APIBase: "http://invalid", TelegramBase: "http://invalid", InitMaxAge: 24 * time.Hour}
}
func testStore(t *testing.T) (*Store, Config) {
	t.Helper()
	c := testConfig(t)
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, c
}
func signedData(t *testing.T, owner, bot int64, at time.Time) (string, ed25519.PublicKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	q := url.Values{"auth_date": {fmt.Sprint(at.Unix())}, "user": {fmt.Sprintf(`{"id":%d,"first_name":"Ира + & /"}`, owner)}, "chat_type": {"sender"}, "hash": {"unused"}}
	var keys []string
	for k := range q {
		if k != "hash" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	lines := []string{fmt.Sprintf("%d:WebAppData", bot)}
	for _, k := range keys {
		lines = append(lines, k+"="+q.Get(k))
	}
	q.Set("signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(strings.Join(lines, "\n")))))
	return q.Encode(), pub
}

func TestSignedIdentityAndAge(t *testing.T) {
	now := time.Now()
	raw, pub := signedData(t, 1, 42, now)
	if err := validateInitData(raw, 42, 1, now, time.Hour, pub); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"another_tenant": func() error { return validateInitData(raw, 42, 2, now, time.Hour, pub) },
		"another_bot":    func() error { return validateInitData(raw, 43, 1, now, time.Hour, pub) },
		"expired":        func() error { return validateInitData(raw, 42, 1, now.Add(2*time.Hour), time.Hour, pub) },
		"duplicate_key":  func() error { return validateInitData(raw+"&user=x", 42, 1, now, time.Hour, pub) },
		"tampered": func() error {
			return validateInitData(strings.Replace(raw, "sender", "group", 1), 42, 1, now, time.Hour, pub)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if check() == nil {
				t.Fatal("accepted invalid credential")
			}
		})
	}
	u := "https://mini.jardulik.app/#tgWebAppData=" + url.QueryEscape(raw) + "&tgWebAppVersion=9.6"
	got, err := ParseInitData(u)
	if err != nil || got != raw {
		t.Fatalf("lost exact inner encoding: %v", err)
	}
	if _, err = ParseInitData("https://127.0.0.1/#tgWebAppData=x"); err == nil {
		t.Fatal("accepted a visitor-controlled origin")
	}
}

func TestLiveRegisteredLoginResponse(t *testing.T) {
	for _, code := range []int{200, 201} {
		packet := fmt.Sprintf(`{"code":%d,"isReged":true,"message":"Login successful","token":"access","refreshToken":"refresh","userId":5696}`, code)
		s, err := decodeLoginResponse([]byte(packet))
		if err != nil || s.UserID != 5696 || s.Token != "access" || s.Refresh != "refresh" {
			t.Fatalf("registered login code %d rejected: %v", code, err)
		}
	}
	for _, packet := range []string{
		`{"code":201,"token":"access","refreshToken":"refresh","userId":5696}`,
		`{"code":201,"isReged":false,"token":"access","refreshToken":"refresh","userId":5696}`,
		`{"code":405,"isReged":true,"token":"access","refreshToken":"refresh","userId":5696}`,
		`{"code":401,"isReged":true,"token":"access","refreshToken":"refresh","userId":5696}`,
		`{"code":200,"isReged":true,"token":"access","userId":5696}`,
	} {
		if _, err := decodeLoginResponse([]byte(packet)); err == nil {
			t.Fatal("accepted incomplete, unregistered or restricted login")
		}
	}
}

func TestTenantIsolationAndCompleteBatch(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		if err := s.User(ctx, owner, ""); err != nil {
			t.Fatal(err)
		}
	}
	profiles := []Profile{{"userId": 101, "name": "A", "description": "текст без фото"}, {"userId": 102, "name": "B", "photos": []any{"https://mini.jardulik.app/a.webp", "https://mini.jardulik.app/b.webp"}}}
	ids, err := s.Archive(ctx, 1, profiles, "contract-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Version(ctx, 2, ids[0]); err == nil {
		t.Fatal("cross-tenant version readable")
	}
	if err = s.Favorite(ctx, 2, 101); err == nil {
		t.Fatal("cross-tenant favorite created")
	}
	if n := s.Counts(ctx, 1); n["profiles"] != 2 || n["pending_photos"] != 2 {
		t.Fatalf("batch not fully archived: %v", n)
	}
	if n := s.Counts(ctx, 2); n["profiles"] != 0 {
		t.Fatalf("another tenant saw archive: %v", n)
	}
	profiles[0]["description"] = "другая версия"
	if _, err = s.Archive(ctx, 1, profiles, "second-read"); err != nil {
		t.Fatal(err)
	}
	history, err := s.History(ctx, 1, 101)
	if err != nil || len(history) != 2 {
		t.Fatalf("history lost: %d %v", len(history), err)
	}
	if n := s.Counts(ctx, 1); n["versions"] != 3 {
		t.Fatalf("unchanged profile duplicated: %v", n)
	}
	if err = s.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if n := s.Counts(ctx, 1); n["profiles"] != 0 {
		t.Fatal("deletion left tenant observations")
	}
}

func TestSessionCiphertextIsBoundToTenant(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		_ = s.User(ctx, owner, "")
		if err := s.SaveSession(ctx, owner, Session{Token: "secret", Refresh: "refresh", UserID: 9}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.DB.Exec("UPDATE sessions SET encrypted=(SELECT encrypted FROM sessions WHERE owner_id=1) WHERE owner_id=2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Session(ctx, 2); err == nil {
		t.Fatal("ciphertext accepted for another tenant")
	}
}

func jwt(exp int64) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp))) + ".signature"
}
func TestConcurrentRefreshRotatesBothTokensOnce(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	_ = s.User(ctx, 1, "")
	old := jwt(time.Now().Add(time.Hour).Unix())
	newToken := jwt(time.Now().Add(2 * time.Hour).Unix())
	var refreshes atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/appuser/refresh" {
			refreshes.Add(1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["refreshToken"] != "refresh-1" {
				t.Error("refresh token was not preserved")
			}
			time.Sleep(10 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]string{"token": newToken, "refreshToken": "refresh-2"})
			return
		}
		if r.Header.Get("Authorization") == "Bearer "+old {
			w.WriteHeader(401)
			w.Write([]byte(`{}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+newToken {
			t.Error("wrong session used")
		}
		w.Write([]byte(`{"ok":true}`))
	})
	cfg.APIBase = "https://jardulik.test"
	client := NewClient(s, cfg)
	client.HTTP.Transport = handlerTransport{handler}
	if err := s.SaveSession(ctx, 1, Session{Token: old, Refresh: "refresh-1", UserID: 9, Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.Request(ctx, 1, "GET", "/read", nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if refreshes.Load() != 1 {
		t.Fatalf("refresh race: %d", refreshes.Load())
	}
	session, err := s.Session(ctx, 1)
	if err != nil || session.Refresh != "refresh-2" || session.Token != newToken {
		t.Fatal("rotation not committed atomically", err)
	}
}

func TestDisabledActionsNeverReachNetwork(t *testing.T) {
	s, cfg := testStore(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	cfg.APIBase = "https://jardulik.test"
	client := NewClient(s, cfg)
	client.HTTP.Transport = handlerTransport{handler}
	if _, err := client.Like(context.Background(), 1, 9); err == nil {
		t.Fatal("disabled like accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("disabled like performed a request")
	}
}

func TestUpdateDedupAndSnapshot(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	ok, err := s.ClaimUpdate(ctx, 99)
	if !ok || err != nil {
		t.Fatal(err)
	}
	if ok, err = s.ClaimUpdate(ctx, 99); ok || err != nil {
		t.Fatal("duplicate claimed", err)
	}
	if err = s.Snapshot(ctx, s.cfg.DataDir+"/copy.sqlite"); err != nil {
		t.Fatal("consistent snapshot", err)
	}
}

func TestMediaOriginPolicy(t *testing.T) {
	for _, raw := range []string{"https://mini.jardulik.app/a.webp", "https://api.jardulik.app/a.webp"} {
		u, _ := url.Parse(raw)
		if !allowedMediaURL(u) {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{"http://mini.jardulik.app/a", "https://mini.jardulik.app.evil.test/a", "https://mini.jardulik.app:8443/a", "https://127.0.0.1/a", "file:///etc/passwd"} {
		u, _ := url.Parse(raw)
		if allowedMediaURL(u) {
			t.Fatal("SSRF origin accepted", raw)
		}
	}
}

func TestCurrentVersionCanRevert(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	_ = s.User(ctx, 1, "")
	p := Profile{"userId": 7, "description": "A"}
	for _, text := range []string{"A", "B", "A"} {
		p["description"] = text
		if _, err := s.Archive(ctx, 1, []Profile{p}, "read"); err != nil {
			t.Fatal(err)
		}
	}
	current, err := s.Profile(ctx, 1, 7)
	if err != nil || current.Profile.String("description") != "A" {
		t.Fatal("reverted version not current", err)
	}
	history, err := s.History(ctx, 1, 7)
	if err != nil || len(history) != 2 || history[0].Profile.String("description") != "A" {
		t.Fatal("observation order lost", err)
	}
}

func TestDisconnectedMediaAndFailureBackoff(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	_ = s.User(ctx, 1, "")
	_ = s.SaveSession(ctx, 1, Session{Token: "x", Refresh: "y", UserID: 9})
	ids, err := s.Archive(ctx, 1, []Profile{{"userId": 7, "photos": []any{"https://mini.jardulik.app/1.webp", "https://mini.jardulik.app/2.webp"}}}, "read")
	if err != nil {
		t.Fatal(err)
	}
	jobs, _ := s.MediaJobs(ctx, 1, ids[0])
	s.MediaFailure(ctx, jobs[0].ID, "network")
	next, err := s.PendingMedia(ctx)
	if err != nil || next.ID != jobs[1].ID {
		t.Fatal("one failing URL blocked the queue", err)
	}
	if err = s.Disconnect(ctx, 1); err != nil {
		t.Fatal(err)
	}
	m := NewMediaManager(s, cfg)
	if err = m.Ensure(ctx, jobs[1]); err == nil {
		t.Fatal("disconnected media fetched")
	}
}

func TestDeleteCollectsOnlyUnsharedMedia(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		_ = s.User(ctx, owner, "")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "media"), 0700); err != nil {
		t.Fatal(err)
	}
	shared := digest([]byte("shared"))
	exclusive := digest([]byte("exclusive"))
	for _, sha := range []string{shared, exclusive} {
		if err := os.WriteFile(filepath.Join(cfg.DataDir, "media", sha), []byte("bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []int64{1, 2} {
		ids, err := s.Archive(ctx, owner, []Profile{{"userId": owner * 10, "photos": []any{"https://mini.jardulik.app/shared.webp"}}}, "read")
		if err != nil {
			t.Fatal(err)
		}
		jobs, _ := s.MediaJobs(ctx, owner, ids[0])
		if err = s.MediaDone(ctx, jobs[0], shared, filepath.Join(cfg.DataDir, "media", shared), "image/webp", 5); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := s.Archive(ctx, 1, []Profile{{"userId": 99, "photos": []any{"https://mini.jardulik.app/exclusive.webp"}}}, "read")
	if err != nil {
		t.Fatal(err)
	}
	jobs, _ := s.MediaJobs(ctx, 1, ids[0])
	if err = s.MediaDone(ctx, jobs[0], exclusive, filepath.Join(cfg.DataDir, "media", exclusive), "image/webp", 5); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(cfg.DataDir, "media", exclusive)); !os.IsNotExist(err) {
		t.Fatal("orphaned personal bytes retained", err)
	}
	if _, err = os.Stat(filepath.Join(cfg.DataDir, "media", shared)); err != nil {
		t.Fatal("another tenant's media removed", err)
	}
}
