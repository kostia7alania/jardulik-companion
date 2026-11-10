package companion

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type miniAppRoundTripper func(*http.Request) (*http.Response, error)

func (f miniAppRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func miniAppAssetsFixture(t *testing.T) *miniAppAssets {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewMiniAppAssets(dir)
	if err != nil {
		t.Fatal(err)
	}
	return h.(*miniAppAssets)
}
func miniAppSourceResponse(req *http.Request, status int, typ string, body []byte) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {typ}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: req}
}
func miniAppRequest(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestMiniAppEntryPinnedPatchContract(t *testing.T) {
	source := []byte(`const a="https://api.jardulik.app/api",b="https://api.jardulik.app/api";async function c_(e){return ` + miniAppLoginOriginal + `}`)
	hash := sha256.Sum256(source)
	patched, err := patchMiniAppEntry(source, hex.EncodeToString(hash[:]))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(patched, []byte("/miniapp/api")) != 2 || bytes.Count(patched, []byte(miniAppLoginAdapted)) != 1 || bytes.Contains(patched, []byte("https://api.jardulik.app")) {
		t.Fatalf("incorrect exact integration patch: %s", patched)
	}
	if !bytes.Contains(source, []byte(miniAppLoginOriginal)) {
		t.Fatal("patch mutated source buffer")
	}
	if _, err := patchMiniAppEntry(source, miniAppEntrySHA); err == nil {
		t.Fatal("unreviewed source accepted")
	}
	for _, bad := range []string{strings.Replace(string(source), "https://api.jardulik.app/api", "missing", 1), string(source) + miniAppLoginOriginal} {
		sum := sha256.Sum256([]byte(bad))
		if _, err := patchMiniAppEntry([]byte(bad), hex.EncodeToString(sum[:])); err == nil {
			t.Fatal("changed integration count accepted")
		}
	}
}

func TestMiniAppAssetsOriginalShellAndLocalCSP(t *testing.T) {
	a := miniAppAssetsFixture(t)
	calls := 0
	a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != miniAppPublicOrigin+"/" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Fatalf("private request headers or query copied: %v", r)
		}
		res := miniAppSourceResponse(r, 200, "text/html", []byte(miniAppOriginalHTMLFixture))
		res.Header.Set("Set-Cookie", "upstream=secret")
		res.Header.Set("Content-Security-Policy", "connect-src *")
		return res, nil
	})
	req := httptest.NewRequest("GET", "/mutuals?targetUserId=123&noCheck=1", nil)
	req.Header.Set("Cookie", "private=value")
	req.Header.Set("Authorization", "Bearer private")
	w := httptest.NewRecorder()
	a.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `src="https://telegram.org/js/telegram-web-app.js?62"`) || !strings.Contains(w.Body.String(), miniAppEntryPath) || strings.Count(w.Body.String(), "Тестовый просмотр") != 2 {
		t.Fatal("original UI or test label missing")
	}
	if w.Header().Get("Set-Cookie") != "" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "connect-src 'self';") || strings.Contains(w.Header().Get("Content-Security-Policy"), "unsafe-inline'; style") {
		t.Fatal("unsafe source headers or scripts")
	}
	if got := miniAppRequest(t, a, "HEAD", "/mutuals?targetUserId=123&noCheck=1"); got.Code != 200 || got.Body.Len() != 0 || calls != 1 {
		t.Fatalf("HEAD/cache failed: status=%d calls=%d", got.Code, calls)
	}
}

func TestMiniAppAssetsAcceptsProviderJavaScriptNegotiation(t *testing.T) {
	a := miniAppAssetsFixture(t)
	a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
		// The provider rejects the stricter text/javascript Accept value with 406.
		if r.Header.Get("Accept") != "*/*" {
			return miniAppSourceResponse(r, 406, "text/plain", []byte("Not acceptable")), nil
		}
		return miniAppSourceResponse(r, 200, "application/javascript", []byte("export default 1;")), nil
	})
	w := miniAppRequest(t, a, "GET", "/assets/chunk.js")
	if w.Code != 200 || w.Body.String() != "export default 1;" || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/javascript") {
		t.Fatalf("public JS did not survive negotiation: status=%d", w.Code)
	}
}

func TestMiniAppAssetsPathBoundaryAndHeaders(t *testing.T) {
	a := miniAppAssetsFixture(t)
	calls := 0
	a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != miniAppPublicOrigin+"/assets/test.css" {
			t.Fatalf("unexpected upstream %s", r.URL)
		}
		return miniAppSourceResponse(r, 200, "text/css", []byte("body{}")), nil
	})
	for _, path := range []string{"/assets/../secret.css", "/assets/%2e%2e/secret.css", "/assets/a/b.js", "/assets/a.css?url=https://evil.test", "/assets/a.css?x=1", "/assets/.hidden.js", "/assets/x.html", "//evil.test/a.css", "/miniapp/api/appuser/getMe", "/assets/" + strings.Repeat("a", 161) + ".js"} {
		if w := miniAppRequest(t, a, "GET", path); w.Code != 404 {
			t.Fatalf("%s accepted: %d", path, w.Code)
		}
	}
	if calls != 0 {
		t.Fatal("rejected path fetched upstream")
	}
	if w := miniAppRequest(t, a, "POST", "/assets/test.css"); w.Code != 405 {
		t.Fatal(w.Code)
	}
	w := miniAppRequest(t, a, "GET", "/assets/test.css")
	if w.Code != 200 || w.Body.String() != "body{}" || w.Header().Get("Content-Type") != "text/css; charset=utf-8" || w.Header().Get("Cache-Control") != "public, max-age=86400" {
		t.Fatalf("bad public css response: %d %v", w.Code, w.Header())
	}
	if w := miniAppRequest(t, a, "GET", "/assets/test.css"); w.Code != 200 || calls != 1 {
		t.Fatal("public cache miss")
	}
}

func TestMiniAppAssetsFetchFailuresAreNotCached(t *testing.T) {
	for _, kind := range []string{"status", "type", "large", "stream-large", "redirect", "changed-entry", "changed-html"} {
		t.Run(kind, func(t *testing.T) {
			a := miniAppAssetsFixture(t)
			calls := 0
			path := "/assets/test.css"
			if kind == "changed-entry" {
				path = miniAppEntryPath
			}
			if kind == "changed-html" {
				path = "/mutuals"
			}
			a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				res := miniAppSourceResponse(r, 200, "text/css", []byte("body{}"))
				switch kind {
				case "status":
					res.StatusCode = 503
				case "type":
					res.Header.Set("Content-Type", "text/html")
				case "large":
					res.ContentLength = miniAppAssetLimit + 1
				case "stream-large":
					res.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", miniAppAssetLimit+1)))
					res.ContentLength = -1
				case "redirect":
					res.StatusCode = 302
					res.Header.Set("Location", "https://evil.test/steal")
				case "changed-entry":
					res.Header.Set("Content-Type", "application/javascript")
				case "changed-html":
					res.Header.Set("Content-Type", "text/html")
				}
				return res, nil
			})
			if w := miniAppRequest(t, a, "GET", path); w.Code != 502 {
				t.Fatalf("bad source accepted: %d", w.Code)
			}
			files, err := os.ReadDir(a.cacheDir)
			if err != nil || len(files) != 0 || calls != 1 {
				t.Fatalf("failure cached or redirect followed: %d files, %d calls, %v", len(files), calls, err)
			}
		})
	}
}

func TestMiniAppAssetsDiskBoundAndSymlinkRefusal(t *testing.T) {
	a := miniAppAssetsFixture(t)
	for i := 0; i < miniAppCacheFiles; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprint(i)))
		name := filepath.Join(a.cacheDir, hex.EncodeToString(sum[:])+".asset")
		if err := os.WriteFile(name, []byte("old"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
		return miniAppSourceResponse(r, 200, "text/css", []byte("new")), nil
	})
	if w := miniAppRequest(t, a, "GET", "/assets/test.css"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	entries, err := os.ReadDir(a.cacheDir)
	if err != nil || len(entries) != miniAppCacheFiles {
		t.Fatalf("cache not bounded: %d %v", len(entries), err)
	}
	sum := sha256.Sum256([]byte("/assets/symlink.css"))
	target := filepath.Join(filepath.Dir(a.cacheDir), "not-a-public-asset")
	if err := os.WriteFile(target, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(a.cacheDir, hex.EncodeToString(sum[:])+".asset")); err != nil {
		t.Fatal(err)
	}
	if w := miniAppRequest(t, a, "GET", "/assets/symlink.css"); w.Code != 502 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("symlink cache entry served")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "private" {
		t.Fatal("symlink target changed")
	}
}

func TestMiniAppAssetsRejectSymlinkDataDirectory(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := NewMiniAppAssets(link); err == nil {
		t.Fatal("symlink data directory accepted")
	}
}

func TestMiniAppAssetsExpiredCacheRefetch(t *testing.T) {
	a := miniAppAssetsFixture(t)
	calls := 0
	a.client.Transport = miniAppRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		return miniAppSourceResponse(r, 200, "text/css", []byte("new")), nil
	})
	key := sha256.Sum256([]byte("/assets/test.css"))
	name := filepath.Join(a.cacheDir, hex.EncodeToString(key[:])+".asset")
	if err := os.WriteFile(name, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(name, old, old); err != nil {
		t.Fatal(err)
	}
	if w := miniAppRequest(t, a, "GET", "/assets/test.css"); w.Code != 200 || w.Body.String() != "new" || calls != 1 {
		t.Fatal("expired asset retained")
	}
}

const miniAppOriginalHTMLFixture = "<!doctype html>\n<html lang=\"en\">\n\t<head>\n\t\t<meta charset=\"UTF-8\" />\n\t\t<link rel=\"icon\" href=\"/favicon.svg\" />\n\t\t<meta\n\t\t\tname=\"viewport\"\n\t\t\tcontent=\"width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover\"\n\t\t/>\n\t\t<meta name=\"theme-color\" content=\"#000000\" />\n\t\t<script src=\"https://telegram.org/js/telegram-web-app.js?62\"></script>\n\t\t<title>jærdulik</title>\n\t\t<script type=\"module\" crossorigin src=\"/assets/index-BFeBP-cW.js\"></script>\n\t\t<link rel=\"stylesheet\" crossorigin href=\"/assets/index-BREeF2Nh.css\">\n\t</head>\n\t<body>\n\t\t<div id=\"root\"></div>\n\t\t<script>\n\t\t\tdocument.addEventListener(\n\t\t\t\t\"contextmenu\",\n\t\t\t\tfunction (e) {\n\t\t\t\t\te.preventDefault();\n\t\t\t\t},\n\t\t\t\t{ capture: true },\n\t\t\t);\n\t\t</script>\n\t</body>\n</html>\n"
