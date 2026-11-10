package companion

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	miniAppPublicOrigin  = "https://mini.jardulik.app"
	miniAppEntryPath     = "/assets/index-BFeBP-cW.js"
	miniAppEntrySHA      = "b8e7b1a5947c75272dcb5da9a882e577396b9650ad0bd4e6a837bd963e217fe1"
	miniAppHTMLSHA       = "41596b230907a0a2f120649c79416d90e52e4648d7282d1691f0fc77cf51dfda"
	miniAppAssetLimit    = 8 << 20
	miniAppCacheLimit    = 32 << 20
	miniAppCacheFiles    = 128
	miniAppLoginOriginal = `x2.post("/appuser/login/telegram",{initData:e})`
	miniAppLoginAdapted  = `x2.post("/appuser/login/telegram",{initData:e,targetUserId:Number(new URLSearchParams(location.search).get("targetUserId"))})`
)

var miniAppAssetName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*(?:\.[A-Za-z0-9_-]+)*\.(?:js|css|woff2?|ttf|otf|svg|png|webp|jpg|jpeg|gif|ico|json|wasm)$`)

// miniAppAssets handles public files only. All authenticated data stays in the gateway.
type miniAppAssets struct {
	cacheDir string
	client   *http.Client
	mu       sync.Mutex
}

// NewMiniAppAssets prepares an offline handler; public source fetching is lazy.
func NewMiniAppAssets(dataDir string) (http.Handler, error) {
	if dataDir == "" {
		return nil, errors.New("mini app assets require a data directory")
	}
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	// Refuse symlinked ancestors as well as symlinked cache entries.
	for p := dir; ; p = filepath.Dir(p) {
		info, statErr := os.Lstat(p)
		if statErr != nil {
			return nil, statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("unsafe mini app data directory")
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	cacheDir := filepath.Join(dir, "miniapp-public-assets")
	if err := os.Mkdir(cacheDir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	info, err := os.Lstat(cacheDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe mini app cache directory")
	}
	return &miniAppAssets{cacheDir: cacheDir, client: &http.Client{
		Timeout:       20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("mini app asset redirects are denied") },
	}}, nil
}

func (a *miniAppAssets) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/viewer" {
		serveMiniAppViewer(w, r)
		return
	}
	path := r.URL.Path
	if strings.Contains(r.URL.EscapedPath(), "%") || len(path) > 180 {
		http.NotFound(w, r)
		return
	}
	isHTML := path == "/" || path == "/mutuals"
	if isHTML {
		path = "/"
	}
	contentType, ok := miniAppAssetType(path)
	if !ok || (!isHTML && r.URL.RawQuery != "") {
		http.NotFound(w, r)
		return
	}
	body, err := a.source(r, path, contentType)
	if err == nil && isHTML {
		body, err = adaptMiniAppHTML(body)
	}
	if err == nil && path == miniAppEntryPath {
		body, err = patchMiniAppEntry(body, miniAppEntrySHA)
	}
	if err != nil {
		http.Error(w, "public mini app assets unavailable", http.StatusBadGateway)
		return
	}
	if isHTML {
		// The single pinned inline handler is allowed without allowing arbitrary inline JS.
		start := bytes.Index(body, []byte("<script>")) + len("<script>")
		end := bytes.Index(body[start:], []byte("</script>")) + start
		sum := sha256.Sum256(body[start:end])
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self' https://telegram.org 'sha256-"+base64.StdEncoding.EncodeToString(sum[:])+"'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' https: data: blob:; font-src 'self' data:; media-src 'self' https: blob:; worker-src 'self' blob:; base-uri 'none'; form-action 'none'; frame-ancestors https://telegram.org https://*.telegram.org")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func miniAppAssetType(path string) (string, bool) {
	if path == "/" {
		return "text/html; charset=utf-8", true
	}
	if path == "/favicon.svg" {
		return "image/svg+xml", true
	}
	if !strings.HasPrefix(path, "/assets/") {
		return "", false
	}
	name := strings.TrimPrefix(path, "/assets/")
	if len(name) > 160 || !miniAppAssetName.MatchString(name) {
		return "", false
	}
	switch filepath.Ext(name) {
	case ".js":
		return "text/javascript; charset=utf-8", true
	case ".css":
		return "text/css; charset=utf-8", true
	case ".woff":
		return "font/woff", true
	case ".woff2":
		return "font/woff2", true
	case ".ttf":
		return "font/ttf", true
	case ".otf":
		return "font/otf", true
	case ".svg":
		return "image/svg+xml", true
	case ".png":
		return "image/png", true
	case ".webp":
		return "image/webp", true
	case ".jpg", ".jpeg":
		return "image/jpeg", true
	case ".gif":
		return "image/gif", true
	case ".ico":
		return "image/x-icon", true
	case ".json":
		return "application/json", true
	case ".wasm":
		return "application/wasm", true
	}
	return "", false
}

// patchMiniAppEntry changes two reviewed integration points in this exact upstream release.
func patchMiniAppEntry(body []byte, expectedSHA string) ([]byte, error) {
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != expectedSHA {
		return nil, errors.New("mini app entry changed; review required")
	}
	if bytes.Count(body, []byte("https://api.jardulik.app/api")) != 2 || bytes.Count(body, []byte(miniAppLoginOriginal)) != 1 {
		return nil, errors.New("mini app integration points changed")
	}
	body = bytes.ReplaceAll(body, []byte("https://api.jardulik.app/api"), []byte("/miniapp/api"))
	body = bytes.Replace(body, []byte(miniAppLoginOriginal), []byte(miniAppLoginAdapted), 1)
	return body, nil
}

func adaptMiniAppHTML(body []byte) ([]byte, error) {
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != miniAppHTMLSHA {
		return nil, errors.New("mini app HTML changed; review required")
	}
	if bytes.Count(body, []byte(`src="`+miniAppEntryPath+`"`)) != 1 || bytes.Count(body, []byte("<script>")) != 1 || bytes.Count(body, []byte("</body>")) != 1 {
		return nil, errors.New("mini app HTML integration points changed")
	}
	label := `<div aria-label="Тестовый просмотр" style="position:fixed;top:max(3px,env(safe-area-inset-top));right:6px;z-index:2147483647;padding:3px 7px;border-radius:8px;background:#111c;color:#ddd;font:10px/1.3 system-ui;pointer-events:none">Тестовый просмотр</div>`
	return bytes.Replace(body, []byte("</body>"), []byte(label+"</body>"), 1), nil
}

func (a *miniAppAssets) source(r *http.Request, path, contentType string) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	root, err := os.OpenRoot(a.cacheDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := os.Lstat(a.cacheDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe mini app cache directory")
	}
	key := sha256.Sum256([]byte(path))
	name := hex.EncodeToString(key[:]) + ".asset"
	if body, err := readMiniAppCache(root, name, path); err == nil {
		return body, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, miniAppPublicOrigin+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", contentType)
	// Never copy caller headers, cookies, auth, query strings or upstream response headers.
	res, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New("mini app public source rejected request")
	}
	got, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	want, _, _ := mime.ParseMediaType(contentType)
	if err != nil || !miniAppContentTypeMatches(want, got) {
		return nil, errors.New("unexpected mini app content type")
	}
	limit := int64(miniAppAssetLimit)
	if path == "/" {
		limit = 64 << 10
	}
	if res.ContentLength > limit {
		return nil, errors.New("mini app asset too large")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		return nil, errors.New("mini app asset read limit")
	}
	if path == "/" {
		if _, err := adaptMiniAppHTML(body); err != nil {
			return nil, err
		}
	}
	if path == miniAppEntryPath {
		if _, err := patchMiniAppEntry(body, miniAppEntrySHA); err != nil {
			return nil, err
		}
	}
	if err := pruneMiniAppCache(root, int64(len(body))); err != nil {
		return nil, err
	}
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	_, writeErr := f.Write(body)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		_ = root.Remove(name)
		return nil, errors.New("mini app cache write failed")
	}
	return body, nil
}

func miniAppContentTypeMatches(want, got string) bool {
	if want == got {
		return true
	}
	switch want {
	case "text/javascript":
		return got == "application/javascript"
	case "font/woff":
		return got == "application/font-woff" || got == "application/octet-stream"
	case "font/woff2":
		return got == "application/font-woff2" || got == "application/octet-stream"
	case "font/ttf":
		return got == "application/x-font-ttf" || got == "application/octet-stream"
	case "font/otf":
		return got == "application/x-font-opentype" || got == "application/octet-stream"
	}
	return false
}

func readMiniAppCache(root *os.Root, name, path string) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > miniAppAssetLimit {
		return nil, errors.New("unsafe mini app cache file")
	}
	ttl := 24 * time.Hour
	if path == "/" {
		ttl = 10 * time.Minute
	}
	if time.Since(info.ModTime()) > ttl {
		if err := root.Remove(name); err != nil {
			return nil, err
		}
		return nil, os.ErrNotExist
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, miniAppAssetLimit+1))
	if err != nil || len(body) > miniAppAssetLimit {
		return nil, errors.New("mini app cache read limit")
	}
	return body, nil
}

func pruneMiniAppCache(root *os.Root, incoming int64) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, err := dir.ReadDir(-1)
	_ = dir.Close()
	if err != nil {
		return err
	}
	type cached struct {
		name string
		info os.FileInfo
	}
	files := make([]cached, 0, len(entries))
	var total int64
	for _, e := range entries {
		info, err := root.Lstat(e.Name())
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || len(e.Name()) != 70 || !strings.HasSuffix(e.Name(), ".asset") {
			return errors.New("unsafe mini app cache contents")
		}
		files = append(files, cached{e.Name(), info})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().Before(files[j].info.ModTime()) })
	for len(files) >= miniAppCacheFiles || total+incoming > miniAppCacheLimit {
		if len(files) == 0 {
			return errors.New("mini app cache capacity exceeded")
		}
		if err := root.Remove(files[0].name); err != nil {
			return err
		}
		total -= files[0].info.Size()
		files = files[1:]
	}
	return nil
}
