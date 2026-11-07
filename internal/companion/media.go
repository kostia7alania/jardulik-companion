package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MediaManager struct {
	Store  *Store
	Config Config
	HTTP   *http.Client
	mu     sync.Mutex
}

func NewMediaManager(s *Store, c Config) *MediaManager {
	return &MediaManager{Store: s, Config: c, HTTP: &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 3 || !allowedMediaURL(req.URL) {
			return errors.New("untrusted media redirect")
		}
		return nil
	}}}
}
func allowedMediaURL(u *url.URL) bool {
	return u.Scheme == "https" && u.User == nil && (u.Host == "mini.jardulik.app" || u.Host == "api.jardulik.app")
}
func FreeBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
func (m *MediaManager) CheckDisk() error {
	n, err := FreeBytes(m.Config.DataDir)
	if err != nil {
		return err
	}
	if n < m.Config.MinFreeBytes {
		return errors.New("сбор приостановлен: на сервере осталось меньше резервного места")
	}
	return nil
}
func (m *MediaManager) Ensure(ctx context.Context, j MediaJob) (resultErr error) {
	mu := m.Store.SessionMutex(j.OwnerID)
	mu.Lock()
	defer mu.Unlock()
	m.Store.mediaMu.Lock()
	defer m.Store.mediaMu.Unlock()
	var connected bool
	if err := m.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE owner_id=? AND state='connected')", j.OwnerID).Scan(&connected); err != nil {
		return err
	}
	if !connected {
		return errors.New("media collection stopped for disconnected account")
	}
	if j.State == "complete" {
		return nil
	}
	var state string
	if err := m.Store.DB.QueryRowContext(ctx, "SELECT state FROM media_jobs WHERE id=? AND owner_id=?", j.ID, j.OwnerID).Scan(&state); err != nil {
		return err
	} else if state == "complete" {
		return nil
	}
	if err := m.CheckDisk(); err != nil {
		return err
	}
	u, err := url.Parse(j.URL)
	if err != nil || !allowedMediaURL(u) {
		m.Store.MediaError(ctx, j.ID, "untrusted_media_origin")
		return errors.New("untrusted media origin")
	}
	r, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	upstreamHeaders(r, m.Config.UserAgent, true)
	defer func() {
		if resultErr != nil && ctx.Err() == nil {
			m.Store.MediaFailure(ctx, j.ID, "download_failed")
		}
	}()
	res, err := m.HTTP.Do(r)
	if err != nil {
		return redactedTransportError(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		m.Store.MediaError(ctx, j.ID, fmt.Sprintf("HTTP_%d", res.StatusCode))
		return errors.New("photo unavailable")
	}
	mime := strings.ToLower(strings.Split(res.Header.Get("Content-Type"), ";")[0])
	if !strings.HasPrefix(mime, "image/") {
		m.Store.MediaError(ctx, j.ID, "unexpected_content_type")
		return errors.New("media response is not an image")
	}
	dir := filepath.Join(m.Config.DataDir, "media")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, 49<<20))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if n >= 49<<20 {
		m.Store.MediaError(ctx, j.ID, "oversize")
		return errors.New("photo exceeds whole-file limit")
	}
	sha := hex.EncodeToString(h.Sum(nil))
	path := filepath.Join(dir, sha)
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	return m.Store.MediaDone(ctx, j, sha, path, mime, n)
}
func (m *MediaManager) EnsureProfile(ctx context.Context, owner, version int64) error {
	jobs, err := m.Store.MediaJobs(ctx, owner, version)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.State == "pending" {
			if err = m.Ensure(ctx, j); err != nil {
				return err
			}
		}
	}
	return nil
}
func (m *MediaManager) Run(ctx context.Context) {
	for ctx.Err() == nil {
		_ = m.Store.CollectOrphans(ctx)
		j, err := m.Store.PendingMedia(ctx)
		if err == nil {
			_ = m.Ensure(ctx, j)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
