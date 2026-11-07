package companion

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type archiveRoundTripper func(*http.Request) (*http.Response, error)

func (f archiveRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func archiveTestStore(t *testing.T) (Config, *Store) {
	t.Helper()
	c := Config{DataDir: t.TempDir(), OwnerID: 123, SourceBotID: 456, BackupChatID: -100123, BackupThreadID: 7, Token: "test-only-token", SessionKey: bytes.Repeat([]byte{1}, 32), BackupKey: bytes.Repeat([]byte{2}, 32)}
	s, err := OpenStore(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.User(context.Background(), 123, "test"); err != nil {
		t.Fatal(err)
	}
	return c, s
}

func TestServiceBackupRestoresSQLiteMediaAndSessionKey(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	if err := s.SaveSession(ctx, 123, Session{Token: "private-access", Refresh: "private-refresh", UserID: 99, Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Пример", "description": "Исходный текст", "photos": []string{"https://mini.jardulik.app/photo.jpg"}}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.MediaJobs(ctx, 123, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	photo := []byte("whole-original-photo-bytes")
	sha := digest(photo)
	if err = os.MkdirAll(filepath.Join(c.DataDir, "media"), 0700); err != nil {
		t.Fatal(err)
	}
	mediaPath := filepath.Join(c.DataDir, "media", sha)
	if err = os.WriteFile(mediaPath, photo, 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.MediaDone(ctx, jobs[0], sha, mediaPath, "image/jpeg", int64(len(photo))); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	if err = a.init(ctx); err != nil {
		t.Fatal(err)
	}
	path, manifest, err := a.serviceZIP(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Coverage["media_embedded"] != true {
		t.Fatal("small source media not embedded")
	}
	plain, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := seal(c.BackupKey, plain, serviceBackupPurpose)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := append([]byte(serviceBackupMagic), sealed...)
	encPath := path + ".enc"
	if err = os.WriteFile(encPath, encrypted, 0600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err = RestoreArchive(encPath, dest, c.BackupKey); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "media", sha))
	if err != nil || !bytes.Equal(photo, got) {
		t.Fatalf("source photo changed: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "recovery-config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recovered map[string]any
	if err = json.Unmarshal(b, &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered["SESSION_KEY"] != base64.StdEncoding.EncodeToString(c.SessionKey) {
		t.Fatal("session key absent from authenticated recovery config")
	}
	if _, ok := recovered["BACKUP_KEY"]; ok {
		t.Fatal("backup key must not be packed inside its own archive")
	}
	if recovered["ALLOW_REACTIONS"] != false {
		t.Fatal("restore must default to observation")
	}
	rc := c
	rc.DataDir = dest
	rs, err := OpenStore(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	session, err := rs.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if session.Token != "private-access" || session.Refresh != "private-refresh" {
		t.Fatal("encrypted account session not recoverable")
	}
	if err = RestoreArchive(encPath, dest, c.BackupKey); err == nil {
		t.Fatal("restore overwrote a nonempty destination")
	}
}

func TestRestoreAuthenticatesBeforeWriting(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	sealed, err := seal(key, []byte("not a ZIP"), serviceBackupPurpose)
	if err != nil {
		t.Fatal(err)
	}
	b := append([]byte(serviceBackupMagic), sealed...)
	b[len(b)-1] ^= 1
	input := filepath.Join(t.TempDir(), "bad.enc")
	if err = os.WriteFile(input, b, 0600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "must-not-exist")
	if err = RestoreArchive(input, dest, key); err == nil {
		t.Fatal("tampered archive accepted")
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("unauthenticated backup wrote destination")
	}
}

func TestRestoreRejectsTraversalAndSymlinksBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		symlink bool
	}{{"../escape", false}, {"media/" + strings.Repeat("a", 64), true}} {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			w := zip.NewWriter(&b)
			h := &zip.FileHeader{Name: tc.name}
			if tc.symlink {
				h.SetMode(os.ModeSymlink | 0777)
			}
			entry, err := w.CreateHeader(h)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = entry.Write([]byte("../outside"))
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := zip.NewReader(bytes.NewReader(b.Bytes()), int64(b.Len()))
			if err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(t.TempDir(), "new")
			if err = restoreServiceZIP(r, dest); err == nil {
				t.Fatal("unsafe archive accepted")
			}
			if _, err = os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("unsafe archive wrote files")
			}
		})
	}
}

func TestMirrorFingerprintIgnoresViewerDistanceButKeepsProfileChanges(t *testing.T) {
	p := Profile{"userId": 99, "name": "Пример", "distance": 1, "reactionView": false, "description": "один"}
	a, err := mirrorFingerprint(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	p["distance"] = 100
	p["reactionView"] = true
	b, err := mirrorFingerprint(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("viewer-specific fields produced a new moderation version")
	}
	p["description"] = "два"
	b, err = mirrorFingerprint(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("description change was hidden")
	}
}

func TestMirrorFingerprintKeepsEveryPhotoRoleAndPosition(t *testing.T) {
	p := Profile{"userId": 99, "photos": []string{"https://mini.jardulik.app/full.jpg"}, "miniPhotos": []string{"https://mini.jardulik.app/thumb.jpg"}}
	jobs := []MediaJob{{Role: "photos", Position: 0, SHA: strings.Repeat("a", 64), State: "complete"}, {Role: "miniPhotos", Position: 0, SHA: strings.Repeat("b", 64), State: "complete"}}
	first, err := mirrorFingerprint(p, jobs)
	if err != nil {
		t.Fatal(err)
	}
	jobs[1].SHA = strings.Repeat("c", 64)
	changedThumb, err := mirrorFingerprint(p, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if first == changedThumb {
		t.Fatal("a new thumbnail would be omitted from every profile ZIP")
	}
	jobs[1].Position = 1
	changedPosition, err := mirrorFingerprint(p, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if changedThumb == changedPosition {
		t.Fatal("photo position was lost from archive deduplication")
	}
}

func archiveTestMedia(t *testing.T, s *Store, c Config, profileID int64) (int64, string, []byte) {
	t.Helper()
	ctx := context.Background()
	ids, err := s.Archive(ctx, 123, []Profile{{"userId": profileID, "name": "Пример", "photos": []string{"https://mini.jardulik.app/photo.jpg"}}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.MediaJobs(ctx, 123, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	photo := []byte("immutable complete source photo")
	sha := digest(photo)
	path := filepath.Join(c.DataDir, "media", sha)
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, photo, 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.MediaDone(ctx, jobs[0], sha, path, "image/jpeg", int64(len(photo))); err != nil {
		t.Fatal(err)
	}
	return ids[0], sha, photo
}

func archiveTestReceipt(t *testing.T, s *Store, sha string, message int64) {
	t.Helper()
	_, err := s.DB.ExecContext(context.Background(), `INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size) VALUES(?,'profile_version','source.zip',4,-100123,7,?,'file','unique',4)`, sha, message)
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceZIPInventoryComesFromSnapshot(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	_, sha, photo := archiveTestMedia(t, s, c, 99)
	oldReceipt := strings.Repeat("d", 64)
	archiveTestReceipt(t, s, oldReceipt, 1)
	snapshot := filepath.Join(t.TempDir(), "consistent.sqlite")
	if err := s.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	// Change only live inventory after the snapshot. The ZIP must describe the
	// earlier copy, including its receipt set and zero pending photo count.
	if _, err := s.Archive(ctx, 123, []Profile{{"userId": 100, "name": "Позже", "photos": []string{"https://mini.jardulik.app/later.jpg"}}}, "later"); err != nil {
		t.Fatal(err)
	}
	archiveTestReceipt(t, s, strings.Repeat("e", 64), 2)
	s.mediaMu.Lock()
	path, m, err := a.serviceZIPFromSnapshot(ctx, 1, snapshot)
	s.mediaMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Receipts) != 1 || m.Receipts[0].SourceSHA != oldReceipt {
		t.Fatal("receipt inventory read from live database")
	}
	if m.Coverage["pending_media"] != int64(0) || m.Coverage["media_file_count"] != 1 {
		t.Fatalf("live inventory leaked into snapshot coverage: %#v", m.Coverage)
	}
	z, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	found := false
	for _, f := range z.File {
		if f.Name == "media/"+sha {
			r, e := f.Open()
			if e != nil {
				t.Fatal(e)
			}
			b, e := io.ReadAll(r)
			r.Close()
			if e != nil || !bytes.Equal(b, photo) {
				t.Fatal("snapshot media missing or changed")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("snapshot's original photo was omitted")
	}
}

func TestServiceZIPBlocksDeletionUntilFilesAreCopied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	_, sha, photo := archiveTestMedia(t, s, c, 99)
	// Hold the sole SQL connection so serviceZIP pauses inside Snapshot after
	// taking mediaMu. This gives deletion a deterministic opportunity to race.
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waits := s.DB.Stats().WaitCount
	type result struct {
		path     string
		manifest archiveManifest
		err      error
	}
	packed := make(chan result, 1)
	go func() { path, m, e := a.serviceZIP(ctx, 1); packed <- result{path, m, e} }()
	for s.DB.Stats().WaitCount == waits {
		if ctx.Err() != nil {
			t.Fatal("snapshot did not wait for SQL connection")
		}
		runtime.Gosched()
	}
	if s.mediaMu.TryLock() {
		s.mediaMu.Unlock()
		t.Fatal("snapshot is not protecting referenced media from deletion")
	}
	deleted := make(chan error, 1)
	go func() { deleted <- s.DeleteUser(ctx, 123) }()
	mu := s.SessionMutex(123)
	for {
		if !mu.TryLock() {
			break
		}
		mu.Unlock()
		if ctx.Err() != nil {
			t.Fatal("delete did not start")
		}
		runtime.Gosched()
	}
	select {
	case e := <-deleted:
		t.Fatalf("delete passed snapshot media lock: %v", e)
	default:
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	var got result
	select {
	case got = <-packed:
	case <-ctx.Done():
		t.Fatal("snapshot did not finish")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	select {
	case err = <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("deletion did not finish after snapshot")
	}
	if got.manifest.Coverage["source_complete"] != true {
		t.Fatal("complete protected snapshot lost coverage")
	}
	if _, err = os.Stat(filepath.Join(c.DataDir, "media", sha)); !os.IsNotExist(err) {
		t.Fatal("test did not perform the competing garbage collection")
	}
	z, err := zip.OpenReader(got.path)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	found := false
	for _, f := range z.File {
		if f.Name == "media/"+sha {
			r, e := f.Open()
			if e != nil {
				t.Fatal(e)
			}
			b, e := io.ReadAll(r)
			r.Close()
			if e != nil || !bytes.Equal(b, photo) {
				t.Fatal("concurrent deletion corrupted archived photo")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("archive claims complete but omits the deleted source photo")
	}
}

func TestDeletedVersionAuditDoesNotHideNewProfile(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Раньше"}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state) VALUES(?,123,99,'old','sent')", first[0]); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteUser(ctx, 123); err != nil {
		t.Fatal(err)
	}
	if err = s.User(ctx, 456, "test"); err != nil {
		t.Fatal(err)
	}
	second, err := s.Archive(ctx, 456, []Profile{{"userId": 100, "name": "Позже"}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if second[0] <= first[0] {
		t.Fatal("new version reused an ID retained in archive audit")
	}
	tg := NewTelegram(c)
	tg.Base = "https://telegram.test.invalid"
	var calls atomic.Int64
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Body != nil {
			if _, e := io.Copy(io.Discard, r.Body); e != nil {
				return nil, e
			}
			r.Body.Close()
		}
		body := `{"ok":true,"result":{"message_id":42,"document":{"file_id":"file","file_unique_id":"unique"}}}`
		if strings.HasSuffix(r.URL.Path, "/createForumTopic") {
			body = `{"ok":true,"result":{"message_thread_id":55}}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	a.TG = tg
	var clock atomic.Int64
	start := time.Now()
	a.now = func() time.Time { return start.Add(time.Duration(clock.Add(1)) * 5 * time.Second) }
	if err = a.processOne(ctx); err != nil {
		t.Fatal(err)
	}
	var state string
	if err = s.DB.QueryRowContext(ctx, "SELECT state FROM archive_versions WHERE version_id=? AND profile_id=100", second[0]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "sent" || calls.Load() == 0 {
		t.Fatal("new profile was suppressed by deleted version audit")
	}
	var oldProfile int64
	if err = s.DB.QueryRowContext(ctx, "SELECT profile_id FROM archive_versions WHERE version_id=?", first[0]).Scan(&oldProfile); err != nil || oldProfile != 99 {
		t.Fatal("old audit record was overwritten")
	}
}

func TestCompressibleSnapshotOverRestoreBudgetIsNotPublished(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	var documents atomic.Int64
	tg := NewTelegram(c)
	tg.Base = "https://telegram.test.invalid"
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/sendDocument") {
			documents.Add(1)
		}
		if r.Body != nil {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return nil, err
			}
			r.Body.Close()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":42}}`)), Request: r}, nil
	})}
	a := NewArchiver(c, s, tg, nil)
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, "CREATE TABLE compressible_source(payload BLOB)"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, "INSERT INTO compressible_source(payload) VALUES(zeroblob(?))", serviceRestoreLimit+4096); err != nil {
		t.Fatal(err)
	}
	if err := a.processOne(ctx); err != nil {
		t.Fatal(err)
	}
	var state, path string
	if err := s.DB.QueryRowContext(ctx, "SELECT state,path FROM archive_backups ORDER BY id DESC LIMIT 1").Scan(&state, &path); err != nil {
		t.Fatal(err)
	}
	if state != "oversize" || documents.Load() != 0 {
		t.Fatalf("unrestorable archive published: state=%s documents=%d", state, documents.Load())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal("whole source archive not preserved", err)
	}
	if info.Size() >= serviceBackupLimit {
		t.Fatal("fixture did not exercise a small compressed archive")
	}
	if err = preflightServiceArchive(path); !errors.Is(err, errArchiveBudget) {
		t.Fatalf("creation preflight: %v", err)
	}
	z, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	dest := filepath.Join(t.TempDir(), "restore")
	if err = restoreServiceZIP(&z.Reader, dest); !errors.Is(err, errArchiveBudget) {
		t.Fatalf("restore preflight disagrees: %v", err)
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("over-budget archive wrote restore destination")
	}
}

func TestUncertainTopicCreationIsNotRetried(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	var count atomic.Int32
	tg := NewTelegram(c)
	tg.Base = "https://telegram.test.invalid"
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		count.Add(1)
		return nil, errors.New("connection lost after request")
	})}
	a := NewArchiver(c, s, tg, nil)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	p := StoredProfile{ID: 99, VersionID: 1, Profile: Profile{"name": "Пример"}}
	if _, err := a.topic(ctx, p); err == nil {
		t.Fatal("network ambiguity accepted")
	}
	if _, err := a.topic(ctx, p); err == nil {
		t.Fatal("uncertain creation automatically retried")
	}
	if count.Load() != 1 {
		t.Fatalf("topic request replayed %d times", count.Load())
	}
	var state string
	if err := s.DB.QueryRowContext(ctx, "SELECT state FROM archive_topic_jobs WHERE profile_id=99").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "uncertain" {
		t.Fatalf("state %s", state)
	}
}

func TestDocumentWithoutFileIDDoesNotBecomeSentBackup(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	tg := NewTelegram(c)
	tg.Base = "https://telegram.test.invalid"
	var count atomic.Int32
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		count.Add(1)
		if r.Body != nil {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return nil, err
			}
			r.Body.Close()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":10}}`)), Request: r}, nil
	})}
	a := NewArchiver(c, s, tg, nil)
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.processOne(ctx); err == nil {
		t.Fatal("missing document receipt accepted")
	}
	var state string
	if err := s.DB.QueryRowContext(ctx, "SELECT state FROM archive_backups ORDER BY id DESC LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "uncertain" {
		t.Fatalf("unconfirmed backup marked %s", state)
	}
	var receipts int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM archive_receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatal("false sent receipt recorded")
	}
	if count.Load() != 1 {
		t.Fatalf("expected one actual mocked upload, got %d", count.Load())
	}
}

func TestGroupRateSlotPersistsAcrossArchiverInstances(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.groupSlot(ctx); err != nil {
		t.Fatal(err)
	}
	b := NewArchiver(c, s, nil, nil)
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := b.groupSlot(short); err == nil {
		t.Fatal("new archiver bypassed group-wide slot")
	}
}

type archiveTelegramCall struct {
	method string
	fields map[string]string
	json   map[string]any
}

func archiveFixtureTelegram(c Config, respond func(archiveTelegramCall) (string, error)) *Telegram {
	tg := NewTelegram(c)
	tg.Base = "https://telegram.test.invalid"
	tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		call := archiveTelegramCall{method: filepath.Base(r.URL.Path), fields: map[string]string{}}
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
			reader, err := r.MultipartReader()
			if err != nil {
				return nil, err
			}
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				data, err := io.ReadAll(part)
				if err != nil {
					return nil, err
				}
				call.fields[part.FormName()] = string(data)
			}
		} else if err := json.NewDecoder(r.Body).Decode(&call.json); err != nil {
			return nil, err
		}
		_ = r.Body.Close()
		body, err := respond(call)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	return tg
}

func archivePresentationFixture(t *testing.T) (Config, *Store, *Archiver, StoredProfile, []MediaJob) {
	t.Helper()
	c, s := archiveTestStore(t)
	ctx := context.Background()
	versions, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Пример", "description": "Полное описание", "photos": []string{"https://mini.jardulik.app/one.jpg", "https://mini.jardulik.app/two.jpg"}, "miniPhotos": []string{"https://mini.jardulik.app/small.jpg"}}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.MediaJobs(ctx, 123, versions[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		data := []byte("original:" + j.URL)
		sha := digest(data)
		path := filepath.Join(c.DataDir, "media", sha)
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err = s.MediaDone(ctx, j, sha, path, "image/jpeg", int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err = s.MediaJobs(ctx, 123, versions[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Version(ctx, 123, versions[0])
	if err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	if err = a.init(ctx); err != nil {
		t.Fatal(err)
	}
	var tick atomic.Int64
	start := time.Now()
	a.now = func() time.Time { return start.Add(time.Duration(tick.Add(1)) * 5 * time.Second) }
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO mirror_topics(profile_id,thread_id) VALUES(?,55)", p.ID); err != nil {
		t.Fatal(err)
	}
	return c, s, a, p, jobs
}

func TestArchiveProfileUsesOneCaptionedAlbumAfterWholeZIP(t *testing.T) {
	c, s, a, p, jobs := archivePresentationFixture(t)
	var calls []archiveTelegramCall
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		calls = append(calls, call)
		switch call.method {
		case "sendDocument":
			if call.fields["message_thread_id"] != "55" {
				t.Fatal("source ZIP was posted outside its profile topic")
			}
			if !strings.Contains(call.fields["caption"], "SHA-256") || !strings.Contains(call.fields["caption"], "Устройство:") {
				t.Fatal("source provenance missing")
			}
			raw := []byte(call.fields["document"])
			zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
			if err != nil {
				t.Fatal(err)
			}
			found := map[string]bool{}
			var manifest archiveManifest
			for _, file := range zr.File {
				found[file.Name] = true
				if file.Name == "manifest.json" {
					r, e := file.Open()
					if e != nil {
						t.Fatal(e)
					}
					if e = json.NewDecoder(r).Decode(&manifest); e != nil {
						t.Fatal(e)
					}
					r.Close()
				}
			}
			if len(manifest.Media) != len(jobs) {
				t.Fatal("source role/position inventory lost")
			}
			for _, j := range jobs {
				if !found["media/"+j.SHA] {
					t.Fatalf("original %s omitted from ZIP", j.Role)
				}
			}
			return fmt.Sprintf(`{"ok":true,"result":{"message_id":100,"document":{"file_id":"whole-file","file_unique_id":"whole-unique","file_size":%d}}}`, len(raw)), nil
		case "sendMediaGroup":
			if call.fields["message_thread_id"] != "55" {
				t.Fatal("album topic changed")
			}
			var media []struct {
				Caption string `json:"caption"`
			}
			if err := json.Unmarshal([]byte(call.fields["media"]), &media); err != nil {
				t.Fatal(err)
			}
			if len(media) != 2 || !strings.Contains(media[0].Caption, "Полное описание") || media[1].Caption != "" {
				t.Fatalf("album caption/media incorrect: %#v", media)
			}
			return `{"ok":true,"result":[{"message_id":101,"chat":{"id":-100123}},{"message_id":102,"chat":{"id":-100123}}]}`, nil
		case "editMessageReplyMarkup":
			if call.json["message_id"] != float64(101) {
				t.Fatal("buttons attached to wrong album item")
			}
			return `{"ok":true,"result":{"message_id":101}}`, nil
		default:
			t.Fatalf("unexpected profile post %s", call.method)
		}
		return "", nil
	})
	if err := a.mirrorVersion(context.Background(), 123, p.VersionID); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 {
		t.Fatalf("profile was split into extra posts: %d", len(calls))
	}
	var ids string
	if err := s.DB.QueryRow("SELECT message_ids FROM archive_versions WHERE version_id=?", p.VersionID).Scan(&ids); err != nil || ids != "[101,102]" {
		t.Fatalf("profile card IDs include the separate whole ZIP: %s, %v", ids, err)
	}
	var thread, message int64
	if err := s.DB.QueryRow("SELECT thread_id,message_id FROM archive_receipts WHERE file_id='whole-file'").Scan(&thread, &message); err != nil || thread != 55 || message != 100 {
		t.Fatalf("whole source receipt incorrect: %d/%d %v", thread, message, err)
	}
}

func archiveLegacyPresentation(t *testing.T, s *Store, a *Archiver, p StoredProfile, jobs []MediaJob) string {
	t.Helper()
	fp, err := mirrorFingerprint(p.Profile, jobs)
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("e", 64)
	if _, err = s.DB.Exec("INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha) VALUES(?,123,?,?,'sent',55,'[11,12,13,14,15]',?)", p.VersionID, p.ID, fp, sha); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec("INSERT INTO mirror_posts(fingerprint,profile_id,thread_id,message_ids) VALUES(?,?,55,'[11,12,13,14,15]')", fp, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size) VALUES(?,'profile_version','source.zip',42,?,55,11,'whole-file','unique',42)`, sha, a.Config.BackupChatID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`INSERT INTO backup_files(source_sha,kind,filename,size,chat_id,thread_id,message_id,file_id,encrypted_sha) VALUES(?,'profile_version','source.zip',42,?,55,11,'whole-file','')`, sha, a.Config.BackupChatID); err != nil {
		t.Fatal(err)
	}
	return sha
}

func TestRepairProfilePostsRetainsZIPBeforeReplacingAndDeletingLegacy(t *testing.T) {
	c, s, a, p, jobs := archivePresentationFixture(t)
	sha := archiveLegacyPresentation(t, s, a, p, jobs)
	var methods []string
	var deleted []int64
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		methods = append(methods, call.method)
		switch call.method {
		case "editMessageCaption":
			if call.json["message_id"] != float64(11) {
				t.Fatal("edited an unrelated source")
			}
			return `{"ok":true,"result":{"message_id":11}}`, nil
		case "sendMediaGroup":
			var thread, msg int64
			var file string
			if err := s.DB.QueryRow("SELECT thread_id,message_id,file_id FROM archive_receipts WHERE source_sha=?", sha).Scan(&thread, &msg, &file); err != nil || thread != 55 || msg != 11 || file != "whole-file" {
				t.Fatal("original ZIP was moved or its receipt changed")
			}
			return `{"ok":true,"result":[{"message_id":101,"chat":{"id":-100123}},{"message_id":102,"chat":{"id":-100123}}]}`, nil
		case "editMessageReplyMarkup":
			return `{"ok":true,"result":{"message_id":101}}`, nil
		case "deleteMessage":
			if len(methods) < 4 {
				t.Fatal("deleted before confirming replacement")
			}
			deleted = append(deleted, int64(call.json["message_id"].(float64)))
			return `{"ok":true,"result":true}`, nil
		default:
			t.Fatalf("unexpected repair operation %s", call.method)
		}
		return "", nil
	})
	if err := a.RepairProfilePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(deleted) != "[12 13 14 15]" || methods[0] != "editMessageCaption" {
		t.Fatalf("unexpected delete/edit sequence: %v %v", methods, deleted)
	}
	var state, ids string
	if err := s.DB.QueryRow("SELECT state,new_message_ids FROM archive_profile_repairs WHERE version_id=?", p.VersionID).Scan(&state, &ids); err != nil || state != "sent" || ids != "[101,102]" {
		t.Fatalf("repair receipt missing: %s %s %v", state, ids, err)
	}
	previous := len(methods)
	if err := a.RepairProfilePosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(methods) != previous {
		t.Fatal("successful repair replayed")
	}
}

func TestRepairProfilePostsUnknownOutcomeNeverDeletesOrReplays(t *testing.T) {
	for _, uncertainAt := range []string{"editMessageCaption", "sendMediaGroup", "editMessageReplyMarkup", "deleteMessage"} {
		t.Run(uncertainAt, func(t *testing.T) {
			c, s, a, p, jobs := archivePresentationFixture(t)
			archiveLegacyPresentation(t, s, a, p, jobs)
			var methods []string
			a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
				methods = append(methods, call.method)
				if call.method == uncertainAt {
					return "", errors.New("connection interrupted after request")
				}
				switch call.method {
				case "editMessageCaption":
					return `{"ok":true,"result":{"message_id":11}}`, nil
				case "sendMediaGroup":
					return `{"ok":true,"result":[{"message_id":101,"chat":{"id":-100123}},{"message_id":102,"chat":{"id":-100123}}]}`, nil
				case "editMessageReplyMarkup":
					return `{"ok":true,"result":{"message_id":101}}`, nil
				case "deleteMessage":
					t.Fatal("delete continued after an unknown outcome")
				default:
					t.Fatalf("unexpected operation %s", call.method)
				}
				return "", nil
			})
			if err := a.RepairProfilePosts(context.Background()); err == nil {
				t.Fatal("unknown repair reported successful")
			}
			firstCount := len(methods)
			if err := a.RepairProfilePosts(context.Background()); err == nil {
				t.Fatal("uncertain repair was not reported")
			}
			if len(methods) != firstCount {
				t.Fatal("unknown external operation replayed")
			}
			if uncertainAt != "deleteMessage" {
				for _, method := range methods {
					if method == "deleteMessage" {
						t.Fatal("old presentation deleted before complete replacement")
					}
				}
			}
			var state string
			if err := s.DB.QueryRow("SELECT state FROM archive_profile_repairs WHERE version_id=?", p.VersionID).Scan(&state); err != nil || state != "uncertain" {
				t.Fatalf("durable uncertain state missing: %s %v", state, err)
			}
		})
	}
}

func TestRepairProfilePostsRespectsWorkerLock(t *testing.T) {
	c, _, a, _, _ := archivePresentationFixture(t)
	lock, err := os.OpenFile(filepath.Join(c.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = a.RepairProfilePosts(context.Background()); err == nil || !strings.Contains(err.Error(), "worker is active") {
		t.Fatalf("active worker was not rejected: %v", err)
	}
}

func TestArchiveCardMoreThanTenKeepsEverySelectedPhoto(t *testing.T) {
	c, _, a, p, jobs := archivePresentationFixture(t)
	selected := make([]MediaJob, 12)
	for i := range selected {
		selected[i] = jobs[0]
		selected[i].Role = "photos"
		selected[i].Position = i
	}
	groups := 0
	var persisted []int64
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		if call.method == "editMessageReplyMarkup" {
			return `{"ok":true,"result":{"message_id":100}}`, nil
		}
		if call.method != "sendMediaGroup" {
			t.Fatalf("unexpected send %s", call.method)
		}
		var media []map[string]any
		if err := json.Unmarshal([]byte(call.fields["media"]), &media); err != nil {
			t.Fatal(err)
		}
		want := 10
		if groups == 1 {
			want = 2
		}
		if len(media) != want {
			t.Fatalf("album omitted photos: %d", len(media))
		}
		for i, m := range media {
			_, caption := m["caption"]
			if caption != (groups == 0 && i == 0) {
				t.Fatal("caption repeated or missing")
			}
		}
		messages := make([]map[string]any, len(media))
		for i := range messages {
			messages[i] = map[string]any{"message_id": int64(100 + groups*10 + i), "chat": map[string]int64{"id": c.BackupChatID}}
		}
		groups++
		encoded, _ := json.Marshal(messages)
		return `{"ok":true,"result":` + string(encoded) + `}`, nil
	})
	ids, err := a.sendProfileCard(context.Background(), p, selected, 55, func(ids []int64) error { persisted = append([]int64(nil), ids...); return nil })
	if err != nil || len(ids) != 12 || len(persisted) != 12 || groups != 2 {
		t.Fatalf("not all photos delivered: %v %v groups=%s", ids, err, strconv.Itoa(groups))
	}
}

func TestArchiveRejectedPhotoAlbumNeverFallsBackToDocuments(t *testing.T) {
	c, _, a, p, jobs := archivePresentationFixture(t)
	calls := 0
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		calls++
		if call.method != "sendMediaGroup" {
			t.Fatalf("photo was sent as an extra document: %s", call.method)
		}
		return `{"ok":false,"error_code":400,"description":"photo rejected"}`, nil
	})
	ids, err := a.sendProfileCard(context.Background(), p, jobs, 55, func([]int64) error { return nil })
	if err == nil || calls != 1 || len(ids) != 0 {
		t.Fatalf("rejected album retried/fell back: %v %d %v", err, calls, ids)
	}
}

func TestArchiveOversizedPhotoFailsBeforeAnyCardIsSent(t *testing.T) {
	c, _, a, p, jobs := archivePresentationFixture(t)
	path, err := a.mediaSourcePath(jobs[0].SHA)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(10 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		t.Fatalf("known oversized photo started a card: %s", call.method)
		return "", nil
	})
	if _, err = a.sendProfileCard(context.Background(), p, jobs, 55, func([]int64) error { return nil }); err == nil {
		t.Fatal("oversized photo accepted")
	}
}

func TestQueuedProfileRepairUsesWorkerLockAndDoesNotReplay(t *testing.T) {
	c, s, a, p, jobs := archivePresentationFixture(t)
	archiveLegacyPresentation(t, s, a, p, jobs)
	calls := 0
	a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
		calls++
		switch call.method {
		case "editMessageCaption":
			return `{"ok":true,"result":{"message_id":11}}`, nil
		case "sendMediaGroup":
			return `{"ok":true,"result":[{"message_id":101,"chat":{"id":-100123}},{"message_id":102,"chat":{"id":-100123}}]}`, nil
		case "editMessageReplyMarkup":
			return `{"ok":true,"result":{"message_id":101}}`, nil
		case "deleteMessage":
			return `{"ok":true,"result":true}`, nil
		default:
			t.Fatalf("unexpected queued maintenance call %s", call.method)
		}
		return "", nil
	})
	ctx := context.Background()
	if err := a.RequestProfileRepair(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestProfileRepair(ctx); err != nil {
		t.Fatalf("pending queue was not idempotent: %v", err)
	}
	// Run owns this lock already. The queued path must not acquire it a second time.
	lock, err := os.OpenFile(filepath.Join(c.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	a.mu.Lock()
	err = a.processOne(ctx)
	a.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if state := s.Meta(ctx, "archive_repair_requested"); state != "done" {
		t.Fatalf("repair queue did not finish: %s", state)
	}
	firstCalls := calls
	a.mu.Lock()
	err = a.processOne(ctx)
	a.mu.Unlock()
	if err != nil || calls != firstCalls {
		t.Fatalf("completed repair replayed: %d -> %d, %v", firstCalls, calls, err)
	}
}

func TestQueuedProfileRepairUnknownAndCancelledRequestsCannotReset(t *testing.T) {
	for _, cancelAtCall := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelAtCall), func(t *testing.T) {
			c, s, a, p, jobs := archivePresentationFixture(t)
			archiveLegacyPresentation(t, s, a, p, jobs)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			a.TG = archiveFixtureTelegram(c, func(call archiveTelegramCall) (string, error) {
				calls++
				if cancelAtCall {
					cancel()
				}
				return "", errors.New("Telegram response outcome unknown")
			})
			if err := a.RequestProfileRepair(ctx); err != nil {
				t.Fatal(err)
			}
			a.mu.Lock()
			err := a.processOne(ctx)
			a.mu.Unlock()
			if err == nil {
				t.Fatal("unknown request succeeded")
			}
			if state := s.Meta(context.Background(), "archive_repair_requested"); state != "uncertain" {
				t.Fatalf("request uncertainty not retained after cancellation: %s", state)
			}
			if err = a.RequestProfileRepair(context.Background()); err == nil {
				t.Fatal("request reset uncertain maintenance")
			}
			// A normal worker iteration ignores the unresolved maintenance request.
			a.mu.Lock()
			err = a.processOne(context.Background())
			a.mu.Unlock()
			if err != nil || calls != 1 {
				t.Fatalf("uncertain repair repeated: %v calls=%d", err, calls)
			}
		})
	}
}
