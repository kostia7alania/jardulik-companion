package companion

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
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
