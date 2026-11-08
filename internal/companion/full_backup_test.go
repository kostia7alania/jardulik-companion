package companion

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fullTestMedia(t *testing.T, c Config, s *Store, version int64, body []byte, mime string) string {
	t.Helper()
	ctx := context.Background()
	jobs, err := s.MediaJobs(ctx, 123, version)
	if err != nil {
		t.Fatal(err)
	}
	sha := digest(body)
	path := filepath.Join(c.DataDir, "media", sha)
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		if err = s.MediaDone(ctx, j, sha, path, mime, int64(len(body))); err != nil {
			t.Fatal(err)
		}
	}
	return sha
}
func fullTestOpen(t *testing.T, path string, key []byte) *zip.ReadCloser {
	t.Helper()
	plain := filepath.Join(t.TempDir(), "plain.zip")
	if err := openFullBackupFile(path, plain, key); err != nil {
		t.Fatal(err)
	}
	z, err := zip.OpenReader(plain)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { z.Close() })
	return z
}
func fullTestZipBytes(t *testing.T, z *zip.Reader) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, f := range z.File {
		r, e := f.Open()
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(r)
		r.Close()
		if e != nil {
			t.Fatal(e)
		}
		out[f.Name] = b
	}
	return out
}
func fullTestReceipt(t *testing.T, a *Archiver, artifact FullBackupArtifact) {
	t.Helper()
	ctx := context.Background()
	if err := a.initFullBackup(ctx); err != nil {
		t.Fatal(err)
	}
	randomID, err := newBackupRandomID()
	if err != nil {
		t.Fatal(err)
	}
	r := FullBackupReceipt{Status: "sent", Transport: "mtproto", ChatID: a.Config.BackupChatID, ThreadID: a.Config.BackupThreadID, MessageID: 123, DocumentID: 456, ByteSize: artifact.ByteSize, SourceSHA: artifact.EncryptedSHA, RandomID: randomID}
	raw, _ := json.Marshal(r)
	_, err = a.Store.DB.ExecContext(ctx, `INSERT INTO full_backup_jobs(requested_at,if_changed,state,source_fingerprint,artifact_path,source_sha,encrypted_sha,byte_size,random_id,receipt_json) VALUES('fixture',0,'sent',?,?,?,?,?,?,?)`, artifact.SourceFingerprint, artifact.Path, artifact.SourceSHA, artifact.EncryptedSHA, artifact.ByteSize, randomID, string(raw))
	if err != nil {
		t.Fatal(err)
	}
}

func TestFullBackupRoundTripKeepsVersionsObserversAndOriginals(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "fixture-access", Refresh: "fixture-refresh", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	first := Profile{"userId": 99, "name": "Пример", "description": "Первая версия", "fireText": "контекст первого наблюдателя", "photos": []string{"https://mini.jardulik.app/one.jpg"}}
	ids, err := s.Archive(ctx, 123, []Profile{first}, "discovery")
	if err != nil {
		t.Fatal(err)
	}
	one := fullTestMedia(t, c, s, ids[0], []byte("original-jpeg-bytes"), "image/jpeg")
	second := Profile{"userId": 99, "name": "Пример", "description": "Вторая версия", "photos": []string{"https://mini.jardulik.app/two.png"}}
	ids, err = s.Archive(ctx, 123, []Profile{second}, "watch")
	if err != nil {
		t.Fatal(err)
	}
	two := fullTestMedia(t, c, s, ids[0], []byte("original-png-bytes"), "image/png")
	if err = s.User(ctx, 124, "other"); err != nil {
		t.Fatal(err)
	}
	other := Profile{"userId": 99, "name": "Пример", "description": "Версия другого наблюдателя", "fireText": "частный контекст второго"}
	if _, err = s.Archive(ctx, 124, []Profile{other}, "other"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Archive(ctx, 123, []Profile{{"userId": 100, "name": "Без фото"}}, "test"); err != nil {
		t.Fatal(err)
	}
	orphan := []byte("unreferenced-but-stored-original")
	orphanSHA := digest(orphan)
	if err = os.WriteFile(filepath.Join(c.DataDir, "media", orphanSHA), orphan, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO media(sha,size,mime,path) VALUES(?,?,?,?)", orphanSHA, len(orphan), "image/webp", "media/"+orphanSHA); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	artifact, err := a.BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.UploadEnabled || artifact.Profiles != 2 || artifact.Versions != 4 || artifact.MediaFiles != 3 || artifact.SourceFingerprint == "" {
		t.Fatalf("unexpected artifact: %+v", artifact)
	}
	info, err := os.Stat(artifact.Path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("artifact permissions: %v", err)
	}
	z := fullTestOpen(t, artifact.Path, c.BackupKey)
	files := fullTestZipBytes(t, &z.Reader)
	var global fullManifest
	if err = json.Unmarshal(files["manifest.json"], &global); err != nil {
		t.Fatal(err)
	}
	if len(global.People) != 2 || global.Coverage["media_embedded"] != true {
		t.Fatal("person inventory or embedded media absent")
	}
	personBytes := files["profiles/jardulik-99.zip"]
	pz, err := zip.NewReader(bytes.NewReader(personBytes), int64(len(personBytes)))
	if err != nil {
		t.Fatal(err)
	}
	pf := fullTestZipBytes(t, pz)
	var pm personManifest
	if err = json.Unmarshal(pf["manifest.json"], &pm); err != nil {
		t.Fatal(err)
	}
	if len(pm.Versions) != 3 || pf["media/"+one+".jpg"] == nil || pf["media/"+two+".png"] == nil {
		t.Fatal("all versions or source photo extensions missing")
	}
	owners := map[int64]int{}
	for _, v := range pm.Versions {
		owners[v.ObserverID]++
		var raw string
		if err = s.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=? AND owner_id=?", v.VersionID, v.ObserverID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if string(pf[v.RawFile]) != raw {
			t.Fatal("source JSON changed or observer data mixed")
		}
	}
	if owners[123] != 2 || owners[124] != 1 {
		t.Fatal("observer provenance collapsed")
	}
	if !strings.Contains(string(files["index.txt"]), "#99 | Пример") || !strings.Contains(string(files["index.txt"]), "#100 | Без фото") {
		t.Fatal("line index missing people")
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err = RestoreArchive(artifact.Path, dest, c.BackupKey); err != nil {
		t.Fatal(err)
	}
	for sha, want := range map[string][]byte{one: []byte("original-jpeg-bytes"), two: []byte("original-png-bytes"), orphanSHA: orphan} {
		got, e := os.ReadFile(filepath.Join(dest, "media", sha))
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("restored original differs: %v", e)
		}
	}
	rc := c
	rc.DataDir = dest
	rs, err := OpenStore(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	session, err := rs.Session(ctx, 123)
	if err != nil || session.Token != "fixture-access" {
		t.Fatalf("session recovery failed: %v", err)
	}
	if err = RestoreArchive(artifact.Path, dest, c.BackupKey); err == nil {
		t.Fatal("nonempty restore destination replaced")
	}
}

func TestFullBackupIfChangedRequiresConfirmedReceipt(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	p := Profile{"userId": 99, "name": "Пример", "description": "один", "distance": 1}
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "initial-token", Refresh: "initial-refresh"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Archive(ctx, 123, []Profile{p}, "test"); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	first, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Unchanged || retry.Unchanged || first.Path != retry.Path || !retry.Reused {
		t.Fatal("building an unsent artifact incorrectly marked sources clean")
	}
	fullTestReceipt(t, a, first)
	if err = s.SaveSession(ctx, 123, Session{UserID: 500, Token: "rotated-token", Refresh: "rotated-refresh"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, "UPDATE versions SET last_seen='2099-01-01 00:00:00'"); err != nil {
		t.Fatal(err)
	}
	if err = s.SetMeta(ctx, "archive_topic_title:99", "renamed"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO observations(owner_id,version_id,source) SELECT owner_id,id,'unchanged_daily_check' FROM versions"); err != nil {
		t.Fatal(err)
	}
	p["distance"] = 999
	p["reactionView"] = true
	p["fireText"] = "viewer-only"
	if _, err = s.Archive(ctx, 123, []Profile{p}, "viewer_context"); err != nil {
		t.Fatal(err)
	}
	unchanged, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if !unchanged.Unchanged || unchanged.Path != "" {
		t.Fatal("bookkeeping/viewer-only changes caused a daily backup")
	}
	p["description"] = "два"
	if _, err = s.Archive(ctx, 123, []Profile{p}, "changed"); err != nil {
		t.Fatal(err)
	}
	changed, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Unchanged || changed.SourceFingerprint == first.SourceFingerprint {
		t.Fatal("source version change missed")
	}
	fullTestReceipt(t, a, changed)
	if _, err = s.DB.ExecContext(ctx, `CREATE TABLE profile_presence_events(owner_id INTEGER,profile_id INTEGER,transition INTEGER,availability TEXT,observed_at TEXT,state TEXT,updated_at TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, `INSERT INTO profile_presence_events VALUES(123,99,1,'unavailable','2026-11-08T00:00:00Z','pending','now')`); err != nil {
		t.Fatal(err)
	}
	presence, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if presence.Unchanged {
		t.Fatal("availability transition not backed up")
	}
	fullTestReceipt(t, a, presence)
	if _, err = s.DB.ExecContext(ctx, "UPDATE profile_presence_events SET state='sent',updated_at='later'"); err != nil {
		t.Fatal(err)
	}
	final, err := a.BuildFullBackup(ctx, true)
	if err != nil || !final.Unchanged {
		t.Fatalf("presence delivery bookkeeping dirtied backup: %v", err)
	}
}

func TestFullBackupBecomesDirtyWhenOriginalPhotoArrives(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	ids, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Фото позже", "photos": []string{"https://mini.jardulik.app/photo.jpg"}}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	first, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	fullTestReceipt(t, a, first)
	fullTestMedia(t, c, s, ids[0], []byte("arrived-whole-photo"), "image/jpeg")
	next, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if next.Unchanged || next.SourceFingerprint == first.SourceFingerprint || next.MediaFiles != 1 {
		t.Fatal("completed source photo did not dirty backup")
	}
}

func TestFullBackupInventoryAndFingerprintUseSameSnapshot(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	if _, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "До"}}, "before"); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "source.sqlite")
	if err := s.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fp, err := fullSourceFingerprint(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Archive(ctx, 123, []Profile{{"userId": 100, "name": "После"}}, "after"); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	if err = os.MkdirAll(filepath.Join(c.DataDir, "backups"), 0700); err != nil {
		t.Fatal(err)
	}
	path, m, err := a.fullZIPFromSnapshot(ctx, db, snapshot, t.TempDir(), fp)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	if len(m.People) != 1 || m.People[0].ProfileID != 99 || m.SourceFingerprint != fp {
		t.Fatal("live source state leaked into captured bundle")
	}
}

func TestFullRestoreRejectsWrongObserverBeforePublishing(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	if _, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Тест"}}, "test"); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	artifact, err := a.BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	z := fullTestOpen(t, artifact.Path, c.BackupKey)
	files := fullTestZipBytes(t, &z.Reader)
	var global fullManifest
	if err = json.Unmarshal(files["manifest.json"], &global); err != nil {
		t.Fatal(err)
	}
	name := global.People[0].Filename
	pz, err := zip.NewReader(bytes.NewReader(files[name]), int64(len(files[name])))
	if err != nil {
		t.Fatal(err)
	}
	personFiles := fullTestZipBytes(t, pz)
	var pm personManifest
	if err = json.Unmarshal(personFiles["manifest.json"], &pm); err != nil {
		t.Fatal(err)
	}
	pm.Versions[0].ObserverID = 124
	personFiles["manifest.json"], _ = json.Marshal(pm)
	pack := func(m map[string][]byte) []byte {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		for n, data := range m {
			f, e := w.Create(n)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = f.Write(data); e != nil {
				t.Fatal(e)
			}
		}
		if e := w.Close(); e != nil {
			t.Fatal(e)
		}
		return b.Bytes()
	}
	files[name] = pack(personFiles)
	for i := range global.Files {
		if global.Files[i].Name == name {
			global.Files[i].ByteSize = int64(len(files[name]))
			global.Files[i].SHA256 = digest(files[name])
		}
	}
	files["manifest.json"], _ = json.Marshal(global)
	modified := filepath.Join(t.TempDir(), "modified.zip")
	if err = os.WriteFile(modified, pack(files), 0600); err != nil {
		t.Fatal(err)
	}
	sealed := modified + ".enc"
	if err = sealFullBackupFile(modified, sealed, c.BackupKey); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "not-published")
	if err = RestoreArchive(sealed, dest, c.BackupKey); err == nil {
		t.Fatal("observer mixing accepted")
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("invalid source archive published destination")
	}
}

func TestFullBackupLargeRoundTripUsesBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("60 MB streaming acceptance fixture")
	}
	ctx := context.Background()
	c, s := archiveTestStore(t)
	profiles := make([]Profile, 150)
	for i := range profiles {
		n := 2
		if i < 88 {
			n++
		}
		photos := make([]string, n)
		for j := range photos {
			photos[j] = fmt.Sprintf("https://mini.jardulik.app/fixture-%d-%d.jpg", i, j)
		}
		profiles[i] = Profile{"userId": int64(1000 + i), "name": fmt.Sprintf("Synthetic %03d", i), "description": "Synthetic streaming backup fixture", "photos": photos}
	}
	versions, err := s.Archive(ctx, 123, profiles, "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(c.DataDir, "media"), 0700); err != nil {
		t.Fatal(err)
	}
	index := 0
	buf := make([]byte, 154562)
	for _, version := range versions {
		jobs, e := s.MediaJobs(ctx, 123, version)
		if e != nil {
			t.Fatal(e)
		}
		for _, j := range jobs {
			size := 154561
			if index < 354 {
				size++
			}
			rng := rand.New(rand.NewSource(int64(index + 1)))
			if _, e = rng.Read(buf[:size]); e != nil {
				t.Fatal(e)
			}
			sha := digest(buf[:size])
			path := filepath.Join(c.DataDir, "media", sha)
			if e = os.WriteFile(path, buf[:size], 0600); e != nil {
				t.Fatal(e)
			}
			if e = s.MediaDone(ctx, j, sha, path, "image/jpeg", int64(size)); e != nil {
				t.Fatal(e)
			}
			index++
		}
	}
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	peak.Store(base.Alloc)
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		timer := time.NewTicker(5 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for old := peak.Load(); m.Alloc > old; old = peak.Load() {
					if peak.CompareAndSwap(old, m.Alloc) {
						break
					}
				}
			}
		}
	}()
	a := NewArchiver(c, s, nil, nil)
	artifact, err := a.BuildFullBackup(ctx, false)
	if err == nil {
		err = RestoreArchive(artifact.Path, filepath.Join(t.TempDir(), "restore"), c.BackupKey)
	}
	close(done)
	<-sampled
	if err != nil {
		t.Fatal(err)
	}
	increase := peak.Load() - base.Alloc
	t.Logf("profiles=%d versions=%d media=%d source_bytes=%d encrypted_bytes=%d sampled_heap_growth=%d", artifact.Profiles, artifact.Versions, artifact.MediaFiles, artifact.MediaBytes, artifact.ByteSize, increase)
	if artifact.Profiles != 150 || artifact.MediaFiles != 388 || artifact.MediaBytes != 59970022 || artifact.ByteSize < 50_000_000 || !artifact.HostedUploadLimitExceeded {
		t.Fatal("fixture did not exercise real full-backup scale and hosted upload blocker")
	}
	if increase > 64<<20 {
		t.Fatalf("streaming full backup grew heap by %d bytes", increase)
	}
	// Optional local artifact retention supports manual review without using real accounts.
	if dir := os.Getenv("JARDULIK_FULL_BACKUP_ARTIFACT_DIR"); dir != "" {
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		from, e := os.Open(artifact.Path)
		if e != nil {
			t.Fatal(e)
		}
		defer from.Close()
		to, e := os.OpenFile(filepath.Join(dir, "synthetic-150-profiles-full.zip.enc"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = io.Copy(to, from)
		closeErr := to.Close()
		if e != nil {
			t.Fatal(e)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		artifact.Path = filepath.Join(dir, "synthetic-150-profiles-full.zip.enc")
		b, _ := json.MarshalIndent(artifact, "", "  ")
		if err = os.WriteFile(filepath.Join(dir, "artifact.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFullRestoreEnforcesOneBudgetAcrossPersonZIPs(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	for i := 0; i < 2; i++ {
		ids, err := s.Archive(ctx, 123, []Profile{{"userId": int64(99 + i), "name": "Budget fixture", "photos": []string{fmt.Sprintf("https://mini.jardulik.app/%d.jpg", i)}}}, "test")
		if err != nil {
			t.Fatal(err)
		}
		fullTestMedia(t, c, s, ids[0], bytes.Repeat([]byte{byte(i + 1)}, 100_000), "image/jpeg")
	}
	a := NewArchiver(c, s, nil, nil)
	artifact, err := a.BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	z := fullTestOpen(t, artifact.Path, c.BackupKey)
	var top, maxPerson int64
	for _, f := range z.File {
		top += int64(f.UncompressedSize64)
		if _, ok := fullProfileID(f.Name); !ok {
			continue
		}
		r, e := f.Open()
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(r)
		r.Close()
		if e != nil {
			t.Fatal(e)
		}
		p, e := zip.NewReader(bytes.NewReader(b), int64(len(b)))
		if e != nil {
			t.Fatal(e)
		}
		var expanded int64
		for _, entry := range p.File {
			expanded += int64(entry.UncompressedSize64)
		}
		if expanded > maxPerson {
			maxPerson = expanded
		}
	}
	// Each nested ZIP fits independently, but two exceed the common budget.
	dest := filepath.Join(t.TempDir(), "not-published")
	if err = restoreFullZIP(ctx, &z.Reader, dest, top+maxPerson); !errors.Is(err, errArchiveBudget) {
		t.Fatalf("nested expansion bypassed common budget: %v", err)
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("over-budget restore published destination")
	}
}

func fullTestFingerprint(t *testing.T, s *Store) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.sqlite")
	if err := s.Snapshot(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fp, err := fullSourceFingerprint(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

func TestFullBackupDirtyWhenCurrentHeadReturnsToAnArchivedVersion(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	aProfile := Profile{"userId": 99, "name": "Пример", "description": "A"}
	bProfile := Profile{"userId": 99, "name": "Пример", "description": "B"}
	aIDs, err := s.Archive(ctx, 123, []Profile{aProfile}, "A")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Archive(ctx, 123, []Profile{bProfile}, "B"); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	first, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	fullTestReceipt(t, a, first)
	if _, err = s.Archive(ctx, 123, []Profile{aProfile}, "A_again"); err != nil {
		t.Fatal(err)
	}
	var count int
	var current int64
	if err = s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM versions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRowContext(ctx, "SELECT version_id FROM queue WHERE owner_id=123 AND profile_id=99").Scan(&current); err != nil {
		t.Fatal(err)
	}
	if count != 2 || current != aIDs[0] {
		t.Fatal("fixture did not return the current head to existing A")
	}
	changed, err := a.BuildFullBackup(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Unchanged || changed.SourceFingerprint == first.SourceFingerprint {
		t.Fatal("A→B→A current state change skipped despite unchanged version set")
	}
}

func TestFullBackupFingerprintTracksDurableUserState(t *testing.T) {
	ctx := context.Background()
	type mutation func(*testing.T, *Store)
	exec := func(t *testing.T, s *Store, query string, args ...any) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	filters := func(t *testing.T, s *Store) { exec(t, s, filterSchema) }
	action := func(t *testing.T, s *Store, state, response string) {
		exec(t, s, "INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(123,99,'Like',?,?)", state, response)
	}
	cases := []struct {
		name          string
		setup, change mutation
		dirty         bool
	}{
		{name: "favorite added", change: func(t *testing.T, s *Store) {
			if err := s.Favorite(ctx, 123, 99); err != nil {
				t.Fatal(err)
			}
		}, dirty: true},
		{name: "favorite removed", setup: func(t *testing.T, s *Store) {
			if err := s.Favorite(ctx, 123, 99); err != nil {
				t.Fatal(err)
			}
		}, change: func(t *testing.T, s *Store) { exec(t, s, "DELETE FROM favorites WHERE owner_id=123 AND profile_id=99") }, dirty: true},
		{name: "reaction pending outcome", change: func(t *testing.T, s *Store) { action(t, s, "pending", "") }, dirty: true},
		{name: "reaction confirmed", change: func(t *testing.T, s *Store) { action(t, s, "succeeded", `{"Success":true,"Mutual":false}`) }, dirty: true},
		{name: "reaction uncertain", change: func(t *testing.T, s *Store) { action(t, s, "unknown", "unrecognized upstream response") }, dirty: true},
		{name: "reaction outcome resolved", setup: func(t *testing.T, s *Store) { action(t, s, "unknown", "") }, change: func(t *testing.T, s *Store) {
			exec(t, s, `UPDATE actions SET state='succeeded',response='{"Success":true}'`)
		}, dirty: true},
		{name: "failed reaction bookkeeping", change: func(t *testing.T, s *Store) { action(t, s, "failed", `{"Success":false}`) }},
		{name: "reaction JSON formatting", setup: func(t *testing.T, s *Store) { action(t, s, "unknown", `{"Success":true,"Mutual":false}`) }, change: func(t *testing.T, s *Store) {
			exec(t, s, `UPDATE actions SET response='{ "Mutual": false, "Success": true }'`)
		}},
		{name: "applied filters", setup: filters, change: func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO feed_filters(owner_id,applied_json) VALUES(123,'{"ageMin":25,"ageMax":35}')`)
		}, dirty: true},
		{name: "outside filters enabled", setup: filters, change: func(t *testing.T, s *Store) {
			exec(t, s, "INSERT INTO feed_filters(owner_id,include_others) VALUES(123,1)")
		}, dirty: true},
		{name: "draft filters and city candidates", setup: filters, change: func(t *testing.T, s *Store) {
			exec(t, s, `INSERT INTO feed_filters(owner_id,draft_json,others_id) VALUES(123,'{"id":"draft","values":{"ageMin":20},"cities":["candidate"]}','offer')`)
		}},
		{name: "pending filter queue", setup: filters, change: func(t *testing.T, s *Store) {
			exec(t, s, "INSERT INTO feed_filter_pending(owner_id,profile_id) VALUES(123,99)")
		}},
		{name: "filter formatting and draft changes", setup: func(t *testing.T, s *Store) {
			filters(t, s)
			exec(t, s, `INSERT INTO feed_filters(owner_id,applied_json) VALUES(123,'{"ageMin":25,"ageMax":35}')`)
		}, change: func(t *testing.T, s *Store) {
			exec(t, s, `UPDATE feed_filters SET applied_json='{ "ageMax":35, "ageMin":25 }',draft_json='{"id":"new-draft"}',others_id='new-offer'`)
		}},
		{name: "deleted transient account tombstone", change: func(t *testing.T, s *Store) {
			if err := s.User(ctx, 124, "temporary"); err != nil {
				t.Fatal(err)
			}
			if err := s.DeleteUser(ctx, 124); err != nil {
				t.Fatal(err)
			}
		}, dirty: true},
		{name: "user pause", change: func(t *testing.T, s *Store) { s.Pause(ctx, 123, true) }, dirty: true},
		{name: "connection requires reconnect", change: func(t *testing.T, s *Store) { s.SessionState(ctx, 123, "reconnect_required", "refresh_not_confirmed") }, dirty: true},
		{name: "disconnect", change: func(t *testing.T, s *Store) {
			if err := s.Disconnect(ctx, 123); err != nil {
				t.Fatal(err)
			}
		}, dirty: true},
		{name: "session token rotation", change: func(t *testing.T, s *Store) {
			if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "rotated", Refresh: "new-refresh", Expires: 9999999999}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "account counters", change: func(t *testing.T, s *Store) {
			if err := s.SaveAccount(ctx, 123, Profile{"userId": 500, "fire": 60, "clean": 60, "likes": 2, "mutuals": 1, "type": 1}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "same connection new error", change: func(t *testing.T, s *Store) { s.SessionState(ctx, 123, "connected", "different_error") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, s := archiveTestStore(t)
			if _, err := s.Archive(ctx, 123, []Profile{{"userId": 99, "name": "Пример"}}, "test"); err != nil {
				t.Fatal(err)
			}
			if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "initial", Refresh: "initial-refresh"}); err != nil {
				t.Fatal(err)
			}
			if tc.setup != nil {
				tc.setup(t, s)
			}
			before := fullTestFingerprint(t, s)
			tc.change(t, s)
			after := fullTestFingerprint(t, s)
			if (before != after) != tc.dirty {
				t.Fatalf("dirty=%v, want %v", before != after, tc.dirty)
			}
		})
	}
}

func TestFullBackupFingerprintTracksSemanticRelationshipHistoryAndHeads(t *testing.T) {
	ctx := context.Background()
	add := func(t *testing.T, s *Store, mutual bool, profiles []Profile) {
		t.Helper()
		raw, err := json.Marshal(profiles)
		if err != nil {
			t.Fatal(err)
		}
		inserted, err := s.DB.ExecContext(ctx, "INSERT INTO relation_batches(owner_id,mutual,source_page,raw_json) VALUES(123,?,1,?)", mutual, string(raw))
		if err != nil {
			t.Fatal(err)
		}
		batch, err := inserted.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range profiles {
			body, _ := json.Marshal(p)
			if _, err = s.DB.ExecContext(ctx, "INSERT INTO relation_profiles(batch_id,owner_id,profile_id,position,raw_json) VALUES(?,123,?,?,?)", batch, p.ID(), i, string(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	base := func() []Profile {
		return []Profile{{"userId": 501, "name": "Первый", "reactionView": false, "distance": 1, "isFire": true, "fireText": "Привет"}, {"userId": 502, "name": "Второй"}}
	}
	cases := []struct {
		name   string
		setupB bool
		change func(*testing.T, *Store)
		dirty  bool
	}{
		{name: "repeat GET different batch and timestamps", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["reactionView"] = true
			p[0]["distance"] = 999
			p[0]["observed_at"] = "later"
			p[0]["last_seen"] = "later"
			add(t, s, false, p)
			if _, err := s.DB.ExecContext(ctx, "UPDATE relation_batches SET captured_at='2099-01-01'"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "page reorder", change: func(t *testing.T, s *Store) { p := base(); add(t, s, false, []Profile{p[1], p[0]}) }},
		{name: "incoming membership", change: func(t *testing.T, s *Store) { add(t, s, false, base()[:1]) }, dirty: true},
		{name: "mutual membership", change: func(t *testing.T, s *Store) { add(t, s, true, base()) }, dirty: true},
		{name: "superlike message", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["fireText"] = "Новое сообщение"
			add(t, s, false, p)
		}, dirty: true},
		{name: "return to archived relation page", setupB: true, change: func(t *testing.T, s *Store) { add(t, s, false, base()) }, dirty: true},
		{name: "new historical payload then old head", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["fireText"] = "Между проверками"
			add(t, s, false, p)
			add(t, s, false, base())
		}, dirty: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, s := archiveTestStore(t)
			if err := s.initRelations(ctx); err != nil {
				t.Fatal(err)
			}
			add(t, s, false, base())
			if tc.setupB {
				p := base()
				p[0]["fireText"] = "Версия B"
				add(t, s, false, p)
			}
			before := fullTestFingerprint(t, s)
			tc.change(t, s)
			after := fullTestFingerprint(t, s)
			if (before != after) != tc.dirty {
				t.Fatalf("dirty=%v, want %v", before != after, tc.dirty)
			}
		})
	}
}
