package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFullBackupRetentionOnlyPrunesVerifiedOldFiles(t *testing.T) {
	ctx := context.Background()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.initFullBackup(ctx); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(c.DataDir, "backups")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := func(i int) string {
		return fmt.Sprintf("20261124T030000Z__jardulik-full__1-profiles-%d.zip.enc", i)
	}
	exec := func(stmt string, args ...any) {
		t.Helper()
		if _, err := s.DB.ExecContext(ctx, stmt, args...); err != nil {
			t.Fatal(err)
		}
	}
	record := func(path, state string, companionID int64) string {
		t.Helper()
		sha, size, err := fileDigest(path)
		if err != nil {
			t.Fatal(err)
		}
		receipt := FullBackupReceipt{Status: "sent", Transport: "mtproto", ChatID: c.BackupChatID, ThreadID: c.BackupThreadID, MessageID: 12, DocumentID: 23, ByteSize: size, SourceSHA: sha, RandomID: 34}
		raw, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		exec(`INSERT INTO full_backup_artifacts(source_fingerprint,source_sha,encrypted_sha,path,byte_size)
 VALUES('fixture','source',?,?,?)`, sha, path, size)
		exec(`INSERT INTO full_backup_jobs(requested_at,if_changed,state,artifact_path,encrypted_sha,byte_size,random_id,receipt_json,companion_message_id)
 VALUES('fixture',0,?,?,?,?,34,?,?)`, state, path, sha, size, string(raw), companionID)
		return sha
	}
	create := func(path, content string) string {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	old := create(filepath.Join(dir, name(1)), "oldest archived backup")
	oldestSHA := record(old, "sent", 100)

	uncertain := create(filepath.Join(dir, name(2)), "ambiguous upload")
	record(uncertain, "sent", 101)
	exec("INSERT INTO full_backup_jobs(requested_at,if_changed,state,artifact_path) VALUES('fixture',0,'uncertain',?)", uncertain)

	failed := create(filepath.Join(dir, name(3)), "unconfirmed file")
	failedSHA := record(failed, "sent", 102)
	exec("INSERT INTO full_backup_jobs(requested_at,if_changed,state,encrypted_sha) VALUES('fixture',0,'error',?)", failedSHA)

	companionMissing := create(filepath.Join(dir, name(4)), "no confirmation message")
	record(companionMissing, "sent", 0)

	tamperedReceipt := create(filepath.Join(dir, name(5)), "invalid remote receipt")
	record(tamperedReceipt, "sent", 103)
	exec("UPDATE full_backup_jobs SET receipt_json=json_set(receipt_json,'$.thread_id',42) WHERE artifact_path=?", tamperedReceipt)

	mutated := create(filepath.Join(dir, name(6)), "old content")
	record(mutated, "sent", 104)
	create(mutated, "new content")

	untracked := create(filepath.Join(dir, name(7)), "missing artifact registration")
	record(untracked, "sent", 105)
	exec("DELETE FROM full_backup_artifacts WHERE path=?", untracked)

	outside := create(filepath.Join(t.TempDir(), name(8)), "in a different directory")
	record(outside, "sent", 106)
	link := filepath.Join(dir, name(9))
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	record(link, "sent", 107)

	prunable := create(filepath.Join(dir, name(10)), "third newest valid backup")
	record(prunable, "sent", 108)
	retained := create(filepath.Join(dir, name(11)), "second newest valid backup")
	record(retained, "sent", 109)
	latest := create(filepath.Join(dir, name(12)), "latest valid backup")
	record(latest, "sent", 110)
	record(latest, "sent", 110) // Repeated job records must not count as another local file.

	removed, err := a.PruneFullBackups(ctx)
	if err != nil || removed.Files != 0 {
		t.Fatalf("default retention modified files: %+v %v", removed, err)
	}
	a.Config.BackupLocalKeep = 2
	removed, err = a.PruneFullBackups(ctx)
	if err != nil || removed.Files != 2 || removed.Bytes != int64(len("oldest archived backup")+len("third newest valid backup")) {
		t.Fatalf("unexpected retention result: %+v %v", removed, err)
	}
	for _, path := range []string{old, prunable} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("expected %s to be pruned: %v", filepath.Base(path), err)
		}
	}
	for _, path := range []string{uncertain, failed, companionMissing, tamperedReceipt, mutated, untracked, outside, link, retained, latest} {
		if _, err := os.Lstat(path); err != nil {
			t.Errorf("expected %s to remain: %v", filepath.Base(path), err)
		}
	}
	var jobRows, artifactRows int
	err = s.DB.QueryRow(`SELECT
 (SELECT count(*) FROM full_backup_jobs WHERE encrypted_sha=? AND state='sent' AND companion_message_id=100 AND receipt_json<>''),
 (SELECT count(*) FROM full_backup_artifacts WHERE encrypted_sha=?)`, oldestSHA, oldestSHA).Scan(&jobRows, &artifactRows)
	if err != nil || jobRows != 1 || artifactRows != 1 {
		t.Fatalf("original backup registry or receipt lost: %d %d %v", jobRows, artifactRows, err)
	}
	removed, err = a.PruneFullBackups(ctx)
	if err != nil || removed.Files != 0 || removed.Bytes != 0 {
		t.Fatalf("a repeated run should be a no-op: %+v %v", removed, err)
	}
}

func TestFullBackupRetentionSharesArchiveWorkerLock(t *testing.T) {
	c, s := archiveTestStore(t)
	c.BackupLocalKeep = 1
	a := NewArchiver(c, s, nil, nil)
	f, err := os.OpenFile(filepath.Join(c.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PruneFullBackups(context.Background()); err == nil {
		t.Fatal("manual pruning overlapped an archive worker")
	}
}
