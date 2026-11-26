package companion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

var fullBackupFilename = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z__jardulik-full__[0-9]+-profiles-[0-9]+\.zip\.enc$`)

type FullBackupPruneResult struct {
	Files int   `json:"files_removed"`
	Bytes int64 `json:"bytes_removed"`
}

// PruneFullBackups takes the archive worker lock for an operator-initiated run.
func (a *Archiver) PruneFullBackups(ctx context.Context) (FullBackupPruneResult, error) {
	var result FullBackupPruneResult
	if a.Config.BackupLocalKeep <= 0 {
		return result, nil
	}
	lock, err := os.OpenFile(filepath.Join(a.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return result, errors.New("archive worker is active; pause it before manual pruning")
		}
		return result, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err = a.initFullBackup(ctx); err != nil {
		return result, err
	}
	return a.pruneFullBackupsLocked(ctx)
}

func (a *Archiver) pruneFullBackupsBestEffort(ctx context.Context) {
	if a.Config.BackupLocalKeep <= 0 {
		return
	}
	result, err := a.pruneFullBackupsLocked(ctx)
	if err != nil {
		slog.Warn("local full backup pruning failed", "error", archiveErrorCode(err))
	}
	if result.Files > 0 {
		slog.Info("local full backups pruned", "files", result.Files, "bytes", result.Bytes)
	}
}

type fullBackupPruneCandidate struct {
	name string
	sha  string
	size int64
}

func (a *Archiver) fullBackupPruneCandidates(ctx context.Context, dir string) ([]fullBackupPruneCandidate, error) {
	rows, err := a.Store.DB.QueryContext(ctx, `SELECT j.artifact_path,j.encrypted_sha,j.byte_size,j.random_id,j.receipt_json
 FROM full_backup_jobs j
 WHERE j.state='sent' AND j.error='' AND j.companion_message_id BETWEEN 1 AND 2147483647
 AND EXISTS (SELECT 1 FROM full_backup_artifacts f WHERE f.path=j.artifact_path AND f.encrypted_sha=j.encrypted_sha AND f.byte_size=j.byte_size)
 AND NOT EXISTS (SELECT 1 FROM full_backup_jobs other WHERE other.state<>'sent'
   AND (other.artifact_path=j.artifact_path OR other.encrypted_sha=j.encrypted_sha))
 ORDER BY j.updated_at DESC,j.id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []fullBackupPruneCandidate
	for rows.Next() {
		var candidate fullBackupPruneCandidate
		var path, raw string
		var randomID int64
		if err = rows.Scan(&path, &candidate.sha, &candidate.size, &randomID, &raw); err != nil {
			return nil, err
		}
		candidate.name = filepath.Base(path)
		if filepath.Join(dir, candidate.name) != path || !fullBackupFilename.MatchString(candidate.name) {
			continue
		}
		var receipt FullBackupReceipt
		if len(raw) > transferOutputLimit || json.Unmarshal([]byte(raw), &receipt) != nil || verifyFullReceipt(a.Config, receipt, candidate.sha, candidate.size, randomID, "sent") != nil {
			continue
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

// Caller holds archive-worker.lock. Rows stay intact so Telegram receipts remain usable.
func (a *Archiver) pruneFullBackupsLocked(ctx context.Context) (FullBackupPruneResult, error) {
	var result FullBackupPruneResult
	dir := filepath.Join(a.Config.DataDir, "backups")
	before, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if !before.IsDir() {
		return result, errors.New("local backup directory must be a real directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return result, err
	}
	defer root.Close()
	if opened, err := root.Stat("."); err != nil || !os.SameFile(before, opened) {
		return result, errors.New("local backup directory changed")
	}
	candidates, err := a.fullBackupPruneCandidates(ctx, dir)
	if err != nil {
		return result, err
	}
	seen := make(map[string]bool)
	kept := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if seen[candidate.name] {
			continue
		}
		seen[candidate.name] = true
		matches, err := candidate.matches(root)
		if err != nil {
			return result, err
		}
		if !matches {
			continue
		}
		if kept < a.Config.BackupLocalKeep {
			kept++
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := root.Remove(candidate.name); err != nil {
			return result, err
		}
		result.Files++
		result.Bytes += candidate.size
	}
	return result, nil
}

func (c fullBackupPruneCandidate) matches(root *os.Root) (bool, error) {
	before, err := root.Lstat(c.name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !before.Mode().IsRegular() || before.Size() != c.size {
		return false, nil
	}
	file, err := root.OpenFile(c.name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if opened, err := file.Stat(); err != nil || !os.SameFile(before, opened) {
		return false, errors.New("local backup file changed")
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(file, c.size+1))
	if err != nil {
		return false, err
	}
	if size != c.size || hex.EncodeToString(hash.Sum(nil)) != c.sha {
		return false, nil
	}
	after, err := root.Lstat(c.name)
	if err != nil {
		return false, err
	}
	return os.SameFile(before, after) && after.Size() == before.Size() && after.ModTime().Equal(before.ModTime()), nil
}
