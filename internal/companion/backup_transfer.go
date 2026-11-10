package companion

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

const transferOutputLimit = 64 << 10
const fullTransferSchema = `CREATE TABLE IF NOT EXISTS full_backup_jobs (
 id INTEGER PRIMARY KEY, requested_at TEXT NOT NULL, if_changed INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending', source_fingerprint TEXT NOT NULL DEFAULT '',
 artifact_path TEXT NOT NULL DEFAULT '', source_sha TEXT NOT NULL DEFAULT '', encrypted_sha TEXT NOT NULL DEFAULT '',
 byte_size INTEGER NOT NULL DEFAULT 0, random_id INTEGER NOT NULL DEFAULT 0,
 receipt_json TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', companion_message_id INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

type FullBackupReceipt struct {
	Status     string `json:"status"`
	Transport  string `json:"transport"`
	ChatID     int64  `json:"chat_id"`
	ThreadID   int64  `json:"thread_id"`
	MessageID  int64  `json:"message_id"`
	DocumentID int64  `json:"document_id"`
	ByteSize   int64  `json:"byte_size"`
	SourceSHA  string `json:"source_sha256"`
	RandomID   int64  `json:"random_id"`
	ErrorCode  string `json:"error_code,omitempty"`
	RetryAfter int64  `json:"retry_after,omitempty"`
}

type transferFailure struct{ status, code string }

func (e *transferFailure) Error() string { return "telegram transfer: " + e.code }
func (c Config) transferConfigured() bool {
	return c.TelegramAPIID > 0 && c.TelegramAPIHash != "" && c.TransferHelper != ""
}
func (c Config) transferSessionPath() string {
	if c.TransferSession != "" {
		return c.TransferSession
	}
	return filepath.Join(c.DataDir, "telegram-bot-session.enc")
}

func (a *Archiver) initFullBackup(ctx context.Context) error {
	if err := a.init(ctx); err != nil {
		return err
	}
	if _, err := a.Store.DB.ExecContext(ctx, fullBackupSchema+fullTransferSchema); err != nil {
		return err
	}
	rows, err := a.Store.DB.QueryContext(ctx, "PRAGMA table_info(full_backup_artifacts)")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var id, nn, pk int
		var name, kind string
		var def any
		if err = rows.Scan(&id, &name, &kind, &nn, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == "artifact_json"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		_, err = a.Store.DB.ExecContext(ctx, "ALTER TABLE full_backup_artifacts ADD COLUMN artifact_json TEXT NOT NULL DEFAULT ''")
	}
	return err
}

func (a *Archiver) RequestFullBackup(ctx context.Context, ifChanged bool) error {
	if err := a.initFullBackup(ctx); err != nil {
		return err
	}
	// A request still waiting to build will capture the most recent source state.
	_, err := a.Store.DB.ExecContext(ctx, `INSERT INTO full_backup_jobs(requested_at,if_changed)
 SELECT ?,? WHERE NOT EXISTS(SELECT 1 FROM full_backup_jobs WHERE state IN ('pending','building','waiting_transport','waiting_activation') AND if_changed=?)`, a.now().In(time.FixedZone("MSK", 10800)).Format(time.RFC3339Nano), ifChanged, ifChanged)
	return err
}

func (a *Archiver) fullBackupStatus(ctx context.Context) string {
	if err := a.initFullBackup(ctx); err != nil {
		return "Ошибка состояния полного бэкапа"
	}
	var state, at, detail string
	err := a.Store.DB.QueryRowContext(ctx, "SELECT state,requested_at,error FROM full_backup_jobs ORDER BY id DESC LIMIT 1").Scan(&state, &at, &detail)
	if errors.Is(err, sql.ErrNoRows) {
		return "Полный бэкап ещё не запрашивался"
	}
	if err != nil {
		return "Ошибка состояния полного бэкапа"
	}
	if detail == "full_backup_insufficient_disk" {
		return fmt.Sprintf("Полный бэкап: %s. Освободите место для сборки с сохранением дискового резерва.", displayTime(at))
	}
	if detail != "" {
		return fmt.Sprintf("Полный бэкап: %s, %s (%s)", state, displayTime(at), detail)
	}
	return fmt.Sprintf("Полный бэкап: %s, %s", state, displayTime(at))
}

func validTransferCode(s string) string {
	if s == "" || len(s) > 80 {
		return "helper_failed"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			return "helper_failed"
		}
	}
	return s
}
func transferEnvironment(c Config) []string {
	return []string{
		"TELEGRAM_API_ID=" + strconv.FormatInt(c.TelegramAPIID, 10), "TELEGRAM_API_HASH=" + c.TelegramAPIHash,
		"TELEGRAM_BOT_TOKEN=" + c.Token, "SESSION_KEY=" + base64.StdEncoding.EncodeToString(c.SessionKey),
		"APP_DATA_DIR=" + c.DataDir, "TELEGRAM_TRANSFER_SESSION_FILE=" + c.transferSessionPath(),
		"TELEGRAM_TRANSFER_BYTES_PER_SECOND=" + strconv.FormatInt(c.TransferBytesPerSecond, 10),
		"BACKUP_CHAT_ID=" + strconv.FormatInt(c.BackupChatID, 10), "BACKUP_THREAD_ID=" + strconv.FormatInt(c.BackupThreadID, 10),
	}
}

type boundedTransferOutput struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedTransferOutput) Write(p []byte) (int, error) {
	if len(p) > transferOutputLimit-b.Len() {
		b.overflow = true
		return 0, errors.New("helper output exceeded limit")
	}
	return b.Buffer.Write(p)
}

func runTelegramTransfer(ctx context.Context, c Config, args []string) (FullBackupReceipt, error) {
	var receipt FullBackupReceipt
	if !c.transferConfigured() {
		return receipt, &transferFailure{"failed", "transfer_not_configured"}
	}
	childCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	command := exec.CommandContext(childCtx, c.TransferHelper, args...)
	command.Env = transferEnvironment(c)
	var output boundedTransferOutput
	command.Stdout = &output
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return receipt, &transferFailure{"failed", "helper_start_failed"}
	}
	runErr := command.Wait()
	if output.overflow {
		return receipt, &transferFailure{"uncertain", "helper_output_exceeded_limit"}
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	if decoder.Decode(&receipt) != nil {
		return receipt, &transferFailure{"uncertain", "helper_outcome_unconfirmed"}
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return receipt, &transferFailure{"uncertain", "helper_outcome_unconfirmed"}
	}
	if receipt.Status == "failed" || receipt.Status == "uncertain" {
		return receipt, &transferFailure{receipt.Status, validTransferCode(receipt.ErrorCode)}
	}
	if runErr != nil {
		return receipt, &transferFailure{"uncertain", "helper_outcome_unconfirmed"}
	}
	return receipt, nil
}

func verifyFullReceipt(c Config, r FullBackupReceipt, sha string, size, randomID int64, status string) error {
	if r.Status != status || r.Transport != "mtproto" || r.ChatID != c.BackupChatID || r.ThreadID != c.BackupThreadID || r.MessageID <= 0 || r.MessageID > 1<<31-1 || r.DocumentID == 0 || r.ByteSize != size || r.SourceSHA != sha || !restoreSHA(sha) || r.RandomID != randomID || randomID == 0 || size <= 0 || size > fullBundleLimit || r.ErrorCode != "" || r.RetryAfter != 0 {
		return &transferFailure{"uncertain", "helper_receipt_unconfirmed"}
	}
	return nil
}

func newBackupRandomID() (int64, error) {
	var raw [8]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		id := int64(binary.LittleEndian.Uint64(raw[:]))
		if id != 0 {
			return id, nil
		}
	}
}

type fullBackupJob struct {
	id       int64
	daily    bool
	state    string
	artifact FullBackupArtifact
	randomID int64
}

// Run holds the shared archive mutex and rate gate for every external send.
func (a *Archiver) processNextFullBackup(ctx context.Context) (bool, error) {
	if err := a.initFullBackup(ctx); err != nil {
		return true, err
	}
	var job fullBackupJob
	err := a.Store.DB.QueryRowContext(ctx, `SELECT id,if_changed,state,source_fingerprint,artifact_path,source_sha,encrypted_sha,byte_size,random_id
 FROM full_backup_jobs WHERE state IN ('pending','ready') OR (state IN ('waiting_transport','waiting_activation') AND ?)
 ORDER BY id LIMIT 1`, a.Config.FullBackupEnabled && a.Config.transferConfigured()).Scan(&job.id, &job.daily, &job.state, &job.artifact.SourceFingerprint, &job.artifact.Path, &job.artifact.SourceSHA, &job.artifact.EncryptedSHA, &job.artifact.ByteSize, &job.randomID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !a.Config.FullBackupEnabled {
		_, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='waiting_activation',error='full_transfer_not_activated',updated_at=CURRENT_TIMESTAMP WHERE id=?", job.id)
		return true, err
	}
	if !a.Config.transferConfigured() {
		_, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='waiting_transport',error='transfer_not_configured',updated_at=CURRENT_TIMESTAMP WHERE id=?", job.id)
		return true, err
	}
	if job.state != "ready" {
		plannedCipher, plannedRandom := job.artifact.EncryptedSHA, job.randomID
		if _, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='building',error='',updated_at=CURRENT_TIMESTAMP WHERE id=?", job.id); err != nil {
			return true, err
		}
		job.artifact, err = a.buildFullBackupLocked(ctx, job.daily)
		if err != nil {
			code := "full_backup_build_failed"
			var sqliteErr interface{ Code() int }
			switch {
			case errors.Is(err, errFullBackupDisk), errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT),
				errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_FULL:
				code = "full_backup_insufficient_disk"
			case errors.Is(err, errArchiveBudget):
				code = "full_backup_size_limit"
			case errors.Is(err, errFullBackupMedia):
				code = "full_backup_source_media_invalid"
			}
			_, _ = a.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE full_backup_jobs SET state='error',error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", code, job.id)
			if code == "full_backup_insufficient_disk" {
				return true, errFullBackupDisk
			}
			return true, errors.New("full backup build failed")
		}
		if job.artifact.Unchanged {
			_, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='skipped',error='',updated_at=CURRENT_TIMESTAMP WHERE id=?", job.id)
			return true, err
		}
		var previous int64
		var state string
		var random int64
		var previousCipher string
		e := a.Store.DB.QueryRowContext(ctx, `SELECT id,state,random_id,encrypted_sha FROM full_backup_jobs WHERE id<>? AND source_fingerprint=? AND state IN ('ready','sending','uncertain','error') ORDER BY CASE state WHEN 'uncertain' THEN 0 WHEN 'sending' THEN 1 ELSE 2 END,id LIMIT 1`, job.id, job.artifact.SourceFingerprint).Scan(&previous, &state, &random, &previousCipher)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return true, e
		}
		if e == nil && (state == "uncertain" || state == "sending") {
			_, err = a.Store.DB.ExecContext(ctx, `UPDATE full_backup_jobs SET state='uncertain',source_fingerprint=?,artifact_path=?,source_sha=?,encrypted_sha=?,byte_size=?,error='prior_full_backup_delivery_uncertain',updated_at=CURRENT_TIMESTAMP WHERE id=?`, job.artifact.SourceFingerprint, job.artifact.Path, job.artifact.SourceSHA, job.artifact.EncryptedSHA, job.artifact.ByteSize, job.id)
			return true, err
		}
		if e == nil && state == "ready" {
			_, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='superseded',error='same_source_backup_pending',updated_at=CURRENT_TIMESTAMP WHERE id=?", job.id)
			return true, err
		}
		// A definitive pre-send failure can reuse the already persisted identity.
		job.randomID = random
		if previousCipher != job.artifact.EncryptedSHA {
			job.randomID = 0
		}
		if plannedCipher == job.artifact.EncryptedSHA && plannedRandom != 0 {
			job.randomID = plannedRandom
		}
		if job.randomID == 0 {
			job.randomID, err = newBackupRandomID()
			if err != nil {
				return true, err
			}
		}
		_, err = a.Store.DB.ExecContext(ctx, `UPDATE full_backup_jobs SET state='ready',source_fingerprint=?,artifact_path=?,source_sha=?,encrypted_sha=?,byte_size=?,random_id=?,error='',updated_at=CURRENT_TIMESTAMP WHERE id=?`, job.artifact.SourceFingerprint, job.artifact.Path, job.artifact.SourceSHA, job.artifact.EncryptedSHA, job.artifact.ByteSize, job.randomID, job.id)
		if err != nil {
			return true, err
		}
	}
	if job.state == "ready" {
		var raw string
		if err = a.Store.DB.QueryRowContext(ctx, "SELECT artifact_json FROM full_backup_artifacts WHERE encrypted_sha=? ORDER BY id DESC LIMIT 1", job.artifact.EncryptedSHA).Scan(&raw); err != nil {
			return true, errors.New("full backup metadata unavailable")
		}
		var stored FullBackupArtifact
		if len(raw) > transferOutputLimit || json.Unmarshal([]byte(raw), &stored) != nil || stored.Path != job.artifact.Path || stored.EncryptedSHA != job.artifact.EncryptedSHA || stored.SourceSHA != job.artifact.SourceSHA || stored.SourceFingerprint != job.artifact.SourceFingerprint || stored.ByteSize != job.artifact.ByteSize {
			return true, errors.New("full backup metadata unconfirmed")
		}
		job.artifact = stored
	}
	sha, size, err := fileDigest(job.artifact.Path)
	if err != nil || sha != job.artifact.EncryptedSHA || size != job.artifact.ByteSize {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='error',error='full_backup_file_unconfirmed' WHERE id=?", job.id)
		return true, errors.New("full backup file unconfirmed")
	}
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='sending',updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='ready'", job.id); err != nil {
		return true, err
	}
	var receipt FullBackupReceipt
	err = a.groupCall(ctx, func() error {
		var transferErr error
		receipt, transferErr = runTelegramTransfer(ctx, a.Config, []string{"upload", "--file", job.artifact.Path, "--random-id", strconv.FormatInt(job.randomID, 10), "--sha256", job.artifact.EncryptedSHA, "--size", strconv.FormatInt(job.artifact.ByteSize, 10)})
		return transferErr
	})
	if err == nil {
		err = verifyFullReceipt(a.Config, receipt, job.artifact.EncryptedSHA, job.artifact.ByteSize, job.randomID, "sent")
	}
	if err != nil {
		state, code := "uncertain", "transfer_outcome_unconfirmed"
		var failure *transferFailure
		if errors.As(err, &failure) {
			code = failure.code
			if failure.status == "failed" {
				state = "error"
			}
		}
		receipt.ErrorCode = code
		partialReceipt, _ := json.Marshal(receipt)
		_, _ = a.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE full_backup_jobs SET state=?,error=?,receipt_json=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", state, code, string(partialReceipt), job.id)
		return true, &transferFailure{state, code}
	}
	encoded, _ := json.Marshal(receipt)
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET state='sent',receipt_json=?,error='',updated_at=CURRENT_TIMESTAMP WHERE id=?", string(encoded), job.id); err != nil {
		return true, err
	}
	text := fmt.Sprintf("Jardulik: полный зашифрованный бэкап\nАнкет: %d; версий: %d; фото: %d.\nОдин ZIP на человека, index.txt, SQLite и восстановление.\nSHA-256 исходного ZIP: %s\nSHA-256 переданного файла: %s\nTelegram receipt: %s\nХэши относятся к локальным исходным байтам. Байты на сервере Telegram не проверялись.", job.artifact.Profiles, job.artifact.Versions, job.artifact.MediaFiles, job.artifact.SourceSHA, job.artifact.EncryptedSHA, string(encoded))
	if a.TG == nil {
		return true, nil
	}
	var message SentMessage
	err = a.groupCall(ctx, func() error {
		var e error
		message, e = a.TG.SendText(ctx, a.Config.BackupChatID, a.Config.BackupThreadID, text, nil)
		return e
	})
	if err != nil {
		_, _ = a.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE full_backup_jobs SET error='companion_delivery_unconfirmed' WHERE id=?", job.id)
		return true, errors.New("backup companion delivery unconfirmed")
	}
	_, err = a.Store.DB.ExecContext(ctx, "UPDATE full_backup_jobs SET companion_message_id=? WHERE id=?", message.ID, job.id)
	return true, err
}

func (a *Archiver) confirmedFullFingerprint(ctx context.Context) (string, error) {
	rows, err := a.Store.DB.QueryContext(ctx, "SELECT source_fingerprint,encrypted_sha,byte_size,random_id,receipt_json FROM full_backup_jobs WHERE state='sent' ORDER BY id DESC")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var fp, sha, raw string
		var size, randomID int64
		if err = rows.Scan(&fp, &sha, &size, &randomID, &raw); err != nil {
			return "", err
		}
		var r FullBackupReceipt
		if len(raw) > transferOutputLimit || json.Unmarshal([]byte(raw), &r) != nil || verifyFullReceipt(a.Config, r, sha, size, randomID, "sent") != nil {
			return "", errors.New("confirmed full backup receipt invalid")
		}
		return fp, nil
	}
	return "", rows.Err()
}

func DownloadFullBackup(ctx context.Context, c Config, receiptPath, output string) error {
	info, err := os.Stat(receiptPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > transferOutputLimit {
		return errors.New("invalid full backup receipt file")
	}
	raw, err := os.ReadFile(receiptPath)
	if err != nil {
		return errors.New("full backup receipt unavailable")
	}
	var original FullBackupReceipt
	if json.Unmarshal(raw, &original) != nil || verifyFullReceipt(c, original, original.SourceSHA, original.ByteSize, original.RandomID, "sent") != nil {
		return errors.New("invalid full backup receipt")
	}
	if original.ByteSize <= 0 || original.ByteSize > fullBundleLimit {
		return errArchiveBudget
	}
	if _, err = os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		return errors.New("download destination must not exist")
	}
	receipt, err := runTelegramTransfer(ctx, c, []string{"download", "--receipt", receiptPath, "--output", output, "--sha256", original.SourceSHA, "--size", strconv.FormatInt(original.ByteSize, 10)})
	if err != nil {
		return err
	}
	if err = verifyFullReceipt(c, receipt, original.SourceSHA, original.ByteSize, original.RandomID, "downloaded"); err != nil {
		return err
	}
	if receipt.MessageID != original.MessageID || receipt.DocumentID != original.DocumentID {
		return errors.New("downloaded full backup locator unconfirmed")
	}
	sha, size, err := fileDigest(output)
	if err != nil || sha != original.SourceSHA || size != original.ByteSize {
		return errors.New("downloaded full backup hash unconfirmed")
	}
	return nil
}

// ProcessFullBackupOnce is an operator acceptance path, not another worker.
func (a *Archiver) ProcessFullBackupOnce(ctx context.Context) (bool, error) {
	if !a.Config.FullBackupEnabled {
		return false, errors.New("full backup transfer is not activated")
	}
	if err := os.MkdirAll(a.Config.DataDir, 0700); err != nil {
		return false, err
	}
	lock, err := os.OpenFile(filepath.Join(a.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return false, errors.New("archive worker is active; pause it before a one-shot transfer")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.initFullBackup(ctx); err != nil {
		return false, err
	}
	if err := a.recoverFullBackupJobs(ctx); err != nil {
		return false, err
	}
	return a.processNextFullBackup(ctx)
}

// Call only while holding archive-worker.lock, before starting the sender.
func (a *Archiver) recoverFullBackupJobs(ctx context.Context) error {
	_, err := a.Store.DB.ExecContext(ctx, `UPDATE archive_backups SET state='disabled',error='legacy_metadata_backup_disabled' WHERE state='pending';
 UPDATE full_backup_jobs SET state='pending',error='' WHERE state='building';
 UPDATE full_backup_jobs SET state='uncertain',error='worker_restarted_during_transfer' WHERE state='sending';`)
	return err
}
