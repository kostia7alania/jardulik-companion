package companion

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testShellQuote(v string) string { return "'" + strings.ReplaceAll(v, "'", "'\"'\"'") + "'" }

func transferFixture(t *testing.T) (Config, *Store, *Archiver, string, string) {
	t.Helper()
	c, s := archiveTestStore(t)
	dir := t.TempDir()
	mode := filepath.Join(dir, "mode")
	calls := filepath.Join(dir, "calls")
	c.FullBackupEnabled = true
	c.TelegramAPIID = 12345
	c.TelegramAPIHash = strings.Repeat("a", 32)
	c.TransferHelper = filepath.Join(dir, "helper")
	c.TransferSession = filepath.Join(c.DataDir, "telegram-bot-session.enc")
	script := `#!/bin/sh
command=$1
shift
file= random= sha= size= receipt= output=
while [ "$#" -gt 0 ]; do
 key=$1; value=$2; shift 2
 case "$key" in --file) file=$value;; --random-id) random=$value;; --sha256) sha=$value;; --size) size=$value;; --receipt) receipt=$value;; --output) output=$value;; esac
done
mode=sent
if [ -f MODE ]; then read mode < MODE; fi
printf '%s|%s|%s|%s\n' "$command" "$random" "$sha" "$file" >> CALLS
case "$mode" in
 uncertain) printf '{"status":"uncertain","error_code":"send_unconfirmed"}'; exit 1;;
 failed) printf '{"status":"failed","error_code":"auth_failed"}'; exit 1;;
 invalid) printf '%s' "$TELEGRAM_API_HASH"; printf '%s' "$TELEGRAM_BOT_TOKEN" >&2; exit 1;;
 overflow) printf '%070000d' 0; exit 1;;
esac
printf '{"status":"sent","transport":"mtproto","chat_id":%s,"thread_id":%s,"message_id":123,"document_id":456,"byte_size":%s,"source_sha256":"%s","random_id":%s}' "$BACKUP_CHAT_ID" "$BACKUP_THREAD_ID" "$size" "$sha" "$random"
`
	script = strings.ReplaceAll(script, "MODE", testShellQuote(mode))
	script = strings.ReplaceAll(script, "CALLS", testShellQuote(calls))
	if err := os.WriteFile(c.TransferHelper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, s, nil, nil)
	if err := a.initFullBackup(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c, s, a, mode, calls
}
func transferProcess(t *testing.T, a *Archiver) error {
	t.Helper()
	_ = a.Store.SetMeta(context.Background(), "archive_next_send_ns", "")
	_, err := a.processNextFullBackup(context.Background())
	return err
}
func transferCalls(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(strings.TrimSpace(string(b)))
}
func transferJobState(t *testing.T, s *Store) string {
	t.Helper()
	var state string
	if err := s.DB.QueryRow("SELECT state FROM full_backup_jobs ORDER BY id DESC LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestFullTransferDailyBaselineSkipAndManualForce(t *testing.T) {
	ctx := context.Background()
	_, s, a, _, calls := transferFixture(t)
	if _, err := s.DB.ExecContext(ctx, "INSERT INTO archive_backups(requested_at,state) VALUES('old','pending')"); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestFullBackup(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "sent" || len(transferCalls(t, calls)) != 1 {
		t.Fatal("first full baseline not confirmed")
	}
	fp, err := a.lastConfirmedFullFingerprint(ctx)
	if err != nil || fp == "" {
		t.Fatalf("verified receipt not accepted: %v", err)
	}
	if err = a.RequestFullBackup(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err = transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "skipped" || len(transferCalls(t, calls)) != 1 {
		t.Fatal("unchanged sources sent another daily archive")
	}
	if err = a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "sent" || len(transferCalls(t, calls)) != 2 {
		t.Fatal("manual force did not send a full snapshot")
	}
	var legacy string
	if err = s.DB.QueryRowContext(ctx, "SELECT state FROM archive_backups WHERE requested_at='old'").Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	if legacy != "pending" {
		t.Fatal("full worker executed a legacy metadata-only request")
	}
}

func TestFullTransferUnknownDoesNotRetryOrRebuild(t *testing.T) {
	ctx := context.Background()
	_, s, a, mode, calls := transferFixture(t)
	if err := os.WriteFile(mode, []byte("uncertain\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err == nil {
		t.Fatal("uncertain helper result accepted")
	}
	if transferJobState(t, s) != "uncertain" {
		t.Fatal("unknown send not retained")
	}
	var firstPath string
	var firstRandom int64
	if err := s.DB.QueryRowContext(ctx, "SELECT artifact_path,random_id FROM full_backup_jobs ORDER BY id LIMIT 1").Scan(&firstPath, &firstRandom); err != nil {
		t.Fatal(err)
	}
	if firstRandom == 0 {
		t.Fatal("random_id not persisted before send")
	}
	if err := os.WriteFile(mode, []byte("sent\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	var path string
	var artifacts int
	if err := s.DB.QueryRowContext(ctx, "SELECT artifact_path FROM full_backup_jobs ORDER BY id DESC LIMIT 1").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM full_backup_artifacts").Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if path != firstPath || artifacts != 1 || len(transferCalls(t, calls)) != 1 || transferJobState(t, s) != "uncertain" {
		t.Fatal("unknown send was retried or rebuilt")
	}
	fp, err := a.lastConfirmedFullFingerprint(ctx)
	if err != nil || fp != "" {
		t.Fatal("uncertain send marked sources clean")
	}
}

func TestFullTransferDefinitiveFailureReusesFileAndRandomID(t *testing.T) {
	ctx := context.Background()
	_, s, a, mode, calls := transferFixture(t)
	if err := os.WriteFile(mode, []byte("failed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err == nil {
		t.Fatal("failed helper accepted")
	}
	if transferJobState(t, s) != "error" {
		t.Fatal("definitive pre-send failure misclassified")
	}
	if err := os.WriteFile(mode, []byte("sent\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	got := transferCalls(t, calls)
	if len(got) != 2 || got[0] != got[1] || transferJobState(t, s) != "sent" {
		t.Fatalf("retry changed persisted transfer identity: %v", got)
	}
}

func TestFullTransferRedactsUnexpectedHelperOutput(t *testing.T) {
	for _, modeValue := range []string{"invalid", "overflow"} {
		t.Run(modeValue, func(t *testing.T) {
			ctx := context.Background()
			c, s, a, mode, _ := transferFixture(t)
			if err := os.WriteFile(mode, []byte(modeValue+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := a.RequestBackup(ctx); err != nil {
				t.Fatal(err)
			}
			err := transferProcess(t, a)
			if err == nil {
				t.Fatal("malformed helper accepted")
			}
			if strings.Contains(err.Error(), c.TelegramAPIHash) || strings.Contains(err.Error(), c.Token) {
				t.Fatal("helper output leaked secrets")
			}
			if transferJobState(t, s) != "uncertain" {
				t.Fatal("post-start invalid output was retryable")
			}
		})
	}
}

func TestFullTransferReceiptMustMatchBindings(t *testing.T) {
	c := Config{BackupChatID: -100123, BackupThreadID: 7}
	sha := strings.Repeat("a", 64)
	valid := FullBackupReceipt{Status: "sent", Transport: "mtproto", ChatID: c.BackupChatID, ThreadID: c.BackupThreadID, MessageID: 12, DocumentID: -23, ByteSize: 100, SourceSHA: sha, RandomID: -34}
	if err := verifyFullReceipt(c, valid, sha, 100, -34, "sent"); err != nil {
		t.Fatal(err)
	}
	changes := []func(*FullBackupReceipt){func(r *FullBackupReceipt) { r.ChatID-- }, func(r *FullBackupReceipt) { r.ThreadID++ }, func(r *FullBackupReceipt) { r.RandomID++ }, func(r *FullBackupReceipt) { r.SourceSHA = strings.Repeat("b", 64) }, func(r *FullBackupReceipt) { r.ByteSize++ }, func(r *FullBackupReceipt) { r.MessageID = 0 }, func(r *FullBackupReceipt) { r.DocumentID = 0 }, func(r *FullBackupReceipt) { r.Status = "failed" }}
	for i, change := range changes {
		r := valid
		change(&r)
		if err := verifyFullReceipt(c, r, sha, 100, -34, "sent"); err == nil {
			t.Fatalf("invalid binding %d accepted", i)
		}
	}
}

func TestFullTransferWaitsForCredentialsAndRecoversSecretsOnlyInsideEncryption(t *testing.T) {
	ctx := context.Background()
	c, s, a, _, calls := transferFixture(t)
	a.Config.TelegramAPIID = 0
	a.Config.TelegramAPIHash = ""
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "waiting_transport" || len(transferCalls(t, calls)) != 0 {
		t.Fatal("unconfigured transfer attempted")
	}
	a.Config = c
	if err := os.WriteFile(c.TransferSession, []byte("already-encrypted-bot-session"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	var artifactPath string
	if err := s.DB.QueryRowContext(ctx, "SELECT artifact_path FROM full_backup_jobs ORDER BY id DESC LIMIT 1").Scan(&artifactPath); err != nil {
		t.Fatal(err)
	}
	z := fullTestOpen(t, artifactPath, c.BackupKey)
	files := fullTestZipBytes(t, &z.Reader)
	if !bytes.Contains(files["recovery-config.json"], []byte(c.TelegramAPIHash)) || !bytes.Contains(files["recovery-config.json"], []byte(strconv.FormatInt(c.TelegramAPIID, 10))) {
		t.Fatal("API pair absent from encrypted recovery config")
	}
	if string(files["telegram-bot-session.enc"]) != "already-encrypted-bot-session" {
		t.Fatal("bot session not preserved")
	}
	dest := filepath.Join(t.TempDir(), "restore")
	if err := RestoreArchive(artifactPath, dest, c.BackupKey); err != nil {
		t.Fatal(err)
	}
	session, err := os.ReadFile(filepath.Join(dest, "telegram-bot-session.enc"))
	if err != nil || string(session) != "already-encrypted-bot-session" {
		t.Fatal("encrypted bot session not restored")
	}
}

func TestDownloadFullBackupOverHostedLimitChecksReceiptAndHash(t *testing.T) {
	ctx := context.Background()
	c, _ := archiveTestStore(t)
	dir := t.TempDir()
	payload := filepath.Join(dir, "source.enc")
	body := bytes.Repeat([]byte("download fixture"), 2<<20)
	if err := os.WriteFile(payload, body, 0600); err != nil {
		t.Fatal(err)
	}
	sha := digest(body)
	c.TelegramAPIID = 123
	c.TelegramAPIHash = strings.Repeat("b", 32)
	c.TransferHelper = filepath.Join(dir, "helper")
	script := `#!/bin/sh
shift
receipt= output= sha= size=
while [ "$#" -gt 0 ]; do key=$1;value=$2;shift 2;case "$key" in --output)output=$value;;--sha256)sha=$value;;--size)size=$value;;esac;done
/bin/cp SOURCE "$output"
printf '{"status":"downloaded","transport":"mtproto","chat_id":%s,"thread_id":%s,"message_id":123,"document_id":456,"byte_size":%s,"source_sha256":"%s","random_id":789}' "$BACKUP_CHAT_ID" "$BACKUP_THREAD_ID" "$size" "$sha"
`
	script = strings.ReplaceAll(script, "SOURCE", testShellQuote(payload))
	if err := os.WriteFile(c.TransferHelper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	receipt := FullBackupReceipt{Status: "sent", Transport: "mtproto", ChatID: c.BackupChatID, ThreadID: c.BackupThreadID, MessageID: 123, DocumentID: 456, ByteSize: int64(len(body)), SourceSHA: sha, RandomID: 789}
	raw, _ := json.Marshal(receipt)
	receiptFile := filepath.Join(dir, "receipt.json")
	if err := os.WriteFile(receiptFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "download.enc")
	if err := DownloadFullBackup(ctx, c, receiptFile, output); err != nil {
		t.Fatal(err)
	}
	got, size, err := fileDigest(output)
	if err != nil || got != sha || size <= 20<<20 {
		t.Fatalf("whole download failed: %v", err)
	}
	if err := DownloadFullBackup(ctx, c, receiptFile, output); err == nil {
		t.Fatal("existing file overwritten")
	}
}

func TestFullTransferDailyRequestsCoalesceBeforeBuilding(t *testing.T) {
	ctx := context.Background()
	_, s, a, _, _ := transferFixture(t)
	a.now = func() time.Time { return time.Date(2026, 11, 8, 0, 20, 0, 0, time.UTC) }
	for i := 0; i < 5; i++ {
		if err := a.RequestFullBackup(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM full_backup_jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("duplicate daily requests queued")
	}
}

func TestFullTransferActivationGateAndOneShotLock(t *testing.T) {
	ctx := context.Background()
	_, s, a, _, calls := transferFixture(t)
	a.Config.FullBackupEnabled = false
	if err := a.RequestBackup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "waiting_activation" || len(transferCalls(t, calls)) != 0 {
		t.Fatal("default-off activation gate was bypassed")
	}
	var count int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM full_backup_artifacts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("inactive scheduled transfer built an archive")
	}
	if _, err := a.ProcessFullBackupOnce(ctx); err == nil {
		t.Fatal("one-shot bypassed disabled gate")
	}
	a.Config.FullBackupEnabled = true
	lock, err := os.OpenFile(filepath.Join(a.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ProcessFullBackupOnce(ctx); err == nil {
		t.Fatal("one-shot overlapped active archive worker")
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	handled, err := a.ProcessFullBackupOnce(ctx)
	if err != nil || !handled {
		t.Fatalf("explicit one-shot failed: %v", err)
	}
	if transferJobState(t, s) != "sent" || len(transferCalls(t, calls)) != 1 {
		t.Fatal("one-shot did not send exactly one queued job")
	}
}

func TestFullTransferRestartPreservesUnknownAndResumesBuild(t *testing.T) {
	ctx := context.Background()
	_, s, a, _, calls := transferFixture(t)
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO full_backup_jobs(requested_at,if_changed,state,source_fingerprint,random_id) VALUES('first',0,'sending','unknown-source',123),('second',0,'building','',0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, "INSERT INTO archive_backups(requested_at,state) VALUES('old','pending')"); err != nil {
		t.Fatal(err)
	}
	if err := a.recoverFullBackupJobs(ctx); err != nil {
		t.Fatal(err)
	}
	var first, second, legacy string
	if err := s.DB.QueryRowContext(ctx, "SELECT state FROM full_backup_jobs WHERE requested_at='first'").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT state FROM full_backup_jobs WHERE requested_at='second'").Scan(&second); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRowContext(ctx, "SELECT state FROM archive_backups WHERE requested_at='old'").Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	if first != "uncertain" || second != "pending" || legacy != "disabled" || len(transferCalls(t, calls)) != 0 {
		t.Fatal("restart replayed an external effect or retained the legacy scheduler")
	}
}

func TestFullTransferConfigDefaultsOffAndContainsSessionPath(t *testing.T) {
	for name, value := range map[string]string{"APP_DATA_DIR": t.TempDir(), "TELEGRAM_BOT_TOKEN": "fixture-token", "OWNER_TELEGRAM_ID": "123", "JARDULIK_BOT_ID": "456", "BACKUP_CHAT_ID": "-100123", "BACKUP_THREAD_ID": "7", "SESSION_KEY": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), "BACKUP_KEY": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32)), "TELEGRAM_API_ID": "12345", "TELEGRAM_API_HASH": strings.Repeat("a", 32), "TELEGRAM_TRANSFER_HELPER": "", "TELEGRAM_TRANSFER_SESSION_FILE": "", "FULL_BACKUP_TRANSFER_ENABLED": "", "TELEGRAM_TRANSFER_BYTES_PER_SECOND": ""} {
		t.Setenv(name, value)
	}
	c, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.FullBackupEnabled || c.TransferHelper != "/opt/jardulik-companion/current/telegram-transfer" || c.TransferSession != filepath.Join(c.DataDir, "telegram-bot-session.enc") {
		t.Fatal("unsafe transfer defaults")
	}
	t.Setenv("FULL_BACKUP_TRANSFER_ENABLED", "true")
	c, err = LoadConfig()
	if err != nil || !c.FullBackupEnabled {
		t.Fatal("explicit activation not accepted")
	}
	t.Setenv("TELEGRAM_TRANSFER_SESSION_FILE", filepath.Join(t.TempDir(), "outside.enc"))
	if _, err = LoadConfig(); err == nil {
		t.Fatal("session outside private data directory accepted")
	}
	t.Setenv("TELEGRAM_TRANSFER_SESSION_FILE", "")
	for _, tc := range []struct {
		raw     string
		want    int64
		invalid bool
	}{{"", 0, false}, {"0", 0, false}, {"1048576", 1048576, false}, {"1073741824", 1 << 30, false}, {"-1", 0, true}, {"invalid", 0, true}, {"1073741825", 0, true}} {
		t.Run("rate_"+tc.raw, func(t *testing.T) {
			t.Setenv("TELEGRAM_TRANSFER_BYTES_PER_SECOND", tc.raw)
			got, loadErr := LoadConfig()
			if (loadErr != nil) != tc.invalid || (loadErr == nil && got.TransferBytesPerSecond != tc.want) {
				t.Fatalf("rate validation mismatch: %v", loadErr)
			}
		})
	}
}

func TestFullTransferActivationPauseKeepsPlannedRandomID(t *testing.T) {
	ctx := context.Background()
	_, s, a, _, calls := transferFixture(t)
	artifact, err := a.BuildFullBackup(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, `INSERT INTO full_backup_jobs(requested_at,if_changed,state,source_fingerprint,artifact_path,source_sha,encrypted_sha,byte_size,random_id) VALUES('fixture',0,'ready',?,?,?,?,?,321)`, artifact.SourceFingerprint, artifact.Path, artifact.SourceSHA, artifact.EncryptedSHA, artifact.ByteSize); err != nil {
		t.Fatal(err)
	}
	a.Config.FullBackupEnabled = false
	if err = transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	if transferJobState(t, s) != "waiting_activation" {
		t.Fatal("planned transfer did not pause")
	}
	a.Config.FullBackupEnabled = true
	if err = transferProcess(t, a); err != nil {
		t.Fatal(err)
	}
	got := transferCalls(t, calls)
	if len(got) != 1 || !strings.HasPrefix(got[0], "upload|321|") {
		t.Fatalf("activation changed persisted random_id: %v", got)
	}
}

func TestTransferRateEnvironmentForwarding(t *testing.T) {
	t.Setenv("UNRELATED_PRIVATE_VALUE", "must-not-be-inherited")
	c := Config{DataDir: t.TempDir(), TransferBytesPerSecond: 1048576}
	found := false
	for _, entry := range transferEnvironment(c) {
		found = found || entry == "TELEGRAM_TRANSFER_BYTES_PER_SECOND=1048576"
		if strings.HasPrefix(entry, "UNRELATED_PRIVATE_VALUE=") {
			t.Fatal("helper inherited unrelated environment")
		}
	}
	if !found {
		t.Fatal("acceptance rate missing from helper environment")
	}
	a := NewArchiver(c, nil, nil, nil)
	if a.fullRecoveryConfig()["TELEGRAM_TRANSFER_BYTES_PER_SECOND"] != int64(1048576) {
		t.Fatal("transfer rate missing from recovery configuration")
	}
}
