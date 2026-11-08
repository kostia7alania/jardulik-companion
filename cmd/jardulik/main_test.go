package main

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreRequiresOnlyBackupKey(t *testing.T) {
	args := os.Args
	t.Cleanup(func() { os.Args = args })
	os.Args = []string{"jardulik", "restore", filepath.Join(t.TempDir(), "missing.enc"), filepath.Join(t.TempDir(), "restored")}
	t.Setenv("BACKUP_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	for _, name := range []string{"SESSION_KEY", "OWNER_TELEGRAM_ID", "TELEGRAM_BOT_TOKEN"} {
		t.Setenv(name, "")
	}
	if err := run(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore must reach the archive using only BACKUP_KEY: %v", err)
	}
}

func TestRestoreRejectsInvalidBackupKeyWithoutServiceConfig(t *testing.T) {
	args := os.Args
	t.Cleanup(func() { os.Args = args })
	os.Args = []string{"jardulik", "restore", "snapshot.enc", filepath.Join(t.TempDir(), "restored")}
	t.Setenv("BACKUP_KEY", "invalid")
	t.Setenv("OWNER_TELEGRAM_ID", "")
	if err := run(); err == nil || !strings.Contains(err.Error(), "BACKUP_KEY") {
		t.Fatalf("expected the recovery-key error: %v", err)
	}
}

func TestServiceRoleCommandsRejectArgumentsBeforeLoadingConfig(t *testing.T) {
	args := os.Args
	t.Cleanup(func() { os.Args = args })
	t.Setenv("SESSION_KEY", "")
	for _, command := range []string{"serve-bot-parser", "serve-poster"} {
		t.Run(command, func(t *testing.T) {
			os.Args = []string{"jardulik", command, "unexpected"}
			if err := run(); err == nil || err.Error() != "usage: jardulik "+command {
				t.Fatalf("invalid role command reached config or worker startup: %v", err)
			}
		})
	}
}
