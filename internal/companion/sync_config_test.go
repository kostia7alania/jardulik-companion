package companion

import (
	"encoding/base64"
	"testing"
)

func TestSyncCycleConfigRequiresExplicitCompatibility(t *testing.T) {
	for key, value := range map[string]string{
		"APP_DATA_DIR": t.TempDir(), "OWNER_TELEGRAM_ID": "1",
		"SESSION_KEY":        base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"BACKUP_KEY":         base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"SYNC_CYCLE_ENABLED": "", "COMPATIBILITY_ENABLED": "",
	} {
		t.Setenv(key, value)
	}
	cfg, err := LoadConfig()
	if err != nil || cfg.SyncCycleEnabled {
		t.Fatal("cycle enabled without configuration", err)
	}
	t.Setenv("SYNC_CYCLE_ENABLED", "true")
	if _, err = LoadConfig(); err == nil {
		t.Fatal("cycle accepted without compatibility executor")
	}
	t.Setenv("COMPATIBILITY_ENABLED", "true")
	cfg, err = LoadConfig()
	if err != nil || !cfg.SyncCycleEnabled {
		t.Fatal("explicit cycle ignored", err)
	}
	t.Setenv("SYNC_CYCLE_ENABLED", "yes")
	if _, err = LoadConfig(); err == nil {
		t.Fatal("invalid cycle flag accepted")
	}
}
