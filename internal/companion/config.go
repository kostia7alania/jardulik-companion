package companion

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const Version = "0.1.0"

type Config struct {
	DataDir, Token, APIBase, TelegramBase              string
	UserAgent                                          string
	OwnerID, SourceBotID, BackupChatID, BackupThreadID int64
	SessionKey, BackupKey                              []byte
	AllowReactions                                     bool
	ArchivePaused                                      bool
	MinFreeBytes                                       uint64
	InitMaxAge                                         time.Duration
	WatchInterval, WatchGap                            time.Duration
	WatchDailyLimit                                    int
}

func LoadConfig() (Config, error) {
	c := Config{DataDir: os.Getenv("APP_DATA_DIR"), Token: os.Getenv("TELEGRAM_BOT_TOKEN"), APIBase: "https://api.jardulik.app/api", TelegramBase: "https://api.telegram.org", MinFreeBytes: 2 << 30, InitMaxAge: 24 * time.Hour, WatchInterval: 24 * time.Hour, WatchGap: 20 * time.Second, WatchDailyLimit: 300}
	c.UserAgent = strings.TrimSpace(os.Getenv("JARDULIK_USER_AGENT"))
	if c.UserAgent == "" {
		c.UserAgent = defaultUserAgent
	}
	if len(c.UserAgent) > 1024 || strings.ContainsAny(c.UserAgent, "\r\n\x00") {
		return c, errors.New("invalid JARDULIK_USER_AGENT")
	}
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	c.DataDir, _ = filepath.Abs(c.DataDir)
	var err error
	for name, dst := range map[string]*int64{"OWNER_TELEGRAM_ID": &c.OwnerID, "JARDULIK_BOT_ID": &c.SourceBotID, "BACKUP_CHAT_ID": &c.BackupChatID, "BACKUP_THREAD_ID": &c.BackupThreadID} {
		if s := os.Getenv(name); s != "" {
			*dst, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				return c, fmt.Errorf("invalid %s", name)
			}
		}
	}
	if c.OwnerID <= 0 {
		return c, errors.New("OWNER_TELEGRAM_ID is required")
	}
	for name, dst := range map[string]*[]byte{"SESSION_KEY": &c.SessionKey, "BACKUP_KEY": &c.BackupKey} {
		*dst, err = base64.StdEncoding.DecodeString(strings.TrimSpace(os.Getenv(name)))
		if err != nil || len(*dst) != 32 {
			return c, fmt.Errorf("%s must be base64-encoded 32 bytes", name)
		}
	}
	c.AllowReactions = os.Getenv("ALLOW_REACTIONS") == "true"
	c.ArchivePaused = os.Getenv("ARCHIVE_WORKER_PAUSED") == "true"
	if s := os.Getenv("MIN_FREE_BYTES"); s != "" {
		c.MinFreeBytes, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return c, errors.New("invalid MIN_FREE_BYTES")
		}
	}
	return c, nil
}

func seal(key, plain []byte, purpose string) ([]byte, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	n := make([]byte, g.NonceSize())
	if _, err = rand.Read(n); err != nil {
		return nil, err
	}
	return g.Seal(n, n, plain, []byte(purpose)), nil
}

func unseal(key, sealed []byte, purpose string) ([]byte, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	if len(sealed) < g.NonceSize() {
		return nil, errors.New("invalid encrypted data")
	}
	return g.Open(nil, sealed[:g.NonceSize()], sealed[g.NonceSize():], []byte(purpose))
}

func sessionPurpose(owner int64) string { return fmt.Sprintf("jardulik-session:%d", owner) }
