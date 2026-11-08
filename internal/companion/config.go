package companion

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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
	FullBackupEnabled                                  bool
	DataDir, Token, APIBase, TelegramBase              string
	UserAgent                                          string
	TelegramAPIID, TransferBytesPerSecond              int64
	TelegramAPIHash, TransferHelper, TransferSession   string
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
	c.TransferHelper = strings.TrimSpace(os.Getenv("TELEGRAM_TRANSFER_HELPER"))
	if c.TransferHelper == "" {
		c.TransferHelper = "/opt/jardulik-companion/current/telegram-transfer"
	}
	if !filepath.IsAbs(c.TransferHelper) || strings.ContainsAny(c.TransferHelper, "\r\n\x00") {
		return c, errors.New("invalid TELEGRAM_TRANSFER_HELPER")
	}
	c.TransferSession = strings.TrimSpace(os.Getenv("TELEGRAM_TRANSFER_SESSION_FILE"))
	if c.TransferSession == "" {
		c.TransferSession = filepath.Join(c.DataDir, "telegram-bot-session.enc")
	}
	c.TransferSession, _ = filepath.Abs(c.TransferSession)
	if relative, pathErr := filepath.Rel(c.DataDir, c.TransferSession); pathErr != nil || !filepath.IsLocal(relative) || relative == "." || strings.ContainsAny(c.TransferSession, "\r\n\x00") {
		return c, errors.New("invalid TELEGRAM_TRANSFER_SESSION_FILE")
	}
	if raw := strings.TrimSpace(os.Getenv("TELEGRAM_API_ID")); raw != "" {
		id, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil || id <= 0 {
			return c, errors.New("invalid TELEGRAM_API_ID")
		}
		c.TelegramAPIID = id
	}
	if raw := strings.TrimSpace(os.Getenv("TELEGRAM_TRANSFER_BYTES_PER_SECOND")); raw != "" {
		rate, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || rate < 0 || rate > 1<<30 {
			return c, errors.New("invalid TELEGRAM_TRANSFER_BYTES_PER_SECOND")
		}
		c.TransferBytesPerSecond = rate
	}
	c.TelegramAPIHash = strings.TrimSpace(os.Getenv("TELEGRAM_API_HASH"))
	if (c.TelegramAPIID == 0) != (c.TelegramAPIHash == "") {
		return c, errors.New("TELEGRAM_API_ID and TELEGRAM_API_HASH must be configured together")
	}
	if c.TelegramAPIHash != "" {
		if _, hashErr := hex.DecodeString(c.TelegramAPIHash); hashErr != nil || len(c.TelegramAPIHash) != 32 {
			return c, errors.New("invalid TELEGRAM_API_HASH")
		}
	}
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
	c.FullBackupEnabled = os.Getenv("FULL_BACKUP_TRANSFER_ENABLED") == "true"
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
