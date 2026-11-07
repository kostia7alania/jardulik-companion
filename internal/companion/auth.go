package companion

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var telegramPublicKey, _ = hex.DecodeString("e7bf03a2fa4602af4580703d88dda5bb59f32ed8b02a56c187fe7d34caed242d")

// ParseInitData extracts launch data locally. A visitor-supplied URL is never fetched.
func ParseInitData(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) > 32*1024 {
		return "", errors.New("слишком длинная ссылка")
	}
	if strings.HasPrefix(input, "https://") || strings.HasPrefix(input, "http://") {
		u, err := url.Parse(input)
		if err != nil || u.Scheme != "https" || u.Host != "mini.jardulik.app" || u.User != nil {
			return "", errors.New("нужна ссылка mini.jardulik.app из Telegram Web")
		}
		// net/url already decodes Fragment. Parse its escaped form exactly once,
		// otherwise encoded ampersands inside initData become outer separators.
		q, err := url.ParseQuery(u.EscapedFragment())
		if err != nil {
			return "", errors.New("некорректный URL fragment")
		}
		if len(q["tgWebAppData"]) != 1 {
			return "", errors.New("в ссылке нет единственного tgWebAppData")
		}
		input = q.Get("tgWebAppData")
	}
	if input == "" {
		return "", errors.New("нет данных подключения")
	}
	return input, nil
}

func ValidateInitData(raw string, botID, expectedOwner int64, now time.Time, maxAge time.Duration) error {
	return validateInitData(raw, botID, expectedOwner, now, maxAge, telegramPublicKey)
}

func validateInitData(raw string, botID, expectedOwner int64, now time.Time, maxAge time.Duration, publicKey ed25519.PublicKey) error {
	if botID <= 0 {
		return errors.New("источник Jardulik ещё не настроен администратором")
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return errors.New("некорректные данные подключения")
	}
	for _, vs := range q {
		if len(vs) != 1 {
			return errors.New("повторяющиеся поля подключения")
		}
	}
	sig, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(q.Get("signature"), "="))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("нет подписи Telegram")
	}
	keys := make([]string, 0, len(q))
	for key := range q {
		if key != "hash" && key != "signature" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := []string{fmt.Sprintf("%d:WebAppData", botID)}
	for _, key := range keys {
		parts = append(parts, key+"="+q.Get(key))
	}
	if !ed25519.Verify(publicKey, []byte(strings.Join(parts, "\n")), sig) {
		return errors.New("подпись Jardulik не подтверждена")
	}
	authDate, err := strconv.ParseInt(q.Get("auth_date"), 10, 64)
	if err != nil || authDate > now.Add(2*time.Minute).Unix() || now.Sub(time.Unix(authDate, 0)) > maxAge {
		return errors.New("ссылка устарела: открой Jardulik заново и скопируй новую")
	}
	var user struct {
		ID int64 `json:"id"`
	}
	if json.Unmarshal([]byte(q.Get("user")), &user) != nil || user.ID != expectedOwner || user.ID <= 0 {
		return errors.New("ссылка принадлежит другому Telegram-аккаунту")
	}
	return nil
}

func tokenExpiry(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	_ = json.Unmarshal(b, &claims)
	return claims.Exp
}
