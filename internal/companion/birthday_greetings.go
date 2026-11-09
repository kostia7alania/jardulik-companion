package companion

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

//go:embed greetings/birthday_ru.json
var birthdayGreetingJSON []byte

var birthdayGreetingSigns = [...]string{
	"aries", "taurus", "gemini", "cancer", "leo", "virgo", "libra", "scorpio", "sagittarius", "capricorn", "aquarius", "pisces", "neutral",
}

var birthdayGreetingThemes = [...]string{
	"spontaneity", "coziness", "humor", "creativity", "curiosity", "values", "travel", "music", "dream", "conversation", "food", "small_joys",
}

type birthdayGreetingTemplate struct {
	ID    string `json:"id"`
	Sign  string `json:"sign"`
	Theme string `json:"theme"`
	Text  string `json:"text"`
}

var birthdayGreetingUsername = regexp.MustCompile(`^[A-Za-z0-9_]{5,32}$`)
var birthdayGreetingTemplates, birthdayGreetingCatalogError = parseBirthdayGreetingCatalog(birthdayGreetingJSON)

func parseBirthdayGreetingCatalog(raw []byte) ([]birthdayGreetingTemplate, error) {
	var templates []birthdayGreetingTemplate
	if err := json.Unmarshal(raw, &templates); err != nil {
		return nil, fmt.Errorf("не удалось прочитать приветствия: %w", err)
	}
	if len(templates) != len(birthdayGreetingSigns)*len(birthdayGreetingThemes) {
		return nil, errors.New("неполный каталог приветствий")
	}
	ids, texts := map[string]bool{}, map[string]bool{}
	for i, template := range templates {
		sign, theme := birthdayGreetingSigns[i/len(birthdayGreetingThemes)], birthdayGreetingThemes[i%len(birthdayGreetingThemes)]
		if template.ID != fmt.Sprintf("%s_%02d", sign, i%len(birthdayGreetingThemes)+1) || template.Sign != sign || template.Theme != theme || ids[template.ID] || texts[template.Text] {
			return nil, errors.New("нарушен порядок или уникальность приветствий")
		}
		ids[template.ID], texts[template.Text] = true, true
		if !strings.HasPrefix(template.Text, "{name}, привет! ") || strings.Count(template.Text, "{name}") != 1 || strings.Count(template.Text, "@{telegram_username}") != 1 || strings.Count(template.Text, "{telegram_username}") != 1 || strings.Count(template.Text, "@") != 1 {
			return nil, errors.New("неверные подстановки приветствия")
		}
		rest := strings.NewReplacer("{name}", "", "{telegram_username}", "").Replace(template.Text)
		if strings.ContainsAny(rest, "{}") {
			return nil, errors.New("неизвестная подстановка приветствия")
		}
		if err := validateReactionMessage(renderBirthdayGreeting(template.Text, strings.Repeat("Ж", 64), strings.Repeat("a", 32))); err != nil {
			return nil, err
		}
	}
	return templates, nil
}

// The date chooses an editorial theme, not a natal chart or a personality claim.
// This helper only reads the frozen profile; sending and intent persistence belong to the caller.
func birthdayReactionMessage(profile Profile, telegramUsername string, variant int64) (string, int64, error) {
	age, known := autoInteger(profile["age"])
	if !known || age < 18 || age > 111 {
		return "", -1, ErrReactionMessageTargetAge
	}
	if !birthdayGreetingUsername.MatchString(telegramUsername) {
		return "", -1, errors.New("укажи Telegram username: от 5 до 32 латинских букв, цифр или подчёркиваний, без @")
	}
	if variant < 0 {
		return "", -1, errors.New("неверный вариант приветствия")
	}
	if birthdayGreetingCatalogError != nil {
		return "", -1, birthdayGreetingCatalogError
	}
	asOf := time.Now().UTC()
	if birthdayGreetingMinorDOB(profile, asOf) {
		return "", -1, ErrReactionMessageTargetAge
	}
	sign := birthdayGreetingSign(profile, asOf)
	slot := int(variant % int64(len(birthdayGreetingThemes)))
	for i, name := range birthdayGreetingSigns {
		if name == sign {
			slot += i * len(birthdayGreetingThemes)
			break
		}
	}
	message := renderBirthdayGreeting(birthdayGreetingTemplates[slot].Text, birthdayGreetingName(profile), telegramUsername)
	if err := validateReactionMessage(message); err != nil {
		return "", -1, err
	}
	return message, int64(slot), nil
}

func renderBirthdayGreeting(text, name, telegramUsername string) string {
	if name == "" {
		text = strings.Replace(text, "{name}, привет!", "Привет!", 1)
	} else {
		text = strings.Replace(text, "{name}", name, 1)
	}
	return strings.Replace(text, "{telegram_username}", telegramUsername, 1)
}

func birthdayGreetingName(profile Profile) string {
	for _, key := range []string{"firstName", "name"} {
		name, ok := profile[key].(string)
		if !ok || !utf8.ValidString(name) {
			continue
		}
		name = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) && !unicode.IsSpace(r) || r == '{' || r == '}' {
				return -1
			}
			return r
		}, name)
		name = strings.Join(strings.Fields(name), " ")
		if !meaningfulProfileValue(name) {
			continue
		}
		name, _ = telegramPrefix(name, 64)
		return strings.TrimSpace(name)
	}
	return ""
}

func birthdayGreetingDate(profile Profile) (time.Time, bool) {
	// Reuse the calendar-date validator, including its timestamp validation, without shifting zones.
	if profileBirthday(profile) == "" {
		return time.Time{}, false
	}
	raw := strings.TrimSpace(profile["birthday"].(string))
	date, err := time.Parse("2006-01-02", raw[:10])
	return date, err == nil
}

func birthdayGreetingMinorDOB(profile Profile, asOf time.Time) bool {
	date, known := birthdayGreetingDate(profile)
	today := time.Date(asOf.Year(), asOf.Month(), asOf.Day(), 0, 0, 0, 0, time.UTC)
	if !known || date.After(today) {
		return false
	}
	age := today.Year() - date.Year()
	if today.Month() < date.Month() || today.Month() == date.Month() && today.Day() < date.Day() {
		age--
	}
	return age < 18
}

func birthdayGreetingSign(profile Profile, asOf time.Time) string {
	date, known := birthdayGreetingDate(profile)
	if !known || date.After(time.Date(asOf.Year(), asOf.Month(), asOf.Day(), 0, 0, 0, 0, time.UTC)) {
		return "neutral"
	}
	day := time.Date(2000, date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	boundaries := [...]struct {
		month time.Month
		day   int
		sign  string
	}{
		{time.January, 20, "aquarius"}, {time.February, 19, "pisces"}, {time.March, 21, "aries"},
		{time.April, 20, "taurus"}, {time.May, 21, "gemini"}, {time.June, 21, "cancer"},
		{time.July, 23, "leo"}, {time.August, 23, "virgo"}, {time.September, 23, "libra"},
		{time.October, 23, "scorpio"}, {time.November, 22, "sagittarius"}, {time.December, 22, "capricorn"},
	}
	sign := "capricorn"
	for _, boundary := range boundaries {
		start := time.Date(2000, boundary.month, boundary.day, 0, 0, 0, 0, time.UTC)
		// Conservative editorial guard, not an astronomical guarantee: use neutral within two days.
		if !day.Before(start.AddDate(0, 0, -2)) && !day.After(start.AddDate(0, 0, 2)) {
			return "neutral"
		}
		if !day.Before(start) {
			sign = boundary.sign
		}
	}
	return sign
}
