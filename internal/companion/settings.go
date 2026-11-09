package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (s *Service) OpenSettings(ctx context.Context, owner int64) error {
	return s.OpenSettingsMessage(ctx, owner, 0)
}

// OpenSettingsMessage refreshes the existing menu without changing the run or its settings.
func (s *Service) OpenSettingsMessage(ctx context.Context, owner, messageID int64) error {
	return s.showSettings(ctx, owner, messageID, "", "")
}

func (s *Service) editMenuMessage(ctx context.Context, owner, messageID int64, text string, keyboard any) error {
	if messageID <= 0 {
		return s.say(ctx, owner, text, keyboard)
	}
	var out SentMessage
	err := s.TG.Call(ctx, "editMessageText", map[string]any{
		"chat_id": owner, "message_id": messageID, "text": text,
		"reply_markup": keyboard, "link_preview_options": map[string]bool{"is_disabled": true},
	}, &out)
	var te *TelegramError
	if errors.As(err, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
		return nil
	}
	if err != nil {
		return err
	}
	if out.ID != messageID || out.Chat.ID != owner {
		return errors.New("Telegram не подтвердил обновление меню")
	}
	return nil
}

func (s *Service) showSettings(ctx context.Context, owner, messageID int64, panel, notice string) error {
	j, err := NewAutoliker(s).settings(ctx, owner)
	if err != nil {
		return err
	}
	b := func(text, action string) map[string]string {
		return map[string]string{"text": text, "callback_data": "as:" + j.Nonce + ":" + action}
	}
	choice := func(label, key string, value, selected int) map[string]string {
		if value == selected {
			label = "✅ " + label
		}
		return b(label, key+":"+strconv.Itoa(value))
	}
	mode := map[string]string{"off": "выключен", "series": "серия", "automatic": "авто"}[j.Mode]
	text := "Автолайкер: " + mode
	var rows [][]map[string]string
	switch panel {
	case "interval":
		text += fmt.Sprintf("\n\nИнтервал между лайками: %d с", j.Interval)
		rows = [][]map[string]string{{
			choice("10 с", "interval", 10, j.Interval), choice("30 с", "interval", 30, j.Interval), choice("60 с", "interval", 60, j.Interval),
		}}
	case "count":
		text += fmt.Sprintf("\n\nРазмер серии: %s\nПосле серии бот останавливается. Изменение размера действует для следующей серии.", settingsLikeCount(j.Count))
		rows = [][]map[string]string{{
			choice("1 лайк", "count", 1, j.Count), choice("10 лайков", "count", 10, j.Count),
		}, {
			choice("25 лайков", "count", 25, j.Count), choice("50 лайков", "count", 50, j.Count),
		}}
		if j.Count != 1 && j.Count != 10 && j.Count != 25 && j.Count != 50 {
			rows = append(rows, []map[string]string{choice(settingsLikeCount(j.Count), "count", j.Count, j.Count)})
		}
	case "cap":
		text += fmt.Sprintf("\n\nМой лимит: %s /24ч\nУчитывает ручные лайки и отправки с неизвестным результатом. Лимит Jardulik учитывается отдельно.", settingsLikeCount(j.DailyCap))
		rows = [][]map[string]string{{
			choice("10 /24ч", "cap", 10, j.DailyCap), choice("20 /24ч", "cap", 20, j.DailyCap),
		}, {
			choice("50 /24ч", "cap", 50, j.DailyCap), choice("100 /24ч", "cap", 100, j.DailyCap),
		}}
		if j.DailyCap != 10 && j.DailyCap != 20 && j.DailyCap != 50 && j.DailyCap != 100 {
			rows = append(rows, []map[string]string{choice(fmt.Sprintf("%d /24ч", j.DailyCap), "cap", j.DailyCap, j.DailyCap)})
		}
	default:
		panel = ""
		if j.Mode == "series" {
			text += fmt.Sprintf("\nОтправлено: %d из %d", j.Sent, j.Limit)
		} else if j.Mode != "off" {
			text += fmt.Sprintf("\nОтправлено за запуск: %d", j.Sent)
		}
		text += "\n\n⭐ Избранное → анкеты по фильтрам. Только анкеты 18+.\nАвто продолжает после снятия лимита. Серия заканчивается после выбранного числа лайков."
		var sourceState, sourceReason string
		var paused bool
		err := s.Store.DB.QueryRowContext(ctx, `SELECT COALESCE(s.state,''),COALESCE(s.last_error,''),u.paused FROM users u LEFT JOIN sessions s ON s.owner_id=u.id WHERE u.id=?`, owner).Scan(&sourceState, &sourceReason, &paused)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if sourceState != "connected" {
			switch {
			case sourceReason == "unexpected_response_or_challenge":
				text += "\n\nПодключение Jardulik приостановлено после необычного ответа. Подключи аккаунт заново."
			case sourceState == "reconnect_required":
				text += "\n\nПодключение Jardulik истекло. Подключи аккаунт заново."
			default:
				text += "\n\nJardulik не подключён."
			}
			rows = append(rows, []map[string]string{{"text": "🔑 Подключить Jardulik", "callback_data": "connect_help"}})
		} else if paused {
			text += "\n\n⏸ Действия приостановлены."
		} else if !s.Config.AllowReactions || !s.Store.cfg.AllowReactions {
			text += "\n\nОтправка лайков сейчас выключена."
		} else if j.Mode == "off" {
			rows = append(rows, []map[string]string{b("▶ Запустить авто", "automatic")}, []map[string]string{b("▶ Серия · "+settingsLikeCount(j.Count), "series")})
		}
		if filters, err := NewAutoliker(s).filters(ctx, owner); err == nil {
			place := filterCountryLabel(filters.Country)
			if filters.Country == "" {
				place = "Любая страна"
			}
			text += fmt.Sprintf("\n\n🔎 %d-%d лет · %d-%d см\n📍 %s", filters.AgeMin, filters.AgeMax, filters.HeightMin, filters.HeightMax, place)
			if filters.City != "" {
				text += ", " + filters.City
			}
		} else {
			text += "\n\nФильтры недоступны до подключения Jardulik."
		}
		rows = append(rows,
			[]map[string]string{b(fmt.Sprintf("⏱ Интервал · %d с", j.Interval), "view:interval"), b(fmt.Sprintf("🔢 Серия · %d", j.Count), "view:count")},
			[]map[string]string{b(fmt.Sprintf("🧮 Мой лимит · %d /24ч", j.DailyCap), "view:cap")},
			[]map[string]string{b("🔎 Фильтры", "filters"), b("↻ Обновить", "refresh")})
		if j.Reason != "" {
			text += "\n\n" + autoReason(j.Reason)
		}
		if j.Mode != "off" && j.NextAttempt > time.Now().UnixMilli() {
			text += "\nСледующая попытка: " + displayTime(time.UnixMilli(j.NextAttempt).UTC().Format(time.RFC3339))
		}
	}
	if j.Mode != "off" {
		rows = append([][]map[string]string{{b("⏹ Остановить", "off")}}, rows...)
	}
	if panel != "" {
		rows = append(rows, []map[string]string{b("← Автолайкер", "home")})
	}
	if notice != "" {
		text += "\n\n" + notice
	}
	return s.editMenuMessage(ctx, owner, messageID, text, map[string]any{"inline_keyboard": rows})
}

func settingsLikeCount(n int) string {
	word := "лайков"
	if n%100 < 11 || n%100 > 14 {
		switch n % 10 {
		case 1:
			word = "лайк"
		case 2, 3, 4:
			word = "лайка"
		}
	}
	return fmt.Sprintf("%d %s", n, word)
}

// Settings dispatch runs under the Service's private-owner gate and userLock.
func (s *Service) HandleSettingsCallback(ctx context.Context, owner int64, data string) (bool, error) {
	return s.handleSettingsCallback(ctx, owner, 0, data)
}

func (s *Service) HandleSettingsMessageCallback(ctx context.Context, owner, messageID int64, data string) (bool, error) {
	return s.handleSettingsCallback(ctx, owner, messageID, data)
}

func (s *Service) handleSettingsCallback(ctx context.Context, owner, messageID int64, data string) (bool, error) {
	if !strings.HasPrefix(data, "as:") {
		return false, nil
	}
	parts := strings.Split(data, ":")
	if len(parts) < 3 || len(parts) > 4 {
		return true, errors.New("неверная кнопка")
	}
	if parts[2] == "off" {
		if err := s.DisableAutolike(ctx, owner, "user_off"); err != nil {
			return true, err
		}
		return true, s.OpenSettingsMessage(ctx, owner, messageID)
	}
	a := NewAutoliker(s)
	j, err := a.settings(ctx, owner)
	if err != nil {
		return true, err
	}
	if parts[1] != j.Nonce {
		if messageID <= 0 {
			return true, errors.New("меню устарело; открой /settings")
		}
		notice := "Меню обновлено. Выбери действие ещё раз."
		if parts[2] == "series" || parts[2] == "automatic" {
			notice = "Настройки изменились. Для запуска нажми кнопку ещё раз."
		}
		return true, s.showSettings(ctx, owner, messageID, "", notice)
	}
	switch parts[2] {
	case "home", "refresh":
		return true, s.OpenSettingsMessage(ctx, owner, messageID)
	case "view":
		if len(parts) != 4 || (parts[3] != "interval" && parts[3] != "count" && parts[3] != "cap") {
			return true, errors.New("неверная кнопка")
		}
		return true, s.showSettings(ctx, owner, messageID, parts[3], "")
	case "filters":
		return true, s.OpenFiltersMessage(ctx, owner, messageID)
	case "series", "automatic":
		if err = a.startLocked(ctx, owner, parts[2], j.Count, j.Nonce); err != nil {
			if messageID > 0 {
				return true, s.showSettings(ctx, owner, messageID, "", "Не удалось запустить: "+err.Error())
			}
			return true, err
		}
		return true, s.OpenSettingsMessage(ctx, owner, messageID)
	}
	if len(parts) != 4 {
		return true, errors.New("неверная кнопка")
	}
	value, err := strconv.Atoi(parts[3])
	if err != nil {
		return true, errors.New("неверная настройка")
	}
	current := map[string]int{"interval": j.Interval, "count": j.Count, "cap": j.DailyCap}
	if selected, ok := current[parts[2]]; !ok || selected != value {
		if err = a.updateSetting(ctx, owner, j.Nonce, parts[2], value); err != nil {
			return true, err
		}
	}
	return true, s.showSettings(ctx, owner, messageID, parts[2], "")
}
func (a *Autoliker) updateSetting(ctx context.Context, owner int64, nonce, key string, value int) error {
	column := ""
	switch key {
	case "interval":
		if value != 10 && value != 30 && value != 60 {
			return errors.New("интервал: 10, 30 или 60 секунд")
		}
		column = "interval_seconds"
	case "count":
		if value < 1 || value > 50 {
			return errors.New("размер серии: 1..50")
		}
		column = "series_count"
	case "cap":
		if value < 1 || value > 500 {
			return errors.New("мой лимит: 1..500")
		}
		column = "daily_cap"
	default:
		return errors.New("неверная настройка")
	}
	next, err := filterNonce()
	if err != nil {
		return err
	}
	res, err := a.Service.Store.DB.ExecContext(ctx, "UPDATE autolike_settings SET "+column+"=?,nonce=? WHERE owner_id=? AND nonce=?", value, next, owner, nonce)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("меню устарело; открой /settings")
	}
	return nil
}
func (s *Service) HandleAutolikeCommand(ctx context.Context, owner int64, text string) (bool, error) {
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return false, nil
	}
	command := strings.Split(parts[0], "@")[0]
	switch command {
	case "/settings", "/autolike":
		return true, s.OpenSettings(ctx, owner)
	case "/autolike_off":
		if err := s.DisableAutolike(ctx, owner, "user_off"); err != nil {
			return true, err
		}
		return true, s.OpenSettings(ctx, owner)
	case "/autolikes":
		if len(parts) != 2 {
			return true, errors.New("/autolikes N, где N от 1 до 50")
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil {
			return true, errors.New("размер серии: 1..50")
		}
		if err = NewAutoliker(s).startLocked(ctx, owner, "series", count, ""); err != nil {
			return true, err
		}
		return true, s.OpenSettings(ctx, owner)
	case "/autolike_cap":
		if len(parts) != 2 {
			return true, errors.New("/autolike_cap N, где N от 1 до 500")
		}
		value, err := strconv.Atoi(parts[1])
		if err != nil {
			return true, errors.New("мой лимит: 1..500")
		}
		a := NewAutoliker(s)
		j, err := a.settings(ctx, owner)
		if err != nil {
			return true, err
		}
		if err = a.updateSetting(ctx, owner, j.Nonce, "cap", value); err != nil {
			return true, err
		}
		return true, s.OpenSettings(ctx, owner)
	}
	return false, nil
}
