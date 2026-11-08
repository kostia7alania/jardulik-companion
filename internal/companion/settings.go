package companion

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (s *Service) OpenSettings(ctx context.Context, owner int64) error {
	j, err := NewAutoliker(s).settings(ctx, owner)
	if err != nil {
		return err
	}
	b := func(text, action string) map[string]string {
		return map[string]string{"text": text, "callback_data": "as:" + j.Nonce + ":" + action}
	}
	rows := [][]map[string]string{
		{b("▶ Серия", "series"), b("♾ Авто", "automatic"), b("⏹ Выключить", "off")},
		{b("10 с", "interval:10"), b("30 с", "interval:30"), b("60 с", "interval:60")},
		{b("1 лайк", "count:1"), b("10", "count:10"), b("25", "count:25"), b("50", "count:50")},
		{b("Лимит 10", "cap:10"), b("20", "cap:20"), b("50", "cap:50"), b("100", "cap:100")},
		{b("🔎 Фильтры", "filters")},
	}
	text := s.AutolikeStatus(ctx, owner) + fmt.Sprintf("\nРазмер серии: %d.\nСначала ⭐ избранное, затем свежая выдача по фильтрам.\nМой лимит /24ч включает ручные и неопределённые отправки; это не квота Jardulik.\nСерия: /autolikes N (1..50). Лимит: /autolike_cap N (1..500).", j.Count)
	return s.say(ctx, owner, text, map[string]any{"inline_keyboard": rows})
}

// Settings dispatch runs under the Service's private-owner gate and userLock.
func (s *Service) HandleSettingsCallback(ctx context.Context, owner int64, data string) (bool, error) {
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
		return true, s.OpenSettings(ctx, owner)
	}
	a := NewAutoliker(s)
	j, err := a.settings(ctx, owner)
	if err != nil {
		return true, err
	}
	if parts[1] != j.Nonce {
		return true, errors.New("меню устарело; открой /settings")
	}
	if parts[2] == "filters" {
		return true, s.OpenFilters(ctx, owner)
	}
	if parts[2] == "series" || parts[2] == "automatic" {
		if err = a.startLocked(ctx, owner, parts[2], j.Count, j.Nonce); err != nil {
			return true, err
		}
		return true, s.OpenSettings(ctx, owner)
	}
	if len(parts) != 4 {
		return true, errors.New("неверная кнопка")
	}
	value, err := strconv.Atoi(parts[3])
	if err != nil {
		return true, errors.New("неверная настройка")
	}
	if err = a.updateSetting(ctx, owner, j.Nonce, parts[2], value); err != nil {
		return true, err
	}
	return true, s.OpenSettings(ctx, owner)
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
