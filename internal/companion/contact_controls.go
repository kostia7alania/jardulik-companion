package companion

import (
	"context"
	"log/slog"
)

func (s *Service) telegramContactButton(ctx context.Context, owner, profile int64) map[string]string {
	contact, err := s.Store.ProfileTelegramContact(ctx, owner, profile)
	if err != nil {
		slog.Warn("saved Telegram contact unavailable", "kind", safeError(err))
		return nil
	}
	if contact == nil {
		return nil
	}
	return map[string]string{"text": "💬 Telegram", "url": contact.ContactURL}
}

func (s *Service) privateProfileButtons(ctx context.Context, owner int64, p StoredProfile, full, jsonSource bool) any {
	markup := profileButtons(p, full, jsonSource).(map[string]any)
	if button := s.telegramContactButton(ctx, owner, p.ID); button != nil {
		markup["inline_keyboard"] = append(markup["inline_keyboard"].([][]map[string]string), []map[string]string{button})
	}
	return markup
}
