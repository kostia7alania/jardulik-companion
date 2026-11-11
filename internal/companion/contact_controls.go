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

func (s *Service) privateProfileButtons(ctx context.Context, owner int64, p StoredProfile, full, jsonSource bool, copyID ...bool) any {
	markup := profileButtons(p, full, jsonSource).(map[string]any)
	rows := markup["inline_keyboard"].([][]any)
	if button := s.telegramContactButton(ctx, owner, p.ID); button != nil {
		rows = append(rows, []any{button})
	}
	if len(copyID) > 0 && copyID[0] {
		if button := profileCopyButton(p.ID); button != nil {
			rows = append(rows, []any{button})
		}
	}
	markup["inline_keyboard"] = rows
	return markup
}
