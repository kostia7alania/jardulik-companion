package companion

import (
	"context"
	"strconv"
)

func (s *Service) showAgeIndex(ctx context.Context, owner int64, offset int) {
	if owner != s.Config.OwnerID || offset < 0 {
		return
	}
	page, err := s.Store.ArchiveAgeIndex(ctx, owner, offset)
	if err != nil {
		_ = s.say(ctx, owner, "Не удалось открыть архив по возрасту.", nil)
		return
	}
	var navigation []map[string]string
	if page.Offset > 0 {
		navigation = append(navigation, map[string]string{"text": "К началу", "callback_data": "ai:0"})
	}
	if page.HasMore {
		navigation = append(navigation, map[string]string{"text": "Дальше ➡️", "callback_data": "ai:" + strconv.Itoa(page.NextOffset)})
	}
	var markup any
	if len(navigation) > 0 {
		markup = map[string]any{"inline_keyboard": [][]map[string]string{navigation}}
	}
	_, _ = s.TG.SendText(ctx, owner, 0, page.Text, markup, page.Entities)
}
