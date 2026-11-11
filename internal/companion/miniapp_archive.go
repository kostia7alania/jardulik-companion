package companion

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
)

const archiveMiniAppAction = "am:"

func miniAppArchiveViewerURL(origin string, p StoredProfile) string {
	return origin + "/viewer?id=" + strconv.FormatInt(p.ID, 10) + "&versionId=" + strconv.FormatInt(p.VersionID, 10)
}

func (s *Service) miniAppArchiveZIP(ctx context.Context, p StoredProfile) (threadID, messageID int64, err error) {
	err = s.Store.DB.QueryRowContext(ctx, `SELECT r.thread_id,r.message_id
 FROM archive_versions v JOIN archive_receipts r ON r.source_sha=v.source_sha
 WHERE v.version_id=? AND v.owner_id=? AND v.profile_id=? AND v.thread_id>0 AND v.thread_id<>?
 AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=v.thread_id AND r.message_id>0 AND r.file_id<>''`,
		p.VersionID, s.Config.OwnerID, p.ID, s.Config.BackupThreadID, s.Config.BackupChatID).Scan(&threadID, &messageID)
	return
}

func (s *Service) archiveMiniAppCallback(ctx context.Context, c Callback, version int64) {
	owner, chat := s.Config.OwnerID, s.Config.BackupChatID
	origin := s.Config.MiniAppPublicOrigin
	if owner <= 0 || chat == 0 || c.From.ID != owner || c.Message.Chat.ID != chat || c.Message.ThreadID <= 0 || c.Message.ID <= 0 || version <= 0 {
		return
	}
	answer := func(text string) {
		_ = s.TG.Call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": c.ID, "text": text}, nil)
	}
	if origin == "" || validateMiniAppEndpoint(s.Config.MiniAppListen, origin) != nil {
		answer("Просмотр архива недоступен")
		return
	}
	p, err := s.Store.Version(ctx, owner, version)
	if err != nil {
		answer("Эта версия анкеты недоступна")
		return
	}
	thread, messageID, err := s.miniAppArchiveZIP(ctx, p)
	if err != nil || thread != c.Message.ThreadID || messageID != c.Message.ID {
		answer("Эта версия анкеты недоступна")
		return
	}
	keyboard := map[string]any{"inline_keyboard": [][]map[string]any{{
		{"text": "Открыть сохранённую анкету", "web_app": map[string]string{"url": miniAppArchiveViewerURL(origin, p)}},
	}}}
	text := fmt.Sprintf("Анкета #%d\nСохранено: %s", p.ID, displayTime(p.FirstSeen))
	message, err := s.TG.SendText(ctx, owner, 0, text, keyboard)
	if err != nil || message.ID <= 0 || message.Chat.ID != owner || message.ThreadID != 0 {
		slog.Warn("archive Mini App delivery unconfirmed")
		answer("Не удалось подтвердить отправку в личку")
		return
	}
	answer("Ссылка отправлена в личку")
}
