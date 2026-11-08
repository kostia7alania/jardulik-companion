package companion

import (
	"context"
	"errors"
	"strings"
	"time"
)

func (s *Service) sendCurrentProfileCard(ctx context.Context, owner int64, p StoredProfile) ([]int64, error) {
	if !s.Config.RichCards {
		return s.sendProfileCard(ctx, owner, p)
	}
	jobs, err := s.Store.MediaJobs(ctx, owner, p.VersionID)
	if err != nil {
		return nil, err
	}
	card, err := BuildRichProfileCard(p, jobs, s.profilePostURL(ctx, owner, p))
	if errors.Is(err, ErrRichCardTooLarge) {
		return s.sendProfileCard(ctx, owner, p)
	}
	if err != nil {
		return nil, err
	}
	m, err := s.TG.SendRichProfileCard(ctx, owner, 0, card, s.privateProfileButtons(ctx, owner, p, len(photoExifBlocks(selectedProfilePhotos(jobs))) > 0, false))
	var ids []int64
	if m.ID > 0 {
		ids = []int64{m.ID}
	}
	if err != nil {
		return ids, err
	}
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelWrite()
	return ids, recordRichCard(writeCtx, s.Store, owner, m.ID, owner, p.VersionID, 0, m.Rich)
}

func (a *Archiver) sendCurrentProfileCard(ctx context.Context, p StoredProfile, jobs []MediaJob, thread int64, save func([]int64) error) ([]int64, error) {
	if !a.Config.RichCards {
		return a.sendProfileCard(ctx, p, jobs, thread, save)
	}
	card, err := BuildRichProfileCard(p, jobs, forumTopicURL(a.Config.BackupChatID, thread))
	if errors.Is(err, ErrRichCardTooLarge) {
		return a.sendProfileCard(ctx, p, jobs, thread, save)
	}
	if err != nil {
		return nil, err
	}
	var m SentMessage
	err = a.groupCall(ctx, func() error {
		var sendErr error
		m, sendErr = a.TG.SendRichProfileCard(ctx, a.Config.BackupChatID, thread, card, nil)
		return sendErr
	})
	var ids []int64
	if m.ID > 0 {
		ids = []int64{m.ID}
		if saveErr := save(ids); saveErr != nil {
			return ids, errors.Join(err, saveErr)
		}
	}
	if err != nil {
		return ids, err
	}
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelWrite()
	var owner int64
	if err := a.Store.DB.QueryRowContext(writeCtx, "SELECT owner_id FROM versions WHERE id=?", p.VersionID).Scan(&owner); err != nil {
		return ids, err
	}
	return ids, recordRichCard(writeCtx, a.Store, a.Config.BackupChatID, m.ID, owner, p.VersionID, thread, m.Rich)
}

// Existing rich cards are edited using confirmed Telegram file IDs, without uploads.
func richProfileEdit(ctx context.Context, store *Store, tg *Telegram, owner, chat, thread, message int64, p StoredProfile, jobs []MediaJob, link string, keyboard ...any) (bool, error) {
	marker, err := loadRichCardMarker(ctx, store, chat, message)
	if err != nil || marker == nil {
		return marker != nil || err != nil, err
	}
	if marker.OwnerID != owner || marker.VersionID != p.VersionID || marker.ThreadID != thread {
		return true, errors.New("rich card receipt owner or version changed")
	}
	card, err := BuildRichProfileCard(p, jobs, link)
	if err != nil {
		return true, err
	}
	content, err := card.WithPhotoFileIDs(marker.PhotoFileIDs)
	if err != nil {
		return true, err
	}
	var out SentMessage
	payload := map[string]any{"chat_id": chat, "message_id": message, "rich_message": content}
	if len(keyboard) > 0 {
		payload["reply_markup"] = keyboard[0]
	}
	err = tg.Call(ctx, "editMessageText", payload, &out)
	var te *TelegramError
	if errors.As(err, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if out.ID != message || out.Chat.ID != chat || (thread > 0 && out.ThreadID != thread) {
		return true, errors.New("rich card edit unconfirmed")
	}
	return true, nil
}
