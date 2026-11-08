package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

func profileButtons(p StoredProfile, full, jsonSource bool) any {
	rows := [][]map[string]string{{{"text": "В избранное", "callback_data": fmt.Sprintf("f:%d", p.ID)}, {"text": "История", "callback_data": fmt.Sprintf("h:%d", p.ID)}}}
	var actions []map[string]string
	if full {
		actions = append(actions, map[string]string{"text": "Полный текст", "callback_data": fmt.Sprintf("t:%d", p.ID)})
	}
	actions = append(actions, map[string]string{"text": "Следующая анкета", "callback_data": "next"})
	rows = append(rows, actions)
	if jsonSource {
		rows = append(rows, []map[string]string{{"text": "JSON Jardulik", "callback_data": fmt.Sprintf("jv:%d", p.VersionID)}})
	}
	return map[string]any{"inline_keyboard": rows}
}

// Each profile is a captioned photo/album, without an automatic second text post.
func (s *Service) sendProfileCard(ctx context.Context, owner int64, p StoredProfile) ([]int64, error) {
	jobs, err := s.Store.MediaJobs(ctx, owner, p.VersionID)
	if err != nil {
		return nil, err
	}
	photos := selectedProfilePhotos(jobs)
	card, err := s.renderPrivateCard(ctx, owner, p, jobs, photos)
	if err != nil {
		return nil, err
	}
	if len(photos) == 0 {
		m, e := s.TG.SendText(ctx, owner, 0, card.Text, profileButtons(p, card.Truncated, !card.SourceJSONInline), card.Entities)
		if e != nil {
			return nil, e
		}
		if m.ID <= 0 || m.Chat.ID != owner {
			return nil, errors.New("unconfirmed profile message")
		}
		return []int64{m.ID}, nil
	}
	buttons := profileButtons(p, card.Truncated || len(photoExifBlocks(photos)) > 0, !card.SourceJSONInline)
	caption := card.Text
	var ids []int64
	for start := 0; start < len(photos); {
		end := start
		for end < len(photos) && end-start < 10 && photos[end].Size <= 9<<20 {
			end++
		}
		if end-start >= 2 {
			var paths []string
			for _, j := range photos[start:end] {
				paths = append(paths, j.Path)
			}
			groupCaption := ""
			if start == 0 {
				groupCaption = caption
			}
			var entities []MessageEntity
			if start == 0 {
				entities = card.Entities
			}
			messages, e := s.TG.SendAlbum(ctx, owner, 0, paths, groupCaption, entities)
			for _, m := range messages {
				if m.ID > 0 {
					ids = append(ids, m.ID)
				}
			}
			if e != nil {
				return ids, e
			} else {
				start = end
				continue
			}
		}
		j := photos[start]
		if j.Size > 9<<20 {
			return ids, errors.New("исходное фото превышает размер для отправки как фото; оригинал сохранён в архиве")
		}
		singleCaption := ""
		var keyboard any
		if start == 0 {
			singleCaption = caption
			keyboard = buttons
		}
		var m SentMessage
		var entities []MessageEntity
		if start == 0 {
			entities = card.Entities
		}
		m, err = s.TG.SendPhotoCard(ctx, owner, 0, j.Path, singleCaption, keyboard, entities)
		if err != nil {
			return ids, err
		}
		if m.ID <= 0 || m.Chat.ID != owner {
			return ids, errors.New("unconfirmed profile media")
		}
		ids = append(ids, m.ID)
		start++
		if start < len(photos) && !sleep(ctx, 1100*time.Millisecond) {
			return ids, ctx.Err()
		}
	}
	return ids, nil
}

func (s *Service) fullText(ctx context.Context, chat int64, p StoredProfile) {
	var owner int64
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT owner_id FROM versions WHERE id=? AND profile_id=?", p.VersionID, p.ID).Scan(&owner); err != nil {
		return
	}
	if chat != owner && chat != s.Config.OwnerID {
		return
	}
	jobs, err := s.Store.MediaJobs(ctx, owner, p.VersionID)
	if err != nil {
		return
	}
	parts, err := fullExifTextParts(s.cardText(ctx, chat, p), selectedProfilePhotos(jobs))
	if err == nil {
		err = s.sendKnownText(ctx, chat, p.VersionID, "full_text", parts)
	}
	if err != nil {
		slog.Warn("full text delivery unavailable", "kind", safeError(err))
	}
}

func (s *Service) profilePostURL(ctx context.Context, recipient int64, p StoredProfile) string {
	if recipient != s.Config.OwnerID {
		return ""
	}
	var thread int64
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT thread_id FROM mirror_topics WHERE profile_id=?", p.ID).Scan(&thread); err != nil {
		return ""
	}
	return forumTopicURL(s.Config.BackupChatID, thread)
}
func (s *Service) cardText(ctx context.Context, owner int64, p StoredProfile) string {
	text := profileText(p)
	if link := s.profilePostURL(ctx, owner, p); link != "" {
		text += "\n📍 " + link
	}
	return text
}
func (s *Service) renderPrivateCard(ctx context.Context, owner int64, p StoredProfile, jobs, photos []MediaJob) (photoText, error) {
	limit := 1024
	if len(photos) == 0 {
		limit = 4096
	}
	return renderProfileCard(p, photos, unavailableMediaCount(jobs), len(photos) == 0, s.profilePostURL(ctx, owner, p), limit, true)
}

// Operator archive callbacks are accepted only from the configured owner in
// the configured private backup group. They never alter tenant authorizations.
func (s *Service) archiveCallback(ctx context.Context, c Callback) {
	if c.From.ID != s.Config.OwnerID || c.Message.Chat.ID != s.Config.BackupChatID || s.Config.BackupChatID == 0 {
		return
	}
	_ = s.TG.Call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": c.ID}, nil)
	parts := strings.Split(c.Data, ":")
	if len(parts) != 2 {
		return
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || id <= 0 {
		return
	}
	var owner int64
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT owner_id FROM versions WHERE id=?", id).Scan(&owner); err != nil {
		return
	}
	p, err := s.Store.Version(ctx, owner, id)
	if err != nil {
		return
	}
	if parts[0] == "aj" {
		s.sourceJSON(ctx, s.Config.OwnerID, owner, id)
		return
	}
	if parts[0] == "av" {
		s.fullText(ctx, s.Config.OwnerID, p)
		return
	}
	if parts[0] != "az" {
		return
	}
	var chat, message int64
	if err = s.Store.DB.QueryRowContext(ctx, `SELECT r.chat_id,r.message_id FROM archive_versions av JOIN archive_receipts r ON r.source_sha=av.source_sha WHERE av.version_id=? AND r.kind='profile_version'`, id).Scan(&chat, &message); err != nil || chat != s.Config.BackupChatID || message <= 0 {
		return
	}
	var out struct {
		ID int64 `json:"message_id"`
	}
	if err = s.TG.Call(ctx, "copyMessage", map[string]any{"chat_id": s.Config.OwnerID, "from_chat_id": chat, "message_id": message}, &out); err != nil {
		slog.Warn("archive original delivery unavailable", "kind", safeError(err))
	}
}

// RepairPrivateCards replaces only our previously confirmed profile deliveries.
// The new card must be confirmed and durably recorded before old messages go.
func (s *Service) RepairPrivateCards(ctx context.Context) error {
	rows, err := s.Store.DB.QueryContext(ctx, "SELECT id,owner_id,version_id,message_ids FROM deliveries WHERE state='sent'")
	if err != nil {
		return err
	}
	type repair struct {
		id, owner, version int64
		raw                string
	}
	var jobs []repair
	for rows.Next() {
		var j repair
		if err = rows.Scan(&j.id, &j.owner, &j.version, &j.raw); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		mu := s.userLock(j.owner)
		mu.Lock()
		err = s.repairPrivateCard(ctx, j.id, j.owner, j.version, j.raw)
		mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) repairPrivateCard(ctx context.Context, deliveryID, owner, version int64, raw string) error {
	j := struct {
		id, owner, version int64
		raw                string
	}{deliveryID, owner, version, raw}
	key := fmt.Sprintf("repaired_card:%d", j.id)
	if s.Store.Meta(ctx, fmt.Sprintf("card_layout:%d", j.id)) == "v2" {
		return nil
	}
	if saved := s.Store.Meta(ctx, key); saved != "" {
		var record privateCardRepair
		if json.Unmarshal([]byte(saved), &record) == nil && record.State == "complete" {
			return nil
		}
		return fmt.Errorf("private card repair %d needs reconciliation; stored original and replacement IDs retained", j.id)
	}
	var old []int64
	if json.Unmarshal([]byte(j.raw), &old) != nil || len(old) == 0 {
		return errors.New("invalid original profile message receipt")
	}
	p, e := s.Store.Version(ctx, j.owner, j.version)
	if e != nil {
		return e
	}
	record := privateCardRepair{State: "sending", OldIDs: old}
	if err := s.savePrivateRepair(ctx, key, record); err != nil {
		return err
	}
	ids, e := s.sendProfileCard(ctx, j.owner, p)
	if e != nil {
		record.State = "uncertain"
		record.NewIDs = ids
		_ = s.savePrivateRepair(context.WithoutCancel(ctx), key, record)
		return fmt.Errorf("card repair unconfirmed; do not retry automatically: %w", e)
	}
	b, _ := json.Marshal(ids)
	record.State = "deleting"
	record.NewIDs = ids
	receipt, _ := json.Marshal(record)
	tx, e := s.Store.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE deliveries SET message_ids=? WHERE id=?", string(b), j.id); e == nil {
		_, e = tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key=?", string(receipt), key)
	}
	if e != nil {
		_ = tx.Rollback()
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	for _, id := range old {
		if err := s.TG.Call(ctx, "deleteMessage", map[string]any{"chat_id": j.owner, "message_id": id}, nil); err != nil {
			return fmt.Errorf("new card delivered, old message cleanup incomplete: %w", err)
		}
		record.DeletedIDs = append(record.DeletedIDs, id)
		if err := s.savePrivateRepair(ctx, key, record); err != nil {
			return err
		}
	}
	record.State = "complete"
	if err := s.savePrivateRepair(ctx, key, record); err != nil {
		return err
	}
	return nil
}

func (s *Service) RequestPrivateCardRepair(ctx context.Context) error {
	r, err := s.Store.DB.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('private_card_repair','pending') ON CONFLICT(key) DO UPDATE SET value='pending' WHERE meta.value='done'`)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n > 0 || s.Store.Meta(ctx, "private_card_repair") == "pending" {
		return nil
	}
	return errors.New("previous private-card repair needs reconciliation")
}

func (s *Service) runPrivateCardRepair(ctx context.Context) {
	if s.Store.Meta(ctx, "private_card_repair") == "processing" {
		_ = s.Store.SetMeta(ctx, "private_card_repair", "uncertain")
	}
	for ctx.Err() == nil {
		r, err := s.Store.DB.ExecContext(ctx, "UPDATE meta SET value='processing' WHERE key='private_card_repair' AND value='pending'")
		if err == nil {
			if n, _ := r.RowsAffected(); n == 1 {
				state := "done"
				text := "Старые личные карточки объединены: фото и описание вместе."
				if err = s.RepairPrivateCards(ctx); err != nil {
					state = "uncertain"
					text = "Объединение личных карточек требует проверки. Старые сообщения сохранены, неопределённые отправки не повторяем."
					slog.Warn("private card repair incomplete", "kind", safeError(err))
				}
				if state == "done" {
					if err = s.RefreshCardCaptions(ctx); err != nil {
						state = "uncertain"
						text = "Обновление подписей требует проверки."
					}
				}
				_ = s.Store.SetMeta(context.WithoutCancel(ctx), "private_card_repair", state)
				if state != "done" {
					_ = s.say(ctx, s.Config.OwnerID, text, nil)
				}
			}
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

func (s *Service) RefreshCardCaptions(ctx context.Context) error {
	rows, err := s.Store.DB.QueryContext(ctx, "SELECT owner_id,version_id,message_ids FROM deliveries WHERE state='sent'")
	if err != nil {
		return err
	}
	type item struct {
		owner, version int64
		ids            string
	}
	var items []item
	for rows.Next() {
		var v item
		if err = rows.Scan(&v.owner, &v.version, &v.ids); err != nil {
			rows.Close()
			return err
		}
		items = append(items, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range items {
		mu := s.userLock(v.owner)
		mu.Lock()
		err = func() error {
			var ids []int64
			if json.Unmarshal([]byte(v.ids), &ids) != nil || len(ids) == 0 || ids[0] <= 0 {
				return errors.New("invalid card receipt")
			}
			p, err := s.Store.Version(ctx, v.owner, v.version)
			if err != nil {
				return err
			}
			jobs, err := s.Store.MediaJobs(ctx, v.owner, p.VersionID)
			if err != nil {
				return err
			}
			photos := selectedProfilePhotos(jobs)
			method := "editMessageCaption"
			if len(photos) == 0 {
				method = "editMessageText"
			}
			card, err := s.renderPrivateCard(ctx, v.owner, p, jobs, photos)
			if err != nil {
				return err
			}
			payload := map[string]any{"chat_id": v.owner, "message_id": ids[0], "caption": card.Text, "caption_entities": card.Entities}
			if len(photos) == 0 {
				delete(payload, "caption")
				delete(payload, "caption_entities")
				payload["text"] = card.Text
				payload["entities"] = card.Entities
				payload["link_preview_options"] = map[string]bool{"is_disabled": true}
			}
			if len(ids) == 1 {
				payload["reply_markup"] = profileButtons(p, card.Truncated || len(photoExifBlocks(photos)) > 0, !card.SourceJSONInline)
			}
			var out SentMessage
			err = s.TG.Call(ctx, method, payload, &out)
			var te *TelegramError
			if errors.As(err, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
				return nil
			}
			if err != nil {
				return err
			}
			if out.ID != ids[0] || out.Chat.ID != v.owner {
				return errors.New("caption edit unconfirmed")
			}
			return nil
		}()
		mu.Unlock()
		if err != nil {
			return err
		}
		if !sleep(ctx, 1100*time.Millisecond) {
			return ctx.Err()
		}
	}
	return nil
}

type privateCardRepair struct {
	State      string  `json:"state"`
	OldIDs     []int64 `json:"old_ids"`
	NewIDs     []int64 `json:"new_ids,omitempty"`
	DeletedIDs []int64 `json:"deleted_ids,omitempty"`
}

func (s *Service) savePrivateRepair(ctx context.Context, key string, record privateCardRepair) error {
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.Store.SetMeta(ctx, key, string(b))
}
