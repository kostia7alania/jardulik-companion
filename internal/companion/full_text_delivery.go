package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const textDeliverySchema = `CREATE TABLE IF NOT EXISTS text_deliveries (
 recipient_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
 action TEXT NOT NULL,state TEXT NOT NULL,render_sha TEXT NOT NULL,
 message_ids TEXT NOT NULL DEFAULT '[]',PRIMARY KEY(recipient_id,version_id,action)
);`

func storedProfileJSON(ctx context.Context, store *Store, owner, version int64) (string, error) {
	var raw string
	if err := store.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE owner_id=? AND id=?", owner, version).Scan(&raw); err != nil {
		return "", err
	}
	return prettyProfileJSON(raw)
}

func prettyProfileJSON(raw string) (string, error) {
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(raw), "", "  "); err != nil {
		return "", errors.New("stored profile JSON is invalid")
	}
	return pretty.String(), nil
}

// Source-derived JSON objects stay intact when a long description needs pages.
func fullExifTextParts(text string, photos []MediaJob) ([]photoText, error) {
	blocks := photoExifBlocks(photos)
	value := fullPhotoText(text, blocks)
	type span struct{ start, end int }
	var protected []span
	if quote, ok := photoQuote(value.Entities); len(blocks) > 0 && ok {
		_, tail := telegramPrefix(value.Text, quote.Offset)
		remaining, _ := telegramPrefix(tail, quote.Length)
		offset := quote.Offset
		for _, block := range blocks {
			at := strings.Index(remaining, block)
			if at < 0 {
				return nil, errors.New("technical JSON lost its quoted source span")
			}
			start := offset + textUnits(remaining[:at])
			protected = append(protected, span{start, start + textUnits(block)})
			offset = start + textUnits(block)
			remaining = remaining[at+len(block):]
		}
	}
	var parts []photoText
	offset := 0
	for value.Text != "" {
		limit := 3900
		for _, block := range protected {
			if offset < block.end && offset+limit > block.start && offset+limit < block.end {
				limit = block.start - offset
				break
			}
		}
		if limit <= 0 {
			return nil, errors.New("technical JSON exceeds Telegram text budget")
		}
		part, rest := photoTextPart(value, limit)
		parts = append(parts, part)
		offset += textUnits(part.Text)
		value = rest
	}
	return parts, nil
}

func (s *Service) claimTextDelivery(ctx context.Context, recipient, version int64, action, sha string) ([]int64, bool, error) {
	if err := s.Store.User(ctx, recipient, ""); err != nil {
		return nil, false, err
	}
	if _, err := s.Store.DB.ExecContext(ctx, textDeliverySchema); err != nil {
		return nil, false, err
	}
	result, err := s.Store.DB.ExecContext(ctx, "INSERT INTO text_deliveries(recipient_id,version_id,action,state,render_sha) VALUES(?,?,?,'sending',?) ON CONFLICT DO NOTHING", recipient, version, action, sha)
	if err != nil {
		return nil, false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if count > 0 {
		return nil, false, nil
	}
	var state, oldSHA, raw string
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT state,render_sha,message_ids FROM text_deliveries WHERE recipient_id=? AND version_id=? AND action=?", recipient, version, action).Scan(&state, &oldSHA, &raw); err != nil {
		return nil, false, err
	}
	var ids []int64
	if json.Unmarshal([]byte(raw), &ids) != nil || len(ids) == 0 {
		return nil, false, errors.New("text delivery requires reconciliation")
	}
	seen := map[int64]bool{}
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return nil, false, errors.New("text delivery receipt is invalid")
		}
		seen[id] = true
	}
	if state != "sent" {
		return nil, false, errors.New("text delivery outcome is unresolved; automatic replay stopped")
	}
	if oldSHA == sha {
		return ids, true, nil
	}
	result, err = s.Store.DB.ExecContext(ctx, "UPDATE text_deliveries SET state='sending' WHERE recipient_id=? AND version_id=? AND action=? AND state='sent' AND render_sha=?", recipient, version, action, oldSHA)
	if err != nil {
		return nil, false, err
	}
	count, err = result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if count != 1 {
		return nil, false, errors.New("text delivery is already being updated")
	}
	return ids, false, nil
}

func (s *Service) saveTextDelivery(ctx context.Context, recipient, version int64, action, state, sha string, ids []int64) error {
	raw, _ := json.Marshal(ids)
	_, err := s.Store.DB.ExecContext(ctx, "UPDATE text_deliveries SET state=?,render_sha=?,message_ids=? WHERE recipient_id=? AND version_id=? AND action=?", state, sha, string(raw), recipient, version, action)
	return err
}

func (s *Service) sendKnownText(ctx context.Context, recipient, version int64, action string, parts []photoText) (retErr error) {
	if len(parts) == 0 {
		return errors.New("text delivery is empty")
	}
	for _, part := range parts {
		if textUnits(part.Text) > 4096 {
			return errors.New("text delivery exceeds Telegram budget")
		}
		if err := validMessageEntities(part.Text, part.Entities); err != nil {
			return err
		}
	}
	rendered, _ := json.Marshal(parts)
	sha := digest(rendered)
	ids, reused, err := s.claimTextDelivery(ctx, recipient, version, action, sha)
	if err != nil {
		return err
	}
	if reused {
		return nil
	}
	defer func() {
		if retErr != nil {
			_ = s.saveTextDelivery(context.WithoutCancel(ctx), recipient, version, action, "uncertain", sha, ids)
		}
	}()
	count := len(parts)
	if len(ids) > count {
		count = len(ids)
	}
	for i := 0; i < count; i++ {
		part := photoText{Text: "Полный текст обновлён в сообщении выше."}
		if i < len(parts) {
			part = parts[i]
		}
		if i < len(ids) {
			var edited SentMessage
			err = s.TG.Call(ctx, "editMessageText", map[string]any{"chat_id": recipient, "message_id": ids[i], "text": part.Text, "entities": part.Entities, "link_preview_options": map[string]bool{"is_disabled": true}}, &edited)
			var rejected *TelegramError
			unchanged := errors.As(err, &rejected) && rejected.Code == 400 && strings.Contains(strings.ToLower(rejected.Description), "message is not modified")
			if !unchanged && (err != nil || edited.ID != ids[i] || edited.Chat.ID != recipient) {
				if err != nil {
					return err
				}
				return errors.New("text edit receipt is unconfirmed")
			}
		} else {
			sent, err := s.TG.SendText(ctx, recipient, 0, part.Text, nil, part.Entities)
			if err != nil {
				return err
			}
			if sent.ID <= 0 || sent.Chat.ID != recipient {
				return errors.New("text delivery receipt is unconfirmed")
			}
			for _, id := range ids {
				if id == sent.ID {
					return errors.New("duplicate text delivery receipt")
				}
			}
			ids = append(ids, sent.ID)
			if err = s.saveTextDelivery(ctx, recipient, version, action, "sending", sha, ids); err != nil {
				return err
			}
		}
		if i+1 < count && !sleep(ctx, 1100*time.Millisecond) {
			return ctx.Err()
		}
	}
	return s.saveTextDelivery(ctx, recipient, version, action, "sent", sha, ids)
}

func (s *Service) sendKnownJSONDocument(ctx context.Context, recipient, version int64, pretty string) (retErr error) {
	if len(pretty) > 49<<20 {
		return errors.New("whole JSON document exceeds upload limit")
	}
	dir := filepath.Join(s.Config.DataDir, "json-staging")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, "jardulik-"+strconv.FormatInt(version, 10)+"-*.json")
	if err != nil {
		return err
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err = file.WriteString(pretty); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	sha := digest([]byte(pretty))
	ids, reused, err := s.claimTextDelivery(ctx, recipient, version, "source_json", sha)
	if err != nil {
		return err
	}
	if reused {
		return nil
	}
	defer func() {
		if retErr != nil {
			_ = s.saveTextDelivery(context.WithoutCancel(ctx), recipient, version, "source_json", "uncertain", sha, ids)
		}
	}()
	var sent SentMessage
	if len(ids) > 0 {
		sent, err = s.TG.EditDocument(ctx, recipient, ids[0], path, "JSON Jardulik целиком.")
	} else {
		sent, err = s.TG.SendDocument(ctx, recipient, 0, path, "JSON Jardulik целиком.")
	}
	if err != nil {
		return err
	}
	if sent.ID <= 0 || sent.Chat.ID != recipient || sent.Document == nil || sent.Document.FileID == "" || (len(ids) > 0 && sent.ID != ids[0]) {
		return errors.New("JSON document receipt is unconfirmed")
	}
	if sent.Document.Size > 0 && sent.Document.Size != int64(len(pretty)) {
		return errors.New("JSON document size is unconfirmed")
	}
	ids = []int64{sent.ID}
	return s.saveTextDelivery(ctx, recipient, version, "source_json", "sent", sha, ids)
}

func (s *Service) sourceJSON(ctx context.Context, recipient, owner, version int64) {
	if recipient != owner && recipient != s.Config.OwnerID {
		return
	}
	pretty, err := storedProfileJSON(ctx, s.Store, owner, version)
	if err == nil {
		if textUnits(pretty) <= 4096 {
			err = s.sendKnownText(ctx, recipient, version, "source_json", []photoText{{Text: pretty, Entities: []MessageEntity{{Type: "expandable_blockquote", Offset: 0, Length: textUnits(pretty)}}}})
		} else {
			err = s.sendKnownJSONDocument(ctx, recipient, version, pretty)
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		slog.Warn("source JSON delivery unavailable", "kind", safeError(err))
	}
}
