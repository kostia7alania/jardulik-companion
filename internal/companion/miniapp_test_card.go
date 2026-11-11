package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrMiniAppTestCardUncertain = errors.New("Mini App test card outcome requires reconciliation")

type MiniAppTestCardResult struct {
	Message     SentMessage
	AlreadySent bool
}

type miniAppTestCardReceipt struct {
	State     string `json:"state"`
	Owner     int64  `json:"owner"`
	Origin    string `json:"origin"`
	Profile   int64  `json:"profile"`
	Version   int64  `json:"version"`
	MessageID int64  `json:"message_id,omitempty"`
	ChatID    int64  `json:"chat_id,omitempty"`
}

// SendMiniAppTestCard is an explicit owner-only experiment, independent of the
// normal delivery queue, reaction controls and background publication pause.
func (s *Service) SendMiniAppTestCard(ctx context.Context, profileID int64) (MiniAppTestCardResult, error) {
	var result MiniAppTestCardResult
	if s == nil || s.Store == nil || s.TG == nil || s.Config.OwnerID <= 0 || profileID <= 0 || s.Config.MiniAppPublicOrigin == "" {
		return result, errors.New("invalid Mini App test card configuration or profile")
	}
	if err := validateMiniAppEndpoint(s.Config.MiniAppListen, s.Config.MiniAppPublicOrigin); err != nil {
		return result, err
	}
	p, err := s.Store.Profile(ctx, s.Config.OwnerID, profileID)
	if err != nil {
		return result, err
	}
	if p.ID != profileID || p.VersionID <= 0 {
		return result, errors.New("Mini App test profile identity mismatch")
	}
	receipt := miniAppTestCardReceipt{State: "sending", Owner: s.Config.OwnerID, Origin: s.Config.MiniAppPublicOrigin, Profile: p.ID, Version: p.VersionID}
	key := "miniapp-test-card:v1:" + digest([]byte(fmt.Sprintf("%d\n%s\n%d\n%d", receipt.Owner, receipt.Origin, receipt.Profile, receipt.Version)))
	if saved, exists, err := s.miniAppTestCardReceipt(ctx, key, receipt); err != nil || exists {
		return saved, err
	}
	jobs, err := s.Store.MediaJobs(ctx, receipt.Owner, p.VersionID)
	if err != nil {
		return result, err
	}
	card, err := BuildRichProfileCard(p, jobs, s.profilePostURL(ctx, receipt.Owner, p))
	if err != nil {
		return result, err
	}
	intent, err := json.Marshal(receipt)
	if err != nil {
		return result, err
	}
	claimed, err := s.Store.DB.ExecContext(ctx, "INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)", key, string(intent))
	if err != nil {
		return result, err
	}
	count, err := claimed.RowsAffected()
	if err != nil {
		return result, err
	}
	if count == 0 {
		saved, exists, err := s.miniAppTestCardReceipt(ctx, key, receipt)
		if err == nil && !exists {
			err = ErrMiniAppTestCardUncertain
		}
		return saved, err
	}
	if count != 1 {
		return result, ErrMiniAppTestCardUncertain
	}
	id := strconv.FormatInt(p.ID, 10)
	keyboard := map[string]any{"inline_keyboard": [][]map[string]any{{
		{"text": "🗃 Анкета", "web_app": map[string]string{"url": miniAppArchiveViewerURL(receipt.Origin, p)}},
		{"text": "Jardulik · прокси", "web_app": map[string]string{"url": receipt.Origin + "/mutuals?targetUserId=" + id + "&noCheck=1"}},
	}}}
	message, sendErr := s.TG.SendRichProfileCard(ctx, receipt.Owner, 0, card, keyboard)
	result.Message = message
	receipt.State, receipt.MessageID, receipt.ChatID = "uncertain", message.ID, message.Chat.ID
	if sendErr == nil && message.ID > 0 && message.Chat.ID == receipt.Owner && message.ThreadID == 0 {
		receipt.State = "sent"
	} else if sendErr == nil {
		sendErr = errors.New("unconfirmed owner Mini App test card")
	}
	final, err := json.Marshal(receipt)
	if err != nil {
		return result, errors.Join(sendErr, ErrMiniAppTestCardUncertain, err)
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	saved, writeErr := s.Store.DB.ExecContext(writeCtx, "UPDATE meta SET value=? WHERE key=? AND value=?", string(final), key, string(intent))
	if writeErr == nil {
		n, countErr := saved.RowsAffected()
		if countErr != nil || n != 1 {
			writeErr = errors.Join(errors.New("Mini App test card intent changed before receipt"), countErr)
		}
	}
	if sendErr != nil || writeErr != nil {
		return result, errors.Join(sendErr, writeErr, ErrMiniAppTestCardUncertain)
	}
	return result, nil
}

func (s *Service) miniAppTestCardReceipt(ctx context.Context, key string, expected miniAppTestCardReceipt) (MiniAppTestCardResult, bool, error) {
	var result MiniAppTestCardResult
	var raw string
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", key).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, false, nil
		}
		return result, false, err
	}
	var receipt miniAppTestCardReceipt
	if err := json.Unmarshal([]byte(raw), &receipt); err != nil || receipt.Owner != expected.Owner || receipt.Origin != expected.Origin || receipt.Profile != expected.Profile || receipt.Version != expected.Version || receipt.State != "sent" || receipt.MessageID <= 0 || receipt.ChatID != expected.Owner {
		return result, true, ErrMiniAppTestCardUncertain
	}
	result.Message.ID, result.Message.Chat.ID, result.AlreadySent = receipt.MessageID, receipt.ChatID, true
	return result, true, nil
}
