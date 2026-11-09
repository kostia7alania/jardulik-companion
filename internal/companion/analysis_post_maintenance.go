package companion

import (
	"context"
	"database/sql"
	"encoding/json"
)

func (s *Service) prepareAnalysisPostEdit(ctx context.Context, j *captionMaintenanceJob, ids []int64) (string, map[string]any, error) {
	if j.owner != s.Config.OwnerID || j.chat != s.Config.BackupChatID || j.thread <= 0 || len(ids) != 1 {
		return "", nil, errCaptionMaintenanceReceipt
	}
	analysis, err := scanProfileAnalysis(s.Store.DB.QueryRowContext(ctx, "SELECT "+analysisColumns+` FROM profile_analyses a
 JOIN sessions se ON se.owner_id=a.owner_id AND se.upstream_id=a.upstream_id
 JOIN analysis_posts p ON p.analysis_id=a.id AND p.owner_id=a.owner_id AND p.upstream_id=a.upstream_id AND p.profile_id=a.profile_id
 WHERE a.id=? AND a.owner_id=? AND p.state='sent' AND p.chat_id=? AND p.thread_id=? AND p.message_id=? AND p.response_sha=?`, j.receipt, j.owner, j.chat, j.thread, ids[0], j.source))
	if err == sql.ErrNoRows {
		return "", nil, errCaptionMaintenanceReceipt
	}
	if err != nil {
		return "", nil, err
	}
	if digest(analysis.ResponseBody) != j.source {
		return "", nil, errCaptionMaintenanceReceipt
	}
	card, err := analysisPostText(analysis)
	if err != nil {
		return "", nil, err
	}
	keyboard := card.Keyboard
	if keyboard == nil {
		keyboard = map[string]any{"inline_keyboard": [][]map[string]string{}}
	}
	return "editMessageText", map[string]any{"chat_id": j.chat, "message_id": ids[0], "text": card.Text, "entities": card.Entities, "reply_markup": keyboard, "link_preview_options": map[string]bool{"is_disabled": true}}, nil
}

func (s *Service) saveAnalysisPostEdit(ctx context.Context, j *captionMaintenanceJob, payload map[string]any) error {
	text, ok := payload["text"].(string)
	if !ok {
		return errCaptionMaintenanceReceipt
	}
	entities, ok := payload["entities"].([]MessageEntity)
	if !ok {
		return errCaptionMaintenanceReceipt
	}
	raw, err := json.Marshal(analysisPostPayload{Text: text, Entities: entities, Keyboard: payload["reply_markup"]})
	if err != nil {
		return err
	}
	result, err := s.Store.DB.ExecContext(ctx, `UPDATE analysis_posts SET payload_json=?,updated_at=CURRENT_TIMESTAMP
 WHERE analysis_id=? AND owner_id=? AND state='sent' AND chat_id=? AND thread_id=? AND message_id=? AND response_sha=?
 AND EXISTS(SELECT 1 FROM sessions se WHERE se.owner_id=analysis_posts.owner_id AND se.upstream_id=analysis_posts.upstream_id)`, raw, j.receipt, j.owner, j.chat, j.thread, payload["message_id"], j.source)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errCaptionMaintenanceReceipt
	}
	return nil
}
