package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (s *Service) analysisPostCallback(ctx context.Context, callback Callback) {
	if callback.From.ID != s.Config.OwnerID || callback.Message.Chat.ID != s.Config.BackupChatID || s.Config.BackupChatID == 0 || !strings.HasPrefix(callback.Data, "ga:") {
		return
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(callback.Data, "ga:"), 10, 64)
	if err != nil || id <= 0 {
		return
	}
	if err = s.sendAnalysisSource(ctx, id); err != nil {
		_ = s.TG.Call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": callback.ID, "text": "Расчёт недоступен для текущего аккаунта."}, nil)
		slog.Warn("analysis source delivery unavailable", "kind", safeError(err))
		return
	}
	_ = s.TG.Call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": callback.ID, "text": "Полный ответ отправлен в личку."}, nil)
}

func (s *Service) sendAnalysisSource(ctx context.Context, id int64) error {
	analysis, err := scanProfileAnalysis(s.Store.DB.QueryRowContext(ctx, "SELECT "+analysisColumns+" FROM profile_analyses a JOIN sessions s ON s.owner_id=a.owner_id AND s.upstream_id=a.upstream_id WHERE a.id=? AND a.owner_id=? AND a.state='response_received' AND a.http_status BETWEEN 200 AND 299", id, s.Config.OwnerID))
	if err != nil {
		return err
	}
	if !json.Valid(analysis.ResponseBody) || !compatibilityGPTSuccess(analysis.ResponseBody) {
		return errors.New("analysis response unavailable")
	}
	dir := filepath.Join(s.Config.DataDir, "json-staging")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, fmt.Sprintf("jardulik-gpt-%d-%d-%d-*.json", analysis.UpstreamID, analysis.ProfileID, analysis.ID))
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(analysis.ResponseBody); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	caption := "Jardulik GPT · " + displayTime(time.UnixMilli(analysis.ObservedAtMillis).UTC().Format(time.RFC3339Nano))
	sent, err := s.TG.SendDocument(ctx, s.Config.OwnerID, 0, file.Name(), caption)
	if err != nil {
		return err
	}
	if sent.ID <= 0 || sent.Chat.ID != s.Config.OwnerID || sent.Document == nil || sent.Document.FileID == "" || sent.Document.Size > 0 && sent.Document.Size != int64(len(analysis.ResponseBody)) {
		return errors.New("analysis document receipt unconfirmed")
	}
	return nil
}
