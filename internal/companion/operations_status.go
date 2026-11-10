package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// OperationsStatus reads existing local state, including stalled publication.
// It never authenticates an account, runs a source request or moves a cursor.
func (s *Service) OperationsStatus(ctx context.Context) (string, error) {
	now := time.Now()
	if s.Archiver != nil && s.Archiver.now != nil {
		now = s.Archiver.now()
	}
	lines := []string{"🖥 Сервер", displayTime(now.UTC().Format(time.RFC3339))}
	var allBeat struct {
		At int64 `json:"at_ms"`
	}
	_ = json.Unmarshal([]byte(s.Store.Meta(ctx, "operations_heartbeat:all")), &allBeat)
	roles := []RuntimeRole{RuntimeBotParser, RuntimePoster, RuntimeCompatibility}
	var miniBeat string
	miniErr := s.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='operations_heartbeat:miniapp'").Scan(&miniBeat)
	if miniErr != nil && !errors.Is(miniErr, sql.ErrNoRows) {
		return "", miniErr
	}
	if miniErr == nil {
		roles = append(roles, RuntimeRole("miniapp"))
	}
	for _, role := range roles {
		var beat struct {
			At int64 `json:"at_ms"`
		}
		icon, label := "⚪️", string(role)
		switch role {
		case RuntimeBotParser:
			label = "Бот / парсер"
		case RuntimePoster:
			label = "Постер"
		case RuntimeCompatibility:
			label = "Планировщик GPT"
		case RuntimeRole("miniapp"):
			label = "Mini App"
		}
		raw := miniBeat
		if role != RuntimeRole("miniapp") {
			raw = s.Store.Meta(ctx, "operations_heartbeat:"+string(role))
		}
		_ = json.Unmarshal([]byte(raw), &beat)
		// The legacy combined runtime does not run the standalone Mini App gateway.
		if role != RuntimeRole("miniapp") && allBeat.At > beat.At {
			beat.At = allBeat.At
		}
		if beat.At > 0 {
			if beat.At <= now.Add(30*time.Second).UnixMilli() && now.UnixMilli()-beat.At <= (90*time.Second).Milliseconds() {
				icon = "🟢"
			} else {
				icon = "🔴"
			}
		}
		lines = append(lines, icon+" "+label)
	}
	var state, reason string
	err := s.Store.DB.QueryRowContext(ctx, "SELECT state,last_error FROM sessions WHERE owner_id=?", s.Config.OwnerID).Scan(&state, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		state = "не подключён"
	} else if err != nil {
		return "", err
	}
	connection := "🔑 Аккаунт: " + state
	if reason != "" {
		connection += " · " + operationsStatusValue(reason, 120)
	}
	lines = append(lines, connection)
	search, err := s.operationsCrawlProgress(ctx)
	if err != nil {
		return "", err
	}
	if search != "" {
		lines = append(lines, search)
	}
	cycle, err := s.Store.SyncCycle(ctx, s.Config.OwnerID)
	if err != nil {
		return "", err
	}
	if cycle != nil {
		labels := map[string]string{"search": "поиск", "refresh": "проверка анкет", "draining": "завершение GPT", "publishing": "публикация", "complete": "завершён", "blocked": "приостановлен"}
		stage := labels[cycle.Stage]
		if stage == "" {
			stage = cycle.Stage
		}
		line := "🔄 Цикл: " + stage
		if cycle.Reason != "" {
			line += " · " + operationsStatusValue(cycle.Reason, 120)
		}
		lines = append(lines, line)
		for _, item := range []struct{ table, label, done string }{
			{"sync_refresh_items", "🔎 Проверено", "available"},
			{"compatibility_cycle_targets", "🧩 GPT", "succeeded"},
		} {
			counts, err := s.operationsStateCounts(ctx, item.table, "cycle_id=?", cycle.ID)
			if err != nil {
				return "", err
			}
			if counts != nil {
				done := counts[item.done]
				if item.table == "sync_refresh_items" {
					done += counts["unavailable"]
				}
				line := fmt.Sprintf("%s: %d/%d", item.label, done, operationsCountTotal(counts))
				if n := counts["unavailable"]; n > 0 {
					line += fmt.Sprintf(" · недоступны %d", n)
				}
				if n := counts["unknown"] + counts["rejected"]; n > 0 {
					line += fmt.Sprintf(" · проверить %d", n)
				}
				lines = append(lines, line)
			}
		}
	}
	for _, item := range []struct {
		table, label, scope, done string
		args                      []any
	}{
		{"archive_versions", "📦 Архив", "owner_id=?", "sent", []any{s.Config.OwnerID}},
		{"analysis_posts", "📣 Совместимости", "owner_id=?", "sent", []any{s.Config.OwnerID}},
		{"caption_maintenance_jobs", "🎨 Оформление", "owner_id=? AND revision=?", "done", []any{s.Config.OwnerID, captionDesignRevision}},
		{"profile_index_pages", "📚 Каталог", "topic_id IN (SELECT id FROM profile_index_topics WHERE owner_id=? AND chat_id=?) AND COALESCE(json_extract(payload_json,'$.legacy'),0)=0", "sent", []any{s.Config.OwnerID, s.Config.BackupChatID}},
		{"opening_audit_posts", "💌 Открывашки", "topic_id IN (SELECT id FROM opening_audit_topics WHERE owner_id=? AND chat_id=?)", "sent", []any{s.Config.OwnerID, s.Config.BackupChatID}},
		{"opening_timeline_posts", "💬 Открывашки в анкетах", "owner_id=? AND chat_id=?", "sent", []any{s.Config.OwnerID, s.Config.BackupChatID}},
		{"full_backup_jobs", "💾 Бэкапы", "1=1", "sent", nil},
	} {
		counts, err := s.operationsStateCounts(ctx, item.table, item.scope, item.args...)
		if err != nil {
			return "", err
		}
		if len(counts) == 0 {
			continue
		}
		done := counts[item.done]
		if item.table == "archive_versions" {
			done += counts["duplicate"] + counts["partial"]
		} else if item.table == "full_backup_jobs" {
			done += counts["skipped"] + counts["superseded"]
		}
		line := fmt.Sprintf("%s: %d/%d", item.label, done, operationsCountTotal(counts))
		if item.table == "archive_versions" && counts["partial"] > 0 {
			line += fmt.Sprintf(" · частично %d", counts["partial"])
		}
		if n := counts["failed"] + counts["error"] + counts["uncertain"] + counts["unknown"]; n > 0 {
			line += fmt.Sprintf(" · ⚠️ %d", n)
		}
		lines = append(lines, line)
	}
	exists, err := snapshotHasTable(ctx, s.Store.DB, "caption_maintenance_jobs")
	if err != nil {
		return "", err
	}
	if exists {
		rows, err := s.Store.DB.QueryContext(ctx, `SELECT id,kind,error FROM caption_maintenance_jobs WHERE owner_id=? AND revision=? AND state='failed' ORDER BY id LIMIT 3`, s.Config.OwnerID, captionDesignRevision)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var id int64
			var kind, failure string
			if err = rows.Scan(&id, &kind, &failure); err != nil {
				rows.Close()
				return "", err
			}
			lines = append(lines, fmt.Sprintf("⚠️ Правка #%d · %s · %s", id, kind, operationsStatusValue(failure, 120)))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return "", err
		}
	}
	hasChannel, err := snapshotHasTable(ctx, s.Store.DB, "operations_channel")
	if err != nil {
		return "", err
	}
	if hasChannel {
		var pinError string
		err = s.Store.DB.QueryRowContext(ctx, "SELECT pin_error FROM operations_channel WHERE owner_id=? AND chat_id=?", s.Config.OwnerID, s.Config.BackupChatID).Scan(&pinError)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		if pinError != "" {
			lines = append(lines, "⚠️ Закрепление статуса: "+operationsStatusValue(pinError, 120))
		}
	}
	text, _ := telegramPrefix(strings.Join(lines, "\n"), 3900)
	return text, nil
}

func (s *Service) operationsCrawlProgress(ctx context.Context) (string, error) {
	exists, err := snapshotHasTable(ctx, s.Store.DB, "feed_crawls")
	if err != nil || !exists {
		return "", err
	}
	var state, reason string
	var mode, page, total, full, unavailable, next int64
	err = s.Store.DB.QueryRowContext(ctx, `SELECT state,reason,mode,page,catalog_profiles,hydrated,unavailable,next_request FROM feed_crawls WHERE owner_id=?`, s.Config.OwnerID).Scan(&state, &reason, &mode, &page, &total, &full, &unavailable, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	line := "🔎 Поиск: "
	if state == "done" {
		line += fmt.Sprintf("завершён · %d анкет", total)
	} else {
		label := state
		if state == "running" {
			label = "идёт"
		} else if state == "queued" {
			label = "очередь"
		}
		line += fmt.Sprintf("%s · mode=%d · страница %d · полных %d/%d", label, mode, page, full, total)
		if (state == "queued" || state == "running") && next > 0 {
			line += " · следующая: " + displayTime(time.UnixMilli(next).UTC().Format(time.RFC3339))
		}
	}
	if unavailable > 0 {
		line += fmt.Sprintf(" · недоступны %d", unavailable)
	}
	if reason != "" && reason != "empty_page" {
		line += " · " + operationsStatusValue(reason, 120)
	}
	return line, nil
}

func operationsStatusValue(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	value, _ = telegramPrefix(value, limit)
	return value
}

func (s *Service) operationsStateCounts(ctx context.Context, table, scope string, args ...any) (map[string]int64, error) {
	exists, err := snapshotHasTable(ctx, s.Store.DB, table)
	if err != nil || !exists {
		return nil, err
	}
	rows, err := s.Store.DB.QueryContext(ctx, "SELECT state,count(*) FROM "+table+" WHERE "+scope+" GROUP BY state", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		counts[state] = n
	}
	return counts, rows.Err()
}

func operationsCountTotal(counts map[string]int64) int64 {
	var total int64
	for _, count := range counts {
		total += count
	}
	return total
}
