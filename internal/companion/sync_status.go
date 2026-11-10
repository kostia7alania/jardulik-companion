package companion

import (
	"context"
	"fmt"
)

// SyncStatus is local-only; it does not refresh an account or start a cycle.
func (s *Service) SyncStatus(ctx context.Context, owner int64) (map[string]any, error) {
	result := map[string]any{"enabled": s.Config.SyncCycleEnabled}
	cycle, err := s.Store.SyncCycle(ctx, owner)
	if err != nil {
		return nil, err
	}
	if cycle == nil {
		result["stage"] = "not_started"
		return result, nil
	}
	result["cycle"] = cycle
	for _, item := range []struct{ table, key string }{
		{"sync_refresh_items", "refresh"}, {"compatibility_cycle_targets", "compatibility"},
	} {
		exists, err := snapshotHasTable(ctx, s.Store.DB, item.table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		rows, err := s.Store.DB.QueryContext(ctx, "SELECT state,count(*) FROM "+item.table+" WHERE cycle_id=? GROUP BY state", cycle.ID)
		if err != nil {
			return nil, err
		}
		counts := map[string]int64{}
		for rows.Next() {
			var state string
			var count int64
			if err = rows.Scan(&state, &count); err != nil {
				rows.Close()
				return nil, err
			}
			counts[state] = count
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		result[item.key] = counts
	}
	return result, nil
}

func (s *Service) SyncStatusText(ctx context.Context, owner int64) string {
	if !s.Config.SyncCycleEnabled {
		return ""
	}
	result, err := s.SyncStatus(ctx, owner)
	if err != nil {
		return "Цикл: не удалось прочитать состояние."
	}
	cycle, ok := result["cycle"].(*SyncCycle)
	if !ok {
		return "🔎 Цикл готовится."
	}
	labels := map[string]string{
		"search":     "🔎 Поиск; 🧩 расчёты параллельно",
		"refresh":    "🔄 Проверка анкет; 🧩 расчёты параллельно",
		"draining":   "🧩 Завершение расчётов",
		"publishing": "📣 Публикация изменений",
		"complete":   "✅ Цикл завершён",
		"blocked":    "⏸ Цикл приостановлен",
	}
	text := labels[cycle.Stage]
	if text == "" {
		text = "Цикл: состояние уточняется."
	}
	if counts, ok := result["compatibility"].(map[string]int64); ok {
		var total int64
		for _, count := range counts {
			total += count
		}
		text += fmt.Sprintf("\n🧩 Готово: %d/%d", counts["succeeded"], total)
		attention := counts["unknown"] + counts["rejected"]
		if attention > 0 {
			text += fmt.Sprintf(" · требуют проверки: %d", attention)
		}
	}
	return text
}
