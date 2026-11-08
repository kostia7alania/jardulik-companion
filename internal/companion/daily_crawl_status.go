package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (c *FeedCrawler) DailyStatus(ctx context.Context, owner int64) string {
	s := c.Service
	if !s.Config.DailyCrawl || owner != s.Config.OwnerID {
		return ""
	}
	var blocked bool
	if err := s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM meta WHERE key=? AND value='1')", dailyCrawlBlockKey(owner)).Scan(&blocked); err != nil {
		return "Ежедневный сбор: не удалось прочитать расписание."
	}
	if blocked {
		return "Ежедневный сбор: остановлен; нужен новый запуск обхода."
	}
	var raw string
	err := s.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", dailyCrawlKey(owner)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "Ежедневный сбор: включён."
	}
	var marker dailyCrawlMarker
	if err != nil || json.Unmarshal([]byte(raw), &marker) != nil {
		return "Ежедневный сбор: не удалось прочитать расписание."
	}
	job, err := s.Store.CrawlStatus(ctx, owner)
	if err != nil {
		return "Ежедневный сбор: не удалось прочитать расписание."
	}
	if marker.Blocked || job == nil || job.State == "stopped" {
		return "Ежедневный сбор: остановлен; нужен новый запуск обхода."
	}
	if marker.NextAt <= c.now().UnixMilli() && job.active() {
		return "Ежедневный сбор: следующий обход после завершения текущего."
	}
	return "Ежедневный сбор: следующий запуск не раньше " + displayTime(time.UnixMilli(marker.NextAt).UTC().Format(time.RFC3339)) + "."
}
