package companion

import (
	"context"
	"log/slog"
	"time"
)

const (
	collectionDiskRecovered = "Место для сбора восстановлено"
	collectionCrawlResumed  = "Обход возобновлён"
)

func (s *Service) runOperationsHeartbeat(ctx context.Context, role RuntimeRole) {
	roles := []RuntimeRole{role}
	if role == RuntimeAll {
		roles = []RuntimeRole{RuntimeAll, RuntimeBotParser, RuntimePoster, RuntimeCompatibility}
	}
	var diskState string
	for ctx.Err() == nil {
		for _, current := range roles {
			if err := s.Store.UpdateOperationsHeartbeat(ctx, current); err != nil && ctx.Err() == nil {
				slog.Warn("operations heartbeat unavailable", "kind", safeError(err))
			}
		}
		if role == RuntimeBotParser || role == RuntimeAll {
			diskState = reportCollectionDisk(s.Config, diskState)
		}
		if !sleep(ctx, 15*time.Second) {
			return
		}
	}
}

func reportCollectionDisk(cfg Config, previous string) string {
	free, err := FreeBytes(cfg.DataDir)
	reserve := cfg.collectionReserve()
	state := "ready"
	if err != nil {
		state = "disk_check_failed"
	} else if free < reserve {
		state = "disk_reserve"
	}
	if state == previous {
		return state
	}
	switch state {
	case "disk_check_failed":
		slog.Warn("Не удалось проверить свободное место для сбора", "kind", state)
	case "disk_reserve":
		slog.Warn("Сбор ждёт свободного места", "kind", state, "free_bytes", free, "reserve_bytes", reserve)
	case "ready":
		if previous != "" {
			slog.Info(collectionDiskRecovered, "state", state, "free_bytes", free, "reserve_bytes", reserve)
		}
	}
	return state
}
