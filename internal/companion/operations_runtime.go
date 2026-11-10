package companion

import (
	"context"
	"log/slog"
	"time"
)

func (s *Service) runOperationsHeartbeat(ctx context.Context, role RuntimeRole) {
	roles := []RuntimeRole{role}
	if role == RuntimeAll {
		roles = operationsRoles
	}
	for ctx.Err() == nil {
		for _, current := range roles {
			if err := s.Store.UpdateOperationsHeartbeat(ctx, current); err != nil && ctx.Err() == nil {
				slog.Warn("operations heartbeat unavailable", "kind", safeError(err))
			}
		}
		if !sleep(ctx, 15*time.Second) {
			return
		}
	}
}
