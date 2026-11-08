package companion

import (
	"context"
	"errors"
)

var errCrawlYield = errors.New("background read yielded to a user command")

type crawlPriorityKey struct{}

func withCrawlPriority(ctx context.Context, s *Service) context.Context {
	return context.WithValue(ctx, crawlPriorityKey{}, s)
}

// Yield only before a GET. In-flight reads, token refresh and reactions finish normally.
func crawlShouldYield(ctx context.Context) bool {
	s, _ := ctx.Value(crawlPriorityKey{}).(*Service)
	return s != nil && s.foregroundPending()
}
