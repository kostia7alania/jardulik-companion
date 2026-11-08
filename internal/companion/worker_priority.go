package companion

import (
	"context"
	"strings"
	"sync/atomic"
)

// Includes both queued and executing owner interactions. Background work checks
// this between portions; an already-started request is never interrupted here.
type foregroundPriority struct {
	pending atomic.Int64
}

func (s *Service) beginForeground()        { s.foreground.pending.Add(1) }
func (s *Service) endForeground()          { s.foreground.pending.Add(-1) }
func (s *Service) foregroundPending() bool { return s.foreground.pending.Load() > 0 }

func (s *Service) isForegroundUpdate(u Update) bool {
	var owner int64
	var chat TGChat
	if u.Callback != nil {
		owner, chat = u.Callback.From.ID, u.Callback.Message.Chat
		if owner == s.Config.OwnerID && chat.ID == s.Config.BackupChatID && s.Config.BackupChatID != 0 {
			data := u.Callback.Data
			return strings.HasPrefix(data, "av:") || strings.HasPrefix(data, "az:") || strings.HasPrefix(data, "aj:")
		}
	} else if u.Message != nil {
		owner, chat = u.Message.From.ID, u.Message.Chat
	} else {
		return false
	}
	return chat.Type == "private" && chat.ID == owner
}

type queuedUpdate struct {
	update     Update
	foreground bool
}

func (s *Service) prepareQueuedUpdate(u Update) queuedUpdate {
	job := queuedUpdate{update: u, foreground: s.isForegroundUpdate(u)}
	if job.foreground {
		s.beginForeground()
	}
	return job
}

func (s *Service) releaseQueuedUpdate(job queuedUpdate) {
	if job.foreground {
		s.endForeground()
	}
}

// Ownership of the counter reservation transfers to the worker only on send.
func (s *Service) enqueueUpdate(ctx context.Context, jobs chan<- queuedUpdate, job queuedUpdate) bool {
	select {
	case jobs <- job:
		return true
	case <-ctx.Done():
		s.releaseQueuedUpdate(job)
		return false
	}
}

func (s *Service) handleQueuedUpdate(ctx context.Context, job queuedUpdate) {
	defer s.releaseQueuedUpdate(job)
	s.handle(ctx, job.update)
	s.Store.FinishUpdate(ctx, job.update.ID)
}

// Run calls this after closing the queues and joining workers, so buffered
// updates abandoned on shutdown cannot leave a foreground reservation behind.
func (s *Service) discardQueuedUpdates(jobs <-chan queuedUpdate) {
	for job := range jobs {
		s.releaseQueuedUpdate(job)
	}
}
