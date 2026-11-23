package companion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type RuntimeRole string

const (
	RuntimeAll           RuntimeRole = "all"
	RuntimeBotParser     RuntimeRole = "bot-parser"
	RuntimePoster        RuntimeRole = "poster"
	RuntimeCompatibility RuntimeRole = "compatibility"
	RuntimeMiniApp       RuntimeRole = "miniapp"
)

// RunBackground holds role locks until the caller cancels ctx and invokes wait.
func (s *Service) RunBackground(ctx context.Context, role RuntimeRole) (wait func(), err error) {
	return s.runBackground(ctx, role, nil)
}

func (s *Service) runBackground(ctx context.Context, role RuntimeRole, botReady <-chan struct{}) (wait func(), err error) {
	var names []string
	switch role {
	case RuntimeAll:
		names = []string{"bot-parser.lock", "archive-poster.lock", "compatibility-planner.lock"}
	case RuntimeBotParser:
		names = []string{"bot-parser.lock"}
	case RuntimePoster:
		names = []string{"archive-poster.lock"}
	case RuntimeCompatibility:
		names = []string{"compatibility-planner.lock"}
	default:
		return nil, errors.New("invalid runtime role")
	}
	var locks []*os.File
	unlock := func() {
		for i := len(locks) - 1; i >= 0; i-- {
			_ = syscall.Flock(int(locks[i].Fd()), syscall.LOCK_UN)
			_ = locks[i].Close()
		}
	}
	for _, name := range names {
		lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, name), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			unlock()
			return nil, err
		}
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = lock.Close()
			unlock()
			return nil, fmt.Errorf("runtime role %s is already running: %w", role, err)
		}
		locks = append(locks, lock)
	}
	if s.Config.CompatibilityEnabled {
		if err = s.Store.InitCompatibility(ctx); err != nil {
			unlock()
			return nil, err
		}
	}
	var debugRun func(context.Context)
	if botReady == nil && (role == RuntimeAll || role == RuntimeBotParser) {
		debugRun, err = s.prepareDebugHTTP()
		if err != nil {
			unlock()
			return nil, err
		}
	}
	var background sync.WaitGroup
	start := func(name string, run func(context.Context), restart bool) {
		background.Add(1)
		go func() {
			defer background.Done()
			defer func() {
				if cause := recover(); cause != nil {
					slog.Error("background worker panicked", "component", name, "kind", fmt.Sprintf("%T", cause))
					panic(cause)
				}
			}()
			for ctx.Err() == nil {
				run(ctx)
				if ctx.Err() != nil || !restart {
					return
				}
				slog.Warn("background worker stopped; retry scheduled", "component", name)
				if !sleep(ctx, 10*time.Second) {
					return
				}
			}
		}()
	}
	// Collection uses an existing source session. Bot work waits for a verified Telegram identity.
	startAfterBot := func(name string, run func(context.Context), restart bool) {
		if botReady == nil {
			start(name, run, restart)
			return
		}
		start(name, func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case <-botReady:
				if ctx.Err() == nil {
					run(ctx)
				}
			}
		}, restart)
	}
	if s.operationsAllowed() && s.Config.Token != "" {
		startAfterBot("operations-heartbeat", func(ctx context.Context) { s.runOperationsHeartbeat(ctx, role) }, true)
	}
	if role == RuntimeAll || role == RuntimeBotParser {
		if debugRun != nil {
			start("debug-http", debugRun, false)
		} else if botReady != nil && s.Config.DebugListen != "" {
			startAfterBot("debug-http", func(ctx context.Context) {
				run, err := s.prepareDebugHTTP()
				if err != nil {
					slog.Warn("debug HTTP startup failed", "kind", safeError(err))
				} else if run != nil {
					run(ctx)
				}
			}, true)
		}
		start("media", s.Media.Run, true)
		if s.Config.SyncCycleEnabled {
			start("sync-cycles", s.RunSyncCycles, true)
		} else {
			start("profile-checks", s.Watcher.RunChecks, true)
		}
		start("crawler", s.Crawler.Run, true)
		startAfterBot("autoliker", s.Autoliker.Run, true)
		startAfterBot("contacts", s.runContacts, true)
		if s.Config.RelationNoticesEnabled && s.Config.OwnerID > 0 {
			startAfterBot("relation-notice-poll", s.RunRelationNoticePoll, true)
		}
		if s.Config.CompatibilityEnabled {
			startAfterBot("compatibility-executor", s.runCompatibilityExecutor, true)
		}
		startAfterBot("private-card-repair", s.runPrivateCardRepair, true)
	}
	if role == RuntimeAll || role == RuntimePoster {
		if s.operationsAllowed() {
			startAfterBot("operations", s.RunOperations, true)
		}
		if s.openingTimelineAllowed() {
			startAfterBot("opening-timeline", s.RunOpeningTimeline, true)
		}
		if !s.Config.ArchivePaused {
			if s.relationNoticeDeliveryAllowed() {
				startAfterBot("relation-notice-delivery", s.RunRelationNoticesDelivery, true)
			}
			startAfterBot("archiver", s.Archiver.Run, true)
			startAfterBot("caption-maintenance", s.RunCaptionMaintenance, true)
			if s.profileIndexesAllowed() {
				startAfterBot("profile-indexes", s.RunProfileIndexes, true)
			}
		}
		startAfterBot("profile-presence", s.Watcher.RunPresence, true)
		if s.Archiver != nil && s.Archiver.TG != nil && s.Config.OwnerID > 0 {
			if s.Config.OwnerID == s.Archiver.Config.OwnerID {
				startAfterBot("analysis-posts", s.RunAnalysisPosts, true)
			}
			if s.Config.BackupChatID != 0 {
				startAfterBot("archive-changes", s.RunArchiveChangePosts, true)
			}
		}
		if s.openingPostsAllowed() {
			startAfterBot("opening-posts", s.RunOpeningPosts, true)
		}
	}
	if s.Config.CompatibilityEnabled && (role == RuntimeAll || role == RuntimeCompatibility) {
		startAfterBot("compatibility-planner", s.runCompatibilityPlanner, true)
	}
	var once sync.Once
	return func() { once.Do(func() { background.Wait(); unlock() }) }, nil
}

// RunPoster never starts Telegram polling or source authentication workers.
func (s *Service) RunPoster(ctx context.Context) error {
	if s.Config.Token == "" {
		return errors.New("TELEGRAM_BOT_TOKEN is required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait, err := s.RunBackground(ctx, RuntimePoster)
	if err != nil {
		return err
	}
	defer wait()
	<-ctx.Done()
	return nil
}

// RunCompatibility starts only the database planner, independently of Telegram and source HTTP.
func (s *Service) RunCompatibility(ctx context.Context) error {
	wait, err := s.RunBackground(ctx, RuntimeCompatibility)
	if err != nil {
		return err
	}
	defer wait()
	<-ctx.Done()
	return nil
}
