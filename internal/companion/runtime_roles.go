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
	start := func(name string, run func(context.Context)) {
		background.Add(1)
		go func() {
			defer background.Done()
			defer func() {
				if cause := recover(); cause != nil {
					slog.Error("background worker panicked", "component", name, "kind", fmt.Sprintf("%T", cause))
					panic(cause)
				}
			}()
			run(ctx)
			if ctx.Err() == nil {
				slog.Warn("background worker stopped", "component", name)
			}
		}()
	}
	// Collection uses an existing source session. Bot work waits for a verified Telegram identity.
	startAfterBot := func(name string, run func(context.Context)) {
		if botReady == nil {
			start(name, run)
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
		})
	}
	if s.operationsAllowed() && s.Config.Token != "" {
		startAfterBot("operations-heartbeat", func(ctx context.Context) { s.runOperationsHeartbeat(ctx, role) })
	}
	if role == RuntimeAll || role == RuntimeBotParser {
		if debugRun != nil {
			start("debug-http", debugRun)
		} else if botReady != nil && s.Config.DebugListen != "" {
			startAfterBot("debug-http", func(ctx context.Context) {
				run, err := s.prepareDebugHTTP()
				if err != nil {
					slog.Warn("debug HTTP startup failed", "kind", safeError(err))
				} else if run != nil {
					run(ctx)
				}
			})
		}
		start("media", s.Media.Run)
		if s.Config.SyncCycleEnabled {
			start("sync-cycles", s.RunSyncCycles)
		} else {
			start("profile-checks", s.Watcher.RunChecks)
		}
		start("crawler", s.Crawler.Run)
		startAfterBot("autoliker", s.Autoliker.Run)
		startAfterBot("contacts", s.runContacts)
		if s.Config.RelationNoticesEnabled {
			startAfterBot("relation-notice-poll", s.RunRelationNoticePoll)
		}
		if s.Config.CompatibilityEnabled {
			startAfterBot("compatibility-executor", s.runCompatibilityExecutor)
		}
		startAfterBot("private-card-repair", s.runPrivateCardRepair)
	}
	if role == RuntimeAll || role == RuntimePoster {
		startAfterBot("operations", s.RunOperations)
		startAfterBot("opening-timeline", s.RunOpeningTimeline)
		if !s.Config.ArchivePaused {
			if s.Config.RelationNoticesEnabled {
				startAfterBot("relation-notice-delivery", s.RunRelationNoticesDelivery)
			}
			startAfterBot("archiver", s.Archiver.Run)
			startAfterBot("caption-maintenance", s.RunCaptionMaintenance)
			startAfterBot("profile-indexes", s.RunProfileIndexes)
		}
		startAfterBot("profile-presence", s.Watcher.RunPresence)
		startAfterBot("analysis-posts", s.RunAnalysisPosts)
		startAfterBot("archive-changes", s.RunArchiveChangePosts)
		if !s.Config.ArchivePaused {
			startAfterBot("opening-posts", s.RunOpeningPosts)
		}
	}
	if s.Config.CompatibilityEnabled && (role == RuntimeAll || role == RuntimeCompatibility) {
		startAfterBot("compatibility-planner", s.runCompatibilityPlanner)
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
