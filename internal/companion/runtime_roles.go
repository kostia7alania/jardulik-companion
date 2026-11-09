package companion

import (
	"context"
	"errors"
	"fmt"
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
)

// RunBackground holds role locks until the caller cancels ctx and invokes wait.
func (s *Service) RunBackground(ctx context.Context, role RuntimeRole) (wait func(), err error) {
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
	if role == RuntimeAll || role == RuntimeBotParser {
		debugRun, err = s.prepareDebugHTTP()
		if err != nil {
			unlock()
			return nil, err
		}
	}
	var background sync.WaitGroup
	start := func(run func(context.Context)) {
		background.Add(1)
		go func() { defer background.Done(); run(ctx) }()
	}
	if role == RuntimeAll || role == RuntimeBotParser {
		if debugRun != nil {
			start(debugRun)
		}
		start(s.Media.Run)
		start(s.Watcher.RunChecks)
		start(s.Crawler.Run)
		start(s.Autoliker.Run)
		start(s.runContacts)
		if s.Config.CompatibilityEnabled {
			start(s.runCompatibilityExecutor)
		}
		start(s.runPrivateCardRepair)
	}
	if role == RuntimeAll || role == RuntimePoster {
		if !s.Config.ArchivePaused {
			start(s.Archiver.Run)
			start(s.RunCaptionMaintenance)
		}
		start(s.Watcher.RunPresence)
		start(s.RunAnalysisPosts)
		start(s.RunArchiveChangePosts)
	}
	if s.Config.CompatibilityEnabled && (role == RuntimeAll || role == RuntimeCompatibility) {
		start(s.runCompatibilityPlanner)
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
