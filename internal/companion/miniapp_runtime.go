package companion

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

// RunMiniApp serves only the owner prototype. It starts no poller, crawler or reactions.
func (s *Service) RunMiniApp(ctx context.Context) error {
	if s.Config.MiniAppListen == "" || s.Config.MiniAppPublicOrigin == "" {
		return errors.New("Mini App listener and public origin are required")
	}
	if err := validateMiniAppEndpoint(s.Config.MiniAppListen, s.Config.MiniAppPublicOrigin); err != nil {
		return err
	}
	assets, err := NewMiniAppAssets(s.Config.DataDir)
	if err != nil {
		return err
	}
	handler, err := NewMiniAppHandler(s, s.Config.MiniAppPublicOrigin, assets)
	if err != nil {
		return err
	}
	handler = limitMiniAppRequests(handler, 4)
	listener, err := net.Listen("tcp", s.Config.MiniAppListen)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	workers.Add(1)
	go func() {
		defer workers.Done()
		s.runOperationsHeartbeat(ctx, RuntimeMiniApp)
	}()
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 90 * time.Second,
		IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err = server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-done
		return err
	}
}

// Public entry assets are several MiB. Bound concurrent work before reading bodies.
func limitMiniAppRequests(next http.Handler, concurrency int) http.Handler {
	slots := make(chan struct{}, concurrency)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next.ServeHTTP(w, r)
		case <-r.Context().Done():
			return
		}
	})
}
