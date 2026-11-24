package companion

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"time"
)

// Only first-party presentation assets and synthetic demo data are embedded.
//
//go:embed web/index.html web/icon.svg web/styles.css web/app.js web/demo.json web/privacy.html web/terms.html web/bot.html web/guide.html
var publicWeb embed.FS

func publicWebHandler() http.Handler {
	files, err := fs.Sub(publicWeb, "web")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(files))
	mux.HandleFunc("GET /api/demo", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, files, "demo.json")
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		mux.ServeHTTP(w, r)
	})
}

// RunWeb starts the public landing without opening SQLite or starting bot roles.
func RunWeb(ctx context.Context, listen string) error {
	if listen == "" {
		listen = "127.0.0.1:8787"
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{
		Handler: publicWebHandler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
