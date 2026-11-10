package companion

import (
	_ "embed"
	"net/http"
)

//go:embed miniapp/viewer.html
var miniAppViewerHTML []byte

func serveMiniAppViewer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(miniAppViewerHTML)
}
