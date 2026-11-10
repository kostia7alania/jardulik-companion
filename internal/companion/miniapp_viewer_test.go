package companion

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiniAppViewerHTTPContract(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			w := httptest.NewRecorder()
			serveMiniAppViewer(w, httptest.NewRequest(method, "/viewer?id=17053", nil))
			if got := w.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("private viewer cache policy = %q", got)
			}
			if method != http.MethodGet {
				if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodGet {
					t.Fatalf("unexpected method response: %d %v", w.Code, w.Header())
				}
				return
			}
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/html; charset=utf-8" {
				t.Fatalf("unexpected HTML response: %d %v", w.Code, w.Header())
			}
			if !strings.Contains(w.Body.String(), "https://telegram.org/js/telegram-web-app.js?62") || !strings.Contains(w.Body.String(), "<html lang=\"ru\">") {
				t.Fatal("embedded viewer document missing")
			}
		})
	}
}
