package companion

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicWebServesOnlyPresentationAndDemo(t *testing.T) {
	handler := publicWebHandler()
	for _, want := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodGet, "/icon.svg", http.StatusOK},
		{http.MethodHead, "/styles.css", http.StatusOK},
		{http.MethodGet, "/api/demo", http.StatusOK},
		{http.MethodGet, "/healthz", http.StatusNoContent},
		{http.MethodGet, "/privacy.html", http.StatusOK},
		{http.MethodGet, "/terms.html", http.StatusOK},
		{http.MethodGet, "/bot.html", http.StatusOK},
		{http.MethodGet, "/guide.html", http.StatusOK},
		{http.MethodGet, "/bot.js", http.StatusOK},
		{http.MethodGet, "/swagger/", http.StatusOK},
		{http.MethodGet, "/swagger/openapi.json", http.StatusOK},
		{http.MethodGet, "/swagger/vendor/swagger-ui-bundle.js", http.StatusOK},
		{http.MethodGet, "/miniapp/profile", http.StatusNotFound},
		{http.MethodGet, "/.env", http.StatusNotFound},
		{http.MethodPost, "/api/demo", http.StatusMethodNotAllowed},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(want.method, want.path, nil))
		if response.Code != want.status {
			t.Errorf("%s %s: status %d, want %d", want.method, want.path, response.Code, want.status)
		}
		if want.path == "/api/demo" && want.method == http.MethodGet && !json.Valid(response.Body.Bytes()) {
			t.Error("synthetic demo response must be JSON")
		}
		if want.path == "/" && !strings.Contains(response.Header().Get("Content-Security-Policy"), "default-src 'self'") {
			t.Error("missing same-origin policy")
		}
	}
}
