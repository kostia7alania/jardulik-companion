package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

func TestOperationsAPIErrorsKeepDiagnosticsWithoutCredentials(t *testing.T) {
	var output bytes.Buffer
	before := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(before)
	tg := &Telegram{Token: "private-bot-value"}
	req, _ := http.NewRequest("POST", "https://telegram.test/botprivate-bot-value/sendMessage", nil)
	response := &http.Response{Request: req, Body: io.NopCloser(strings.NewReader(`{"ok":false,"error_code":429,"description":"retry private-bot-value","parameters":{"retry_after":17}}`))}
	if err := tg.decode(response, nil); err == nil {
		t.Fatal("API rejection was lost")
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["method"] != "sendMessage" || record["http_status"] != float64(429) || record["retry_after"] != float64(17) || strings.Contains(output.String(), tg.Token) {
		t.Fatal("missing diagnostics or leaked credentials", output.String())
	}
	output.Reset()
	for _, err := range []error{nil, context.Canceled, ErrProfileUnavailable, &TelegramError{Code: 400, Description: "message is not modified"}} {
		logOperationsFailure(context.Background(), "fixture", "GET", "/user/getUser?token=private", 123, err)
	}
	if output.Len() != 0 {
		t.Fatal("expected outcomes flooded the error thread")
	}
	logOperationsFailure(context.Background(), "Jardulik", "GET", "/user/getMe?token=private", 123, errors.New("failed"))
	if strings.Contains(output.String(), "token=private") {
		t.Fatal("query parameters were logged")
	}
}
