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
	"time"
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

func TestOperationsReportsActualGetMeAndRefreshRequests(t *testing.T) {
	for _, mode := range []string{"direct_get", "preflight_refresh", "refresh_after_401"} {
		t.Run(mode, func(t *testing.T) {
			s := operationsTestService(t)
			ctx := context.Background()
			s.Config.APIBase = "https://api.jardulik.app/api"
			client := NewClient(s.Store, s.Config)
			now := time.Now()
			client.Now = func() time.Time { return now }
			expires := now.Add(time.Hour).Unix()
			if mode == "preflight_refresh" {
				expires = now.Add(-time.Second).Unix()
			}
			if err := s.Store.User(ctx, 123, "test"); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "fixture", Token: "fixture-access-token", Refresh: "fixture-refresh-token", Expires: expires}); err != nil {
				t.Fatal(err)
			}
			var requests []string
			client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				requests = append(requests, r.Method+" "+r.URL.Path)
				status := http.StatusBadRequest
				if mode == "refresh_after_401" && len(requests) == 1 {
					status = http.StatusUnauthorized
				}
				return albumTestResponse(r, status, `{"error":"response-secret"}`), nil
			})
			var stderr, spool bytes.Buffer
			before := slog.Default()
			slog.SetDefault(slog.New(&operationsLogHandler{base: slog.NewJSONHandler(&stderr, nil), sink: &operationsLogSink{writer: &spool, role: RuntimePoster, boot: "getme"}}))
			defer slog.SetDefault(before)
			_, err := client.Me(ctx, 123)
			var failure *UpstreamError
			if !errors.As(err, &failure) || failure.Status != 400 {
				t.Fatal("missing HTTP rejection", err)
			}
			method, stage := "GET", "/user/getMe"
			wantURL := "https://api.jardulik.app/api/user/getMe?userId=5696"
			if mode != "direct_get" {
				method, stage, wantURL = "PUT", "/appuser/refresh", "https://api.jardulik.app/api/appuser/refresh"
			}
			if failure.Method != method || failure.Endpoint != stage || failure.RequestURL != wantURL {
				t.Fatal("wrong actual request attribution", failure.Method, failure.Endpoint, failure.RequestURL)
			}
			if requests[len(requests)-1] != method+" /api"+stage {
				t.Fatal("wrong mock request", requests)
			}
			if mode == "preflight_refresh" && len(requests) != 1 || mode == "refresh_after_401" && len(requests) != 2 {
				t.Fatal("unexpected request count", requests)
			}
			for _, secret := range []string{"fixture-access-token", "fixture-refresh-token", "response-secret"} {
				if strings.Contains(spool.String(), secret) || strings.Contains(stderr.String(), secret) {
					t.Fatal("request credentials/body exposed")
				}
			}
			var record operationsLogRecord
			if err = json.Unmarshal(spool.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			operationsWriteRecords(t, s, record)
			if err = s.collectOperationsLogs(ctx); err != nil {
				t.Fatal(err)
			}
			var details string
			if err = s.Store.DB.QueryRow("SELECT fields_json FROM operations_events").Scan(&details); err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err = json.Unmarshal([]byte(details), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["method"] != method || fields["stage"] != stage || fields["request_url"] != wantURL {
				t.Fatal("spool import lost actual request", fields)
			}
			if mode != "direct_get" && fields["requested_stage"] != "/user/getMe" {
				t.Fatal("initiating operation missing", fields)
			}
			text, entities := operationsEventText(operationsOperation{Role: "poster", Level: "WARN", Text: "API operation failed", Detail: details, Last: now.UnixMilli()}, s.Config.Token)
			quote := strings.Index(text, "\n\n{")
			if !strings.Contains(text, "Ошибка запроса к Jardulik · HTTP 400") || quote < 0 || !strings.Contains(text[:quote], method+": '"+wantURL+"'") || len(entities) != 2 || entities[0].Type != "code" || entities[1].Type != "expandable_blockquote" || validMessageEntities(text, entities) != nil {
				t.Fatal("request absent from visible notification", text)
			}
			if strings.Contains(text, "/connect") != (mode != "direct_get") {
				t.Fatal("wrong reconnect guidance", text)
			}
		})
	}
}

func TestOperationsSafeUpstreamURLFiltersCredentialsAndAmbiguousQueries(t *testing.T) {
	raw := "https://api.jardulik.app/api/user/getUsers?userId=5696&pageNumber=3&pageSize=150&all=1&ageMin=0&ageMax=111&heightMax=210&token=private&city=private&goal=family&children=have&worldview=&country=Russia&isMan=true&initData=private&viewerUserId=bad&targetUserId=12&targetUserId=13"
	want := "https://api.jardulik.app/api/user/getUsers?ageMax=111&ageMin=0&all=1&children=have&country=Russia&goal=family&heightMax=210&isMan=true&pageNumber=3&pageSize=150&userId=5696&worldview="
	if got, filtered := operationsUpstreamURL(raw); got != want || !filtered {
		t.Fatal(got)
	}
	for _, unsafe := range []string{
		"https://api.telegram.org/bot123456789:abcdefghijklmnopqrstuvwxyz/sendMessage",
		"https://user:private@api.jardulik.app/api/user/getMe?userId=1",
		"https://api.jardulik.app.evil.example/api/user/getMe?userId=1",
		"https://api.jardulik.app/api/user/private?userId=1",
		"https://api.jardulik.app/api/user/getMe#private",
	} {
		if got, _ := operationsUpstreamURL(unsafe); got != "" {
			t.Fatal("unapproved URL retained", got)
		}
	}
}

func TestOperationsFiltersOldSpoolAndEventBeforeRendering(t *testing.T) {
	s := operationsTestService(t)
	ctx := context.Background()
	token := "123456789:abcdefghijklmnopqrstuvwxyz"
	rawURL := "https://api.jardulik.app/api/user/getMe?userId=5696&token=" + token
	record := operationsTestRecord("unfiltered-old-spool", time.Now().UnixMilli())
	record.Message = "API operation failed"
	record.Fields = map[string]any{"source": "Jardulik", "method": "GET", "request_url": rawURL, "http_status": 400, "description": token, "authorization": "Bearer private"}
	operationsWriteRecords(t, s, record)
	if err := s.collectOperationsLogs(ctx); err != nil {
		t.Fatal(err)
	}
	var details string
	if err := s.Store.DB.QueryRow("SELECT fields_json FROM operations_events").Scan(&details); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(details, token) || strings.Contains(details, "authorization") || !strings.Contains(details, "?userId=5696") {
		t.Fatal("spool import failed credential filtering", details)
	}
	legacy, _ := json.Marshal(record.Fields)
	text, _ := operationsEventText(operationsOperation{Role: "poster", Level: "WARN", Text: record.Message, Detail: string(legacy), Last: record.At}, token)
	if strings.Contains(text, token) || strings.Contains(text, "authorization") || !strings.Contains(text, "GET: 'https://api.jardulik.app/api/user/getMe?userId=5696'") || !strings.Contains(text, "Часть параметров URL скрыта.") {
		t.Fatal("legacy event rendered unfiltered", text)
	}
	malformed, _ := operationsEventText(operationsOperation{Role: "poster", Level: "WARN", Text: record.Message, Detail: "invalid old payload " + token, Last: record.At}, token)
	if strings.Contains(malformed, token) || strings.Contains(malformed, "invalid old payload") {
		t.Fatal("unstructured old payload exposed")
	}
}
