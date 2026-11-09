package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type debugFixture struct {
	service    *Service
	handler    *debugHTTP
	calls      []string
	postStatus int
	postBody   string
	postErr    bool
	echoToken  bool
}

func newDebugFixture(t *testing.T) *debugFixture {
	t.Helper()
	f := newAutoFixture(t)
	s := f.worker.Service
	s.Config.OwnerID = 1
	s.Client.Base = "https://api.jardulik.app/api"
	s.Client.UserAgent = "owner-debug-test/1"
	f.add(t, 1, 7, 23, false)
	f.add(t, 1, 8, 24, false)
	ss, e := s.Store.Session(context.Background(), 1)
	if e != nil {
		t.Fatal(e)
	}
	ss.Token, ss.Refresh = "source-access-token", "source-refresh-token"
	if e = s.Store.SaveSession(context.Background(), 1, ss); e != nil {
		t.Fatal(e)
	}
	out := &debugFixture{service: s, postStatus: 200, postBody: "{\n  \"content\": \"analysis\", \"model\": \"reported-model\"\n}\n"}
	s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		out.calls = append(out.calls, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer source-access-token" || r.Header.Get("User-Agent") != "owner-debug-test/1" {
			t.Fatal("upstream did not use backend credentials/headers")
		}
		status := 200
		body := ""
		switch r.URL.Path {
		case "/api/user/getMe":
			body = `{"userId":101,"age":33,"type":1,"freezeState":false}`
		case "/api/user/getUser":
			id := r.URL.Query().Get("targetUserId")
			body = fmt.Sprintf("{\n\"userId\":%s, \"age\":23,\"height\":170\n}\n", id)
		case "/api/photos/user/101":
			body = `[{"userId":101,"isMain":true}]`
		case "/api/user/getUsers":
			body = "[ ]\n"
			if out.echoToken {
				body = `{"echo":"source-access-token"}`
			}
		case "/api/user/getReactUsers":
			body = `[]`
		case "/api/orders/type":
			status = 404
			body = "{ \"message\": \"not found\" }\n"
		case "/api/GPT/checkCompatibility":
			if out.postErr {
				return nil, errors.New("transport may include source-access-token")
			}
			status, body = out.postStatus, out.postBody
		case "/api/reactions/addNew":
			if out.postErr {
				return nil, errors.New("lost response")
			}
			status, body = out.postStatus, `{"IsSuccess":true,"IsLast":true}`
		case "/api/appuser/refresh":
			t.Fatal("POST response caused an automatic refresh/retry")
		default:
			t.Fatalf("unexpected upstream route %s", r.URL.Path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	out.handler, e = newDebugHTTP(s)
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func (f *debugFixture) request(method, path, body, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8788"+path, strings.NewReader(body))
	r.Host = "127.0.0.1:8788"
	r.Header.Set("Origin", "http://127.0.0.1:8788")
	r.Header.Set("X-Jardulik-Debug", f.handler.nonce)
	if method == http.MethodPost {
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Jardulik-Request-Id", id)
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}
func (f *debugFixture) posts() int {
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, "POST ") {
			n++
		}
	}
	return n
}

func TestDebugHTTPHostOriginNonceAndRoutes(t *testing.T) {
	f := newDebugFixture(t)
	for name, mutate := range map[string]func(*http.Request){
		"host":            func(r *http.Request) { r.Host = "evil.test" },
		"origin":          func(r *http.Request) { r.Header.Set("Origin", "https://evil.test") },
		"missing-nonce":   func(r *http.Request) { r.Header.Del("X-Jardulik-Debug") },
		"duplicate-nonce": func(r *http.Request) { r.Header.Add("X-Jardulik-Debug", f.handler.nonce) },
		"cross-site":      func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://127.0.0.1:8788/api/user/getMe", nil)
			r.Header.Set("Origin", "http://127.0.0.1:8788")
			r.Header.Set("X-Jardulik-Debug", f.handler.nonce)
			mutate(r)
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	if len(f.calls) != 0 {
		t.Fatal("rejected local requests reached upstream")
	}
	for _, path := range []string{"/api/appuser/login/telegram", "/api/appuser/refresh", "/api/user/clean", "/api/unknown"} {
		if w := f.request("GET", path, "", ""); w.Code != 404 {
			t.Fatal(path, w.Code)
		}
	}
	r := httptest.NewRequest("GET", "http://127.0.0.1:8788/debug/context", nil)
	r.Header.Set("Origin", "https://evil.test")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("context exposed cross-origin")
	}
	w = f.request("GET", "/debug/context", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"csrf_token"`) || strings.Contains(w.Body.String(), "source-access-token") || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unsafe context", w.Code, w.Body.String())
	}
	w = f.request("GET", "/", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "integrity=\"sha384-") || strings.Contains(w.Body.String(), "source-refresh-token") {
		t.Fatal("private UI exposed credentials or omitted SRI")
	}
}
func TestDebugHTTPBindsOwnIDsAndPreservesRawGET(t *testing.T) {
	f := newDebugFixture(t)
	for _, path := range []string{"/api/user/getMe?userId=102", "/api/user/getMe?userId=101&userId=102", "/api/user/getUser?viewerUserId=102&targetUserId=7", "/api/photos/user/102", "/api/user/getUsers?userId=102"} {
		w := f.request("GET", path, "", "")
		if w.Code != 400 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	if len(f.calls) != 0 {
		t.Fatal("foreign IDs reached upstream")
	}
	w := f.request("GET", "/api/user/getUsers?pageNumber=1&pageSize=3&ageMin=18&ageMax=25", "", "")
	if w.Code != 200 || w.Body.String() != "[ ]\n" {
		t.Fatal("raw body changed", w.Code, w.Body.String())
	}
	w = f.request("GET", "/api/orders/type?type=1", "", "")
	if w.Code != 404 || w.Body.String() != "{ \"message\": \"not found\" }\n" {
		t.Fatal("raw error response changed", w.Code, w.Body.String())
	}
	f.echoToken = true
	w = f.request("GET", "/api/user/getUsers", "", "")
	if w.Code != 502 || strings.Contains(w.Body.String(), "source-access-token") {
		t.Fatal("credential echo exposed", w.Code, w.Body.String())
	}
}
func TestDebugHTTPGPTSinglePostReplayAndStoredBytes(t *testing.T) {
	f := newDebugFixture(t)
	payload := `{"targetUserId":7}`
	key := "request-success-0001"
	w := f.request("POST", "/api/GPT/checkCompatibility", payload, key)
	if w.Code != 200 || w.Body.String() != f.postBody || w.Header().Get("X-Jardulik-Analysis-State") != "saved" {
		t.Fatal(w.Code, w.Body.String(), w.Header())
	}
	firstCalls := len(f.calls)
	w = f.request("POST", "/api/GPT/checkCompatibility", payload, key)
	if w.Code != 200 || w.Header().Get("X-Jardulik-Debug-Replay") != "true" || len(f.calls) != firstCalls || f.posts() != 1 {
		t.Fatal("POST was repeated", w.Code, f.calls)
	}
	saved, e := f.service.Store.LatestProfileAnalysis(context.Background(), 1, 7)
	if e != nil || saved == nil || !bytes.Equal(saved.ResponseBody, []byte(f.postBody)) || saved.Model == nil || *saved.Model != "reported-model" {
		t.Fatal("analysis bytes lost", e)
	}
	w = f.request("POST", "/api/GPT/checkCompatibility", `{"targetUserId":8}`, key)
	if w.Code != 409 || f.posts() != 1 {
		t.Fatal("request ID reused for another body", w.Code)
	}
}
func TestDebugHTTPPost401AndUnknownNeverReplay(t *testing.T) {
	for _, transport := range []bool{false, true} {
		t.Run(fmt.Sprint(transport), func(t *testing.T) {
			f := newDebugFixture(t)
			f.postStatus = 401
			f.postBody = `{"error":"unauthorized"}`
			f.postErr = transport
			w := f.request("POST", "/api/GPT/checkCompatibility", `{"targetUserId":7}`, "request-failed-0001")
			if w.Code != 401 && w.Code != 502 {
				t.Fatal(w.Code, w.Body.String())
			}
			if f.posts() != 1 {
				t.Fatal("POST retried", f.calls)
			}
			f.service.Store.Pause(context.Background(), 1, false)
			f.service.Store.SessionState(context.Background(), 1, "connected", "")
			w = f.request("POST", "/api/GPT/checkCompatibility", `{"targetUserId":7}`, "request-failed-0002")
			if w.Code != 409 || f.posts() != 1 {
				t.Fatal("new ID replayed unknown effect", w.Code, f.calls)
			}
		})
	}
}
func TestDebugHTTPWriteOwnerAdultAndUTF16Guards(t *testing.T) {
	f := newDebugFixture(t)
	for _, payload := range []string{`{"targetUserId":7,"fromUserId":102}`, `{"targetUserId":7,"fromUserId":101,"fromUserId":102}`, `{"targetUserId":7,"FromUserId":101}`} {
		if w := f.request("POST", "/api/GPT/checkCompatibility", payload, "request-invalid-0001"); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, payload := range []string{`{"userId":7,"reactionType":0}`, `{"userId":7,"reactionType":3,"message":" "}`, `{"userId":7,"reactionType":4,"isDeleted":false}`} {
		if w := f.request("POST", "/api/reactions/addNew", payload, "request-invalid-0002"); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	emoji, _ := json.Marshal(strings.Repeat("😀", 251))
	payload := `{"userId":7,"reactionType":3,"message":` + string(emoji) + `}`
	if w := f.request("POST", "/api/reactions/addNew", payload, "request-invalid-0003"); w.Code != 400 {
		t.Fatal("UTF16 limit missing", w.Code)
	}
	if len(f.calls) != 0 {
		t.Fatal("invalid writes made upstream calls", f.calls)
	}
}
func TestDebugHTTPLikeUsesSharedLedger(t *testing.T) {
	f := newDebugFixture(t)
	w := f.request("POST", "/api/reactions/addNew", `{"userId":7,"reactionType":1}`, "request-like-000001")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var count int
	if e := f.service.Store.DB.QueryRow("SELECT COUNT(*) FROM actions WHERE owner_id=1 AND profile_id=7 AND kind='Like' AND state='succeeded'").Scan(&count); e != nil || count != 1 {
		t.Fatal("shared Like ledger missing", count, e)
	}
	w = f.request("POST", "/api/reactions/addNew", `{"userId":7,"reactionType":1}`, "request-like-000002")
	if w.Code != 409 || f.posts() != 1 {
		t.Fatal("duplicate Like bypassed shared ledger", w.Code, f.calls)
	}
}
func TestDebugHTTPRuntimeOptInAndPosterIsolation(t *testing.T) {
	s := runtimeServiceFixture(t)
	s.Config.DebugListen = "0.0.0.0:0"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.RunBackground(ctx, RuntimeBotParser); e == nil {
		t.Fatal("non-loopback debug listener accepted")
	}
	wait, e := s.RunBackground(ctx, RuntimePoster)
	if e != nil {
		t.Fatal("poster attempted debug startup", e)
	}
	wait()
	s.Config.DebugListen = ""
	wait, e = s.RunBackground(ctx, RuntimeBotParser)
	if e != nil {
		t.Fatal("failed debug startup leaked runtime lock", e)
	}
	wait()
	s.Config.DebugListen = "127.0.0.1:0"
	s.Client.Base = "https://api.jardulik.app/api"
	wait, e = s.RunBackground(ctx, RuntimeBotParser)
	if e != nil {
		t.Fatal(e)
	}
	wait()
}
func TestDebugHTTPRejectsKnownUnderageTargetBeforePost(t *testing.T) {
	f := newDebugFixture(t)
	underage := archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		body := `{"userId":101,"age":33,"freezeState":false}`
		if r.URL.Path == "/api/user/getUser" {
			body = `{"userId":7,"age":17}`
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	f.service.Client.HTTP.Transport = underage
	w := f.request("POST", "/api/GPT/checkCompatibility", `{"targetUserId":7}`, "request-underage-001")
	if w.Code != 409 || f.posts() != 0 {
		t.Fatal("underage target passed write preflight", w.Code)
	}
}

type debugPipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func (l *debugPipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *debugPipeListener) Close() error { l.once.Do(func() { close(l.closed) }); return nil }
func (l *debugPipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8788}
}

func TestDebugHTTPShutdownJoinsCancelledHandlers(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer serverConnection.Close()
	defer clientConnection.Close()
	listener := &debugPipeListener{connections: make(chan net.Conn, 1), closed: make(chan struct{})}
	listener.connections <- serverConnection
	started, cancelled, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
		<-release
	})}
	go func() { runDebugHTTP(ctx, server, listener); close(done) }()
	requestDone := make(chan struct{})
	requestError := make(chan error, 1)
	go func() {
		defer close(requestDone)
		client := &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return clientConnection, nil }}, Timeout: 3 * time.Second}
		response, err := client.Get("http://" + listener.Addr().String())
		requestError <- err
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-started:
	case err := <-requestError:
		t.Fatal("request failed before handler", err)
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("runtime cancellation did not reach handler")
	}
	select {
	case <-done:
		t.Fatal("runtime finished before handler audit")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not join completed handler")
	}
	<-requestDone
}

func TestDebugHTTPAuditTriggersBackupFingerprint(t *testing.T) {
	f := newDebugFixture(t)
	s := f.service.Store
	ctx := context.Background()
	ss, err := s.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	beforeRecords, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	before := fullTestFingerprint(t, s)
	body := []byte(`{"fromUserId":101,"userId":7,"reactionType":2,"isDeleted":false,"message":""}`)
	call, fresh, err := f.handler.claim(ctx, ss, "request-backup-0001", "/reactions/addNew", body, 7, 2)
	if err != nil || !fresh {
		t.Fatal("audit request was not claimed", err)
	}
	if err = f.handler.finish(ctx, call.ID, "received", 200, []byte("{ \"IsSuccess\": true }\n"), ""); err != nil {
		t.Fatal(err)
	}
	afterRecords, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil || len(afterRecords) != len(beforeRecords)+1 {
		t.Fatal("audit-only addition did not enter durable state records", err)
	}
	after := fullTestFingerprint(t, s)
	if after == before {
		t.Fatal("audit-only addition did not dirty the daily backup")
	}
	repeated, fresh, err := f.handler.claim(ctx, ss, "request-backup-0001", "/reactions/addNew", body, 7, 2)
	if err != nil || fresh || repeated.ID != call.ID || repeated.State != "received" {
		t.Fatal("repeated audit request was not idempotent", err)
	}
	repeatedRecords, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil || strings.Join(repeatedRecords, "\n") != strings.Join(afterRecords, "\n") || fullTestFingerprint(t, s) != after {
		t.Fatal("unchanged repeated audit dirtied the daily backup", err)
	}
	if len(f.calls) != 0 {
		t.Fatal("audit contract test performed upstream HTTP", f.calls)
	}
}
