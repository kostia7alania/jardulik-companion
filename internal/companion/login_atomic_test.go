package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type loginBindingSnapshot struct {
	Encrypted                       []byte
	Upstream, Expires               int64
	State, Reason, Account, Updated string
}

func loginBinding(t *testing.T, s *Store) *loginBindingSnapshot {
	t.Helper()
	var snapshot loginBindingSnapshot
	err := s.DB.QueryRow(`SELECT encrypted,upstream_id,expires_at,state,last_error,account_json,updated_at FROM sessions WHERE owner_id=1`).Scan(&snapshot.Encrypted, &snapshot.Upstream, &snapshot.Expires, &snapshot.State, &snapshot.Reason, &snapshot.Account, &snapshot.Updated)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return &snapshot
}

func loginSuccessEvents(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow(`SELECT count(*) FROM events WHERE owner_id=1 AND kind='connect_success'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func atomicLoginFixture(t *testing.T, existing bool) (*Store, Config, *Client, string) {
	t.Helper()
	s, cfg := testStore(t)
	ctx := context.Background()
	if err := s.User(ctx, 1, ""); err != nil {
		t.Fatal(err)
	}
	if existing {
		if err := s.SaveSession(ctx, 1, Session{Token: "old-token", Refresh: "old-refresh", UserID: 9, Generation: "old-generation"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.Exec(`UPDATE sessions SET state='paused',last_error='unexpected_response_or_challenge',account_json='{"userId":9,"old":true}',updated_at='2026-01-01 00:00:00' WHERE owner_id=1`); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1794200000, 0)
	raw, key := signedData(t, 1, cfg.SourceBotID, now)
	originalKey := telegramPublicKey
	telegramPublicKey = key
	t.Cleanup(func() { telegramPublicKey = originalKey })
	client := NewClient(s, cfg)
	client.Now = func() time.Time { return now }
	return s, cfg, client, raw
}

const validAtomicLogin = `{"code":201,"isReged":true,"token":"candidate-access-token","refreshToken":"candidate-refresh-token","userId":5}`

func TestLoginAccountCommitsOnlyAfterVerifiedIdentity(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			s, _, client, raw := atomicLoginFixture(t, existing)
			ctx := context.Background()
			before := loginBinding(t, s)
			s.Event(ctx, 1, "earlier-event")
			if _, err := s.DB.Exec(`UPDATE users SET paused=1 WHERE id=1`); err != nil {
				t.Fatal(err)
			}
			accountRaw := "{\n  \"userId\": 5, \"type\": 1, \"unknown\": {\"number\": 9007199254740993, \"text\": \"untouched\"}\n}"
			calls := []string{}
			client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls = append(calls, r.Method+" "+r.URL.RequestURI())
				if !reflect.DeepEqual(loginBinding(t, s), before) || loginSuccessEvents(t, s) != 0 {
					t.Fatal("candidate session became visible before identity verification")
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/appuser/login/telegram":
					var request map[string]string
					if json.NewDecoder(r.Body).Decode(&request) != nil || len(request) != 1 || request["initData"] != raw || r.Header.Get("Authorization") != "" {
						t.Fatal("login credential changed or old bearer leaked")
					}
					fmt.Fprint(w, validAtomicLogin)
				case "/user/getMe":
					if r.Header.Get("Authorization") != "Bearer candidate-access-token" {
						t.Fatal("identity verification used the previous token")
					}
					fmt.Fprint(w, accountRaw)
				default:
					t.Fatalf("unexpected auth request: %s", r.URL.Path)
				}
			})}
			account, err := client.LoginAccount(ctx, 1, raw)
			if err != nil || account.ID() != 5 || account["unknown"].(map[string]any)["number"] != json.Number("9007199254740993") {
				t.Fatalf("verified login failed: %v", err)
			}
			if !reflect.DeepEqual(calls, []string{"POST /appuser/login/telegram", "GET /user/getMe?userId=5"}) {
				t.Fatalf("wrong auth flow: %v", calls)
			}
			stored, err := s.Session(ctx, 1)
			if err != nil || stored.State != "connected" || stored.UserID != 5 || stored.Token != "candidate-access-token" || stored.Refresh != "candidate-refresh-token" || stored.Generation == "" || stored.Generation == "old-generation" || string(stored.Account) != accountRaw {
				t.Fatalf("committed login differs from the verified account: %v", err)
			}
			binding := loginBinding(t, s)
			if binding.Reason != "" || bytes.Contains(binding.Encrypted, []byte(stored.Token)) || !s.Paused(ctx, 1) || loginSuccessEvents(t, s) != 1 {
				t.Fatal("credentials, pause preference or success event were saved incorrectly")
			}
			var earlier int
			if err = s.DB.QueryRow(`SELECT count(*) FROM events WHERE owner_id=1 AND kind='earlier-event'`).Scan(&earlier); err != nil || earlier != 1 {
				t.Fatal("prior events were changed")
			}
		})
	}
}

type loginTransport func(*http.Request) (*http.Response, error)

func (f loginTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLoginAccountFailurePreservesPreviousSession(t *testing.T) {
	for _, tc := range []struct {
		name, stage, contentType, body string
		status                         int
		transport                      bool
	}{
		{name: "login_500", stage: "login", status: 500, body: `{}`},
		{name: "login_challenge", stage: "login", status: 200, contentType: "text/html", body: `<html>challenge</html>`},
		{name: "login_unregistered", stage: "login", status: 200, body: `{"code":201,"isReged":false,"token":"x","refreshToken":"r","userId":5}`},
		{name: "login_transport", stage: "login", transport: true},
		{name: "account_500", stage: "account", status: 500, body: `{}`},
		{name: "account_401", stage: "account", status: 401, body: `{}`},
		{name: "account_403", stage: "account", status: 403, body: `{}`},
		{name: "account_429", stage: "account", status: 429, body: `{}`},
		{name: "account_challenge", stage: "account", status: 200, contentType: "text/html", body: `<html>challenge</html>`},
		{name: "account_null", stage: "account", status: 200, body: `null`},
		{name: "account_array", stage: "account", status: 200, body: `[]`},
		{name: "account_identity_mismatch", stage: "account", status: 200, body: `{"userId":6}`},
		{name: "account_transport", stage: "account", transport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, client, raw := atomicLoginFixture(t, true)
			before := loginBinding(t, s)
			calls := 0
			client.HTTP.Transport = loginTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				stage := "login"
				if r.URL.Path == "/user/getMe" {
					stage = "account"
				} else if r.URL.Path != "/appuser/login/telegram" {
					t.Fatalf("unexpected refresh or replay: %s", r.URL.Path)
				}
				w := httptest.NewRecorder()
				if stage != tc.stage {
					fmt.Fprint(w, validAtomicLogin)
				} else {
					if tc.transport {
						return nil, io.ErrUnexpectedEOF
					}
					w.Header().Set("Content-Type", tc.contentType)
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
				}
				return w.Result(), nil
			})
			if _, err := client.LoginAccount(context.Background(), 1, raw); err == nil {
				t.Fatal("unverified login accepted")
			}
			expectedCalls := 1
			if tc.stage == "account" {
				expectedCalls = 2
			}
			if calls != expectedCalls || !reflect.DeepEqual(loginBinding(t, s), before) || loginSuccessEvents(t, s) != 0 {
				t.Fatal("failed login mutated the prior binding or retried a request")
			}
		})
	}
}

func TestLoginAccountRollbackAndConcurrentBindingChange(t *testing.T) {
	for _, scenario := range []string{"event_failure", "replace", "pause", "disconnect", "new_binding", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			s, cfg, client, raw := atomicLoginFixture(t, scenario != "new_binding")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			other, err := OpenStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if scenario == "event_failure" {
				if _, err = s.DB.Exec(`CREATE TRIGGER fail_connect BEFORE INSERT ON events WHEN NEW.kind='connect_success' BEGIN SELECT RAISE(ABORT,'forced login event failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			before := loginBinding(t, s)
			client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/appuser/login/telegram" {
					fmt.Fprint(w, validAtomicLogin)
					return
				}
				switch scenario {
				case "replace", "new_binding":
					err = other.SaveSession(ctx, 1, Session{Token: "other-session", Refresh: "other-refresh", UserID: 7, Generation: "other-generation"})
				case "pause":
					_, err = other.DB.ExecContext(ctx, `UPDATE sessions SET state='reconnect_required',last_error='other-process' WHERE owner_id=1`)
				case "disconnect":
					err = other.Disconnect(ctx, 1)
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
				before = loginBinding(t, s)
				fmt.Fprint(w, `{"userId":5,"type":1}`)
			})}
			if _, err = client.LoginAccount(ctx, 1, raw); err == nil {
				t.Fatal("failed or stale login committed")
			}
			if !reflect.DeepEqual(loginBinding(t, s), before) || loginSuccessEvents(t, s) != 0 {
				t.Fatal("failed transaction or stale login overwrote another binding")
			}
		})
	}
}

func TestLoginAccountInvalidLaunchDoesNotCallUpstream(t *testing.T) {
	s, _, client, raw := atomicLoginFixture(t, true)
	before := loginBinding(t, s)
	client.HTTP.Transport = loginTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid launch reached upstream")
		return nil, io.ErrUnexpectedEOF
	})
	if err := client.Login(context.Background(), 1, strings.Replace(raw, "sender", "group", 1)); err == nil || !reflect.DeepEqual(loginBinding(t, s), before) {
		t.Fatal("invalid launch accepted or altered the old binding")
	}
}

func TestLoginFailureDoesNotPublishFirstSession(t *testing.T) {
	s, _, client, raw := atomicLoginFixture(t, false)
	calls := 0
	client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/appuser/login/telegram" {
			fmt.Fprint(w, validAtomicLogin)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{}`)
	})}
	if err := client.Login(context.Background(), 1, raw); err == nil {
		t.Fatal("initial login succeeded without account verification")
	}
	if calls != 2 || loginBinding(t, s) != nil || loginSuccessEvents(t, s) != 0 {
		t.Fatal("failed initial login published credentials or success")
	}
}
