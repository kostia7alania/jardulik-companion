package companion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type contactClientFixture struct {
	store                *Store
	cfg                  Config
	client               *Client
	job                  ContactJob
	input                ContactEvidenceInput
	now                  time.Time
	status, getStatus    int
	body, contentType    string
	account, wantToken   string
	getHook, postHook    func()
	gets, posts, refresh atomic.Int64
}

func contactClientFixtureNew(t *testing.T) *contactClientFixture {
	t.Helper()
	s, cfg, input, _ := contactFixture(t)
	ctx := context.Background()
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	if err := s.initContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	f := &contactClientFixture{store: s, cfg: cfg, input: input, now: time.Now().UTC().Truncate(time.Second), status: 200, getStatus: 200, contentType: "application/json", account: `{"userId":500,"freezeState":false}`, wantToken: "offline-access"}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	batch, err := tx.ExecContext(ctx, "INSERT INTO relation_batches(owner_id,mutual,source_page,raw_json) VALUES(1,1,1,'[{\"userId\":77}]')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO relation_profiles(batch_id,owner_id,profile_id,position,raw_json) VALUES(?,1,77,0,'{\"userId\":77}')", id); err != nil {
		t.Fatal(err)
	}
	if err = s.queueMutualContactsTx(ctx, tx, 1, Session{UserID: 500, Generation: "initial"}, id, []Profile{{"userId": 77}}, f.now); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.job, err = s.NextContactJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/user/getMe":
			f.gets.Add(1)
			if r.Method != http.MethodGet || r.URL.Query().Get("userId") != "500" {
				t.Error("contact preflight used an unbound account")
			}
			if f.getHook != nil {
				f.getHook()
			}
			w.WriteHeader(f.getStatus)
			fmt.Fprint(w, f.account)
		case "/appuser/refresh":
			f.refresh.Add(1)
			if r.Method != http.MethodPut || f.posts.Load() != 0 {
				t.Error("refresh ran after contact POST")
			}
			fmt.Fprint(w, `{"token":"fresh-access","refreshToken":"fresh-refresh"}`)
		case "/telegram/getTG":
			f.posts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+f.wantToken || string(body) != `{"fromUserId":500,"targetUserId":77}` {
				t.Error("contact POST changed identity or payload", r.Method, string(body))
			}
			var state, saved string
			if err := s.DB.QueryRow("SELECT state,request_json FROM contact_requests WHERE id=?", f.job.ID).Scan(&state, &saved); err != nil || state != "sending" || saved != string(body) {
				t.Error("contact POST started without its durable claim", state, saved, err)
			}
			if f.postHook != nil {
				f.postHook()
			}
			w.Header().Set("Content-Type", f.contentType)
			if f.status >= 300 && f.status < 400 {
				w.Header().Set("Location", "/must-not-follow")
			}
			w.WriteHeader(f.status)
			fmt.Fprint(w, f.body)
		default:
			t.Error("contact retrieval called another endpoint", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	})
	f.cfg = cfg
	f.client = NewClient(s, cfg)
	f.client.HTTP.Transport = handlerTransport{handler}
	f.client.Now = func() time.Time { return f.now }
	return f
}

func (f *contactClientFixture) saved(t *testing.T) (string, int, string, string) {
	t.Helper()
	var state, body, reason string
	var status int
	if err := f.store.DB.QueryRow("SELECT state,http_status,response_body,reason FROM contact_requests WHERE id=?", f.job.ID).Scan(&state, &status, &body, &reason); err != nil {
		t.Fatal(err)
	}
	return state, status, body, reason
}

func TestContactClientSuccessWaitsForActualReceiptWithoutReplaying(t *testing.T) {
	for _, body := range []string{"", "OK", `{"url":"https://t.me/UnconfirmedName","future":9007199254740993}`} {
		t.Run(fmt.Sprintf("body-%d", len(body)), func(t *testing.T) {
			f := contactClientFixtureNew(t)
			f.body = body
			f.contentType = "text/plain"
			before, _ := f.store.Version(context.Background(), 1, f.input.VersionID)
			if err := f.client.RequestContact(context.Background(), f.job); err != nil {
				t.Fatal(err)
			}
			state, status, raw, _ := f.saved(t)
			if state != "awaiting_receipt" || status != 200 || raw != body || f.posts.Load() != 1 || f.gets.Load() != 1 {
				t.Fatal("successful side effect was lost or mistaken for a contact", state, status, raw)
			}
			if contact, err := f.store.ProfileTelegramContact(context.Background(), 1, 77); err != nil || contact != nil {
				t.Fatal("unverified response URL became a profile contact", err)
			}
			if err := f.client.RequestContact(context.Background(), f.job); err != nil || f.posts.Load() != 1 || f.gets.Load() != 1 {
				t.Fatal("awaiting card caused another request", err)
			}
			after, _ := f.store.Version(context.Background(), 1, f.input.VersionID)
			var actions int
			if err := f.store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 || before.RawJSON != after.RawJSON {
				t.Fatal("contact request altered raw profile or sent a reaction", err)
			}
		})
	}
}

func TestContactClientLifecycleAndCachedContactsNeedNoHTTP(t *testing.T) {
	for _, kind := range []string{"cached", "source-paused", "user-paused", "revoked", "generation", "stale-proof"} {
		t.Run(kind, func(t *testing.T) {
			f := contactClientFixtureNew(t)
			ctx := context.Background()
			switch kind {
			case "cached":
				if _, err := f.store.SaveContactEvidence(ctx, 1, f.input); err != nil {
					t.Fatal(err)
				}
			case "source-paused":
				f.store.SessionState(ctx, 1, "paused", "fixture")
			case "user-paused":
				f.store.Pause(ctx, 1, true)
			case "revoked":
				businessTestIngest(t, f.store, businessTestConnection(t, 1, 102, false))
			case "generation":
				if err := f.store.SaveSession(ctx, 1, Session{UserID: 500, Token: "new", Generation: "new"}); err != nil {
					t.Fatal(err)
				}
			case "stale-proof":
				f.now = f.now.Add(25 * time.Hour)
			}
			if err := f.client.RequestContact(ctx, f.job); err != nil || f.gets.Load() != 0 || f.posts.Load() != 0 || f.refresh.Load() != 0 {
				t.Fatal("ineligible contact job made HTTP", err)
			}
		})
	}
}

func TestContactClientSourceFailuresNeverReplayAndKeepRawReply(t *testing.T) {
	for _, test := range []struct {
		name, body, contentType, state, session string
		status                                  int
	}{
		{"auth", `{"error":"expired"}`, "application/json", "unknown", "reconnect_required", 401},
		{"access", `{"error":"denied"}`, "application/json", "rejected", "paused", 403},
		{"recipient", `{"Code":"recipient_restricted"}`, "application/json", "rejected", "connected", 403},
		{"quota", `{"error":"limited"}`, "application/json", "rejected", "paused", 429},
		{"server", `{"error":"failed"}`, "application/json", "unknown", "connected", 500},
		{"html", "<!doctype html><html>challenge</html>", "text/plain", "unknown", "paused", 200},
		{"redirect", "redirect", "text/plain", "unknown", "paused", 302},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := contactClientFixtureNew(t)
			f.body, f.contentType, f.status = test.body, test.contentType, test.status
			if err := f.client.RequestContact(context.Background(), f.job); err == nil || errors.Is(err, ErrContactPersistence) {
				t.Fatal("source failure was not classified as a durable source outcome", err)
			}
			state, status, raw, _ := f.saved(t)
			session, err := f.store.Session(context.Background(), 1)
			if err != nil || state != test.state || status != test.status || raw != test.body || session.State != test.session {
				t.Fatal("source result not retained", state, status, raw, session.State, err)
			}
			if err = f.client.RequestContact(context.Background(), f.job); err != nil || f.posts.Load() != 1 || f.gets.Load() != 1 || f.refresh.Load() != 0 {
				t.Fatal("failed contact POST repeated", err)
			}
		})
	}
}

func TestContactClientRefreshOnlyBeforeFirstPost(t *testing.T) {
	f := contactClientFixtureNew(t)
	ctx := context.Background()
	session, _ := f.store.Session(ctx, 1)
	session.Expires = f.now.Add(-time.Hour).Unix()
	if err := f.store.SaveSession(ctx, 1, session); err != nil {
		t.Fatal(err)
	}
	f.wantToken, f.status, f.body = "fresh-access", 401, `{"error":"expired again"}`
	if err := f.client.RequestContact(ctx, f.job); err == nil || f.refresh.Load() != 1 || f.gets.Load() != 1 || f.posts.Load() != 1 {
		t.Fatal("refresh was replayed after POST", err)
	}
}

func TestContactClientPreflightBackoffAndFrozenAccount(t *testing.T) {
	for _, kind := range []string{"network", "frozen", "malformed-freeze", "unknown-identity"} {
		t.Run(kind, func(t *testing.T) {
			f := contactClientFixtureNew(t)
			switch kind {
			case "network":
				f.getStatus = 500
			case "frozen":
				f.account = `{"userId":500,"freezeState":true}`
			case "malformed-freeze":
				f.account = `{"userId":500,"freezeState":"unknown"}`
			case "unknown-identity":
				f.account = `{"userId":600,"freezeState":false}`
			}
			if err := f.client.RequestContact(context.Background(), f.job); err == nil || errors.Is(err, ErrContactPersistence) {
				t.Fatal("preflight failure was lost", err)
			}
			if err := f.client.RequestContact(context.Background(), f.job); err != nil || f.gets.Load() != 1 || f.posts.Load() != 0 {
				t.Fatal("preflight failure retried immediately", err)
			}
		})
	}
}

func TestContactClientEarlyReceiptAndAccountRebindDuringPost(t *testing.T) {
	for _, kind := range []string{"receipt", "rebind"} {
		t.Run(kind, func(t *testing.T) {
			f := contactClientFixtureNew(t)
			second, err := OpenStore(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			f.body = "sent"
			f.postHook = func() {
				if kind == "rebind" {
					if err := second.SaveSession(context.Background(), 1, Session{UserID: 600, Generation: "replacement", Token: "replacement"}); err != nil {
						t.Error(err)
					}
					return
				}
				update := mutateBusinessUpdate(t, businessTestMessage(t, f.cfg.SourceBotID, 1, 101, "https://t.me/ConfirmedName"), func(body map[string]any) {
					body["business_message"].(map[string]any)["date"] = f.now.Unix()
				})
				businessTestIngest(t, second, update)
			}
			if kind == "rebind" {
				f.status = 401
			}
			err = f.client.RequestContact(context.Background(), f.job)
			state, status, raw, _ := f.saved(t)
			if kind == "receipt" {
				if err != nil || state != "received" || status != 200 || raw != "sent" {
					t.Fatal("HTTP completion clobbered early receipt or lost reply", state, status, raw, err)
				}
			} else {
				session, sessionErr := f.store.Session(context.Background(), 1)
				if err == nil || state != "unknown" || sessionErr != nil || session.UserID != 600 || session.State != "connected" {
					t.Fatal("old failure paused a replacement account", state, session.State, err, sessionErr)
				}
			}
		})
	}
}

func TestContactClientPreflightCannotOverwriteReplacementAccount(t *testing.T) {
	f := contactClientFixtureNew(t)
	f.getHook = func() {
		if err := f.store.SaveSession(context.Background(), 1, Session{UserID: 600, Generation: "replacement", Token: "replacement"}); err != nil {
			t.Error(err)
		}
	}
	if err := f.client.RequestContact(context.Background(), f.job); err == nil || f.posts.Load() != 0 || f.gets.Load() != 1 {
		t.Fatal("preflight crossed its original account binding", err)
	}
	session, err := f.store.Session(context.Background(), 1)
	if err != nil || session.UserID != 600 || session.Generation != "replacement" || string(session.Account) != "{}" {
		t.Fatal("old preflight overwrote replacement account cache", string(session.Account), err)
	}
}

func TestContactClientPreflightPersistenceErrorIsNotBackedOffAsNetwork(t *testing.T) {
	f := contactClientFixtureNew(t)
	if _, err := f.store.DB.Exec(`CREATE TRIGGER fail_contact_preflight BEFORE UPDATE OF account_json ON sessions BEGIN SELECT RAISE(ABORT,'fixture persistence failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.client.RequestContact(context.Background(), f.job); !errors.Is(err, ErrContactPersistence) || f.posts.Load() != 0 || f.gets.Load() != 1 {
		t.Fatal("preflight DB failure was hidden as a source outage", err)
	}
}

func TestContactClientConcurrentCallersSendOnePost(t *testing.T) {
	f := contactClientFixtureNew(t)
	second, err := OpenStore(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	other := NewClient(second, f.cfg)
	other.HTTP.Transport = f.client.HTTP.Transport
	other.Now = f.client.Now
	var workers sync.WaitGroup
	problems := make(chan error, 2)
	for _, client := range []*Client{f.client, other} {
		workers.Add(1)
		go func(client *Client) {
			defer workers.Done()
			problems <- client.RequestContact(context.Background(), f.job)
		}(client)
	}
	workers.Wait()
	close(problems)
	for err := range problems {
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.posts.Load() != 1 {
		t.Fatal("concurrent workers duplicated POST", f.posts.Load())
	}
}

func TestContactClientPersistenceFailureStopsAfterSingleAttempt(t *testing.T) {
	f := contactClientFixtureNew(t)
	if _, err := f.store.DB.Exec(`CREATE TRIGGER fail_contact_result BEFORE UPDATE OF state ON contact_requests WHEN OLD.state='sending' AND NEW.state='awaiting_receipt' BEGIN SELECT RAISE(ABORT,'fixture persistence failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.client.RequestContact(context.Background(), f.job); !errors.Is(err, ErrContactPersistence) || f.posts.Load() != 1 {
		t.Fatal("DB finalization failure was not surfaced", err)
	}
	if _, err := f.store.DB.Exec("DROP TRIGGER fail_contact_result"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecoverContactJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.client.RequestContact(context.Background(), f.job); err != nil || f.posts.Load() != 1 {
		t.Fatal("interrupted contact send was replayed", err)
	}
}

func TestContactClientTransportAndInterruptedBodyAreUnknown(t *testing.T) {
	for _, kind := range []string{"transport", "interrupted"} {
		t.Run(kind, func(t *testing.T) {
			f := contactClientFixtureNew(t)
			base := f.client.HTTP.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			f.client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/telegram/getTG" {
					return base.RoundTrip(r)
				}
				f.posts.Add(1)
				if kind == "transport" {
					return nil, errors.New("fixture transport failure")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &contactInterruptedBody{}}, nil
			})
			if err := f.client.RequestContact(context.Background(), f.job); err == nil || errors.Is(err, ErrContactPersistence) {
				t.Fatal("uncertain transport was not retained", err)
			}
			state, _, raw, _ := f.saved(t)
			if state != "unknown" || kind == "interrupted" && raw != "partial" {
				t.Fatal("partial body lost or uncertainty replayable", state, raw)
			}
			if err := f.client.RequestContact(context.Background(), f.job); err != nil || f.posts.Load() != 1 || f.gets.Load() != 1 {
				t.Fatal("transport failure repeated POST", err)
			}
		})
	}
}

type contactInterruptedBody struct{ read bool }

func (b *contactInterruptedBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, errors.New("fixture interrupted stream")
	}
	b.read = true
	return copy(p, "partial"), nil
}
func (*contactInterruptedBody) Close() error { return nil }

func TestRecipientRestrictedNeedsUnambiguousObservedCode(t *testing.T) {
	for body, want := range map[string]bool{
		`{"code":"recipient_restricted"}`:                    true,
		`{"Code":"recipient_restricted"}`:                    true,
		`{"code":"recipient_restricted","Code":"different"}`: false,
		`{"description":"recipient_restricted"}`:             false,
		`{"code":403}`:                                       false,
	} {
		if recipientRestricted([]byte(body)) != want {
			t.Fatal("recipient restriction guessed from another field", body)
		}
	}
}
