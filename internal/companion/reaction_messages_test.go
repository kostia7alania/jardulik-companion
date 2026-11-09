package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
)

type reactionMessageWire struct {
	ReactionType int    `json:"reactionType"`
	FromUserID   int64  `json:"fromUserId"`
	UserID       int64  `json:"userId"`
	Message      string `json:"message"`
}

func captureReactionMessages(t *testing.T, f *autoFixture) *[]reactionMessageWire {
	t.Helper()
	var messages []reactionMessageWire
	f.hook = func(r *http.Request) {
		if r.URL.Path != "/reactions/addNew" {
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var message reactionMessageWire
		if err = json.Unmarshal(raw, &message); err != nil {
			t.Fatal(err)
		}
		var persisted []byte
		err = f.worker.Service.Store.DB.QueryRow(`SELECT i.request_body FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.owner_id=1 AND i.profile_id=? AND a.state='pending'`, message.UserID).Scan(&persisted)
		if err != nil || !bytes.Equal(persisted, raw) {
			t.Fatalf("POST began without the exact committed intent: %s / %v", persisted, err)
		}
		if message.ReactionType != 1 || message.FromUserID != 101 {
			t.Fatalf("reaction identity changed: %+v", message)
		}
		messages = append(messages, message)
	}
	return &messages
}

func TestReactionMessageDefaultOffAndExactExplicit(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	f.add(t, 1, 8, 24, false)
	messages := captureReactionMessages(t, f)
	policy, err := s.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.Mode != "off" || len(policy.Templates) != 0 {
		t.Fatalf("unexpected default: %+v %v", policy, err)
	}
	if _, err = s.Client.LikeGuarded(ctx, 1, 7, nil); err != nil {
		t.Fatal(err)
	}
	exact := "  Привет 😀 @kostia7alania  \n"
	if _, err = s.Client.LikeWithMessageGuarded(ctx, 1, 8, exact, nil); err != nil {
		t.Fatal(err)
	}
	if len(*messages) != 2 || (*messages)[0].Message != "" || (*messages)[1].Message != exact {
		t.Fatalf("message changed: %+v", *messages)
	}
	var policies int
	if err = s.Store.DB.QueryRow("SELECT count(*) FROM reaction_message_settings").Scan(&policies); err != nil || policies != 0 {
		t.Fatalf("default unexpectedly seeded settings: %d %v", policies, err)
	}
}

func TestReactionMessageUTF16AndPolicyValidation(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{strings.Repeat("a", 500), true}, {strings.Repeat("😀", 250), true}, {strings.Repeat("😀", 251), false}, {strings.Repeat("a", 501), false}, {string([]byte{0xff}), false},
	} {
		if (validateReactionMessage(tc.value) == nil) != tc.valid {
			t.Fatalf("UTF-16/UTF-8 validation mismatch, bytes=%d", len(tc.value))
		}
	}
	f := newAutoFixture(t)
	s := f.worker.Service
	for _, tc := range []struct {
		mode     string
		messages []string
	}{
		{"rotate", nil}, {"first", nil}, {"bad", []string{"one"}}, {"first", []string{" "}}, {"off", []string{"a", "b", "c", "d"}},
	} {
		if _, err := s.Store.SetReactionMessagePolicy(context.Background(), 1, tc.mode, tc.messages); err == nil {
			t.Fatalf("invalid policy accepted: %+v", tc)
		}
	}
	if _, err := s.Client.LikeWithMessageGuarded(context.Background(), 1, 7, strings.Repeat("😀", 251), nil); !errors.Is(err, ErrInvalidReactionMessage) || len(f.requests) != 0 {
		t.Fatal("invalid explicit text reached preflight", err, f.requests)
	}
}

func TestReactionMessageConcurrentRotationAndOwnerIsolation(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	if _, err := s.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"one", "two", "three"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetReactionMessagePolicy(ctx, 2, "first", []string{"other owner"}); err != nil {
		t.Fatal(err)
	}
	type claimed struct {
		owner, id int64
		profile   StoredProfile
		session   Session
	}
	var jobs []claimed
	for _, owner := range []int64{1, 2} {
		for _, id := range []int64{7, 8, 9} {
			f.add(t, owner, id, 23, false)
			p, err := s.Profile(ctx, owner, id)
			if err != nil {
				t.Fatal(err)
			}
			session, err := s.Session(ctx, owner)
			if err != nil {
				t.Fatal(err)
			}
			action, err := s.ClaimOwnerAction(ctx, owner, id, f.now)
			if err != nil {
				t.Fatal(err)
			}
			jobs = append(jobs, claimed{owner, action, p, session})
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(jobs))
	for _, job := range jobs {
		wg.Add(1)
		go func(j claimed) {
			defer wg.Done()
			_, err := s.prepareLikeMessage(ctx, j.owner, j.id, j.profile, j.session, nil)
			errs <- err
		}(job)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []int64{1, 2} {
		rows, err := s.DB.Query("SELECT message FROM reaction_message_intents WHERE owner_id=?", owner)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var message string
			if err = rows.Scan(&message); err != nil {
				t.Fatal(err)
			}
			got = append(got, message)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(got)
		want := []string{"one", "three", "two"}
		if owner == 2 {
			want = []string{"other owner", "other owner", "other owner"}
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("owner %d received another policy or repeated a rotation slot: %v", owner, got)
		}
	}
}

func TestReactionMessageIntentFailureRollsBackCursorAndClaim(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	if _, err := s.Store.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"one", "two", "three"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`CREATE TRIGGER reaction_intent_failure BEFORE INSERT ON reaction_message_intents BEGIN SELECT RAISE(ABORT,'fixture intent failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client.LikeGuarded(ctx, 1, 7, nil); err == nil || len(f.sent) != 0 {
		t.Fatal("failed intent reached POST", err, f.sent)
	}
	p, err := s.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != 0 {
		t.Fatalf("failed intent advanced cursor: %+v %v", p, err)
	}
	var count int
	if err = s.Store.DB.QueryRow("SELECT count(*) FROM actions").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unsent claim left behind: %d %v", count, err)
	}
}

func TestReactionMessageUnknownNeverResendsOrRechooses(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	if _, err := s.Store.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"original", "second", "third"}); err != nil {
		t.Fatal(err)
	}
	messages := captureReactionMessages(t, f)
	f.results = []string{"transport-error"}
	if _, err := s.Client.LikeGuarded(ctx, 1, 7, nil); err == nil {
		t.Fatal("unconfirmed reaction reported success")
	}
	if _, err := s.Store.SetReactionMessagePolicy(ctx, 1, "first", []string{"replacement"}); err != nil {
		t.Fatal(err)
	}
	s.Store.Pause(ctx, 1, false)
	if _, err := s.Client.LikeGuarded(ctx, 1, 7, nil); !errors.Is(err, ErrActionBlocked) {
		t.Fatal("unknown was not blocked", err)
	}
	if len(*messages) != 1 || (*messages)[0].Message != "original" {
		t.Fatalf("unknown resent or changed message: %+v", *messages)
	}
	var saved, state string
	if err := s.Store.DB.QueryRow("SELECT i.message,a.state FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.profile_id=7").Scan(&saved, &state); err != nil || saved != "original" || state != "unknown" {
		t.Fatal("unknown intent changed", saved, state, err)
	}
}

func TestReactionMessageRechecksPauseAndClaimedGuardAfterIntent(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	if _, err := s.Store.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"one", "two"}); err != nil {
		t.Fatal(err)
	}
	claimedCalls := 0
	_, err := s.Client.LikeGuarded(ctx, 1, 7, func(ctx context.Context) error {
		if ClaimedLikeActionID(ctx) == 0 {
			return nil
		}
		claimedCalls++
		if claimedCalls == 2 {
			var intents int
			if err := s.Store.DB.QueryRow("SELECT count(*) FROM reaction_message_intents").Scan(&intents); err != nil || intents != 1 {
				t.Fatal("final guard did not follow durable intent", intents, err)
			}
			s.Store.Pause(context.Background(), 1, true)
			return ErrReactionPaused
		}
		return nil
	})
	if !errors.Is(err, ErrReactionPaused) || claimedCalls != 2 || len(f.sent) != 0 {
		t.Fatal("pause after preparation failed", err, claimedCalls, f.sent)
	}
	policy, err := s.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 0 {
		t.Fatal("unsent pause consumed rotation slot", policy, err)
	}
}

func TestReactionMessageNonemptyTextRequiresAdultTarget(t *testing.T) {
	for _, age := range []int64{0, 17} {
		t.Run(string(rune('a'+age)), func(t *testing.T) {
			f := newAutoFixture(t)
			s := f.worker.Service
			f.add(t, 1, 7, age, false)
			if _, err := s.Client.LikeWithMessageGuarded(context.Background(), 1, 7, "hello", nil); !errors.Is(err, ErrReactionMessageTargetAge) || len(f.sent) != 0 {
				t.Fatal("message reached unknown or underage target", err, f.sent)
			}
		})
	}
}

func TestReactionMessageManualAndAutoUsePolicy(t *testing.T) {
	t.Run("manual", func(t *testing.T) {
		s, _, _ := manualLikeFixture(t, `{"userId":101,"freezeState":false}`, `{"IsSuccess":true}`)
		if _, err := s.Store.SetReactionMessagePolicy(context.Background(), 1, "first", []string{"manual text"}); err != nil {
			t.Fatal(err)
		}
		uxTap(s, 1, "l:1")
		var message string
		if err := s.Store.DB.QueryRow("SELECT message FROM reaction_message_intents WHERE owner_id=1 AND profile_id=1").Scan(&message); err != nil || message != "manual text" {
			t.Fatal("manual path ignored policy", message, err)
		}
	})
	t.Run("auto", func(t *testing.T) {
		f := newAutoFixture(t)
		s := f.worker.Service
		f.add(t, 1, 7, 23, true)
		messages := captureReactionMessages(t, f)
		if _, err := s.Store.SetReactionMessagePolicy(context.Background(), 1, "first", []string{"automatic text"}); err != nil {
			t.Fatal(err)
		}
		if err := f.worker.Start(context.Background(), 1, "series", 1); err != nil {
			t.Fatal(err)
		}
		f.step(t)
		if len(*messages) != 1 || (*messages)[0].Message != "automatic text" {
			t.Fatalf("automatic path ignored policy: %+v", *messages)
		}
	})
}

func TestReactionMessageDebugSendsExactExplicitTextAndDoesNotConsumePolicy(t *testing.T) {
	f := newDebugFixture(t)
	ctx := context.Background()
	if _, err := f.service.Store.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"policy one", "policy two"}); err != nil {
		t.Fatal(err)
	}
	exact := "  direct 😀 text  "
	payload, _ := json.Marshal(map[string]any{"userId": 7, "reactionType": 1, "message": exact})
	var sent []byte
	base := f.service.Client.HTTP.Transport
	f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/reactions/addNew") {
			var err error
			sent, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(sent))
		}
		return base.RoundTrip(r)
	})
	w := f.request("POST", "/api/reactions/addNew", string(payload), "request-message-0001")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var decoded reactionMessageWire
	if err := json.Unmarshal(sent, &decoded); err != nil || decoded.Message != exact || decoded.ReactionType != 1 {
		t.Fatal("explicit debug message lost", decoded, err)
	}
	var logged, intent []byte
	if err := f.service.Store.DB.QueryRow("SELECT request_body FROM debug_api_calls WHERE request_id='request-message-0001'").Scan(&logged); err != nil {
		t.Fatal(err)
	}
	if err := f.service.Store.DB.QueryRow("SELECT request_body FROM reaction_message_intents WHERE owner_id=1 AND profile_id=7").Scan(&intent); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sent, logged) || !bytes.Equal(sent, intent) {
		t.Fatal("debug, action intent and actual request differ")
	}
	p, err := f.service.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != 0 {
		t.Fatal("explicit debug text consumed policy cursor", p, err)
	}
}
