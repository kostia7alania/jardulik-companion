package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type positiveReactionFixture struct {
	auto  *autoFixture
	posts [][]byte
	fire  any
}

func newPositiveReactionFixture(t *testing.T, fire any) *positiveReactionFixture {
	t.Helper()
	f := newAutoFixture(t)
	out := &positiveReactionFixture{auto: f, fire: fire}
	base := f.worker.Service.Client.HTTP.Transport
	f.worker.Service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/user/getMe" {
			account := map[string]any{"userId": 101, "age": 33, "type": 1, "freezeState": false}
			if out.fire != nil {
				account["fire"] = out.fire
			}
			raw, err := json.Marshal(account)
			if err != nil {
				t.Fatal(err)
			}
			return albumTestResponse(r, http.StatusOK, string(raw)), nil
		}
		if r.URL.Path == "/reactions/addNew" {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var body map[string]any
			if err = json.Unmarshal(raw, &body); err != nil {
				t.Fatal(err)
			}
			target, valid := body["userId"].(float64)
			if !valid {
				t.Fatal("positive reaction has no target")
			}
			var intent []byte
			var message string
			if err = f.worker.Service.Store.DB.QueryRowContext(r.Context(), `SELECT i.request_body,i.message FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.owner_id=1 AND i.profile_id=? AND a.state='pending' ORDER BY i.action_id DESC LIMIT 1`, int64(target)).Scan(&intent, &message); err != nil {
				t.Fatalf("POST started before its durable intent: %v", err)
			}
			if !bytes.Equal(intent, raw) || body["message"] != message {
				t.Fatal("wire body differs from the committed positive-reaction intent")
			}
			out.posts = append(out.posts, append([]byte(nil), raw...))
		}
		return base.RoundTrip(r)
	})
	return out
}

func (f *positiveReactionFixture) profile(t *testing.T, id, age int64) {
	t.Helper()
	f.auto.add(t, 1, id, age, false)
	p := f.auto.full[id]
	p["birthday"] = time.Date(f.auto.now.Year()-int(age), time.January, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05")
	if _, err := f.auto.worker.Service.Store.ArchiveViewed(context.Background(), 1, []Profile{p}, "positive-reaction-fixture"); err != nil {
		t.Fatal(err)
	}
}

func (f *positiveReactionFixture) policy(t *testing.T, mode string, cutoff int) {
	t.Helper()
	config := ReactionMessagePolicyConfig{Mode: mode, TelegramUsername: "fixture_user", SuperBeforeAge: cutoff}
	if mode != "off" {
		config.Templates = []string{"Первый текст @fixture_user", "Второй текст @fixture_user", "Третий текст @fixture_user"}
	}
	if _, err := f.auto.worker.Service.Store.ConfigureReactionMessagePolicy(context.Background(), 1, config); err != nil {
		t.Fatal(err)
	}
}

func TestPositiveReactionSuperPersistsExactExplicitMessageBeforePost(t *testing.T) {
	f := newPositiveReactionFixture(t, int64(7))
	f.profile(t, 81, 22)
	f.policy(t, "rotate", 23)
	store := f.auto.worker.Service.Store
	if _, err := store.DB.Exec("UPDATE reaction_message_settings SET next_cursor=8 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	message := "  Привет, 😀 <не меняй>& @fixture_user \n"
	result, err := f.auto.worker.Service.Client.PositiveReactionGuarded(context.Background(), 1, 81, PositiveSuperLike, &message, nil)
	if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 1 {
		t.Fatalf("explicit Super did not send once: result=%+v posts=%d err=%v", result, len(f.posts), err)
	}
	var body map[string]any
	if err = json.Unmarshal(f.posts[0], &body); err != nil {
		t.Fatal(err)
	}
	if body["reactionType"] != float64(3) || body["isDeleted"] != false || body["fromUserId"] != float64(101) || body["userId"] != float64(81) || body["message"] != message {
		t.Fatal("explicit Super identity, type or exact text changed")
	}
	policy, err := store.ReactionMessagePolicy(context.Background(), 1)
	if err != nil || policy.NextCursor != 8 {
		t.Fatalf("explicit Super consumed the owner rotation: %+v %v", policy, err)
	}
}

func TestPositiveReactionAutomaticCutoffAndDisabledPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		age    int64
		cutoff int
		mode   string
		fire   any
		want   PositiveReactionType
	}{
		{name: "adult lower boundary", age: 18, cutoff: 23, mode: "first", fire: int64(1), want: PositiveSuperLike},
		{name: "last super age", age: 22, cutoff: 23, mode: "first", fire: int64(7), want: PositiveSuperLike},
		{name: "first ordinary age", age: 23, cutoff: 23, mode: "first", fire: int64(0), want: PositiveLike},
		{name: "cutoff27 last super age", age: 26, cutoff: 27, mode: "first", fire: int64(7), want: PositiveSuperLike},
		{name: "cutoff27 first ordinary age", age: 27, cutoff: 27, mode: "first", fire: int64(0), want: PositiveLike},
		{name: "cutoff112 all super", age: 23, cutoff: 112, mode: "first", fire: int64(7), want: PositiveSuperLike},
		{name: "cutoff18 has no adult super ages", age: 18, cutoff: 18, mode: "first", fire: int64(0), want: PositiveLike},
		{name: "cutoff disabled", age: 22, cutoff: 0, mode: "first", fire: nil, want: PositiveLike},
		{name: "all policy off", age: 22, cutoff: 0, mode: "off", fire: int64(0), want: PositiveLike},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPositiveReactionFixture(t, tc.fire)
			f.profile(t, 81, tc.age)
			f.policy(t, tc.mode, tc.cutoff)
			if tc.age > 25 {
				// Keep the fixture's age range from masking the cutoff boundary.
				filters := DefaultFeedFilters()
				filters.AgeMax = tc.age
				raw, err := json.Marshal(filters)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.auto.worker.Service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.auto.worker.Service.Client.AutomaticLikeGuarded(context.Background(), 1, 81, nil)
			if err != nil || result.Decision != ReactionSuccess || len(f.posts) != 1 {
				t.Fatalf("automatic reaction failed: %+v posts=%d err=%v", result, len(f.posts), err)
			}
			var body map[string]any
			if err = json.Unmarshal(f.posts[0], &body); err != nil {
				t.Fatal(err)
			}
			if body["reactionType"] != float64(tc.want) {
				t.Fatalf("age %d cutoff %d selected type %v, want %d", tc.age, tc.cutoff, body["reactionType"], tc.want)
			}
			if tc.mode == "off" && body["message"] != "" {
				t.Fatal("off mode acquired an unsolicited greeting")
			}
		})
	}
}

func TestPositiveReactionSuperFireFailureDoesNotPostOrConsumeCursor(t *testing.T) {
	for _, tc := range []struct {
		name string
		fire any
		want error
	}{
		{name: "zero", fire: int64(0), want: ErrReactionFireUnavailable},
		{name: "missing", fire: nil, want: ErrReactionFireUnknown},
		{name: "fractional", fire: 1.5, want: ErrReactionFireUnknown},
		{name: "not numeric", fire: "unavailable", want: ErrReactionFireUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPositiveReactionFixture(t, tc.fire)
			f.profile(t, 81, 22)
			f.policy(t, "rotate", 23)
			store := f.auto.worker.Service.Store
			if _, err := store.DB.Exec("UPDATE reaction_message_settings SET next_cursor=8 WHERE owner_id=1"); err != nil {
				t.Fatal(err)
			}
			_, err := f.auto.worker.Service.Client.PositiveReactionGuarded(context.Background(), 1, 81, PositiveSuperLike, nil, nil)
			if !errors.Is(err, tc.want) || len(f.posts) != 0 {
				t.Fatalf("fire failure reached POST or lost its reason: posts=%d err=%v", len(f.posts), err)
			}
			policy, err := store.ReactionMessagePolicy(context.Background(), 1)
			if err != nil || policy.NextCursor != 8 {
				t.Fatalf("failed fire preflight consumed its global slot: %+v %v", policy, err)
			}
			var intents int
			if err = store.DB.QueryRow("SELECT COUNT(*) FROM reaction_message_intents WHERE owner_id=1 AND profile_id=81").Scan(&intents); err != nil || intents != 0 {
				t.Fatalf("unsent fire failure retained an intent: %d %v", intents, err)
			}
		})
	}
}

func TestPositiveReactionSuperRejectsEmptyOrOversizeExplicitMessage(t *testing.T) {
	for _, message := range []string{"", " \n\t", strings.Repeat("😀", 251)} {
		t.Run(fmt.Sprint(len(message)), func(t *testing.T) {
			f := newPositiveReactionFixture(t, int64(7))
			f.profile(t, 81, 22)
			f.policy(t, "rotate", 23)
			_, err := f.auto.worker.Service.Client.PositiveReactionGuarded(context.Background(), 1, 81, PositiveSuperLike, &message, nil)
			if err == nil || len(f.posts) != 0 {
				t.Fatalf("invalid Super message reached POST: posts=%d err=%v", len(f.posts), err)
			}
			policy, err := f.auto.worker.Service.Store.ReactionMessagePolicy(context.Background(), 1)
			if err != nil || policy.NextCursor != 0 {
				t.Fatalf("invalid explicit message changed policy: %+v %v", policy, err)
			}
		})
	}
}

func TestPositiveReactionAdultAgeAndDOBGuards(t *testing.T) {
	for _, tc := range []struct {
		name     string
		age      int64
		minorDOB bool
	}{
		{name: "underage age", age: 17},
		{name: "adult age but minor DOB", age: 23, minorDOB: true},
	} {
		for _, kind := range []PositiveReactionType{PositiveLike, PositiveSuperLike} {
			t.Run(fmt.Sprintf("%s/type_%d", tc.name, kind), func(t *testing.T) {
				f := newPositiveReactionFixture(t, int64(7))
				f.profile(t, 81, tc.age)
				if tc.minorDOB {
					p := f.auto.full[81]
					p["birthday"] = time.Date(f.auto.now.Year()-17, time.January, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05")
					if _, err := f.auto.worker.Service.Store.ArchiveViewed(context.Background(), 1, []Profile{p}, "positive-minor-date-fixture"); err != nil {
						t.Fatal(err)
					}
				}
				f.policy(t, "first", 23)
				if _, err := f.auto.worker.Service.Client.PositiveReactionGuarded(context.Background(), 1, 81, kind, nil, nil); err == nil {
					t.Fatalf("positive type %d accepted a minor profile", kind)
				}
				if len(f.posts) != 0 {
					t.Fatal("adult age/DOB rejection reached POST")
				}
			})
		}
	}
}

func TestPositiveReactionUnknownIntentNeverReplaysAcrossPositiveTypes(t *testing.T) {
	f := newPositiveReactionFixture(t, int64(7))
	f.profile(t, 81, 22)
	f.policy(t, "rotate", 23)
	f.auto.results = []string{"transport-error"}
	store := f.auto.worker.Service.Store
	client := f.auto.worker.Service.Client
	_, err := client.PositiveReactionGuarded(context.Background(), 1, 81, PositiveSuperLike, nil, nil)
	if err == nil || len(f.posts) != 1 {
		t.Fatalf("fixture did not make one unconfirmed Super request: posts=%d err=%v", len(f.posts), err)
	}
	var original []byte
	var state string
	if err = store.DB.QueryRow(`SELECT i.request_body,a.state FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id WHERE i.owner_id=1 AND i.profile_id=81`).Scan(&original, &state); err != nil || state != "unknown" || !bytes.Equal(original, f.posts[0]) {
		t.Fatalf("unknown lost its exact outgoing intent: state=%s err=%v", state, err)
	}
	store.Pause(context.Background(), 1, false)
	for _, kind := range []PositiveReactionType{PositiveLike, PositiveSuperLike} {
		if _, err = client.PositiveReactionGuarded(context.Background(), 1, 81, kind, nil, nil); !errors.Is(err, ErrActionBlocked) {
			t.Fatalf("unknown Super became replayable as positive type %d: %v", kind, err)
		}
	}
	if _, err = client.LikeGuarded(context.Background(), 1, 81, nil); !errors.Is(err, ErrActionBlocked) {
		t.Fatalf("ordinary Like wrapper bypassed unknown Super: %v", err)
	}
	var after []byte
	if err = store.DB.QueryRow(`SELECT request_body FROM reaction_message_intents WHERE owner_id=1 AND profile_id=81`).Scan(&after); err != nil || !bytes.Equal(original, after) || len(f.posts) != 1 {
		t.Fatal("blocked replay changed the immutable request or made another POST")
	}
	policy, err := store.ReactionMessagePolicy(context.Background(), 1)
	if err != nil || policy.NextCursor != 1 {
		t.Fatalf("unknown/replay advanced or refunded the sent slot: %+v %v", policy, err)
	}
}
