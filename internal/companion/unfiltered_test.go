package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

type unfilteredSource struct {
	account, profiles string
	before            func(*http.Request)
	calls             []string
	queries           []url.Values
}

func unfilteredFixture(t *testing.T) (*Client, Session, *unfilteredSource) {
	t.Helper()
	s, _, _, _ := filterFixture(t)
	source := &unfilteredSource{
		account:  `{"userId":101,"type":1,"freezeState":false,"minAge":24,"maxAge":25}`,
		profiles: `[{"userId":9007199254740993,"age":27.5,"name":"Source"}]`,
	}
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unfiltered collection attempted mutation: %s", r.Method)
		}
		source.calls = append(source.calls, r.URL.Path)
		if source.before != nil {
			source.before(r)
		}
		switch r.URL.Path {
		case "/user/getMe":
			if r.URL.Query().Get("userId") != "101" {
				t.Fatal("account request used another source identity")
			}
			fmt.Fprint(w, source.account)
		case "/user/getUsers":
			source.queries = append(source.queries, r.URL.Query())
			fmt.Fprint(w, source.profiles)
		default:
			t.Fatalf("unfiltered collection used another endpoint: %s", r.URL.Path)
		}
	})}
	return s.Client, Session{UserID: 101, Generation: "1"}, source
}

func TestUsersUnfilteredFrozenUsesBroadAgesWithoutPersonalFiltersAndPreservesNumbers(t *testing.T) {
	client, expected, source := unfilteredFixture(t)
	ctx := context.Background()
	if _, err := client.Store.DB.ExecContext(ctx, filterSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Store.DB.ExecContext(ctx, `INSERT INTO feed_filters(owner_id,applied_json,draft_json,include_others) VALUES(1,'invalid UI preferences','invalid UI draft',1)`); err != nil {
		t.Fatal(err)
	}
	for _, others := range []bool{false, true} {
		profiles, err := client.UsersUnfilteredFrozen(ctx, 1, expected, 2, others)
		if err != nil || len(profiles) != 1 || profiles[0].ID() != 9007199254740993 {
			t.Fatalf("valid array or exact source ID lost: %+v %v", profiles, err)
		}
		if age, ok := profiles[0]["age"].(json.Number); !ok || age.String() != "27.5" {
			t.Fatal("source numbers were converted through float64")
		}
		all := "0"
		if others {
			all = "1"
		}
		want := url.Values{"userId": {"101"}, "pageNumber": {"2"}, "pageSize": {"150"}, "all": {all}, "ageMin": {"0"}, "ageMax": {"111"}}
		if query := source.queries[len(source.queries)-1]; !reflect.DeepEqual(query, want) {
			t.Fatalf("unfiltered query contains preferences or missing fields: %v", query)
		}
	}
	if !reflect.DeepEqual(source.calls, []string{"/user/getMe", "/user/getUsers", "/user/getMe", "/user/getUsers"}) {
		t.Fatalf("fresh entitlement check or read-only request contract changed: %v", source.calls)
	}
	var applied, draft string
	var others bool
	if err := client.Store.DB.QueryRowContext(ctx, `SELECT applied_json,draft_json,include_others FROM feed_filters WHERE owner_id=1`).Scan(&applied, &draft, &others); err != nil || applied != "invalid UI preferences" || draft != "invalid UI draft" || !others {
		t.Fatal("collection changed UI preferences", err)
	}
}

func TestUsersUnfilteredFrozenRejectsInvalidArrayAndProfileIDs(t *testing.T) {
	for _, raw := range []string{"null", `{}`, `{"items":[]}`, `[null]`, `[1]`, `[{}]`, `[{"userId":0}]`, `[{"userId":-1}]`, `[{"userId":1.5}]`, `[{"userId":9223372036854775808}]`, `[{"userId":true}]`, `[] []`} {
		t.Run(raw, func(t *testing.T) {
			client, expected, source := unfilteredFixture(t)
			source.profiles = raw
			profiles, err := client.UsersUnfilteredFrozen(context.Background(), 1, expected, 1, false)
			if err == nil || profiles != nil {
				t.Fatalf("invalid source response accepted: %s %+v %v", raw, profiles, err)
			}
		})
	}
	client, expected, source := unfilteredFixture(t)
	source.profiles = " \n [] \t"
	if profiles, err := client.UsersUnfilteredFrozen(context.Background(), 1, expected, 1, false); err != nil || len(profiles) != 0 {
		t.Fatal("valid empty array rejected", err)
	}
}

func TestUsersUnfilteredFrozenChecksFreshEntitlementPauseAndSession(t *testing.T) {
	for _, kind := range []string{"invalid-page", "stale-generation", "other-source", "disconnected", "paused-before", "premium", "frozen", "invalid-freeze", "pause-during-account", "session-during-account", "pause-during-feed", "session-during-feed", "disconnect-during-feed"} {
		t.Run(kind, func(t *testing.T) {
			client, expected, source := unfilteredFixture(t)
			ctx := context.Background()
			page := 1
			switch kind {
			case "invalid-page":
				page = 0
			case "stale-generation":
				expected.Generation = "stale"
			case "other-source":
				expected.UserID = 102
			case "disconnected":
				client.Store.SessionState(ctx, 1, "disconnected", "fixture")
			case "paused-before":
				client.Store.Pause(ctx, 1, true)
			case "premium":
				source.account = `{"userId":101,"type":0,"freezeState":false}`
			case "frozen":
				source.account = `{"userId":101,"type":1,"freezeState":true}`
			case "invalid-freeze":
				source.account = `{"userId":101,"type":1,"freezeState":"true"}`
			}
			source.before = func(r *http.Request) {
				if r.URL.Path == "/user/getMe" && kind == "pause-during-account" || r.URL.Path == "/user/getUsers" && kind == "pause-during-feed" {
					client.Store.Pause(ctx, 1, true)
				}
				if r.URL.Path == "/user/getMe" && kind == "session-during-account" || r.URL.Path == "/user/getUsers" && kind == "session-during-feed" {
					if err := client.Store.SaveSession(ctx, 1, Session{Token: "changed", Refresh: "changed", UserID: 101, Generation: "changed"}); err != nil {
						t.Fatal(err)
					}
				}
				if r.URL.Path == "/user/getUsers" && kind == "disconnect-during-feed" {
					client.Store.SessionState(ctx, 1, "disconnected", "fixture")
				}
			}
			profiles, err := client.UsersUnfilteredFrozen(ctx, 1, expected, page, false)
			if err == nil || profiles != nil {
				t.Fatalf("invalidated collection returned profiles: %+v %v", profiles, err)
			}
			if kind == "premium" && !errors.Is(err, ErrFiltersPremium) {
				t.Fatal("Premium denial lost typed reason", err)
			}
			if kind == "frozen" && !errors.Is(err, ErrAccountFrozen) {
				t.Fatal("frozen account lost typed reason", err)
			}
			if kind == "invalid-page" || kind == "stale-generation" || kind == "other-source" || kind == "disconnected" {
				if len(source.calls) != 0 {
					t.Fatalf("invalid identity or page reached HTTP: %v", source.calls)
				}
			}
			if kind != "pause-during-feed" && kind != "session-during-feed" && kind != "disconnect-during-feed" && len(source.queries) != 0 {
				t.Fatal("failed preflight still requested profiles")
			}
		})
	}
}
