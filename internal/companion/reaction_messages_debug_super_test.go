package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestPositiveDebugSuperUsesOneSharedActionAndAudit(t *testing.T) {
	f := newDebugFixture(t)
	prior := f.service.Client.HTTP.Transport
	var posted []byte
	f.service.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/reactions/addNew" {
			var err error
			posted, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(posted))
			var persisted []byte
			if err = f.service.Store.DB.QueryRow(`SELECT i.request_body FROM reaction_message_intents i JOIN debug_api_calls d ON d.action_id=i.action_id AND d.owner_id=i.owner_id AND d.target_id=i.profile_id WHERE d.request_id='debug-super-once' AND d.reaction_type=3 AND i.reaction_type=3`).Scan(&persisted); err != nil || !bytes.Equal(posted, persisted) {
				t.Fatal("POST preceded its linked exact intent", err)
			}
		}
		response, err := prior.RoundTrip(r)
		if err == nil && r.URL.Path == "/api/user/getMe" {
			response.Body.Close()
			response.Body = io.NopCloser(bytes.NewBufferString(`{"userId":101,"age":33,"type":1,"freezeState":false,"fire":7}`))
		}
		return response, err
	})
	payload := `{"userId":7,"reactionType":3,"message":"Привет! @fixture_user"}`
	w := f.request("POST", "/api/reactions/addNew", payload, "debug-super-once")
	if w.Code != 200 || f.posts() != 1 {
		t.Fatalf("Super not sent exactly once: %d %s %v", w.Code, w.Body.String(), f.calls)
	}
	var wire struct {
		Kind    int    `json:"reactionType"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(posted, &wire); err != nil || wire.Kind != 3 || wire.Message != "Привет! @fixture_user" {
		t.Fatalf("wrong Super body: %s %v", posted, err)
	}
	var actionCount, linkedCount int
	if err := f.service.Store.DB.QueryRow("SELECT count(*) FROM actions WHERE owner_id=1 AND profile_id=7 AND kind='Like' AND state='succeeded'").Scan(&actionCount); err != nil {
		t.Fatal(err)
	}
	if err := f.service.Store.DB.QueryRow("SELECT count(*) FROM debug_api_calls WHERE request_id='debug-super-once' AND state='received' AND action_id IS NOT NULL").Scan(&linkedCount); err != nil {
		t.Fatal(err)
	}
	if actionCount != 1 || linkedCount != 1 {
		t.Fatal("Super audit/action linkage missing")
	}
	budget, err := f.service.Store.positiveBudget(context.Background(), 1, f.service.Client.Now())
	if err != nil || budget.Count != 1 {
		t.Fatalf("linked Super counted twice: %+v %v", budget, err)
	}
	w = f.request("POST", "/api/reactions/addNew", payload, "debug-super-once")
	if w.Code != 200 || f.posts() != 1 || w.Header().Get("X-Jardulik-Debug-Replay") != "true" {
		t.Fatal("exact request replay sent another POST")
	}
	w = f.request("POST", "/api/reactions/addNew", `{"userId":7,"reactionType":1}`, "after-super-ordinary")
	if w.Code != 409 || f.posts() != 1 {
		t.Fatal("ordinary Like bypassed previous Super")
	}
}
