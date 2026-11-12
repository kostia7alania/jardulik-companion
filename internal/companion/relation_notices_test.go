package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type relationNoticeReply struct {
	mutual  bool
	body    string
	latency time.Duration
}

func relationNoticePollFixture(t *testing.T, replies []relationNoticeReply) (*Service, *time.Time, *int) {
	t.Helper()
	s, now := syncFixture(t)
	s.Config.RelationNoticesEnabled = true
	ctx := context.Background()
	if err := s.Store.User(ctx, 2, "other tenant"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, 2, Session{Token: "other", UserID: 202, Generation: "source-two", Expires: now.Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := s.initRelationNotices(ctx); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls >= len(replies) {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
		}
		reply := replies[calls]
		calls++
		query := r.URL.Query()
		if r.Method != http.MethodGet || r.URL.Path != "/user/getReactUsers" || query.Get("userId") != "101" ||
			query.Get("isMutual") != strconv.FormatBool(reply.mutual) || query.Get("pageNumber") != "1" || query.Get("pageSize") != "1000" {
			t.Fatalf("request %d: %s %s", calls, r.Method, r.URL.RequestURI())
		}
		*now = now.Add(reply.latency)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, reply.body)
	})}
	return s, now, &calls
}

func checkRelationNoticePoll(t *testing.T, s *Service, now time.Time, want bool) {
	t.Helper()
	if attempted, err := s.relationNoticePollStep(context.Background(), now); err != nil || attempted != want {
		t.Fatalf("at %s attempted=%t want=%t err=%v", now, attempted, want, err)
	}
}

func checkRelationNoticeState(t *testing.T, s *Service, kind string, due time.Time, incoming, mutual bool) {
	t.Helper()
	var next string
	var dueMS int64
	var incomingBaseline, mutualBaseline bool
	err := s.Store.DB.QueryRow(`SELECT next_kind,next_attempt_ms,incoming_baseline,mutual_baseline
 FROM relation_notice_poll_state WHERE owner_id=1 AND upstream_id=101`).Scan(&next, &dueMS, &incomingBaseline, &mutualBaseline)
	if err != nil || next != kind || dueMS != due.UnixMilli() || incomingBaseline != incoming || mutualBaseline != mutual {
		t.Fatalf("state kind=%s due=%d baselines=%t,%t err=%v; want %s %d %t,%t", next, dueMS, incomingBaseline, mutualBaseline, err, kind, due.UnixMilli(), incoming, mutual)
	}
}

func TestRelationNoticePollPersistsCadenceAndDeduplicates(t *testing.T) {
	s, now, calls := relationNoticePollFixture(t, []relationNoticeReply{
		{body: `[{"userId":11}]`, latency: 2 * time.Second},
		{mutual: true, body: `[{"userId":22}]`},
		{body: `[{"userId":11},{"userId":33},{"userId":33}]`},
		{mutual: true, body: `[{"userId":22},{"userId":33}]`},
		{body: `[]`},
		{mutual: true, body: `[{"userId":33}]`},
		{body: `[{"userId":33}]`},
	})
	checkRelationNoticePoll(t, s, *now, true)
	firstMutual := now.Add(30 * time.Second)
	checkRelationNoticeState(t, s, "mutual", firstMutual, true, false)
	if got := watchCount(t, s.Store, "relation_notice_events"); got != 0 {
		t.Fatalf("incoming baseline emitted %d events", got)
	}

	// An independent service must see the saved due time and the next list.
	restarted, err := NewService(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	restarted.Client.Now = func() time.Time { return *now }
	restarted.Client.HTTP.Transport = s.Client.HTTP.Transport
	if err := restarted.initRelationNotices(context.Background()); err != nil {
		t.Fatal(err)
	}
	*now = firstMutual.Add(-time.Millisecond)
	checkRelationNoticePoll(t, restarted, *now, false)
	*now = firstMutual
	checkRelationNoticePoll(t, restarted, *now, true)
	checkRelationNoticeState(t, restarted, "incoming", now.Add(5*time.Minute), true, true)
	if got := watchCount(t, s.Store, "relation_notice_events"); got != 0 {
		t.Fatalf("mutual baseline emitted %d events", got)
	}
	for step := 0; step < 5; step++ {
		due := now.Add(5 * time.Minute)
		*now = due.Add(-time.Millisecond)
		checkRelationNoticePoll(t, restarted, *now, false)
		*now = due
		checkRelationNoticePoll(t, restarted, *now, true)
	}
	if *calls != 7 {
		t.Fatalf("got %d requests, want 7", *calls)
	}
	if got := watchCount(t, s.Store, "relation_notice_seen"); got != 4 {
		t.Fatalf("got %d seen keys, want 4", got)
	}
	rows, err := s.Store.DB.Query(`SELECT owner_id,upstream_id,profile_id,kind,state FROM relation_notice_events ORDER BY kind`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var events []string
	for rows.Next() {
		var owner, upstream, profile int64
		var kind, state string
		if err := rows.Scan(&owner, &upstream, &profile, &kind, &state); err != nil {
			t.Fatal(err)
		}
		events = append(events, fmt.Sprintf("%d:%d:%d:%s:%s", owner, upstream, profile, kind, state))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"1:101:33:incoming:pending", "1:101:33:mutual:pending"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
}

func TestRelationNoticePollSkipsIneligibleOwner(t *testing.T) {
	for _, kind := range []string{"disabled", "user pause", "source pause", "reconnect", "frozen account", "foreground"} {
		t.Run(kind, func(t *testing.T) {
			s, now, _ := relationNoticePollFixture(t, nil)
			ctx := context.Background()
			switch kind {
			case "disabled":
				s.Config.RelationNoticesEnabled = false
			case "user pause":
				s.Store.Pause(ctx, 1, true)
			case "source pause":
				s.Store.SessionState(ctx, 1, "paused", "source access denied")
			case "reconnect":
				s.Store.SessionState(ctx, 1, "reconnect_required", "expired")
			case "frozen account":
				if _, err := s.Store.DB.Exec(`UPDATE sessions SET account_json='{"freezeState":true}' WHERE owner_id=1`); err != nil {
					t.Fatal(err)
				}
			case "foreground":
				s.beginForeground()
				defer s.endForeground()
			}
			checkRelationNoticePoll(t, s, *now, false)
			if got := watchCount(t, s.Store, "relation_notice_poll_state"); got != 0 {
				t.Fatalf("ineligible step claimed %d poll rows", got)
			}
		})
	}
}

func TestRelationNoticePollRequiresCompleteBaseline(t *testing.T) {
	full := make([]map[string]int, 1000)
	for i := range full {
		full[i] = map[string]int{"userId": 1000 + i}
	}
	body, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	for name, partial := range map[string]string{"null": "null", "full first page": string(body)} {
		t.Run(name, func(t *testing.T) {
			s, now, _ := relationNoticePollFixture(t, []relationNoticeReply{
				{body: partial},
				{mutual: true, body: `[]`},
				{body: `[]`},
			})
			checkRelationNoticePoll(t, s, *now, true)
			checkRelationNoticeState(t, s, "mutual", now.Add(5*time.Minute), false, false)
			if got := watchCount(t, s.Store, "relation_notice_seen"); got != 0 {
				t.Fatalf("incomplete page added %d seen keys", got)
			}
			*now = now.Add(5 * time.Minute)
			checkRelationNoticePoll(t, s, *now, true)
			checkRelationNoticeState(t, s, "incoming", now.Add(30*time.Second), false, true)
			*now = now.Add(30 * time.Second)
			checkRelationNoticePoll(t, s, *now, true)
			checkRelationNoticeState(t, s, "mutual", now.Add(5*time.Minute), true, true)
			if got := watchCount(t, s.Store, "relation_notice_events"); got != 0 {
				t.Fatalf("baseline emitted %d events", got)
			}
		})
	}
}

func TestRelationNoticePollDoesNotQueueContactWork(t *testing.T) {
	s, now, _ := relationNoticePollFixture(t, []relationNoticeReply{
		{body: `[]`},
		{mutual: true, body: `[]`},
		{body: `[]`},
		{mutual: true, body: `[{"userId":77,"photos":"https://example.invalid/photo.jpg"}]`},
		{mutual: true, body: `[{"userId":77}]`},
	})
	ctx := context.Background()
	businessTestIngest(t, s.Store, businessTestConnection(t, 1, 100, true))
	if err := s.Store.initContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	checkRelationNoticePoll(t, s, *now, true)
	*now = now.Add(30 * time.Second)
	checkRelationNoticePoll(t, s, *now, true)
	*now = now.Add(5 * time.Minute)
	checkRelationNoticePoll(t, s, *now, true)
	*now = now.Add(5 * time.Minute)
	checkRelationNoticePoll(t, s, *now, true)
	if got := watchCount(t, s.Store, "relation_notice_events"); got != 1 {
		t.Fatalf("mutual should be queued for notification, got %d notices", got)
	}
	for _, table := range []string{"contact_requests", "versions", "media_jobs", "actions"} {
		if got := watchCount(t, s.Store, table); got != 0 {
			t.Errorf("notification poll created %d rows in %s", got, table)
		}
	}
	// The normal relationship list proves that this binding would allow contact work.
	contactJobFetch(t, s.Client, 1, true)
	if got := contactJobCount(t, s.Store); got != 1 {
		t.Fatalf("normal mutual fetch did not queue contact work, got %d jobs", got)
	}
}
