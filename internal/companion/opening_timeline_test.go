package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func timelineIntent(t *testing.T, f *profileIndexesFixture, profile, reaction int64, message string) int64 {
	t.Helper()
	if _, err := f.s.Store.DB.Exec(reactionMessageSchema); err != nil {
		t.Fatal(err)
	}
	result, err := f.s.Store.DB.Exec(`INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(123,?,'Like','succeeded','{"IsSuccess":true}')`, profile)
	if err != nil {
		t.Fatal(err)
	}
	action, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(map[string]any{"fromUserId": 5696, "userId": profile, "reactionType": reaction, "isDeleted": false, "message": message})
	_, err = f.s.Store.DB.Exec(`INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reaction_type,attempted_at_ms)
 VALUES(?,123,5696,?,'older-session-generation',?,?,'birthday',1,0,?,?)`, action, profile, message, request, reaction, time.Date(2026, 11, 10, 8, 7, 0, 0, time.UTC).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func timelineStep(t *testing.T, f *profileIndexesFixture) {
	t.Helper()
	if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || !worked {
		t.Fatalf("timeline delivery: %v %v", worked, err)
	}
}

func timelineState(t *testing.T, f *profileIndexesFixture) string {
	t.Helper()
	var state string
	if err := f.s.Store.DB.QueryRow("SELECT state FROM opening_timeline_posts ORDER BY id LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestOpeningTimelineExactSentMessageOwnThreadAndHistoricalTime(t *testing.T) {
	for _, reaction := range []int64{1, 3} {
		t.Run(fmt.Sprint(reaction), func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 10, 23, "Name", "2003-03-01")
			message := "Дӕ бон хорз 🙂\n**буквально** <без правок> @fixture_user"
			timelineIntent(t, f, 10, reaction, message)
			// Confirmed history remains publishable while collection/publication is paused.
			f.s.Config.ArchivePaused, f.s.Archiver.Config.ArchivePaused = true, true
			f.respond = func(r *http.Request, method string, body map[string]json.RawMessage) (*http.Response, error) {
				if method != "sendMessage" {
					t.Fatalf("unexpected external request: %s", method)
				}
				// A held SQLite transaction would deadlock this single-connection probe.
				probe, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var n int
				if err := f.s.Store.DB.QueryRowContext(probe, "SELECT 1").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":2001,"message_thread_id":110,"chat":{"id":-100123}}}`), nil
			}
			timelineStep(t, f)
			var payload openingPostPayload
			raw, _ := json.Marshal(f.bodies[0])
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatal(err)
			}
			icon := "❤️"
			if reaction == 3 {
				icon = "🔥"
			}
			prefix := "💌 Отправлено · " + icon + "\n🕒 10.11.26, 11:07 (вт, UTC+3)\n\n"
			if payload.Text != prefix+message || len(payload.Entities) != 2 || payload.Entities[1].Type != "expandable_blockquote" || payload.Entities[1].Offset != textUnits(prefix) || payload.Entities[1].Length != textUnits(message) {
				t.Fatalf("changed literal message or presentation: %#v", payload)
			}
			if _, err := openingPostBytes(payload); err != nil {
				t.Fatal(err)
			}
			if timelineState(t, f) != "sent" {
				t.Fatal("missing confirmed timeline receipt")
			}
			if err := f.s.recoverOpeningTimeline(context.Background()); err != nil {
				t.Fatal(err)
			}
			if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked || len(f.operations) != 1 {
				t.Fatalf("duplicate timeline send: %v %v %v", worked, err, f.operations)
			}
		})
	}
}

func TestOpeningTimelineOnlyConfirmedExactOwnerBoundIntents(t *testing.T) {
	for _, test := range []struct {
		name, mutate string
	}{
		{"unknown", "UPDATE actions SET state='unknown'"},
		{"rejected_response", `UPDATE actions SET response='{"IsSuccess":false}'`},
		{"empty_message", "UPDATE reaction_message_intents SET message=''"},
		{"not_attempted", "UPDATE reaction_message_intents SET attempted_at_ms=0"},
		{"different_account", "UPDATE reaction_message_intents SET upstream_id=999"},
		{"wrong_target", "UPDATE reaction_message_intents SET request_body=replace(CAST(request_body AS TEXT),'\"userId\":10','\"userId\":11')"},
		{"wrong_type", "UPDATE reaction_message_intents SET reaction_type=3"},
		{"no_own_publication", "DELETE FROM archive_profile_repairs"},
		{"wrong_destination_proof", "UPDATE archive_receipts SET chat_id=-100999"},
		{"foreign_action_owner", "INSERT INTO users(id) VALUES(999); UPDATE actions SET owner_id=999"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 10, 23, "Name", "2003-03-01")
			timelineIntent(t, f, 10, 1, "actual message")
			if _, err := f.s.Store.DB.Exec(test.mutate); err != nil {
				t.Fatal(err)
			}
			if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked || len(f.operations) != 0 {
				t.Fatalf("ineligible source posted: %v %v %v", worked, err, f.operations)
			}
		})
	}
}

func TestOpeningTimelineLostAcknowledgementNeverBlindlyResends(t *testing.T) {
	for _, mode := range []string{"transport", "wrong_receipt", "sqlite_ack"} {
		t.Run(mode, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 10, 23, "Name", "2003-03-01")
			timelineIntent(t, f, 10, 1, "actual message")
			if mode == "sqlite_ack" {
				if err := f.s.initOpeningTimeline(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.Store.DB.Exec(`CREATE TRIGGER timeline_ack_failure BEFORE UPDATE OF state ON opening_timeline_posts WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'fixture acknowledgement failure'); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				f.respond = func(r *http.Request, _ string, _ map[string]json.RawMessage) (*http.Response, error) {
					if mode == "transport" {
						return nil, errors.New("lost acknowledgement")
					}
					return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":2001,"message_thread_id":999,"chat":{"id":-100123}}}`), nil
				}
			}
			if worked, err := f.s.deliverOpeningTimeline(context.Background()); !worked || err == nil {
				t.Fatalf("expected failed acknowledgement: %v %v", worked, err)
			}
			if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked || len(f.operations) != 1 {
				t.Fatalf("unknown send replayed: %v %v %v", worked, err, f.operations)
			}
			if err := f.s.recoverOpeningTimeline(context.Background()); err != nil {
				t.Fatal(err)
			}
			if timelineState(t, f) != "uncertain" {
				t.Fatal("lost acknowledgement must remain uncertain")
			}
			if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked || len(f.operations) != 1 {
				t.Fatalf("restart replayed unknown send: %v %v %v", worked, err, f.operations)
			}
		})
	}
}

func TestOpeningTimelineRechecksSourceSessionAndThreadAfterClaim(t *testing.T) {
	for _, changed := range []string{"source", "thread", "session", "config"} {
		t.Run(changed, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 10, 23, "Name", "2003-03-01")
			timelineIntent(t, f, 10, 1, "actual message")
			ctx := context.Background()
			if err := f.s.initOpeningTimeline(ctx); err != nil {
				t.Fatal(err)
			}
			expected, err := f.s.Store.Session(ctx, 123)
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := f.s.claimOpeningTimeline(ctx, expected)
			if err != nil || !claimed {
				t.Fatalf("claim: %v %v", claimed, err)
			}
			switch changed {
			case "source":
				_, err = f.s.Store.DB.Exec("UPDATE reaction_message_intents SET message='changed'")
			case "thread":
				_, err = f.s.Store.DB.Exec("UPDATE mirror_topics SET thread_id=999")
			case "session":
				current := expected
				current.Generation += "-reconnected"
				err = f.s.Store.SaveSession(ctx, 123, current)
			case "config":
				f.s.Config.BackupChatID = -100999
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.s.markOpeningTimelineSending(ctx, op, expected); err == nil || len(f.operations) != 0 {
				t.Fatalf("changed binding allowed: %v %v", err, f.operations)
			}
		})
	}
}

func TestOpeningTimelineWaitsForArchiveThenIgnoresMutableProfileChanges(t *testing.T) {
	f := newProfileIndexesFixture(t)
	if _, err := f.s.Store.Archive(context.Background(), 123, []Profile{{"userId": 10, "age": 23}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	timelineIntent(t, f, 10, 1, "frozen original")
	if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked {
		t.Fatalf("not yet archived: %v %v", worked, err)
	}
	f.add(t, 10, 23, "ChangedName", "2003-03-01")
	timelineStep(t, f)
	if _, err := f.s.Store.Archive(context.Background(), 123, []Profile{{"userId": 10, "age": 24, "description": "new profile"}}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if worked, err := f.s.deliverOpeningTimeline(context.Background()); err != nil || worked || len(f.operations) != 1 {
		t.Fatalf("profile update duplicated historical action: %v %v", worked, err)
	}
	var text string
	if err := json.Unmarshal(f.bodies[0]["text"], &text); err != nil || !strings.HasSuffix(text, "frozen original") {
		t.Fatalf("frozen text changed: %q %v", text, err)
	}
}
