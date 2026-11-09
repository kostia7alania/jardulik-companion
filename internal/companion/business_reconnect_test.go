package companion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestBusinessPassiveContactSurvivesJardulikReconnectRequired(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	if sent, err := s.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now()); err != nil || !sent {
		t.Fatal("request claim", err)
	}
	if err := s.FinishContactJob(ctx, job.ID, "unknown", http.StatusUnauthorized, []byte(`{"code":"unauthorized"}`), "unauthorized", time.Now()); err != nil {
		t.Fatal(err)
	}
	s.SessionState(ctx, 1, "reconnect_required", "contact_auth_not_confirmed")
	var sourceCalls atomic.Int64
	client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		sourceCalls.Add(1)
		return nil, errors.New("passive receipt used source HTTP")
	})
	if err := client.RequestContact(ctx, contactJobGet(t, s)); err != nil {
		t.Fatal("terminal contact request did not stay inactive", err)
	}
	if sent, err := s.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now()); err != nil || sent {
		t.Fatal("reconnect allowed another POST claim", err)
	}
	// A new connection event must not rebind this unavailable Jardulik session.
	businessTestIngest(t, s, businessTestConnection(t, 1, 102, true))
	var connectionUpdate int64
	if err := s.DB.QueryRow("SELECT update_id FROM business_contact_connections WHERE owner_id=1").Scan(&connectionUpdate); err != nil || connectionUpdate != 100 {
		t.Fatal("new Business connection guard changed", err)
	}
	receipt := businessTestMessage(t, s.cfg.SourceBotID, 1, 103, "https://t.me/Example16")
	businessTestIngest(t, s, receipt)
	var rawInbox, rawContact string
	if err := s.DB.QueryRow("SELECT raw_json FROM business_contact_inbox WHERE update_id=103").Scan(&rawInbox); err != nil {
		t.Fatal("passive raw inbox lost", err)
	}
	if err := s.DB.QueryRow("SELECT raw_json FROM profile_business_contacts WHERE update_id=103").Scan(&rawContact); err != nil {
		t.Fatal("passive contact lost", err)
	}
	if rawInbox != string(receipt.RawJSON) || rawContact != string(receipt.RawJSON) {
		t.Fatal("passive source JSON changed")
	}
	if current := contactJobGet(t, s); current.State != "received" {
		t.Fatal("late card did not resolve unknown request", current)
	}
	var status int
	var response []byte
	if err := s.DB.QueryRow("SELECT http_status,response_body FROM contact_requests WHERE id=?", job.ID).Scan(&status, &response); err != nil || status != http.StatusUnauthorized || string(response) != `{"code":"unauthorized"}` {
		t.Fatal("receipt rewrote the original POST result", err)
	}
	current, err := s.Session(ctx, 1)
	if err != nil || current.State != "reconnect_required" || current.UserID != session.UserID || current.Generation != session.Generation {
		t.Fatal("passive receipt changed source lifecycle", err)
	}
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact == nil || contact.ContactURL != "https://t.me/Example16" {
		t.Fatal("saved contact unavailable in reconnect state", err)
	}
	if sourceCalls.Load() != 0 {
		t.Fatal("passive receipt made source HTTP calls", sourceCalls.Load())
	}
}

func TestBusinessReconnectPassiveReceiveStillPinsCurrentIdentity(t *testing.T) {
	for _, mismatch := range []string{"generation", "upstream", "disabled"} {
		t.Run(mismatch, func(t *testing.T) {
			s, _, client, session := contactJobFixture(t)
			ctx := context.Background()
			switch mismatch {
			case "generation":
				session.Generation = "other"
			case "upstream":
				session.UserID++
			case "disabled":
				businessTestIngest(t, s, businessTestConnection(t, 1, 101, false))
			}
			if mismatch != "disabled" {
				if err := s.SaveSession(ctx, 1, session); err != nil {
					t.Fatal(err)
				}
			}
			s.SessionState(ctx, 1, "reconnect_required", "fixture")
			var calls atomic.Int64
			client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("unexpected source HTTP")
			})
			businessTestIngest(t, s, businessTestMessage(t, s.cfg.SourceBotID, 1, 103, "https://t.me/Example16"))
			for _, table := range []string{"business_contact_inbox", "profile_business_contacts"} {
				var count int
				if err := s.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
					t.Fatal("passive receive crossed lifecycle boundary", table, err)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("receive used HTTP")
			}
		})
	}
}

func TestBusinessEditedContactUsesEditDateForConnectionBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		original, edit         int64
		wantInbox, wantContact bool
	}{
		{"new edit of old card", 100, 300, true, true},
		{"edit older than connection", 100, 199, false, false},
		{"invalid edit of old card", 100, 90, false, false},
		{"invalid edit of new card retained raw", 250, 249, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, client, _ := contactJobFixture(t)
			// Replace the fixture connection with a later confirmed event.
			connection := mutateBusinessUpdate(t, businessTestConnection(t, 1, 102, true), func(body map[string]any) { body["business_connection"].(map[string]any)["date"] = int64(200) })
			businessTestIngest(t, s, connection)
			contactJobFetch(t, client, 1, true)
			var calls atomic.Int64
			client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("edited receipt used HTTP")
			})
			receipt := mutateBusinessUpdate(t, businessTestMessage(t, s.cfg.SourceBotID, 1, 103, "https://t.me/Example16"), func(body map[string]any) {
				message := body["business_message"].(map[string]any)
				message["date"] = tc.original
				message["edit_date"] = tc.edit
				message["future_field"] = json.Number("9007199254740993")
				delete(body, "business_message")
				body["edited_business_message"] = message
			})
			businessTestIngest(t, s, receipt)
			var inbox, contacts int
			if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_inbox").Scan(&inbox); err != nil {
				t.Fatal(err)
			}
			if err := s.DB.QueryRow("SELECT COUNT(*) FROM profile_business_contacts").Scan(&contacts); err != nil {
				t.Fatal(err)
			}
			if (inbox == 1) != tc.wantInbox || (contacts == 1) != tc.wantContact {
				t.Fatalf("wrong effective-date boundary: inbox=%d contacts=%d", inbox, contacts)
			}
			if tc.wantInbox {
				var raw, parse string
				if err := s.DB.QueryRow("SELECT raw_json,parse_state FROM business_contact_inbox").Scan(&raw, &parse); err != nil || raw != string(receipt.RawJSON) {
					t.Fatal("edited raw JSON changed", err)
				}
				if tc.wantContact && parse != "contact" || !tc.wantContact && parse != "unresolved" {
					t.Fatal("invalid edited source classification", parse)
				}
			}
			if tc.wantContact {
				var timestamp int64
				if err := s.DB.QueryRow("SELECT source_message_timestamp FROM profile_business_contacts").Scan(&timestamp); err != nil || timestamp != tc.edit {
					t.Fatal("edit timestamp was not used", err)
				}
				if contactJobGet(t, s).State != "received" {
					t.Fatal("edited card did not reconcile matching job")
				}
			} else if contactJobGet(t, s).State != "queued" {
				t.Fatal("stale or invalid edit completed a contact job")
			}
			if calls.Load() != 0 {
				t.Fatal("edited receipt used HTTP")
			}
		})
	}
}
