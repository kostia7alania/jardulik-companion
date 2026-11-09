package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func contactJobFixture(t *testing.T) (*Store, Config, *Client, Session) {
	t.Helper()
	s, cfg, _, _ := contactFixture(t)
	businessTestIngest(t, s, businessTestConnection(t, 1, 100, true))
	session, err := s.Session(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(s, cfg)
	client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/user/getReactUsers" {
			t.Fatalf("contact queue attempted action %s %s", r.Method, r.URL.Path)
		}
		fmt.Fprint(w, `[{"userId":77,"name":"Original partial preview","unknown_field":"unchanged"}]`)
	})}
	return s, cfg, client, session
}

func contactJobFetch(t *testing.T, client *Client, owner int64, mutual bool) RelationPage {
	t.Helper()
	result, err := client.ReactUsers(context.Background(), owner, mutual, 1)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func contactJobCount(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM contact_requests").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func contactJobGet(t *testing.T, s *Store) ContactJob {
	t.Helper()
	result, err := scanContactJob(s.DB.QueryRow("SELECT " + contactJobColumns + " FROM contact_requests ORDER BY id LIMIT 1"))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func contactJobRequest(job ContactJob) []byte {
	return []byte(fmt.Sprintf(`{"fromUserId":%d,"targetUserId":%d}`, job.UpstreamID, job.ProfileID))
}

func TestContactJobsFreshMutualOnlyAndKnownContactsSkipped(t *testing.T) {
	for _, known := range []string{"none", "dom", "business"} {
		t.Run(known, func(t *testing.T) {
			s, _, client, _ := contactJobFixture(t)
			ctx := context.Background()
			before, err := s.Profile(ctx, 1, 77)
			if err != nil {
				t.Fatal(err)
			}
			if known == "dom" {
				now := time.Now().UTC()
				input := ContactEvidenceInput{UpstreamID: 500, ProfileID: 77, SourceBotID: s.cfg.SourceBotID, SourcePeerID: s.cfg.SourceBotID, SourceLocatorKind: "telegram_web_dom", SourceMessageLocator: "data-mid:contact-job-fixture", SourceMessageTimestamp: now.Add(-time.Minute).Unix(), ContactURL: "https://t.me/Example16", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1", ObservedAt: now}
				if _, err = s.SaveContactEvidence(ctx, 1, input); err != nil {
					t.Fatal(err)
				}
			} else if known == "business" {
				businessTestIngest(t, s, businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16"))
			}
			contactJobFetch(t, client, 1, false)
			if err := s.initContactJobs(ctx); err != nil {
				t.Fatal(err)
			}
			if contactJobCount(t, s) != 0 {
				t.Fatal("incoming preview became mutual authority")
			}
			first := contactJobFetch(t, client, 1, true)
			second := contactJobFetch(t, client, 1, true)
			want := 1
			if known != "none" {
				want = 0
			}
			if contactJobCount(t, s) != want {
				t.Fatal("known contact requested again or duplicate job created")
			}
			if want == 1 {
				job := contactJobGet(t, s)
				if job.OwnerID != 1 || job.UpstreamID != 500 || job.ProfileID != 77 || job.Generation != "initial" || job.MutualBatchID != second.ID || first.ID == second.ID {
					t.Fatal("fresh mutual binding lost", job)
				}
			}
			after, err := s.Profile(ctx, 1, 77)
			if err != nil || after.RawJSON != before.RawJSON {
				t.Fatal("contact queue rewrote raw profile", err)
			}
			var actions int
			if err = s.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
				t.Fatal("contact queue sent a reaction", err)
			}
		})
	}
}

func TestContactJobsOnlyBoundOwnerAndActualVersionCanSend(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 2, true)
	if contactJobCount(t, s) != 0 {
		t.Fatal("owner without Business binding queued contacts")
	}
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	bad := job
	bad.OwnerID = 2
	if ready, err := s.ContactJobReady(ctx, bad, time.Now()); err != nil || ready {
		t.Fatal("caller supplied owner overrode durable job", err)
	}
	for _, raw := range []string{`{"fromUserId":600,"targetUserId":77}`, `{"fromUserId":500,"targetUserId":78}`, `{"fromUserId":500,"targetUserId":77,"extra":true}`, `{"fromUserId":"500","targetUserId":77}`} {
		if sent, _ := s.ClaimContactJob(ctx, job.ID, session, []byte(raw), time.Now()); sent {
			t.Fatal("request body did not match pinned contact")
		}
	}
	var attempted int64
	if err := s.DB.QueryRow("SELECT attempted_at_ms FROM contact_requests WHERE id=?", job.ID).Scan(&attempted); err != nil || attempted != 0 {
		t.Fatal("invalid request consumed durable claim", err)
	}
	// A batch for another owner cannot replace a mutual proof, even for the same profile ID.
	other := contactJobFetch(t, client, 2, true)
	if _, err := s.DB.Exec("UPDATE contact_requests SET mutual_batch_id=? WHERE id=?", other.ID, job.ID); err != nil {
		t.Fatal(err)
	}
	current := contactJobGet(t, s)
	if ready, err := s.ContactJobReady(ctx, current, time.Now()); err != nil || ready {
		t.Fatal("another owner's mutual batch became authority", err)
	}
}

func TestContactJobsPauseFreezeFreshnessAndBackoff(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	now := time.Now().UTC()
	assertBlocked := func(label string) {
		t.Helper()
		if ready, err := s.ContactJobReady(ctx, contactJobGet(t, s), time.Now()); err != nil || ready {
			t.Fatal(label, "readiness escaped guard", err)
		}
		if sent, err := s.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now()); err != nil || sent {
			t.Fatal(label, "claim escaped guard", err)
		}
		if _, err := s.NextContactJob(ctx); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(label, "worker selected blocked job", err)
		}
	}
	s.Pause(ctx, 1, true)
	assertBlocked("owner pause")
	s.Pause(ctx, 1, false)
	s.SessionState(ctx, 1, "paused", "source protection")
	assertBlocked("source pause")
	s.SessionState(ctx, 1, "connected", "")
	for _, freeze := range []any{true, "unknown"} {
		if err := s.SaveAccount(ctx, 1, Profile{"freezeState": freeze}); err != nil {
			t.Fatal(err)
		}
		assertBlocked("account freeze")
	}
	if err := s.SaveAccount(ctx, 1, Profile{"freezeState": false}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("UPDATE contact_requests SET proof_at_ms=?", now.Add(-25*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	assertBlocked("stale proof")
	contactJobFetch(t, client, 1, true)
	job = contactJobGet(t, s)
	if err := s.DeferContactJob(ctx, job.ID, "preflight transport", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertBlocked("preflight backoff")
	contactJobFetch(t, client, 1, true)
	job = contactJobGet(t, s)
	if ready, err := s.ContactJobReady(ctx, job, time.Now()); err != nil || !ready {
		t.Fatal("fresh mutual proof did not release only an unsent retry delay", err)
	}
}

func TestContactJobsClaimAcrossStoresAndRecoveryNeverReplays(t *testing.T) {
	s, cfg, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	second, err := OpenStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var claims atomic.Int64
	var group sync.WaitGroup
	problems := make(chan error, 12)
	for i := 0; i < cap(problems); i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			store := s
			if i%2 != 0 {
				store = second
			}
			mu := store.SessionMutex(1)
			mu.Lock()
			defer mu.Unlock()
			claimed, err := store.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now())
			if claimed {
				claims.Add(1)
			}
			problems <- err
		}(i)
	}
	group.Wait()
	close(problems)
	for err := range problems {
		if err != nil {
			t.Fatal(err)
		}
	}
	if claims.Load() != 1 {
		t.Fatal("more than one worker could send the same contact", claims.Load())
	}
	if err := s.RecoverContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if current := contactJobGet(t, s); current.State != "unknown" {
		t.Fatal("interrupted sending was made retryable", current)
	}
	contactJobFetch(t, client, 1, true)
	// Reconnecting cannot erase the uncertain outcome for this upstream/profile key.
	session.Generation = "reconnected"
	if err := s.SaveSession(ctx, 1, session); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 102, true))
	contactJobFetch(t, client, 1, true)
	if contactJobCount(t, s) != 1 || contactJobGet(t, s).State != "unknown" {
		t.Fatal("fresh proof or reconnect replayed an uncertain POST")
	}
	if _, err := s.NextContactJob(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("uncertain job selected again", err)
	}
}

func TestContactJobsFreshGenerationRebindsOnlyNeverAttempted(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	old := contactJobGet(t, s)
	session.Generation = "new"
	if err := s.SaveSession(ctx, 1, session); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestConnection(t, 1, 102, true))
	if ready, err := s.ContactJobReady(ctx, old, time.Now()); err != nil || ready {
		t.Fatal("old generation was still ready", err)
	}
	contactJobFetch(t, client, 1, true)
	current := contactJobGet(t, s)
	if current.ID != old.ID || current.Generation != "new" {
		t.Fatal("unsent job was duplicated rather than rebound", current)
	}
	if ready, err := s.ContactJobReady(ctx, current, time.Now()); err != nil || !ready {
		t.Fatal("fresh generation did not receive authority", err)
	}
}

func TestContactJobsEarlyReceiptKeepsStateAndExactHTTPResponse(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	request := contactJobRequest(job)
	if claimed, err := s.ClaimContactJob(ctx, job.ID, session, request, time.Now()); err != nil || !claimed {
		t.Fatal("claim", err)
	}
	receipt := businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16")
	businessTestIngest(t, s, receipt)
	response := []byte(" {\"unknown_result\":9007199254740993} \n")
	if err := s.FinishContactJob(ctx, job.ID, "awaiting_receipt", 200, response, "accepted", time.Now()); err != nil {
		t.Fatal(err)
	}
	var state, rawRequest, rawReceipt string
	var status int
	var rawResponse []byte
	if err := s.DB.QueryRow("SELECT state,request_json,http_status,response_body FROM contact_requests WHERE id=?", job.ID).Scan(&state, &rawRequest, &status, &rawResponse); err != nil {
		t.Fatal(err)
	}
	if state != "received" || rawRequest != string(request) || status != 200 || string(rawResponse) != string(response) {
		t.Fatal("HTTP completion overwrote early receipt or changed bytes")
	}
	if err := s.DB.QueryRow("SELECT raw_json FROM profile_business_contacts").Scan(&rawReceipt); err != nil || rawReceipt != string(receipt.RawJSON) {
		t.Fatal("receipt source changed", err)
	}
}

func TestContactJobsAtomicMutualPageAndReceiptRollback(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	if err := s.initContactJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("CREATE TRIGGER contact_job_insert_failure BEFORE INSERT ON contact_requests BEGIN SELECT RAISE(ABORT,'fixture queue failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReactUsers(ctx, 1, true, 1); err == nil {
		t.Fatal("failed enqueue still confirmed the mutual batch")
	}
	for _, table := range []string{"relation_batches", "relation_profiles", "contact_requests"} {
		var n int
		if err := s.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatal("partial mutual transaction", table, err)
		}
	}
	if _, err := s.DB.Exec("DROP TRIGGER contact_job_insert_failure"); err != nil {
		t.Fatal(err)
	}
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	if claimed, err := s.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now()); err != nil || !claimed {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("CREATE TRIGGER contact_job_receive_failure BEFORE UPDATE ON contact_requests WHEN NEW.state='received' BEGIN SELECT RAISE(ABORT,'fixture receipt failure'); END"); err != nil {
		t.Fatal(err)
	}
	receipt := businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16")
	if _, err := s.IngestBusinessContactUpdate(ctx, receipt); err == nil {
		t.Fatal("receipt insertion ignored failed job reconciliation")
	}
	if businessTestCount(t, s) != 0 || contactJobGet(t, s).State != "sending" {
		t.Fatal("partial source receipt escaped rollback")
	}
	if _, err := s.DB.Exec("DROP TRIGGER contact_job_receive_failure"); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, receipt)
	if businessTestCount(t, s) != 1 || contactJobGet(t, s).State != "received" {
		t.Fatal("replay did not recover atomic receipt")
	}
}

func TestContactJobsImportedPositiveAndOwnerDeletion(t *testing.T) {
	s, _, client, _ := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	// A later known contact resolves an unsent queue without another source call.
	businessTestIngest(t, s, businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16"))
	if _, err := s.NextContactJob(ctx); !errors.Is(err, sql.ErrNoRows) || contactJobGet(t, s).State != "received" {
		t.Fatal("known positive was still queued", err)
	}
	if err := s.DeleteUser(ctx, 1); err != nil || contactJobCount(t, s) != 0 {
		t.Fatal("contact request survived owner deletion", err)
	}
}

// Keep raw JSON checks independent of map ordering or decoded number conversion.
func TestContactJobsResultValidationDoesNotInventContact(t *testing.T) {
	s, _, client, session := contactJobFixture(t)
	ctx := context.Background()
	contactJobFetch(t, client, 1, true)
	job := contactJobGet(t, s)
	if sent, err := s.ClaimContactJob(ctx, job.ID, session, contactJobRequest(job), time.Now()); err != nil || !sent {
		t.Fatal(err)
	}
	if err := s.FinishContactJob(ctx, job.ID, "received", 200, []byte(`{"url":"https://t.me/Fabricated"}`), "", time.Now()); err == nil {
		t.Fatal("HTTP result declared an unobserved contact")
	}
	if err := s.FinishContactJob(ctx, job.ID, "awaiting_receipt", 204, nil, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	var response []byte
	if err := s.DB.QueryRow("SELECT response_body FROM contact_requests").Scan(&response); err != nil || len(response) != 0 {
		t.Fatal("empty HTTP response not retained", err)
	}
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact != nil {
		t.Fatal("accepted POST invented a contact", err)
	}
	var rows int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&rows); err != nil || rows != 0 {
		t.Fatal("contact queue generated a Like", err)
	}
	// Fixtures use the same exact body shape as the first-party request.
	var request map[string]json.RawMessage
	if err := json.Unmarshal(contactJobRequest(job), &request); err != nil || len(request) != 2 || strings.Contains(string(contactJobRequest(job)), "reaction") {
		t.Fatal("invalid request fixture")
	}
}

func TestContactJobsFrozenRelationshipReadStopsBeforeHTTP(t *testing.T) {
	_, _, client, session := contactJobFixture(t)
	var calls atomic.Int64
	client.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("wrong account HTTP")
	})
	old := session
	old.Generation = "other-generation"
	if _, err := client.ReactUsersFrozen(context.Background(), 1, old, true, 1); !errors.Is(err, ErrContactSessionChanged) || calls.Load() != 0 {
		t.Fatal("frozen mutual source used a replacement account", err)
	}
	old = session
	old.UserID++
	if _, err := client.ReactUsersFrozen(context.Background(), 1, old, true, 1); !errors.Is(err, ErrContactSessionChanged) || calls.Load() != 0 {
		t.Fatal("frozen mutual source changed upstream", err)
	}
}

func TestContactJobsInboxRetainsUnknownOfficialMessagesWithoutInventingContact(t *testing.T) {
	s, _, _, _ := contactJobFixture(t)
	ctx := context.Background()
	update := mutateBusinessUpdate(t, businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16"), func(body map[string]any) {
		message := body["business_message"].(map[string]any)
		delete(message, "reply_markup")
		message["future_payload"] = map[string]any{"id": json.Number("9007199254740993"), "value": "source untouched"}
	})
	businessTestIngest(t, s, update)
	businessTestIngest(t, s, update)
	var count int
	var state, raw string
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_inbox").Scan(&count); err != nil || count != 1 {
		t.Fatal("unknown official source was dropped or duplicated", err)
	}
	if err := s.DB.QueryRow("SELECT parse_state,raw_json FROM business_contact_inbox").Scan(&state, &raw); err != nil || state != "unresolved" || raw != string(update.RawJSON) || !strings.Contains(raw, "9007199254740993") {
		t.Fatal("unknown source bytes or parse state changed", err)
	}
	if contact, err := s.ProfileTelegramContact(ctx, 1, 77); err != nil || contact != nil || businessTestCount(t, s) != 0 {
		t.Fatal("unknown payload invented a profile or contact", err)
	}
	conflict := mutateBusinessUpdate(t, update, func(body map[string]any) { body["extra"] = "changed" })
	if _, err := s.IngestBusinessContactUpdate(ctx, conflict); !errors.Is(err, ErrContactEvidenceConflict) {
		t.Fatal("same source update accepted changed raw payload", err)
	}
	if err := s.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_inbox").Scan(&count); err != nil || count != 0 {
		t.Fatal("owner deletion retained source inbox", err)
	}
}

func TestContactJobsInboxIgnoresForeignForwardedAndReboundMessages(t *testing.T) {
	for name, change := range map[string]func(map[string]any){
		"source": func(body map[string]any) {
			body["business_message"].(map[string]any)["from"].(map[string]any)["id"] = int64(999)
		},
		"chat": func(body map[string]any) {
			body["business_message"].(map[string]any)["chat"].(map[string]any)["id"] = int64(999)
		},
		"forward": func(body map[string]any) {
			body["business_message"].(map[string]any)["forward_origin"] = map[string]any{"type": "user", "sender_user": map[string]any{"id": 42}}
		},
		"on-behalf": func(body map[string]any) {
			body["business_message"].(map[string]any)["sender_business_bot"] = map[string]any{"id": 42, "is_bot": true}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, _, _, _ := contactJobFixture(t)
			update := mutateBusinessUpdate(t, businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16"), change)
			businessTestIngest(t, s, update)
			var count int
			if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_inbox").Scan(&count); err != nil || count != 0 {
				t.Fatal("untrusted other-chat content retained", err)
			}
		})
	}
	s, _, _, session := contactJobFixture(t)
	session.Generation = "rebound"
	if err := s.SaveSession(context.Background(), 1, session); err != nil {
		t.Fatal(err)
	}
	businessTestIngest(t, s, businessTestMessage(t, s.cfg.SourceBotID, 1, 101, "https://t.me/Example16"))
	var count int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM business_contact_inbox").Scan(&count); err != nil || count != 0 {
		t.Fatal("old Business generation retained a new account message", err)
	}
}
