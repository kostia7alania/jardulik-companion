package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

type contactImportNoNetwork struct{ calls *int }

func (n contactImportNoNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	*n.calls++
	return nil, errors.New("import attempted a network request")
}

func contactImportFixture(t *testing.T) (*companion.Service, companion.ContactEvidenceInput, *int) {
	t.Helper()
	config := companion.Config{DataDir: t.TempDir(), OwnerID: 1, SourceBotID: 42, SessionKey: make([]byte, 32), BackupKey: make([]byte, 32), APIBase: "https://source.invalid/api", TelegramBase: "https://telegram.invalid"}
	s, err := companion.NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err = s.Store.User(context.Background(), 1, "test"); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SaveSession(context.Background(), 1, companion.Session{Token: "fixture", UserID: 101, Generation: "fixture", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	var calls int
	s.Client.HTTP.Transport = contactImportNoNetwork{&calls}
	s.TG.HTTP.Transport = contactImportNoNetwork{&calls}
	evidence := companion.ContactEvidenceInput{UpstreamID: 101, ProfileID: 77, SourceBotID: 42, SourcePeerID: 42, SourceLocatorKind: "telegram_web_dom", SourceMessageLocator: "data-mid:9001", SourceMessageTimestamp: time.Now().Add(-time.Hour).Unix(), ContactURL: "https://t.me/Example16", ProfileURL: "https://mini.jardulik.app/mutuals?targetUserId=77&noCheck=1", ObservedAt: time.Now().UTC()}
	return s, evidence, &calls
}

func TestContactImportWritesIdempotentlyWithoutNetwork(t *testing.T) {
	s, evidence, calls := contactImportFixture(t)
	raw, _ := json.Marshal(evidence)
	var previousID float64
	for i := 0; i < 2; i++ {
		var output bytes.Buffer
		if err := importContactEvidence(context.Background(), s, 1, bytes.NewReader(raw), &output); err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result["stored"] != true || result["profile_id"] != float64(77) || result["contact_evidence_id"] == float64(0) {
			t.Fatalf("missing receipt: %v", result)
		}
		id := result["contact_evidence_id"].(float64)
		if i > 0 && id != previousID {
			t.Fatal("repeat import created another record")
		}
		previousID = id
	}
	contact, err := s.Store.ProfileTelegramContact(context.Background(), 1, 77)
	if err != nil || contact == nil || contact.ContactURL != evidence.ContactURL || *calls != 0 {
		t.Fatalf("wrong saved contact or network requests: %+v %v calls=%d", contact, err, *calls)
	}
}

func TestContactImportRejectsForeignOwnerAndInvalidInputBeforeStorage(t *testing.T) {
	for _, kind := range []string{"owner", "unknown", "trailing", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			s, evidence, calls := contactImportFixture(t)
			raw, _ := json.Marshal(evidence)
			owner := int64(1)
			switch kind {
			case "owner":
				owner = 2
			case "unknown":
				raw = append(raw[:len(raw)-1], []byte(`,"extra":"unaccepted"}`)...)
			case "trailing":
				raw = append(raw, []byte(` {}`)...)
			case "oversize":
				raw = []byte(strings.Repeat(" ", (64<<10)+1))
			}
			var output bytes.Buffer
			if err := importContactEvidence(context.Background(), s, owner, bytes.NewReader(raw), &output); err == nil {
				t.Fatal("invalid import accepted")
			}
			var tables int
			if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='profile_contact_evidence'").Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if tables != 0 || output.Len() != 0 || *calls != 0 {
				t.Fatalf("rejected input changed state or performed HTTP: tables=%d output=%q calls=%d", tables, output.String(), *calls)
			}
		})
	}
}
