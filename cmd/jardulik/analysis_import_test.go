package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func analysisImportFixture(t *testing.T) (*companion.Service, companion.AnalysisCaptureInput, *int) {
	t.Helper()
	s, _, calls := contactImportFixture(t)
	return s, companion.AnalysisCaptureInput{
		UpstreamID: 101, ProfileID: 77, Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: time.Now().UTC().UnixMilli(),
		HTTPStatus: 200, State: "response_received", Generation: "fixture",
		RequestBody:  []byte(`{"fromUserId":101,"targetUserId":77}`),
		ResponseBody: []byte("{\n \"content\":\"75% text\",\n \"model\":\"gpt-4o-mini\"\n}\n"),
		OwnProfile:   []byte(`{"userId":101}`), TargetProfile: []byte(`{"userId":77}`),
	}, calls
}

func TestAnalysisImportStoresOnceWithoutNetwork(t *testing.T) {
	s, input, calls := analysisImportFixture(t)
	raw, _ := json.Marshal(input)
	var firstID float64
	for i := 0; i < 2; i++ {
		var output bytes.Buffer
		if err := importAnalysisCapture(context.Background(), s, 1, bytes.NewReader(raw), &output); err != nil {
			t.Fatal(err)
		}
		var receipt map[string]any
		if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
			t.Fatal(err)
		}
		id := receipt["analysis_id"].(float64)
		if receipt["stored"] != true || receipt["model"] != "gpt-4o-mini" || receipt["provider"] != "jardulik" || id <= 0 || i > 0 && id != firstID {
			t.Fatalf("wrong import receipt: %v", receipt)
		}
		firstID = id
	}
	saved, err := s.Store.LatestProfileAnalysis(context.Background(), 1, 77)
	if err != nil || saved == nil || !bytes.Equal(saved.ResponseBody, input.ResponseBody) || *calls != 0 {
		t.Fatalf("raw response changed or import used network: err=%v calls=%d", err, *calls)
	}
}

func TestAnalysisImportRejectsForeignOwnerAndMalformedEnvelope(t *testing.T) {
	for _, kind := range []string{"owner", "unknown", "trailing", "oversize", "base64"} {
		t.Run(kind, func(t *testing.T) {
			s, input, calls := analysisImportFixture(t)
			raw, _ := json.Marshal(input)
			owner := int64(1)
			switch kind {
			case "owner":
				owner = 2
			case "unknown":
				raw = append(raw[:len(raw)-1], []byte(`,"model":"invented"}`)...)
			case "trailing":
				raw = append(raw, []byte(` {}`)...)
			case "oversize":
				raw = []byte(strings.Repeat(" ", companion.AnalysisImportLimit+1))
			case "base64":
				var fields map[string]any
				if err := json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				fields["response_body"] = "invalid base64!"
				raw, _ = json.Marshal(fields)
			}
			var output bytes.Buffer
			if err := importAnalysisCapture(context.Background(), s, owner, bytes.NewReader(raw), &output); err == nil {
				t.Fatal("invalid capture accepted")
			}
			var tables int
			if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='profile_analyses'").Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if tables != 0 || output.Len() != 0 || *calls != 0 {
				t.Fatal("rejected import changed storage or contacted network")
			}
		})
	}
}
