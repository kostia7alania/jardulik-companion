package companion

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func analysisFixture(t *testing.T) (*Store, AnalysisCaptureInput) {
	t.Helper()
	_, s := archiveTestStore(t)
	ctx := context.Background()
	if err := s.SaveSession(ctx, 123, Session{Token: "fixture", UserID: 500, Generation: "capture-generation"}); err != nil {
		t.Fatal(err)
	}
	versions, err := s.Archive(ctx, 123, []Profile{{"userId": 77, "name": "Target", "description": "unchanged source"}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return s, AnalysisCaptureInput{
		UpstreamID: 500, ProfileID: 77, VersionID: versions[0],
		Method: "POST", Path: "/GPT/checkCompatibility", ObservedAtMillis: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC).UnixMilli(),
		HTTPStatus: 200, State: "response_received", Generation: "capture-generation",
		RequestBody:   []byte("{\n  \"fromUserId\": 500, \"targetUserId\": 77\n}\n"),
		ResponseBody:  []byte("{\n  \"content\": \"75% text, not a numeric field\",\n  \"model\": \"gpt-4o-mini\"\n}\n"),
		OwnProfile:    []byte("{\n  \"userId\":500, \"name\":\"Own\"\n}\n"),
		TargetProfile: []byte("[ { \"userId\":77, \"name\":\"Target\" } ]\n"),
	}
}

func TestAnalysisCaptureKeepsExactBodiesAndSourceJSON(t *testing.T) {
	s, input := analysisFixture(t)
	ctx := context.Background()
	before, err := s.Version(ctx, 123, input.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.SaveAnalysisCapture(ctx, 123, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID <= 0 || len(first.CaptureSHA) != 64 || first.Provider != "jardulik" || first.Model == nil || *first.Model != "gpt-4o-mini" {
		t.Fatalf("wrong capture receipt: %+v", first)
	}
	repeated, err := s.SaveAnalysisCapture(ctx, 123, input)
	if err != nil || repeated.ID != first.ID || repeated.CaptureSHA != first.CaptureSHA {
		t.Fatalf("reimport changed capture: id=%d err=%v", repeated.ID, err)
	}
	saved, err := s.LatestProfileAnalysis(ctx, 123, 77)
	if err != nil || saved == nil || saved.ID != first.ID {
		t.Fatalf("missing scoped result: %+v err=%v", saved, err)
	}
	for label, pair := range map[string][2][]byte{
		"request": {saved.RequestBody, input.RequestBody}, "response": {saved.ResponseBody, input.ResponseBody},
		"own": {saved.OwnProfile, input.OwnProfile}, "target": {saved.TargetProfile, input.TargetProfile},
	} {
		if !bytes.Equal(pair[0], pair[1]) {
			t.Fatalf("%s body changed", label)
		}
	}
	after, err := s.Version(ctx, 123, input.VersionID)
	if err != nil || after.RawJSON != before.RawJSON {
		t.Fatal("analysis modified exact profile JSON", err)
	}
}

func TestAnalysisCaptureRejectsMismatchedEvidence(t *testing.T) {
	for _, kind := range []string{"owner", "account", "request-from", "request-target", "own-snapshot", "target-snapshot", "target-array", "version-owner", "version-profile", "path", "state", "status", "timestamp", "generation"} {
		t.Run(kind, func(t *testing.T) {
			s, input := analysisFixture(t)
			ctx := context.Background()
			owner := int64(123)
			switch kind {
			case "owner":
				owner = 456
				if err := s.User(ctx, owner, "fixture"); err != nil {
					t.Fatal(err)
				}
				if err := s.SaveSession(ctx, owner, Session{UserID: 600, Token: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "account":
				if err := s.SaveSession(ctx, owner, Session{UserID: 600, Token: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "request-from":
				input.RequestBody = []byte(`{"fromUserId":600,"targetUserId":77}`)
			case "request-target":
				input.RequestBody = []byte(`{"fromUserId":500,"targetUserId":78}`)
			case "own-snapshot":
				input.OwnProfile = []byte(`{"userId":600}`)
			case "target-snapshot":
				input.TargetProfile = []byte(`{"userId":78}`)
			case "target-array":
				input.TargetProfile = []byte(`[{"userId":77},{"userId":78}]`)
			case "version-owner", "version-profile":
				other, profile := int64(456), int64(77)
				if kind == "version-profile" {
					other, profile = owner, 78
				}
				if err := s.User(ctx, other, "fixture"); err != nil {
					t.Fatal(err)
				}
				versions, err := s.Archive(ctx, other, []Profile{{"userId": profile, "name": "Other"}}, "fixture")
				if err != nil {
					t.Fatal(err)
				}
				input.VersionID = versions[0]
			case "path":
				input.Path = "/user/getMe?userId=77"
			case "state":
				input.State = "invented_success"
			case "status":
				input.HTTPStatus = 0
			case "timestamp":
				input.ObservedAtMillis = 0
			case "generation":
				input.Generation = "bad\ngeneration"
			}
			if _, err := s.SaveAnalysisCapture(ctx, owner, input); err == nil {
				t.Fatal("mismatched capture was accepted")
			}
			if exists, err := snapshotHasTable(ctx, s.DB, "profile_analyses"); err != nil {
				t.Fatal(err)
			} else if exists {
				var count int
				if err := s.DB.QueryRow("SELECT COUNT(*) FROM profile_analyses").Scan(&count); err != nil || count != 0 {
					t.Fatal("rejected capture was stored", err)
				}
			}
		})
	}
}

func TestAnalysisLookupKeepsOwnerAndAccountIsolation(t *testing.T) {
	s, input := analysisFixture(t)
	ctx := context.Background()
	if _, err := s.SaveAnalysisCapture(ctx, 123, input); err != nil {
		t.Fatal(err)
	}
	if err := s.User(ctx, 456, "fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSession(ctx, 456, Session{UserID: 500, Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if result, err := s.LatestProfileAnalysis(ctx, 456, 77); err != nil || result != nil {
		t.Fatal("result leaked to another Telegram owner", err)
	}
	if err := s.SaveSession(ctx, 123, Session{UserID: 600, Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if result, err := s.LatestProfileAnalysis(ctx, 123, 77); err != nil || result != nil {
		t.Fatal("result leaked to another Jardulik account", err)
	}
	if err := s.SaveSession(ctx, 123, Session{UserID: 500, Token: "fixture", Generation: "refreshed"}); err != nil {
		t.Fatal(err)
	}
	if result, err := s.LatestProfileAnalysis(ctx, 123, 77); err != nil || result == nil {
		t.Fatal("same account lost its historical result", err)
	}
	if err := s.Disconnect(ctx, 123); err != nil {
		t.Fatal(err)
	}
	if result, err := s.LatestProfileAnalysis(ctx, 123, 77); err != nil || result != nil {
		t.Fatal("disconnected account exposed a result", err)
	}
}

func TestAnalysisCaptureModelComesOnlyFromRawResponse(t *testing.T) {
	for _, body := range []string{`{"content":"75%"}`, `{"model":null}`, `{"model":17}`, `not JSON`} {
		t.Run(body, func(t *testing.T) {
			s, input := analysisFixture(t)
			input.ResponseBody = []byte(body)
			result, err := s.SaveAnalysisCapture(context.Background(), 123, input)
			if err != nil || result.Model != nil || !bytes.Equal(result.ResponseBody, input.ResponseBody) {
				t.Fatal("missing/unknown model was invented or source was changed", err)
			}
		})
	}
	t.Run("transport unknown", func(t *testing.T) {
		s, input := analysisFixture(t)
		input.State, input.HTTPStatus, input.ResponseBody = "transport_unknown", 0, nil
		result, err := s.SaveAnalysisCapture(context.Background(), 123, input)
		if err != nil || result.Model != nil || len(result.ResponseBody) != 0 || result.State != "transport_unknown" {
			t.Fatal("unknown transport result was changed", err)
		}
	})
}
