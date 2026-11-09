package companion

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestAnalysisCaptureChangesDailyFingerprintAndSurvivesSnapshot(t *testing.T) {
	s, input := analysisFixture(t)
	ctx := context.Background()
	before := fullTestFingerprint(t, s)
	first, err := s.SaveAnalysisCapture(ctx, 123, input)
	if err != nil {
		t.Fatal(err)
	}
	after := fullTestFingerprint(t, s)
	if before == after {
		t.Fatal("analysis-only addition did not dirty the daily backup")
	}
	if repeated, err := s.SaveAnalysisCapture(ctx, 123, input); err != nil || repeated.ID != first.ID {
		t.Fatal("reimport was not idempotent", err)
	}
	if fullTestFingerprint(t, s) != after {
		t.Fatal("reimport dirtied the daily backup")
	}
	if _, err := s.LatestProfileAnalysis(ctx, 123, 77); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != after {
		t.Fatal("read dirtied the daily backup")
	}
	input.ObservedAtMillis++
	input.ResponseBody = []byte("{ \"content\": \"new result\", \"model\": \"gpt-4o-mini\" }\n")
	second, err := s.SaveAnalysisCapture(ctx, 123, input)
	if err != nil || second.ID == first.ID {
		t.Fatal("new result replaced history", err)
	}
	if fullTestFingerprint(t, s) == after {
		t.Fatal("new analysis response did not dirty the backup")
	}
	path := filepath.Join(t.TempDir(), "snapshot.sqlite")
	if err := s.Snapshot(ctx, path); err != nil {
		t.Fatal(err)
	}
	db, err := openFullSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT response_body FROM profile_analyses WHERE id=?", second.ID).Scan(&raw); err != nil || !bytes.Equal(raw, input.ResponseBody) {
		t.Fatal("snapshot changed exact analysis response", err)
	}
}
