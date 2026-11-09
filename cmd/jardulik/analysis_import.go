package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func importAnalysisCapture(ctx context.Context, service *companion.Service, owner int64, input io.Reader, output io.Writer) error {
	if owner != service.Config.OwnerID {
		return errors.New("analysis import requires the configured operator account")
	}
	raw, err := io.ReadAll(io.LimitReader(input, companion.AnalysisImportLimit+1))
	if err != nil {
		return err
	}
	if len(raw) > companion.AnalysisImportLimit {
		return errors.New("analysis capture exceeds import limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var capture companion.AnalysisCaptureInput
	if err = decoder.Decode(&capture); err != nil {
		return err
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return errors.New("analysis import requires one JSON object")
	}
	saved, err := service.Store.SaveAnalysisCapture(ctx, owner, capture)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"stored": true, "analysis_id": saved.ID, "profile_id": saved.ProfileID, "provider": saved.Provider, "model": saved.Model, "capture_sha": saved.CaptureSHA, "http_status": saved.HTTPStatus, "state": saved.State})
}
