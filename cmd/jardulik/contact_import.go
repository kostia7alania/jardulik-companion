package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func importContactEvidence(ctx context.Context, service *companion.Service, owner int64, input io.Reader, output io.Writer) error {
	if owner != service.Config.OwnerID {
		return errors.New("contact import requires the configured operator account")
	}
	raw, err := io.ReadAll(io.LimitReader(input, (64<<10)+1))
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("contact evidence exceeds import limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var evidence companion.ContactEvidenceInput
	if err := decoder.Decode(&evidence); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("contact import requires one JSON object")
	}
	saved, err := service.Store.SaveContactEvidence(ctx, owner, evidence)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"stored": true, "contact_evidence_id": saved.ID, "profile_id": saved.ProfileID})
}
