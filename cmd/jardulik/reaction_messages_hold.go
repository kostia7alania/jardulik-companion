package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func resolveReactionMessageHold(ctx context.Context, s *companion.Service, owner int64, input io.Reader, output io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(input, 16385))
	if err != nil {
		return err
	}
	if len(raw) > 16384 || !utf8.Valid(raw) {
		return errors.New("invalid hold resolution JSON")
	}
	var request struct {
		DraftID int64  `json:"draft_id"`
		Reason  string `json:"reason"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&request); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one hold resolution object")
	}
	result, err := s.Store.ResolveReactionMessageHold(ctx, owner, request.DraftID, request.Reason)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
