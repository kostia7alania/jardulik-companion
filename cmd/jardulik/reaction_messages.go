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

func setReactionMessagePolicy(ctx context.Context, s *companion.Service, owner int64, input io.Reader, output io.Writer) error {
	const limit = 16 << 10
	raw, err := io.ReadAll(io.LimitReader(input, limit+1))
	if err != nil {
		return err
	}
	if len(raw) > limit || !utf8.Valid(raw) {
		return errors.New("invalid reaction message policy input")
	}
	var in companion.ReactionMessagePolicyConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&in); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("reaction message policy must be one JSON object")
	}
	policy, err := s.Store.ConfigureReactionMessagePolicy(ctx, owner, in)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(policy)
}
