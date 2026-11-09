package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kostia7alania/jardulik-companion/internal/companion"
	"io"
)

func importReactionMessageDrafts(ctx context.Context, s *companion.Service, owner int64, input io.Reader, output io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(input, companion.ReactionMessageDraftImportLimit+1))
	if err != nil {
		return err
	}
	if len(raw) > companion.ReactionMessageDraftImportLimit {
		return errors.New("greeting bundle exceeds import limit")
	}
	result, err := s.Store.ImportReactionMessageDrafts(ctx, owner, raw)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
