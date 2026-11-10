package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const ReactionMessageDraftSchema = "jardulik-reviewed-greetings-v1"
const ReactionMessageDraftImportLimit = 8 << 20
const reactionMessageDraftTable = `CREATE TABLE IF NOT EXISTS reaction_message_drafts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, telegram_username TEXT NOT NULL,
 content_hash TEXT NOT NULL, basis_hash TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('approved','hold')),
 message TEXT NOT NULL, capture_json BLOB NOT NULL, capture_sha TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,upstream_id,profile_id,telegram_username,content_hash,state)
); CREATE INDEX IF NOT EXISTS reaction_message_draft_holds ON reaction_message_drafts(owner_id,upstream_id,profile_id,state);`

var ErrReactionMessageReviewHold = errors.New("отправка остановлена: анкета находится на проверке")
var reactionDraftFields = []string{"name", "age", "height", "description", "profession", "city", "country", "languages"}

type reactionDraftEvidence struct {
	Field string `json:"field"`
	Quote string `json:"quote"`
}
type reactionDraftReview struct {
	Approved bool                    `json:"approved"`
	Notes    string                  `json:"notes"`
	Evidence []reactionDraftEvidence `json:"evidence"`
}
type reactionDraftInput struct {
	ProfileID  int64           `json:"profile_id"`
	Basis      json.RawMessage `json:"basis"`
	State      string          `json:"state"`
	Message    string          `json:"message"`
	Review     json.RawMessage `json:"review"`
	Provenance json.RawMessage `json:"provenance,omitempty"`
}
type reactionDraftBundle struct {
	Schema           string            `json:"schema"`
	OwnerID          int64             `json:"owner_id"`
	UpstreamID       int64             `json:"upstream_id"`
	TelegramUsername string            `json:"telegram_username"`
	Drafts           []json.RawMessage `json:"drafts"`
}
type ReactionMessageDraftImportResult struct {
	Imported  int `json:"imported"`
	Unchanged int `json:"unchanged"`
	Approved  int `json:"approved"`
	Hold      int `json:"hold"`
}

// These are exactly the source fields supplied to the copy/review passes.
func reactionDraftBasis(profile Profile) (Profile, error) {
	basis := Profile{}
	for _, field := range reactionDraftFields {
		value := profile[field]
		if field == "age" || field == "height" {
			if value != nil {
				n, valid := autoInteger(value)
				if !valid {
					return nil, errors.New("invalid numeric greeting basis")
				}
				value = n
			}
		} else if value != nil {
			if _, valid := value.(string); !valid {
				return nil, errors.New("invalid greeting basis field")
			}
		}
		basis[field] = value
	}
	return basis, nil
}
func reactionDraftContent(profile Profile) (Profile, string, error) {
	basis, err := reactionDraftBasis(profile)
	if err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(basis)
	if err != nil {
		return nil, "", err
	}
	return basis, digest(raw), nil
}
func reactionDraftBindingHash(owner, upstream, profile int64, handle, content string) string {
	raw, _ := json.Marshal(map[string]any{"schema": ReactionMessageDraftSchema, "owner_id": owner, "upstream_id": upstream, "profile_id": profile, "telegram_username": handle, "content_hash": content})
	return digest(raw)
}
func reactionDraftHasHandle(message, handle string) bool {
	token := "@" + handle
	for offset := 0; offset < len(message); {
		at := strings.Index(message[offset:], token)
		if at < 0 {
			return false
		}
		end := offset + at + len(token)
		if end == len(message) || !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_", rune(message[end])) {
			return true
		}
		offset = end
	}
	return false
}
func parseReactionDraft(raw []byte) (reactionDraftInput, Profile, string, error) {
	var input reactionDraftInput
	if _, err := debugObject(raw, "profile_id", "basis", "state", "message", "review", "provenance"); err != nil {
		return input, nil, "", errors.New("invalid greeting draft fields")
	}
	if err := json.Unmarshal(raw, &input); err != nil || input.ProfileID <= 0 {
		return input, nil, "", errors.New("invalid greeting draft")
	}
	fields, err := debugObject(input.Basis, reactionDraftFields...)
	if err != nil || len(fields) != len(reactionDraftFields) {
		return input, nil, "", errors.New("greeting basis requires the exact eight source fields")
	}
	profile, err := decodeProfile(input.Basis)
	if err != nil {
		return input, nil, "", err
	}
	basis, hash, err := reactionDraftContent(profile)
	if err != nil {
		return input, nil, "", err
	}
	reviewFields, err := debugObject(input.Review, "approved", "notes", "evidence")
	if err != nil || len(reviewFields) != 3 {
		return input, nil, "", errors.New("invalid greeting review")
	}
	approvedRaw := bytes.TrimSpace(reviewFields["approved"])
	if !bytes.Equal(approvedRaw, []byte("true")) && !bytes.Equal(approvedRaw, []byte("false")) {
		return input, nil, "", errors.New("greeting review needs an explicit boolean decision")
	}
	var review reactionDraftReview
	if err = json.Unmarshal(input.Review, &review); err != nil || strings.TrimSpace(review.Notes) == "" {
		return input, nil, "", errors.New("greeting review needs notes and evidence")
	}
	// Reject unknown evidence fields instead of silently dropping reviewer metadata.
	if evidenceBytes := bytes.TrimSpace(reviewFields["evidence"]); len(evidenceBytes) == 0 || evidenceBytes[0] != '[' {
		return input, nil, "", errors.New("greeting evidence must be an array")
	}
	var evidenceRaw []json.RawMessage
	if err = json.Unmarshal(reviewFields["evidence"], &evidenceRaw); err != nil {
		return input, nil, "", err
	}
	for i, evidence := range review.Evidence {
		if _, err = debugObject(evidenceRaw[i], "field", "quote"); err != nil {
			return input, nil, "", errors.New("invalid greeting evidence")
		}
		value, exists := basis[evidence.Field]
		if !exists || evidence.Quote == "" || !strings.Contains(valueString(value), evidence.Quote) {
			return input, nil, "", errors.New("greeting evidence does not match its source")
		}
	}
	if input.State == "approved" {
		if !review.Approved || strings.TrimSpace(input.Message) == "" || validateReactionMessage(input.Message) != nil {
			return input, nil, "", errors.New("invalid approved greeting")
		}
	} else if input.State == "hold" {
		if review.Approved || input.Message != "" || len(review.Evidence) == 0 {
			return input, nil, "", errors.New("hold must not contain an approved message")
		}
	} else {
		return input, nil, "", errors.New("greeting state must be approved or hold")
	}
	return input, basis, hash, nil
}

func reactionDraftHoldTx(ctx context.Context, tx *sql.Tx, owner, upstream, profile int64) error {
	var held bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM reaction_message_drafts WHERE owner_id=? AND upstream_id=? AND profile_id=? AND state='hold')", owner, upstream, profile).Scan(&held); err != nil {
		return err
	}
	if held {
		return ErrReactionMessageReviewHold
	}
	return nil
}
func reactionDraftMessageTx(ctx context.Context, tx *sql.Tx, owner, upstream, profile int64, handle string, current Profile) (string, bool, error) {
	_, hash, err := reactionDraftContent(current)
	if err != nil {
		return "", false, err
	}
	var message string
	err = tx.QueryRowContext(ctx, `SELECT message FROM reaction_message_drafts WHERE owner_id=? AND upstream_id=? AND profile_id=? AND telegram_username=? AND content_hash=? AND basis_hash=? AND state='approved'`, owner, upstream, profile, handle, hash, reactionDraftBindingHash(owner, upstream, profile, handle, hash)).Scan(&message)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if strings.TrimSpace(message) == "" || validateReactionMessage(message) != nil || !reactionDraftHasHandle(message, handle) {
		return "", false, errors.New("invalid cached greeting")
	}
	return message, true, nil
}

func (s *Store) ImportReactionMessageDrafts(ctx context.Context, owner int64, raw []byte) (ReactionMessageDraftImportResult, error) {
	var result ReactionMessageDraftImportResult
	if len(raw) > ReactionMessageDraftImportLimit || !utf8.Valid(raw) {
		return result, errors.New("invalid greeting bundle encoding or size")
	}
	if _, err := debugObject(raw, "schema", "owner_id", "upstream_id", "telegram_username", "drafts"); err != nil {
		return result, errors.New("invalid greeting bundle fields")
	}
	var bundle reactionDraftBundle
	if err := json.Unmarshal(raw, &bundle); err != nil || bundle.Schema != ReactionMessageDraftSchema || bundle.OwnerID != owner || owner <= 0 || bundle.UpstreamID <= 0 || !reactionMessageUsernamePattern.MatchString(bundle.TelegramUsername) || len(bundle.Drafts) == 0 || len(bundle.Drafts) > 1000 {
		return result, errors.New("invalid greeting bundle binding")
	}
	type prepared struct {
		input reactionDraftInput
		basis Profile
		hash  string
		raw   []byte
	}
	inputs := make([]prepared, 0, len(bundle.Drafts))
	seen := map[int64]bool{}
	for _, entry := range bundle.Drafts {
		input, basis, hash, err := parseReactionDraft(entry)
		if err != nil {
			return result, err
		}
		if seen[input.ProfileID] {
			return result, errors.New("duplicate profile in greeting bundle")
		}
		seen[input.ProfileID] = true
		if input.State == "approved" && !reactionDraftHasHandle(input.Message, bundle.TelegramUsername) {
			return result, errors.New("greeting is missing its exact owner handle")
		}
		inputs = append(inputs, prepared{input, basis, hash, entry})
	}
	if err := s.initReactionMessages(ctx); err != nil {
		return result, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return result, err
	}
	var upstream int64
	var accountRaw string
	if err = tx.QueryRowContext(ctx, "SELECT upstream_id,account_json FROM sessions WHERE owner_id=?", owner).Scan(&upstream, &accountRaw); err != nil {
		return result, err
	}
	if upstream != bundle.UpstreamID {
		return result, ErrReactionSessionChanged
	}
	own, err := decodeProfile([]byte(accountRaw))
	if err != nil || own.ID() != upstream {
		return result, errors.New("invalid current greeting source")
	}
	policy, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return result, err
	}
	if policy.TelegramUsername != bundle.TelegramUsername {
		return result, errors.New("greeting handle differs from current owner policy")
	}
	filters := existingFeedFilters(own)
	var filterTable bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_filters')").Scan(&filterTable); err != nil {
		return result, err
	}
	if filterTable {
		var applied sql.NullString
		e := tx.QueryRowContext(ctx, "SELECT applied_json FROM feed_filters WHERE owner_id=?", owner).Scan(&applied)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return result, e
		}
		if applied.Valid && json.Unmarshal([]byte(applied.String), &filters) != nil {
			return result, errors.New("invalid current greeting filters")
		}
	}
	for _, entry := range inputs {
		var targetRaw string
		if err = tx.QueryRowContext(ctx, `SELECT v.raw_json FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?`, owner, entry.input.ProfileID).Scan(&targetRaw); err != nil {
			return ReactionMessageDraftImportResult{}, fmt.Errorf("greeting profile %d is not current for this owner", entry.input.ProfileID)
		}
		target, e := decodeProfile([]byte(targetRaw))
		if e != nil || target.ID() != entry.input.ProfileID {
			return ReactionMessageDraftImportResult{}, errors.New("invalid current greeting profile")
		}
		_, currentHash, e := reactionDraftContent(target)
		if e != nil {
			return ReactionMessageDraftImportResult{}, e
		}
		if currentHash != entry.hash {
			return ReactionMessageDraftImportResult{}, fmt.Errorf("greeting profile %d source has changed", entry.input.ProfileID)
		}
		if entry.input.State == "approved" {
			if !compatibilityMatchesFilters(target, filters) || birthdayGreetingMinorDOB(target, time.Now().UTC()) {
				return ReactionMessageDraftImportResult{}, fmt.Errorf("greeting profile %d is outside current adult filters", entry.input.ProfileID)
			}
			if e = reactionDraftHoldTx(ctx, tx, owner, upstream, entry.input.ProfileID); e != nil {
				return ReactionMessageDraftImportResult{}, e
			}
		}
		var oldMessage string
		e = tx.QueryRowContext(ctx, `SELECT message FROM reaction_message_drafts WHERE owner_id=? AND upstream_id=? AND profile_id=? AND telegram_username=? AND content_hash=? AND state=?`, owner, upstream, entry.input.ProfileID, bundle.TelegramUsername, entry.hash, entry.input.State).Scan(&oldMessage)
		if e == nil {
			if oldMessage != entry.input.Message {
				return ReactionMessageDraftImportResult{}, fmt.Errorf("greeting profile %d conflicts with approved content", entry.input.ProfileID)
			}
			result.Unchanged++
		} else if !errors.Is(e, sql.ErrNoRows) {
			return ReactionMessageDraftImportResult{}, e
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_drafts(owner_id,upstream_id,profile_id,telegram_username,content_hash,basis_hash,state,message,capture_json,capture_sha) VALUES(?,?,?,?,?,?,?,?,?,?)`, owner, upstream, entry.input.ProfileID, bundle.TelegramUsername, entry.hash, reactionDraftBindingHash(owner, upstream, entry.input.ProfileID, bundle.TelegramUsername, entry.hash), entry.input.State, entry.input.Message, []byte(entry.raw), digest(entry.raw))
			if err != nil {
				return ReactionMessageDraftImportResult{}, err
			}
			result.Imported++
		}
		if entry.input.State == "approved" {
			result.Approved++
		} else {
			result.Hold++
		}
	}
	return result, tx.Commit()
}

func (s *Store) ReactionMessageDraftStatus(ctx context.Context, owner int64) (map[string]int, error) {
	result := map[string]int{"approved": 0, "hold": 0}
	if err := s.initReactionMessages(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT d.state,count(*) FROM reaction_message_drafts d JOIN sessions s ON s.owner_id=d.owner_id AND s.upstream_id=d.upstream_id WHERE d.owner_id=? GROUP BY d.state`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int
		if err = rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		result[state] = count
	}
	return result, rows.Err()
}
