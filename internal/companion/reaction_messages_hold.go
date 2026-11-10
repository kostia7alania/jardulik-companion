package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const reactionMessageHoldResolutionTable = `CREATE TABLE IF NOT EXISTS reaction_message_hold_resolutions (
 draft_id INTEGER PRIMARY KEY REFERENCES reaction_message_drafts(id),
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 evidence_json BLOB NOT NULL, evidence_sha TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

type ReactionMessageHoldResolution struct {
	DraftID         int64 `json:"draft_id"`
	OwnerID         int64 `json:"owner_id"`
	UpstreamID      int64 `json:"upstream_id"`
	ProfileID       int64 `json:"profile_id"`
	AlreadyResolved bool  `json:"already_resolved"`
}

type reactionMessageHoldEvidence struct {
	Reason         string `json:"reason"`
	DraftID        int64  `json:"draft_id"`
	OwnerID        int64  `json:"owner_id"`
	UpstreamID     int64  `json:"upstream_id"`
	ProfileID      int64  `json:"profile_id"`
	HoldCaptureSHA string `json:"hold_capture_sha"`
	VersionID      int64  `json:"version_id"`
	TargetRawJSON  string `json:"target_raw_json"`
	TargetRawSHA   string `json:"target_raw_sha"`
	ObservedAt     string `json:"observed_at"`
	ResolvedAt     string `json:"resolved_at"`
}

// ResolveReactionMessageHold records a review of one immutable hold. It performs
// no network requests and does not alter reaction intents, policy or automation.
func (s *Store) ResolveReactionMessageHold(ctx context.Context, owner, draftID int64, reason string) (ReactionMessageHoldResolution, error) {
	result := ReactionMessageHoldResolution{OwnerID: owner, DraftID: draftID}
	if owner <= 0 || draftID <= 0 || !utf8.ValidString(reason) || strings.TrimSpace(reason) == "" || textUnits(reason) > 2000 {
		return result, errors.New("укажи draft_id и непустое основание снятия проверки до 2000 UTF-16 символов")
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
	var holdSHA string
	if err = tx.QueryRowContext(ctx, `SELECT upstream_id,profile_id,capture_sha FROM reaction_message_drafts WHERE id=? AND owner_id=? AND state='hold'`, draftID, owner).Scan(&result.UpstreamID, &result.ProfileID, &holdSHA); err != nil {
		return result, err
	}
	session, err := s.businessSession(ctx, tx, owner)
	if err != nil {
		return result, err
	}
	if session.UserID != result.UpstreamID {
		return result, ErrReactionSessionChanged
	}
	var accountRaw string
	if err = tx.QueryRowContext(ctx, "SELECT account_json FROM sessions WHERE owner_id=?", owner).Scan(&accountRaw); err != nil {
		return result, err
	}
	own, err := decodeProfile([]byte(accountRaw))
	if err != nil || own.ID() != result.UpstreamID {
		return result, errors.New("invalid current greeting source")
	}
	var previous []byte
	err = tx.QueryRowContext(ctx, `SELECT evidence_json FROM reaction_message_hold_resolutions WHERE draft_id=? AND owner_id=? AND upstream_id=? AND profile_id=?`, draftID, owner, result.UpstreamID, result.ProfileID).Scan(&previous)
	if err == nil {
		var evidence reactionMessageHoldEvidence
		if json.Unmarshal(previous, &evidence) != nil || evidence.Reason != reason {
			return result, errors.New("hold resolution already exists with a different review")
		}
		result.AlreadyResolved = true
		return result, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	var targetRaw, observed string
	var versionID int64
	if err = tx.QueryRowContext(ctx, `SELECT v.id,v.raw_json,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?`, owner, result.ProfileID).Scan(&versionID, &targetRaw, &observed); err != nil {
		return result, err
	}
	target, err := decodeProfile([]byte(targetRaw))
	if err != nil || target.ID() != result.ProfileID {
		return result, errors.New("invalid current greeting profile")
	}
	age, knownAge := autoInteger(target["age"])
	now := time.Now().UTC()
	dob, knownDOB := birthdayGreetingDate(target)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if !knownAge || age < 18 || age > 111 || !knownDOB || dob.After(today) || birthdayGreetingMinorDOB(target, now) {
		return result, ErrReactionMessageTargetAge
	}
	evidence := reactionMessageHoldEvidence{Reason: reason, DraftID: draftID, OwnerID: owner, UpstreamID: result.UpstreamID, ProfileID: result.ProfileID, HoldCaptureSHA: holdSHA, VersionID: versionID, TargetRawJSON: targetRaw, TargetRawSHA: digest([]byte(targetRaw)), ObservedAt: observed, ResolvedAt: now.Format(time.RFC3339Nano)}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_hold_resolutions(draft_id,owner_id,upstream_id,profile_id,evidence_json,evidence_sha) VALUES(?,?,?,?,?,?)`, draftID, owner, result.UpstreamID, result.ProfileID, raw, digest(raw)); err != nil {
		return result, err
	}
	return result, tx.Commit()
}
