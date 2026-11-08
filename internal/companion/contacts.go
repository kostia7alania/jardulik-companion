package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var (
	ErrContactSessionUnavailable = errors.New("contact evidence requires a connected or paused account")
	ErrContactSessionChanged     = errors.New("contact evidence belongs to another Jardulik account")
	ErrContactVersionMismatch    = errors.New("contact evidence version does not belong to this profile and owner")
	ErrContactEvidenceConflict   = errors.New("contact evidence source timestamp conflicts with the recorded proof")
)

type ContactEvidenceInput struct {
	UpstreamID             int64     `json:"upstream_id"`
	ProfileID              int64     `json:"profile_id"`
	VersionID              int64     `json:"version_id,omitempty"`
	SourceBotID            int64     `json:"source_bot_id"`
	SourcePeerID           int64     `json:"source_peer_id"`
	SourceLocatorKind      string    `json:"source_locator_kind"`
	SourceMessageLocator   string    `json:"source_message_locator"`
	SourceMessageTimestamp int64     `json:"source_message_timestamp"`
	ContactURL             string    `json:"contact_url"`
	ProfileURL             string    `json:"profile_url"`
	ObservedAt             time.Time `json:"observed_at"`
}

type ContactEvidence struct {
	ID      int64 `json:"id"`
	OwnerID int64 `json:"owner_id"`
	ContactEvidenceInput
	EvidenceKind string `json:"evidence_kind"`
	InputJSON    string `json:"-"`
}

const contactEvidenceSchema = `
CREATE TABLE IF NOT EXISTS profile_contact_evidence (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 version_id INTEGER REFERENCES versions(id) ON DELETE SET NULL,
 source_bot_id INTEGER NOT NULL, source_peer_id INTEGER NOT NULL,
 source_locator_kind TEXT NOT NULL CHECK(source_locator_kind='telegram_web_dom'),
 source_message_locator TEXT NOT NULL, source_message_timestamp INTEGER NOT NULL,
 contact_url TEXT NOT NULL, profile_url TEXT NOT NULL,
 evidence_kind TEXT NOT NULL CHECK(evidence_kind='official_bot_ui'),
 observed_at_ms INTEGER NOT NULL, input_json TEXT NOT NULL,
 UNIQUE(owner_id,upstream_id,profile_id,source_bot_id,source_peer_id,source_locator_kind,source_message_locator,contact_url,profile_url)
);
CREATE INDEX IF NOT EXISTS profile_contact_latest ON profile_contact_evidence(owner_id,upstream_id,profile_id,source_message_timestamp DESC,observed_at_ms DESC,id DESC);
`

func (s *Store) initContactEvidence(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, contactEvidenceSchema)
	return err
}

func contactEvidenceURL(raw, host, path string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 512 || strings.TrimSpace(raw) != raw || strings.IndexFunc(raw, unicode.IsControl) >= 0 || u.Scheme != "https" || u.Host != host || u.User != nil || u.Opaque != "" || u.RawPath != "" || u.Fragment != "" || u.RawFragment != "" || path != "" && u.Path != path {
		return nil, errors.New("invalid contact evidence URL")
	}
	return u, nil
}

func (s *Store) validateContactEvidence(owner int64, input ContactEvidenceInput) error {
	if owner <= 0 || input.UpstreamID <= 0 || input.ProfileID <= 0 || input.VersionID < 0 || s.cfg.SourceBotID <= 0 || input.SourceBotID != s.cfg.SourceBotID || input.SourcePeerID != input.SourceBotID {
		return errors.New("invalid contact evidence identity")
	}
	// A DOM locator is opaque UI evidence, never an MTProto/Bot API message ID.
	if input.SourceLocatorKind != "telegram_web_dom" || !strings.HasPrefix(input.SourceMessageLocator, "data-mid:") || len(input.SourceMessageLocator) <= len("data-mid:") || len(input.SourceMessageLocator) > 160 || strings.IndexFunc(input.SourceMessageLocator, unicode.IsControl) >= 0 {
		return errors.New("invalid contact evidence source locator")
	}
	if input.ObservedAt.IsZero() || input.SourceMessageTimestamp <= 0 || input.SourceMessageTimestamp > input.ObservedAt.Unix() {
		return errors.New("invalid contact evidence timestamps")
	}
	contact, err := contactEvidenceURL(input.ContactURL, "t.me", "")
	if err != nil || contact.RawQuery != "" || contact.ForceQuery || !strings.HasPrefix(contact.Path, "/") {
		return errors.New("invalid public Telegram contact URL")
	}
	username := strings.TrimPrefix(contact.Path, "/")
	if len(username) < 5 || len(username) > 32 {
		return errors.New("invalid public Telegram contact URL")
	}
	for i, char := range username {
		letter := char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z'
		if !letter && (i == 0 || char < '0' || char > '9') && (i == 0 || char != '_') {
			return errors.New("invalid public Telegram contact URL")
		}
	}
	profile, err := contactEvidenceURL(input.ProfileURL, "mini.jardulik.app", "/mutuals")
	if err != nil || profile.ForceQuery && profile.RawQuery == "" {
		return errors.New("invalid Jardulik profile URL")
	}
	query, err := url.ParseQuery(profile.RawQuery)
	if err != nil || len(query) != 2 || len(query["targetUserId"]) != 1 || len(query["noCheck"]) != 1 || query.Get("noCheck") != "1" || query.Get("targetUserId") != strconv.FormatInt(input.ProfileID, 10) {
		return errors.New("Jardulik profile URL does not match the observed target")
	}
	return nil
}

const contactEvidenceColumns = "id,owner_id,COALESCE(version_id,0),evidence_kind,input_json"

func scanContactEvidence(row interface{ Scan(...any) error }) (ContactEvidence, error) {
	var result ContactEvidence
	var version int64
	if err := row.Scan(&result.ID, &result.OwnerID, &version, &result.EvidenceKind, &result.InputJSON); err != nil {
		return result, err
	}
	if err := json.Unmarshal([]byte(result.InputJSON), &result.ContactEvidenceInput); err != nil {
		return result, errors.New("invalid stored contact evidence")
	}
	result.VersionID = version
	return result, nil
}

// SaveContactEvidence records an operator's observed official bot UI, not an API attestation.
func (s *Store) SaveContactEvidence(ctx context.Context, owner int64, input ContactEvidenceInput) (ContactEvidence, error) {
	var result ContactEvidence
	if err := s.validateContactEvidence(owner, input); err != nil {
		return result, err
	}
	body, err := json.Marshal(input)
	if err != nil {
		return result, errors.New("invalid contact evidence input")
	}
	if err = s.initContactEvidence(ctx); err != nil {
		return result, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	// Reserve the writer before reading the account binding, including across Store instances.
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_contact_evidence'"); err != nil {
		return result, err
	}
	var upstream int64
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT upstream_id,state FROM sessions WHERE owner_id=?", owner).Scan(&upstream, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, ErrContactSessionUnavailable
		}
		return result, err
	}
	if state != "connected" && state != "paused" {
		return result, ErrContactSessionUnavailable
	}
	if upstream != input.UpstreamID {
		return result, ErrContactSessionChanged
	}
	var version any
	if input.VersionID > 0 {
		var owned bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM versions WHERE id=? AND owner_id=? AND profile_id=?)", input.VersionID, owner, input.ProfileID).Scan(&owned); err != nil {
			return result, err
		}
		if !owned {
			return result, ErrContactVersionMismatch
		}
		version = input.VersionID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO profile_contact_evidence(owner_id,upstream_id,profile_id,version_id,source_bot_id,source_peer_id,source_locator_kind,source_message_locator,source_message_timestamp,contact_url,profile_url,evidence_kind,observed_at_ms,input_json)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,'official_bot_ui',?,?) ON CONFLICT(owner_id,upstream_id,profile_id,source_bot_id,source_peer_id,source_locator_kind,source_message_locator,contact_url,profile_url) DO NOTHING`, owner, input.UpstreamID, input.ProfileID, version, input.SourceBotID, input.SourcePeerID, input.SourceLocatorKind, input.SourceMessageLocator, input.SourceMessageTimestamp, input.ContactURL, input.ProfileURL, input.ObservedAt.UnixMilli(), string(body))
	if err != nil {
		return result, err
	}
	result, err = scanContactEvidence(tx.QueryRowContext(ctx, "SELECT "+contactEvidenceColumns+" FROM profile_contact_evidence WHERE owner_id=? AND upstream_id=? AND profile_id=? AND source_bot_id=? AND source_peer_id=? AND source_locator_kind=? AND source_message_locator=? AND contact_url=? AND profile_url=?", owner, input.UpstreamID, input.ProfileID, input.SourceBotID, input.SourcePeerID, input.SourceLocatorKind, input.SourceMessageLocator, input.ContactURL, input.ProfileURL))
	if err != nil {
		return result, err
	}
	if result.SourceMessageTimestamp != input.SourceMessageTimestamp {
		return ContactEvidence{}, ErrContactEvidenceConflict
	}
	if err = tx.Commit(); err != nil {
		return ContactEvidence{}, err
	}
	return result, nil
}

// ProfileTelegramContact returns only evidence revealed to the currently bound account.
func (s *Store) ProfileTelegramContact(ctx context.Context, owner, profileID int64) (*ContactEvidence, error) {
	if owner <= 0 || profileID <= 0 {
		return nil, errors.New("invalid contact evidence profile")
	}
	if err := s.initContactEvidence(ctx); err != nil {
		return nil, err
	}
	result, err := scanContactEvidence(s.DB.QueryRowContext(ctx, `SELECT e.id,e.owner_id,COALESCE(e.version_id,0),e.evidence_kind,e.input_json
 FROM profile_contact_evidence e JOIN sessions s ON s.owner_id=e.owner_id AND s.upstream_id=e.upstream_id
 WHERE e.owner_id=? AND e.profile_id=? AND e.source_bot_id=? AND e.source_peer_id=? AND s.state IN ('connected','paused','reconnect_required')
 ORDER BY e.source_message_timestamp DESC,e.observed_at_ms DESC,e.id DESC LIMIT 1`, owner, profileID, s.cfg.SourceBotID, s.cfg.SourceBotID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &result, nil
}
