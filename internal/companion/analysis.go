package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode"
)

const AnalysisImportLimit = 8 << 20

// Byte fields use base64 in the import envelope to preserve exact source bodies.
type AnalysisCaptureInput struct {
	UpstreamID       int64  `json:"upstream_id"`
	ProfileID        int64  `json:"profile_id"`
	VersionID        int64  `json:"version_id,omitempty"`
	Method           string `json:"method"`
	Path             string `json:"path"`
	ObservedAtMillis int64  `json:"observed_at_ms"`
	HTTPStatus       int    `json:"http_status"`
	State            string `json:"state"`
	Generation       string `json:"generation"`
	RequestBody      []byte `json:"request_body"`
	ResponseBody     []byte `json:"response_body"`
	OwnProfile       []byte `json:"own_profile"`
	TargetProfile    []byte `json:"target_profile"`
}

type ProfileAnalysis struct {
	ID      int64 `json:"id"`
	OwnerID int64 `json:"owner_id"`
	AnalysisCaptureInput
	Provider   string  `json:"provider"`
	Model      *string `json:"model"`
	CaptureSHA string  `json:"capture_sha"`
}

const analysisSchema = `
CREATE TABLE IF NOT EXISTS profile_analyses (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 version_id INTEGER REFERENCES versions(id) ON DELETE SET NULL,
 provider TEXT NOT NULL CHECK(provider='jardulik'), model TEXT,
 observed_at_ms INTEGER NOT NULL, http_status INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('response_received','transport_unknown')),
 generation TEXT NOT NULL, request_body BLOB NOT NULL, response_body BLOB NOT NULL,
 own_profile BLOB NOT NULL, target_profile BLOB NOT NULL, capture_sha TEXT NOT NULL,
 UNIQUE(owner_id,upstream_id,capture_sha)
);
CREATE INDEX IF NOT EXISTS profile_analyses_latest ON profile_analyses(owner_id,upstream_id,profile_id,observed_at_ms DESC,id DESC);
`

func analysisSnapshotID(raw []byte, singleArray bool) (int64, error) {
	body := bytes.TrimSpace(raw)
	if singleArray && len(body) > 0 && body[0] == '[' {
		var entries []json.RawMessage
		if json.Unmarshal(body, &entries) != nil || len(entries) != 1 {
			return 0, errors.New("analysis snapshot requires one profile")
		}
		body = entries[0]
	}
	var profile struct {
		UserID int64 `json:"userId"`
	}
	if json.Unmarshal(body, &profile) != nil || profile.UserID <= 0 {
		return 0, errors.New("analysis snapshot has no valid userId")
	}
	return profile.UserID, nil
}

func validateAnalysisCapture(owner int64, input AnalysisCaptureInput) error {
	if owner <= 0 || input.UpstreamID <= 0 || input.ProfileID <= 0 || input.VersionID < 0 || input.ObservedAtMillis <= 0 {
		return errors.New("invalid analysis capture identity or timestamp")
	}
	if input.Method != http.MethodPost || input.Path != "/GPT/checkCompatibility" || len(input.Generation) == 0 || len(input.Generation) > 256 || strings.IndexFunc(input.Generation, unicode.IsControl) >= 0 {
		return errors.New("invalid analysis capture request metadata")
	}
	if input.State != "response_received" && input.State != "transport_unknown" || input.HTTPStatus < 0 || input.HTTPStatus > 599 || input.HTTPStatus > 0 && input.HTTPStatus < 100 || input.State == "response_received" && input.HTTPStatus == 0 {
		return errors.New("invalid analysis capture outcome")
	}
	if len(input.RequestBody)+len(input.ResponseBody)+len(input.OwnProfile)+len(input.TargetProfile) > AnalysisImportLimit {
		return errors.New("analysis capture exceeds import limit")
	}
	var request struct {
		FromUserID   int64 `json:"fromUserId"`
		TargetUserID int64 `json:"targetUserId"`
	}
	if json.Unmarshal(input.RequestBody, &request) != nil || request.FromUserID != input.UpstreamID || request.TargetUserID != input.ProfileID {
		return errors.New("analysis capture request does not match its account and profile")
	}
	own, err := analysisSnapshotID(input.OwnProfile, false)
	if err != nil || own != input.UpstreamID {
		return errors.New("analysis capture own snapshot does not match its account")
	}
	target, err := analysisSnapshotID(input.TargetProfile, true)
	if err != nil || target != input.ProfileID {
		return errors.New("analysis capture target snapshot does not match its profile")
	}
	return nil
}

const analysisColumns = "a.id,a.owner_id,a.upstream_id,a.profile_id,COALESCE(a.version_id,0),a.provider,a.model,a.observed_at_ms,a.http_status,a.state,a.generation,a.request_body,a.response_body,a.own_profile,a.target_profile,a.capture_sha"

func scanProfileAnalysis(row interface{ Scan(...any) error }) (ProfileAnalysis, error) {
	var result ProfileAnalysis
	var model sql.NullString
	err := row.Scan(&result.ID, &result.OwnerID, &result.UpstreamID, &result.ProfileID, &result.VersionID, &result.Provider, &model, &result.ObservedAtMillis, &result.HTTPStatus, &result.State, &result.Generation, &result.RequestBody, &result.ResponseBody, &result.OwnProfile, &result.TargetProfile, &result.CaptureSHA)
	if model.Valid {
		result.Model = &model.String
	}
	result.Method, result.Path = http.MethodPost, "/GPT/checkCompatibility"
	return result, err
}

// SaveAnalysisCapture imports an operator HTTP capture. It never sends requests or changes profile JSON.
func (s *Store) SaveAnalysisCapture(ctx context.Context, owner int64, input AnalysisCaptureInput) (ProfileAnalysis, error) {
	var empty ProfileAnalysis
	if err := validateAnalysisCapture(owner, input); err != nil {
		return empty, err
	}
	// Version attachment is local metadata, not a new source response.
	identity := input
	identity.VersionID = 0
	if identity.ResponseBody == nil {
		identity.ResponseBody = []byte{}
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		return empty, err
	}
	sha := digest(raw)
	var response struct {
		Model *string `json:"model"`
	}
	var model any
	if json.Unmarshal(input.ResponseBody, &response) == nil && response.Model != nil {
		model = *response.Model
	}
	if _, err = s.DB.ExecContext(ctx, analysisSchema); err != nil {
		return empty, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	// Reserve the writer before checking account binding across Store instances.
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_analyses'"); err != nil {
		return empty, err
	}
	var upstream int64
	if err = tx.QueryRowContext(ctx, "SELECT upstream_id FROM sessions WHERE owner_id=?", owner).Scan(&upstream); err != nil {
		return empty, err
	}
	if upstream != input.UpstreamID {
		return empty, errors.New("analysis capture belongs to another Jardulik account")
	}
	var version any
	if input.VersionID > 0 {
		var owned bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM versions WHERE id=? AND owner_id=? AND profile_id=?)", input.VersionID, owner, input.ProfileID).Scan(&owned); err != nil {
			return empty, err
		}
		if !owned {
			return empty, errors.New("analysis version does not belong to this owner and profile")
		}
		version = input.VersionID
	}
	body := input.ResponseBody
	if body == nil {
		body = []byte{}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO profile_analyses(owner_id,upstream_id,profile_id,version_id,provider,model,observed_at_ms,http_status,state,generation,request_body,response_body,own_profile,target_profile,capture_sha)
 VALUES(?,?,?,?,'jardulik',?,?,?,?,?,?,?,?,?,?) ON CONFLICT(owner_id,upstream_id,capture_sha) DO NOTHING`, owner, input.UpstreamID, input.ProfileID, version, model, input.ObservedAtMillis, input.HTTPStatus, input.State, input.Generation, input.RequestBody, body, input.OwnProfile, input.TargetProfile, sha)
	if err != nil {
		return empty, err
	}
	result, err := scanProfileAnalysis(tx.QueryRowContext(ctx, "SELECT "+analysisColumns+" FROM profile_analyses a WHERE a.owner_id=? AND a.upstream_id=? AND a.capture_sha=?", owner, upstream, sha))
	if err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	return result, nil
}

// LatestProfileAnalysis returns only captures of the owner's currently bound account.
func (s *Store) LatestProfileAnalysis(ctx context.Context, owner, profile int64) (*ProfileAnalysis, error) {
	exists, err := snapshotHasTable(ctx, s.DB, "profile_analyses")
	if err != nil || !exists {
		return nil, err
	}
	result, err := scanProfileAnalysis(s.DB.QueryRowContext(ctx, "SELECT "+analysisColumns+" FROM profile_analyses a JOIN sessions s ON s.owner_id=a.owner_id AND s.upstream_id=a.upstream_id WHERE a.owner_id=? AND a.profile_id=? ORDER BY a.observed_at_ms DESC,a.id DESC LIMIT 1", owner, profile))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &result, nil
}
