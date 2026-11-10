package companion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// All callers retain the existing observer: confirmed missing profiles are saved
// before ErrProfileUnavailable is returned; network/auth errors are not absence.
func (c *Client) checkedProfileObserved(ctx context.Context, owner int64, expected Session, target int64) (Profile, []byte, time.Time, error) {
	if err := c.Store.KnownProfile(ctx, owner, target); err != nil {
		return nil, nil, time.Time{}, err
	}
	query := url.Values{"viewerUserId": {strconv.FormatInt(expected.UserID, 10)}, "targetUserId": {strconv.FormatInt(target, 10)}}
	raw, observed, err := c.requestSessionObserved(ctx, owner, http.MethodGet, "/user/getUser?"+query.Encode(), nil, &expected)
	if err != nil {
		return nil, raw, observed, err
	}
	if err = c.debugBinding(ctx, owner, expected); err != nil {
		return nil, raw, observed, err
	}
	profile, err := debugProfile(raw)
	if err != nil || profile.ID() != target {
		return nil, raw, observed, errors.New("Jardulik profile identity or format changed")
	}
	return profile, raw, observed, nil
}

func profileKnownUnavailable(ctx context.Context, q reactionQuerier, owner, target int64) (bool, error) {
	var exists bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='profile_presence')").Scan(&exists); err != nil || !exists {
		return false, err
	}
	var unavailable bool
	err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM profile_presence WHERE owner_id=? AND profile_id=? AND availability='unavailable')", owner, target).Scan(&unavailable)
	return unavailable, err
}

// The caller owns SessionMutex. Successful reads use one preservation path in
// the daily checker and GPT executor, while the negative path stays in Client.
func (s *Service) captureCheckedProfileLocked(ctx context.Context, owner int64, expected Session, profile Profile, observed time.Time) (StoredProfile, error) {
	if err := s.Client.debugBinding(ctx, owner, expected); err != nil {
		return StoredProfile{}, err
	}
	versions, err := s.Store.ArchiveViewed(ctx, owner, []Profile{profile}, "GET /user/getUser")
	if err != nil {
		return StoredProfile{}, err
	}
	if _, err = s.Store.recordProfilePresence(ctx, owner, profile.ID(), true, observed); err != nil {
		return StoredProfile{}, err
	}
	if s.Config.SyncCycleEnabled {
		if err = s.Store.RecordSyncFullCapture(ctx, owner, expected, profile.ID(), versions[0], observed); err != nil {
			return StoredProfile{}, err
		}
	}
	return s.Store.Version(ctx, owner, versions[0])
}

// A source-bound full read from this sweep is already the preflight GET. The
// provider can still race with deactivation; that outcome triggers a new check.
func (s *Service) cachedCheckedProfile(ctx context.Context, owner int64, expected Session, profile StoredProfile, now time.Time) (bool, error) {
	if !s.Config.SyncCycleEnabled {
		return false, nil
	}
	cycle, err := s.Store.SyncCycle(ctx, owner)
	if err != nil || cycle == nil || cycle.UserID != expected.UserID || cycle.Generation != expected.Generation {
		return false, err
	}
	exists, err := snapshotHasTable(ctx, s.Store.DB, "sync_full_reads")
	if err != nil || !exists {
		return false, err
	}
	missing, err := profileKnownUnavailable(ctx, s.Store.DB, owner, profile.ID)
	if err != nil || missing {
		return false, err
	}
	var fresh bool
	err = s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_full_reads r
 JOIN queue q ON q.owner_id=r.owner_id AND q.profile_id=r.profile_id AND q.version_id=r.version_id
 WHERE r.owner_id=? AND r.upstream_id=? AND r.generation=? AND r.profile_id=? AND r.version_id=?
 AND r.observed_at_ms>=? AND r.observed_at_ms<=?)`, owner, expected.UserID, expected.Generation, profile.ID, profile.VersionID, cycle.StartedAt, now.UnixMilli()).Scan(&fresh)
	return fresh, err
}

// Saving a full GET queues its directed calculation without doing paid HTTP or
// moving the scan cursor. A later normal planner tick remains the repair path.
func (s *Service) queueCheckedCompatibility(ctx context.Context, owner int64, expected Session, target StoredProfile) error {
	if !s.Config.CompatibilityEnabled || !s.Config.SyncCycleEnabled {
		return nil
	}
	cycle, err := s.Store.SyncCycle(ctx, owner)
	if err != nil || cycle == nil {
		return err
	}
	if cycle.UserID != expected.UserID || cycle.Generation != expected.Generation || (cycle.Stage != "search" && cycle.Stage != "refresh" && cycle.Stage != "draining") {
		return nil
	}
	if cycle.Stage != "search" {
		var member bool
		if err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sync_refresh_items WHERE cycle_id=? AND profile_id=?)", cycle.ID, target.ID).Scan(&member); err != nil || !member {
			return err
		}
	}
	current, err := s.Store.Session(ctx, owner)
	if err != nil {
		return err
	}
	own, err := debugProfile(current.Account)
	frozen, known := own["freezeState"].(bool)
	if err != nil || !known || frozen || !compatibilityAdult(own) || current.UserID != expected.UserID || current.Generation != expected.Generation || current.State != "connected" || s.Store.Paused(ctx, owner) {
		return nil
	}
	if err = s.Store.InitCompatibility(ctx); err != nil {
		return err
	}
	// The cycle coordinator creates the scan row; do not race its frontier setup.
	var scan bool
	if err = s.Store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM compatibility_cycle_scans WHERE cycle_id=?)", cycle.ID).Scan(&scan); err != nil || !scan {
		return err
	}
	return s.planSyncCompatibilityTarget(ctx, *cycle, current, compatibilityProfileHash(own), target, s.Client.Now())
}

// This receipt controls polling only. Changes to the cached account projection
// bypass it, and the server account is refreshed at least once per five minutes.
func (s *Service) compatibilityOwnProfile(ctx context.Context, j CompatibilityJob, expected Session) (Profile, []byte, error) {
	key := "compatibility_own_checked:" + strconv.FormatInt(j.OwnerID, 10)
	var receipt struct {
		User       int64  `json:"user"`
		Generation string `json:"generation"`
		Observed   int64  `json:"observed"`
		Hash       string `json:"hash"`
	}
	now := s.Client.Now()
	current, err := s.Store.Session(ctx, j.OwnerID)
	if err != nil {
		return nil, nil, err
	}
	own, err := debugProfile(current.Account)
	if current.UserID != expected.UserID || current.Generation != expected.Generation || current.State != "connected" {
		return nil, nil, ErrReactionSessionChanged
	}
	if err == nil && compatibilityProfileHash(own) != j.OwnHash {
		return own, append([]byte(nil), current.Account...), nil
	}
	if err == nil && own.ID() == expected.UserID && json.Unmarshal([]byte(s.Store.Meta(ctx, key)), &receipt) == nil && receipt.User == expected.UserID && receipt.Generation == expected.Generation && receipt.Observed <= now.UnixMilli() && now.UnixMilli()-receipt.Observed < (5*time.Minute).Milliseconds() && receipt.Hash == compatibilityProfileHash(own) {
		return own, append([]byte(nil), current.Account...), nil
	}
	_, raw, observed, err := s.Client.debugReadObserved(ctx, j.OwnerID, expected, "/user/getMe?userId="+strconv.FormatInt(j.FromUserID, 10))
	if err != nil {
		return nil, nil, err
	}
	own, err = debugProfile(raw)
	if err != nil || own.ID() != j.FromUserID {
		return nil, nil, errors.New("Jardulik account identity or format changed")
	}
	mu := s.Store.SessionMutex(j.OwnerID)
	mu.Lock()
	defer mu.Unlock()
	if err = s.Client.debugBinding(ctx, j.OwnerID, expected); err == nil {
		err = s.Store.SaveAccount(ctx, j.OwnerID, own)
	}
	if err == nil {
		receipt.User, receipt.Generation, receipt.Observed, receipt.Hash = expected.UserID, expected.Generation, observed.UnixMilli(), compatibilityProfileHash(own)
		encoded, _ := json.Marshal(receipt)
		err = s.Store.SetMeta(ctx, key, string(encoded))
	}
	return own, raw, err
}

// Keep a failed/unknown paid attempt intact, but independently check whether its
// target has disappeared. Never turn this check into a second GPT POST.
func (s *Service) checkCompatibilityTargetAfterFailure(ctx context.Context, j CompatibilityJob, expected Session) {
	if ctx.Err() != nil || s.foregroundPending() {
		return
	}
	profile, _, observed, err := s.Client.checkedProfileObserved(ctx, j.OwnerID, expected, j.TargetUserID)
	if err != nil {
		return
	}
	mu := s.Store.SessionMutex(j.OwnerID)
	mu.Lock()
	_, _ = s.captureCheckedProfileLocked(ctx, j.OwnerID, expected, profile, observed)
	mu.Unlock()
}
