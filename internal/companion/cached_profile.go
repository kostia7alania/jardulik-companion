package companion

import (
	"context"
	"errors"
)

// Only callers that archived a successful full-profile response use these exact tags.
func (s *Store) ProfileHasFullCapture(ctx context.Context, owner, profile, version int64) (bool, error) {
	if owner <= 0 || profile <= 0 || version <= 0 {
		return false, errors.New("invalid full profile cache identity")
	}
	var full bool
	err := s.DB.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM observations o JOIN versions v ON v.id=o.version_id AND v.owner_id=o.owner_id
 WHERE o.owner_id=? AND v.profile_id=? AND v.id=? AND o.source IN (
 'GET /user/getUser','GET /user/getUser (crawl catalog)','GET /user/getUser (autolike)',
 'profile_watch','incoming_viewed','mutual_viewed'))`, owner, profile, version).Scan(&full)
	return full, err
}
