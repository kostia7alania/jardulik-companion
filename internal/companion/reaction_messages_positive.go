package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type PositiveReactionType int64

const (
	PositiveLike      PositiveReactionType = 1
	PositiveSuperLike PositiveReactionType = 3
)

var ErrReactionFireUnavailable = errors.New("в Jardulik закончились реакции Огонь")
var ErrReactionFireUnknown = errors.New("не удалось подтвердить остаток реакций Огонь")
var ErrPositiveSpacing = errors.New("между отправками нужно подождать 60 секунд")

type PositiveSpacingError struct{ RetryAt time.Time }

func (e *PositiveSpacingError) Error() string { return ErrPositiveSpacing.Error() }
func (e *PositiveSpacingError) Unwrap() error { return ErrPositiveSpacing }

type positiveAuditContextKey struct{}
type positiveSpacingContextKey struct{}
type automaticDispatchContextKey struct{}
type automaticDispatchBinding struct {
	Owner                         int64
	RunID, Generation, FilterHash string
	Upstream                      int64
}

func claimedPositiveAuditID(ctx context.Context) int64 {
	n, _ := ctx.Value(positiveAuditContextKey{}).(int64)
	return n
}

func validSuperBeforeAge(cutoff int) bool { return cutoff == 0 || cutoff >= 18 && cutoff <= 112 }

func positiveReactionForAge(age int64, cutoff int) PositiveReactionType {
	if validSuperBeforeAge(cutoff) && cutoff != 0 && age >= 18 && age < int64(cutoff) {
		return PositiveSuperLike
	}
	return PositiveLike
}

func (s *Store) checkDebugPositive(ctx context.Context, q reactionQuerier, owner, target int64) error {
	var state string
	err := q.QueryRowContext(ctx, `SELECT d.state FROM debug_api_calls d JOIN sessions s ON s.owner_id=d.owner_id AND s.upstream_id=d.upstream_id WHERE d.owner_id=? AND d.target_id=? AND d.path='/reactions/addNew' AND d.reaction_type=3 AND d.state IN ('sending','received','unknown') AND d.id<>? ORDER BY d.id DESC LIMIT 1`, owner, target, claimedPositiveAuditID(ctx)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "received" {
		state = "succeeded"
	}
	return &ActionBlockedError{State: state}
}

type positiveBudget struct {
	Count       int
	RetryAt     time.Time
	Latest      time.Time
	LatestSuper time.Time
}

// Debug type1 is already in actions. New debug type3 is linked atomically to its
// action before POST; only legacy/unlinked type3 contributes another reservation.
func (s *Store) positiveBudget(ctx context.Context, owner int64, now time.Time) (positiveBudget, error) {
	return s.positiveBudgetRows(ctx, s.DB, owner, now)
}
func (s *Store) positiveBudgetRows(ctx context.Context, q reactionQuerier, owner int64, now time.Time) (positiveBudget, error) {
	var result positiveBudget
	var count sql.NullInt64
	var oldest, latest, super sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT sum(cap),min(CASE WHEN cap=1 THEN at END),max(spacing_at),max(CASE WHEN super=1 THEN spacing_at END) FROM (
 SELECT CASE WHEN a.state IN ('pending','processing','succeeded','unknown') THEN 1 ELSE 0 END AS cap,CAST(strftime('%s',a.created_at) AS INTEGER) AS at,
 CASE WHEN i.attempted_at_ms>0 THEN i.attempted_at_ms ELSE CAST(strftime('%s',a.created_at) AS INTEGER)*1000+1000 END AS spacing_at,
 CASE WHEN i.reaction_type=3 THEN 1 ELSE 0 END AS super
 FROM actions a LEFT JOIN reaction_message_intents i ON i.action_id=a.id AND i.owner_id=a.owner_id AND i.profile_id=a.profile_id
 WHERE a.owner_id=? AND a.kind='Like' AND a.state IN ('pending','processing','succeeded','unknown','rejected','quota_waiting','fire_waiting') AND datetime(a.created_at)>datetime(?,'-24 hours') AND a.id<>?
 UNION ALL SELECT CASE WHEN d.state IN ('sending','received','unknown') THEN 1 ELSE 0 END,CAST(strftime('%s',d.created_at) AS INTEGER),CAST(strftime('%s',d.updated_at) AS INTEGER)*1000+1000,1
 FROM debug_api_calls d JOIN sessions s ON s.owner_id=d.owner_id AND s.upstream_id=d.upstream_id
 WHERE d.owner_id=? AND d.path='/reactions/addNew' AND d.reaction_type=3 AND d.state IN ('sending','received','unknown','rejected') AND d.action_id IS NULL AND datetime(d.created_at)>datetime(?,'-24 hours') AND d.id<>?
 )`, owner, now.UTC().Format("2006-01-02 15:04:05"), ClaimedLikeActionID(ctx), owner, now.UTC().Format("2006-01-02 15:04:05"), claimedPositiveAuditID(ctx)).Scan(&count, &oldest, &latest, &super)
	result.Count = int(count.Int64)
	if oldest.Valid {
		result.RetryAt = time.Unix(oldest.Int64, 0).Add(24*time.Hour + time.Second)
	}
	if latest.Valid {
		result.Latest = time.UnixMilli(latest.Int64)
	}
	if super.Valid {
		result.LatestSuper = time.UnixMilli(super.Int64)
	}
	return result, err
}

// Existing ordinary-only accounts retain their interval. The age strategy and
// manual superlikes enforce 60 seconds; a preceding superlike also holds the gap.
func (s *Store) checkPositiveBudget(ctx context.Context, owner int64, now time.Time, forceSpacing bool) error {
	policy, err := s.ReactionMessagePolicy(ctx, owner)
	if err != nil {
		return err
	}
	return s.checkPositiveBudgetRows(ctx, s.DB, policy, owner, now, forceSpacing)
}
func (s *Store) checkPositiveBudgetRows(ctx context.Context, q reactionQuerier, policy ReactionMessagePolicy, owner int64, now time.Time, forceSpacing bool) error {
	budget, err := s.positiveBudgetRows(ctx, q, owner, now)
	if err != nil {
		return err
	}
	cap := 100
	var exists bool
	if err = q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='autolike_settings')").Scan(&exists); err != nil {
		return err
	}
	if exists {
		err = q.QueryRowContext(ctx, "SELECT daily_cap FROM autolike_settings WHERE owner_id=?", owner).Scan(&cap)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	forced, _ := ctx.Value(positiveSpacingContextKey{}).(bool)
	if (forceSpacing || forced || policy.SuperBeforeAge != 0) && cap > 100 {
		cap = 100
	}
	if cap < 1 || cap > 500 {
		return errors.New("invalid positive reaction cap")
	}
	if budget.Count >= cap {
		return &OwnLikeLimitError{Limit: cap, RetryAt: budget.RetryAt}
	}
	// Legacy second precision is padded in positiveBudget; current attempts use milliseconds.
	latest := budget.LatestSuper
	if forceSpacing || forced || policy.SuperBeforeAge != 0 {
		latest = budget.Latest
	}
	if !latest.IsZero() && now.Before(latest.Add(time.Minute)) {
		return &PositiveSpacingError{RetryAt: latest.Add(time.Minute)}
	}
	return nil
}

// Only the automatic type changes; message source text and its cursor stay intact.
func (s *Store) SetSuperBeforeAge(ctx context.Context, owner int64, cutoff int) error {
	if !validSuperBeforeAge(cutoff) {
		return errors.New("правило суперлайков: 0 или возраст от 18 до 112")
	}
	if err := s.initReactionMessages(ctx); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return err
	}
	p, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return err
	}
	if cutoff != 0 && p.Mode == "off" {
		return errors.New("сначала настрой сообщения для суперлайков")
	}
	if p.SuperBeforeAge == cutoff {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, "UPDATE reaction_message_settings SET super_before_age=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE owner_id=?", cutoff, owner); err != nil {
		return err
	}
	return tx.Commit()
}

// Reserve the writer before reading the clock, then persist the actual dispatch
// boundary after all preflight guards. No POST has started when this fails.
func (s *Store) markPositiveAttempt(ctx context.Context, owner, action int64, expected Session, automatic bool, clock func() time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return err
	}
	ss, err := s.businessSession(ctx, tx, owner)
	if err != nil || ss.State != "connected" || ss.UserID != expected.UserID || ss.Generation != expected.Generation {
		return ErrReactionSessionChanged
	}
	var paused bool
	if err = tx.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&paused); err != nil {
		return err
	}
	if paused {
		return ErrReactionPaused
	}
	if !s.cfg.AllowReactions {
		return ErrReactionDisabled
	}
	dispatchCtx := context.WithValue(ctx, claimedLikeContextKey{}, action)
	var target int64
	if err = tx.QueryRowContext(ctx, "SELECT profile_id FROM reaction_message_intents WHERE action_id=? AND owner_id=?", action, owner).Scan(&target); err != nil {
		return err
	}
	if err = s.checkDebugPositive(dispatchCtx, tx, owner, target); err != nil {
		return err
	}
	var targetRaw string
	if err = tx.QueryRowContext(ctx, `SELECT v.raw_json FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?`, owner, target).Scan(&targetRaw); err != nil {
		return err
	}
	profile, err := decodeProfile([]byte(targetRaw))
	if err != nil || profile.ID() != target {
		return ErrActionBlocked
	}
	age, knownAge := autoInteger(profile["age"])
	if !knownAge || age < 18 || age > 111 || birthdayGreetingMinorDOB(profile, time.Now().UTC()) {
		return ErrReactionMessageTargetAge
	}
	var kind PositiveReactionType
	if err = tx.QueryRowContext(ctx, "SELECT reaction_type FROM reaction_message_intents WHERE action_id=? AND owner_id=?", action, owner).Scan(&kind); err != nil {
		return err
	}
	if kind == PositiveSuperLike {
		var raw string
		if err = tx.QueryRowContext(ctx, "SELECT account_json FROM sessions WHERE owner_id=?", owner).Scan(&raw); err != nil {
			return err
		}
		own, e := decodeProfile([]byte(raw))
		if e != nil || own.ID() != expected.UserID {
			return ErrReactionSessionChanged
		}
		fire, known := autoInteger(own["fire"])
		if !known || fire < 0 {
			return ErrReactionFireUnknown
		}
		if fire == 0 {
			return ErrReactionFireUnavailable
		}
	}

	policy, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return err
	}
	if automatic && kind != positiveReactionForAge(age, policy.SuperBeforeAge) {
		return errAutoChanged
	}
	if err = reactionDraftHoldTx(ctx, tx, owner, expected.UserID, target); err != nil {
		return err
	}
	if err = s.checkPositiveBudgetRows(dispatchCtx, tx, policy, owner, clock(), false); err != nil {
		return err
	}
	var filters FeedFilters
	if automatic {
		filters, err = s.currentPositiveFiltersTx(ctx, tx, owner, expected.UserID)
		if err != nil {
			return err
		}
		if age < filters.AgeMin || age > filters.AgeMax {
			return errAutoChanged
		}
	}
	if binding, ok := ctx.Value(automaticDispatchContextKey{}).(automaticDispatchBinding); automatic && ok {
		var mode, run, generation, filterHash string
		var upstream int64
		if err = tx.QueryRowContext(ctx, "SELECT mode,run_id,generation,upstream_id,filter_hash FROM autolike_settings WHERE owner_id=?", owner).Scan(&mode, &run, &generation, &upstream, &filterHash); err != nil {
			return err
		}
		if owner != binding.Owner || mode == "off" || run != binding.RunID || generation != binding.Generation || upstream != binding.Upstream || filterHash != binding.FilterHash {
			return errAutoChanged
		}
		if autoFilterHash(filters) != binding.FilterHash {
			return errAutoChanged
		}
	}
	if automatic {
		var revision, current int64
		if err = tx.QueryRowContext(ctx, `SELECT i.policy_revision,COALESCE(p.revision,0) FROM reaction_message_intents i LEFT JOIN reaction_message_settings p ON p.owner_id=i.owner_id WHERE i.action_id=? AND i.owner_id=?`, action, owner).Scan(&revision, &current); err != nil {
			return err
		}
		if revision != current {
			return errors.New("правило отправки изменилось; действие отменено")
		}
	}
	r, err := tx.ExecContext(ctx, `UPDATE reaction_message_intents SET attempted_at_ms=? WHERE action_id=? AND owner_id=? AND upstream_id=? AND generation=? AND attempted_at_ms=0 AND EXISTS(SELECT 1 FROM actions a WHERE a.id=action_id AND a.owner_id=reaction_message_intents.owner_id AND a.profile_id=reaction_message_intents.profile_id AND a.state='pending')`, clock().UnixMilli(), action, owner, expected.UserID, expected.Generation)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrActionBlocked
	}
	return tx.Commit()
}

func (s *Store) currentPositiveFiltersTx(ctx context.Context, tx *sql.Tx, owner, upstream int64) (FeedFilters, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, "SELECT account_json FROM sessions WHERE owner_id=?", owner).Scan(&raw); err != nil {
		return FeedFilters{}, err
	}
	own, err := decodeProfile([]byte(raw))
	if err != nil || own.ID() != upstream {
		return FeedFilters{}, ErrReactionSessionChanged
	}
	filters := existingFeedFilters(own)
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_filters')").Scan(&exists); err != nil {
		return filters, err
	}
	if exists {
		var applied sql.NullString
		err = tx.QueryRowContext(ctx, "SELECT applied_json FROM feed_filters WHERE owner_id=?", owner).Scan(&applied)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return filters, err
		}
		if applied.Valid && json.Unmarshal([]byte(applied.String), &filters) != nil {
			return filters, errors.New("invalid current reaction filters")
		}
	}
	return filters, filters.validate()
}
