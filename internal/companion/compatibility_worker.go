package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Planning runs in its own process. Only the bot/parser owns source HTTP sessions.
func (s *Service) runCompatibilityExecutor(ctx context.Context) {
	for ctx.Err() == nil {
		if err := s.Store.RecoverCompatibilityJobs(ctx, s.Client.Now()); err == nil {
			break
		} else if ctx.Err() == nil {
			slog.Warn("compatibility recovery deferred", "kind", safeError(err))
		}
		if !sleep(ctx, 5*time.Second) {
			return
		}
	}
	for ctx.Err() == nil {
		if _, err := s.compatibilityStep(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("compatibility step failed", "kind", safeError(err))
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

func (s *Service) compatibilityStep(ctx context.Context) (bool, error) {
	if !s.Config.CompatibilityEnabled || s.foregroundPending() {
		return false, nil
	}
	free, err := FreeBytes(s.Config.DataDir)
	if err != nil || free < s.Config.MinFreeBytes {
		return false, err
	}
	job, err := s.Store.ClaimCompatibilityJob(ctx, s.Client.Now())
	if err != nil || job == nil {
		return false, err
	}
	settle := func(state, reason string) (bool, error) {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if state == "queued" {
			return false, s.Store.ReleaseCompatibilityJob(writeCtx, *job, s.Client.Now(), reason)
		}
		return false, s.Store.FinishCompatibilityJob(writeCtx, *job, state, 0, nil, 0, reason, s.Client.Now())
	}
	mu := s.userLock(job.OwnerID)
	if !mu.TryLock() {
		return settle("queued", "foreground_busy")
	}
	defer mu.Unlock()
	if s.foregroundPending() {
		return settle("queued", "foreground_busy")
	}
	session, err := s.Store.Session(ctx, job.OwnerID)
	if err != nil || session.UserID != job.FromUserID || session.Generation != job.Generation || session.State != "connected" || s.Store.Paused(ctx, job.OwnerID) {
		return settle("cancelled", "session_changed")
	}
	stored, err := s.Store.Profile(ctx, job.OwnerID, job.TargetUserID)
	missing, missingErr := profileKnownUnavailable(ctx, s.Store.DB, job.OwnerID, job.TargetUserID)
	if missingErr != nil {
		return settle("queued", "preflight_unavailable")
	}
	if missing {
		return settle("cancelled", "target_unavailable")
	}
	eligible, eligibilityErr := s.compatibilityEligible(ctx, *job, stored.Profile)
	if err != nil || eligibilityErr != nil || !eligible {
		return settle("cancelled", "outside_current_filters")
	}
	if compatibilityProfileHash(stored.Profile) != job.TargetHash {
		return settle("cancelled", "inputs_changed_before_send")
	}

	own, ownRaw, err := s.compatibilityOwnProfile(ctx, *job, session)
	if err != nil {
		return settle("queued", "preflight_unavailable")
	}
	ownAge, knownAge := autoInteger(own["age"])
	frozen, knownFrozen := own["freezeState"].(bool)
	if own.ID() != job.FromUserID || !knownAge || !knownFrozen {
		return settle("cancelled", "own_profile_unavailable")
	}
	if ownAge < 18 || ownAge > 111 || frozen {
		return settle("cancelled", "own_profile_unavailable")
	}
	if compatibilityProfileHash(own) != job.OwnHash {
		return settle("cancelled", "inputs_changed_before_send")
	}
	if s.foregroundPending() {
		return settle("queued", "foreground_busy")
	}
	target, targetRaw := stored.Profile, []byte(stored.RawJSON)
	fresh, err := s.cachedCheckedProfile(ctx, job.OwnerID, session, stored, s.Client.Now())
	if err != nil {
		return settle("queued", "preflight_unavailable")
	}
	if !fresh {
		var targetObserved time.Time
		target, targetRaw, targetObserved, err = s.Client.checkedProfileObserved(ctx, job.OwnerID, session, job.TargetUserID)
		if err != nil {
			if errors.Is(err, ErrProfileUnavailable) {
				return settle("cancelled", "target_unavailable")
			}
			return settle("queued", "preflight_unavailable")
		}
		sessionMu := s.Store.SessionMutex(job.OwnerID)
		sessionMu.Lock()
		stored, err = s.captureCheckedProfileLocked(ctx, job.OwnerID, session, target, targetObserved)
		if err == nil {
			err = s.queueCheckedCompatibility(ctx, job.OwnerID, session, stored)
		}
		sessionMu.Unlock()
		if err != nil {
			return settle("queued", "preflight_unavailable")
		}
	}
	eligible, eligibilityErr = s.compatibilityEligible(ctx, *job, target)
	if target.ID() != job.TargetUserID || eligibilityErr != nil || !eligible {
		return settle("cancelled", "outside_current_filters")
	}
	if compatibilityProfileHash(own) != job.OwnHash || compatibilityProfileHash(target) != job.TargetHash {
		return settle("cancelled", "inputs_changed_before_send")
	}
	job.VersionID = stored.VersionID

	request, err := json.Marshal(map[string]int64{"fromUserId": job.FromUserID, "targetUserId": job.TargetUserID})
	if err != nil {
		return settle("cancelled", "request_unavailable")
	}
	started := false
	observed := s.Client.Now()
	status, response, postErr := s.Client.debugPostGuarded(ctx, job.OwnerID, session, "/GPT/checkCompatibility", request, func(guardCtx context.Context) error {
		if s.foregroundPending() {
			return ErrCompatibilityBusy
		}
		eligible, eligibilityErr := s.compatibilityEligible(guardCtx, *job, target)
		if eligibilityErr != nil {
			return eligibilityErr
		}
		if !eligible {
			return ErrCompatibilityUnavailable
		}
		observed = s.Client.Now()
		gap := s.Config.CompatibilityGap
		if gap <= 0 {
			gap = time.Minute
		}
		if err := s.Store.StartCompatibilityJob(guardCtx, *job, ownRaw, targetRaw, request, observed, gap); err != nil {
			return err
		}
		started = true
		return nil
	})
	if !started {
		if errors.Is(postErr, ErrCompatibilityCached) {
			return false, nil
		}
		if errors.Is(postErr, ErrCompatibilityBusy) {
			return settle("queued", "foreground_busy")
		}
		if errors.Is(postErr, ErrCompatibilityInputChanged) || errors.Is(postErr, ErrCompatibilityUnavailable) || errors.Is(postErr, ErrReactionSessionChanged) {
			return settle("cancelled", "session_or_inputs_changed")
		}
		return settle("queued", "preflight_unavailable")
	}

	state, reason := "succeeded", ""
	if postErr != nil {
		state, reason = "unknown", "transport_unconfirmed"
		var upstream *UpstreamError
		if errors.As(postErr, &upstream) && upstream.Status >= 400 && upstream.Status < 500 && upstream.Status != http.StatusUnauthorized {
			state, reason = "rejected", upstream.Kind
		}
	} else if !compatibilityGPTSuccess(response) {
		state, reason = "rejected", "unexpected_gpt_response"
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	input := AnalysisCaptureInput{UpstreamID: job.FromUserID, ProfileID: job.TargetUserID, VersionID: job.VersionID, Method: http.MethodPost, Path: "/GPT/checkCompatibility", ObservedAtMillis: observed.UnixMilli(), HTTPStatus: status, State: "response_received", Generation: job.Generation, RequestBody: request, ResponseBody: response, OwnProfile: ownRaw, TargetProfile: targetRaw}
	if status == 0 {
		input.State = "transport_unknown"
	}
	analysis, saveErr := s.Store.SaveAnalysisCapture(writeCtx, job.OwnerID, input)
	cancel()
	if saveErr != nil {
		state, reason = "unknown", "analysis_save_unconfirmed"
	}
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelFinish()
	if err = s.Store.FinishCompatibilityJob(finishCtx, *job, state, status, response, analysis.ID, reason, s.Client.Now()); err != nil {
		return true, fmt.Errorf("compatibility result not committed: %w", err)
	}
	if state != "succeeded" && status != http.StatusUnauthorized && status != http.StatusForbidden && status != http.StatusTooManyRequests {
		s.checkCompatibilityTargetAfterFailure(ctx, *job, session)
	}
	return true, saveErr
}
