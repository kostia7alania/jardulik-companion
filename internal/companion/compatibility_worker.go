package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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
	filters, err := s.Autoliker.filters(ctx, job.OwnerID)
	if err != nil {
		return settle("cancelled", "filters_unavailable")
	}
	stored, err := s.Store.Profile(ctx, job.OwnerID, job.TargetUserID)
	if err != nil || !compatibilityMatchesFilters(stored.Profile, filters) {
		return settle("cancelled", "outside_current_filters")
	}
	if compatibilityProfileHash(stored.Profile) != job.TargetHash {
		return settle("cancelled", "inputs_changed_before_send")
	}

	_, ownRaw, err := s.Client.debugRead(ctx, job.OwnerID, session, "/user/getMe?userId="+strconv.FormatInt(job.FromUserID, 10))
	if err != nil {
		return settle("queued", "preflight_unavailable")
	}
	own, decodeErr := debugProfile(ownRaw)
	ownAge, knownAge := autoInteger(own["age"])
	frozen, knownFrozen := own["freezeState"].(bool)
	if decodeErr != nil || own.ID() != job.FromUserID || !knownAge || !knownFrozen {
		return settle("cancelled", "own_profile_unavailable")
	}
	sessionMu := s.Store.SessionMutex(job.OwnerID)
	sessionMu.Lock()
	err = s.Client.debugBinding(ctx, job.OwnerID, session)
	if err == nil {
		err = s.Store.SaveAccount(ctx, job.OwnerID, own)
	}
	sessionMu.Unlock()
	if err != nil {
		return settle("queued", "preflight_unavailable")
	}
	if ownAge < 18 || ownAge > 111 || frozen {
		return settle("cancelled", "own_profile_unavailable")
	}
	if s.foregroundPending() {
		return settle("queued", "foreground_busy")
	}
	_, targetRaw, err := s.Client.debugRead(ctx, job.OwnerID, session, "/user/getUser?viewerUserId="+strconv.FormatInt(job.FromUserID, 10)+"&targetUserId="+strconv.FormatInt(job.TargetUserID, 10))
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) && upstream.Status == http.StatusNotFound {
			return settle("cancelled", "target_unavailable")
		}
		return settle("queued", "preflight_unavailable")
	}
	target, decodeErr := debugProfile(targetRaw)
	if decodeErr != nil || target.ID() != job.TargetUserID || !compatibilityMatchesFilters(target, filters) {
		return settle("cancelled", "outside_current_filters")
	}
	// Refresh the cache used by the planner before replacing a superseded input job.
	sessionMu.Lock()
	err = s.Client.debugBinding(ctx, job.OwnerID, session)
	if err == nil {
		_, err = s.Store.ArchiveViewed(ctx, job.OwnerID, []Profile{target}, "GET /user/getUser")
	}
	sessionMu.Unlock()
	if err != nil {
		return settle("queued", "preflight_unavailable")
	}
	if compatibilityProfileHash(own) != job.OwnHash || compatibilityProfileHash(target) != job.TargetHash {
		return settle("cancelled", "inputs_changed_before_send")
	}

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
		currentFilters, err := s.Autoliker.filters(guardCtx, job.OwnerID)
		if err != nil || !compatibilityMatchesFilters(target, currentFilters) {
			return ErrCompatibilityUnavailable
		}
		observed = s.Client.Now()
		gap := s.Config.CompatibilityGap
		if gap <= 0 {
			gap = time.Minute
		}
		if err = s.Store.StartCompatibilityJob(guardCtx, *job, ownRaw, targetRaw, request, observed, gap); err != nil {
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
	return true, saveErr
}
