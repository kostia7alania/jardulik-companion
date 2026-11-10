package companion

import "context"

// A later source observation must not leave the frozen frontier waiting on an
// event it deliberately did not enroll, or publish over an unavailable status.
// Only definitely unsent notices are deferred; ambiguous/error sends still block.
func (s *Service) ignoreLateSyncNotices(ctx context.Context, cycle SyncCycle) error {
	_, err := s.Store.DB.ExecContext(ctx, `UPDATE sync_publication_items AS pi SET disposition='ignored',reason=CASE
 WHEN EXISTS(SELECT 1 FROM profile_presence pp WHERE pp.owner_id=? AND pp.profile_id=pi.profile_id AND pp.availability='unavailable') THEN 'late_source_unavailable' ELSE 'late_presence_pending' END
 WHERE pi.cycle_id=? AND pi.kind IN ('analysis','change') AND pi.disposition='required'
 AND (EXISTS(SELECT 1 FROM profile_presence pp WHERE pp.owner_id=? AND pp.profile_id=pi.profile_id AND pp.availability='unavailable')
 OR EXISTS(SELECT 1 FROM profile_presence_events pe WHERE pe.owner_id=? AND pe.profile_id=pi.profile_id AND pe.state IN ('pending','processing')
 AND NOT EXISTS(SELECT 1 FROM sync_publication_items included WHERE included.cycle_id=pi.cycle_id AND included.kind='presence' AND included.item_id=pe.id)))
 AND ((pi.kind='analysis' AND EXISTS(SELECT 1 FROM analysis_posts p WHERE p.analysis_id=pi.item_id AND p.owner_id=? AND p.upstream_id=? AND p.profile_id=pi.profile_id AND p.state='pending' AND p.message_id=0))
 OR (pi.kind='change' AND (NOT EXISTS(SELECT 1 FROM archive_change_posts p WHERE p.version_id=pi.item_id)
 OR EXISTS(SELECT 1 FROM archive_change_posts p WHERE p.version_id=pi.item_id AND p.owner_id=? AND p.profile_id=pi.profile_id AND p.state='pending' AND p.message_id=0))))`, cycle.OwnerID, cycle.ID, cycle.OwnerID, cycle.OwnerID, cycle.OwnerID, cycle.UserID, cycle.OwnerID)
	return err
}

func (s *Service) syncPublicationCaptionsComplete(ctx context.Context, cycle SyncCycle) (bool, error) {
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		return false, err
	}
	var pending bool
	err := s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM caption_maintenance_jobs j WHERE j.revision=? AND j.owner_id=? AND j.state<>'done'
 AND (j.kind='private'
 OR EXISTS(SELECT 1 FROM sync_publication_items pi WHERE pi.cycle_id=? AND pi.kind='version' AND pi.item_id=j.version_id)
 OR (j.kind='analysis' AND EXISTS(SELECT 1 FROM sync_publication_items pi WHERE pi.cycle_id=? AND pi.kind='analysis' AND pi.item_id=j.receipt_id))
 OR (j.kind='presence' AND EXISTS(SELECT 1 FROM sync_publication_items pi WHERE pi.cycle_id=? AND pi.kind='presence' AND pi.item_id=j.receipt_id))))`, captionDesignRevision, cycle.OwnerID, cycle.ID, cycle.ID, cycle.ID).Scan(&pending)
	return !pending, err
}
