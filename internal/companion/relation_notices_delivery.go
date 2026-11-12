package companion

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type relationNoticePost struct {
	ID, Profile, Observed, Thread int64
	Kind                          string
}

func (s *Service) relationNoticeDeliveryAllowed() bool {
	return s.Config.RelationNoticesEnabled && s.openingTimelineAllowed() &&
		forumTopicURL(s.Config.BackupChatID, s.Config.BackupThreadID) != "" && !s.Config.ArchivePaused && !s.Archiver.Config.ArchivePaused
}

// RunRelationNoticesDelivery publishes observations already stored by the source poller.
func (s *Service) RunRelationNoticesDelivery(ctx context.Context) {
	if !s.relationNoticeDeliveryAllowed() {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "relation-notices-delivery.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	recovered := false
	for ctx.Err() == nil {
		if !recovered {
			var exists bool
			exists, err = snapshotHasTable(ctx, s.Store.DB, "relation_notice_events")
			if err == nil && exists {
				err = s.recoverRelationNoticeDelivery(ctx)
				recovered = err == nil
			}
		}
		if recovered {
			_, err = s.deliverRelationNotice(ctx)
		}
		if err != nil && ctx.Err() == nil {
			_ = s.Store.SetMeta(ctx, "relation_notice_delivery_last_error", archiveErrorCode(err))
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

func (s *Service) recoverRelationNoticeDelivery(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, `UPDATE relation_notice_events
 SET state=CASE state WHEN 'sending' THEN 'uncertain' ELSE 'pending' END,
 error=CASE state WHEN 'sending' THEN 'worker_restarted_during_request' ELSE '' END
 WHERE owner_id=? AND chat_id=? AND message_id=0 AND state IN ('preparing','sending')`, s.Config.OwnerID, s.Config.BackupChatID)
	return err
}

func (s *Service) relationNoticeBinding(ctx context.Context, tx *sql.Tx, expected Session) error {
	if !s.relationNoticeDeliveryAllowed() {
		return errOpeningPostBinding
	}
	current, err := s.Store.businessSession(ctx, tx, s.Config.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return errOpeningPostBinding
	}
	if err != nil {
		return err
	}
	if current.State != "connected" || current.UserID != expected.UserID || current.Generation != expected.Generation {
		return errOpeningPostBinding
	}
	return nil
}

func (s *Service) claimRelationNotice(ctx context.Context, expected Session) (relationNoticePost, bool, error) {
	var post relationNoticePost
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return post, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='relation_notice_delivery'"); err != nil {
		return post, false, err
	}
	if err = s.relationNoticeBinding(ctx, tx, expected); err != nil {
		return post, false, err
	}

	err = tx.QueryRowContext(ctx, `WITH published AS (`+archiveChangePublishedSQL("?", "?", "?")+`)
 SELECT e.id,e.profile_id,e.kind,e.observed_at_ms,p.thread_id
 FROM relation_notice_events e JOIN published p ON p.owner_id=e.owner_id AND p.profile_id=e.profile_id
 WHERE e.owner_id=? AND e.upstream_id=? AND e.profile_id>0 AND e.profile_id<>e.upstream_id
 AND e.kind IN ('incoming','mutual') AND e.observed_at_ms>0
 AND e.state='pending' AND e.message_id=0 AND e.next_attempt_ms<=?
 AND e.chat_id IN (0,?) AND e.thread_id IN (0,p.thread_id)
 ORDER BY e.observed_at_ms,e.id,julianday(p.sent_at) DESC,p.version_id DESC LIMIT 1`,
		s.Config.BackupChatID, s.Config.OwnerID, s.Config.BackupThreadID, s.Config.OwnerID, expected.UserID,
		s.Archiver.now().UnixMilli(), s.Config.BackupChatID).Scan(&post.ID, &post.Profile, &post.Kind, &post.Observed, &post.Thread)
	if errors.Is(err, sql.ErrNoRows) {
		return post, false, nil
	}
	if err != nil {
		return post, false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE relation_notice_events
 SET state='preparing',chat_id=?,thread_id=?,error='' WHERE id=? AND state='pending'`, s.Config.BackupChatID, post.Thread, post.ID); err != nil {
		return post, false, err
	}
	err = tx.Commit()
	return post, err == nil, err
}

const relationNoticePostIdentity = `id=? AND owner_id=? AND upstream_id=? AND profile_id=? AND kind=? AND observed_at_ms=?
 AND chat_id=? AND thread_id=? AND message_id=0 AND state IN ('preparing','sending')`

func (s *Service) relationNoticeIdentity(post relationNoticePost, upstream int64) []any {
	return []any{post.ID, s.Config.OwnerID, upstream, post.Profile, post.Kind, post.Observed, s.Config.BackupChatID, post.Thread}
}

// The pacing wait precedes this check. Rebind the card link to the latest
// confirmed card only within the exact claimed profile thread.
func (s *Service) markRelationNoticeSending(ctx context.Context, post relationNoticePost, expected Session) (string, int64, error) {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='relation_notice_delivery'"); err != nil {
		return "", 0, err
	}
	if err = s.relationNoticeBinding(ctx, tx, expected); err != nil {
		return "", 0, err
	}
	var card, version int64
	err = tx.QueryRowContext(ctx, `SELECT p.version_id,json_extract(av.message_ids,'$[0]')
 FROM (`+archiveChangePublishedSQL("?", "?", "?")+`) p JOIN archive_versions av ON av.version_id=p.version_id
 WHERE p.profile_id=? AND p.thread_id=? ORDER BY julianday(p.sent_at) DESC,p.version_id DESC LIMIT 1`,
		s.Config.BackupChatID, s.Config.OwnerID, s.Config.BackupThreadID, post.Profile, post.Thread).Scan(&version, &card)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, errOpeningPostSource
	}
	if err != nil {
		return "", 0, err
	}
	url := profileIndexCardURL(s.Config.BackupChatID, post.Thread, card)
	if url == "" {
		return "", 0, errOpeningPostSource
	}
	result, err := tx.ExecContext(ctx, "UPDATE relation_notice_events SET state='sending' WHERE "+relationNoticePostIdentity, s.relationNoticeIdentity(post, expected.UserID)...)
	if err != nil {
		return "", 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return "", 0, err
	}
	if n != 1 {
		return "", 0, errOpeningPostSource
	}
	return url, version, tx.Commit()
}

func (s *Service) relationNoticePhoto(ctx context.Context, profile, version int64) string {
	jobs, err := s.Store.MediaJobs(ctx, s.Config.OwnerID, version)
	if err != nil {
		return ""
	}
	// MediaJobs orders original photos before previews and then by position.
	for _, job := range jobs {
		if job.ProfileID != profile || (job.Role != "photos" && job.Role != "miniPhotos") ||
			job.State != "complete" || job.Size <= 0 || job.Size > 9<<20 {
			continue
		}
		path, err := s.Archiver.mediaSourcePath(job.SHA)
		if err != nil {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != job.Size {
			continue
		}
		if hash, size, err := fileDigest(path); err == nil && size == job.Size && hash == job.SHA {
			return path
		}
	}
	return ""
}

func (s *Service) finishRelationNotice(ctx context.Context, post relationNoticePost, upstream, message int64, state string, next int64, detail string) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	args := append([]any{state, message, next, detail}, s.relationNoticeIdentity(post, upstream)...)
	result, err := s.Store.DB.ExecContext(write, `UPDATE relation_notice_events SET state=?,message_id=?,next_attempt_ms=?,error=? WHERE `+relationNoticePostIdentity, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("relation notice receipt not committed")
	}
	return err
}

func (s *Service) deliverRelationNotice(ctx context.Context) (bool, error) {
	if !s.relationNoticeDeliveryAllowed() {
		return false, nil
	}
	send, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	a := s.Archiver
	for !a.mu.TryLock() {
		if !sleep(send, 25*time.Millisecond) {
			return false, send.Err()
		}
	}
	defer a.mu.Unlock()
	unlock, locked, err := s.Store.trySyncPublicationLock()
	if err != nil || !locked {
		return false, err
	}
	defer unlock()

	expected, err := s.Store.Session(send, s.Config.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || expected.UserID <= 0 || expected.Generation == "" || expected.State != "connected" {
		return false, err
	}
	post, claimed, err := s.claimRelationNotice(send, expected)
	if !claimed || err != nil {
		return false, err
	}

	var last error
	var message, mediaVersion int64
	var photo string
	attempted := false
	err = a.groupCall(send, func() error {
		link, version, err := s.markRelationNoticeSending(send, post, expected)
		if err != nil {
			return err
		}
		header := "❤️ Вам поставили лайк"
		if post.Kind == "mutual" {
			header = "💘 У вас взаимная симпатия!"
		}
		observed := displayTime(time.UnixMilli(post.Observed).UTC().Format(time.RFC3339))
		prefix := header + "\n" + observed + "\n"
		const label = "Анкета"
		entities := []MessageEntity{
			{Type: "bold", Length: textUnits(header)},
			{Type: "text_link", Offset: textUnits(prefix), Length: textUnits(label), URL: link},
		}
		// A confirmed 429 can retry the same route. A POST failure never changes it to text.
		if !attempted {
			mediaVersion = version
			photo = s.relationNoticePhoto(send, post.Profile, version)
		} else if version != mediaVersion {
			return errOpeningPostSource
		}
		attempted = true
		var result SentMessage
		if photo != "" {
			result, err = a.TG.SendPhotoCard(send, s.Config.BackupChatID, post.Thread, photo, prefix+label, nil, entities)
		} else {
			result, err = a.TG.SendText(send, s.Config.BackupChatID, post.Thread, prefix+label, nil, entities)
		}
		if err == nil && (result.ID <= 0 || result.Chat.ID != s.Config.BackupChatID || result.ThreadID != post.Thread) {
			err = errOpeningPostReceipt
		}
		last = err
		if err == nil {
			message = result.ID
		}
		return err
	})
	if err != nil {
		state, detail, next := openingPostFailure(err, last, attempted, false, a.now())
		return true, errors.Join(err, s.finishRelationNotice(ctx, post, expected.UserID, 0, state, next, detail))
	}
	return true, s.finishRelationNotice(ctx, post, expected.UserID, message, "sent", 0, "")
}
