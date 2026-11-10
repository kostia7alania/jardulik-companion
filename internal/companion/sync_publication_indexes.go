package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// Freeze the final index pages only after the profile/notices frontier is confirmed.
func (s *Service) syncPublicationIndexesComplete(ctx context.Context, cycle SyncCycle, frozen bool) (bool, error) {
	if !frozen {
		if err := s.initProfileIndexes(ctx); err != nil {
			return false, err
		}
		ss, err := s.Store.Session(ctx, cycle.OwnerID)
		if err != nil {
			return false, err
		}
		if ss.UserID != cycle.UserID || ss.Generation != cycle.Generation {
			return false, nil
		}
		if err = s.seedProfileIndexTopics(ctx, ss); err != nil {
			return false, err
		}
		if _, err = s.Store.DB.ExecContext(ctx, "UPDATE profile_index_topics SET next_refresh=0 WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=?", cycle.OwnerID, cycle.UserID, cycle.Generation, s.Config.BackupChatID); err != nil {
			return false, err
		}
		if err = s.refreshProfileIndexPlan(ctx, ss); err != nil {
			return false, err
		}
		tx, err := s.Store.DB.BeginTx(ctx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		if err = syncCycleBindingTx(ctx, tx, s.Store, cycle); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO sync_publication_indexes(cycle_id,topic_id,page_no,payload_sha)
 SELECT ?,p.topic_id,p.page_no,p.desired_sha FROM profile_index_pages p JOIN profile_index_topics t ON t.id=p.topic_id
 WHERE t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=?
 AND NOT(p.message_id=0 AND p.state='error' AND p.error='unpublished_page_superseded')`, cycle.ID, cycle.OwnerID, cycle.UserID, cycle.Generation, s.Config.BackupChatID); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE sync_publication_manifests SET indexes_frozen=1 WHERE cycle_id=?", cycle.ID); err != nil {
			return false, err
		}
		if err = tx.Commit(); err != nil {
			return false, err
		}
	}
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT p.state,p.message_id,p.desired_sha,p.sent_sha,p.receipt_json,t.state,t.thread_id,t.receipt_json,i.payload_sha
 FROM sync_publication_indexes i LEFT JOIN profile_index_pages p ON p.topic_id=i.topic_id AND p.page_no=i.page_no
 LEFT JOIN profile_index_topics t ON t.id=i.topic_id AND t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=?
 WHERE i.cycle_id=?`, cycle.OwnerID, cycle.UserID, cycle.Generation, s.Config.BackupChatID, cycle.ID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var state, desired, sent, topicState sql.NullString
		var message, thread sql.NullInt64
		var receipt, topicReceipt []byte
		var expected string
		if err = rows.Scan(&state, &message, &desired, &sent, &receipt, &topicState, &thread, &topicReceipt, &expected); err != nil {
			return false, err
		}
		if state.String != "sent" || topicState.String != "sent" || message.Int64 <= 0 || thread.Int64 <= 0 || thread.Int64 == s.Config.BackupThreadID || desired.String != expected || sent.String != expected {
			return false, nil
		}
		var topic struct {
			Thread int64 `json:"message_thread_id"`
		}
		if json.Unmarshal(topicReceipt, &topic) != nil || topic.Thread != thread.Int64 {
			return false, nil
		}
		var ack SentMessage
		if json.Unmarshal(receipt, &ack) != nil {
			return false, nil
		}
		if ack.ID != message.Int64 || ack.Chat.ID != s.Config.BackupChatID || ack.ThreadID != thread.Int64 {
			var unchanged struct {
				NotModified bool  `json:"not_modified"`
				Message     int64 `json:"message_id"`
				Code        int   `json:"code"`
			}
			if json.Unmarshal(receipt, &unchanged) != nil || !unchanged.NotModified || unchanged.Message != message.Int64 || unchanged.Code != 400 {
				return false, nil
			}
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if count == 0 {
		return false, errors.New("frozen index manifest has no pages")
	}
	rows.Close()
	return s.catalogueTitleConfirmed(ctx, cycle.OwnerID, cycle.UserID, cycle.Generation)
}
