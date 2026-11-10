package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type profileIndexTopic struct {
	ID, Owner, Upstream, Chat, Thread int64
	Generation, Kind, State           string
	NextRefresh                       int64
}
type profileIndexOperation struct {
	Topic   profileIndexTopic
	Page    int
	Message int64
	Payload profileIndexPayload
	Raw     []byte
	SHA     string
}

var errProfileIndexBinding = errors.New("profile index source or receipt changed")
var errProfileIndexReceipt = errors.New("profile index Telegram receipt unconfirmed")

func (s *Service) profileIndexesAllowed() bool {
	return s.Archiver != nil && s.Archiver.TG != nil && s.Config.OwnerID > 0 && s.Config.OwnerID == s.Archiver.Config.OwnerID && s.Config.BackupChatID != 0 && s.Config.BackupChatID == s.Archiver.Config.BackupChatID && s.Config.BackupThreadID == s.Archiver.Config.BackupThreadID && !s.Config.ArchivePaused && !s.Archiver.Config.ArchivePaused
}

// RunProfileIndexes only publishes the owner's saved archive. It never calls a
// source service, computes compatibility, deletes posts or changes user filters.
func (s *Service) RunProfileIndexes(ctx context.Context) {
	if !s.profileIndexesAllowed() {
		return
	}
	lock, err := os.OpenFile(filepath.Join(s.Config.DataDir, "profile-indexes.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = s.initProfileIndexes(ctx); err != nil {
		return
	}
	if err = s.recoverProfileIndexes(ctx); err != nil {
		return
	}
	for ctx.Err() == nil {
		_, err = s.deliverProfileIndex(ctx)
		delay := 2 * time.Second
		if err != nil {
			_ = s.Store.SetMeta(ctx, "profile_indexes_last_error", archiveErrorCode(err))
			delay = 5 * time.Second
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}

func (s *Service) recoverProfileIndexes(ctx context.Context) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_indexes'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE profile_index_topics SET state=CASE state WHEN 'sending' THEN 'uncertain' ELSE 'pending' END,
 error=CASE state WHEN 'sending' THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE owner_id=? AND state IN ('preparing','sending')`, s.Config.OwnerID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE profile_index_pages SET state=CASE WHEN state='sending' AND message_id=0 THEN 'uncertain' ELSE 'pending' END,
 error=CASE WHEN state='sending' AND message_id=0 THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE topic_id IN (SELECT id FROM profile_index_topics WHERE owner_id=?) AND state IN ('preparing','sending')`, s.Config.OwnerID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) profileIndexBindingTx(ctx context.Context, tx *sql.Tx, expected Session) error {
	if !s.profileIndexesAllowed() {
		return errProfileIndexBinding
	}
	current, err := s.Store.businessSession(ctx, tx, s.Config.OwnerID)
	if err != nil {
		return err
	}
	if current.UserID != expected.UserID || current.Generation != expected.Generation || expected.UserID <= 0 || expected.Generation == "" {
		return errProfileIndexBinding
	}
	return nil
}

func (s *Service) seedProfileIndexTopics(ctx context.Context, expected Session) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_indexes'"); err != nil {
		return err
	}
	if err = s.profileIndexBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO profile_index_topics(owner_id,upstream_id,generation,kind,chat_id) VALUES(?,?,?,?,?)
 ON CONFLICT(owner_id,upstream_id,kind,chat_id) DO UPDATE SET generation=excluded.generation,next_refresh=0,updated_at=CURRENT_TIMESTAMP
	WHERE profile_index_topics.generation<>excluded.generation`, s.Config.OwnerID, expected.UserID, expected.Generation, profileIndexCompatibility, s.Config.BackupChatID); err != nil {
		return err
	}
	// Existing Age topics remain an archive; new accounts need only the Catalog.
	if _, err = tx.ExecContext(ctx, `UPDATE profile_index_topics SET generation=?,next_refresh=0,updated_at=CURRENT_TIMESTAMP
 WHERE owner_id=? AND upstream_id=? AND kind=? AND chat_id=? AND generation<>?`, expected.Generation, s.Config.OwnerID, expected.UserID, profileIndexAge, s.Config.BackupChatID, expected.Generation); err != nil {
		return err
	}
	return tx.Commit()
}

func scanProfileIndexTopic(row interface{ Scan(...any) error }) (profileIndexTopic, error) {
	var t profileIndexTopic
	err := row.Scan(&t.ID, &t.Owner, &t.Upstream, &t.Generation, &t.Kind, &t.Chat, &t.Thread, &t.State, &t.NextRefresh)
	return t, err
}

const profileIndexTopicColumns = "id,owner_id,upstream_id,generation,kind,chat_id,thread_id,state,next_refresh"

func (s *Service) refreshProfileIndexPlan(ctx context.Context, expected Session) error {
	if s.Store.cfg.SyncCycleEnabled {
		var frozen bool
		if err := s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_publication_manifests pm JOIN sync_cycles sc ON sc.id=pm.cycle_id WHERE sc.owner_id=? AND sc.upstream_id=? AND sc.generation=? AND sc.stage='publishing' AND pm.indexes_frozen=1)`, s.Config.OwnerID, expected.UserID, expected.Generation).Scan(&frozen); err != nil {
			return err
		}
		if frozen {
			return nil
		}
	}
	var due bool
	now := s.Archiver.now()
	if err := s.Store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM profile_index_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND next_refresh<=?)`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, now.UnixMilli()).Scan(&due); err != nil {
		return err
	}
	if !due {
		return nil
	}
	entries, err := s.profileIndexEntries(ctx, s.Config.OwnerID, expected.UserID, now)
	if err != nil {
		return err
	}
	plans := map[string][]profileIndexPayload{}
	for _, kind := range []string{profileIndexCompatibility, profileIndexAge} {
		plans[kind], err = renderProfileIndexPagesMode(kind, entries, s.Config.BackupChatID, s.Config.RichCards)
		if err != nil {
			return err
		}
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_indexes'"); err != nil {
		return err
	}
	if err = s.profileIndexBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	primary, err := scanProfileIndexTopic(tx.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+" FROM profile_index_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND kind=? AND chat_id=?", s.Config.OwnerID, expected.UserID, expected.Generation, profileIndexCompatibility, s.Config.BackupChatID))
	if err != nil {
		return err
	}
	active := map[int]bool{}
	for _, kind := range []string{profileIndexCompatibility, profileIndexAge} {
		for i, payload := range plans[kind] {
			slot := cataloguePhysicalPage(kind, i+1)
			active[slot] = true
			if err = s.stageProfileIndexPageTx(ctx, tx, primary.ID, slot, payload); err != nil {
				return err
			}
		}
	}
	if primary.Thread > 0 {
		link := forumThreadURL(primary.Chat, primary.Thread)
		if err = s.retireCataloguePagesTx(ctx, tx, primary, active, link); err != nil {
			return err
		}
		legacy, e := scanProfileIndexTopic(tx.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+" FROM profile_index_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND kind=? AND chat_id=?", s.Config.OwnerID, expected.UserID, expected.Generation, profileIndexAge, s.Config.BackupChatID))
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e == nil {
			if err = s.retireCataloguePagesTx(ctx, tx, legacy, nil, link); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE profile_index_topics SET next_refresh=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=?", now.Add(5*time.Minute).UnixMilli(), s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) stageProfileIndexPageTx(ctx context.Context, tx *sql.Tx, topic int64, page int, payload profileIndexPayload) error {
	raw, sha, err := profileIndexPayloadBytes(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO profile_index_pages(topic_id,page_no,payload_json,desired_sha) VALUES(?,?,?,?)
 ON CONFLICT(topic_id,page_no) DO UPDATE SET payload_json=excluded.payload_json,desired_sha=excluded.desired_sha,
 state=CASE WHEN profile_index_pages.sent_sha=excluded.desired_sha THEN 'sent' ELSE 'pending' END,next_attempt=0,error='',updated_at=CURRENT_TIMESTAMP
 WHERE (profile_index_pages.desired_sha<>excluded.desired_sha OR (profile_index_pages.message_id=0 AND profile_index_pages.state='error' AND profile_index_pages.error='unpublished_page_superseded')) AND NOT(profile_index_pages.state IN ('uncertain','sending') AND profile_index_pages.message_id=0)`, topic, page, raw, sha)
	return err
}

func (s *Service) claimProfileIndexOperation(ctx context.Context, expected Session) (profileIndexOperation, bool, error) {
	return s.claimProfileIndexOperationMode(ctx, expected, false)
}

func (s *Service) claimProfileIndexOperationMode(ctx context.Context, expected Session, knownOnly bool) (profileIndexOperation, bool, error) {
	var op profileIndexOperation
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return op, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_indexes'"); err != nil {
		return op, false, err
	}
	if err = s.profileIndexBindingTx(ctx, tx, expected); err != nil {
		return op, false, err
	}
	primary, e := scanProfileIndexTopic(tx.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+" FROM profile_index_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND kind=? AND state='sent' AND thread_id>0", s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, profileIndexCompatibility))
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return op, false, e
	}
	if e == nil {
		state, e := readCatalogueTitle(ctx, tx, primary)
		if e != nil {
			return op, false, e
		}
		if state.State != "sent" && state.State != "error" && state.Next <= s.Archiver.now().UnixMilli() {
			if e = writeCatalogueTitle(ctx, tx, primary, catalogueTitleState{State: "preparing"}); e != nil {
				return op, false, e
			}
			return profileIndexOperation{Topic: primary, Page: -1}, true, tx.Commit()
		}
	}
	createKind := "t.kind='compatibility'"
	if s.Store.cfg.SyncCycleEnabled {
		// Finish an already frozen old-layout frontier without creating new Age
		// topics for subsequent cycles or changing that frontier's payload hashes.
		createKind += ` OR EXISTS(SELECT 1 FROM sync_publication_indexes i
 JOIN sync_publication_manifests pm ON pm.cycle_id=i.cycle_id
 JOIN sync_cycles sc ON sc.id=i.cycle_id
 WHERE i.topic_id=t.id AND pm.indexes_frozen=1 AND sc.stage='publishing'
 AND sc.owner_id=t.owner_id AND sc.upstream_id=t.upstream_id AND sc.generation=t.generation)`
	}
	op.Topic, err = scanProfileIndexTopic(tx.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+` FROM profile_index_topics t WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND (`+createKind+`) AND state='pending' AND thread_id=0 AND next_attempt<=? AND ?=0 ORDER BY id LIMIT 1`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, s.Archiver.now().UnixMilli(), knownOnly))
	if err == nil {
		if _, err = tx.ExecContext(ctx, "UPDATE profile_index_topics SET state='preparing',error='',updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='pending'", op.Topic.ID); err != nil {
			return op, false, err
		}
		return op, true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return op, false, err
	}
	var topic int64
	err = tx.QueryRowContext(ctx, `SELECT p.topic_id,p.page_no,p.message_id,p.payload_json,p.desired_sha FROM profile_index_pages p JOIN profile_index_topics t ON t.id=p.topic_id
 WHERE t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=? AND t.state='sent' AND t.thread_id>0 AND p.state='pending' AND p.next_attempt<=? AND (?=0 OR p.message_id>0)
 ORDER BY p.page_no,t.id LIMIT 1`, s.Config.OwnerID, expected.UserID, expected.Generation, s.Config.BackupChatID, s.Archiver.now().UnixMilli(), knownOnly).Scan(&topic, &op.Page, &op.Message, &op.Raw, &op.SHA)
	if errors.Is(err, sql.ErrNoRows) {
		return op, false, tx.Commit()
	}
	if err != nil {
		return op, false, err
	}
	op.Topic, err = scanProfileIndexTopic(tx.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+" FROM profile_index_topics WHERE id=?", topic))
	if err != nil {
		return op, false, err
	}
	if json.Unmarshal(op.Raw, &op.Payload) != nil || digest(op.Raw) != op.SHA {
		return op, false, errors.New("invalid saved profile index payload")
	}
	if err = validProfileIndexPayload(op.Payload); err != nil {
		return op, false, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE profile_index_pages SET state='preparing',error='',updated_at=CURRENT_TIMESTAMP WHERE topic_id=? AND page_no=? AND state='pending'", topic, op.Page); err != nil {
		return op, false, err
	}
	return op, true, tx.Commit()
}

func (s *Service) markProfileIndexSending(ctx context.Context, op profileIndexOperation, expected Session) error {
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_indexes'"); err != nil {
		return err
	}
	if err = s.profileIndexBindingTx(ctx, tx, expected); err != nil {
		return err
	}
	var allowed bool
	if op.Page < 0 {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM profile_index_topics WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND thread_id=? AND kind='compatibility' AND state='sent')`, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation, op.Topic.Chat, op.Topic.Thread).Scan(&allowed)
	} else if op.Page == 0 {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM profile_index_topics WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND thread_id=0 AND state IN ('preparing','sending'))`, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation, op.Topic.Chat).Scan(&allowed)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM profile_index_pages p JOIN profile_index_topics t ON t.id=p.topic_id
 WHERE t.id=? AND t.owner_id=? AND t.upstream_id=? AND t.generation=? AND t.chat_id=? AND t.thread_id=? AND t.state='sent'
 AND p.page_no=? AND p.message_id=? AND p.desired_sha=? AND p.state IN ('preparing','sending'))`, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation, op.Topic.Chat, op.Topic.Thread, op.Page, op.Message, op.SHA).Scan(&allowed)
	}
	if err != nil {
		return err
	}
	if !allowed {
		return errProfileIndexBinding
	}
	if op.Page < 0 {
		err = writeCatalogueTitle(ctx, tx, op.Topic, catalogueTitleState{State: "sending"})
	} else if op.Page == 0 {
		_, err = tx.ExecContext(ctx, "UPDATE profile_index_topics SET state='sending',updated_at=CURRENT_TIMESTAMP WHERE id=?", op.Topic.ID)
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE profile_index_pages SET state='sending',updated_at=CURRENT_TIMESTAMP WHERE topic_id=? AND page_no=?", op.Topic.ID, op.Page)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func profileIndexFailure(err, last error, attempted bool, editing bool, now time.Time) (state, detail string, next int64) {
	classification := err
	var rejected *TelegramError
	if errors.As(last, &rejected) && rejected.Code == 429 {
		classification = last
	}
	if !attempted {
		return "pending", archiveErrorCode(classification), now.Add(5 * time.Second).UnixMilli()
	}
	if errors.As(classification, &rejected) {
		if rejected.Code >= 500 {
			if editing {
				return "pending", archiveErrorCode(classification), now.Add(5 * time.Second).UnixMilli()
			}
			return "uncertain", archiveErrorCode(classification), 0
		}
		if rejected.Code == 429 {
			return "pending", archiveErrorCode(classification), now.Add(max(4*time.Second, time.Duration(rejected.RetryAfter)*time.Second)).UnixMilli()
		}
		return "error", archiveErrorCode(classification), 0
	}
	if errors.Is(classification, errProfileIndexReceipt) {
		return "uncertain", archiveErrorCode(classification), 0
	}
	if editing {
		return "pending", archiveErrorCode(classification), now.Add(5 * time.Second).UnixMilli()
	}
	return "uncertain", archiveErrorCode(classification), 0
}

func (s *Service) finishProfileIndexOperation(ctx context.Context, op profileIndexOperation, state string, next int64, detail string, receipt []byte, thread, message int64) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if receipt == nil {
		receipt = []byte{}
	}
	if op.Page < 0 {
		return writeCatalogueTitle(write, s.Store.DB, op.Topic, catalogueTitleState{State: state, Next: next, Error: detail})
	}
	var r sql.Result
	var err error
	if op.Page == 0 {
		r, err = s.Store.DB.ExecContext(write, `UPDATE profile_index_topics SET state=?,thread_id=CASE WHEN ?>0 THEN ? ELSE thread_id END,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP
 WHERE id=? AND owner_id=? AND upstream_id=? AND generation=? AND state IN ('preparing','sending')`, state, thread, thread, receipt, next, detail, op.Topic.ID, op.Topic.Owner, op.Topic.Upstream, op.Topic.Generation)
	} else {
		r, err = s.Store.DB.ExecContext(write, `UPDATE profile_index_pages SET state=?,message_id=CASE WHEN ?>0 THEN ? ELSE message_id END,
 sent_sha=CASE WHEN ?='sent' THEN ? ELSE sent_sha END,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP
 WHERE topic_id=? AND page_no=? AND desired_sha=? AND state IN ('preparing','sending')`, state, message, message, state, op.SHA, receipt, next, detail, op.Topic.ID, op.Page, op.SHA)
	}
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("profile index receipt not committed")
	}
	if op.Page == 0 && state == "sent" && thread > 0 && op.Topic.Kind == profileIndexCompatibility {
		topic := op.Topic
		topic.Thread = thread
		return writeCatalogueTitle(write, s.Store.DB, topic, catalogueTitleState{State: "sent"})
	}
	return nil
}

func (s *Service) deliverProfileIndex(ctx context.Context) (bool, error) {
	if !s.profileIndexesAllowed() {
		return false, nil
	}
	a := s.Archiver
	send, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !a.mu.TryLock() {
		if !sleep(send, 25*time.Millisecond) {
			return false, send.Err()
		}
	}
	defer a.mu.Unlock()

	unlock, locked, lockErr := s.Store.trySyncPublicationLock()
	if lockErr != nil {
		return false, lockErr
	}
	if !locked {
		return false, nil
	}
	defer unlock()
	if err := s.initProfileIndexes(send); err != nil {
		return false, err
	}
	expected, err := s.Store.Session(send, s.Config.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if expected.UserID <= 0 || expected.Generation == "" {
		return false, nil
	}
	if _, err = s.savedMessageEditSession(send, s.Config.OwnerID); err != nil {
		if errors.Is(err, errSyncPublicationDeferred) {
			return false, nil
		}
		return false, err
	}
	// The owner requested in-place layout edits during collection. Creating a
	// topic or sending a new message still waits for the complete-cycle barrier.
	publicationErr := s.Store.requireSyncPublication(send, s.Config.OwnerID)
	knownOnly := errors.Is(publicationErr, errSyncPublicationDeferred)
	if publicationErr != nil && !knownOnly {
		return false, publicationErr
	}
	if err = s.seedProfileIndexTopics(send, expected); err != nil {
		return false, err
	}
	if err = s.refreshProfileIndexPlan(send, expected); err != nil {
		return false, err
	}
	op, claimed, err := s.claimProfileIndexOperationMode(send, expected, knownOnly)
	if err != nil || !claimed {
		return false, err
	}
	attempted := false
	var last error
	var receipt []byte
	var thread, message int64
	err = a.groupCall(send, func() error {
		if op.Message > 0 || op.Page < 0 {
			current, err := s.savedMessageEditSession(send, s.Config.OwnerID)
			if err != nil {
				return err
			}
			if s.Config.SyncCycleEnabled && (current.UserID != expected.UserID || current.Generation != expected.Generation) {
				return errProfileIndexBinding
			}
		} else if err := s.Store.requireSyncPublication(send, s.Config.OwnerID); err != nil {
			return err
		}
		if e := s.markProfileIndexSending(send, op, expected); e != nil {
			return e
		}
		attempted = true
		if op.Page < 0 {
			var renamed bool
			last = a.TG.Call(send, "editForumTopic", map[string]any{"chat_id": op.Topic.Chat, "message_thread_id": op.Topic.Thread, "name": profileCatalogueTitle}, &renamed)
			var te *TelegramError
			if errors.As(last, &te) && te.Code == 400 && (strings.Contains(strings.ToLower(te.Description), "not modified") || strings.Contains(strings.ToLower(te.Description), "not_modified")) {
				last, renamed = nil, true
			}
			if last == nil && !renamed {
				return errProfileIndexReceipt
			}
			return last
		}
		if op.Page == 0 {
			title := profileCatalogueTitle
			if op.Topic.Kind != profileIndexCompatibility {
				title = profileIndexTitle(op.Topic.Kind)
			}
			var result struct {
				Thread int64 `json:"message_thread_id"`
			}
			last = a.TG.Call(send, "createForumTopic", map[string]any{"chat_id": op.Topic.Chat, "name": title}, &result)
			if last == nil {
				thread = result.Thread
				receipt, _ = json.Marshal(result)
			}
			return last
		}
		var result SentMessage
		if op.Message == 0 {
			if op.Payload.Rich != nil {
				last = a.TG.Call(send, "sendRichMessage", map[string]any{"chat_id": op.Topic.Chat, "message_thread_id": op.Topic.Thread, "rich_message": op.Payload.Rich, "disable_notification": true}, &result)
			} else {
				result, last = a.TG.SendText(send, op.Topic.Chat, op.Topic.Thread, op.Payload.Text, nil, op.Payload.Entities)
			}
		} else {
			payload := map[string]any{"chat_id": op.Topic.Chat, "message_id": op.Message}
			if op.Payload.Rich != nil {
				payload["rich_message"] = op.Payload.Rich
			} else {
				payload["text"], payload["entities"] = op.Payload.Text, op.Payload.Entities
				payload["link_preview_options"] = map[string]bool{"is_disabled": true}
			}
			last = a.TG.Call(send, "editMessageText", payload, &result)
			var te *TelegramError
			if errors.As(last, &te) && te.Code == 400 && strings.Contains(strings.ToLower(te.Description), "message is not modified") {
				message = op.Message
				receipt, _ = json.Marshal(map[string]any{"not_modified": true, "message_id": op.Message, "code": te.Code, "description": te.Description})
				last = nil
				return nil
			}
		}
		if last == nil {
			receipt, _ = json.Marshal(result)
			if result.ID <= 0 || result.Chat.ID != op.Topic.Chat || result.ThreadID != op.Topic.Thread || op.Message > 0 && result.ID != op.Message {
				return errProfileIndexReceipt
			}
			message = result.ID
		}
		return last
	})
	if err == nil && op.Page == 0 {
		var collision bool
		if thread <= 0 || thread == s.Config.BackupThreadID {
			err = errProfileIndexReceipt
		} else {
			e := s.Store.DB.QueryRowContext(send, `SELECT EXISTS(SELECT 1 FROM mirror_topics WHERE thread_id=? UNION ALL SELECT 1 FROM profile_index_topics WHERE chat_id=? AND thread_id=? AND id<>?)`, thread, op.Topic.Chat, thread, op.Topic.ID).Scan(&collision)
			if e != nil {
				err = e
			} else if collision {
				err = errProfileIndexReceipt
			}
		}
	}
	state, detail, next := "sent", "", int64(0)
	if err != nil {
		state, detail, next = profileIndexFailure(err, last, attempted, op.Message > 0 || op.Page < 0, a.now())
		if op.Page == 0 {
			thread = 0
		}
		if op.Message > 0 {
			message = op.Message
		} else {
			message = 0
		}
	}
	finish := s.finishProfileIndexOperation(ctx, op, state, next, detail, receipt, thread, message)
	return true, errors.Join(err, finish)
}
