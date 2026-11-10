package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const archiveChangePostsSchema = `CREATE TABLE IF NOT EXISTS archive_change_posts (
 version_id INTEGER PRIMARY KEY REFERENCES versions(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, profile_id INTEGER NOT NULL,
 thread_id INTEGER NOT NULL, published_at TEXT NOT NULL, old_version_id INTEGER REFERENCES versions(id) ON DELETE CASCADE,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','skipped','error','uncertain')),
 notice_text TEXT NOT NULL DEFAULT '', message_id INTEGER NOT NULL DEFAULT 0,
 next_attempt INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

var errArchiveChangeBlocked = errors.New("archive change post deferred")

func (s *Service) initArchiveChangePosts(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, profilePresenceSchema+archiveChangePostsSchema)
	return err
}

func (s *Service) recoverArchiveChangePosts(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, `UPDATE archive_change_posts SET state=CASE state WHEN 'sending' THEN 'uncertain' ELSE 'pending' END,
 error=CASE state WHEN 'sending' THEN 'worker_restarted_during_request' ELSE '' END,updated_at=CURRENT_TIMESTAMP
 WHERE owner_id=? AND state IN ('preparing','sending')`, s.Config.OwnerID)
	return err
}

// The source ZIP receipt orders actual publications, including historical backfill.
func (s *Service) enqueueArchiveChangePosts(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, `WITH published AS (
 SELECT av.version_id,av.owner_id,av.profile_id,av.thread_id,COALESCE(original.sent_at,r.sent_at) AS sent_at
 FROM archive_versions av JOIN versions v ON v.id=av.version_id AND v.owner_id=av.owner_id AND v.profile_id=av.profile_id
 JOIN archive_profile_repairs layout ON layout.version_id=av.version_id AND layout.state='sent'
 JOIN archive_receipts r ON r.source_sha=av.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=av.thread_id AND r.message_id>0 AND r.file_id<>''
 JOIN mirror_topics t ON t.profile_id=av.profile_id AND t.thread_id=av.thread_id
 LEFT JOIN archive_zip_repairs replacement ON replacement.version_id=av.version_id AND replacement.old_message_id=r.message_id
 LEFT JOIN archive_receipts original ON original.source_sha=replacement.old_source_sha AND original.kind='profile_version' AND original.chat_id=r.chat_id AND original.thread_id=r.thread_id AND original.message_id=r.message_id AND original.file_id<>''
 WHERE av.owner_id=? AND av.state IN ('sent','partial') AND av.thread_id>0 AND av.thread_id<>? AND julianday(COALESCE(original.sent_at,r.sent_at)) IS NOT NULL
 AND CASE WHEN json_valid(av.message_ids) THEN json_type(av.message_ids)='array' AND json_type(av.message_ids,'$[0]')='integer' AND json_extract(av.message_ids,'$[0]')>0 AND json_extract(av.message_ids,'$[0]')<>r.message_id ELSE 0 END
 ), ordered AS (
 SELECT *,LAG(version_id) OVER(PARTITION BY owner_id,profile_id,thread_id ORDER BY julianday(sent_at),version_id) AS previous
 FROM published)
 INSERT OR IGNORE INTO archive_change_posts(version_id,owner_id,profile_id,thread_id,published_at,old_version_id)
 SELECT version_id,owner_id,profile_id,thread_id,sent_at,previous FROM ordered WHERE previous IS NOT NULL`, s.Config.BackupChatID, s.Config.OwnerID, s.Config.BackupThreadID)
	return err
}

// Called once by the service runtime; all external effects share the archive gate.
func (s *Service) RunArchiveChangePosts(ctx context.Context) {
	if s.Archiver == nil || s.Archiver.TG == nil || s.Config.OwnerID <= 0 || s.Config.BackupChatID == 0 {
		return
	}
	if err := s.initArchiveChangePosts(ctx); err != nil {
		return
	}
	if err := s.recoverArchiveChangePosts(ctx); err != nil {
		return
	}
	for ctx.Err() == nil {
		if _, err := s.deliverArchiveChangePost(ctx); err != nil && ctx.Err() == nil {
			_ = s.Store.SetMeta(ctx, "archive_change_posts_last_error", archiveErrorCode(err))
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

func (s *Service) archiveChangeSnapshot(ctx context.Context, version int64) (StoredProfile, error) {
	var p StoredProfile
	err := s.Store.DB.QueryRowContext(ctx, "SELECT id,profile_id,raw_json,first_seen,last_seen FROM versions WHERE id=? AND owner_id=?", version, s.Config.OwnerID).Scan(&p.VersionID, &p.ID, &p.RawJSON, &p.FirstSeen, &p.LastSeen)
	if err != nil {
		return p, err
	}
	if !json.Valid([]byte(p.RawJSON)) {
		return p, errors.New("invalid source profile JSON")
	}
	p.Profile, err = decodeProfile([]byte(p.RawJSON))
	if err == nil && (p.Profile == nil || p.Profile.ID() != p.ID) {
		err = errors.New("source snapshot identity mismatch")
	}
	return p, err
}

func archiveChangeRaw(value any, exists bool) string {
	if !exists {
		return "(поле отсутствует)"
	}
	raw, _ := json.Marshal(value)
	return string(raw)
}

func archiveChangeValue(p Profile, key string, exists bool) string {
	if !exists || p[key] == nil {
		return archiveChangeRaw(p[key], exists)
	}
	if key == "type" {
		if n, ok := autoInteger(p[key]); ok {
			if n == 1 {
				return "да"
			}
			if n == 0 {
				return "нет"
			}
		}
		return archiveChangeRaw(p[key], true)
	}
	if _, ok := p[key].(string); !ok {
		if key != "languages" {
			return archiveChangeRaw(p[key], true)
		}
	}
	if key == "country" && p.String(key) == "South Ossetia" {
		return "Южная Осетия"
	}
	if key == "city" && p.String(key) == "Tskhinval" {
		return profileValue(Profile{"city": "Tskhinvali"}, "city")
	}
	if value := profileValue(p, key); value != "" {
		return value
	}
	return archiveChangeRaw(p[key], true)
}

func archiveChangePhotoRows(before, after []MediaJob) []string {
	type item struct {
		position int
		source   string
	}
	collect := func(jobs []MediaJob) map[string][]item {
		roles := map[string][]item{}
		for _, j := range jobs {
			source := j.SHA
			if source == "" {
				source = "unavailable:" + j.URL
			}
			roles[j.Role] = append(roles[j.Role], item{j.Position, source})
		}
		for key := range roles {
			sort.Slice(roles[key], func(i, j int) bool {
				a, b := roles[key][i], roles[key][j]
				if a.position != b.position {
					return a.position < b.position
				}
				return a.source < b.source
			})
		}
		return roles
	}
	old, new := collect(before), collect(after)
	roles := []string{"photos", "miniPhotos"}
	known := map[string]bool{"photos": true, "miniPhotos": true}
	var extra []string
	for _, set := range []map[string][]item{old, new} {
		for role := range set {
			if !known[role] {
				extra = append(extra, role)
				known[role] = true
			}
		}
	}
	sort.Strings(extra)
	roles = append(roles, extra...)
	var lines []string
	var photoChange, previewChange string
	for _, role := range roles {
		a, b := old[role], new[role]
		same := len(a) == len(b)
		if same {
			for i := range a {
				if a[i] != b[i] {
					same = false
					break
				}
			}
		}
		if same {
			continue
		}
		counts := map[string]int{}
		for _, v := range a {
			counts[v.source]--
		}
		for _, v := range b {
			counts[v.source]++
		}
		added, removed := 0, 0
		for _, n := range counts {
			if n > 0 {
				added += n
			} else {
				removed -= n
			}
		}
		summary := "изменён порядок/позиции"
		if added != 0 || removed != 0 {
			summary = fmt.Sprintf("добавлено %d, убрано %d", added, removed)
		}
		if role != "photos" && role != "miniPhotos" {
			lines = append(lines, "Файлы "+role+": "+summary)
			continue
		}
		var parts []string
		if added > 0 {
			parts = append(parts, fmt.Sprintf("добавлено: %d", added))
		}
		if removed > 0 {
			parts = append(parts, fmt.Sprintf("убрано: %d", removed))
		}
		summary = strings.Join(parts, ", ")
		if summary == "" {
			summary = "изменён порядок"
		}
		if role == "photos" {
			photoChange = summary
		} else {
			previewChange = summary
		}
	}
	if photoChange == "" {
		photoChange = previewChange
	}
	if photoChange != "" {
		lines = append([]string{"фото: " + photoChange}, lines...)
	}
	return lines
}

func archiveProfileChangeRows(before, after Profile, oldPhotos, newPhotos []MediaJob) []string {
	fields := []struct{ key, label string }{{"name", "Имя"}, {"surName", "Фамилия"}, {"age", "Возраст"}, {"height", "Рост"}, {"country", "Страна"}, {"city", "Город"}, {"type", "Premium"}, {"birthday", "Дата рождения"}, {"goal", "Цель знакомства"}, {"worldview", "Мировоззрение"}, {"children", "Дети"}, {"profession", "Занятие"}, {"languages", "Языки"}, {"familyNames", "Родственные фамилии"}, {"description", "О себе"}}
	known := map[string]bool{"distance": true, "reactionView": true, "isFire": true, "fireText": true, "photos": true, "miniPhotos": true}
	for _, f := range fields {
		known[f.key] = true
	}
	var extra []string
	for _, p := range []Profile{before, after} {
		for key := range p {
			if !known[key] {
				extra = append(extra, key)
				known[key] = true
			}
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		fields = append(fields, struct{ key, label string }{key, key})
	}
	var rows []string
	for _, f := range fields {
		old, hadOld := before[f.key]
		new, hadNew := after[f.key]
		oldRaw, newRaw := archiveChangeRaw(old, hadOld), archiveChangeRaw(new, hadNew)
		if hadOld == hadNew && oldRaw == newRaw {
			continue
		}
		left, right := oldRaw, newRaw
		if f.label != f.key {
			left, right = archiveChangeValue(before, f.key, hadOld), archiveChangeValue(after, f.key, hadNew)
			if left == right {
				left, right = oldRaw, newRaw
			}
		}
		rows = append(rows, f.label+": "+left+" → "+right)
	}
	return append(rows, archiveChangePhotoRows(oldPhotos, newPhotos)...)
}

func archiveChangeNotice(p StoredProfile, rows []string) (string, []MessageEntity, any) {
	const title = "✏️ Изменения анкеты\n"
	at := p.FirstSeen
	if at == "" {
		at = p.LastSeen
	}
	footer := "\n\nСнимок: " + displayTime(at)
	body := strings.Join(rows, "\n")
	text := title + body + footer
	if textUnits(text) <= 4096 {
		return text, nil, nil
	}
	const limited = "\n\nПоказана часть изменений. Полный исходный JSON по кнопке в личку."
	prefix, _ := telegramPrefix(body, 4096-textUnits(title+limited+footer)-1)
	prefix = strings.TrimRight(prefix, " \n") + "…"
	text = title + prefix + limited + footer
	entities := []MessageEntity{{Type: "expandable_blockquote", Offset: textUnits(title), Length: textUnits(prefix)}}
	keyboard := map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "JSON Jardulik в личку", "callback_data": fmt.Sprintf("aj:%d", p.VersionID)}}}}
	return text, entities, keyboard
}

// Rechecked inside the group-slot closure, so an absence event wins while waiting.
func (s *Service) claimArchiveChangeSend(ctx context.Context, version int64) (bool, error) {
	r, err := s.Store.DB.ExecContext(ctx, `UPDATE archive_change_posts SET state='sending',updated_at=CURRENT_TIMESTAMP
 WHERE version_id=? AND owner_id=? AND state IN ('preparing','sending')
 AND NOT EXISTS(SELECT 1 FROM profile_presence p WHERE p.owner_id=archive_change_posts.owner_id AND p.profile_id=archive_change_posts.profile_id AND p.availability='unavailable')
 AND NOT EXISTS(SELECT 1 FROM profile_presence_events e WHERE e.owner_id=archive_change_posts.owner_id AND e.profile_id=archive_change_posts.profile_id AND e.state IN ('pending','processing'))
 AND EXISTS(SELECT 1 FROM archive_versions av JOIN archive_profile_repairs layout ON layout.version_id=av.version_id AND layout.state='sent'
 JOIN archive_receipts r ON r.source_sha=av.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=av.thread_id AND r.message_id>0 AND r.file_id<>''
 JOIN mirror_topics t ON t.profile_id=av.profile_id AND t.thread_id=av.thread_id
 WHERE av.version_id=archive_change_posts.version_id AND av.owner_id=archive_change_posts.owner_id AND av.profile_id=archive_change_posts.profile_id AND av.thread_id=archive_change_posts.thread_id AND av.state IN ('sent','partial')
 AND CASE WHEN json_valid(av.message_ids) THEN json_type(av.message_ids)='array' AND json_type(av.message_ids,'$[0]')='integer' AND json_extract(av.message_ids,'$[0]')>0 AND json_extract(av.message_ids,'$[0]')<>r.message_id ELSE 0 END)
 AND EXISTS(SELECT 1 FROM archive_versions prior JOIN archive_profile_repairs layout ON layout.version_id=prior.version_id AND layout.state='sent'
 JOIN archive_receipts r ON r.source_sha=prior.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=prior.thread_id AND r.message_id>0 AND r.file_id<>''
 WHERE prior.version_id=archive_change_posts.old_version_id AND prior.owner_id=archive_change_posts.owner_id AND prior.profile_id=archive_change_posts.profile_id AND prior.thread_id=archive_change_posts.thread_id AND prior.state IN ('sent','partial')
 AND CASE WHEN json_valid(prior.message_ids) THEN json_type(prior.message_ids)='array' AND json_type(prior.message_ids,'$[0]')='integer' AND json_extract(prior.message_ids,'$[0]')>0 AND json_extract(prior.message_ids,'$[0]')<>r.message_id ELSE 0 END)`, version, s.Config.OwnerID, s.Config.BackupChatID, s.Config.BackupChatID)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (s *Service) deliverArchiveChangePost(ctx context.Context) (bool, error) {
	a := s.Archiver
	if a == nil || a.TG == nil || s.Config.OwnerID <= 0 || s.Config.BackupChatID == 0 || s.Config.ArchivePaused || a.Config.ArchivePaused {
		return false, nil
	}
	sendCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !a.mu.TryLock() {
		if !sleep(sendCtx, 25*time.Millisecond) {
			return false, sendCtx.Err()
		}
	}
	defer a.mu.Unlock()
	if err := a.init(sendCtx); err != nil {
		return false, err
	}
	if err := s.initArchiveChangePosts(sendCtx); err != nil {
		return false, err
	}
	if err := s.enqueueArchiveChangePosts(sendCtx); err != nil {
		return false, err
	}
	var version, profile, thread int64
	var previous sql.NullInt64
	err := s.Store.DB.QueryRowContext(sendCtx, `SELECT version_id,profile_id,thread_id,old_version_id FROM archive_change_posts n
 WHERE owner_id=? AND state='pending' AND next_attempt<=?
 AND NOT EXISTS(SELECT 1 FROM profile_presence p WHERE p.owner_id=n.owner_id AND p.profile_id=n.profile_id AND p.availability='unavailable')
 AND NOT EXISTS(SELECT 1 FROM profile_presence_events e WHERE e.owner_id=n.owner_id AND e.profile_id=n.profile_id AND e.state IN ('pending','processing'))
 ORDER BY julianday(published_at) DESC,version_id DESC LIMIT 1`, s.Config.OwnerID, a.now().Unix()).Scan(&version, &profile, &thread, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	result, err := s.Store.DB.ExecContext(sendCtx, "UPDATE archive_change_posts SET state='preparing',updated_at=CURRENT_TIMESTAMP WHERE version_id=? AND owner_id=? AND state='pending'", version, s.Config.OwnerID)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return false, nil
	}
	finish := func(state, text string, message, next int64, detail string) error {
		writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		_, e := s.Store.DB.ExecContext(writeCtx, "UPDATE archive_change_posts SET state=?,notice_text=?,message_id=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=? AND owner_id=?", state, text, message, next, detail, version, s.Config.OwnerID)
		return e
	}
	if !previous.Valid {
		return true, finish("skipped", "", 0, 0, "initial_published_version")
	}
	current, err := s.archiveChangeSnapshot(sendCtx, version)
	if err != nil {
		return true, errors.Join(err, finish("error", "", 0, 0, "version_unavailable"))
	}
	old, err := s.archiveChangeSnapshot(sendCtx, previous.Int64)
	if err != nil || old.ID != profile || current.ID != profile {
		return true, errors.Join(err, finish("error", "", 0, 0, "previous_version_unavailable"))
	}
	oldPhotos, err := s.Store.MediaJobs(sendCtx, s.Config.OwnerID, old.VersionID)
	if err != nil {
		return true, errors.Join(err, finish("error", "", 0, 0, "media_inventory_unavailable"))
	}
	newPhotos, err := s.Store.MediaJobs(sendCtx, s.Config.OwnerID, current.VersionID)
	if err != nil {
		return true, errors.Join(err, finish("error", "", 0, 0, "media_inventory_unavailable"))
	}
	rows := archiveProfileChangeRows(old.Profile, current.Profile, oldPhotos, newPhotos)
	if len(rows) == 0 {
		return true, finish("skipped", "", 0, 0, "no_profile_changes")
	}
	text, entities, keyboard := archiveChangeNotice(current, rows)
	if _, err = s.Store.DB.ExecContext(sendCtx, "UPDATE archive_change_posts SET notice_text=? WHERE version_id=? AND owner_id=? AND state='preparing'", text, version, s.Config.OwnerID); err != nil {
		return true, errors.Join(err, finish("error", text, 0, 0, "notice_prepare_failed"))
	}
	var message SentMessage
	var lastSendErr error
	attempted := false
	err = a.groupCall(sendCtx, func() error {
		allowed, e := s.claimArchiveChangeSend(sendCtx, version)
		if e != nil {
			return e
		}
		if !allowed {
			return errArchiveChangeBlocked
		}
		attempted = true
		message, lastSendErr = a.TG.SendText(sendCtx, s.Config.BackupChatID, thread, text, keyboard, entities)
		return lastSendErr
	})
	if err == nil && (message.ID <= 0 || message.Chat.ID != s.Config.BackupChatID || message.ThreadID != thread) {
		err = errors.New("archive change receipt unconfirmed")
	}
	state, detail, next := "sent", "", int64(0)
	if err != nil {
		classification := err
		var rejected *TelegramError
		if errors.As(lastSendErr, &rejected) && rejected.Code == 429 {
			classification = lastSendErr
		}
		state, detail = "uncertain", archiveErrorCode(classification)
		if errors.Is(err, errArchiveChangeBlocked) {
			state, detail = "pending", "publication_or_presence_not_ready"
			next = a.now().Add(30 * time.Second).Unix()
		} else if !attempted {
			state, detail = "pending", ""
		} else if errors.As(classification, &rejected) {
			state = "error"
			if rejected.Code == 429 {
				state = "pending"
				next = a.now().Add(max(4*time.Second, time.Duration(rejected.RetryAfter)*time.Second)).Unix()
			}
		}
	}
	writeErr := finish(state, text, message.ID, next, detail)
	if errors.Is(err, errArchiveChangeBlocked) {
		err = nil
	}
	return true, errors.Join(err, writeErr)
}
