package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const profileIndexesSchema = `
CREATE TABLE IF NOT EXISTS profile_index_topics (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('compatibility','age')),
 chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 next_attempt INTEGER NOT NULL DEFAULT 0, next_refresh INTEGER NOT NULL DEFAULT 0,
 receipt_json BLOB NOT NULL DEFAULT X'', error TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(owner_id,upstream_id,kind,chat_id)
);
CREATE TABLE IF NOT EXISTS profile_index_pages (
 topic_id INTEGER NOT NULL REFERENCES profile_index_topics(id) ON DELETE CASCADE,
 page_no INTEGER NOT NULL CHECK(page_no>0), message_id INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 payload_json BLOB NOT NULL, desired_sha TEXT NOT NULL, sent_sha TEXT NOT NULL DEFAULT '',
 receipt_json BLOB NOT NULL DEFAULT X'', next_attempt INTEGER NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(topic_id,page_no)
);`

func (s *Service) initProfileIndexes(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, archiveSchema+analysisSchema+profileIndexesSchema)
	return err
}

// The current source fields are paired with the latest confirmed archived card.
// A pending/duplicate new version or unavailable source does not hide its backup.
func (s *Service) profileIndexEntries(ctx context.Context, owner, upstream int64, now time.Time) ([]profileIndexEntry, error) {
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT q.profile_id,v.raw_json,t.thread_id,(
 SELECT CAST(json_extract(published.message_ids,'$[0]') AS INTEGER)
 FROM archive_versions mine
 JOIN versions owned ON owned.id=mine.version_id AND owned.owner_id=mine.owner_id AND owned.profile_id=mine.profile_id
 JOIN archive_versions published ON published.profile_id=mine.profile_id AND published.thread_id=mine.thread_id AND published.state IN ('sent','partial')
 AND ((mine.state IN ('sent','partial') AND published.version_id=mine.version_id AND published.owner_id=mine.owner_id)
 OR (mine.state='duplicate' AND mine.fingerprint<>'' AND published.fingerprint=mine.fingerprint))
 JOIN versions source ON source.id=published.version_id AND source.owner_id=published.owner_id AND source.profile_id=published.profile_id
 JOIN archive_profile_repairs l ON l.version_id=published.version_id AND l.state='sent'
 JOIN archive_receipts r ON r.source_sha=published.source_sha AND r.kind='profile_version' AND r.chat_id=? AND r.thread_id=published.thread_id AND r.message_id>0 AND r.file_id<>''
 LEFT JOIN archive_zip_repairs replacement ON replacement.version_id=published.version_id AND replacement.old_message_id=r.message_id
 LEFT JOIN archive_receipts original ON original.source_sha=replacement.old_source_sha AND original.kind='profile_version' AND original.chat_id=r.chat_id AND original.thread_id=r.thread_id AND original.message_id=r.message_id AND original.file_id<>''
 WHERE mine.owner_id=q.owner_id AND mine.profile_id=q.profile_id AND mine.thread_id=t.thread_id
 AND julianday(COALESCE(original.sent_at,r.sent_at)) IS NOT NULL
 AND CASE WHEN json_valid(published.message_ids) THEN json_type(published.message_ids)='array' AND json_type(published.message_ids,'$[0]')='integer' AND json_extract(published.message_ids,'$[0]')>0 AND json_extract(published.message_ids,'$[0]')<>r.message_id ELSE 0 END
 ORDER BY julianday(COALESCE(original.sent_at,r.sent_at)) DESC,published.version_id DESC LIMIT 1)
 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id
 JOIN mirror_topics t ON t.profile_id=q.profile_id AND t.thread_id>0 AND t.thread_id<>?
 WHERE q.owner_id=?`, s.Config.BackupChatID, s.Config.BackupThreadID, owner)
	if err != nil {
		return nil, err
	}
	var entries []profileIndexEntry
	for rows.Next() {
		var e profileIndexEntry
		var raw string
		var card sql.NullInt64
		if err = rows.Scan(&e.ID, &raw, &e.ThreadID, &card); err != nil {
			rows.Close()
			return nil, err
		}
		if !card.Valid || card.Int64 <= 0 {
			continue
		}
		p, parseErr := decodeProfile([]byte(raw))
		if parseErr != nil || p.ID() != e.ID {
			rows.Close()
			return nil, errors.New("invalid profile index source identity")
		}
		e.CardID = card.Int64
		e.Name = profileIndexName(p, e.ID)
		e.Birthday = profileAgeOrderKey(p["birthday"])
		e.Zodiac = profileIndexZodiac(p, now)
		e.Premium, e.PremiumKnown = profileIndexPremium(p)
		e.Age, e.AgeKnown = autoInteger(p["age"])
		e.AgeKnown = e.AgeKnown && e.Age > 0 && e.Age <= 111
		entries = append(entries, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	scores, err := s.profileIndexScores(ctx, owner, upstream)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		if score, ok := scores[entries[i].ID]; ok {
			entries[i].Percent = score
			entries[i].PercentKnown = true
		}
	}
	return entries, nil
}

func (s *Service) profileIndexScores(ctx context.Context, owner, upstream int64) (map[int64]int, error) {
	scores := map[int64]int{}
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT a.profile_id,a.response_body,a.own_profile,a.target_profile FROM profile_analyses a
 WHERE a.owner_id=? AND a.upstream_id=? AND `+analysisPostSuccessSQL("a")+` ORDER BY a.observed_at_ms DESC,a.id DESC`, owner, upstream)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[int64]bool{}
	for rows.Next() {
		var id int64
		var response, ownRaw, targetRaw []byte
		if err = rows.Scan(&id, &response, &ownRaw, &targetRaw); err != nil {
			return nil, err
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		own, e := debugProfile(ownRaw)
		if e != nil || own.ID() != upstream {
			continue
		}
		target, e := debugProfile(targetRaw)
		if e != nil || target.ID() != id {
			continue
		}
		ownAge, ownKnown := autoInteger(own["age"])
		targetAge, targetKnown := autoInteger(target["age"])
		if !ownKnown || !targetKnown || ownAge < 18 || ownAge > 111 || targetAge < 18 || targetAge > 111 {
			continue
		}
		var body struct {
			Content string `json:"content"`
		}
		if json.Unmarshal(response, &body) != nil {
			continue
		}
		if value, known := analysisPostRating(body.Content); known {
			scores[id] = value
		}
	}
	return scores, rows.Err()
}
