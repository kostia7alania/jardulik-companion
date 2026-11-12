package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

const relationSchema = `
CREATE TABLE IF NOT EXISTS relation_batches (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL DEFAULT 0 CHECK(upstream_id>=0),
 mutual INTEGER NOT NULL CHECK(mutual IN (0,1)), source_page INTEGER NOT NULL, raw_json TEXT NOT NULL,
 captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS relation_profiles (
 batch_id INTEGER NOT NULL REFERENCES relation_batches(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, profile_id INTEGER NOT NULL,
 position INTEGER NOT NULL, raw_json TEXT NOT NULL, PRIMARY KEY(batch_id,position)
);
CREATE INDEX IF NOT EXISTS relation_owner_profile ON relation_profiles(owner_id,profile_id);`

func (s *Store) initRelations(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='relation_batches'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, relationSchema); err != nil {
		return err
	}
	var bound, contactJobs bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pragma_table_info('relation_batches') WHERE name='upstream_id')").Scan(&bound); err != nil {
		return err
	}
	if bound {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, "ALTER TABLE relation_batches ADD COLUMN upstream_id INTEGER NOT NULL DEFAULT 0 CHECK(upstream_id>=0)"); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='contact_requests')").Scan(&contactJobs); err != nil {
		return err
	}
	if contactJobs {
		// A contact request records the source account when its exact mutual batch is saved.
		_, err = tx.ExecContext(ctx, `WITH proof AS (
 SELECT b.id,MIN(r.upstream_id) AS upstream_id FROM relation_batches b
 JOIN contact_requests r ON r.mutual_batch_id=b.id AND r.owner_id=b.owner_id
 JOIN relation_profiles p ON p.batch_id=b.id AND p.owner_id=b.owner_id AND p.profile_id=r.profile_id
 WHERE b.mutual=1 GROUP BY b.id
 HAVING MIN(r.upstream_id)>0 AND MIN(r.upstream_id)=MAX(r.upstream_id) AND MIN(length(r.generation))>0
 ) UPDATE relation_batches SET upstream_id=(SELECT proof.upstream_id FROM proof WHERE proof.id=relation_batches.id)
 WHERE id IN (SELECT id FROM proof)`)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// KnownProfile accepts only this owner's discovery, relationship or location provenance.
func (s *Store) KnownProfile(ctx context.Context, owner, id int64) error {
	if owner <= 0 || id <= 0 {
		return sql.ErrNoRows
	}
	var found int
	err := s.DB.QueryRowContext(ctx, `SELECT 1 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id WHERE q.owner_id=? AND q.profile_id=? LIMIT 1`, owner, id).Scan(&found)
	if err == nil || !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = s.initRelations(ctx); err != nil {
		return err
	}
	err = s.DB.QueryRowContext(ctx, `SELECT 1 FROM relation_profiles p JOIN relation_batches b ON b.id=p.batch_id AND b.owner_id=p.owner_id
 JOIN sessions se ON se.owner_id=b.owner_id AND se.upstream_id=b.upstream_id AND se.state='connected'
 WHERE p.owner_id=? AND p.profile_id=? AND b.upstream_id>0 LIMIT 1`, owner, id).Scan(&found)
	if err == nil || !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var catalog bool
	if err = s.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='location_profiles')").Scan(&catalog); err != nil {
		return err
	}
	if !catalog {
		return sql.ErrNoRows
	}
	return s.DB.QueryRowContext(ctx, `SELECT 1 FROM location_profiles p JOIN location_batches b ON b.id=p.batch_id AND b.owner_id=p.owner_id WHERE p.owner_id=? AND p.profile_id=? LIMIT 1`, owner, id).Scan(&found)
}

type RelationPage struct {
	ID         int64
	Mutual     bool
	SourcePage int
	Profiles   []Profile
}

func decodeRelations(raw []byte) ([]Profile, error) {
	var profiles []Profile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&profiles); err != nil {
		return nil, errors.New("Jardulik relationship list format changed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("Jardulik relationship list has trailing data")
	}
	if len(profiles) > 1000 {
		return nil, errors.New("Jardulik relationship page exceeds requested size")
	}
	for _, profile := range profiles {
		if profile.ID() <= 0 {
			return nil, errors.New("Jardulik relationship has no valid profile ID")
		}
	}
	return profiles, nil
}

// ReactUsers preserves partial source previews; it never hydrates or archives a profile.
func (c *Client) ReactUsers(ctx context.Context, owner int64, mutual bool, page int) (RelationPage, error) {
	return c.reactUsers(ctx, owner, nil, mutual, page, true)
}

func (c *Client) ReactUsersFrozen(ctx context.Context, owner int64, expected Session, mutual bool, page int) (RelationPage, error) {
	return c.reactUsers(ctx, owner, &expected, mutual, page, true)
}

func (c *Client) reactUsers(ctx context.Context, owner int64, expected *Session, mutual bool, page int, queueContacts bool) (RelationPage, error) {
	result := RelationPage{Mutual: mutual, SourcePage: page}
	if page < 1 {
		return result, errors.New("invalid relationship page")
	}
	free, err := FreeBytes(c.Store.cfg.DataDir)
	if err != nil {
		return result, err
	}
	if free < c.Store.cfg.MinFreeBytes {
		return result, errors.New("сбор приостановлен: недостаточно резервного места")
	}
	session, err := c.Store.Session(ctx, owner)
	if err != nil {
		return result, err
	}
	if expected != nil && (session.UserID != expected.UserID || session.Generation != expected.Generation) {
		return result, ErrContactSessionChanged
	}
	query := url.Values{"userId": {strconv.FormatInt(session.UserID, 10)}, "isMutual": {strconv.FormatBool(mutual)}, "pageNumber": {strconv.Itoa(page)}, "pageSize": {"1000"}}
	raw, err := c.requestSession(ctx, owner, "GET", "/user/getReactUsers?"+query.Encode(), nil, &session)
	if err != nil {
		return result, err
	}
	result.Profiles, err = decodeRelations(raw)
	if err != nil {
		return result, err
	}
	mutex := c.Store.SessionMutex(owner)
	mutex.Lock()
	defer mutex.Unlock()
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != session.Generation || current.UserID != session.UserID {
		return result, errors.New("сессия изменилась; старый список отменён")
	}
	if err = c.Store.initRelations(ctx); err != nil {
		return result, err
	}
	if mutual && queueContacts {
		if err = c.Store.initContactJobs(ctx); err != nil {
			return result, err
		}
	}
	tx, err := c.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	inserted, err := tx.ExecContext(ctx, "INSERT INTO relation_batches(owner_id,upstream_id,mutual,source_page,raw_json) VALUES(?,?,?,?,?)", owner, session.UserID, mutual, page, string(raw))
	if err != nil {
		return result, err
	}
	result.ID, err = inserted.LastInsertId()
	if err != nil {
		return result, err
	}
	for pos, profile := range result.Profiles {
		value, e := json.Marshal(profile)
		if e != nil {
			return result, e
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO relation_profiles(batch_id,owner_id,profile_id,position,raw_json) VALUES(?,?,?,?,?)", result.ID, owner, profile.ID(), pos, string(value)); err != nil {
			return result, err
		}
	}
	if mutual && queueContacts {
		if err = c.Store.queueMutualContactsTx(ctx, tx, owner, session, result.ID, result.Profiles, c.Now().UTC()); err != nil {
			return result, err
		}
	}
	return result, tx.Commit()
}

func (s *Store) relationPage(ctx context.Context, owner, upstream, batch int64, mutual bool) (RelationPage, error) {
	result := RelationPage{ID: batch, Mutual: mutual}
	var raw string
	if err := s.initRelations(ctx); err != nil {
		return result, err
	}
	err := s.DB.QueryRowContext(ctx, `SELECT b.source_page,b.raw_json FROM relation_batches b
 JOIN sessions se ON se.owner_id=b.owner_id AND se.upstream_id=b.upstream_id AND se.state='connected'
 WHERE b.owner_id=? AND b.upstream_id=? AND b.upstream_id>0 AND b.id=? AND b.mutual=?`, owner, upstream, batch, mutual).Scan(&result.SourcePage, &raw)
	if err != nil {
		return result, err
	}
	result.Profiles, err = decodeRelations([]byte(raw))
	return result, err
}

func (s *Service) OpenRelations(ctx context.Context, owner int64, mutual bool) {
	if s.Media != nil {
		if err := s.Media.CheckDisk(); err != nil {
			_ = s.say(ctx, owner, "Сбор данных приостановлен: недостаточно места.", nil)
			return
		}
	}
	session, err := s.Store.Session(ctx, owner)
	if err != nil {
		_ = s.say(ctx, owner, "Не удалось загрузить список. Проверь подключение: /connect", nil)
		return
	}
	page, err := s.Client.ReactUsersFrozen(ctx, owner, session, mutual, 1)
	if err != nil {
		_ = s.say(ctx, owner, "Не удалось загрузить список. Проверь подключение: /connect", nil)
		return
	}
	s.showRelations(ctx, owner, session, page, 0)
}

func (s *Service) showRelations(ctx context.Context, owner int64, session Session, page RelationPage, offset int) {
	title, prefix, open := "Входящие", "rl", "ri"
	if page.Mutual {
		title, prefix, open = "Взаимности", "rm", "rt"
	}
	if offset < 0 || offset > len(page.Profiles) || (offset == len(page.Profiles) && offset > 0 && offset < 1000) {
		return
	}
	if offset == len(page.Profiles) && len(page.Profiles) == 1000 {
		next, err := s.Client.ReactUsersFrozen(ctx, owner, session, page.Mutual, page.SourcePage+1)
		if err != nil {
			_ = s.say(ctx, owner, "Следующая часть списка пока недоступна.", nil)
			return
		}
		s.showRelations(ctx, owner, session, next, 0)
		return
	}
	if len(page.Profiles) == 0 {
		_ = s.say(ctx, owner, title+": в этой загрузке записей нет.", nil)
		return
	}
	end := offset + 10
	if end > len(page.Profiles) {
		end = len(page.Profiles)
	}
	var buttons [][]map[string]string
	for _, profile := range page.Profiles[offset:end] {
		label := profileHeading(profile)
		if label == "" {
			label = fmt.Sprintf("Анкета #%d", profile.ID())
		}
		if fire, ok := profile["isFire"].(bool); ok && fire {
			label = "🔥 " + label
		}
		buttons = append(buttons, []map[string]string{{"text": truncateRunes(label, 64), "callback_data": fmt.Sprintf("%s:%d:%d", open, page.ID, profile.ID())}})
	}
	var navigation []map[string]string
	if offset > 0 {
		navigation = append(navigation, map[string]string{"text": "К началу", "callback_data": fmt.Sprintf("%s:%d:0", prefix, page.ID)})
	}
	if end < len(page.Profiles) || len(page.Profiles) == 1000 {
		navigation = append(navigation, map[string]string{"text": "Ещё", "callback_data": fmt.Sprintf("%s:%d:%d", prefix, page.ID, end)})
	}
	if len(navigation) > 0 {
		buttons = append(buttons, navigation)
	}
	text := fmt.Sprintf("%s\nЗагружено записей: %d. Показаны %d–%d.", title, len(page.Profiles), offset+1, end)
	_ = s.say(ctx, owner, text, map[string]any{"inline_keyboard": buttons})
}

// The caller holds the normal per-user UI lock; callbacks never carry authority.
func (s *Service) HandleRelationCallback(ctx context.Context, owner int64, data string) bool {
	parts := strings.Split(data, ":")
	if len(parts) == 0 || (parts[0] != "rl" && parts[0] != "rm" && parts[0] != "ri" && parts[0] != "rt") {
		return false
	}
	if len(parts) != 3 {
		return true
	}
	batch, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || batch <= 0 {
		return true
	}
	value, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || value < 0 {
		return true
	}
	mutual := parts[0] == "rm" || parts[0] == "rt"
	session, err := s.Store.Session(ctx, owner)
	if err != nil || session.State != "connected" {
		_ = s.say(ctx, owner, "Этот список недоступен. Открой его заново после /connect.", nil)
		return true
	}
	page, err := s.Store.relationPage(ctx, owner, session.UserID, batch, mutual)
	if err != nil {
		_ = s.say(ctx, owner, "Этот список недоступен. Открой его заново.", nil)
		return true
	}
	if parts[0] == "rl" || parts[0] == "rm" {
		if value <= int64(len(page.Profiles)) {
			s.showRelations(ctx, owner, session, page, int(value))
		}
		return true
	}
	if value == 0 {
		return true
	}
	found := false
	for _, profile := range page.Profiles {
		if profile.ID() == value {
			found = true
			break
		}
	}
	if !found {
		_ = s.say(ctx, owner, "Анкета отсутствует в этом списке.", nil)
		return true
	}
	if !mutual {
		viewer, e := s.Client.meForSession(ctx, owner, session)
		if e != nil {
			_ = s.say(ctx, owner, "Не удалось проверить твой тариф. Повтори подключение: /connect", nil)
			return true
		}
		if number(viewer["type"]) != 1 {
			_ = s.say(ctx, owner, "Для открытия обычных входящих нужен Premium Jardulik. Тариф автора анкеты на это не влияет.", nil)
			return true
		}
	}
	profile, _, _, err := s.Client.checkedProfileObserved(ctx, owner, session, value)
	if err != nil {
		_ = s.say(ctx, owner, "Полная анкета сейчас недоступна.", nil)
		return true
	}
	mutex := s.Store.SessionMutex(owner)
	mutex.Lock()
	current, e := s.Store.Session(ctx, owner)
	if e != nil || current.State != "connected" || current.Generation != session.Generation || current.UserID != session.UserID {
		mutex.Unlock()
		return true
	}
	source := "incoming_viewed"
	if mutual {
		source = "mutual_viewed"
	}
	versions, e := s.Store.ArchiveViewed(ctx, owner, []Profile{profile}, source)
	mutex.Unlock()
	if e != nil || len(versions) != 1 {
		_ = s.say(ctx, owner, "Не удалось сохранить полную анкету.", nil)
		return true
	}
	stored, e := s.Store.Version(ctx, owner, versions[0])
	if e == nil {
		_ = s.Deliver(ctx, owner, stored)
	}
	return true
}
