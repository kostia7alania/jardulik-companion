package companion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

type Store struct {
	DB           *sql.DB
	cfg          Config
	sessionLocks sync.Map
	mediaMu      sync.Mutex
}

func (s *Store) SessionMutex(owner int64) *sync.Mutex {
	v, _ := s.sessionLocks.LoadOrStore(owner, &sync.Mutex{})
	return v.(*sync.Mutex)
}

type Session struct {
	Token      string          `json:"token"`
	Refresh    string          `json:"refreshToken"`
	UserID     int64           `json:"userId"`
	Expires    int64           `json:"expires"`
	Generation string          `json:"generation,omitempty"`
	State      string          `json:"-"`
	Account    json.RawMessage `json:"-"`
	encrypted  []byte
}
type Profile map[string]any
type StoredProfile struct {
	VersionID, ID       int64
	Profile             Profile
	RawJSON             string `json:"-"`
	FirstSeen, LastSeen string
}
type MediaJob struct {
	ID, OwnerID, VersionID, ProfileID int64
	Position                          int
	Role, URL, SHA, State, Path, MIME string
	Size                              int64
}

func OpenStore(c Config) (*Store, error) {
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(c.DataDir, "jardulik.sqlite")}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{DB: db, cfg: c}
	if err = s.initReactionActions(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) Meta(ctx context.Context, key string) string {
	var v string
	_ = s.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", key).Scan(&v)
	return v
}
func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.DB.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return err
}
func (s *Store) User(ctx context.Context, id int64, source string) error {
	_, err := s.DB.ExecContext(ctx, "INSERT INTO users(id,source) VALUES(?,?) ON CONFLICT(id) DO NOTHING", id, source)
	return err
}
func (s *Store) Event(ctx context.Context, owner int64, kind string) {
	_, _ = s.DB.ExecContext(ctx, "INSERT INTO events(owner_id,kind) VALUES(?,?)", owner, kind)
}
func (s *Store) SaveSession(ctx context.Context, owner int64, session Session) error {
	b, err := json.Marshal(session)
	if err != nil {
		return err
	}
	b, err = seal(s.cfg.SessionKey, b, sessionPurpose(owner))
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO sessions(owner_id,upstream_id,encrypted,expires_at,state) VALUES(?,?,?,?,'connected')
	 ON CONFLICT(owner_id) DO UPDATE SET upstream_id=excluded.upstream_id,encrypted=excluded.encrypted,expires_at=excluded.expires_at,state='connected',last_error='',updated_at=CURRENT_TIMESTAMP`, owner, session.UserID, b, session.Expires)
	return err
}
func (s *Store) Session(ctx context.Context, owner int64) (Session, error) {
	var session Session
	var encrypted []byte
	var raw string
	err := s.DB.QueryRowContext(ctx, "SELECT encrypted,state,account_json FROM sessions WHERE owner_id=?", owner).Scan(&encrypted, &session.State, &raw)
	if err != nil {
		return session, err
	}
	b, err := unseal(s.cfg.SessionKey, encrypted, sessionPurpose(owner))
	if err != nil {
		return session, errors.New("session decryption failed")
	}
	state := session.State
	err = json.Unmarshal(b, &session)
	session.State = state
	session.Account = json.RawMessage(raw)
	if err == nil {
		session.encrypted = encrypted
	}
	return session, err
}
func (s *Store) SessionState(ctx context.Context, owner int64, state, reason string) {
	_, _ = s.DB.ExecContext(ctx, "UPDATE sessions SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=?", state, reason, owner)
}

// A provider failure can change only the connected session used for the request.
func (s *Store) setSessionStateIfCurrent(ctx context.Context, owner int64, expected Session, state, reason string) {
	_, _ = s.DB.ExecContext(ctx, `UPDATE sessions SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND upstream_id=? AND encrypted=? AND state='connected'`, state, reason, owner, expected.UserID, expected.encrypted)
}
func (s *Store) SaveAccount(ctx context.Context, owner int64, p Profile) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "UPDATE sessions SET account_json=? WHERE owner_id=?", string(b), owner)
	return err
}
func (s *Store) Disconnect(ctx context.Context, owner int64) error {
	mu := s.SessionMutex(owner)
	mu.Lock()
	defer mu.Unlock()
	_, err := s.DB.ExecContext(ctx, "DELETE FROM sessions WHERE owner_id=?", owner)
	return err
}
func (s *Store) DeleteUser(ctx context.Context, owner int64) error {
	mu := s.SessionMutex(owner)
	mu.Lock()
	defer mu.Unlock()
	s.mediaMu.Lock()
	defer s.mediaMu.Unlock()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO gc_jobs(sha,path) SELECT m.sha,m.path FROM media m JOIN media_jobs j ON j.sha=m.sha WHERE j.owner_id=?`, owner); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO deletions(owner_id) VALUES(?) ON CONFLICT(owner_id) DO UPDATE SET deleted_at=CURRENT_TIMESTAMP", owner); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM users WHERE id=?", owner); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	_ = s.collectOrphansLocked(ctx)
	return nil
}
func (s *Store) CollectOrphans(ctx context.Context) error {
	s.mediaMu.Lock()
	defer s.mediaMu.Unlock()
	return s.collectOrphansLocked(ctx)
}
func (s *Store) collectOrphansLocked(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, "SELECT sha,path FROM gc_jobs")
	if err != nil {
		return err
	}
	type item struct{ sha, path string }
	var jobs []item
	for rows.Next() {
		var j item
		if err = rows.Scan(&j.sha, &j.path); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, j)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, j := range jobs {
		j.path = filepath.Join(s.cfg.DataDir, "media", j.sha)
		var refs int
		if err = s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM media_jobs WHERE sha=?", j.sha).Scan(&refs); err != nil {
			return err
		}
		if refs == 0 {
			if len(j.sha) != 64 || strings.ContainsAny(j.sha, "/\\") {
				return errors.New("invalid media garbage collection path")
			}
			if err = os.Remove(j.path); err != nil && !os.IsNotExist(err) {
				return err
			}
			if _, err = s.DB.ExecContext(ctx, "DELETE FROM media WHERE sha=?", j.sha); err != nil {
				return err
			}
		}
		if _, err = s.DB.ExecContext(ctx, "DELETE FROM gc_jobs WHERE sha=?", j.sha); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Paused(ctx context.Context, owner int64) bool {
	var v int
	_ = s.DB.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&v)
	return v != 0
}
func (s *Store) Pause(ctx context.Context, owner int64, pause bool) {
	if !pause {
		_, _ = s.DB.ExecContext(ctx, "UPDATE users SET paused=0 WHERE id=?", owner)
		return
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE users SET paused=1 WHERE id=?", owner); err != nil {
		return
	}
	// /resume must not re-enable a daily crawl between scheduler ticks.
	if _, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) SELECT ?,'1' WHERE EXISTS(SELECT 1 FROM users WHERE id=?)
 ON CONFLICT(key) DO UPDATE SET value='1'`, dailyCrawlBlockKey(owner), owner); err != nil {
		return
	}
	_ = tx.Commit()
}

func valueString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return string(x)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
func number(v any) int64 { n, _ := strconv.ParseInt(valueString(v), 10, 64); return n }
func (p Profile) ID() int64 {
	if n := number(p["userId"]); n > 0 {
		return n
	}
	return number(p["id"])
}
func (p Profile) String(key string) string { return valueString(p[key]) }
func (p Profile) Photos(key string) []string {
	var a []string
	switch v := p[key].(type) {
	case []any:
		for _, x := range v {
			if u := strings.TrimSpace(valueString(x)); u != "" {
				a = append(a, u)
			}
		}
	case []string:
		a = v
	case string:
		if strings.HasPrefix(strings.TrimSpace(v), "[") {
			_ = json.Unmarshal([]byte(v), &a)
		} else {
			a = strings.FieldsFunc(v, func(c rune) bool { return c == ',' || c == '\n' || c == '|' || c == ';' })
		}
	}
	for i := range a {
		a[i] = strings.TrimSpace(a[i])
	}
	return a
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (s *Store) Archive(ctx context.Context, owner int64, profiles []Profile, source string) ([]int64, error) {
	return s.archive(ctx, owner, profiles, source, false)
}

// ArchiveViewed preserves full snapshots without adding a viewed relation to discovery.
func (s *Store) ArchiveViewed(ctx context.Context, owner int64, profiles []Profile, source string) ([]int64, error) {
	return s.archive(ctx, owner, profiles, source, true)
}

func (s *Store) archive(ctx context.Context, owner int64, profiles []Profile, source string, viewed bool) ([]int64, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make([]int64, 0, len(profiles))
	for _, p := range profiles {
		id := p.ID()
		if id <= 0 {
			return nil, errors.New("upstream returned a profile without a valid id")
		}
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		fp := digest(b)
		text := strings.ToLower(strings.Join(strings.Fields(p.String("description")), " "))
		_, err = tx.ExecContext(ctx, "INSERT INTO versions(owner_id,profile_id,fingerprint,text_fingerprint,raw_json) VALUES(?,?,?,?,?) ON CONFLICT(owner_id,profile_id,fingerprint) DO UPDATE SET last_seen=CURRENT_TIMESTAMP", owner, id, fp, digest([]byte(text)), string(b))
		if err != nil {
			return nil, err
		}
		var vid int64
		if err = tx.QueryRowContext(ctx, "SELECT id FROM versions WHERE owner_id=? AND profile_id=? AND fingerprint=?", owner, id, fp).Scan(&vid); err != nil {
			return nil, err
		}
		ids = append(ids, vid)
		if _, err = tx.ExecContext(ctx, "INSERT INTO observations(owner_id,version_id,source) VALUES(?,?,?)", owner, vid, source); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO queue(owner_id,profile_id,version_id,delivered) VALUES(?,?,?,?) ON CONFLICT(owner_id,profile_id) DO UPDATE SET version_id=excluded.version_id", owner, id, vid, viewed); err != nil {
			return nil, err
		}
		for _, role := range []string{"photos", "miniPhotos"} {
			for pos, u := range p.Photos(role) {
				if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO media_jobs(owner_id,version_id,position,role,url) VALUES(?,?,?,?,?)", owner, vid, pos, role, u); err != nil {
					return nil, err
				}
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}
func scanProfile(row interface{ Scan(...any) error }) (StoredProfile, error) {
	var p StoredProfile
	err := row.Scan(&p.VersionID, &p.ID, &p.RawJSON, &p.FirstSeen, &p.LastSeen)
	if err == nil {
		err = json.Unmarshal([]byte(p.RawJSON), &p.Profile)
	}
	return p, err
}
func (s *Store) Next(ctx context.Context, owner int64) (StoredProfile, error) {
	return scanProfile(s.DB.QueryRowContext(ctx, `SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id WHERE q.owner_id=? AND v.owner_id=? AND q.delivered=0 ORDER BY q.rowid LIMIT 1`, owner, owner))
}
func (s *Store) Profile(ctx context.Context, owner, id int64) (StoredProfile, error) {
	return scanProfile(s.DB.QueryRowContext(ctx, "SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id WHERE q.owner_id=? AND q.profile_id=? AND v.owner_id=q.owner_id", owner, id))
}
func (s *Store) Version(ctx context.Context, owner, vid int64) (StoredProfile, error) {
	return scanProfile(s.DB.QueryRowContext(ctx, "SELECT id,profile_id,raw_json,first_seen,last_seen FROM versions WHERE owner_id=? AND id=?", owner, vid))
}
func (s *Store) Delivered(ctx context.Context, owner, id int64) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE queue SET delivered=1 WHERE owner_id=? AND profile_id=?", owner, id); err != nil {
		return
	}
	// Filter tables are lazy; manual delivery also works before filters are used.
	var filterMarkers bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='feed_filter_pending')").Scan(&filterMarkers); err != nil {
		return
	}
	if filterMarkers {
		if _, err = tx.ExecContext(ctx, "DELETE FROM feed_filter_pending WHERE owner_id=? AND profile_id=?", owner, id); err != nil {
			return
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE users SET first_card_at=COALESCE(first_card_at,CURRENT_TIMESTAMP) WHERE id=?", owner); err != nil {
		return
	}
	_ = tx.Commit()
}
func (s *Store) Page(ctx context.Context, owner int64) int {
	var n int
	_ = s.DB.QueryRowContext(ctx, "SELECT feed_page FROM users WHERE id=?", owner).Scan(&n)
	if n < 1 {
		return 1
	}
	return n
}
func (s *Store) AdvancePage(ctx context.Context, owner int64, empty bool) {
	if empty {
		_, _ = s.DB.ExecContext(ctx, "UPDATE users SET feed_page=1 WHERE id=?", owner)
	} else {
		_, _ = s.DB.ExecContext(ctx, "UPDATE users SET feed_page=feed_page+1 WHERE id=?", owner)
	}
}
func (s *Store) Favorite(ctx context.Context, owner, id int64) error {
	if _, err := s.Profile(ctx, owner, id); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, "INSERT OR IGNORE INTO favorites(owner_id,profile_id) VALUES(?,?)", owner, id)
	return err
}

type ProfileListItem struct {
	StoredProfile
	Cursor int64
}

func (s *Store) List(ctx context.Context, owner int64, favorites bool, before int64) ([]ProfileListItem, error) {
	if before < 0 {
		return nil, errors.New("invalid list cursor")
	}
	q := `SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen,q.rowid FROM queue q JOIN versions v ON v.id=q.version_id WHERE q.owner_id=? AND v.owner_id=q.owner_id`
	if favorites {
		q += ` AND EXISTS(SELECT 1 FROM favorites f WHERE f.owner_id=v.owner_id AND f.profile_id=v.profile_id)`
	}
	args := []any{owner}
	if before > 0 {
		q += " AND q.rowid<?"
		args = append(args, before)
	}
	q += " ORDER BY q.rowid DESC LIMIT 11"
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProfileListItem
	for rows.Next() {
		var p ProfileListItem
		if err := rows.Scan(&p.VersionID, &p.ID, &p.RawJSON, &p.FirstSeen, &p.LastSeen, &p.Cursor); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(p.RawJSON), &p.Profile); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) History(ctx context.Context, owner, id int64) ([]StoredProfile, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen FROM versions v WHERE v.owner_id=? AND v.profile_id=? ORDER BY (SELECT MAX(o.id) FROM observations o WHERE o.version_id=v.id AND o.owner_id=v.owner_id) DESC LIMIT 10", owner, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredProfile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) MediaJobs(ctx context.Context, owner, version int64) ([]MediaJob, error) {
	q := `SELECT j.id,j.owner_id,j.version_id,v.profile_id,j.position,j.role,j.url,COALESCE(j.sha,''),j.state,COALESCE(m.path,''),COALESCE(m.mime,''),COALESCE(m.size,0) FROM media_jobs j JOIN versions v ON v.id=j.version_id LEFT JOIN media m ON m.sha=j.sha WHERE j.owner_id=? AND j.version_id=? ORDER BY j.role DESC,j.position`
	rows, err := s.DB.QueryContext(ctx, q, owner, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MediaJob
	for rows.Next() {
		var j MediaJob
		if err = rows.Scan(&j.ID, &j.OwnerID, &j.VersionID, &j.ProfileID, &j.Position, &j.Role, &j.URL, &j.SHA, &j.State, &j.Path, &j.MIME, &j.Size); err != nil {
			return nil, err
		}
		if j.SHA != "" {
			j.Path = filepath.Join(s.cfg.DataDir, "media", j.SHA)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) PendingMedia(ctx context.Context) (MediaJob, error) {
	var owner, version int64
	err := s.DB.QueryRowContext(ctx, `SELECT j.owner_id,j.version_id FROM media_jobs j JOIN sessions s ON s.owner_id=j.owner_id JOIN users u ON u.id=j.owner_id WHERE u.paused=0 AND j.state='pending' AND j.next_attempt<=unixepoch() AND s.state='connected' ORDER BY j.id LIMIT 1`).Scan(&owner, &version)
	if err != nil {
		return MediaJob{}, err
	}
	jobs, err := s.MediaJobs(ctx, owner, version)
	if err != nil {
		return MediaJob{}, err
	}
	for _, j := range jobs {
		if j.State == "pending" {
			var ready bool
			_ = s.DB.QueryRowContext(ctx, "SELECT next_attempt<=unixepoch() FROM media_jobs WHERE id=?", j.ID).Scan(&ready)
			if !ready {
				continue
			}
			return j, nil
		}
	}
	return MediaJob{}, sql.ErrNoRows
}
func (s *Store) MediaDone(ctx context.Context, j MediaJob, sha, path, mime string, size int64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO media(sha,size,mime,path) VALUES(?,?,?,?)", sha, size, mime, filepath.Join("media", sha)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE media_jobs SET sha=?,state='complete',error='' WHERE id=?", sha, j.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) MediaError(ctx context.Context, id int64, reason string) {
	_, _ = s.DB.ExecContext(ctx, "UPDATE media_jobs SET state='error',error=? WHERE id=?", reason, id)
}
func (s *Store) MediaFailure(ctx context.Context, id int64, reason string) {
	_, _ = s.DB.ExecContext(ctx, `UPDATE media_jobs SET attempts=attempts+1,next_attempt=unixepoch()+20*(1<<attempts),state=CASE WHEN attempts+1>=3 THEN 'error' ELSE 'pending' END,error=? WHERE id=? AND state='pending'`, reason, id)
}
func (s *Store) ClaimUpdate(ctx context.Context, id int64) (bool, error) {
	r, err := s.DB.ExecContext(ctx, "INSERT OR IGNORE INTO updates(id,state) VALUES(?,'started')", id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
func (s *Store) FinishUpdate(ctx context.Context, id int64) {
	_, _ = s.DB.ExecContext(ctx, "UPDATE updates SET state='done' WHERE id=?", id)
}

type ActionBlockedError struct {
	State   string
	RetryAt time.Time
}

func (e *ActionBlockedError) Error() string {
	return "предыдущее действие не разрешает повтор"
}
func (e *ActionBlockedError) Unwrap() error { return ErrActionBlocked }

func (s *Store) initReactionActions(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_actions_schema'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, debugAuditSchema); err != nil {
		return err
	}
	var auditLink int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('debug_api_calls') WHERE name='action_id'").Scan(&auditLink); err != nil {
		return err
	}
	if auditLink == 0 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE debug_api_calls ADD COLUMN action_id INTEGER REFERENCES actions(id) ON DELETE SET NULL"); err != nil {
			return err
		}
	}
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info(actions)")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, kind string
		var defaultValue any
		if err = rows.Scan(&cid, &name, &kind, &notnull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == "retry_at"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE actions ADD COLUMN retry_at INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	// Keep the original audit rows. A confirmed quota is protected until its
	// deadline; a retry closes that old attempt and creates a new pending row.
	if _, err = tx.ExecContext(ctx, "DROP INDEX IF EXISTS actions_no_duplicate"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE UNIQUE INDEX actions_no_duplicate ON actions(owner_id,profile_id,kind)
 WHERE state IN ('pending','processing','succeeded','unknown','quota_waiting','rejected')`); err != nil {
		return err
	}
	return tx.Commit()
}

type reactionQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) checkReactionAction(ctx context.Context, q reactionQuerier, owner, profile int64, now time.Time) error {
	if !s.cfg.AllowReactions {
		return ErrReactionDisabled
	}
	var paused bool
	var sessionState string
	var known bool
	err := q.QueryRowContext(ctx, `SELECT u.paused,s.state,EXISTS(
 SELECT 1 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id
 WHERE q.owner_id=u.id AND q.profile_id=?) FROM users u JOIN sessions s ON s.owner_id=u.id WHERE u.id=?`, profile, owner).Scan(&paused, &sessionState, &known)
	if err != nil {
		return err
	}
	if paused {
		return ErrReactionPaused
	}
	if sessionState != "connected" {
		return ErrReactionSessionChanged
	}
	if !known {
		return errors.New("анкета недоступна этому аккаунту")
	}
	if err = s.checkDebugPositive(ctx, q, owner, profile); err != nil {
		return err
	}
	var state string
	var retry int64
	// A service quota is account-wide, including clicks on another profile.
	err = q.QueryRowContext(ctx, `SELECT state,retry_at FROM actions WHERE owner_id=? AND kind='Like'
 AND state='quota_waiting' AND (retry_at=0 OR retry_at>?) ORDER BY retry_at=0 DESC,retry_at DESC LIMIT 1`, owner, now.Unix()).Scan(&state, &retry)
	if err == nil {
		blocked := &ActionBlockedError{State: state}
		if retry > 0 {
			blocked.RetryAt = time.Unix(retry, 0).UTC()
		}
		return blocked
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	err = q.QueryRowContext(ctx, `SELECT state,retry_at FROM actions WHERE owner_id=? AND profile_id=? AND kind='Like'
 AND state NOT IN ('quota_waiting','quota_retried','fire_waiting') ORDER BY id DESC LIMIT 1`, owner, profile).Scan(&state, &retry)
	if err == nil {
		return &ActionBlockedError{State: state}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return nil
}

func (s *Store) CanAttemptLike(ctx context.Context, owner, profile int64, at ...time.Time) (bool, error) {
	now := time.Now()
	if len(at) > 0 {
		now = at[0]
	}
	err := s.checkReactionAction(ctx, s.DB, owner, profile, now)
	return err == nil, err
}

func (s *Store) ClaimAction(ctx context.Context, owner, profile int64) (int64, error) {
	return s.ClaimOwnerAction(ctx, owner, profile, time.Now())
}

func (s *Store) ClaimOwnerAction(ctx context.Context, owner, profile int64, now time.Time) (int64, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_actions_schema'"); err != nil {
		return 0, err
	}
	if err = s.checkReactionAction(ctx, tx, owner, profile, now); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE actions SET state='quota_retried' WHERE owner_id=? AND profile_id=? AND kind='Like'
 AND state='quota_waiting' AND retry_at>0 AND retry_at<=?`, owner, profile, now.Unix()); err != nil {
		return 0, err
	}
	r, err := tx.ExecContext(ctx, "INSERT INTO actions(owner_id,profile_id,kind,state,created_at) VALUES(?,?,'Like','pending',?)", owner, profile, now.UTC().Format("2006-01-02 15:04:05"))
	if err != nil {
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (s *Store) finishReactionAction(ctx context.Context, id int64, state string, response []byte, retryAt time.Time) error {
	retry := int64(0)
	if !retryAt.IsZero() {
		// Never retry before the entire server duration has elapsed.
		retry = retryAt.Unix()
		if retryAt.Nanosecond() > 0 {
			retry++
		}
	}
	r, err := s.DB.ExecContext(ctx, "UPDATE actions SET state=?,response=?,retry_at=? WHERE id=? AND state='pending'", state, string(response), retry, id)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("не удалось сохранить состояние реакции")
	}
	return nil
}

func (s *Store) releaseUnsentReaction(ctx context.Context, id int64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Reserve the writer before inspecting a legacy action or refunding its intent.
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return err
	}
	var tables int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('reaction_message_settings','reaction_message_intents')").Scan(&tables); err != nil {
		return err
	}
	if tables == 2 {
		_, err = tx.ExecContext(ctx, `UPDATE reaction_message_settings AS p
 SET next_cursor=(SELECT i.reserved_cursor FROM reaction_message_intents i WHERE i.action_id=?),updated_at=CURRENT_TIMESTAMP
 WHERE EXISTS(SELECT 1 FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id
 WHERE i.action_id=? AND i.owner_id=p.owner_id AND i.policy_revision=p.revision
 AND i.message_mode IN ('rotate','birthday') AND i.reserved_cursor>=0 AND p.next_cursor=i.reserved_cursor+1
 AND a.owner_id=i.owner_id AND a.profile_id=i.profile_id AND a.kind='Like' AND a.state='pending' AND a.response='')`, id, id)
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM actions WHERE id=? AND state='pending' AND response=''", id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) FinishAction(ctx context.Context, id int64, state string, response []byte) {
	_, _ = s.DB.ExecContext(ctx, "UPDATE actions SET state=?,response=? WHERE id=?", state, string(response), id)
}
func (s *Store) Snapshot(ctx context.Context, path string) error {
	_, err := s.DB.ExecContext(ctx, "VACUUM INTO ?", path)
	return err
}
func (s *Store) Counts(ctx context.Context, owner int64) map[string]int64 {
	out := map[string]int64{}
	queries := map[string]string{"profiles": "SELECT COUNT(DISTINCT profile_id) FROM versions WHERE owner_id=?", "versions": "SELECT COUNT(*) FROM versions WHERE owner_id=?", "photos": "SELECT COUNT(*) FROM media_jobs WHERE owner_id=? AND state='complete'", "pending_photos": "SELECT COUNT(*) FROM media_jobs WHERE owner_id=? AND state='pending'", "favorites": "SELECT COUNT(*) FROM favorites WHERE owner_id=?", "likes": "SELECT COUNT(*) FROM actions WHERE owner_id=? AND state='succeeded'"}
	for k, q := range queries {
		var n int64
		_ = s.DB.QueryRowContext(ctx, q, owner).Scan(&n)
		out[k] = n
	}
	return out
}
func (s *Store) AdminCounts(ctx context.Context) map[string]int64 {
	out := map[string]int64{}
	for k, q := range map[string]string{"users": "SELECT COUNT(*) FROM users", "connected": "SELECT COUNT(*) FROM sessions WHERE state='connected'", "starts": "SELECT COUNT(DISTINCT owner_id) FROM events WHERE kind='start'", "connect_success": "SELECT COUNT(DISTINCT owner_id) FROM events WHERE kind='connect_success'", "first_card_delivered": "SELECT COUNT(*) FROM users WHERE first_card_at IS NOT NULL"} {
		var n int64
		_ = s.DB.QueryRowContext(ctx, q).Scan(&n)
		out[k] = n
	}
	return out
}
func (s *Store) Check(ctx context.Context) error {
	var v string
	if err := s.DB.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&v); err != nil {
		return err
	}
	if v != "ok" {
		return fmt.Errorf("SQLite integrity: %s", v)
	}
	return nil
}
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }
