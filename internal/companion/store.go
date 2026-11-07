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
}
type Profile map[string]any
type StoredProfile struct {
	VersionID, ID       int64
	Profile             Profile
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
	return &Store{DB: db, cfg: c}, nil
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
	var b []byte
	var raw string
	err := s.DB.QueryRowContext(ctx, "SELECT encrypted,state,account_json FROM sessions WHERE owner_id=?", owner).Scan(&b, &session.State, &raw)
	if err != nil {
		return session, err
	}
	b, err = unseal(s.cfg.SessionKey, b, sessionPurpose(owner))
	if err != nil {
		return session, errors.New("session decryption failed")
	}
	state := session.State
	err = json.Unmarshal(b, &session)
	session.State = state
	session.Account = json.RawMessage(raw)
	return session, err
}
func (s *Store) SessionState(ctx context.Context, owner int64, state, reason string) {
	_, _ = s.DB.ExecContext(ctx, "UPDATE sessions SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE owner_id=?", state, reason, owner)
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
	_, _ = s.DB.ExecContext(ctx, "UPDATE users SET paused=? WHERE id=?", pause, owner)
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
		if _, err = tx.ExecContext(ctx, "INSERT INTO queue(owner_id,profile_id,version_id) VALUES(?,?,?) ON CONFLICT(owner_id,profile_id) DO UPDATE SET version_id=excluded.version_id", owner, id, vid); err != nil {
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
	var raw string
	err := row.Scan(&p.VersionID, &p.ID, &raw, &p.FirstSeen, &p.LastSeen)
	if err == nil {
		err = json.Unmarshal([]byte(raw), &p.Profile)
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
	_, _ = s.DB.ExecContext(ctx, "UPDATE queue SET delivered=1 WHERE owner_id=? AND profile_id=?", owner, id)
	_, _ = s.DB.ExecContext(ctx, "UPDATE users SET first_card_at=COALESCE(first_card_at,CURRENT_TIMESTAMP) WHERE id=?", owner)
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
func (s *Store) List(ctx context.Context, owner int64, favorites bool) ([]StoredProfile, error) {
	q := `SELECT v.id,v.profile_id,v.raw_json,v.first_seen,v.last_seen FROM queue q JOIN versions v ON v.id=q.version_id WHERE q.owner_id=? AND v.owner_id=q.owner_id`
	if favorites {
		q += ` AND EXISTS(SELECT 1 FROM favorites f WHERE f.owner_id=v.owner_id AND f.profile_id=v.profile_id)`
	}
	q += " ORDER BY v.id DESC LIMIT 10"
	rows, err := s.DB.QueryContext(ctx, q, owner)
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
	err := s.DB.QueryRowContext(ctx, `SELECT j.owner_id,j.version_id FROM media_jobs j JOIN sessions s ON s.owner_id=j.owner_id WHERE j.state='pending' AND j.next_attempt<=unixepoch() AND s.state='connected' ORDER BY j.id LIMIT 1`).Scan(&owner, &version)
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
func (s *Store) ClaimAction(ctx context.Context, owner, profile int64) (int64, error) {
	r, err := s.DB.ExecContext(ctx, "INSERT INTO actions(owner_id,profile_id,kind,state) VALUES(?,?,'Like','pending')", owner, profile)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
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
