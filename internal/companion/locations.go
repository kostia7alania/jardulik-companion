package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const locationPageSize = 20

const locationSchema = `
CREATE TABLE IF NOT EXISTS location_batches (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, upstream_id INTEGER NOT NULL, generation TEXT NOT NULL,
 country TEXT NOT NULL, city TEXT NOT NULL, source_page INTEGER NOT NULL, page_size INTEGER NOT NULL,
 total_count INTEGER NOT NULL, raw_json TEXT NOT NULL, source_sha TEXT NOT NULL, captured_at INTEGER NOT NULL,
 UNIQUE(owner_id,run_id,generation,country,city,source_page,page_size,source_sha)
);
CREATE TABLE IF NOT EXISTS location_profiles (
 batch_id INTEGER NOT NULL REFERENCES location_batches(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, position INTEGER NOT NULL, raw_json TEXT NOT NULL,
 PRIMARY KEY(batch_id,position)
);
CREATE INDEX IF NOT EXISTS locations_owner_profile ON location_profiles(owner_id,profile_id);
CREATE TABLE IF NOT EXISTS crawl_location_results (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 run_id TEXT NOT NULL, profile_id INTEGER NOT NULL,
 first_batch_id INTEGER NOT NULL REFERENCES location_batches(id) ON DELETE CASCADE,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','hydrated','unavailable')),
 version_id INTEGER NOT NULL DEFAULT 0, completed_at INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(owner_id,run_id,profile_id)
);
`

type LocationEntry struct {
	ID  int64
	Raw json.RawMessage
}

type LocationPage struct {
	Country, City string
	Page          int
	Total         int64
	Raw           []byte
	Entries       []LocationEntry
	Fingerprint   string
}

// The source is a bare array of partial map previews; pageCount is not a page limit.
func decodeLocations(raw []byte) (LocationPage, error) {
	result := LocationPage{Raw: raw}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '[' {
		return result, errors.New("Jardulik location list format changed")
	}
	var entries []json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&entries); err != nil {
		return result, errors.New("Jardulik location list format changed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return result, errors.New("Jardulik location list has trailing data")
	}
	if len(entries) > locationPageSize {
		return result, errors.New("Jardulik location page exceeds requested size")
	}
	profiles := make([]Profile, 0, len(entries))
	for i, entry := range entries {
		profile, err := decodeProfile(entry)
		if err != nil || profile == nil || number(profile["userId"]) <= 0 {
			return result, errors.New("Jardulik location has no valid profile ID")
		}
		if i == 0 {
			total, ok := profile["totalCount"]
			if !ok {
				return result, errors.New("Jardulik location total missing")
			}
			result.Total, err = strconv.ParseInt(valueString(total), 10, 64)
			if err != nil || result.Total < int64(len(entries)) {
				return result, errors.New("Jardulik location total invalid")
			}
		}
		result.Entries = append(result.Entries, LocationEntry{ID: profile.ID(), Raw: entry})
		profiles = append(profiles, profile)
	}
	var err error
	result.Fingerprint, err = crawlFingerprint(profiles)
	return result, err
}

func (c *Client) crawlPremium(ctx context.Context, owner int64, expected Session) error {
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != expected.Generation || current.UserID != expected.UserID {
		return errors.New("сессия изменилась; обход остановлен")
	}
	account, err := c.Me(ctx, owner)
	if err != nil {
		return err
	}
	if value, exists := account["freezeState"]; exists && value != nil {
		frozen, ok := value.(bool)
		if !ok {
			return errors.New("неизвестное состояние аккаунта")
		}
		if frozen {
			return ErrAccountFrozen
		}
	}
	if number(account["type"]) != 1 {
		return ErrFiltersPremium
	}
	if c.Store.Paused(ctx, owner) {
		return errors.New("аккаунт на паузе; обход остановлен")
	}
	current, err = c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != expected.Generation || current.UserID != expected.UserID {
		return errors.New("сессия изменилась; обход остановлен")
	}
	return nil
}

func (c *Client) LocationsFrozen(ctx context.Context, owner int64, expected Session, country, city string, page int) (LocationPage, error) {
	if page < 1 || page > crawlPageCap {
		return LocationPage{}, errors.New("invalid location page")
	}
	if err := c.crawlPremium(ctx, owner, expected); err != nil {
		return LocationPage{}, err
	}
	query := url.Values{"userId": {strconv.FormatInt(expected.UserID, 10)}, "pageNumber": {strconv.Itoa(page)}, "pageSize": {strconv.Itoa(locationPageSize)}}
	if country != "" {
		query.Set("country", country)
	}
	if city != "" {
		query.Set("city", city)
	}
	raw, err := c.requestSession(ctx, owner, http.MethodGet, "/user/getUsersLocations?"+query.Encode(), nil, &expected)
	if err != nil {
		return LocationPage{}, err
	}
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != expected.Generation || current.UserID != expected.UserID {
		return LocationPage{}, errors.New("сессия изменилась; старый каталог отменён")
	}
	result, err := decodeLocations(raw)
	result.Country, result.City, result.Page = country, city, page
	return result, err
}

// Freezes both entitlement and request identity while retaining KnownProfile's source-ID guard.
func (c *Client) FullProfileFrozen(ctx context.Context, owner int64, expected Session, id int64) (Profile, error) {
	profile, _, err := c.fullProfileFrozenObserved(ctx, owner, expected, id)
	return profile, err
}

func (c *Client) fullProfileFrozenObserved(ctx context.Context, owner int64, expected Session, id int64) (Profile, time.Time, error) {
	var observed time.Time
	if err := c.Store.KnownProfile(ctx, owner, id); err != nil {
		return nil, observed, errors.New("анкета недоступна этому аккаунту")
	}
	if err := c.crawlPremium(ctx, owner, expected); err != nil {
		return nil, observed, err
	}
	query := url.Values{"viewerUserId": {strconv.FormatInt(expected.UserID, 10)}, "targetUserId": {strconv.FormatInt(id, 10)}}
	raw, observed, err := c.requestSessionObserved(ctx, owner, http.MethodGet, "/user/getUser?"+query.Encode(), nil, &expected)
	current, currentErr := c.Store.Session(ctx, owner)
	if currentErr != nil || current.Generation != expected.Generation || current.UserID != expected.UserID {
		return nil, observed, errors.New("сессия изменилась; старая анкета отменена")
	}
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) && upstream.Endpoint == "/user/getUser" && (upstream.Status == http.StatusNotFound || upstream.Status == http.StatusGone) {
			return nil, observed, ErrProfileUnavailable
		}
		return nil, observed, err
	}
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) || bytes.Equal(raw, []byte("[]")) {
		return nil, observed, ErrProfileUnavailable
	}
	if len(raw) > 0 && raw[0] == '[' {
		var entries []json.RawMessage
		if json.Unmarshal(raw, &entries) != nil || len(entries) != 1 {
			return nil, observed, errors.New("Jardulik profile format changed")
		}
		raw = entries[0]
	}
	profile, err := decodeProfile(raw)
	if err != nil || profile == nil || profile.ID() != id {
		return nil, observed, errors.New("Jardulik profile identity or format changed")
	}
	return profile, observed, nil
}

// Save the exact partial batch before its cursor. Partial previews never become versions.
func (s *Store) saveCrawlLocations(ctx context.Context, job *CrawlJob, page LocationPage, captured time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM feed_crawls WHERE owner_id=? AND run_id=? AND generation=? AND upstream_id=? AND mode=2 AND page=? AND state IN ('queued','running'))", job.Owner, job.RunID, job.Generation, job.UserID, job.Page).Scan(&active); err != nil {
		return err
	}
	if !active {
		return errors.New("location crawl changed before archive")
	}
	sourceSHA := digest(page.Raw)
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO location_batches(owner_id,run_id,upstream_id,generation,country,city,source_page,page_size,total_count,raw_json,source_sha,captured_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, job.Owner, job.RunID, job.UserID, job.Generation, page.Country, page.City, page.Page, locationPageSize, page.Total, string(page.Raw), sourceSHA, captured.UnixMilli())
	if err != nil {
		return err
	}
	var batch int64
	if err = tx.QueryRowContext(ctx, `SELECT id FROM location_batches WHERE owner_id=? AND run_id=? AND generation=? AND country=? AND city=? AND source_page=? AND page_size=? AND source_sha=?`, job.Owner, job.RunID, job.Generation, page.Country, page.City, page.Page, locationPageSize, sourceSHA).Scan(&batch); err != nil {
		return err
	}
	for position, entry := range page.Entries {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO location_profiles(batch_id,owner_id,profile_id,position,raw_json) VALUES(?,?,?,?,?)", batch, job.Owner, entry.ID, position, string(entry.Raw)); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO crawl_location_results(owner_id,run_id,profile_id,first_batch_id) VALUES(?,?,?,?)", job.Owner, job.RunID, entry.ID, batch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) nextCrawlLocation(ctx context.Context, job *CrawlJob) (int64, error) {
	var retryTarget int64
	var retryRaw string
	err := s.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", crawlRetryKey(job.Owner)).Scan(&retryRaw)
	if err == nil {
		var retry crawlRetry
		if err = json.Unmarshal([]byte(retryRaw), &retry); err != nil {
			return 0, errors.New("invalid crawl retry checkpoint")
		}
		if retry.RunID == job.RunID && retry.Mode == job.Mode && retry.Page == job.Page && retry.Target > 0 {
			retryTarget = retry.Target
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT r.profile_id,json_extract(v.raw_json,'$.birthday')
 FROM crawl_location_results r JOIN location_batches b ON b.id=r.first_batch_id AND b.owner_id=r.owner_id AND b.run_id=r.run_id
 LEFT JOIN queue q ON q.owner_id=r.owner_id AND q.profile_id=r.profile_id
 LEFT JOIN versions v ON v.id=q.version_id AND v.owner_id=r.owner_id AND v.profile_id=r.profile_id
 WHERE r.owner_id=? AND r.run_id=? AND r.state='pending' AND b.generation=? AND b.upstream_id=?`, job.Owner, job.RunID, job.Generation, job.UserID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var target int64
	var youngest string
	var found, retryPending bool
	for rows.Next() {
		var id int64
		var birthday any
		if err = rows.Scan(&id, &birthday); err != nil {
			return 0, err
		}
		retryPending = retryPending || id == retryTarget
		key := profileAgeOrderKey(birthday)
		if !found || key > youngest || key == youngest && id < target {
			target, youngest, found = id, key, true
		}
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if retryPending {
		return retryTarget, nil
	}
	if !found {
		return 0, sql.ErrNoRows
	}
	return target, nil
}
