package companion

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const fullBundleFormat = "jardulik-companion-full/v2"
const personBundleFormat = "jardulik-companion-person/v2"
const fullBundleLimit int64 = 1 << 30
const fullBundleRestoreLimit int64 = 2 << 30
const fullBundleEntries = 100000

// FullBackupArtifact is built locally. This path has no Telegram sender and does
// not enable scheduled full backups; transport approval is a separate step.
type FullBackupArtifact struct {
	RecoveryFingerprint       string `json:"recovery_fingerprint,omitempty"`
	Reused                    bool   `json:"reused,omitempty"`
	Path                      string `json:"path,omitempty"`
	SourceFingerprint         string `json:"source_fingerprint"`
	SourceSHA                 string `json:"source_sha256,omitempty"`
	EncryptedSHA              string `json:"encrypted_sha256,omitempty"`
	ByteSize                  int64  `json:"byte_size,omitempty"`
	Profiles                  int    `json:"profiles"`
	Versions                  int    `json:"versions"`
	MediaFiles                int    `json:"media_files"`
	MediaBytes                int64  `json:"media_bytes"`
	Unchanged                 bool   `json:"unchanged"`
	HostedUploadLimitExceeded bool   `json:"hosted_upload_limit_exceeded"`
	UploadEnabled             bool   `json:"upload_enabled"`
}

type fullMedia struct {
	SHA  string `json:"sha256"`
	Size int64  `json:"byte_size"`
	MIME string `json:"mime"`
}
type fullPerson struct {
	ProfileID  int64  `json:"profile_id"`
	Filename   string `json:"filename"`
	Versions   int    `json:"versions"`
	MediaFiles int    `json:"media_files"`
}
type fullManifest struct {
	Format            string         `json:"format"`
	CapturedAt        string         `json:"captured_at"`
	AppVersion        string         `json:"app_version"`
	SourceFingerprint string         `json:"source_fingerprint"`
	Files             []archiveFile  `json:"files"`
	People            []fullPerson   `json:"people"`
	Media             []fullMedia    `json:"media"`
	Coverage          map[string]any `json:"coverage"`
}
type personVersion struct {
	VersionID    int64                `json:"version_id"`
	ObserverID   int64                `json:"observer_id"`
	RawFile      string               `json:"raw_file"`
	FirstSeen    string               `json:"first_seen"`
	LastSeen     string               `json:"last_seen"`
	Media        []archiveMedia       `json:"media,omitempty"`
	Observations []archiveObservation `json:"observations,omitempty"`
}
type sourcePresence struct {
	OwnerID      int64  `json:"observer_id"`
	ProfileID    int64  `json:"profile_id"`
	Transition   int64  `json:"transition"`
	Availability string `json:"availability"`
	ObservedAt   string `json:"observed_at"`
}
type personManifest struct {
	Format    string           `json:"format"`
	ProfileID int64            `json:"profile_id"`
	Files     []archiveFile    `json:"files"`
	Versions  []personVersion  `json:"versions"`
	Presence  []sourcePresence `json:"availability_transitions,omitempty"`
}

const fullBackupSchema = `CREATE TABLE IF NOT EXISTS full_backup_artifacts (
 id INTEGER PRIMARY KEY, source_fingerprint TEXT NOT NULL, source_sha TEXT NOT NULL,
 encrypted_sha TEXT NOT NULL, path TEXT NOT NULL, byte_size INTEGER NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, artifact_json TEXT NOT NULL DEFAULT ''
);`

func openFullSnapshot(path string) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: path}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// A version cursor and its photo/observation cursor may coexist.
	db.SetMaxOpenConns(2)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func snapshotHasTable(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&n)
	return n != 0, err
}

func fullSnapshotJobs(ctx context.Context, db *sql.DB, version int64) ([]MediaJob, error) {
	rows, err := db.QueryContext(ctx, `SELECT j.owner_id,j.version_id,j.position,j.role,j.url,COALESCE(j.sha,''),j.state,
 COALESCE(m.size,0),COALESCE(m.mime,'') FROM media_jobs j LEFT JOIN media m ON m.sha=j.sha
 WHERE j.version_id=? ORDER BY j.role,j.position`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MediaJob
	for rows.Next() {
		if len(out) >= fullBundleEntries {
			return nil, errArchiveBudget
		}
		var j MediaJob
		if err = rows.Scan(&j.OwnerID, &j.VersionID, &j.Position, &j.Role, &j.URL, &j.SHA, &j.State, &j.Size, &j.MIME); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func fullSnapshotPresence(ctx context.Context, db *sql.DB, profile int64) ([]sourcePresence, error) {
	exists, err := snapshotHasTable(ctx, db, "profile_presence_events")
	if err != nil || !exists {
		return nil, err
	}
	query := "SELECT owner_id,profile_id,transition,availability,observed_at FROM profile_presence_events"
	var args []any
	if profile > 0 {
		query += " WHERE profile_id=?"
		args = append(args, profile)
	}
	query += " ORDER BY owner_id,profile_id,transition"
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sourcePresence
	for rows.Next() {
		if len(out) >= fullBundleEntries {
			return nil, errArchiveBudget
		}
		var p sourcePresence
		if err = rows.Scan(&p.OwnerID, &p.ProfileID, &p.Transition, &p.Availability, &p.ObservedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Source history, current heads and durable user state make a daily backup dirty.
// Session secrets, observations, Telegram delivery and captions are bookkeeping.
func fullSourceFingerprint(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, `SELECT v.id,v.owner_id,v.profile_id,v.raw_json,
 EXISTS(SELECT 1 FROM queue q WHERE q.owner_id=v.owner_id AND q.profile_id=v.profile_id AND q.version_id=v.id)
 FROM versions v ORDER BY v.id`)
	if err != nil {
		return "", err
	}
	var records []string
	for rows.Next() {
		if len(records) >= fullBundleEntries {
			rows.Close()
			return "", errArchiveBudget
		}
		var id, owner, profile int64
		var current bool
		var raw string
		if err = rows.Scan(&id, &owner, &profile, &raw, &current); err != nil {
			rows.Close()
			return "", err
		}
		if len(raw) > serviceManifestLimit {
			rows.Close()
			return "", errArchiveBudget
		}
		var p Profile
		if err = json.Unmarshal([]byte(raw), &p); err != nil {
			rows.Close()
			return "", err
		}
		jobs, e := fullSnapshotJobs(ctx, db, id)
		if e != nil {
			rows.Close()
			return "", e
		}
		fp, e := mirrorFingerprint(p, jobs)
		if e != nil {
			rows.Close()
			return "", e
		}
		records = append(records, fmt.Sprintf("version:%d:%d:%s", owner, profile, fp))
		if current {
			records = append(records, fmt.Sprintf("head:%d:%d:%s", owner, profile, fp))
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	media, err := fullSnapshotMedia(ctx, db)
	if err != nil {
		return "", err
	}
	for _, m := range media {
		records = append(records, fmt.Sprintf("media:%s:%d:%s", m.SHA, m.Size, m.MIME))
	}
	presence, err := fullSnapshotPresence(ctx, db, 0)
	if err != nil {
		return "", err
	}
	for _, p := range presence {
		b, e := json.Marshal(p)
		if e != nil {
			return "", e
		}
		records = append(records, "presence:"+string(b))
	}
	state, err := fullDurableStateRecords(ctx, db)
	if err != nil {
		return "", err
	}
	records = append(records, state...)
	relations, err := fullRelationRecords(ctx, db)
	if err != nil {
		return "", err
	}
	records = append(records, relations...)
	locations, err := fullLocationRecords(ctx, db)
	if err != nil {
		return "", err
	}
	records = append(records, locations...)
	sort.Strings(records)
	h := sha256.New()
	last := ""
	for _, r := range records {
		if r != last {
			io.WriteString(h, r)
			io.WriteString(h, "\n")
			last = r
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fullSnapshotMedia(ctx context.Context, db *sql.DB) ([]fullMedia, error) {
	rows, err := db.QueryContext(ctx, "SELECT sha,size,mime FROM media ORDER BY sha")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fullMedia
	for rows.Next() {
		if len(out) >= fullBundleEntries {
			return nil, errArchiveBudget
		}
		var m fullMedia
		if err = rows.Scan(&m.SHA, &m.Size, &m.MIME); err != nil {
			return nil, err
		}
		if !restoreSHA(m.SHA) || m.Size < 0 || m.Size > fullBundleLimit {
			return nil, errors.New("invalid source media inventory")
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (a *Archiver) lastConfirmedFullFingerprint(ctx context.Context) (string, error) {
	return a.confirmedFullFingerprint(ctx)
}

func (a *Archiver) BuildFullBackup(ctx context.Context, ifChanged bool) (result FullBackupArtifact, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.buildFullBackupLocked(ctx, ifChanged)
}

func (a *Archiver) buildFullBackupLocked(ctx context.Context, ifChanged bool) (result FullBackupArtifact, err error) {
	if err = a.initFullBackup(ctx); err != nil {
		return result, err
	}
	if _, err = a.Store.DB.ExecContext(ctx, fullBackupSchema); err != nil {
		return result, err
	}
	a.Store.mediaMu.Lock()
	defer a.Store.mediaMu.Unlock()
	dir := filepath.Join(a.Config.DataDir, "backups")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return result, err
	}
	work, err := os.MkdirTemp(dir, ".full-build-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(work)
	snapshotPath := filepath.Join(work, "jardulik.sqlite")
	if err = a.Store.Snapshot(ctx, snapshotPath); err != nil {
		return result, err
	}
	db, err := openFullSnapshot(snapshotPath)
	if err != nil {
		return result, err
	}
	defer db.Close()
	result.SourceFingerprint, err = fullSourceFingerprint(ctx, db)
	if err != nil {
		return result, err
	}
	if ifChanged {
		previous, e := a.lastConfirmedFullFingerprint(ctx)
		if e != nil {
			return result, e
		}
		if previous == result.SourceFingerprint {
			result.Unchanged = true
			return result, nil
		}
	}
	recoveryBytes, err := json.Marshal(a.fullRecoveryConfig())
	if err != nil {
		return result, err
	}
	result.RecoveryFingerprint = digest(recoveryBytes)
	if reusable, ok, reuseErr := a.reusableFullArtifact(ctx, result.SourceFingerprint, result.RecoveryFingerprint); reuseErr != nil {
		return result, reuseErr
	} else if ok {
		reusable.Reused = true
		return reusable, nil
	}
	path, manifest, err := a.fullZIPFromSnapshot(ctx, db, snapshotPath, work, result.SourceFingerprint)
	if err != nil {
		return result, err
	}
	defer os.Remove(path)
	result.SourceSHA, _, err = fileDigest(path)
	if err != nil {
		return result, err
	}
	result.Path = path + ".enc"
	if err = sealFullBackupFile(path, result.Path, a.Config.BackupKey); err != nil {
		return result, err
	}
	keep := false
	defer func() {
		if !keep {
			os.Remove(result.Path)
		}
	}()
	result.EncryptedSHA, result.ByteSize, err = fileDigest(result.Path)
	if err != nil {
		return result, err
	}
	if result.ByteSize > fullBundleLimit {
		return result, errArchiveBudget
	}
	result.Profiles = len(manifest.People)
	result.MediaFiles = len(manifest.Media)
	for _, p := range manifest.People {
		result.Versions += p.Versions
	}
	for _, m := range manifest.Media {
		result.MediaBytes += m.Size
	}
	result.HostedUploadLimitExceeded = result.ByteSize > 50_000_000
	artifactJSON, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	_, err = a.Store.DB.ExecContext(ctx, `INSERT INTO full_backup_artifacts(source_fingerprint,source_sha,encrypted_sha,path,byte_size,artifact_json) VALUES(?,?,?,?,?,?)`, result.SourceFingerprint, result.SourceSHA, result.EncryptedSHA, result.Path, result.ByteSize, string(artifactJSON))
	if err != nil {
		return result, err
	}
	keep = true
	return result, nil
}

func (a *Archiver) fullZIPFromSnapshot(ctx context.Context, db *sql.DB, snapshotPath, work, fingerprint string) (path string, manifest fullManifest, err error) {
	manifest = fullManifest{Format: fullBundleFormat, CapturedAt: nowStamp(), AppVersion: Version, SourceFingerprint: fingerprint, Coverage: map[string]any{"telegram_server_bytes_verified": false, "media_embedded": true}}
	manifest.Media, err = fullSnapshotMedia(ctx, db)
	if err != nil {
		return "", manifest, err
	}
	var mediaBytes int64
	inventory := make(map[string]fullMedia, len(manifest.Media))
	for _, m := range manifest.Media {
		inventory[m.SHA] = m
		mediaBytes += m.Size
	}
	var estimate int64
	err = db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size),0) FROM (SELECT DISTINCT v.profile_id,j.sha,m.size FROM media_jobs j JOIN versions v ON v.id=j.version_id JOIN media m ON m.sha=j.sha WHERE j.state='complete')`).Scan(&estimate)
	if err != nil {
		return "", manifest, err
	}
	info, err := os.Stat(snapshotPath)
	if err != nil {
		return "", manifest, err
	}
	estimate += mediaBytes + info.Size() + serviceManifestLimit
	if estimate > fullBundleLimit {
		return "", manifest, errArchiveBudget
	}
	free, err := FreeBytes(a.Config.DataDir)
	if err != nil {
		return "", manifest, err
	}
	if free < a.Config.MinFreeBytes+uint64(estimate)*3 {
		return "", manifest, errors.New("insufficient reserved disk for whole full backup")
	}
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT profile_id FROM versions ORDER BY profile_id")
	if err != nil {
		return "", manifest, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return "", manifest, err
		}
		if id <= 0 || len(ids) >= fullBundleEntries {
			rows.Close()
			return "", manifest, errArchiveBudget
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", manifest, err
	}
	f, err := os.CreateTemp(filepath.Join(a.Config.DataDir, "backups"), a.now().UTC().Format("20060102T150405Z")+fmt.Sprintf("__jardulik-full__%d-profiles-*.zip", len(ids)))
	if err != nil {
		return "", manifest, err
	}
	path = f.Name()
	success := false
	defer func() {
		f.Close()
		if !success {
			os.Remove(path)
		}
	}()
	z := zip.NewWriter(f)
	add := func(e archiveFile, eerr error) error {
		if eerr == nil {
			manifest.Files = append(manifest.Files, e)
		}
		return eerr
	}
	if err = add(addZipFile(z, "jardulik.sqlite", snapshotPath)); err != nil {
		return path, manifest, err
	}
	recovery := a.fullRecoveryConfig()
	b, err := json.MarshalIndent(recovery, "", "  ")
	if err != nil {
		return path, manifest, err
	}
	if err = add(addZipBytes(z, "recovery-config.json", b)); err != nil {
		return path, manifest, err
	}
	if sessionInfo, statErr := os.Lstat(a.Config.transferSessionPath()); statErr == nil {
		if !sessionInfo.Mode().IsRegular() || sessionInfo.Size() > 1<<20 {
			return path, manifest, errors.New("invalid encrypted transfer session")
		}
		if err = add(addZipFile(z, "telegram-bot-session.enc", a.Config.transferSessionPath())); err != nil {
			return path, manifest, err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return path, manifest, errors.New("encrypted transfer session unavailable")
	}
	embedded := map[string]bool{}
	var index strings.Builder
	index.WriteString("Jardulik | один ZIP на анкету | все сохранённые версии и исходные фото\n")
	for _, id := range ids {
		if err = ctx.Err(); err != nil {
			return path, manifest, err
		}
		personPath, person, line, e := a.personZIPFromSnapshot(ctx, db, id, work, inventory, embedded)
		if e != nil {
			return path, manifest, e
		}
		person.Filename = fmt.Sprintf("profiles/jardulik-%d.zip", id)
		entry, e := addStoredZipFile(z, person.Filename, personPath)
		os.Remove(personPath)
		if e != nil {
			return path, manifest, e
		}
		manifest.Files = append(manifest.Files, entry)
		manifest.People = append(manifest.People, person)
		fmt.Fprintf(&index, "%s | %s | SHA-256 %s\n", line, person.Filename, entry.SHA256)
		if index.Len() > serviceManifestLimit {
			return path, manifest, errArchiveBudget
		}
	}
	for _, m := range manifest.Media {
		if embedded[m.SHA] {
			continue
		}
		source, e := a.mediaSourcePath(m.SHA)
		if e != nil {
			return path, manifest, e
		}
		entry, e := addZipFile(z, archiveMediaName(m.SHA, m.MIME), source)
		if e != nil {
			return path, manifest, e
		}
		if entry.SHA256 != m.SHA || entry.ByteSize != m.Size {
			return path, manifest, errors.New("source media hash or size mismatch")
		}
		manifest.Files = append(manifest.Files, entry)
	}
	if err = add(addZipBytes(z, "index.txt", []byte(index.String()))); err != nil {
		return path, manifest, err
	}
	var pending, failed int64
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM media_jobs WHERE state='pending'").Scan(&pending); err != nil {
		return path, manifest, err
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM media_jobs WHERE state='error'").Scan(&failed); err != nil {
		return path, manifest, err
	}
	manifest.Coverage["pending_media"] = pending
	manifest.Coverage["failed_media"] = failed
	manifest.Coverage["source_complete"] = pending == 0 && failed == 0
	manifest.Coverage["local_archive_complete"] = true
	manifest.Coverage["original_media_bytes"] = mediaBytes
	b, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return path, manifest, err
	}
	if len(b) > serviceManifestLimit {
		return path, manifest, errArchiveBudget
	}
	if _, err = addZipBytes(z, "manifest.json", b); err != nil {
		return path, manifest, err
	}
	if err = z.Close(); err != nil {
		return path, manifest, err
	}
	if err = f.Sync(); err != nil {
		return path, manifest, err
	}
	if err = f.Close(); err != nil {
		return path, manifest, err
	}
	info, err = os.Stat(path)
	if err != nil {
		return path, manifest, err
	}
	if info.Size() > fullBundleLimit {
		return path, manifest, errArchiveBudget
	}
	success = true
	return path, manifest, nil
}

func (a *Archiver) personZIPFromSnapshot(ctx context.Context, db *sql.DB, id int64, work string, inventory map[string]fullMedia, embedded map[string]bool) (path string, person fullPerson, line string, err error) {
	person.ProfileID = id
	f, err := os.CreateTemp(work, ".person-*.zip")
	if err != nil {
		return "", person, "", err
	}
	path = f.Name()
	success := false
	defer func() {
		f.Close()
		if !success {
			os.Remove(path)
		}
	}()
	z := zip.NewWriter(f)
	manifest := personManifest{Format: personBundleFormat, ProfileID: id}
	seen := map[string]bool{}
	rows, err := db.QueryContext(ctx, "SELECT id,owner_id,raw_json,first_seen,last_seen FROM versions WHERE profile_id=? ORDER BY id", id)
	if err != nil {
		return path, person, "", err
	}
	defer rows.Close()
	var latest Profile
	for rows.Next() {
		if len(manifest.Files) >= fullBundleEntries {
			return path, person, "", errArchiveBudget
		}
		var version personVersion
		var raw string
		if err = rows.Scan(&version.VersionID, &version.ObserverID, &raw, &version.FirstSeen, &version.LastSeen); err != nil {
			return path, person, "", err
		}
		if len(raw) > serviceManifestLimit {
			return path, person, "", errArchiveBudget
		}
		latest = nil
		if err = json.Unmarshal([]byte(raw), &latest); err != nil || latest.ID() != id {
			return path, person, "", errors.New("source version belongs to another profile")
		}
		version.RawFile = fmt.Sprintf("versions/v%d.json", version.VersionID)
		entry, e := addZipBytes(z, version.RawFile, []byte(raw))
		if e != nil {
			return path, person, "", e
		}
		manifest.Files = append(manifest.Files, entry)
		jobs, e := fullSnapshotJobs(ctx, db, version.VersionID)
		if e != nil {
			return path, person, "", e
		}
		for _, j := range jobs {
			version.Media = append(version.Media, archiveMedia{URL: j.URL, OriginalName: sourceFilename(j.URL), SHA256: j.SHA, Role: j.Role, Position: j.Position, State: j.State, Size: j.Size})
			if j.State != "complete" || j.SHA == "" {
				continue
			}
			m, ok := inventory[j.SHA]
			if !ok || m.Size != j.Size {
				return path, person, "", errors.New("version media absent from snapshot inventory")
			}
			if seen[j.SHA] {
				continue
			}
			seen[j.SHA] = true
			embedded[j.SHA] = true
			source, e := a.mediaSourcePath(j.SHA)
			if e != nil {
				return path, person, "", e
			}
			entry, e := addZipFile(z, archiveMediaName(j.SHA, m.MIME), source)
			if e != nil {
				return path, person, "", e
			}
			if entry.SHA256 != j.SHA || entry.ByteSize != m.Size {
				return path, person, "", errors.New("source media hash or size mismatch")
			}
			manifest.Files = append(manifest.Files, entry)
		}
		observations, e := db.QueryContext(ctx, "SELECT source,observed_at FROM observations WHERE owner_id=? AND version_id=? ORDER BY id", version.ObserverID, version.VersionID)
		if e != nil {
			return path, person, "", e
		}
		for observations.Next() {
			var o archiveObservation
			if e = observations.Scan(&o.Source, &o.At); e != nil {
				observations.Close()
				return path, person, "", e
			}
			version.Observations = append(version.Observations, o)
			if len(version.Observations) >= fullBundleEntries {
				observations.Close()
				return path, person, "", errArchiveBudget
			}
		}
		e = observations.Err()
		observations.Close()
		if e != nil {
			return path, person, "", e
		}
		manifest.Versions = append(manifest.Versions, version)
	}
	if err = rows.Err(); err != nil {
		return path, person, "", err
	}
	rows.Close()
	manifest.Presence, err = fullSnapshotPresence(ctx, db, id)
	if err != nil {
		return path, person, "", err
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return path, person, "", err
	}
	if len(b) > serviceManifestLimit {
		return path, person, "", errArchiveBudget
	}
	if _, err = addZipBytes(z, "manifest.json", b); err != nil {
		return path, person, "", err
	}
	if err = z.Close(); err != nil {
		return path, person, "", err
	}
	if err = f.Close(); err != nil {
		return path, person, "", err
	}
	person.Versions = len(manifest.Versions)
	person.MediaFiles = len(seen)
	brief := func(key string) string { return strings.Join(strings.Fields(latest.String(key)), " ") }
	line = fmt.Sprintf("#%d | %s | возраст %s | рост %s | %s | версий %d | фото %d", id, brief("name"), brief("age"), brief("height"), brief("city"), person.Versions, person.MediaFiles)
	success = true
	return path, person, line, nil
}

func addStoredZipFile(z *zip.Writer, name, path string) (archiveFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return archiveFile{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return archiveFile{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > fullBundleLimit {
		return archiveFile{}, errArchiveBudget
	}
	h := &zip.FileHeader{Name: name, Method: zip.Store}
	h.SetMode(0600)
	w, err := z.CreateHeader(h)
	if err != nil {
		return archiveFile{}, err
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, hash), f)
	if err != nil {
		return archiveFile{}, err
	}
	if n != info.Size() {
		return archiveFile{}, errors.New("nested ZIP changed during copying")
	}
	return archiveFile{Name: name, SHA256: hex.EncodeToString(hash.Sum(nil)), ByteSize: n}, nil
}

func fullProfileFilename(id int64) string { return fmt.Sprintf("profiles/jardulik-%d.zip", id) }
func fullProfileID(name string) (int64, bool) {
	if !strings.HasPrefix(name, "profiles/jardulik-") || !strings.HasSuffix(name, ".zip") {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "profiles/jardulik-"), ".zip"), 10, 64)
	return id, err == nil && id > 0 && fullProfileFilename(id) == name
}

func fullZipEntries(z *zip.Reader, allowed func(string) bool) (map[string]*zip.File, error) {
	if len(z.File) > fullBundleEntries {
		return nil, errArchiveBudget
	}
	entries := map[string]*zip.File{}
	var total uint64
	for _, f := range z.File {
		name := f.Name
		if !filepath.IsLocal(name) || filepath.Clean(name) != name || strings.Contains(name, "\\") || !f.Mode().IsRegular() || !allowed(name) || entries[name] != nil {
			return nil, errors.New("unsafe, unexpected or duplicate full archive entry")
		}
		if f.UncompressedSize64 > uint64(fullBundleRestoreLimit) || total > uint64(fullBundleRestoreLimit)-f.UncompressedSize64 {
			return nil, errArchiveBudget
		}
		total += f.UncompressedSize64
		entries[name] = f
	}
	if entries["manifest.json"] == nil || entries["manifest.json"].UncompressedSize64 > serviceManifestLimit {
		return nil, errors.New("missing or oversized full archive manifest")
	}
	return entries, nil
}
func readFullJSON(f *zip.File, value any) error {
	if f == nil || f.UncompressedSize64 > serviceManifestLimit {
		return errArchiveBudget
	}
	r, err := f.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	decoder := json.NewDecoder(io.LimitReader(r, serviceManifestLimit+1))
	if err = decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data in full archive")
	}
	return nil
}
func fullExpectedFiles(entries map[string]*zip.File, files []archiveFile) (map[string]archiveFile, error) {
	expected := map[string]archiveFile{}
	for _, e := range files {
		f := entries[e.Name]
		if e.Name == "manifest.json" || f == nil || expected[e.Name].Name != "" || e.ByteSize < 0 || uint64(e.ByteSize) != f.UncompressedSize64 || !restoreSHA(e.SHA256) {
			return nil, errors.New("full archive manifest file mismatch")
		}
		expected[e.Name] = e
	}
	if len(expected) != len(entries)-1 {
		return nil, errors.New("full archive manifest does not cover every file")
	}
	return expected, nil
}
func preflightFullZIP(z *zip.Reader) (fullManifest, map[string]*zip.File, map[string]archiveFile, error) {
	var manifest fullManifest
	entries, err := fullZipEntries(z, func(name string) bool {
		if name == "manifest.json" || name == "jardulik.sqlite" || name == "recovery-config.json" || name == "index.txt" || name == "telegram-bot-session.enc" {
			return true
		}
		if _, ok := fullProfileID(name); ok {
			return true
		}
		_, err := archiveMediaSHA(name)
		return err == nil
	})
	if err != nil {
		return manifest, nil, nil, err
	}
	if err = readFullJSON(entries["manifest.json"], &manifest); err != nil {
		return manifest, nil, nil, err
	}
	if manifest.Format != fullBundleFormat || !restoreSHA(manifest.SourceFingerprint) {
		return manifest, nil, nil, errors.New("unsupported full archive manifest")
	}
	for _, name := range []string{"jardulik.sqlite", "recovery-config.json", "index.txt"} {
		if entries[name] == nil {
			return manifest, nil, nil, errors.New("incomplete full service archive")
		}
	}
	if entries["recovery-config.json"].UncompressedSize64 > serviceManifestLimit || entries["index.txt"].UncompressedSize64 > serviceManifestLimit {
		return manifest, nil, nil, errArchiveBudget
	}
	expected, err := fullExpectedFiles(entries, manifest.Files)
	if err != nil {
		return manifest, nil, nil, err
	}
	people := map[int64]bool{}
	for _, p := range manifest.People {
		id, ok := fullProfileID(p.Filename)
		if !ok || id != p.ProfileID || people[id] || p.Versions <= 0 || p.MediaFiles < 0 || entries[p.Filename] == nil {
			return manifest, nil, nil, errors.New("invalid or duplicate person ZIP inventory")
		}
		people[id] = true
	}
	media := map[string]bool{}
	for _, m := range manifest.Media {
		if !restoreSHA(m.SHA) || m.Size < 0 || m.Size > fullBundleLimit || media[m.SHA] {
			return manifest, nil, nil, errors.New("invalid full archive media inventory")
		}
		media[m.SHA] = true
	}
	rootMedia := map[string]bool{}
	for name := range entries {
		if id, ok := fullProfileID(name); ok {
			if !people[id] {
				return manifest, nil, nil, errors.New("unlisted person ZIP")
			}
			continue
		}
		if strings.HasPrefix(name, "media/") {
			sha, e := archiveMediaSHA(name)
			if e != nil || !media[sha] || rootMedia[sha] || expected[name].SHA256 != sha {
				return manifest, nil, nil, errors.New("unlisted or duplicate root media")
			}
			rootMedia[sha] = true
		}
	}
	return manifest, entries, expected, nil
}

func copyFullEntry(ctx context.Context, f *zip.File, path string, e archiveFile) error {
	in, err := f.Open()
	if err != nil {
		return err
	}
	defer in.Close()
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = verifyRestoreSource(ctx, in, out, e.SHA256, e.ByteSize)
	closeErr := out.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// Authentication finishes in a private temporary file before an extracted
// destination is published. Neither tampered files nor partial restores replace it.
func restoreFullArchive(input, dest string, key []byte) error {
	info, err := os.Lstat(input)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > fullBundleLimit+(1<<20) {
		return errors.New("invalid or oversized full archive")
	}
	work, err := os.MkdirTemp("", "jardulik-full-open-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	plain := filepath.Join(work, "full.zip")
	if err = openFullBackupFile(input, plain, key); err != nil {
		return err
	}
	z, err := zip.OpenReader(plain)
	if err != nil {
		return err
	}
	defer z.Close()
	return restoreFullZIP(context.Background(), &z.Reader, dest, fullBundleRestoreLimit)
}

func restoreFullZIP(ctx context.Context, z *zip.Reader, dest string, budget int64) error {
	manifest, entries, expected, err := preflightFullZIP(z)
	if err != nil {
		return err
	}
	if err = consumeFullZipBudget(entries, &budget); err != nil {
		return err
	}
	if info, e := os.Lstat(dest); e == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("restore destination is not an ordinary directory")
		}
		files, e := os.ReadDir(dest)
		if e != nil {
			return e
		}
		if len(files) != 0 {
			return errors.New("restore destination must be empty")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	parent := filepath.Dir(dest)
	if err = os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".jardulik-full-restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	for _, name := range []string{"jardulik.sqlite", "recovery-config.json", "index.txt"} {
		if err = copyFullEntry(ctx, entries[name], filepath.Join(stage, name), expected[name]); err != nil {
			return err
		}
	}
	if session := entries["telegram-bot-session.enc"]; session != nil {
		if session.UncompressedSize64 > 1<<20 {
			return errArchiveBudget
		}
		if err = copyFullEntry(ctx, session, filepath.Join(stage, "telegram-bot-session.enc"), expected[session.Name]); err != nil {
			return err
		}
	}
	// Store the manifest itself for provenance; its bytes were authenticated by v2.
	mr, err := entries["manifest.json"].Open()
	if err != nil {
		return err
	}
	mf, err := os.OpenFile(filepath.Join(stage, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		mr.Close()
		return err
	}
	_, err = io.Copy(mf, io.LimitReader(mr, serviceManifestLimit+1))
	mr.Close()
	closeErr := mf.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	db, err := openFullSnapshot(filepath.Join(stage, "jardulik.sqlite"))
	if err != nil {
		return err
	}
	defer db.Close()
	var status string
	if err = db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&status); err != nil {
		return err
	}
	if status != "ok" {
		return errors.New("restored SQLite integrity check failed")
	}
	actualMedia, err := fullSnapshotMedia(ctx, db)
	if err != nil {
		return err
	}
	left, _ := json.Marshal(actualMedia)
	right, _ := json.Marshal(manifest.Media)
	if string(left) != string(right) {
		return errors.New("full archive media inventory differs from SQLite")
	}
	inventory := map[string]fullMedia{}
	for _, m := range manifest.Media {
		inventory[m.SHA] = m
	}
	seenMedia := map[string]bool{}
	seenVersions := map[int64]bool{}
	for name, f := range entries {
		if !strings.HasPrefix(name, "media/") {
			continue
		}
		sha, e := archiveMediaSHA(name)
		if e != nil {
			return e
		}
		m := inventory[sha]
		if expected[name].ByteSize != m.Size {
			return errors.New("root media size differs from SQLite")
		}
		if err = copyFullEntry(ctx, f, filepath.Join(stage, "media", sha), expected[name]); err != nil {
			return err
		}
		seenMedia[sha] = true
	}
	for _, p := range manifest.People {
		if err = ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(stage, filepath.FromSlash(p.Filename))
		if err = copyFullEntry(ctx, entries[p.Filename], path, expected[p.Filename]); err != nil {
			return err
		}
		personZIP, e := zip.OpenReader(path)
		if e != nil {
			return e
		}
		e = restorePersonZIP(ctx, &personZIP.Reader, p, db, stage, inventory, seenMedia, seenVersions, &budget)
		personZIP.Close()
		if e != nil {
			return e
		}
	}
	var versionCount int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM versions").Scan(&versionCount); err != nil {
		return err
	}
	if len(seenVersions) != versionCount || len(seenMedia) != len(inventory) {
		return errors.New("full archive source coverage is incomplete")
	}
	fingerprint, err := fullSourceFingerprint(ctx, db)
	if err != nil {
		return err
	}
	if fingerprint != manifest.SourceFingerprint {
		return errors.New("restored source fingerprint differs")
	}
	// Close SQLite before publishing, including on platforms that lock open files.
	if err = db.Close(); err != nil {
		return err
	}
	return os.Rename(stage, dest)
}

func restorePersonZIP(ctx context.Context, z *zip.Reader, person fullPerson, db *sql.DB, stage string, inventory map[string]fullMedia, seenMedia map[string]bool, seenVersions map[int64]bool, budget *int64) error {
	entries, err := fullZipEntries(z, func(name string) bool {
		if name == "manifest.json" {
			return true
		}
		if strings.HasPrefix(name, "versions/v") && strings.HasSuffix(name, ".json") {
			id, e := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "versions/v"), ".json"), 10, 64)
			return e == nil && id > 0 && fmt.Sprintf("versions/v%d.json", id) == name
		}
		_, e := archiveMediaSHA(name)
		return e == nil
	})
	if err != nil {
		return err
	}
	if err = consumeFullZipBudget(entries, budget); err != nil {
		return err
	}
	var manifest personManifest
	if err = readFullJSON(entries["manifest.json"], &manifest); err != nil {
		return err
	}
	if manifest.Format != personBundleFormat || manifest.ProfileID != person.ProfileID || len(manifest.Versions) != person.Versions {
		return errors.New("person ZIP identity or version count mismatch")
	}
	expected, err := fullExpectedFiles(entries, manifest.Files)
	if err != nil {
		return err
	}
	ownMedia := map[string]bool{}
	rawNames := map[string]bool{}
	for name := range entries {
		if !strings.HasPrefix(name, "media/") {
			continue
		}
		sha, e := archiveMediaSHA(name)
		if e != nil {
			return e
		}
		m, ok := inventory[sha]
		if !ok || ownMedia[sha] || expected[name].SHA256 != sha || expected[name].ByteSize != m.Size {
			return errors.New("invalid or duplicate person media")
		}
		ownMedia[sha] = true
	}
	if len(ownMedia) != person.MediaFiles {
		return errors.New("person media count mismatch")
	}
	for _, v := range manifest.Versions {
		rawEntry := entries[v.RawFile]
		if v.VersionID <= 0 || v.ObserverID <= 0 || seenVersions[v.VersionID] || v.RawFile != fmt.Sprintf("versions/v%d.json", v.VersionID) || rawEntry == nil || rawEntry.UncompressedSize64 > serviceManifestLimit || rawNames[v.RawFile] {
			return errors.New("invalid or duplicate person version")
		}
		var owner, profile int64
		var raw string
		if err = db.QueryRowContext(ctx, "SELECT owner_id,profile_id,raw_json FROM versions WHERE id=?", v.VersionID).Scan(&owner, &profile, &raw); err != nil {
			return err
		}
		if owner != v.ObserverID || profile != person.ProfileID {
			return errors.New("person version observer provenance differs from SQLite")
		}
		r, e := rawEntry.Open()
		if e != nil {
			return e
		}
		h := sha256.New()
		var b strings.Builder
		n, e := io.Copy(io.MultiWriter(&b, h), io.LimitReader(restoreContextReader{ctx, r}, serviceManifestLimit+1))
		r.Close()
		if e != nil {
			return e
		}
		if n != expected[v.RawFile].ByteSize || hex.EncodeToString(h.Sum(nil)) != expected[v.RawFile].SHA256 || b.String() != raw {
			return errors.New("person source JSON differs from SQLite")
		}
		jobs, e := fullSnapshotJobs(ctx, db, v.VersionID)
		if e != nil {
			return e
		}
		for _, j := range jobs {
			if j.State == "complete" && j.SHA != "" && !ownMedia[j.SHA] {
				return errors.New("person ZIP omits a referenced source photo")
			}
		}
		seenVersions[v.VersionID] = true
		rawNames[v.RawFile] = true
	}
	for name, f := range entries {
		if strings.HasPrefix(name, "versions/") && !rawNames[name] {
			return errors.New("person ZIP has an unlisted source version")
		}
		if !strings.HasPrefix(name, "media/") {
			continue
		}
		sha, _ := archiveMediaSHA(name)
		if seenMedia[sha] {
			r, e := f.Open()
			if e != nil {
				return e
			}
			e = verifyRestoreSource(ctx, r, io.Discard, sha, inventory[sha].Size)
			r.Close()
			if e != nil {
				return e
			}
		} else {
			if err = copyFullEntry(ctx, f, filepath.Join(stage, "media", sha), expected[name]); err != nil {
				return err
			}
			seenMedia[sha] = true
		}
	}
	return nil
}

// Nested archives share one expansion budget, including the retained ZIPs.
func consumeFullZipBudget(entries map[string]*zip.File, remaining *int64) error {
	for _, f := range entries {
		if *remaining < 0 || f.UncompressedSize64 > uint64(*remaining) {
			return errArchiveBudget
		}
		*remaining -= int64(f.UncompressedSize64)
	}
	return nil
}

func fullDurableStateRecords(ctx context.Context, db *sql.DB) ([]string, error) {
	var records []string
	// These are existing tables. Draft filters, offers and account counters are excluded.
	queries := []struct{ table, prefix, query string }{
		{"users", "user", "SELECT id,paused FROM users"},
		{"deletions", "deletion", "SELECT owner_id,deleted_at FROM deletions"},
		{"sessions", "connection", "SELECT owner_id,upstream_id,state FROM sessions"},
		{"favorites", "favorite", "SELECT owner_id,profile_id,created_at FROM favorites"},
		{"actions", "reaction", `SELECT owner_id,profile_id,kind,state,response,created_at FROM actions WHERE state IN ('pending','succeeded','unknown','quota_waiting','quota_retried','fire_waiting')`},
		{"feed_filters", "filters", `SELECT owner_id,applied_json,include_others FROM feed_filters WHERE applied_json IS NOT NULL OR include_others<>0`},
		{"autolike_settings", "autolike", "SELECT owner_id,mode,interval_seconds,series_count,daily_cap FROM autolike_settings"},
		{"reaction_message_settings", "reaction-message-policy", "SELECT owner_id,mode,templates_json,telegram_username,revision FROM reaction_message_settings"},
		{"reaction_message_hold_resolutions", "reaction-message-hold-resolution", "SELECT draft_id,owner_id,upstream_id,profile_id,evidence_json,evidence_sha FROM reaction_message_hold_resolutions"},
		{"reaction_message_drafts", "reaction-message-draft", "SELECT owner_id,upstream_id,profile_id,telegram_username,content_hash,basis_hash,state,message,capture_json,capture_sha FROM reaction_message_drafts"},
		{"reaction_message_intents", "reaction-message-intent", "SELECT action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reserved_cursor,reaction_type,attempted_at_ms FROM reaction_message_intents"},
		{"profile_contact_evidence", "contact", "SELECT owner_id,upstream_id,profile_id,source_bot_id,source_peer_id,source_locator_kind,source_message_locator,source_message_timestamp,contact_url,profile_url,evidence_kind FROM profile_contact_evidence"},
		{"business_contact_connections", "business-connection", "SELECT owner_id,upstream_id,id,enabled FROM business_contact_connections"},
		{"profile_business_contacts", "business-contact", "SELECT DISTINCT owner_id,upstream_id,profile_id,source_bot_id,connection_id,source_message_id,source_message_timestamp,contact_url,profile_url FROM profile_business_contacts"},
		{"contact_requests", "contact-request", "SELECT owner_id,upstream_id,profile_id,state,request_json,response_body,http_status,reason FROM contact_requests"},
		{"profile_analyses", "analysis", "SELECT owner_id,upstream_id,profile_id,provider,model,observed_at_ms,http_status,state,generation,request_body,response_body,own_profile,target_profile,capture_sha FROM profile_analyses"},
		{"compatibility_pairs", "compatibility-pair", "SELECT owner_id,from_user_id,target_user_id,latest_analysis_id FROM compatibility_pairs"},
		{"compatibility_jobs", "compatibility-job", "SELECT pair_id,generation,version_id,own_hash,target_hash,input_hash,filter_hash,own_profile,target_profile,request_body,response_body,http_status,state,analysis_id,reason FROM compatibility_jobs"},
		{"debug_api_calls", "debug-api-call", "SELECT owner_id,request_id,upstream_id,generation,path,target_id,reaction_type,request_body,response_body,http_status,state,error,action_id FROM debug_api_calls"},
		{"business_contact_inbox", "business-contact-inbox", "SELECT owner_id,upstream_id,source_bot_id,connection_id,source_message_id,source_message_timestamp,update_id,parse_state,CAST(raw_json AS BLOB) FROM business_contact_inbox"},
	}
	for _, q := range queries {
		exists, err := snapshotHasTable(ctx, db, q.table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		if q.table == "reaction_message_settings" {
			q.query = "SELECT owner_id,mode,templates_json,telegram_username"
			for _, column := range []string{"prefer_drafts", "iron_mix", "super_before_age"} {
				var present int
				if err = db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_settings') WHERE name=?", column).Scan(&present); err != nil {
					return nil, err
				}
				if present > 0 {
					q.query += "," + column
				}
			}
			q.query += ",revision FROM reaction_message_settings"
		}
		if q.table == "reaction_message_intents" || q.table == "debug_api_calls" {
			if q.table == "reaction_message_intents" {
				var present int
				if err = db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_intents') WHERE name='attempted_at_ms'").Scan(&present); err != nil {
					return nil, err
				}
				if present == 0 {
					q.query = strings.Replace(q.query, ",attempted_at_ms FROM", ",0 AS attempted_at_ms FROM", 1)
				}
			}
			column := "reaction_type"
			if q.table == "debug_api_calls" {
				column = "action_id"
			}
			var present int
			if err = db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('"+q.table+"') WHERE name=?", column).Scan(&present); err != nil {
				return nil, err
			}
			if present == 0 {
				fallback := "1 AS reaction_type"
				if column == "action_id" {
					fallback = "NULL AS action_id"
				}
				q.query = strings.Replace(q.query, ","+column+",", ","+fallback+",", 1)
				if column == "action_id" {
					q.query = strings.Replace(q.query, ",action_id FROM", ",NULL AS action_id FROM", 1)
				}
			}
		}
		rows, err := db.QueryContext(ctx, q.query)
		if err != nil {
			return nil, err
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, err
		}
		count := 0
		for rows.Next() {
			count++
			if count > fullBundleEntries {
				rows.Close()
				return nil, errArchiveBudget
			}
			values := make([]any, len(columns))
			pointers := make([]any, len(values))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				return nil, err
			}
			for i, v := range values {
				text, ok := v.(string)
				if !ok {
					continue
				}
				if len(text) > serviceManifestLimit {
					rows.Close()
					return nil, errArchiveBudget
				}
				// Normalize valid JSON while retaining an unknown reaction response verbatim.
				var parsed any
				decoder := json.NewDecoder(strings.NewReader(text))
				decoder.UseNumber()
				if decoder.Decode(&parsed) == nil {
					var extra any
					if errors.Is(decoder.Decode(&extra), io.EOF) {
						values[i] = parsed
					}
				}
			}
			value, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				return nil, err
			}
			records = append(records, q.prefix+":"+string(value))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

func fullRelationRecords(ctx context.Context, db *sql.DB) ([]string, error) {
	exists, err := snapshotHasTable(ctx, db, "relation_batches")
	if err != nil || !exists {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT owner_id,mutual,source_page,raw_json FROM relation_batches ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []string
	heads := map[string]string{}
	count := 0
	for rows.Next() {
		count++
		if count > fullBundleEntries {
			return nil, errArchiveBudget
		}
		var owner int64
		var mutual bool
		var page int
		var raw string
		if err = rows.Scan(&owner, &mutual, &page, &raw); err != nil {
			return nil, err
		}
		if len(raw) > serviceManifestLimit {
			return nil, errArchiveBudget
		}
		profiles, err := decodeRelations([]byte(raw))
		if err != nil {
			return nil, err
		}
		normalized := make([]string, 0, len(profiles))
		for _, p := range profiles {
			for _, key := range []string{"distance", "reactionView", "observed_at", "last_seen", "observedAt", "lastSeen"} {
				delete(p, key)
			}
			value, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, string(value))
		}
		sort.Strings(normalized)
		hash := sha256.New()
		previous := ""
		for _, value := range normalized {
			if value != previous {
				io.WriteString(hash, value)
				io.WriteString(hash, "\n")
				previous = value
			}
		}
		fp := hex.EncodeToString(hash.Sum(nil))
		key := fmt.Sprintf("%d:%t:%d", owner, mutual, page)
		records = append(records, "relation-version:"+key+":"+fp)
		heads[key] = fp
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for key, fp := range heads {
		records = append(records, "relation-head:"+key+":"+fp)
	}
	return records, nil
}

func fullLocationRecords(ctx context.Context, db *sql.DB) ([]string, error) {
	exists, err := snapshotHasTable(ctx, db, "location_batches")
	if err != nil || !exists {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT owner_id,country,city,source_page,raw_json FROM location_batches ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []string
	heads := map[string]string{}
	count := 0
	for rows.Next() {
		count++
		if count > fullBundleEntries {
			return nil, errArchiveBudget
		}
		var owner int64
		var page int
		var country, city, raw string
		if err = rows.Scan(&owner, &country, &city, &page, &raw); err != nil {
			return nil, err
		}
		if len(raw) > serviceManifestLimit || len(country) > serviceManifestLimit || len(city) > serviceManifestLimit {
			return nil, errArchiveBudget
		}
		locations, err := decodeLocations([]byte(raw))
		if err != nil {
			return nil, err
		}
		normalized := make([]string, 0, len(locations.Entries))
		for _, entry := range locations.Entries {
			p, err := decodeProfile(entry.Raw)
			if err != nil {
				return nil, err
			}
			// Keep preview content while excluding map rendering and scan counters.
			for _, key := range []string{"distance", "line", "randomAngle", "totalCount", "pageCount"} {
				delete(p, key)
			}
			value, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, string(value))
		}
		sort.Strings(normalized)
		hash := sha256.New()
		previous := ""
		for _, value := range normalized {
			if value != previous {
				io.WriteString(hash, value)
				io.WriteString(hash, "\n")
				previous = value
			}
		}
		fp := hex.EncodeToString(hash.Sum(nil))
		key, err := json.Marshal([]any{owner, country, city, page})
		if err != nil {
			return nil, err
		}
		records = append(records, "location-version:"+string(key)+":"+fp)
		heads[string(key)] = fp
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for key, fp := range heads {
		records = append(records, "location-head:"+key+":"+fp)
	}
	return records, nil
}

func (a *Archiver) fullRecoveryConfig() map[string]any {
	return map[string]any{"APP_DATA_DIR": a.Config.DataDir, "TELEGRAM_BOT_TOKEN": a.Config.Token, "OWNER_TELEGRAM_ID": a.Config.OwnerID, "JARDULIK_BOT_ID": a.Config.SourceBotID, "BACKUP_CHAT_ID": a.Config.BackupChatID, "BACKUP_THREAD_ID": a.Config.BackupThreadID, "SESSION_KEY": base64.StdEncoding.EncodeToString(a.Config.SessionKey), "ALLOW_REACTIONS": false, "MIN_FREE_BYTES": a.Config.MinFreeBytes, "JARDULIK_USER_AGENT": a.Config.UserAgent,
		"TELEGRAM_API_ID": a.Config.TelegramAPIID, "TELEGRAM_API_HASH": a.Config.TelegramAPIHash, "TELEGRAM_TRANSFER_HELPER": a.Config.TransferHelper, "TELEGRAM_TRANSFER_SESSION_FILE": a.Config.transferSessionPath(), "TELEGRAM_TRANSFER_BYTES_PER_SECOND": a.Config.TransferBytesPerSecond, "BACKUP_TRANSPORT": "mtproto", "FULL_BACKUP_TRANSFER_ENABLED": false}
}

func (a *Archiver) reusableFullArtifact(ctx context.Context, fp, recovery string) (FullBackupArtifact, bool, error) {
	var result FullBackupArtifact
	rows, err := a.Store.DB.QueryContext(ctx, `SELECT artifact_json FROM full_backup_artifacts f WHERE source_fingerprint=? AND artifact_json<>''
 AND NOT EXISTS(SELECT 1 FROM full_backup_jobs j WHERE j.encrypted_sha=f.encrypted_sha AND j.state='sent') ORDER BY id DESC LIMIT 100`, fp)
	if err != nil {
		return result, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return result, false, err
		}
		if len(raw) > transferOutputLimit || json.Unmarshal([]byte(raw), &result) != nil || result.SourceFingerprint != fp || result.RecoveryFingerprint != recovery {
			continue
		}
		info, e := os.Lstat(result.Path)
		if e != nil || !info.Mode().IsRegular() || info.Size() != result.ByteSize {
			continue
		}
		sha, size, e := fileDigest(result.Path)
		if e == nil && sha == result.EncryptedSHA && size == result.ByteSize {
			return result, true, nil
		}
	}
	return FullBackupArtifact{}, false, rows.Err()
}
