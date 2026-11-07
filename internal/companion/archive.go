package companion

import (
	"archive/zip"
	"bytes"
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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const serviceBackupLimit = 18 << 20
const serviceRestoreLimit = 128 << 20
const serviceManifestLimit = 8 << 20
const serviceArchiveEntries = 10003
const serviceBackupMagic = "JDCBK1\n"
const serviceBackupPurpose = "jardulik-service-backup:v1"

var errArchiveBudget = errors.New("service archive exceeds restore budget")

// Archiver is the sole sender to the private archive group. Topics do not
// isolate users: no service customer is invited to this operator-only group.
type Archiver struct {
	Config Config
	Store  *Store
	TG     *Telegram
	Media  *MediaManager
	mu     sync.Mutex
	now    func() time.Time
}

func NewArchiver(c Config, s *Store, tg *Telegram, m *MediaManager) *Archiver {
	return &Archiver{Config: c, Store: s, TG: tg, Media: m, now: time.Now}
}

const archiveSchema = `
CREATE TABLE IF NOT EXISTS archive_topic_jobs (
 profile_id INTEGER PRIMARY KEY, state TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS archive_versions (
 version_id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, state TEXT NOT NULL, thread_id INTEGER NOT NULL DEFAULT 0,
 message_ids TEXT NOT NULL DEFAULT '[]', source_sha TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS archive_profile_repairs (
 version_id INTEGER PRIMARY KEY, state TEXT NOT NULL, old_message_ids TEXT NOT NULL,
 new_message_ids TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS archive_backups (
 id INTEGER PRIMARY KEY, requested_at TEXT NOT NULL, state TEXT NOT NULL, path TEXT NOT NULL DEFAULT '',
 source_sha TEXT NOT NULL DEFAULT '', encrypted_sha TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
 message_id INTEGER NOT NULL DEFAULT 0, file_id TEXT NOT NULL DEFAULT '', companion_message_id INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS archive_receipts (
 source_sha TEXT PRIMARY KEY, kind TEXT NOT NULL, filename TEXT NOT NULL, byte_size INTEGER NOT NULL,
 encrypted_sha TEXT NOT NULL DEFAULT '', chat_id INTEGER NOT NULL, thread_id INTEGER NOT NULL,
 message_id INTEGER NOT NULL, file_id TEXT NOT NULL, file_unique_id TEXT NOT NULL,
 returned_file_size INTEGER NOT NULL, sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

func (a *Archiver) init(ctx context.Context) error {
	if _, err := a.Store.DB.ExecContext(ctx, archiveSchema); err != nil {
		return err
	}
	rows, err := a.Store.DB.QueryContext(ctx, "PRAGMA table_info(archive_backups)")
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
		if name == "companion_message_id" {
			found = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		_, err = a.Store.DB.ExecContext(ctx, "ALTER TABLE archive_backups ADD COLUMN companion_message_id INTEGER NOT NULL DEFAULT 0")
	}
	return err
}

func (a *Archiver) RequestBackup(ctx context.Context) error {
	if err := a.init(ctx); err != nil {
		return err
	}
	_, err := a.Store.DB.ExecContext(ctx, "INSERT INTO archive_backups(requested_at,state) VALUES(?,'pending')", a.now().UTC().Format(time.RFC3339Nano))
	return err
}

func (a *Archiver) BackupStatus(ctx context.Context) string {
	if err := a.init(ctx); err != nil {
		return "Ошибка состояния резервного копирования"
	}
	var state, created, detail string
	if err := a.Store.DB.QueryRowContext(ctx, "SELECT state,created_at,error FROM archive_backups ORDER BY id DESC LIMIT 1").Scan(&state, &created, &detail); err != nil {
		return "Бэкап ещё не запрашивался"
	}
	if detail != "" {
		return fmt.Sprintf("Бэкап: %s, %s (%s)", state, displayTime(created), detail)
	}
	return fmt.Sprintf("Бэкап: %s, %s", state, displayTime(created))
}

func (a *Archiver) Run(ctx context.Context) {
	if err := a.init(ctx); err != nil {
		return
	}
	lock, err := os.OpenFile(filepath.Join(a.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	// Processing means an external effect might have happened before a crash.
	// Only an operator can reconcile those records; never replay them blindly.
	_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_topic_jobs SET state='uncertain',error='worker_restarted_during_request' WHERE state='processing'")
	_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state='uncertain',error='worker_restarted_during_request' WHERE state='processing'")
	_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state='uncertain',error='worker_restarted_during_request' WHERE state='processing'")
	_, _ = a.Store.DB.ExecContext(ctx, "UPDATE meta SET value='uncertain' WHERE key='archive_repair_requested' AND value='processing'")
	for ctx.Err() == nil {
		a.mu.Lock()
		err = a.processOne(ctx)
		a.mu.Unlock()
		if err != nil {
			_ = a.Store.SetMeta(ctx, "archive_last_error", archiveErrorCode(err))
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func archiveErrorCode(err error) string {
	var te *TelegramError
	if errors.As(err, &te) {
		return fmt.Sprintf("telegram_%d", te.Code)
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "archive_operation_failed"
}

func archiveFailureState(err error) string {
	var te *TelegramError
	if errors.As(err, &te) {
		if te.Code == 429 {
			return "pending"
		}
		return "error"
	}
	return "uncertain"
}

func (a *Archiver) groupSlot(ctx context.Context) error {
	if raw := a.Store.Meta(ctx, "archive_next_send_ns"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			if delay := time.Unix(0, n).Sub(a.now()); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
	return a.Store.SetMeta(ctx, "archive_next_send_ns", strconv.FormatInt(a.now().Add(4*time.Second).UnixNano(), 10))
}

func (a *Archiver) groupCall(ctx context.Context, call func() error) error {
	for attempt := 0; attempt < 3; attempt++ {
		if err := a.groupSlot(ctx); err != nil {
			return err
		}
		err := call()
		if err == nil {
			return nil
		}
		var te *TelegramError
		if !errors.As(err, &te) || te.Code != 429 {
			return err
		}
		wait := time.Duration(te.RetryAfter) * time.Second
		if wait < 4*time.Second {
			wait = 4 * time.Second
		}
		if err = a.Store.SetMeta(ctx, "archive_next_send_ns", strconv.FormatInt(a.now().Add(wait).UnixNano(), 10)); err != nil {
			return err
		}
		if attempt == 2 {
			return te
		}
	}
	return errors.New("archive retry limit")
}

func (a *Archiver) processOne(ctx context.Context) error {
	if handled, err := a.processQueuedRepair(ctx); handled || err != nil {
		return err
	}
	if a.Config.BackupChatID == 0 || a.Config.BackupThreadID <= 0 {
		return nil
	}
	var id int64
	err := a.Store.DB.QueryRowContext(ctx, "SELECT id FROM archive_backups WHERE state='pending' ORDER BY id LIMIT 1").Scan(&id)
	if err == nil {
		return a.processBackup(ctx, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var owner, version int64
	err = a.Store.DB.QueryRowContext(ctx, `SELECT v.owner_id,v.id FROM versions v
 LEFT JOIN archive_versions av ON av.version_id=v.id
 WHERE (av.version_id IS NULL OR av.state='pending')
 AND NOT EXISTS(SELECT 1 FROM media_jobs j WHERE j.version_id=v.id AND j.state='pending')
 ORDER BY v.id LIMIT 1`).Scan(&owner, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return a.mirrorVersion(ctx, owner, version)
}

// Distances and reactions belong to the observer, not the person's identity.
// The raw source is still preserved in each ZIP; the moderation topic suppresses
// duplicate content when only these viewer-specific values changed.
func mirrorFingerprint(p Profile, jobs []MediaJob) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	var stable map[string]any
	if err = json.Unmarshal(b, &stable); err != nil {
		return "", err
	}
	for _, key := range []string{"distance", "reactionView", "isFire", "fireText"} {
		delete(stable, key)
	}
	delete(stable, "photos")
	delete(stable, "miniPhotos")
	type mediaIdentity struct {
		Role     string `json:"role"`
		Position int    `json:"position"`
		Source   string `json:"source"`
	}
	var media []mediaIdentity
	for _, j := range jobs {
		source := j.SHA
		if source == "" {
			source = "unavailable:" + j.URL
		}
		media = append(media, mediaIdentity{Role: j.Role, Position: j.Position, Source: source})
	}
	sort.Slice(media, func(i, j int) bool {
		if media[i].Role != media[j].Role {
			return media[i].Role < media[j].Role
		}
		if media[i].Position != media[j].Position {
			return media[i].Position < media[j].Position
		}
		return media[i].Source < media[j].Source
	})
	stable["archived_photo_sources"] = media
	b, err = json.Marshal(stable)
	if err != nil {
		return "", err
	}
	return digest(b), nil
}

func (a *Archiver) topic(ctx context.Context, p StoredProfile) (int64, error) {
	var thread int64
	if err := a.Store.DB.QueryRowContext(ctx, "SELECT thread_id FROM mirror_topics WHERE profile_id=?", p.ID).Scan(&thread); err == nil {
		return thread, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	var state string
	err := a.Store.DB.QueryRowContext(ctx, "SELECT state FROM archive_topic_jobs WHERE profile_id=?", p.ID).Scan(&state)
	if err == nil && state != "pending" {
		return 0, errors.New("topic creation requires operator reconciliation")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if _, err = a.Store.DB.ExecContext(ctx, "INSERT INTO archive_topic_jobs(profile_id,state) VALUES(?,'processing') ON CONFLICT(profile_id) DO UPDATE SET state='processing',updated_at=CURRENT_TIMESTAMP", p.ID); err != nil {
		return 0, err
	}
	name := fmt.Sprintf("Jardulik %d: %s", p.ID, p.Profile.String("name"))
	if age := p.Profile.String("age"); age != "" {
		name += ", " + age
	}
	name = truncateRunes(name, 128)
	var result struct {
		Thread int64 `json:"message_thread_id"`
	}
	err = a.groupCall(ctx, func() error {
		return a.TG.Call(ctx, "createForumTopic", map[string]any{"chat_id": a.Config.BackupChatID, "name": name}, &result)
	})
	if err != nil {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_topic_jobs SET state=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE profile_id=?", archiveFailureState(err), archiveErrorCode(err), p.ID)
		return 0, err
	}
	if result.Thread <= 0 {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_topic_jobs SET state='uncertain',error='missing_thread_id' WHERE profile_id=?", p.ID)
		return 0, errors.New("missing Telegram thread id")
	}
	tx, err := a.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO mirror_topics(profile_id,thread_id) VALUES(?,?)", p.ID, result.Thread); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE archive_topic_jobs SET state='sent',error='' WHERE profile_id=?", p.ID); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return result.Thread, nil
}

func truncateRunes(v string, n int) string {
	// Telegram's entity offsets use UTF-16. Keeping both code units and rune
	// count within the bound also handles biographies containing many emoji.
	units := 0
	for offset, r := range v {
		width := 1
		if r > 0xffff {
			width = 2
		}
		if units+width > n {
			return v[:offset]
		}
		units += width
	}
	return v
}

type archiveFile struct {
	Name     string `json:"name"`
	SHA256   string `json:"sha256"`
	ByteSize int64  `json:"byte_size"`
}
type archiveManifest struct {
	Format        string               `json:"format"`
	CapturedAt    string               `json:"captured_at"`
	CaptureDevice string               `json:"capture_device_ref"`
	OS            string               `json:"os"`
	OSRelease     string               `json:"os_release"`
	Kernel        string               `json:"kernel_release"`
	Hardware      string               `json:"hardware_model"`
	Architecture  string               `json:"architecture"`
	AppVersion    string               `json:"app_version"`
	ProfileID     int64                `json:"profile_id,omitempty"`
	VersionID     int64                `json:"version_id,omitempty"`
	ObserverID    int64                `json:"observer_id,omitempty"`
	FirstSeen     string               `json:"first_seen,omitempty"`
	LastSeen      string               `json:"last_seen,omitempty"`
	Files         []archiveFile        `json:"files"`
	Media         []archiveMedia       `json:"media,omitempty"`
	Receipts      []archiveReceipt     `json:"telegram_receipts,omitempty"`
	Observations  []archiveObservation `json:"observations,omitempty"`
	Coverage      map[string]any       `json:"coverage"`
}
type archiveMedia struct {
	URL          string `json:"source_url"`
	OriginalName string `json:"original_filename,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
	Role         string `json:"role"`
	Position     int    `json:"position"`
	State        string `json:"state"`
	Size         int64  `json:"byte_size"`
}
type archiveObservation struct {
	Source string `json:"source"`
	At     string `json:"observed_at"`
}
type archiveReceipt struct {
	SourceSHA    string `json:"source_sha256"`
	Kind         string `json:"kind"`
	Name         string `json:"filename"`
	Size         int64  `json:"byte_size"`
	EncryptedSHA string `json:"encrypted_sha256,omitempty"`
	ChatID       int64  `json:"chat_id"`
	ThreadID     int64  `json:"topic_id"`
	MessageID    int64  `json:"message_id"`
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	ReturnedSize int64  `json:"returned_file_size"`
	SentAt       string `json:"sent_at"`
}

func baseManifest() archiveManifest {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	readMetadata := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			return "unknown"
		}
		return truncateRunes(strings.TrimSpace(string(b)), 2048)
	}
	return archiveManifest{Format: "jardulik-companion-archive/v1", CapturedAt: nowStamp(), CaptureDevice: host, OS: runtime.GOOS, OSRelease: readMetadata("/etc/os-release"), Kernel: readMetadata("/proc/sys/kernel/osrelease"), Hardware: readMetadata("/sys/devices/virtual/dmi/id/product_name"), Architecture: runtime.GOARCH, AppVersion: Version, Coverage: map[string]any{"telegram_server_bytes_verified": false}}
}

func sourceFilename(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "unknown"
	}
	name := filepath.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		return "unknown"
	}
	return name
}

func (a *Archiver) mediaSourcePath(sha string) (string, error) {
	if len(sha) != 64 {
		return "", errors.New("invalid media hash")
	}
	if _, err := hex.DecodeString(sha); err != nil {
		return "", errors.New("invalid media hash")
	}
	return filepath.Join(a.Config.DataDir, "media", sha), nil
}

func addZipBytes(z *zip.Writer, name string, b []byte) (archiveFile, error) {
	w, err := z.Create(name)
	if err != nil {
		return archiveFile{}, err
	}
	if _, err = w.Write(b); err != nil {
		return archiveFile{}, err
	}
	return archiveFile{Name: name, SHA256: digest(b), ByteSize: int64(len(b))}, nil
}
func addZipFile(z *zip.Writer, name, path string) (archiveFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return archiveFile{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return archiveFile{}, err
	}
	if !info.Mode().IsRegular() {
		return archiveFile{}, errors.New("archive source is not a regular file")
	}
	w, err := z.Create(name)
	if err != nil {
		return archiveFile{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), f)
	if err != nil {
		return archiveFile{}, err
	}
	if n != info.Size() {
		return archiveFile{}, errors.New("archive source changed while reading")
	}
	return archiveFile{Name: name, SHA256: hex.EncodeToString(h.Sum(nil)), ByteSize: n}, nil
}

func (a *Archiver) profileZIP(ctx context.Context, p StoredProfile, owner int64, jobs []MediaJob) (string, string, archiveManifest, error) {
	manifest := baseManifest()
	manifest.ProfileID = p.ID
	manifest.VersionID = p.VersionID
	manifest.ObserverID = owner
	manifest.FirstSeen = p.FirstSeen
	manifest.LastSeen = p.LastSeen
	dir := filepath.Join(a.Config.DataDir, "archive-staging")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", "", manifest, err
	}
	f, err := os.CreateTemp(dir, fmt.Sprintf("jardulik-%d-v%d-*.zip", p.ID, p.VersionID))
	if err != nil {
		return "", "", manifest, err
	}
	path := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	z := zip.NewWriter(f)
	raw, err := json.MarshalIndent(p.Profile, "", "  ")
	if err != nil {
		return "", "", manifest, err
	}
	entry, err := addZipBytes(z, "raw.json", raw)
	if err != nil {
		return "", "", manifest, err
	}
	manifest.Files = append(manifest.Files, entry)
	rows, err := a.Store.DB.QueryContext(ctx, "SELECT source,observed_at FROM observations WHERE owner_id=? AND version_id=? ORDER BY id", owner, p.VersionID)
	if err != nil {
		return "", "", manifest, err
	}
	for rows.Next() {
		var o archiveObservation
		if err = rows.Scan(&o.Source, &o.At); err != nil {
			rows.Close()
			return "", "", manifest, err
		}
		manifest.Observations = append(manifest.Observations, o)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", "", manifest, err
	}
	seen := map[string]bool{}
	missing := 0
	for _, j := range jobs {
		manifest.Media = append(manifest.Media, archiveMedia{URL: j.URL, OriginalName: sourceFilename(j.URL), SHA256: j.SHA, Role: j.Role, Position: j.Position, State: j.State, Size: j.Size})
		if j.State != "complete" || j.SHA == "" {
			missing++
			continue
		}
		if seen[j.SHA] {
			continue
		}
		seen[j.SHA] = true
		mediaPath, e := a.mediaSourcePath(j.SHA)
		if e != nil {
			return "", "", manifest, e
		}
		entry, err = addZipFile(z, "media/"+j.SHA, mediaPath)
		if err != nil {
			return "", "", manifest, err
		}
		if entry.SHA256 != j.SHA {
			return "", "", manifest, errors.New("source media hash mismatch")
		}
		manifest.Files = append(manifest.Files, entry)
	}
	manifest.Coverage["missing_or_failed_media"] = missing
	manifest.Coverage["source_complete"] = missing == 0
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", manifest, err
	}
	if _, err = addZipBytes(z, "manifest.json", b); err != nil {
		return "", "", manifest, err
	}
	if err = z.Close(); err != nil {
		return "", "", manifest, err
	}
	if err = f.Close(); err != nil {
		return "", "", manifest, err
	}
	sha, _, err := fileDigest(path)
	if err != nil {
		return "", "", manifest, err
	}
	ok = true
	return path, sha, manifest, nil
}

func fileDigest(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func (a *Archiver) receipt(ctx context.Context, sourceSHA, kind, path, encryptedSHA string, thread int64, m SentMessage) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if m.ID <= 0 || m.Document == nil || m.Document.FileID == "" {
		return errors.New("document accepted without delivery identifiers")
	}
	if m.Document.Size > 0 && m.Document.Size != info.Size() {
		return errors.New("Telegram returned a different document size")
	}
	_, err = a.Store.DB.ExecContext(ctx, `INSERT OR REPLACE INTO archive_receipts(source_sha,kind,filename,byte_size,encrypted_sha,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, sourceSHA, kind, filepath.Base(path), info.Size(), encryptedSHA, a.Config.BackupChatID, thread, m.ID, m.Document.FileID, m.Document.UniqueID, m.Document.Size)
	if err != nil {
		return err
	}
	_, err = a.Store.DB.ExecContext(ctx, `INSERT OR REPLACE INTO backup_files(source_sha,kind,filename,size,chat_id,thread_id,message_id,file_id,encrypted_sha) VALUES(?,?,?,?,?,?,?,?,?)`, sourceSHA, kind, filepath.Base(path), info.Size(), a.Config.BackupChatID, thread, m.ID, m.Document.FileID, encryptedSHA)
	return err
}

func moderationText(p StoredProfile, _ string, missing int) string {
	text := profileText(p) + fmt.Sprintf("\nВерсия архива: %d", p.VersionID)
	if missing > 0 {
		text += fmt.Sprintf("\nНедоступных исходных фото: %d. Подробности в ZIP.", missing)
	}
	return text
}

func archiveProfileKeyboard(version int64) any {
	return map[string]any{"inline_keyboard": [][]map[string]string{{
		{"text": "Полный текст", "callback_data": fmt.Sprintf("av:%d", version)},
		{"text": "Исходные файлы ZIP", "callback_data": fmt.Sprintf("az:%d", version)},
	}}}
}

// Each profile topic keeps its whole original ZIP followed by the visible card.
// Telegram limits albums to ten files, so larger profiles use consecutive groups.
func (a *Archiver) sendProfileCard(ctx context.Context, p StoredProfile, jobs []MediaJob, thread int64, save func([]int64) error) ([]int64, error) {
	missing := 0
	hasFull := false
	for _, j := range jobs {
		if j.State != "complete" || j.SHA == "" {
			missing++
		}
		if j.Role == "photos" && j.State == "complete" && j.SHA != "" {
			hasFull = true
		}
	}
	selected := make([]MediaJob, 0, len(jobs))
	for _, j := range jobs {
		if j.State != "complete" || j.SHA == "" || (hasFull && j.Role != "photos") || (!hasFull && j.Role != "miniPhotos") {
			continue
		}
		selected = append(selected, j)
	}
	// Reject a known local limitation before creating part of the replacement.
	for _, j := range selected {
		path, err := a.mediaSourcePath(j.SHA)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("profile photo is not a readable local file; original retained in ZIP")
		}
		if info.Size() > 9<<20 {
			return nil, errors.New("photo exceeds Telegram preview upload budget; original retained in profile ZIP")
		}
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].Position < selected[j].Position })
	text := moderationText(p, "", missing)
	caption := profileCaption(text)
	keyboard := archiveProfileKeyboard(p.VersionID)
	var ids []int64
	record := func(messages []SentMessage) error {
		for _, m := range messages {
			if m.ID <= 0 {
				return errors.New("profile card accepted without message id")
			}
			ids = append(ids, m.ID)
		}
		return save(ids)
	}
	if len(selected) == 0 {
		body := text + "\nФото пока недоступны."
		if prefix, rest := telegramPrefix(body, 4096); rest != "" {
			const tail = "\n\nПолный текст по кнопке."
			body = truncateRunes(prefix, 4096-len([]rune(tail))) + tail
		}
		var m SentMessage
		err := a.groupCall(ctx, func() (err error) {
			m, err = a.TG.SendText(ctx, a.Config.BackupChatID, thread, body, keyboard)
			return err
		})
		if err != nil {
			return ids, err
		}
		err = record([]SentMessage{m})
		return ids, err
	}
	first := true
	for i := 0; i < len(selected); {
		end := i
		for end < len(selected) && end-i < 10 && selected[end].Size <= 9<<20 {
			end++
		}
		if end-i >= 2 {
			paths := make([]string, 0, end-i)
			for _, j := range selected[i:end] {
				path, err := a.mediaSourcePath(j.SHA)
				if err != nil {
					return ids, err
				}
				paths = append(paths, path)
			}
			groupCaption := ""
			if first {
				groupCaption = caption
			}
			var album []SentMessage
			err := a.groupCall(ctx, func() (err error) {
				album, err = a.TG.SendAlbum(ctx, a.Config.BackupChatID, thread, paths, groupCaption)
				return err
			})
			if err == nil {
				if len(album) != len(paths) {
					return ids, errors.New("profile album returned an incomplete message inventory")
				}
				if err = record(album); err != nil {
					return ids, err
				}
				if first {
					var accepted SentMessage
					err = a.groupCall(ctx, func() error {
						return a.TG.Call(ctx, "editMessageReplyMarkup", map[string]any{"chat_id": a.Config.BackupChatID, "message_id": ids[0], "reply_markup": keyboard}, &accepted)
					})
					if err != nil {
						return ids, err
					}
					if accepted.ID != ids[0] {
						return ids, errors.New("profile keyboard edit unconfirmed")
					}
				}
				first = false
				i = end
				continue
			}
			if len(album) > 0 {
				var known []SentMessage
				for _, m := range album {
					if m.ID > 0 && m.Chat.ID == a.Config.BackupChatID {
						known = append(known, m)
					}
				}
				if len(known) > 0 {
					if saveErr := record(known); saveErr != nil {
						return ids, errors.Join(err, saveErr)
					}
				}
				return ids, err
			}
			return ids, err
		}

		j := selected[i]
		path, err := a.mediaSourcePath(j.SHA)
		if err != nil {
			return ids, err
		}
		thisCaption := ""
		var buttons any
		if first {
			thisCaption = caption
			buttons = keyboard
		}
		var m SentMessage
		if j.Size > 9<<20 {
			return ids, errors.New("photo exceeds Telegram preview upload budget; original retained in profile ZIP")
		}
		err = a.groupCall(ctx, func() (err error) {
			m, err = a.TG.SendPhotoCard(ctx, a.Config.BackupChatID, thread, path, thisCaption, buttons)
			return err
		})
		if err != nil {
			return ids, err
		}
		if err = record([]SentMessage{m}); err != nil {
			return ids, err
		}
		first = false
		i++
	}
	return ids, nil
}

func (a *Archiver) mirrorVersion(ctx context.Context, owner, version int64) error {
	p, err := a.Store.Version(ctx, owner, version)
	if err != nil {
		return err
	}
	jobs, err := a.Store.MediaJobs(ctx, owner, version)
	if err != nil {
		return err
	}
	fp, err := mirrorFingerprint(p.Profile, jobs)
	if err != nil {
		return err
	}
	var prior int64
	if err = a.Store.DB.QueryRowContext(ctx, "SELECT thread_id FROM mirror_posts WHERE fingerprint=?", fp).Scan(&prior); err == nil {
		_, err = a.Store.DB.ExecContext(ctx, "INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id) VALUES(?,?,?,?,'duplicate',?) ON CONFLICT(version_id) DO UPDATE SET state='duplicate',thread_id=excluded.thread_id", version, owner, p.ID, fp, prior)
		return err
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = a.Store.DB.ExecContext(ctx, "INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state) VALUES(?,?,?,?,'pending') ON CONFLICT(version_id) DO NOTHING", version, owner, p.ID, fp); err != nil {
		return err
	}
	thread, err := a.topic(ctx, p)
	if err != nil {
		var state string
		_ = a.Store.DB.QueryRowContext(ctx, "SELECT state FROM archive_topic_jobs WHERE profile_id=?", p.ID).Scan(&state)
		if state == "uncertain" || state == "error" {
			_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state=?,error='topic_unavailable' WHERE version_id=?", state, version)
		}
		return err
	}
	path, sha, manifest, err := a.profileZIP(ctx, p, owner, jobs)
	if err != nil {
		return err
	}
	_, size, err := fileDigest(path)
	if err != nil {
		return err
	}
	if size > 49<<20 {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state='oversize',source_sha=?,error='whole_profile_zip_exceeds_cloud_limit' WHERE version_id=?", sha, version)
		return nil
	}
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state='processing',thread_id=?,source_sha=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=?", thread, sha, version); err != nil {
		return err
	}
	var document SentMessage
	err = a.groupCall(ctx, func() error {
		var e error
		document, e = a.TG.SendDocument(ctx, a.Config.BackupChatID, thread, path, fmt.Sprintf("Jardulik %d, версия %d. Целый исходный архив.\nSHA-256 %s\nУстройство: %s\nИсточник: Jardulik API; снимок %s", p.ID, p.VersionID, sha, manifest.CaptureDevice, displayTime(p.LastSeen)))
		return e
	})
	if err == nil {
		err = a.receipt(ctx, sha, "profile_version", path, "", thread, document)
	}
	if err != nil {
		state := archiveFailureState(err)
		if document.ID > 0 {
			state = "uncertain"
		}
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=?", state, archiveErrorCode(err), version)
		return err
	}
	saveIDs := func(ids []int64) error {
		b, _ := json.Marshal(ids)
		_, e := a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET message_ids=? WHERE version_id=?", string(b), version)
		return e
	}
	ids, err := a.sendProfileCard(ctx, p, jobs, thread, saveIDs)
	if err != nil {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET state='uncertain',error='source_zip_sent_card_unconfirmed',updated_at=CURRENT_TIMESTAMP WHERE version_id=?", version)
		return err
	}
	missing := 0
	for _, j := range jobs {
		if j.State != "complete" || j.SHA == "" {
			missing++
		}
	}
	state := "sent"
	detail := ""
	if missing > 0 {
		state = "partial"
		detail = "source_media_missing"
	}
	idsJSON, _ := json.Marshal(ids)
	tx, err := a.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE archive_versions SET state=?,error=?,message_ids=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=?", state, detail, string(idsJSON), version); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO mirror_posts(fingerprint,profile_id,thread_id,message_ids) VALUES(?,?,?,?)", fp, p.ID, thread, string(idsJSON)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]',?)", version, string(idsJSON)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return os.Remove(path)
}

// RequestProfileRepair queues one maintenance pass for the existing worker.
// A completed request may be queued again; unresolved outcomes need an operator.
func (a *Archiver) RequestProfileRepair(ctx context.Context) error {
	if err := a.init(ctx); err != nil {
		return err
	}
	var unresolved int
	if err := a.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM archive_profile_repairs WHERE state<>'sent'").Scan(&unresolved); err != nil {
		return err
	}
	if unresolved > 0 {
		return errors.New("profile repairs require operator reconciliation before another request")
	}
	result, err := a.Store.DB.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('archive_repair_requested','pending') ON CONFLICT(key) DO UPDATE SET value='pending' WHERE value='done'")
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	var state string
	if err = a.Store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='archive_repair_requested'").Scan(&state); err != nil {
		return err
	}
	if state == "pending" {
		return nil
	}
	return fmt.Errorf("profile repair request is %s; reconcile it before requesting another pass", state)
}

// Only the sole archive worker calls this, between complete normal operations.
func (a *Archiver) processQueuedRepair(ctx context.Context) (bool, error) {
	result, err := a.Store.DB.ExecContext(ctx, "UPDATE meta SET value='processing' WHERE key='archive_repair_requested' AND value='pending'")
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	err = a.repairProfilePostsLocked(ctx)
	state := "done"
	finishCtx := context.WithoutCancel(ctx)
	if err != nil {
		state = "error"
		var unknown int
		if scanErr := a.Store.DB.QueryRowContext(finishCtx, "SELECT COUNT(*) FROM archive_profile_repairs WHERE state='uncertain'").Scan(&unknown); scanErr != nil || unknown > 0 || ctx.Err() != nil {
			state = "uncertain"
		}
	} else if ctx.Err() != nil {
		state = "uncertain"
		err = ctx.Err()
	}
	if saveErr := a.Store.SetMeta(finishCtx, "archive_repair_requested", state); saveErr != nil {
		return true, errors.Join(err, saveErr)
	}
	return true, err
}

// RepairProfilePosts retains each recorded whole ZIP and replaces only its
// legacy text/photo messages. Unknown outcomes always require reconciliation.
func (a *Archiver) RepairProfilePosts(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Config.BackupChatID == 0 || a.Config.BackupThreadID <= 0 {
		return errors.New("archive chat and service snapshot topic are required")
	}
	lock, err := os.OpenFile(filepath.Join(a.Config.DataDir, "archive-worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("archive worker is active; stop the service before repairing profile posts")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return a.repairProfilePostsLocked(ctx)
}

// Caller owns the worker mutex and file lock throughout every external call.
func (a *Archiver) repairProfilePostsLocked(ctx context.Context) error {
	if a.Config.BackupChatID == 0 || a.Config.BackupThreadID <= 0 {
		return errors.New("archive chat and service snapshot topic are required")
	}
	if err := a.init(ctx); err != nil {
		return err
	}
	if _, err := a.Store.DB.ExecContext(ctx, "UPDATE archive_profile_repairs SET state='uncertain',error='repair_restarted_during_request' WHERE state IN ('editing_zip','sending','deleting')"); err != nil {
		return err
	}
	rows, err := a.Store.DB.QueryContext(ctx, `SELECT av.version_id FROM archive_versions av
 JOIN archive_receipts r ON r.source_sha=av.source_sha
 LEFT JOIN archive_profile_repairs repair ON repair.version_id=av.version_id
 WHERE av.state IN ('sent','partial') AND r.kind='profile_version' AND repair.version_id IS NULL ORDER BY av.version_id`)
	if err != nil {
		return err
	}
	var versions []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		versions = append(versions, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures []error
	for _, version := range versions {
		if err = a.repairProfilePost(ctx, version); err != nil {
			failures = append(failures, fmt.Errorf("profile version %d: %w", version, err))
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	var unresolved int
	if err = a.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM archive_profile_repairs WHERE state<>'sent'").Scan(&unresolved); err != nil {
		failures = append(failures, err)
	} else if unresolved > 0 {
		failures = append(failures, fmt.Errorf("%d profile repairs require operator reconciliation; inspect archive_profile_repairs before retrying", unresolved))
	}
	return errors.Join(failures...)
}

func (a *Archiver) repairProfilePost(ctx context.Context, version int64) (retErr error) {
	var owner, thread, receiptChat, receiptThread, receiptMessage int64
	var sourceSHA, oldJSON, sentAt, fingerprint string
	err := a.Store.DB.QueryRowContext(ctx, `SELECT av.owner_id,av.thread_id,av.source_sha,av.message_ids,av.fingerprint,r.chat_id,r.thread_id,r.message_id,r.sent_at
 FROM archive_versions av JOIN archive_receipts r ON r.source_sha=av.source_sha WHERE av.version_id=?`, version).Scan(&owner, &thread, &sourceSHA, &oldJSON, &fingerprint, &receiptChat, &receiptThread, &receiptMessage, &sentAt)
	if err != nil {
		return err
	}
	if thread <= 0 || thread == a.Config.BackupThreadID || receiptChat != a.Config.BackupChatID || receiptThread != thread || receiptMessage <= 0 {
		return errors.New("legacy receipt does not identify the recorded profile topic")
	}
	var oldIDs []int64
	if err = json.Unmarshal([]byte(oldJSON), &oldIDs); err != nil || len(oldIDs) == 0 {
		return errors.New("legacy profile message inventory is missing")
	}
	seen := make(map[int64]bool)
	var deleteIDs []int64
	for _, id := range oldIDs {
		if id <= 0 || seen[id] {
			return errors.New("legacy profile message inventory is invalid")
		}
		seen[id] = true
		if id != receiptMessage {
			deleteIDs = append(deleteIDs, id)
		}
	}
	if !seen[receiptMessage] {
		return errors.New("legacy ZIP is absent from the profile message inventory")
	}
	// Telegram cannot delete ordinary messages older than 48 hours. Do not create
	// a second card when the old presentation cannot be removed safely.
	var created time.Time
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05"} {
		created, err = time.Parse(layout, sentAt)
		if err == nil {
			break
		}
	}
	if err != nil || a.now().Sub(created) >= 48*time.Hour || created.After(a.now().Add(time.Minute)) {
		return errors.New("legacy messages are outside the verified 48-hour deletion window")
	}
	p, err := a.Store.Version(ctx, owner, version)
	if err != nil {
		return err
	}
	jobs, err := a.Store.MediaJobs(ctx, owner, version)
	if err != nil {
		return err
	}
	if _, err = a.Store.DB.ExecContext(ctx, "INSERT INTO archive_profile_repairs(version_id,state,old_message_ids) VALUES(?,'editing_zip',?)", version, oldJSON); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_, _ = a.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE archive_profile_repairs SET state='uncertain',error=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=? AND state<>'sent'", archiveErrorCode(retErr), version)
		}
	}()
	var edited SentMessage
	caption := fmt.Sprintf("Jardulik %d, версия %d. Целый исходный архив.\nSHA-256 %s\nИсточник: Jardulik API; снимок %s\nTelegram: ZIP сохранён %s", p.ID, p.VersionID, sourceSHA, displayTime(p.LastSeen), displayTime(sentAt))
	err = a.groupCall(ctx, func() error {
		return a.TG.Call(ctx, "editMessageCaption", map[string]any{"chat_id": a.Config.BackupChatID, "message_id": receiptMessage, "caption": caption}, &edited)
	})
	if err != nil {
		return err
	}
	if edited.ID != receiptMessage {
		return errors.New("source ZIP caption edit unconfirmed")
	}
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_profile_repairs SET state='sending',updated_at=CURRENT_TIMESTAMP WHERE version_id=?", version); err != nil {
		return err
	}
	save := func(ids []int64) error {
		b, _ := json.Marshal(ids)
		_, e := a.Store.DB.ExecContext(ctx, "UPDATE archive_profile_repairs SET new_message_ids=?,updated_at=CURRENT_TIMESTAMP WHERE version_id=?", string(b), version)
		return e
	}
	ids, err := a.sendProfileCard(ctx, p, jobs, thread, save)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return errors.New("replacement profile card is unconfirmed")
	}
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_profile_repairs SET state='deleting',updated_at=CURRENT_TIMESTAMP WHERE version_id=?", version); err != nil {
		return err
	}
	for _, id := range deleteIDs {
		var deleted bool
		if err = a.groupCall(ctx, func() error {
			return a.TG.Call(ctx, "deleteMessage", map[string]any{"chat_id": a.Config.BackupChatID, "message_id": id}, &deleted)
		}); err != nil {
			return err
		}
		if !deleted {
			return errors.New("old profile message deletion unconfirmed")
		}
	}
	newJSON, _ := json.Marshal(ids)
	tx, err := a.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE archive_versions SET message_ids=?,error='',updated_at=CURRENT_TIMESTAMP WHERE version_id=?", string(newJSON), version); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE mirror_posts SET message_ids=? WHERE fingerprint=? AND thread_id=?", string(newJSON), fingerprint, thread); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE archive_profile_repairs SET state='sent',error='',updated_at=CURRENT_TIMESTAMP WHERE version_id=?", version); err != nil {
		return err
	}
	return tx.Commit()
}

func (a *Archiver) allReceipts(ctx context.Context, db *sql.DB) ([]archiveReceipt, error) {
	rows, err := db.QueryContext(ctx, "SELECT source_sha,kind,filename,byte_size,encrypted_sha,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at FROM archive_receipts ORDER BY sent_at,source_sha")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []archiveReceipt
	for rows.Next() {
		var r archiveReceipt
		if err = rows.Scan(&r.SourceSHA, &r.Kind, &r.Name, &r.Size, &r.EncryptedSHA, &r.ChatID, &r.ThreadID, &r.MessageID, &r.FileID, &r.FileUniqueID, &r.ReturnedSize, &r.SentAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (a *Archiver) serviceZIP(ctx context.Context, id int64) (string, archiveManifest, error) {
	// The database copy defines the inventory. Holding the same mutex used by
	// deletion/GC keeps every file referenced by that copy alive until packed.
	// This lock is released before any Telegram request.
	a.Store.mediaMu.Lock()
	defer a.Store.mediaMu.Unlock()
	var manifest archiveManifest
	dir := filepath.Join(a.Config.DataDir, "backups")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", manifest, err
	}
	db, err := os.CreateTemp(dir, ".snapshot-*.sqlite")
	if err != nil {
		return "", manifest, err
	}
	dbpath := db.Name()
	db.Close()
	os.Remove(dbpath)
	defer os.Remove(dbpath)
	if err = a.Store.Snapshot(ctx, dbpath); err != nil {
		return "", manifest, err
	}
	return a.serviceZIPFromSnapshot(ctx, id, dbpath)
}

// Caller holds Store.mediaMu from snapshot creation through file copying.
func (a *Archiver) serviceZIPFromSnapshot(ctx context.Context, id int64, dbpath string) (string, archiveManifest, error) {
	manifest := baseManifest()
	u := url.URL{Scheme: "file", Path: dbpath}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}}.Encode()
	snapshot, err := sql.Open("sqlite", u.String())
	if err != nil {
		return "", manifest, err
	}
	snapshot.SetMaxOpenConns(1)
	defer snapshot.Close()
	if err = snapshot.PingContext(ctx); err != nil {
		return "", manifest, err
	}
	dir := filepath.Join(a.Config.DataDir, "backups")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return "", manifest, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s__jardulik-service__backup-%d.zip", a.now().UTC().Format("20060102T150405Z"), id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", manifest, err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	z := zip.NewWriter(f)
	entry, err := addZipFile(z, "jardulik.sqlite", dbpath)
	if err != nil {
		return "", manifest, err
	}
	manifest.Files = append(manifest.Files, entry)
	// The recovery key is deliberately excluded. Its escrow is a separate owner task.
	recovery := map[string]any{"APP_DATA_DIR": a.Config.DataDir, "TELEGRAM_BOT_TOKEN": a.Config.Token, "OWNER_TELEGRAM_ID": a.Config.OwnerID, "JARDULIK_BOT_ID": a.Config.SourceBotID, "BACKUP_CHAT_ID": a.Config.BackupChatID, "BACKUP_THREAD_ID": a.Config.BackupThreadID, "SESSION_KEY": base64.StdEncoding.EncodeToString(a.Config.SessionKey), "ALLOW_REACTIONS": false, "MIN_FREE_BYTES": a.Config.MinFreeBytes}
	b, err := json.MarshalIndent(recovery, "", "  ")
	if err != nil {
		return "", manifest, err
	}
	entry, err = addZipBytes(z, "recovery-config.json", b)
	if err != nil {
		return "", manifest, err
	}
	manifest.Files = append(manifest.Files, entry)
	manifest.Receipts, err = a.allReceipts(ctx, snapshot)
	if err != nil {
		return "", manifest, err
	}
	var pending, failed, unmirrored int64
	if err = snapshot.QueryRowContext(ctx, "SELECT COUNT(*) FROM media_jobs WHERE state='pending'").Scan(&pending); err != nil {
		return "", manifest, err
	}
	if err = snapshot.QueryRowContext(ctx, "SELECT COUNT(*) FROM media_jobs WHERE state='error'").Scan(&failed); err != nil {
		return "", manifest, err
	}
	if err = snapshot.QueryRowContext(ctx, "SELECT COUNT(*) FROM versions v LEFT JOIN archive_versions av ON av.version_id=v.id WHERE av.state IS NULL OR av.state NOT IN ('sent','duplicate')").Scan(&unmirrored); err != nil {
		return "", manifest, err
	}
	manifest.Coverage["pending_media"] = pending
	manifest.Coverage["failed_media"] = failed
	manifest.Coverage["versions_without_complete_telegram_archive"] = unmirrored
	rows, err := snapshot.QueryContext(ctx, "SELECT sha,path,size,mime FROM media ORDER BY sha")
	if err != nil {
		return "", manifest, err
	}
	type item struct {
		sha, path, mime string
		size            int64
	}
	var items []item
	var total int64
	for rows.Next() {
		var m item
		if err = rows.Scan(&m.sha, &m.path, &m.size, &m.mime); err != nil {
			rows.Close()
			return "", manifest, err
		}
		items = append(items, m)
		total += m.size
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", manifest, err
	}
	embed := total <= 8<<20 && len(items) <= 1000
	manifest.Coverage["media_embedded"] = embed
	manifest.Coverage["media_file_count"] = len(items)
	manifest.Coverage["original_media_bytes"] = total
	manifest.Coverage["source_complete"] = pending == 0 && failed == 0 && (embed || unmirrored == 0)
	for _, m := range items {
		manifest.Media = append(manifest.Media, archiveMedia{SHA256: m.sha, State: "complete", Size: m.size})
		if embed {
			mediaPath, e := a.mediaSourcePath(m.sha)
			if e != nil {
				return "", manifest, e
			}
			entry, err = addZipFile(z, "media/"+m.sha, mediaPath)
			if err != nil {
				return "", manifest, err
			}
			if entry.SHA256 != m.sha {
				return "", manifest, errors.New("media source hash mismatch")
			}
			manifest.Files = append(manifest.Files, entry)
		}
	}
	b, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", manifest, err
	}
	if _, err = addZipBytes(z, "manifest.json", b); err != nil {
		return "", manifest, err
	}
	if err = z.Close(); err != nil {
		return "", manifest, err
	}
	if err = f.Close(); err != nil {
		return "", manifest, err
	}
	ok = true
	return path, manifest, nil
}

func (a *Archiver) processBackup(ctx context.Context, id int64) error {
	if _, err := a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state='processing' WHERE id=? AND state='pending'", id); err != nil {
		return err
	}
	path, manifest, err := a.serviceZIP(ctx, id)
	if err != nil {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state='error',error='snapshot_failed' WHERE id=?", id)
		return err
	}
	if err = preflightServiceArchive(path); err != nil {
		state, reason := "error", "invalid_generated_archive"
		if errors.Is(err, errArchiveBudget) {
			state, reason = "oversize", "whole_archive_exceeds_restore_budget"
		}
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state=?,path=?,error=? WHERE id=?", state, path, reason, id)
		if state == "oversize" {
			return a.notify(ctx, "Бэкап не отправлен: целый архив превышает проверенный бюджет восстановления. Он сохранён на VPS без дробления; нужна отдельная схема передачи и восстановления целого файла.")
		}
		return err
	}
	plain, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sourceSHA := digest(plain)
	encrypted, err := seal(a.Config.BackupKey, plain, serviceBackupPurpose)
	if err != nil {
		return err
	}
	encrypted = append([]byte(serviceBackupMagic), encrypted...)
	encPath := path + ".enc"
	if err = os.WriteFile(encPath, encrypted, 0600); err != nil {
		return err
	}
	encryptedSHA := digest(encrypted)
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET path=?,source_sha=?,encrypted_sha=? WHERE id=?", encPath, sourceSHA, encryptedSHA, id); err != nil {
		return err
	}
	var sent SentMessage
	err = a.groupCall(ctx, func() error {
		var e error
		sent, e = a.TG.SendDocument(ctx, a.Config.BackupChatID, a.Config.BackupThreadID, encPath, "Jardulik Companion: зашифрованный целый снимок сервиса. Ключ восстановления хранится отдельно.")
		return e
	})
	if err == nil {
		err = a.receipt(ctx, sourceSHA, "service_snapshot", encPath, encryptedSHA, a.Config.BackupThreadID, sent)
	}
	if err != nil {
		state := archiveFailureState(err)
		if sent.ID > 0 {
			state = "uncertain"
		}
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state=?,error=? WHERE id=?", state, archiveErrorCode(err), id)
		return err
	}
	if _, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state='sent',message_id=?,file_id=?,error='' WHERE id=?", sent.ID, sent.Document.FileID, id); err != nil {
		return err
	}
	_ = os.Remove(path) // A plaintext recovery configuration is never left after encryption and delivery.
	text := fmt.Sprintf("Jardulik Companion: снимок сервиса\nСнято: %s\nВерсия: %s; устройство: %s; %s/%s\nДокумент принят Telegram: chat=%d, topic=%d, message=%d\nSHA-256 исходного ZIP: %s\nSHA-256 зашифрованного файла: %s\nФото в ZIP: %v; ожидают загрузки: %v; ошибки фото: %v; версии без полного TG-архива: %v.\nМанифест содержит исходные хэши и Telegram IDs. Это не проверка байтов на сервере Telegram.\n#Jardulik #бэкап #восстановление", displayTime(manifest.CapturedAt), Version, manifest.CaptureDevice, manifest.OS, manifest.Architecture, a.Config.BackupChatID, a.Config.BackupThreadID, sent.ID, sourceSHA, encryptedSHA, manifest.Coverage["media_embedded"], manifest.Coverage["pending_media"], manifest.Coverage["failed_media"], manifest.Coverage["versions_without_complete_telegram_archive"])
	var companion SentMessage
	err = a.groupCall(ctx, func() error {
		var e error
		companion, e = a.TG.SendText(ctx, a.Config.BackupChatID, a.Config.BackupThreadID, truncateRunes(text, 4000), nil)
		return e
	})
	if err != nil {
		_, _ = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET state='partial',error='document_sent_metadata_unconfirmed' WHERE id=?", id)
		return err
	}
	_, err = a.Store.DB.ExecContext(ctx, "UPDATE archive_backups SET companion_message_id=? WHERE id=?", companion.ID, id)
	return err
}

func (a *Archiver) notify(ctx context.Context, text string) error {
	return a.groupCall(ctx, func() error {
		_, err := a.TG.SendText(ctx, a.Config.BackupChatID, a.Config.BackupThreadID, truncateRunes(text, 4000), nil)
		return err
	})
}

// RestoreArchive authenticates the entire bounded backup before writing. It
// restores into an empty directory and validates each file and the SQLite DB.
// Large media omitted from a snapshot are recovered from the manifest's whole
// profile documents; this function never calls that manual step "complete".
func RestoreArchive(input, dest string, key []byte) error {
	info, err := os.Stat(input)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > serviceBackupLimit+1024 {
		return errors.New("invalid or oversized service archive")
	}
	b, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(b, []byte(serviceBackupMagic)) {
		return errors.New("unsupported backup format")
	}
	plain, err := unseal(key, b[len(serviceBackupMagic):], serviceBackupPurpose)
	if err != nil {
		return errors.New("backup authentication failed")
	}
	if len(plain) > serviceBackupLimit {
		return fmt.Errorf("%w: compressed size", errArchiveBudget)
	}
	z, err := zip.NewReader(bytes.NewReader(plain), int64(len(plain)))
	if err != nil {
		return err
	}
	return restoreServiceZIP(z, dest)
}

// The exact same non-writing checks gate publication and restoration. A small
// compressed ZIP can still exceed the uncompressed or manifest budget.
func preflightServiceArchive(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > serviceBackupLimit {
		return fmt.Errorf("%w: compressed size", errArchiveBudget)
	}
	z, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer z.Close()
	_, _, err = preflightServiceZIP(&z.Reader)
	return err
}

func preflightServiceZIP(z *zip.Reader) (map[string]*zip.File, map[string]archiveFile, error) {
	if len(z.File) > serviceArchiveEntries {
		return nil, nil, fmt.Errorf("%w: too many entries", errArchiveBudget)
	}
	entries := map[string]*zip.File{}
	var total uint64
	for _, f := range z.File {
		name := f.Name
		if !filepath.IsLocal(name) || strings.Contains(name, "\\") || filepath.Clean(name) != name || f.Mode()&os.ModeSymlink != 0 || f.FileInfo().IsDir() {
			return nil, nil, errors.New("unsafe archive path or entry")
		}
		if name != "manifest.json" && name != "jardulik.sqlite" && name != "recovery-config.json" {
			if !strings.HasPrefix(name, "media/") || len(strings.TrimPrefix(name, "media/")) != 64 {
				return nil, nil, errors.New("unexpected archive entry")
			}
			if _, err := hex.DecodeString(strings.TrimPrefix(name, "media/")); err != nil {
				return nil, nil, errors.New("invalid media filename")
			}
		}
		if entries[name] != nil {
			return nil, nil, errors.New("duplicate archive path")
		}
		entries[name] = f
		if f.UncompressedSize64 > serviceRestoreLimit || total > serviceRestoreLimit-f.UncompressedSize64 {
			return nil, nil, fmt.Errorf("%w: uncompressed size", errArchiveBudget)
		}
		total += f.UncompressedSize64
	}
	if entries["manifest.json"] == nil || entries["jardulik.sqlite"] == nil || entries["recovery-config.json"] == nil {
		return nil, nil, errors.New("incomplete service archive")
	}
	if entries["manifest.json"].UncompressedSize64 > serviceManifestLimit {
		return nil, nil, fmt.Errorf("%w: manifest size", errArchiveBudget)
	}
	mf, err := entries["manifest.json"].Open()
	if err != nil {
		return nil, nil, err
	}
	var manifest archiveManifest
	err = json.NewDecoder(io.LimitReader(mf, serviceManifestLimit)).Decode(&manifest)
	mf.Close()
	if err != nil {
		return nil, nil, err
	}
	if manifest.Format != "jardulik-companion-archive/v1" {
		return nil, nil, errors.New("unsupported manifest format")
	}
	expected := map[string]archiveFile{}
	for _, f := range manifest.Files {
		if _, exists := expected[f.Name]; exists {
			return nil, nil, errors.New("duplicate manifest entry")
		}
		expected[f.Name] = f
	}
	if len(expected) != len(entries)-1 {
		return nil, nil, errors.New("manifest does not cover every source file")
	}
	for name, f := range entries {
		if name == "manifest.json" {
			continue
		}
		e, ok := expected[name]
		if !ok || e.ByteSize < 0 || uint64(e.ByteSize) != f.UncompressedSize64 || len(e.SHA256) != 64 {
			return nil, nil, errors.New("manifest file metadata mismatch")
		}
	}
	return entries, expected, nil
}

func restoreServiceZIP(z *zip.Reader, dest string) error {
	entries, expected, err := preflightServiceZIP(z)
	if err != nil {
		return err
	}
	if st, err := os.Lstat(dest); err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("restore destination is not an ordinary directory")
		}
		items, err := os.ReadDir(dest)
		if err != nil {
			return err
		}
		if len(items) != 0 {
			return errors.New("restore destination must be empty")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if err = os.MkdirAll(dest, 0700); err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := entries[name]
		path := filepath.Join(dest, filepath.FromSlash(name))
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		in, err := f.Open()
		if err != nil {
			out.Close()
			return err
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(out, h), io.LimitReader(in, serviceRestoreLimit+1))
		in.Close()
		closeErr := out.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if n > serviceRestoreLimit {
			return errors.New("restore size exceeded")
		}
		if name != "manifest.json" {
			e := expected[name]
			if n != e.ByteSize || hex.EncodeToString(h.Sum(nil)) != e.SHA256 {
				return errors.New("restored file hash mismatch")
			}
		}
	}
	u := filepath.ToSlash(filepath.Join(dest, "jardulik.sqlite"))
	db, err := sql.Open("sqlite", "file:"+u+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var status string
	if err = db.QueryRow("PRAGMA quick_check").Scan(&status); err != nil {
		return err
	}
	if status != "ok" {
		return errors.New("restored SQLite integrity check failed")
	}
	return nil
}
