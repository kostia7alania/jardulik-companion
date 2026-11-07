package companion

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const restoreDownloadLimit = 20 << 20

type RestoreMediaResult struct {
	Expected              int            `json:"expected"`
	Verified              int            `json:"verified"` // Includes newly restored originals.
	Restored              int            `json:"restored"`
	Missing               int            `json:"missing"`
	ExpectedMediaComplete bool           `json:"expected_media_complete"`
	SnapshotCoverage      map[string]any `json:"snapshot_coverage"`
	SupportGaps           []string       `json:"support_gaps,omitempty"`
}

// RestoreMedia completes only the snapshot's recorded media inventory. Pending
// or failed upstream downloads remain explicit in SnapshotCoverage. It neither
// opens SQLite nor requests anything from Jardulik.
func RestoreMedia(ctx context.Context, tg *Telegram, dest string) (result RestoreMediaResult, resultErr error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	st, err := os.Lstat(dest)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return result, errors.New("media restore destination must be an ordinary restored directory")
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return result, err
	}
	defer root.Close()
	manifestFile, err := openRestoreRegular(root, "manifest.json")
	if err != nil {
		return result, err
	}
	b, err := io.ReadAll(io.LimitReader(restoreContextReader{ctx, manifestFile}, serviceManifestLimit+1))
	manifestFile.Close()
	if err != nil {
		return result, err
	}
	var manifest archiveManifest
	if len(b) > serviceManifestLimit || json.Unmarshal(b, &manifest) != nil || manifest.Format != "jardulik-companion-archive/v1" || manifest.ProfileID != 0 {
		return result, errors.New("invalid service snapshot manifest")
	}
	result.SnapshotCoverage = manifest.Coverage
	if manifest.Coverage["source_complete"] != true {
		result.SupportGaps = append(result.SupportGaps, "snapshot_source_coverage_incomplete_or_unknown")
	}
	expected := make(map[string]archiveMedia, len(manifest.Media))
	for _, m := range manifest.Media {
		if !restoreSHA(m.SHA256) || m.State != "complete" || m.Size < 0 || m.Size > serviceRestoreLimit {
			return result, errors.New("invalid snapshot media inventory")
		}
		if prior, exists := expected[m.SHA256]; exists && prior.Size != m.Size {
			return result, errors.New("conflicting snapshot media sizes")
		}
		expected[m.SHA256] = m
	}
	result.Expected, result.Missing = len(expected), len(expected)
	defer func() {
		result.Missing = len(expected) - result.Verified
		result.ExpectedMediaComplete = result.Missing == 0 && ctx.Err() == nil && resultErr == nil
	}()
	if st, err = root.Lstat("media"); err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return result, errors.New("media restore path is not an ordinary directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	needed := make(map[string]archiveMedia, len(expected))
	for sha, m := range expected {
		name := "media/" + sha
		info, e := root.Lstat(name)
		if e == nil {
			if !info.Mode().IsRegular() {
				return result, errors.New("media destination contains a non-regular file")
			}
			if info.Size() == m.Size {
				f, e := openRestoreRegular(root, name)
				if e != nil {
					return result, e
				}
				e = verifyRestoreSource(ctx, f, io.Discard, m.SHA256, m.Size)
				f.Close()
				if e == nil {
					result.Verified++
					continue
				}
				if ctx.Err() != nil {
					return result, ctx.Err()
				}
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return result, e
		}
		needed[sha] = m
	}
	var failures []error
	seen := map[string]bool{}
	for _, receipt := range manifest.Receipts {
		if len(needed) == 0 {
			break
		}
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if receipt.Kind != "profile_version" || seen[receipt.SourceSHA] {
			continue
		}
		seen[receipt.SourceSHA] = true
		gap := ""
		switch {
		case !restoreSHA(receipt.SourceSHA) || receipt.Size <= 0 || receipt.FileID == "" || receipt.MessageID <= 0 || receipt.ChatID == 0 || receipt.EncryptedSHA != "" || (receipt.ReturnedSize > 0 && receipt.ReturnedSize != receipt.Size):
			gap = "invalid_whole_profile_receipt"
		case receipt.Size > restoreDownloadLimit:
			gap = "whole_profile_zip_exceeds_20_mib_download_budget"
		case tg == nil || tg.HTTP == nil:
			gap = "telegram_download_client_unavailable"
		}
		if gap != "" {
			result.SupportGaps = append(result.SupportGaps, gap)
			failures = append(failures, errors.New(gap))
			continue
		}
		err = restoreProfileDocument(ctx, root, tg, receipt, expected, needed, &result)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			result.SupportGaps = append(result.SupportGaps, "profile_zip_recovery_failed:"+receipt.SourceSHA)
			failures = append(failures, fmt.Errorf("profile ZIP %s: %w", receipt.SourceSHA, err))
		}
	}
	if len(needed) > 0 {
		result.SupportGaps = append(result.SupportGaps, "snapshot_media_not_recovered_from_delivered_whole_zips")
		failures = append(failures, fmt.Errorf("%d snapshot media originals remain missing", len(needed)))
	}
	return result, errors.Join(failures...)
}

func restoreProfileDocument(ctx context.Context, root *os.Root, tg *Telegram, receipt archiveReceipt, expected, needed map[string]archiveMedia, result *RestoreMediaResult) (resultErr error) {
	file, name, err := newRestoreTemp(root, ".restore-profile-")
	if err != nil {
		return err
	}
	defer func() {
		closeErr := file.Close()
		removeErr := root.Remove(name)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		resultErr = errors.Join(resultErr, closeErr, removeErr)
	}()
	if err = downloadRestoreZIP(ctx, tg, receipt, file); err != nil {
		return err
	}
	z, err := zip.NewReader(file, receipt.Size)
	if err != nil {
		return err
	}
	entries, err := preflightProfileMediaZIP(ctx, z, expected)
	if err != nil {
		return err
	}
	for _, entry := range z.File {
		sha := strings.TrimPrefix(entry.Name, "media/")
		m, ok := needed[sha]
		if !ok || entries["media/"+sha] != entry {
			continue
		}
		if err = root.Mkdir("media", 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		st, err := root.Lstat("media")
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("media restore path changed to an unsafe destination")
		}
		mediaRoot, err := root.OpenRoot("media")
		if err != nil {
			return err
		}
		err = restoreMediaEntry(ctx, mediaRoot, entry, m)
		mediaRoot.Close()
		if err != nil {
			return err
		}
		delete(needed, sha)
		result.Verified++
		result.Restored++
	}
	return nil
}

func preflightProfileMediaZIP(ctx context.Context, z *zip.Reader, expected map[string]archiveMedia) (map[string]*zip.File, error) {
	if len(z.File) > serviceArchiveEntries {
		return nil, fmt.Errorf("%w: profile entry count", errArchiveBudget)
	}
	entries := map[string]*zip.File{}
	var total uint64
	for _, f := range z.File {
		name := f.Name
		if !filepath.IsLocal(name) || filepath.Clean(name) != name || strings.Contains(name, "\\") || !f.Mode().IsRegular() || entries[name] != nil {
			return nil, errors.New("unsafe or duplicate profile ZIP entry")
		}
		if name != "raw.json" && name != "manifest.json" && (!strings.HasPrefix(name, "media/") || !restoreSHA(strings.TrimPrefix(name, "media/"))) {
			return nil, errors.New("unexpected profile ZIP entry")
		}
		if f.UncompressedSize64 > serviceRestoreLimit || total > serviceRestoreLimit-f.UncompressedSize64 {
			return nil, fmt.Errorf("%w: profile uncompressed size", errArchiveBudget)
		}
		total += f.UncompressedSize64
		entries[name] = f
	}
	if entries["manifest.json"] == nil || entries["raw.json"] == nil || entries["manifest.json"].UncompressedSize64 > serviceManifestLimit {
		return nil, errors.New("missing or oversized profile manifest")
	}
	f, err := entries["manifest.json"].Open()
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(restoreContextReader{ctx, f}, serviceManifestLimit+1))
	f.Close()
	var manifest archiveManifest
	if err != nil {
		return nil, err
	}
	if len(b) > serviceManifestLimit || json.Unmarshal(b, &manifest) != nil || manifest.Format != "jardulik-companion-archive/v1" || manifest.ProfileID <= 0 || manifest.VersionID <= 0 || manifest.ObserverID <= 0 || len(manifest.Files) != len(entries)-1 {
		return nil, errors.New("invalid profile manifest")
	}
	files := map[string]archiveFile{}
	for _, source := range manifest.Files {
		entry := entries[source.Name]
		if source.Name == "manifest.json" || entry == nil || files[source.Name].Name != "" || source.ByteSize < 0 || uint64(source.ByteSize) != entry.UncompressedSize64 || !restoreSHA(source.SHA256) {
			return nil, errors.New("invalid profile file inventory")
		}
		if strings.HasPrefix(source.Name, "media/") && source.Name != "media/"+source.SHA256 {
			return nil, errors.New("profile media filename/hash mismatch")
		}
		files[source.Name] = source
		in, err := entry.Open()
		if err != nil {
			return nil, err
		}
		err = verifyRestoreSource(ctx, in, io.Discard, source.SHA256, source.ByteSize)
		in.Close()
		if err != nil {
			return nil, err
		}
	}
	declared := map[string]bool{}
	for _, m := range manifest.Media {
		if m.State != "complete" {
			continue
		}
		source, ok := files["media/"+m.SHA256]
		if !ok || !restoreSHA(m.SHA256) || source.SHA256 != m.SHA256 || source.ByteSize != m.Size {
			return nil, errors.New("profile media is absent from its file inventory")
		}
		if want, ok := expected[m.SHA256]; ok && want.Size != m.Size {
			return nil, errors.New("profile media conflicts with snapshot inventory")
		}
		declared["media/"+m.SHA256] = true
	}
	for name := range files {
		if strings.HasPrefix(name, "media/") && !declared[name] {
			return nil, errors.New("profile ZIP contains undeclared media")
		}
	}
	return entries, nil
}

// Use the existing Telegram client without handing a path-string API authority
// to write outside the opened restore root if a directory is replaced by a link.
func downloadRestoreZIP(ctx context.Context, tg *Telegram, receipt archiveReceipt, out *os.File) error {
	client := *tg.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	api := *tg
	api.HTTP = &client
	var info struct {
		Path string `json:"file_path"`
		Size int64  `json:"file_size"`
	}
	if err := api.Call(ctx, "getFile", map[string]string{"file_id": receipt.FileID}, &info); err != nil {
		return err
	}
	if info.Size < 0 || info.Size > restoreDownloadLimit || (info.Size > 0 && info.Size != receipt.Size) {
		return errors.New("Telegram file exceeds the 20 MiB budget or differs from the whole-file receipt")
	}
	if info.Path == "" || path.IsAbs(info.Path) || path.Clean(info.Path) != info.Path || strings.Contains(info.Path, "\\") || info.Path == ".." || strings.HasPrefix(info.Path, "../") {
		return errors.New("unsafe Telegram file path")
	}
	u, err := url.Parse(tg.Base)
	if err != nil {
		return errors.New("invalid Telegram download origin")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/file/bot" + tg.Token + "/" + info.Path
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return errors.New("invalid Telegram download request")
	}
	response, err := client.Do(req)
	if err != nil {
		return redactedTransportError(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > receipt.Size {
		return errors.New("Telegram whole-file download rejected or oversized")
	}
	return verifyRestoreSource(ctx, response.Body, out, receipt.SourceSHA, receipt.Size)
}

func restoreMediaEntry(ctx context.Context, root *os.Root, entry *zip.File, expected archiveMedia) error {
	out, name, err := newRestoreTemp(root, ".restore-media-")
	if err != nil {
		return err
	}
	defer root.Remove(name)
	in, err := entry.Open()
	if err == nil {
		err = verifyRestoreSource(ctx, in, out, expected.SHA256, expected.Size)
		in.Close()
	}
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return root.Rename(name, expected.SHA256)
}

func openRestoreRegular(root *os.Root, name string) (*os.File, error) {
	st, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("restore input is not a regular file")
	}
	return root.Open(name)
}

func newRestoreTemp(root *os.Root, prefix string) (*os.File, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, "", err
	}
	name := prefix + hex.EncodeToString(nonce[:])
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	return f, name, err
}

func restoreSHA(v string) bool {
	if len(v) != 64 || strings.ToLower(v) != v {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

func verifyRestoreSource(ctx context.Context, in io.Reader, out io.Writer, sha string, size int64) error {
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), io.LimitReader(restoreContextReader{ctx, in}, size+1))
	if err != nil {
		return err
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != sha {
		return errors.New("whole-file size or SHA-256 verification failed")
	}
	return ctx.Err()
}

type restoreContextReader struct {
	ctx context.Context
	in  io.Reader
}

func (r restoreContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.in.Read(p)
}
