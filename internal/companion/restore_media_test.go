package companion

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type restoreMediaFixture struct {
	dest, sha string
	photo     []byte
	zip       []byte
	manifest  archiveManifest
	tg        *Telegram
	calls     atomic.Int64
	fileSize  int64
}

func newRestoreMediaFixture(t *testing.T) *restoreMediaFixture {
	t.Helper()
	ctx := context.Background()
	c, s := archiveTestStore(t)
	a := NewArchiver(c, s, nil, nil)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	version, sha, photo := archiveTestMedia(t, s, c, 99)
	p, err := s.Version(ctx, 123, version)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.MediaJobs(ctx, 123, version)
	if err != nil {
		t.Fatal(err)
	}
	profilePath, sourceSHA, _, err := a.profileZIP(ctx, p, 123, jobs)
	if err != nil {
		t.Fatal(err)
	}
	profileZIP, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	sent := SentMessage{ID: 42, Document: &Document{FileID: "whole-profile", UniqueID: "unique", Size: int64(len(profileZIP))}}
	if err = a.receipt(ctx, sourceSHA, "profile_version", profilePath, "", 7, sent); err != nil {
		t.Fatal(err)
	}
	servicePath, manifest, err := a.serviceZIP(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := seal(c.BackupKey, plain, serviceBackupPurpose)
	if err != nil {
		t.Fatal(err)
	}
	encryptedPath := filepath.Join(t.TempDir(), "service.zip.enc")
	if err = os.WriteFile(encryptedPath, append([]byte(serviceBackupMagic), sealed...), 0600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err = RestoreArchive(encryptedPath, dest, c.BackupKey); err != nil {
		t.Fatal(err)
	}
	// Exercise the large-snapshot recovery path with a small real archive.
	if err = os.Remove(filepath.Join(dest, "media", sha)); err != nil {
		t.Fatal(err)
	}
	f := &restoreMediaFixture{dest: dest, sha: sha, photo: photo, zip: profileZIP, manifest: manifest}
	f.tg = NewTelegram(c)
	f.tg.Base = "https://telegram.test.invalid"
	f.tg.HTTP = &http.Client{Transport: archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.calls.Add(1)
		if r.URL.Host != "telegram.test.invalid" {
			return nil, errors.New("unexpected download origin")
		}
		var body []byte
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/getFile") {
			size := int64(len(f.zip))
			if f.fileSize != 0 {
				size = f.fileSize
			}
			body = []byte(fmt.Sprintf(`{"ok":true,"result":{"file_path":"documents/profile.zip","file_size":%d}}`, size))
		} else if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/documents/profile.zip") {
			body = f.zip
		} else {
			return nil, errors.New("unexpected Telegram method")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
	})}
	return f
}

func (f *restoreMediaFixture) saveManifest(t *testing.T) {
	t.Helper()
	b, err := json.Marshal(f.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(f.dest, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *restoreMediaFixture) trustZIP(t *testing.T, b []byte) {
	t.Helper()
	f.zip = b
	f.manifest.Receipts[0].SourceSHA = digest(b)
	f.manifest.Receipts[0].Size = int64(len(b))
	f.manifest.Receipts[0].ReturnedSize = int64(len(b))
	f.saveManifest(t)
}

func TestRestoreMediaRecoversWholeProfileZIPAndLeavesSQLiteUnchanged(t *testing.T) {
	f := newRestoreMediaFixture(t)
	dbBefore, err := os.ReadFile(filepath.Join(f.dest, "jardulik.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := RestoreMedia(context.Background(), f.tg, f.dest)
	if err != nil || result.Expected != 1 || result.Restored != 1 || result.Verified != 1 || result.Missing != 0 || !result.ExpectedMediaComplete {
		t.Fatalf("recovery: %+v, %v", result, err)
	}
	got, err := os.ReadFile(filepath.Join(f.dest, "media", f.sha))
	if err != nil || !bytes.Equal(got, f.photo) {
		t.Fatalf("original photo changed: %v", err)
	}
	dbAfter, err := os.ReadFile(filepath.Join(f.dest, "jardulik.sqlite"))
	if err != nil || !bytes.Equal(dbBefore, dbAfter) {
		t.Fatal("media recovery modified SQLite")
	}
	if f.calls.Load() != 2 {
		t.Fatalf("expected one getFile and one whole-file download, got %d calls", f.calls.Load())
	}
	result, err = RestoreMedia(context.Background(), nil, f.dest)
	if err != nil || result.Restored != 0 || result.Verified != 1 || !result.ExpectedMediaComplete {
		t.Fatalf("existing verified original was not skipped: %+v %v", result, err)
	}
}

func TestRestoreMediaAcceptsOmittedObserverAndLegacyIdentity(t *testing.T) {
	for _, observer := range []int64{0, 123, -1} {
		t.Run(fmt.Sprint(observer), func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			z, err := zip.NewReader(bytes.NewReader(f.zip), int64(len(f.zip)))
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			w := zip.NewWriter(&out)
			for _, entry := range z.File {
				in, err := entry.Open()
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(in)
				in.Close()
				if err != nil {
					t.Fatal(err)
				}
				if entry.Name == "manifest.json" {
					var manifest map[string]any
					if err = json.Unmarshal(b, &manifest); err != nil {
						t.Fatal(err)
					}
					delete(manifest, "observer_id")
					if observer != 0 {
						manifest["observer_id"] = observer
					}
					b, err = json.Marshal(manifest)
					if err != nil {
						t.Fatal(err)
					}
				}
				writer, err := w.Create(entry.Name)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = writer.Write(b); err != nil {
					t.Fatal(err)
				}
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			f.trustZIP(t, out.Bytes())
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if observer < 0 {
				if err == nil || result.ExpectedMediaComplete || result.Missing != 1 {
					t.Fatalf("negative observer accepted: %+v %v", result, err)
				}
				assertNoRecoveredMedia(t, f)
			} else if err != nil || !result.ExpectedMediaComplete || result.Restored != 1 {
				t.Fatalf("privacy or legacy archive rejected: %+v %v", result, err)
			}
		})
	}
}

func rewriteRestoreMediaNames(t *testing.T, f *restoreMediaFixture, extension string, aliases ...string) []byte {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(f.zip), int64(len(f.zip)))
	if err != nil {
		t.Fatal(err)
	}
	contents := map[string][]byte{}
	for _, entry := range z.File {
		in, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		contents[entry.Name], err = io.ReadAll(in)
		in.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	var manifest archiveManifest
	if err = json.Unmarshal(contents["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	newName := "media/" + f.sha + extension
	for i := range manifest.Files {
		if strings.HasPrefix(manifest.Files[i].Name, "media/") {
			manifest.Files[i].Name = newName
		}
	}
	var out bytes.Buffer
	w := zip.NewWriter(&out)
	write := func(name string, data []byte) {
		t.Helper()
		entry, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = entry.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range z.File {
		if entry.Name == "manifest.json" {
			continue
		}
		name := entry.Name
		if strings.HasPrefix(name, "media/") {
			name = newName
		}
		write(name, contents[entry.Name])
	}
	for _, alias := range aliases {
		name := "media/" + f.sha + alias
		write(name, f.photo)
		manifest.Files = append(manifest.Files, archiveFile{Name: name, SHA256: f.sha, ByteSize: int64(len(f.photo))})
	}
	b, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	write("manifest.json", b)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestRestoreMediaSupportsLegacyAndExtendedArchiveNames(t *testing.T) {
	for _, extension := range []string{"", ".jpg", ".webp"} {
		t.Run("extension="+extension, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			f.trustZIP(t, rewriteRestoreMediaNames(t, f, extension))
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err != nil || result.Restored != 1 || result.Verified != 1 || result.Missing != 0 || !result.ExpectedMediaComplete {
				t.Fatalf("archive compatibility failed: %+v %v", result, err)
			}
			got, err := os.ReadFile(filepath.Join(f.dest, "media", f.sha))
			if err != nil || !bytes.Equal(got, f.photo) {
				t.Fatalf("canonical media file changed: %v", err)
			}
			entries, err := os.ReadDir(filepath.Join(f.dest, "media"))
			if err != nil || len(entries) != 1 || entries[0].Name() != f.sha {
				t.Fatal("archive extension leaked into the application's canonical media paths")
			}
		})
	}
}

func TestRestoreMediaRejectsNormalizedSHAArchiveAliases(t *testing.T) {
	for _, tc := range []struct{ original, alias string }{{"", ".jpg"}, {".jpg", ".webp"}} {
		t.Run(tc.original+"+"+tc.alias, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			f.trustZIP(t, rewriteRestoreMediaNames(t, f, tc.original, tc.alias))
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err == nil || !strings.Contains(err.Error(), "duplicate normalized profile media SHA") || result.Restored != 0 || result.Missing != 1 || result.ExpectedMediaComplete {
				t.Fatalf("media aliases were accepted: %+v %v", result, err)
			}
			assertNoRecoveredMedia(t, f)
		})
	}
}

func TestRestoreMediaRejectsUnsupportedArchiveExtensions(t *testing.T) {
	for _, extension := range []string{".exe", ".jpg.exe"} {
		t.Run(extension, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			f.trustZIP(t, rewriteRestoreMediaNames(t, f, extension))
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err == nil || result.Restored != 0 || result.Missing != 1 || result.ExpectedMediaComplete {
				t.Fatalf("unsupported archive extension accepted: %+v %v", result, err)
			}
			assertNoRecoveredMedia(t, f)
		})
	}
}

func TestRestoreMediaRejectsCorruptedWholeDocumentBeforeExtraction(t *testing.T) {
	f := newRestoreMediaFixture(t)
	f.zip[len(f.zip)-1] ^= 1 // Receipt still identifies the original whole ZIP.
	result, err := RestoreMedia(context.Background(), f.tg, f.dest)
	if err == nil || result.Missing != 1 || result.Restored != 0 || result.ExpectedMediaComplete {
		t.Fatalf("corrupted document accepted: %+v %v", result, err)
	}
	assertNoRecoveredMedia(t, f)
}

func appendUnsafeProfileEntry(t *testing.T, original []byte, name string, symlink bool) []byte {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(original), int64(len(original)))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := zip.NewWriter(&out)
	for _, entry := range z.File {
		in, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		dst, err := w.Create(entry.Name)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(dst, in)
		in.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	header := &zip.FileHeader{Name: name}
	if symlink {
		header.SetMode(os.ModeSymlink | 0777)
	}
	entry, err := w.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("../../outside"))
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestRestoreMediaRejectsUnsafeProfileEntries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		symlink bool
	}{{"../outside", false}, {"media/" + strings.Repeat("a", 64), true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			f.trustZIP(t, appendUnsafeProfileEntry(t, f.zip, tc.name, tc.symlink))
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err == nil || result.Missing != 1 || result.ExpectedMediaComplete {
				t.Fatalf("unsafe profile archive accepted: %+v %v", result, err)
			}
			assertNoRecoveredMedia(t, f)
			if _, err = os.Stat(filepath.Join(filepath.Dir(f.dest), "outside")); !os.IsNotExist(err) {
				t.Fatal("profile archive wrote outside the destination")
			}
		})
	}
}

func TestRestoreMediaRejectsZIPBombMetadataBeforeExtraction(t *testing.T) {
	f := newRestoreMediaFixture(t)
	b := append([]byte(nil), f.zip...)
	central := bytes.Index(b, []byte{'P', 'K', 1, 2})
	if central < 0 {
		t.Fatal("fixture central directory missing")
	}
	binary.LittleEndian.PutUint32(b[central+24:central+28], serviceRestoreLimit+1)
	f.trustZIP(t, b)
	result, err := RestoreMedia(context.Background(), f.tg, f.dest)
	if !errors.Is(err, errArchiveBudget) || result.Missing != 1 || result.ExpectedMediaComplete {
		t.Fatalf("over-budget ZIP accepted: %+v %v", result, err)
	}
	assertNoRecoveredMedia(t, f)
}

func TestRestoreMediaRejectsDestinationSymlinksBeforeNetwork(t *testing.T) {
	for _, kind := range []string{"destination", "media", "file"} {
		t.Run(kind, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			outside := t.TempDir()
			marker := filepath.Join(outside, "keep")
			if err := os.WriteFile(marker, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			dest := f.dest
			switch kind {
			case "destination":
				dest = filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(f.dest, dest); err != nil {
					t.Fatal(err)
				}
			case "media":
				if err := os.Remove(filepath.Join(dest, "media")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(dest, "media")); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.Symlink(marker, filepath.Join(dest, "media", f.sha)); err != nil {
					t.Fatal(err)
				}
			}
			result, err := RestoreMedia(context.Background(), f.tg, dest)
			if err == nil || result.ExpectedMediaComplete || f.calls.Load() != 0 {
				t.Fatalf("unsafe destination reached the network: %+v %v calls=%d", result, err, f.calls.Load())
			}
			got, err := os.ReadFile(marker)
			if err != nil || string(got) != "untouched" {
				t.Fatal("symlink target was changed")
			}
		})
	}
}

func TestRestoreMediaReportsMissingAndUnsupportedReceipts(t *testing.T) {
	for _, kind := range []string{"missing", "oversized-receipt", "oversized-getFile", "inventory-size"} {
		t.Run(kind, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			switch kind {
			case "missing":
				f.manifest.Receipts = nil
			case "oversized-receipt":
				f.manifest.Receipts[0].Size = restoreDownloadLimit + 1
				f.manifest.Receipts[0].ReturnedSize = restoreDownloadLimit + 1
			case "oversized-getFile":
				f.fileSize = restoreDownloadLimit + 1
			case "inventory-size":
				f.manifest.Media[0].Size++
			}
			f.saveManifest(t)
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err == nil || result.Missing != 1 || result.Restored != 0 || result.ExpectedMediaComplete || len(result.SupportGaps) == 0 {
				t.Fatalf("gap was hidden: %+v %v", result, err)
			}
			if (kind == "missing" || kind == "oversized-receipt") && f.calls.Load() != 0 {
				t.Fatal("unsupported receipt caused an unnecessary download")
			}
			assertNoRecoveredMedia(t, f)
		})
	}
}

func TestRestoreMediaUsesReplacementAfterUnavailableHistoricalCopy(t *testing.T) {
	for _, failure := range []string{"getFile", "download", "oversized-receipt", "oversized-getFile"} {
		t.Run(failure, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			old := f.manifest.Receipts[0]
			old.FileID, old.MessageID = "historical-copy", 41
			if failure == "oversized-receipt" {
				old.SourceSHA = digest([]byte("old oversized whole ZIP"))
				old.Size, old.ReturnedSize = restoreDownloadLimit+1, restoreDownloadLimit+1
			}
			f.manifest.Receipts = append([]archiveReceipt{old}, f.manifest.Receipts...)
			f.saveManifest(t)
			base := f.tg.HTTP.Transport
			f.tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := ""
				status := 200
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/getFile") {
					b, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}
					r.Body = io.NopCloser(bytes.NewReader(b))
					var input map[string]string
					if err = json.Unmarshal(b, &input); err != nil {
						t.Fatal(err)
					}
					if input["file_id"] == "historical-copy" {
						switch failure {
						case "getFile":
							body = `{"ok":false,"error_code":400,"description":"historical file unavailable"}`
						case "download":
							body = fmt.Sprintf(`{"ok":true,"result":{"file_path":"documents/historical.zip","file_size":%d}}`, old.Size)
						case "oversized-getFile":
							body = fmt.Sprintf(`{"ok":true,"result":{"file_path":"documents/historical.zip","file_size":%d}}`, restoreDownloadLimit+1)
						default:
							t.Fatal("unsupported receipt reached Telegram")
						}
					}
				} else if strings.HasSuffix(r.URL.Path, "/documents/historical.zip") {
					status, body = 404, "copy unavailable"
				}
				if body != "" {
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
				}
				return base.RoundTrip(r)
			})
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err != nil || !result.ExpectedMediaComplete || result.Missing != 0 || result.Verified != result.Expected || result.Restored != 1 || len(result.SupportGaps) == 0 {
				t.Fatalf("replacement was incorrectly treated as incomplete: %+v %v", result, err)
			}
			b, err := os.ReadFile(filepath.Join(f.dest, "media", f.sha))
			if err != nil || !bytes.Equal(b, f.photo) {
				t.Fatal("replacement did not preserve the original media")
			}
		})
	}
}

func TestRestoreMediaKeepsIntegrityFailureAfterSuccessfulReplacement(t *testing.T) {
	for _, failure := range []string{"source-hash", "local-rename"} {
		t.Run(failure, func(t *testing.T) {
			f := newRestoreMediaFixture(t)
			old := f.manifest.Receipts[0]
			old.FileID, old.MessageID = "failed-copy", 41
			f.manifest.Receipts = append([]archiveReceipt{old}, f.manifest.Receipts...)
			f.saveManifest(t)
			base := f.tg.HTTP.Transport
			first := true
			blocked := filepath.Join(f.dest, "media", f.sha)
			f.tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(r)
				if err == nil && r.Method == http.MethodGet {
					if first {
						first = false
						if failure == "source-hash" {
							response.Body.Close()
							b := bytes.Clone(f.zip)
							b[len(b)-1] ^= 1
							response.Body = io.NopCloser(bytes.NewReader(b))
						} else if err = os.Mkdir(blocked, 0700); err != nil {
							t.Fatal(err)
						}
					} else if failure == "local-rename" {
						if err = os.Remove(blocked); err != nil {
							t.Fatal(err)
						}
					}
				}
				return response, err
			})
			result, err := RestoreMedia(context.Background(), f.tg, f.dest)
			if err == nil || result.ExpectedMediaComplete || result.Missing != 0 || result.Verified != result.Expected || result.Restored != 1 || len(result.SupportGaps) == 0 {
				t.Fatalf("successful replacement hid an integrity/write failure: %+v %v", result, err)
			}
		})
	}
}

func TestRestoreMediaKeepsIncompleteSnapshotCoverageExplicit(t *testing.T) {
	f := newRestoreMediaFixture(t)
	f.manifest.Coverage["pending_media"] = 2
	f.manifest.Coverage["failed_media"] = 1
	f.manifest.Coverage["source_complete"] = false
	f.saveManifest(t)
	result, err := RestoreMedia(context.Background(), f.tg, f.dest)
	if err != nil || !result.ExpectedMediaComplete || result.Missing != 0 || len(result.SupportGaps) == 0 {
		t.Fatalf("snapshot coverage lost: %+v %v", result, err)
	}
	if result.SnapshotCoverage["pending_media"] != float64(2) || result.SnapshotCoverage["failed_media"] != float64(1) || result.SnapshotCoverage["source_complete"] != false {
		t.Fatal("snapshot coverage was rewritten during media recovery")
	}
}

func TestRestoreMediaHonorsCancelledContext(t *testing.T) {
	f := newRestoreMediaFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := RestoreMedia(ctx, f.tg, f.dest)
	if !errors.Is(err, context.Canceled) || result.ExpectedMediaComplete || f.calls.Load() != 0 {
		t.Fatalf("cancelled restore performed work: %+v %v", result, err)
	}
	assertNoRecoveredMedia(t, f)
}

func assertNoRecoveredMedia(t *testing.T, f *restoreMediaFixture) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(f.dest, "media", f.sha)); !os.IsNotExist(err) {
		t.Fatal("failed verification materialized media")
	}
	for _, dir := range []string{f.dest, filepath.Join(f.dest, "media")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".restore-") {
				t.Fatal("temporary restore file was left behind")
			}
		}
	}
}
