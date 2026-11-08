package companion

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func fullBackupTestKey() []byte { return bytes.Repeat([]byte{0x37}, 32) }

func fullBackupTestDigest(b []byte) string {
	sha := sha256.Sum256(b)
	return hex.EncodeToString(sha[:])
}

func fullBackupTestFileDigest(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func fullBackupTestSeal(t *testing.T, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := sealFullBackup(&out, bytes.NewReader(plain), fullBackupTestKey()); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func fullBackupTestRecords(t *testing.T, sealed []byte) [][]byte {
	t.Helper()
	var records [][]byte
	for at := fullBackupHeaderSize; at < len(sealed); {
		if len(sealed)-at < 4 {
			t.Fatal("short test frame")
		}
		end := at + 4 + int(binary.BigEndian.Uint32(sealed[at:])) + 16
		if end > len(sealed) {
			t.Fatal("short test ciphertext")
		}
		records = append(records, sealed[at:end])
		at = end
	}
	return records
}

func TestFullBackupCryptoRoundTripAndRandomFileKeys(t *testing.T) {
	for _, size := range []int{0, 1, fullBackupChunkSize - 1, fullBackupChunkSize, fullBackupChunkSize + 7, 2 * fullBackupChunkSize} {
		plain := bytes.Repeat([]byte{0x69}, size)
		sealed := fullBackupTestSeal(t, plain)
		var out bytes.Buffer
		if err := openFullBackup(&out, bytes.NewReader(sealed), fullBackupTestKey()); err != nil || !bytes.Equal(out.Bytes(), plain) {
			t.Fatalf("round trip size %d: %v", size, err)
		}
		other := fullBackupTestSeal(t, plain)
		if bytes.Equal(sealed, other) || bytes.Equal(sealed[len(fullBackupMagic):fullBackupHeaderSize-4], other[len(fullBackupMagic):fullBackupHeaderSize-4]) {
			t.Fatal("file salt/key was reused")
		}
		records := fullBackupTestRecords(t, sealed)
		if len(records) != (size+fullBackupChunkSize-1)/fullBackupChunkSize+1 || binary.BigEndian.Uint32(records[len(records)-1]) != 0 {
			t.Fatal("missing authenticated terminal")
		}
	}
}

func TestFullBackupCryptoRejectsTamperingFramingAndTruncation(t *testing.T) {
	sealed := fullBackupTestSeal(t, bytes.Repeat([]byte{0xa5}, fullBackupChunkSize+21))
	records := fullBackupTestRecords(t, sealed)
	join := func(parts ...[]byte) []byte {
		out := append([]byte(nil), sealed[:fullBackupHeaderSize]...)
		for _, part := range parts {
			out = append(out, part...)
		}
		return out
	}
	mutate := func(at int) []byte {
		out := bytes.Clone(sealed)
		out[at] ^= 1
		return out
	}
	tooLong := bytes.Clone(sealed)
	binary.BigEndian.PutUint32(tooLong[fullBackupHeaderSize:], fullBackupChunkSize+1)
	badChunkSize := bytes.Clone(sealed)
	binary.BigEndian.PutUint32(badChunkSize[fullBackupHeaderSize-4:], ^uint32(0))
	other := fullBackupTestSeal(t, bytes.Repeat([]byte{0x5a}, fullBackupChunkSize+21))
	otherRecords := fullBackupTestRecords(t, other)
	for name, input := range map[string][]byte{
		"magic": mutate(0), "salt": mutate(len(fullBackupMagic)),
		"chunk_size": badChunkSize, "oversized_record": tooLong,
		"record_length":     mutate(fullBackupHeaderSize + 3),
		"ciphertext":        mutate(fullBackupHeaderSize + 4),
		"data_tag":          mutate(fullBackupHeaderSize + len(records[0]) - 1),
		"terminal_tag":      mutate(len(sealed) - 1),
		"short_header":      sealed[:fullBackupHeaderSize-1],
		"short_frame":       sealed[:fullBackupHeaderSize+2],
		"short_ciphertext":  sealed[:fullBackupHeaderSize+19],
		"missing_terminal":  sealed[:len(sealed)-len(records[2])],
		"short_terminal":    sealed[:len(sealed)-1],
		"append":            append(bytes.Clone(sealed), 0),
		"append_terminal":   append(bytes.Clone(sealed), records[2]...),
		"reorder":           join(records[1], records[0], records[2]),
		"duplicate":         join(records[0], records[0], records[1], records[2]),
		"remove":            join(records[1], records[2]),
		"splice_other_file": join(otherRecords[0], records[1], records[2]),
	} {
		t.Run(name, func(t *testing.T) {
			if err := openFullBackup(io.Discard, bytes.NewReader(input), fullBackupTestKey()); !errors.Is(err, errFullBackupAuthentication) {
				t.Fatalf("altered stream accepted or unexpected error: %v", err)
			}
		})
	}
	wrongKey := bytes.Repeat([]byte{0x38}, 32)
	if err := openFullBackup(io.Discard, bytes.NewReader(sealed), wrongKey); !errors.Is(err, errFullBackupAuthentication) {
		t.Fatalf("wrong key accepted: %v", err)
	}
}

func TestFullBackupFilesPublishOnlyAfterCompleteAuthentication(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "plain.zip")
	sealed := filepath.Join(dir, "backup.enc")
	restored := filepath.Join(dir, "restored.zip")
	plain := bytes.Repeat([]byte{0x47}, fullBackupChunkSize+17)
	if err := os.WriteFile(source, plain, 0600); err != nil {
		t.Fatal(err)
	}
	if err := sealFullBackupFile(source, sealed, fullBackupTestKey()); err != nil {
		t.Fatal(err)
	}
	if err := openFullBackupFile(sealed, restored, fullBackupTestKey()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{sealed, restored} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private file mode missing for %s: %v", path, err)
		}
	}
	before, _, err := fullBackupTestFileDigest(restored)
	if err != nil || before != fullBackupTestDigest(plain) {
		t.Fatal("restored source changed")
	}
	if err = openFullBackupFile(sealed, restored, fullBackupTestKey()); err == nil {
		t.Fatal("existing destination was overwritten")
	}
	if err = sealFullBackupFile(source, source, fullBackupTestKey()); err == nil {
		t.Fatal("source was overwritten")
	}
	if after, _, err := fullBackupTestFileDigest(source); err != nil || after != fullBackupTestDigest(plain) {
		t.Fatal("original source bytes were changed")
	}
	link := filepath.Join(dir, "existing-symlink")
	if err = os.Symlink(filepath.Join(dir, "missing-target"), link); err != nil {
		t.Fatal(err)
	}
	if err = openFullBackupFile(sealed, link, fullBackupTestKey()); err == nil {
		t.Fatal("existing symlink was replaced")
	}
	bad := fullBackupTestSeal(t, plain)
	bad[len(bad)-1] ^= 1
	badPath := filepath.Join(dir, "bad.enc")
	if err = os.WriteFile(badPath, bad, 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "unverified.zip")
	if err = openFullBackupFile(badPath, missing, fullBackupTestKey()); err == nil {
		t.Fatal("bad terminal accepted")
	}
	if _, err = os.Lstat(missing); !os.IsNotExist(err) {
		t.Fatal("partly authenticated plaintext was published")
	}
	failure := errors.New("injected transform failure")
	if err = transformFullBackupFile(source, missing, fullBackupTestKey(), fullBackupPlainLimit, func(w io.Writer, _ io.Reader, _ []byte) error {
		_, _ = w.Write([]byte("private partial plaintext"))
		return failure
	}); !errors.Is(err, failure) {
		t.Fatalf("transform failure lost: %v", err)
	}
	raceTarget := filepath.Join(dir, "created-during-transform.zip")
	if err = transformFullBackupFile(source, raceTarget, fullBackupTestKey(), fullBackupPlainLimit, func(w io.Writer, _ io.Reader, _ []byte) error {
		if _, err := w.Write([]byte("verified output")); err != nil {
			return err
		}
		return os.WriteFile(raceTarget, []byte("other writer"), 0600)
	}); err == nil {
		t.Fatal("destination created during transform was replaced")
	}
	if sha, _, err := fullBackupTestFileDigest(raceTarget); err != nil || sha != fullBackupTestDigest([]byte("other writer")) {
		t.Fatal("atomic publication changed the other writer's destination")
	}
	staging, err := filepath.Glob(filepath.Join(dir, ".jdc-backup-*"))
	if err != nil || len(staging) != 0 {
		t.Fatal("failed operation left private staging files")
	}
}

func TestFullBackupLargeFileUsesBoundedMemory(t *testing.T) {
	const size = 64 << 20
	dir := t.TempDir()
	source := filepath.Join(dir, "large.zip")
	f, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(size); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	sealed, restored := filepath.Join(dir, "large.enc"), filepath.Join(dir, "restored.zip")
	for _, operation := range []struct {
		name string
		run  func() error
	}{{"seal", func() error { return sealFullBackupFile(source, sealed, fullBackupTestKey()) }}, {"open", func() error { return openFullBackupFile(sealed, restored, fullBackupTestKey()) }}} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if err = operation.run(); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		allocated := after.TotalAlloc - before.TotalAlloc
		t.Logf("%s: %d bytes allocated for a 64MiB source", operation.name, allocated)
		if allocated > 12<<20 {
			t.Fatalf("%s allocated %d bytes for a 64MiB source", operation.name, allocated)
		}
	}
	a, n, err := fullBackupTestFileDigest(source)
	if err != nil || n != size {
		t.Fatal(err)
	}
	b, n, err := fullBackupTestFileDigest(restored)
	if err != nil || n != size || a != b {
		t.Fatal("large restore does not match source")
	}
}
