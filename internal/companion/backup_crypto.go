package companion

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const (
	fullBackupMagic       = "JDCBK2\n"
	fullBackupKeyDomain   = "jardulik-service-backup:v2:key\x00"
	fullBackupChunkSize   = 1 << 20
	fullBackupMaxChunks   = 1 << 20
	fullBackupHeaderSize  = len(fullBackupMagic) + 32 + 4
	fullBackupAADSize     = fullBackupHeaderSize + 8 + 4 + 1
	fullBackupPlainLimit  = int64(fullBackupChunkSize) * fullBackupMaxChunks
	fullBackupCipherLimit = fullBackupPlainLimit + int64(fullBackupMaxChunks+1)*20 + int64(fullBackupHeaderSize)
)

var errFullBackupAuthentication = errors.New("invalid or unauthenticated full backup")

// V2 has a fixed header (magic, random 256-bit salt, big-endian chunk size),
// followed by records: uint32 plaintext length, then AES-GCM ciphertext/tag.
// A zero-length record is an authenticated, mandatory terminal, followed by EOF.
// The header, implicit sequence, length and terminal flag are AAD for each record.
// Per-file HMAC key derivation separates this use from session/v1 encryption;
// a random salt gives every file its own key. Counter nonces cannot repeat under
// that key; the chunk-count bound also prevents counter overflow.
func fullBackupAEAD(key, header []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("backup key must contain 32 bytes")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(fullBackupKeyDomain))
	_, _ = mac.Write(header[len(fullBackupMagic) : len(fullBackupMagic)+32])
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func fullBackupRecordContext(header []byte, sequence uint64, length uint32, nonce *[12]byte, aad *[fullBackupAADSize]byte) {
	binary.BigEndian.PutUint64(nonce[4:], sequence)
	copy(aad[:], header)
	binary.BigEndian.PutUint64(aad[fullBackupHeaderSize:], sequence)
	binary.BigEndian.PutUint32(aad[fullBackupHeaderSize+8:], length)
	aad[len(aad)-1] = 0
	if length == 0 {
		aad[len(aad)-1] = 1
	}
}

func writeFullBackupBytes(dst io.Writer, b []byte) error {
	n, err := dst.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

func sealFullBackup(dst io.Writer, src io.Reader, key []byte) error {
	header := make([]byte, fullBackupHeaderSize)
	copy(header, fullBackupMagic)
	if _, err := rand.Read(header[len(fullBackupMagic) : len(fullBackupMagic)+32]); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(header[len(header)-4:], fullBackupChunkSize)
	aead, err := fullBackupAEAD(key, header)
	if err != nil {
		return err
	}
	if err = writeFullBackupBytes(dst, header); err != nil {
		return err
	}
	buffer := make([]byte, fullBackupChunkSize+aead.Overhead())
	var nonce [12]byte
	var aad [fullBackupAADSize]byte
	var frame [4]byte
	var sequence uint64
	for {
		n, readErr := io.ReadFull(src, buffer[:fullBackupChunkSize])
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return readErr
		}
		if n > 0 {
			if sequence >= fullBackupMaxChunks {
				return errors.New("full backup exceeds stream size limit")
			}
			fullBackupRecordContext(header, sequence, uint32(n), &nonce, &aad)
			sealed := aead.Seal(buffer[:0], nonce[:], buffer[:n], aad[:])
			binary.BigEndian.PutUint32(frame[:], uint32(n))
			if err = writeFullBackupBytes(dst, frame[:]); err != nil {
				return err
			}
			if err = writeFullBackupBytes(dst, sealed); err != nil {
				return err
			}
			sequence++
		}
		if readErr == nil {
			continue
		}
		fullBackupRecordContext(header, sequence, 0, &nonce, &aad)
		sealed := aead.Seal(buffer[:0], nonce[:], nil, aad[:])
		clear(frame[:])
		if err = writeFullBackupBytes(dst, frame[:]); err != nil {
			return err
		}
		return writeFullBackupBytes(dst, sealed)
	}
}

// openFullBackup writes authenticated chunks to dst, but whole-file acceptance
// requires the terminal and EOF. Callers must keep dst private until success;
// openFullBackupFile supplies that staging/publication contract.
func openFullBackup(dst io.Writer, src io.Reader, key []byte) error {
	var header [fullBackupHeaderSize]byte
	if _, err := io.ReadFull(src, header[:]); err != nil || string(header[:len(fullBackupMagic)]) != fullBackupMagic || binary.BigEndian.Uint32(header[len(header)-4:]) != fullBackupChunkSize {
		return errFullBackupAuthentication
	}
	aead, err := fullBackupAEAD(key, header[:])
	if err != nil {
		return err
	}
	buffer := make([]byte, fullBackupChunkSize+aead.Overhead())
	var nonce [12]byte
	var aad [fullBackupAADSize]byte
	var frame [4]byte
	for sequence := uint64(0); sequence <= fullBackupMaxChunks; sequence++ {
		if _, err = io.ReadFull(src, frame[:]); err != nil {
			return errFullBackupAuthentication
		}
		length := binary.BigEndian.Uint32(frame[:])
		if length > fullBackupChunkSize || length != 0 && sequence == fullBackupMaxChunks {
			return errFullBackupAuthentication
		}
		sealed := buffer[:int(length)+aead.Overhead()]
		if _, err = io.ReadFull(src, sealed); err != nil {
			return errFullBackupAuthentication
		}
		fullBackupRecordContext(header[:], sequence, length, &nonce, &aad)
		plain, err := aead.Open(buffer[:0], nonce[:], sealed, aad[:])
		if err != nil {
			return errFullBackupAuthentication
		}
		if length == 0 {
			var extra [1]byte
			if _, err = io.ReadFull(src, extra[:]); err != io.EOF {
				return errFullBackupAuthentication
			}
			return nil
		}
		if err = writeFullBackupBytes(dst, plain); err != nil {
			return err
		}
	}
	return errFullBackupAuthentication
}

func sealFullBackupFile(src, dst string, key []byte) error {
	return transformFullBackupFile(src, dst, key, fullBackupPlainLimit, sealFullBackup)
}

func openFullBackupFile(src, dst string, key []byte) error {
	return transformFullBackupFile(src, dst, key, fullBackupCipherLimit, openFullBackup)
}

// Publish with Link rather than check-then-Rename: Link atomically refuses an
// existing destination, including a symlink. Temp and destination share a directory.
// No destination is visible until complete authentication, Sync and Close succeed.
func transformFullBackupFile(src, dst string, key []byte, inputLimit int64, transform func(io.Writer, io.Reader, []byte) error) error {
	if len(key) != 32 {
		return errors.New("backup key must contain 32 bytes")
	}
	if _, err := os.Lstat(dst); err == nil {
		return errors.New("backup destination already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > inputLimit {
		return errors.New("invalid or oversized backup source")
	}
	out, err := os.CreateTemp(filepath.Dir(dst), ".jdc-backup-*")
	if err != nil {
		return err
	}
	temp := out.Name()
	defer os.Remove(temp)
	defer out.Close()
	if err = transform(out, in, key); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	if err = os.Link(temp, dst); err != nil {
		return err
	}
	if err = os.Remove(temp); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(dst))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
