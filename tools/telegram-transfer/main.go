// telegram-transfer transfers whole encrypted backups as the configured bot.
// It never performs user authentication or subscribes to Telegram updates.
package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	maxDocumentSize = int64(1 << 30)
	maxReceiptSize  = 64 << 10
	maxSessionSize  = 1 << 20
	partSize        = 512 << 10
	transferTimeout = 25 * time.Minute
	sessionMagic    = "JDTGS1\n"
)

type config struct {
	APIID                int
	APIHash, Token       string
	BotID, ChatID        int64
	UploadBytesPerSecond int64
	ThreadID             int
	SessionKey           []byte
	DataDir, SessionFile string
}

// IDs are signed integers from Telegram; document and random IDs are opaque longs.
// SourceSHA256 identifies the encrypted file, not the plaintext ZIP inside it.
type receipt struct {
	Status       string `json:"status"`
	Transport    string `json:"transport"`
	ChatID       int64  `json:"chat_id"`
	ThreadID     int    `json:"thread_id"`
	MessageID    int    `json:"message_id"`
	DocumentID   int64  `json:"document_id"`
	ByteSize     int64  `json:"byte_size"`
	SourceSHA256 string `json:"source_sha256"`
	RandomID     int64  `json:"random_id"`
	ErrorCode    string `json:"error_code,omitempty"`
	RetryAfter   int    `json:"retry_after,omitempty"`
}

type command struct {
	Mode, File, ReceiptFile, Output, SHA string
	Size, RandomID                       int64
	Saved                                receipt
}

type transferError struct{ code string }

func (e *transferError) Error() string { return e.code }
func fail(code string) error           { return &transferError{code} }

func loadConfig(getenv func(string) string) (c config, err error) {
	c.APIID, err = strconv.Atoi(getenv("TELEGRAM_API_ID"))
	if err != nil || c.APIID <= 0 || int64(c.APIID) > 1<<31-1 {
		return c, fail("invalid_api_credentials")
	}
	c.APIHash = getenv("TELEGRAM_API_HASH")
	if len(c.APIHash) != 32 {
		return c, fail("invalid_api_credentials")
	}
	if _, err = hex.DecodeString(c.APIHash); err != nil {
		return c, fail("invalid_api_credentials")
	}
	c.Token = getenv("TELEGRAM_BOT_TOKEN")
	prefix, secret, ok := strings.Cut(c.Token, ":")
	c.BotID, err = strconv.ParseInt(prefix, 10, 64)
	if !ok || err != nil || c.BotID <= 0 || len(secret) < 20 || len(c.Token) > 512 {
		return c, fail("invalid_bot_token")
	}
	for _, b := range secret {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-') {
			return c, fail("invalid_bot_token")
		}
	}
	c.ChatID, err = strconv.ParseInt(getenv("BACKUP_CHAT_ID"), 10, 64)
	if err != nil || c.ChatID >= -1_000_000_000_000 || c.ChatID < -(1<<52) {
		return c, fail("invalid_backup_target")
	}
	c.ThreadID, err = strconv.Atoi(getenv("BACKUP_THREAD_ID"))
	if err != nil || c.ThreadID <= 0 || int64(c.ThreadID) > 1<<31-1 {
		return c, fail("invalid_backup_target")
	}
	c.SessionKey, err = base64.StdEncoding.DecodeString(getenv("SESSION_KEY"))
	if err != nil || len(c.SessionKey) != 32 {
		return c, fail("invalid_session_key")
	}
	c.DataDir = filepath.Clean(getenv("APP_DATA_DIR"))
	if !filepath.IsAbs(c.DataDir) {
		return c, fail("invalid_data_directory")
	}
	c.SessionFile = getenv("TELEGRAM_TRANSFER_SESSION_FILE")
	if c.SessionFile == "" {
		c.SessionFile = filepath.Join(c.DataDir, "telegram-bot-session.enc")
	}
	c.SessionFile = filepath.Clean(c.SessionFile)
	rel, e := filepath.Rel(c.DataDir, c.SessionFile)
	if e != nil || !filepath.IsAbs(c.SessionFile) || !filepath.IsLocal(rel) || rel == "." {
		return c, fail("invalid_session_path")
	}
	if raw := strings.TrimSpace(getenv("TELEGRAM_TRANSFER_BYTES_PER_SECOND")); raw != "" {
		c.UploadBytesPerSecond, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || c.UploadBytesPerSecond < 0 || c.UploadBytesPerSecond > maxDocumentSize {
			return c, fail("invalid_upload_rate")
		}
	}
	return c, nil
}

func validateUploadRate(c config, cmd command) error {
	if cmd.Mode == "upload" && c.UploadBytesPerSecond > 0 && (cmd.Size+c.UploadBytesPerSecond-1)/c.UploadBytesPerSecond > int64(transferTimeout/time.Second) {
		return fail("upload_rate_exceeds_deadline")
	}
	return nil
}

func validSHA(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func parseCommand(args []string) (cmd command, err error) {
	if len(args) == 0 || args[0] != "upload" && args[0] != "download" {
		return cmd, fail("invalid_arguments")
	}
	cmd.Mode = args[0]
	flags := flag.NewFlagSet(cmd.Mode, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cmd.SHA, "sha256", "", "expected encrypted file SHA-256")
	flags.Int64Var(&cmd.Size, "size", 0, "expected encrypted file size")
	if cmd.Mode == "upload" {
		flags.StringVar(&cmd.File, "file", "", "encrypted backup path")
		flags.Int64Var(&cmd.RandomID, "random-id", 0, "persisted send random ID")
	} else {
		flags.StringVar(&cmd.ReceiptFile, "receipt", "", "saved MTProto receipt")
		flags.StringVar(&cmd.Output, "output", "", "new encrypted backup path")
	}
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || !validSHA(cmd.SHA) || cmd.Size <= 0 || cmd.Size > maxDocumentSize {
		return cmd, fail("invalid_arguments")
	}
	if cmd.Mode == "upload" && (!filepath.IsAbs(cmd.File) || cmd.RandomID == 0) || cmd.Mode == "download" && (!filepath.IsAbs(cmd.ReceiptFile) || !filepath.IsAbs(cmd.Output)) {
		return cmd, fail("invalid_arguments")
	}
	return cmd, nil
}

func readReceipt(path string) (value receipt, err error) {
	f, err := os.Open(path)
	if err != nil {
		return value, fail("invalid_receipt")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxReceiptSize {
		return value, fail("invalid_receipt")
	}
	d := json.NewDecoder(io.LimitReader(f, maxReceiptSize+1))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil {
		return value, fail("invalid_receipt")
	}
	var extra any
	if !errors.Is(d.Decode(&extra), io.EOF) {
		return value, fail("invalid_receipt")
	}
	return value, nil
}

func validateReceipt(value receipt, c config, cmd command) error {
	if value.Status != "sent" || value.Transport != "mtproto" || value.ChatID != c.ChatID || value.ThreadID != c.ThreadID || value.MessageID <= 0 || int64(value.MessageID) > 1<<31-1 || value.DocumentID == 0 || value.RandomID == 0 || value.ByteSize != cmd.Size || value.SourceSHA256 != cmd.SHA || value.ErrorCode != "" || value.RetryAfter != 0 {
		return fail("invalid_receipt")
	}
	return nil
}

func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return fail("session_storage_failed")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fail("session_storage_failed")
	}
	return nil
}

type encryptedSession struct {
	mu sync.Mutex
	c  config
}

func (s *encryptedSession) aead() (cipher.AEAD, []byte, error) {
	purpose := []byte(fmt.Sprintf("jardulik-telegram-transfer:bot-session:v1:%d:%d", s.c.BotID, s.c.APIID))
	mac := hmac.New(sha256.New, s.c.SessionKey)
	_, _ = mac.Write(append([]byte("jardulik-telegram-transfer:session-key:v1\x00"), purpose...))
	b, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, nil, fail("session_storage_failed")
	}
	aead, err := cipher.NewGCM(b)
	return aead, append([]byte(sessionMagic), purpose...), err
}

func (s *encryptedSession) LoadSession(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	info, err := os.Lstat(s.c.SessionFile)
	if os.IsNotExist(err) {
		return nil, session.ErrNotFound
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > maxSessionSize+128 || info.Size() < int64(len(sessionMagic)+12+16) {
		return nil, fail("session_storage_failed")
	}
	f, err := os.Open(s.c.SessionFile)
	if err != nil {
		return nil, fail("session_storage_failed")
	}
	defer f.Close()
	data := make([]byte, info.Size())
	if _, err = io.ReadFull(f, data); err != nil || string(data[:len(sessionMagic)]) != sessionMagic {
		return nil, fail("session_storage_failed")
	}
	var extra [1]byte
	if _, err = f.Read(extra[:]); err != io.EOF {
		return nil, fail("session_storage_failed")
	}
	aead, aad, err := s.aead()
	if err != nil {
		return nil, fail("session_storage_failed")
	}
	data = data[len(sessionMagic):]
	plain, err := aead.Open(nil, data[:aead.NonceSize()], data[aead.NonceSize():], aad)
	if err != nil || len(plain) > maxSessionSize {
		return nil, fail("session_storage_failed")
	}
	return plain, nil
}

func (s *encryptedSession) StoreSession(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(data) > maxSessionSize || privateDirectory(filepath.Dir(s.c.SessionFile)) != nil {
		return fail("session_storage_failed")
	}
	if info, err := os.Lstat(s.c.SessionFile); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) || err != nil && !os.IsNotExist(err) {
		return fail("session_storage_failed")
	}
	aead, aad, err := s.aead()
	if err != nil {
		return fail("session_storage_failed")
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return fail("session_storage_failed")
	}
	sealed := append([]byte(sessionMagic), nonce...)
	sealed = aead.Seal(sealed, nonce, data, aad)
	f, err := os.CreateTemp(filepath.Dir(s.c.SessionFile), ".telegram-session-*")
	if err != nil {
		return fail("session_storage_failed")
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(sealed); err != nil {
		return fail("session_storage_failed")
	}
	if err = f.Sync(); err != nil {
		return fail("session_storage_failed")
	}
	if err = f.Close(); err != nil {
		return fail("session_storage_failed")
	}
	if err = os.Rename(f.Name(), s.c.SessionFile); err != nil {
		return fail("session_storage_failed")
	}
	return syncDirectory(filepath.Dir(s.c.SessionFile))
}

func sessionLock(c config) (*os.File, error) {
	if privateDirectory(c.DataDir) != nil || privateDirectory(filepath.Dir(c.SessionFile)) != nil {
		return nil, fail("session_storage_failed")
	}
	path := c.SessionFile + ".lock"
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) || err != nil && !os.IsNotExist(err) {
		return nil, fail("session_storage_failed")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fail("session_storage_failed")
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fail("transfer_busy")
	}
	return f, nil
}

func botOptions(storage session.Storage) telegram.Options {
	return telegram.Options{NoUpdates: true, SessionStorage: storage}
}

func verifyBot(self *tg.User, c config) error {
	if self == nil || !self.Bot || self.ID != c.BotID {
		return fail("wrong_bot_identity")
	}
	return nil
}

func channelID(c config) int64 { return -1_000_000_000_000 - c.ChatID }

func messageDocument(message tg.MessageClass, c config, id int, expectedSize int64) (*tg.Document, error) {
	m, ok := message.(*tg.Message)
	if !ok || m.ID != id || m.Out == false {
		return nil, fail("invalid_message_receipt")
	}
	peer, ok := m.PeerID.(*tg.PeerChannel)
	if !ok || peer.ChannelID != channelID(c) {
		return nil, fail("invalid_message_receipt")
	}
	from, ok := m.FromID.(*tg.PeerUser)
	if !ok || from.UserID != c.BotID {
		return nil, fail("invalid_message_receipt")
	}
	reply, hasReply := m.ReplyTo.(*tg.MessageReplyHeader)
	topic := 1
	if hasReply && reply.ForumTopic {
		topic = reply.ReplyToTopID
		if topic == 0 {
			topic = reply.ReplyToMsgID
		}
	}
	if topic != c.ThreadID {
		return nil, fail("invalid_message_receipt")
	}
	media, ok := m.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, fail("invalid_document_receipt")
	}
	document, ok := media.Document.(*tg.Document)
	if !ok || document.ID == 0 || document.Size != expectedSize || document.Size <= 0 || document.Size > maxDocumentSize {
		return nil, fail("invalid_document_receipt")
	}
	return document, nil
}

func fetchDocument(ctx context.Context, api *tg.Client, c config, id int, expectedSize int64) (*tg.Document, error) {
	result, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: &tg.InputChannel{ChannelID: channelID(c), AccessHash: 0}, ID: []tg.InputMessageClass{&tg.InputMessageID{ID: id}}})
	if err != nil {
		return nil, err
	}
	modified, ok := result.AsModified()
	if !ok || len(modified.GetMessages()) != 1 {
		return nil, fail("invalid_message_receipt")
	}
	return messageDocument(modified.GetMessages()[0], c, id, expectedSize)
}

func sentMessageID(updates tg.UpdatesClass, c config, randomID int64) (int, error) {
	ids := map[int]bool{}
	mapped := map[int]bool{}
	sawMapping := false
	var visit func(tg.UpdateClass)
	visit = func(update tg.UpdateClass) {
		switch value := update.(type) {
		case *tg.UpdateMessageID:
			sawMapping = true
			if value.RandomID == randomID && value.ID > 0 {
				mapped[value.ID] = true
			}
		case *tg.UpdateNewChannelMessage:
			if m, ok := value.Message.(*tg.Message); ok {
				p, peerOK := m.PeerID.(*tg.PeerChannel)
				from, fromOK := m.FromID.(*tg.PeerUser)
				if peerOK && fromOK && p.ChannelID == channelID(c) && from.UserID == c.BotID && m.Out && m.ID > 0 {
					ids[m.ID] = true
				}
			}
		}
	}
	switch value := updates.(type) {
	case *tg.Updates:
		for _, update := range value.Updates {
			visit(update)
		}
	case *tg.UpdatesCombined:
		for _, update := range value.Updates {
			visit(update)
		}
	case *tg.UpdateShort:
		visit(value.Update)
	case *tg.UpdateShortSentMessage:
		if value.ID > 0 {
			ids[value.ID] = true
		}
	}
	if sawMapping {
		ids = mapped
	}
	if len(ids) != 1 {
		return 0, fail("unconfirmed_send")
	}
	for id := range ids {
		return id, nil
	}
	return 0, fail("unconfirmed_send")
}

type hashReader struct {
	io.Reader
	info os.FileInfo
	hash hash.Hash
	n    int64
}

// Scheduling accounts for elapsed network time and limits the reader before
// bytes reach the uploader. Context cancellation also interrupts a rate wait.
type uploadRateReader struct {
	ctx                   context.Context
	from                  io.Reader
	bytesPerSecond, total int64
	started               time.Time
	now                   func() time.Time
	wait                  func(context.Context, time.Duration) error
}

func waitUploadRate(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *uploadRateReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.from.Read(p)
	if n > 0 && r.bytesPerSecond > 0 {
		r.total += int64(n)
		// Input is bounded to 1 GiB + 1, so the nanosecond product cannot overflow.
		due := r.started.Add(time.Duration(r.total * int64(time.Second) / r.bytesPerSecond))
		if delay := due.Sub(r.now()); delay > 0 {
			if waitErr := r.wait(r.ctx, delay); waitErr != nil {
				return 0, waitErr
			}
		}
	}
	return n, err
}

func (r *hashReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	_, _ = r.hash.Write(b[:n])
	r.n += int64(n)
	return n, err
}
func (r *hashReader) Stat() (os.FileInfo, error) { return r.info, nil }

func uploadFile(ctx context.Context, api uploader.Client, cmd command, bytesPerSecond int64) (tg.InputFileClass, string, error) {
	f, err := os.Open(cmd.File)
	if err != nil {
		return nil, "", fail("invalid_source")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != cmd.Size {
		return nil, "", fail("invalid_source")
	}
	name := filepath.Base(cmd.File)
	if len(name) > 128 || strings.ContainsAny(name, "\\/") || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return nil, "", fail("invalid_source")
	}
	pre := sha256.New()
	if _, err = io.Copy(pre, io.LimitReader(f, cmd.Size+1)); err != nil || hex.EncodeToString(pre.Sum(nil)) != cmd.SHA {
		return nil, "", fail("source_hash_mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, "", fail("invalid_source")
	}
	limited := &uploadRateReader{ctx: ctx, from: io.LimitReader(f, cmd.Size+1), bytesPerSecond: bytesPerSecond, started: time.Now(), now: time.Now, wait: waitUploadRate}
	r := &hashReader{Reader: limited, info: info, hash: sha256.New()}
	file, err := uploader.NewUploader(api).WithPartSize(partSize).WithThreads(1).FromFile(ctx, r)
	if err != nil {
		return nil, "", err
	}
	if r.n != cmd.Size || hex.EncodeToString(r.hash.Sum(nil)) != cmd.SHA {
		return nil, "", fail("source_hash_mismatch")
	}
	return file, name, nil
}

type checkedWriter struct {
	out         io.Writer
	hash        hash.Hash
	size, limit int64
}

func (w *checkedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.size {
		return 0, fail("download_size_mismatch")
	}
	n, err := w.out.Write(p)
	_, _ = w.hash.Write(p[:n])
	w.size += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fail("storage_failed")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return fail("storage_failed")
	}
	return nil
}

func downloadFile(cmd command, stream func(io.Writer) error) error {
	if _, err := os.Lstat(cmd.Output); err == nil {
		return fail("destination_exists")
	} else if !os.IsNotExist(err) {
		return fail("storage_failed")
	}
	f, err := os.CreateTemp(filepath.Dir(cmd.Output), ".telegram-download-*")
	if err != nil {
		return fail("storage_failed")
	}
	defer os.Remove(f.Name())
	defer f.Close()
	w := &checkedWriter{out: f, hash: sha256.New(), limit: cmd.Size}
	if err = stream(w); err != nil {
		return err
	}
	if w.size != cmd.Size || hex.EncodeToString(w.hash.Sum(nil)) != cmd.SHA {
		return fail("download_hash_mismatch")
	}
	if f.Sync() != nil || f.Close() != nil {
		return fail("storage_failed")
	}
	if err = os.Link(f.Name(), cmd.Output); err != nil {
		return fail("destination_exists")
	}
	if os.Remove(f.Name()) != nil {
		return fail("storage_failed")
	}
	return syncDirectory(filepath.Dir(cmd.Output))
}

func safeError(err error, sendAttempted bool) (status, code string, retryAfter int) {
	status, code = "failed", "transfer_failed"
	if sendAttempted {
		status, code = "uncertain", "send_not_confirmed"
	}
	var own *transferError
	if errors.As(err, &own) {
		return status, own.code, 0
	}
	if rpc, ok := tgerr.As(err); ok {
		if rpc.Type == "FLOOD_WAIT" || rpc.Type == "FLOOD_PREMIUM_WAIT" {
			return status, "flood_wait", max(0, min(rpc.Argument, 7*24*60*60))
		}
		if rpc.Code == 400 || rpc.Code == 401 || rpc.Code == 403 {
			return status, "telegram_rejected", 0
		}
	}
	if errors.Is(err, context.Canceled) {
		code = "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
	}
	return status, code, 0
}

func transfer(ctx context.Context, c config, cmd command) receipt {
	result := receipt{Status: "failed", Transport: "mtproto", ChatID: c.ChatID, ThreadID: c.ThreadID, ByteSize: cmd.Size, SourceSHA256: cmd.SHA, RandomID: cmd.RandomID}
	if cmd.Mode == "download" {
		result = cmd.Saved
		result.Status = "failed"
	}
	lock, err := sessionLock(c)
	if err != nil {
		result.Status, result.ErrorCode, result.RetryAfter = safeError(err, false)
		return result
	}
	defer lock.Close()
	storage := &encryptedSession{c: c}
	client := telegram.NewClient(c.APIID, c.APIHash, botOptions(storage))
	sendAttempted := false
	err = client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			if _, err = client.Auth().Bot(ctx, c.Token); err != nil {
				return err
			}
		}
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		if err = verifyBot(self, c); err != nil {
			return err
		}
		if cmd.Mode == "download" {
			doc, err := fetchDocument(ctx, client.API(), c, cmd.Saved.MessageID, cmd.Size)
			if err != nil {
				return err
			}
			if doc.ID != cmd.Saved.DocumentID {
				return fail("invalid_document_receipt")
			}
			if err = downloadFile(cmd, func(w io.Writer) error {
				_, err := downloader.NewDownloader().WithPartSize(partSize).Download(client.API(), doc.AsInputDocumentFileLocation("")).Stream(ctx, w)
				return err
			}); err != nil {
				return err
			}
			result.Status = "downloaded"
			return nil
		}
		file, name, err := uploadFile(ctx, client.API(), cmd, c.UploadBytesPerSecond)
		if err != nil {
			return err
		}
		request := &tg.MessagesSendMediaRequest{Peer: &tg.InputPeerChannel{ChannelID: channelID(c), AccessHash: 0},
			RandomID: cmd.RandomID, Silent: true,
			Media:   &tg.InputMediaUploadedDocument{File: file, MimeType: "application/octet-stream", ForceFile: true, Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: name}}},
			Message: "Jardulik Companion: полный зашифрованный архив. SHA-256: " + cmd.SHA}
		if c.ThreadID != 1 {
			request.ReplyTo = &tg.InputReplyToMessage{ReplyToMsgID: c.ThreadID}
		}
		sendAttempted = true
		updates, err := client.API().MessagesSendMedia(ctx, request)
		if err != nil {
			return err
		}
		id, err := sentMessageID(updates, c, cmd.RandomID)
		if err != nil {
			return err
		}
		result.MessageID = id // A locator is still useful if readback becomes uncertain.
		doc, err := fetchDocument(ctx, client.API(), c, id, cmd.Size)
		if err != nil {
			return err
		}
		result.Status, result.MessageID, result.DocumentID = "sent", id, doc.ID
		return nil
	})
	if result.Status == "sent" || result.Status == "downloaded" {
		return result
	}
	if err != nil {
		result.Status, result.ErrorCode, result.RetryAfter = safeError(err, sendAttempted)
	}
	return result
}

func writeReceipt(out io.Writer, result receipt) error {
	data, err := json.Marshal(result)
	if err != nil || len(data)+1 > maxReceiptSize {
		return fail("receipt_encoding_failed")
	}
	data = append(data, '\n')
	n, err := out.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) int {
	result := receipt{Status: "failed", Transport: "mtproto"}
	cmd, err := parseCommand(args)
	if err == nil {
		var c config
		c, err = loadConfig(getenv)
		if err == nil {
			err = validateUploadRate(c, cmd)
		}
		if err == nil && cmd.Mode == "download" {
			cmd.Saved, err = readReceipt(cmd.ReceiptFile)
			if err == nil {
				err = validateReceipt(cmd.Saved, c, cmd)
			}
			cmd.RandomID = cmd.Saved.RandomID
		}
		if err == nil {
			ctx, cancel := context.WithTimeout(ctx, transferTimeout)
			defer cancel()
			result = transfer(ctx, c, cmd)
		}
	}
	if err != nil {
		result.Status, result.ErrorCode, result.RetryAfter = safeError(err, false)
	}
	if writeReceipt(out, result) != nil {
		return 1
	}
	if result.Status == "sent" || result.Status == "downloaded" {
		return 0
	}
	return 1
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout)
	cancel()
	os.Exit(code)
}
