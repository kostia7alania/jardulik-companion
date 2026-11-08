package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

func testEnvironment(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"TELEGRAM_API_ID": "12345", "TELEGRAM_API_HASH": strings.Repeat("a", 32),
		"TELEGRAM_BOT_TOKEN": "123456:" + strings.Repeat("b", 35),
		"SESSION_KEY":        base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x19}, 32)),
		"APP_DATA_DIR":       dir, "BACKUP_CHAT_ID": "-1009876543210", "BACKUP_THREAD_ID": "42",
	}
}

func testConfig(t *testing.T) config {
	t.Helper()
	env := testEnvironment(t)
	c, err := loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testSHA(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func TestConfigKeepsCredentialsInEnvironmentAndTargetWhitelisted(t *testing.T) {
	env := testEnvironment(t)
	c, err := loadConfig(func(k string) string { return env[k] })
	if err != nil || c.BotID != 123456 || c.ChatID != -1009876543210 || c.ThreadID != 42 || channelID(c) != 9876543210 || c.SessionFile != filepath.Join(c.DataDir, "telegram-bot-session.enc") {
		t.Fatalf("config contract: %v", err)
	}
	for key, value := range map[string]string{
		"TELEGRAM_API_ID": "2147483648", "TELEGRAM_API_HASH": "not-a-hash",
		"TELEGRAM_BOT_TOKEN": "123456:credential\nheader", "SESSION_KEY": "wrong-key",
		"APP_DATA_DIR": "relative", "BACKUP_CHAT_ID": "123456", "BACKUP_THREAD_ID": "0",
		"TELEGRAM_TRANSFER_SESSION_FILE": filepath.Join(t.TempDir(), "outside.enc"),
	} {
		t.Run(key, func(t *testing.T) {
			copy := map[string]string{}
			for k, v := range env {
				copy[k] = v
			}
			copy[key] = value
			if _, err := loadConfig(func(k string) string { return copy[k] }); err == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
}

func TestArgumentsRequirePersistedRandomIDAndRejectTargetOrSecretFlags(t *testing.T) {
	file := filepath.Join(t.TempDir(), "snapshot.zip.enc")
	args := []string{"upload", "--file", file, "--random-id", "-9123", "--sha256", testSHA([]byte("blob")), "--size", "4"}
	cmd, err := parseCommand(args)
	if err != nil || cmd.RandomID != -9123 || cmd.File != file {
		t.Fatal("explicit random ID was not preserved")
	}
	for _, extra := range [][]string{{"--token", "secret"}, {"--chat-id", "42"}, {"--thread-id", "9"}, {"--random-id", "0"}, {"--size", "1073741825"}, {"unexpected"}} {
		if _, err := parseCommand(append(append([]string(nil), args...), extra...)); err == nil {
			t.Fatalf("unsafe arguments accepted: %s", extra[0])
		}
	}
	if _, err := parseCommand([]string{"login"}); err == nil {
		t.Fatal("user login command exists")
	}
}

func TestFailuresNeverEchoCredentialsPathsOrRPCMessages(t *testing.T) {
	env := testEnvironment(t)
	secret := env["TELEGRAM_BOT_TOKEN"]
	var out bytes.Buffer
	if run(context.Background(), []string{"upload", "--token", secret}, func(k string) string { return env[k] }, &out) == 0 {
		t.Fatal("invalid flags succeeded")
	}
	if strings.Contains(out.String(), secret) || strings.Contains(out.String(), "--token") || strings.Contains(out.String(), env["APP_DATA_DIR"]) {
		t.Fatal("CLI echoed untrusted arguments or secrets")
	}
	for _, input := range []error{errors.New("POST /bot" + secret + "/sendMedia /private/source"), tgerr.New(403, "PRIVATE_"+secret), context.DeadlineExceeded} {
		status, code, wait := safeError(input, true)
		if status != "uncertain" {
			t.Fatal("post-send error was treated as a definite failure")
		}
		out.Reset()
		if err := writeReceipt(&out, receipt{Status: status, Transport: "mtproto", ErrorCode: code, RetryAfter: wait}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), secret) || strings.Contains(out.String(), "private/source") {
			t.Fatal("error serialization leaked transport data")
		}
	}
	status, code, wait := safeError(tgerr.New(420, "FLOOD_WAIT_31"), false)
	if status != "failed" || code != "flood_wait" || wait != 31 {
		t.Fatal("flood wait classification lost")
	}
}

func TestEncryptedBotSessionIsAtomicPrivateAndDomainBound(t *testing.T) {
	c := testConfig(t)
	s := &encryptedSession{c: c}
	ctx := context.Background()
	if _, err := s.LoadSession(ctx); !errors.Is(err, session.ErrNotFound) {
		t.Fatal("missing session not identified")
	}
	plain := []byte(`{"AuthKey":"private fixture session material"}`)
	if err := s.StoreSession(ctx, plain); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := os.ReadFile(c.SessionFile)
	if err != nil || bytes.Contains(ciphertext, plain) || !bytes.HasPrefix(ciphertext, []byte(sessionMagic)) {
		t.Fatal("session was not encrypted")
	}
	if info, err := os.Stat(c.SessionFile); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("session permissions are not private")
	}
	loaded, err := s.LoadSession(ctx)
	if err != nil || !bytes.Equal(loaded, plain) {
		t.Fatal("session round trip failed")
	}
	for name, change := range map[string]func(*config){
		"bot": func(c *config) { c.BotID++ }, "app": func(c *config) { c.APIID++ },
		"key": func(c *config) { c.SessionKey = bytes.Repeat([]byte{0x20}, 32) },
	} {
		t.Run(name, func(t *testing.T) {
			other := c
			change(&other)
			if _, err := (&encryptedSession{c: other}).LoadSession(ctx); err == nil {
				t.Fatal("wrong identity/key accepted")
			}
		})
	}
	if err := s.StoreSession(ctx, []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	newCiphertext, err := os.ReadFile(c.SessionFile)
	if err != nil || bytes.Equal(newCiphertext, ciphertext) {
		t.Fatal("session update reused ciphertext")
	}
	if err = s.StoreSession(ctx, make([]byte, maxSessionSize+1)); err == nil {
		t.Fatal("oversized session accepted")
	}
	if after, err := os.ReadFile(c.SessionFile); err != nil || !bytes.Equal(after, newCiphertext) {
		t.Fatal("rejected session update replaced previous state")
	}
	newCiphertext[len(newCiphertext)-1] ^= 1
	if err = os.WriteFile(c.SessionFile, newCiphertext, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadSession(ctx); err == nil {
		t.Fatal("tampered session accepted")
	}
	staging, err := filepath.Glob(filepath.Join(c.DataDir, ".telegram-session-*"))
	if err != nil || len(staging) != 0 {
		t.Fatal("private session staging was retained")
	}
}

func TestSessionRejectsSymlinksPublicPermissionsAndConcurrentUse(t *testing.T) {
	c := testConfig(t)
	lock, err := sessionLock(c)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := sessionLock(c); err == nil {
		second.Close()
		t.Fatal("concurrent session use accepted")
	}
	lock.Close()
	target := filepath.Join(c.DataDir, "other")
	if err = os.WriteFile(target, []byte("do not replace"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, c.SessionFile); err != nil {
		t.Fatal(err)
	}
	s := &encryptedSession{c: c}
	if err = s.StoreSession(context.Background(), []byte("new")); err == nil {
		t.Fatal("session symlink followed")
	}
	if err = os.Remove(c.SessionFile); err != nil {
		t.Fatal(err)
	}
	if err = s.StoreSession(context.Background(), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(c.SessionFile, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadSession(context.Background()); err == nil {
		t.Fatal("public session accepted")
	}
}

func TestBotIdentityAndNoUpdatesAreMandatory(t *testing.T) {
	c := testConfig(t)
	options := botOptions(&encryptedSession{c: c})
	if !options.NoUpdates || options.UpdateHandler != nil || options.SessionStorage == nil {
		t.Fatal("helper can subscribe to updates")
	}
	if err := verifyBot(&tg.User{ID: c.BotID, Bot: true}, c); err != nil {
		t.Fatal(err)
	}
	for _, self := range []*tg.User{nil, {ID: c.BotID}, {ID: c.BotID + 1, Bot: true}} {
		if err := verifyBot(self, c); err == nil {
			t.Fatal("user or wrong bot identity accepted")
		}
	}
}

func testMessage(c config) *tg.Message {
	return &tg.Message{ID: 77, Out: true, PeerID: &tg.PeerChannel{ChannelID: channelID(c)}, FromID: &tg.PeerUser{UserID: c.BotID}, ReplyTo: &tg.MessageReplyHeader{ForumTopic: true, ReplyToMsgID: c.ThreadID}, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: -987, Size: 4}}}
}

func TestReceiptDocumentMustBelongToExactBotChannelAndTopic(t *testing.T) {
	c := testConfig(t)
	if doc, err := messageDocument(testMessage(c), c, 77, 4); err != nil || doc.ID != -987 {
		t.Fatal("valid signed document ID rejected")
	}
	for name, mutate := range map[string]func(*tg.Message){
		"incoming": func(m *tg.Message) { m.Out = false }, "message": func(m *tg.Message) { m.ID++ },
		"peer":        func(m *tg.Message) { m.PeerID = &tg.PeerChannel{ChannelID: 42} },
		"sender":      func(m *tg.Message) { m.FromID = &tg.PeerUser{UserID: c.BotID + 1} },
		"topic":       func(m *tg.Message) { m.ReplyTo = &tg.MessageReplyHeader{ForumTopic: true, ReplyToMsgID: 91} },
		"no_topic":    func(m *tg.Message) { m.ReplyTo = nil },
		"size":        func(m *tg.Message) { m.Media = &tg.MessageMediaDocument{Document: &tg.Document{ID: 9, Size: 5}} },
		"no_document": func(m *tg.Message) { m.Media = &tg.MessageMediaPhoto{} },
	} {
		t.Run(name, func(t *testing.T) {
			m := testMessage(c)
			mutate(m)
			if _, err := messageDocument(m, c, 77, 4); err == nil {
				t.Fatal("foreign receipt accepted")
			}
		})
	}
	general := c
	general.ThreadID = 1
	if _, err := messageDocument(testMessage(c), general, 77, 4); err == nil {
		t.Fatal("non-General topic accepted as General")
	}
	m := testMessage(general)
	m.ReplyTo = nil
	if _, err := messageDocument(m, general, 77, 4); err != nil {
		t.Fatal("General topic rejected")
	}
}

func TestSendReceiptUsesServerMappingForExactRandomID(t *testing.T) {
	c := testConfig(t)
	const randomID = int64(-9123)
	updates := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 77, RandomID: randomID}, &tg.UpdateNewChannelMessage{Message: testMessage(c)}}}
	if id, err := sentMessageID(updates, c, randomID); err != nil || id != 77 {
		t.Fatal("server mapping lost")
	}
	if _, err := sentMessageID(updates, c, randomID+1); err == nil {
		t.Fatal("wrong random ID mapping accepted")
	}
	other := testMessage(c)
	other.ID = 78
	updates.Updates = append(updates.Updates, &tg.UpdateNewChannelMessage{Message: other})
	if id, err := sentMessageID(updates, c, randomID); err != nil || id != 77 {
		t.Fatal("another message displaced exact random ID mapping")
	}
	if _, err := sentMessageID(&tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 77, RandomID: randomID}, &tg.UpdateMessageID{ID: 78, RandomID: randomID}}}, c, randomID); err == nil {
		t.Fatal("ambiguous mapping accepted")
	}
	if id, err := sentMessageID(&tg.UpdateShortSentMessage{ID: 77}, c, randomID); err != nil || id != 77 {
		t.Fatal("short server response rejected before fresh readback")
	}
}

func TestReceiptJSONIsBoundedAndWhitelisted(t *testing.T) {
	c := testConfig(t)
	cmd := command{SHA: testSHA([]byte("blob")), Size: 4}
	r := receipt{Status: "sent", Transport: "mtproto", ChatID: c.ChatID, ThreadID: c.ThreadID, MessageID: 77, DocumentID: -987, ByteSize: 4, SourceSHA256: cmd.SHA, RandomID: -9123}
	if err := validateReceipt(r, c, cmd); err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.ThreadID++
	if err := validateReceipt(bad, c, cmd); err == nil {
		t.Fatal("foreign target receipt accepted")
	}
	var out bytes.Buffer
	if err := writeReceipt(&out, r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	decoded, err := readReceipt(path)
	if err != nil || decoded != r {
		t.Fatal("receipt round trip failed")
	}
	if err := os.WriteFile(path, append(out.Bytes(), []byte(`{"extra":true}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readReceipt(path); err == nil {
		t.Fatal("trailing receipt JSON accepted")
	}
	out.Reset()
	r.ErrorCode = strings.Repeat("x", maxReceiptSize)
	if err := writeReceipt(&out, r); err == nil || out.Len() != 0 {
		t.Fatal("oversized receipt emitted")
	}
}

type uploadSink struct {
	hash hash.Hash
	size int64
	call func() error
}

func (s *uploadSink) save(data []byte) (bool, error) {
	if s.call != nil {
		fn := s.call
		s.call = nil
		if err := fn(); err != nil {
			return false, err
		}
	}
	_, _ = s.hash.Write(data)
	s.size += int64(len(data))
	return true, nil
}
func (s *uploadSink) UploadSaveFilePart(_ context.Context, r *tg.UploadSaveFilePartRequest) (bool, error) {
	return s.save(r.Bytes)
}
func (s *uploadSink) UploadSaveBigFilePart(_ context.Context, r *tg.UploadSaveBigFilePartRequest) (bool, error) {
	return s.save(r.Bytes)
}

func TestUploadStreamsAndDetectsSourceChangesBeforeSending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.zip.enc")
	data := bytes.Repeat([]byte{0x27}, 12<<20)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := command{File: path, Size: int64(len(data)), SHA: testSHA(data)}
	sink := &uploadSink{hash: sha256.New()}
	uploaded, name, err := uploadFile(context.Background(), sink, cmd, 0)
	if err != nil || name != "fixture.zip.enc" || sink.size != cmd.Size || hex.EncodeToString(sink.hash.Sum(nil)) != cmd.SHA {
		t.Fatalf("streamed upload mismatch: %v", err)
	}
	big, ok := uploaded.(*tg.InputFileBig)
	if !ok || big.ID == 0 || big.Parts != (len(data)+partSize-1)/partSize || big.Name != "fixture.zip.enc" {
		t.Fatal("large upload did not produce the correct InputFileBig contract")
	}
	sink = &uploadSink{hash: sha256.New(), call: func() error {
		f, err := os.OpenFile(path, os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err = f.WriteAt([]byte{0x28}, int64(len(data)-1)); err != nil {
			return err
		}
		return nil
	}}
	if _, _, err := uploadFile(context.Background(), sink, cmd, 0); err == nil {
		t.Fatal("source changed during upload but send was permitted")
	}
}

func TestDownloadVerifiesHashAndPublishesWithoutOverwrite(t *testing.T) {
	data := []byte("whole encrypted backup bytes")
	dir := t.TempDir()
	path := filepath.Join(dir, "download.enc")
	cmd := command{Output: path, Size: int64(len(data)), SHA: testSHA(data)}
	stream := func(w io.Writer) error { _, err := w.Write(data); return err }
	if err := downloadFile(cmd, stream); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
		t.Fatal("download changed bytes")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("download is public")
	}
	if err := downloadFile(cmd, stream); err == nil {
		t.Fatal("existing file overwritten")
	}
	for _, name := range []string{"short", "oversized", "wrong_hash", "error", "race"} {
		t.Run(name, func(t *testing.T) {
			output := filepath.Join(dir, name+".enc")
			bad := cmd
			bad.Output = output
			err := downloadFile(bad, func(w io.Writer) error {
				switch name {
				case "short":
					_, err := w.Write(data[:len(data)-1])
					return err
				case "oversized":
					_, err := w.Write(append(bytes.Clone(data), 0))
					return err
				case "wrong_hash":
					_, err := w.Write(bytes.Repeat([]byte{0}, len(data)))
					return err
				case "error":
					_, _ = w.Write(data[:3])
					return errors.New("injected failure")
				case "race":
					if err := os.WriteFile(output, []byte("other writer"), 0600); err != nil {
						return err
					}
					return stream(w)
				}
				return nil
			})
			if err == nil {
				t.Fatal("invalid download accepted")
			}
			if name != "race" {
				if _, err := os.Lstat(output); !os.IsNotExist(err) {
					t.Fatal("partial download was published")
				}
			} else {
				if got, err := os.ReadFile(output); err != nil || string(got) != "other writer" {
					t.Fatal("raced destination overwritten")
				}
			}
		})
	}
	staging, err := filepath.Glob(filepath.Join(dir, ".telegram-download-*"))
	if err != nil || len(staging) != 0 {
		t.Fatal("failed downloads retained staging files")
	}
}

func TestLargeUploadAllocationsStayBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.enc")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(64 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	cmd := command{File: path, Size: 64 << 20, SHA: hex.EncodeToString(h.Sum(nil))}
	sink := &uploadSink{hash: sha256.New()}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	peak.Store(before.HeapAlloc)
	done, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				for old := peak.Load(); current.HeapAlloc > old; old = peak.Load() {
					if peak.CompareAndSwap(old, current.HeapAlloc) {
						break
					}
				}
			}
		}
	}()
	_, _, err = uploadFile(context.Background(), sink, cmd, 0)
	close(done)
	<-sampled
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	growth := peak.Load() - before.HeapAlloc
	t.Logf("64MiB offline upload: total allocations=%d, sampled heap growth=%d", allocated, growth)
	if growth > 32<<20 || sink.size != cmd.Size {
		t.Fatal("upload buffers whole source")
	}
}

func TestUploadRateConfigAndDeadlineBudget(t *testing.T) {
	env := testEnvironment(t)
	for _, value := range []string{"", "0", "1048576", "1073741824"} {
		env["TELEGRAM_TRANSFER_BYTES_PER_SECOND"] = value
		if _, err := loadConfig(func(key string) string { return env[key] }); err != nil {
			t.Fatalf("valid rate rejected: %q", value)
		}
	}
	for _, value := range []string{"-1", "1073741825", "not-a-number"} {
		env["TELEGRAM_TRANSFER_BYTES_PER_SECOND"] = value
		if _, err := loadConfig(func(key string) string { return env[key] }); err == nil {
			t.Fatal("unbounded rate config accepted")
		}
	}
	cmd := command{Mode: "upload", Size: 60_000_000}
	if err := validateUploadRate(config{UploadBytesPerSecond: 1 << 20}, cmd); err != nil {
		t.Fatal(err)
	}
	if err := validateUploadRate(config{UploadBytesPerSecond: 1}, cmd); err == nil {
		t.Fatal("upload cannot fit deadline but was accepted")
	}
	cmd.Mode = "download"
	if err := validateUploadRate(config{UploadBytesPerSecond: 1}, cmd); err != nil {
		t.Fatal("upload rate affected download")
	}
}

func TestUploadRateReaderUsesElapsedTimeWithoutRealWaiting(t *testing.T) {
	started := time.Unix(100, 0)
	now := started
	var waited time.Duration
	reader := &uploadRateReader{ctx: context.Background(), from: strings.NewReader(strings.Repeat("x", 1000)), bytesPerSecond: 1000, started: started, now: func() time.Time { return now }, wait: func(_ context.Context, delay time.Duration) error { waited += delay; now = now.Add(delay); return nil }}
	buffer := make([]byte, 250)
	var total int
	for {
		n, err := reader.Read(buffer)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 1000 || waited != time.Second || now.Sub(started) != time.Second {
		t.Fatalf("rate budget mismatch: bytes=%d waited=%s", total, waited)
	}
	// Existing transfer/network latency already satisfies the bandwidth budget.
	now = started.Add(2 * time.Second)
	waited = 0
	reader.from = strings.NewReader(strings.Repeat("x", 1000))
	reader.total = 0
	if _, err := io.Copy(io.Discard, reader); err != nil || waited != 0 {
		t.Fatal("limiter added delay after budget was already satisfied")
	}
}

func TestUploadRateWaitStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := strings.NewReader("do not consume")
	reader := &uploadRateReader{ctx: ctx, from: source}
	if n, err := reader.Read(make([]byte, 32)); n != 0 || !errors.Is(err, context.Canceled) || source.Len() != len("do not consume") {
		t.Fatal("cancelled rate reader consumed input")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := waitUploadRate(ctx, time.Hour); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatal("rate wait ignored context deadline")
	}
}

type targetCheckRPC struct {
	c          config
	channel    tg.ChatClass
	message    tg.MessageClass
	err        error
	resolveErr error
	calls      int
}

func (r *targetCheckRPC) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	r.calls++
	if request, ok := input.(*tg.ChannelsGetChannelsRequest); ok {
		if len(request.ID) != 1 {
			return errors.New("unexpected peer discovery scope")
		}
		channel, ok := request.ID[0].(*tg.InputChannel)
		if !ok || channel.ChannelID != channelID(r.c) || channel.AccessHash != 0 {
			return errors.New("unexpected peer bootstrap target")
		}
		if r.resolveErr != nil {
			return r.resolveErr
		}
		result, ok := output.(*tg.MessagesChatsBox)
		if !ok {
			return errors.New("unexpected peer bootstrap response")
		}
		result.Chats = &tg.MessagesChats{Chats: []tg.ChatClass{r.channel}}
		return nil
	}
	request, ok := input.(*tg.ChannelsGetMessagesRequest)
	if !ok {
		return errors.New("unexpected diagnostic method")
	}
	channel, ok := request.Channel.(*tg.InputChannel)
	if !ok || channel.ChannelID != channelID(r.c) || channel.AccessHash != 987654321 || len(request.ID) != 1 {
		return errors.New("unexpected diagnostic target")
	}
	id, ok := request.ID[0].(*tg.InputMessageID)
	if !ok || id.ID != r.c.ThreadID {
		return errors.New("unexpected diagnostic message")
	}
	if r.err != nil {
		return r.err
	}
	result, ok := output.(*tg.MessagesMessagesBox)
	if !ok {
		return errors.New("unexpected diagnostic response")
	}
	result.Messages = &tg.MessagesChannelMessages{Messages: []tg.MessageClass{r.message}}
	return nil
}

func testFullChannel(c config) *tg.Channel {
	channel := &tg.Channel{ID: channelID(c), Forum: true, Megagroup: true}
	channel.SetAccessHash(987654321)
	return channel
}

func TestResolveTargetRequiresExactAccessibleForumAndFullHash(t *testing.T) {
	c := testConfig(t)
	rpc := &targetCheckRPC{c: c, channel: testFullChannel(c)}
	target, err := resolveTarget(context.Background(), tg.NewClient(rpc), c)
	if err != nil || !validResolvedTarget(c, target) || target.AccessHash != 987654321 || rpc.calls != 1 {
		t.Fatal("known peer resolution failed")
	}
	for name, mutate := range map[string]func(*tg.Channel){
		"wrong_id": func(c *tg.Channel) { c.ID++ }, "not_forum": func(c *tg.Channel) { c.Forum = false },
		"not_group": func(c *tg.Channel) { c.Megagroup = false }, "left": func(c *tg.Channel) { c.Left = true },
		"min": func(c *tg.Channel) { c.Min = true }, "zero_hash": func(c *tg.Channel) { c.SetAccessHash(0) },
		"missing_hash_flag": func(c *tg.Channel) { c.Flags.Unset(13) },
	} {
		t.Run(name, func(t *testing.T) {
			channel := testFullChannel(c)
			mutate(channel)
			rpc := &targetCheckRPC{c: c, channel: channel}
			if _, err := resolveTarget(context.Background(), tg.NewClient(rpc), c); err == nil || rpc.calls != 1 {
				t.Fatal("unsafe peer accepted or broad discovery performed")
			}
		})
	}
	for _, channel := range []tg.ChatClass{&tg.ChannelForbidden{ID: channelID(c), AccessHash: 987654321}, &tg.ChatEmpty{ID: channelID(c)}} {
		rpc := &targetCheckRPC{c: c, channel: channel}
		if _, err := resolveTarget(context.Background(), tg.NewClient(rpc), c); err == nil {
			t.Fatal("forbidden or non-channel peer accepted")
		}
	}
	rpc = &targetCheckRPC{c: c, resolveErr: tgerr.New(400, "CHANNEL_INVALID")}
	_, err = resolveTarget(context.Background(), tg.NewClient(rpc), c)
	var diagnostic receipt
	diagnosticFailure(&diagnostic, err, "resolve_target", false)
	if rpc.calls != 1 || diagnostic.ErrorCode != "resolve_target_channel_invalid" || diagnostic.Status != "failed" {
		t.Fatal("failed peer bootstrap was bypassed or obscured")
	}
}

func TestCheckTargetOnlyReadsKnownTopicCreationMessage(t *testing.T) {
	c := testConfig(t)
	c.ChatID, c.ThreadID = -1004401653948, 2
	if channelID(c) != 4401653948 {
		t.Fatal("Bot API to MTProto channel conversion truncated the ID")
	}
	message := &tg.MessageService{ID: 2, PeerID: &tg.PeerChannel{ChannelID: 4401653948}, Action: &tg.MessageActionTopicCreate{Title: "private title must not be printed"}}
	rpc := &targetCheckRPC{c: c, channel: testFullChannel(c), message: message}
	api := tg.NewClient(rpc)
	target, err := resolveTarget(context.Background(), api, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTarget(context.Background(), api, c, target); err != nil || rpc.calls != 2 {
		t.Fatalf("bounded target probe failed: %v", err)
	}
	for _, wrong := range []tg.MessageClass{&tg.MessageEmpty{ID: 2}, &tg.Message{ID: 2}, &tg.MessageService{ID: 3, PeerID: message.PeerID, Action: message.Action}, &tg.MessageService{ID: 2, PeerID: &tg.PeerChannel{ChannelID: 7}, Action: message.Action}, &tg.MessageService{ID: 2, PeerID: message.PeerID}} {
		rpc = &targetCheckRPC{c: c, message: wrong}
		if err := checkTarget(context.Background(), tg.NewClient(rpc), c, target); err == nil || rpc.calls != 1 {
			t.Fatal("unconfirmed topic probe accepted or scanned extra messages")
		}
	}
	general := c
	general.ThreadID = 1
	if err := checkTarget(context.Background(), nil, general, nil); err == nil {
		t.Fatal("General diagnostic guessed a creation message")
	}
	if cmd, err := parseCommand([]string{"check-target"}); err != nil || cmd.Mode != "check-target" {
		t.Fatal("standalone target diagnostic command rejected")
	}
	for _, args := range [][]string{{"check-target", "--chat-id", "9"}, {"check-target", "--file", "/private/any"}, {"check-target", "--thread-id", "99"}} {
		if _, err := parseCommand(args); err == nil {
			t.Fatal("diagnostic target/file override accepted")
		}
	}
}

func TestDiagnosticFailureExposesOnlyPhaseAndKnownRPCNames(t *testing.T) {
	r := receipt{MessageID: 77}
	diagnosticFailure(&r, tgerr.New(400, "PEER_ID_INVALID"), "send_media", true)
	if r.Status != "uncertain" || r.Phase != "send_media" || r.RPCType != "PEER_ID_INVALID" || r.ErrorCode != "send_media_peer_id_invalid" || r.MessageID != 77 {
		t.Fatal("safe diagnostic lost phase, server locator or uncertainty")
	}
	diagnosticFailure(&r, tgerr.New(403, "CHAT_ADMIN_REQUIRED"), "check_target", false)
	if r.Status != "failed" || r.ErrorCode != "check_target_chat_admin_required" {
		t.Fatal("read-only diagnostic classification lost")
	}
	secret := "SENSITIVE_" + strings.Repeat("A", 32)
	for _, err := range []error{tgerr.New(400, secret), errors.New("/bot123456:secret /private/path " + secret), tgerr.New(400, "")} {
		diagnosticFailure(&r, err, "/private/secret-phase", true)
		var out bytes.Buffer
		if writeReceipt(&out, r) != nil || r.Status != "uncertain" || r.Phase != "transfer" || r.RPCType != "" || strings.Contains(out.String(), secret) || strings.Contains(out.String(), "private/") {
			t.Fatal("diagnostic echoed untrusted error/phase data")
		}
	}
}
