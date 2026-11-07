package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type telegramAlbumTransport func(*http.Request) (*http.Response, error)

func (f telegramAlbumTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func albumTestPhotos(t *testing.T) ([]string, [][]byte) {
	t.Helper()
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "first image.jpg"), filepath.Join(dir, "второе.png")}
	contents := [][]byte{{0, 1, 2, 255}, []byte("second exact local source")}
	for i, path := range paths {
		if err := os.WriteFile(path, contents[i], 0600); err != nil {
			t.Fatal(err)
		}
	}
	return paths, contents
}

func albumTestResponse(r *http.Request, code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func TestSendAlbumStreamsMatchingMultipartAndFirstCaption(t *testing.T) {
	paths, contents := albumTestPhotos(t)
	caption := strings.Repeat("Я", 1024)
	var calls int
	tg := NewTelegram(Config{Token: "test-only-token", TelegramBase: "https://telegram.test.invalid"})
	tg.HTTP.Transport = telegramAlbumTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/sendMediaGroup") {
			t.Error("wrong album method")
		}
		contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || contentType != "multipart/form-data" {
			return nil, errors.New("missing multipart content type")
		}
		fields := map[string]string{}
		files := map[string][]byte{}
		names := map[string]string{}
		reader := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			b, err := io.ReadAll(part)
			if err != nil {
				return nil, err
			}
			if part.FileName() == "" {
				fields[part.FormName()] = string(b)
			} else {
				files[part.FormName()] = b
				names[part.FormName()] = part.FileName()
			}
		}
		if len(fields) != 3 || fields["chat_id"] != "123" || fields["message_thread_id"] != "77" {
			t.Errorf("wrong routing fields: %#v", fields)
		}
		var media []map[string]string
		if err := json.Unmarshal([]byte(fields["media"]), &media); err != nil {
			return nil, err
		}
		if len(media) != len(paths) || len(files) != len(paths) {
			return nil, errors.New("wrong number of multipart files")
		}
		for i, item := range media {
			field := "photo" + strconv.Itoa(i)
			if item["type"] != "photo" || item["media"] != "attach://"+field || !bytes.Equal(files[field], contents[i]) || names[field] != filepath.Base(paths[i]) {
				t.Errorf("photo %d did not match its attachment", i)
			}
			if i == 0 && item["caption"] != caption {
				t.Error("first photo lost the profile caption")
			}
			if i > 0 {
				if _, present := item["caption"]; present {
					t.Error("caption was repeated on another photo")
				}
			}
		}
		return albumTestResponse(r, 200, `{"ok":true,"result":[{"message_id":41,"message_thread_id":77,"chat":{"id":123}},{"message_id":42,"message_thread_id":77,"chat":{"id":123}}]}`), nil
	})
	before := tg.HTTP.Timeout
	messages, err := tg.SendAlbum(context.Background(), 123, 77, paths, caption)
	if err != nil || len(messages) != 2 || messages[0].ID != 41 || messages[1].ID != 42 || calls != 1 {
		t.Fatalf("album result: messages=%#v calls=%d error=%v", messages, calls, err)
	}
	if tg.HTTP.Timeout != before {
		t.Fatal("upload changed the long-poll client timeout")
	}
}

func TestSendAlbumRejectsInvalidFilesAndCaptionBeforeNetwork(t *testing.T) {
	paths, _ := albumTestPhotos(t)
	large := filepath.Join(t.TempDir(), "large.jpg")
	f, err := os.Create(large)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate((9 << 20) + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	var calls int
	tg := NewTelegram(Config{TelegramBase: "https://telegram.test.invalid"})
	tg.HTTP.Transport = telegramAlbumTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected network")
	})
	for _, tc := range []struct {
		name, caption string
		paths         []string
	}{
		{name: "empty"},
		{name: "one", paths: paths[:1]},
		{name: "eleven", paths: make([]string, 11)},
		{name: "missing_second", paths: []string{paths[0], filepath.Join(t.TempDir(), "missing.jpg")}},
		{name: "directory", paths: []string{paths[0], t.TempDir()}},
		{name: "oversize", paths: []string{paths[0], large}},
		{name: "remote_url", paths: []string{paths[0], "https://example.invalid/photo.jpg"}},
		{name: "caption", paths: paths, caption: strings.Repeat("Я", 1025)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tg.SendAlbum(context.Background(), 123, 0, tc.paths, tc.caption); err == nil {
				t.Fatal("invalid album accepted")
			}
		})
	}
	if calls != 0 {
		t.Fatalf("invalid local input caused %d requests", calls)
	}
}

func TestSendAlbumDistinguishesRejectionAndUnknownWithoutRetry(t *testing.T) {
	paths, _ := albumTestPhotos(t)
	for _, explicit := range []bool{true, false} {
		t.Run(fmt.Sprintf("explicit_%t", explicit), func(t *testing.T) {
			tg := NewTelegram(Config{Token: "123:secret-token", TelegramBase: "https://telegram.test.invalid"})
			calls := 0
			tg.HTTP.Transport = telegramAlbumTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if !explicit {
					return nil, &url.Error{Op: "Post", URL: r.URL.String(), Err: fmt.Errorf("failed %s", tg.Token)}
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return nil, err
				}
				body, _ := json.Marshal(map[string]any{"ok": false, "error_code": 400, "description": "photo rejected " + tg.Token + " " + url.QueryEscape(tg.Token)})
				return albumTestResponse(r, 400, string(body)), nil
			})
			_, err := tg.SendAlbum(context.Background(), 123, 0, paths, "")
			if err == nil || calls != 1 {
				t.Fatalf("failure was hidden or retried: calls=%d error=%v", calls, err)
			}
			var rejected *TelegramError
			if errors.As(err, &rejected) != explicit || (explicit && rejected.Code != 400) {
				t.Fatalf("failure classification: %T %v", err, err)
			}
			if strings.Contains(err.Error(), tg.Token) || strings.Contains(err.Error(), url.QueryEscape(tg.Token)) {
				t.Fatal("error exposed the bot token")
			}
		})
	}
}

func TestSendAlbumRejectsIncompleteOrWrongReceipts(t *testing.T) {
	paths, _ := albumTestPhotos(t)
	for name, result := range map[string]string{
		"missing_message": `[{"message_id":41,"chat":{"id":123}}]`,
		"missing_id":      `[{"chat":{"id":123}},{"message_id":42,"chat":{"id":123}}]`,
		"wrong_chat":      `[{"message_id":41,"chat":{"id":456}},{"message_id":42,"chat":{"id":123}}]`,
		"duplicate_id":    `[{"message_id":41,"chat":{"id":123}},{"message_id":41,"chat":{"id":123}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			tg := NewTelegram(Config{TelegramBase: "https://telegram.test.invalid"})
			calls := 0
			tg.HTTP.Transport = telegramAlbumTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return nil, err
				}
				return albumTestResponse(r, 200, `{"ok":true,"result":`+result+`}`), nil
			})
			messages, err := tg.SendAlbum(context.Background(), 123, 0, paths, "")
			if err == nil || calls != 1 || len(messages) == 0 {
				t.Fatalf("invalid receipts accepted, retried or discarded: calls=%d messages=%#v error=%v", calls, messages, err)
			}
			var rejected *TelegramError
			if errors.As(err, &rejected) {
				t.Fatal("unknown delivery presented as a confirmed API rejection")
			}
		})
	}
}

func TestSendAlbumCancellationStopsBlockedMultipartWriter(t *testing.T) {
	paths, _ := albumTestPhotos(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tg := NewTelegram(Config{Token: "test-only-token", TelegramBase: "https://telegram.test.invalid"})
	entered := make(chan struct{})
	var calls atomic.Int64
	tg.HTTP.Transport = telegramAlbumTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		close(entered)
		// Do not read the pipe: cleanup must release its blocked writer too.
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	done := make(chan error, 1)
	go func() { _, err := tg.SendAlbum(ctx, 123, 0, paths, ""); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("cancellation lost or retried: calls=%d error=%v", calls.Load(), err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation leaked the blocked multipart writer")
	}
}

func TestSendMediaCardKeepsPhotoCaptionAndKeyboardInOneRequest(t *testing.T) {
	paths, contents := albumTestPhotos(t)
	caption := "Пример, 28\nО себе: исходное описание"
	keyboard := map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "Следующая анкета", "callback_data": "next"}}}}
	for _, document := range []bool{false, true} {
		t.Run(fmt.Sprintf("document_%t", document), func(t *testing.T) {
			field, method := "photo", "sendPhoto"
			if document {
				field, method = "document", "sendDocument"
			}
			calls := 0
			tg := NewTelegram(Config{Token: "test-only-token", TelegramBase: "https://telegram.test.invalid"})
			tg.HTTP.Transport = telegramAlbumTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if !strings.HasSuffix(r.URL.Path, "/"+method) {
					t.Error("card used another API method")
				}
				_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
				if err != nil {
					return nil, err
				}
				reader := multipart.NewReader(r.Body, params["boundary"])
				fields := map[string][]byte{}
				for {
					part, err := reader.NextPart()
					if err == io.EOF {
						break
					}
					if err != nil {
						return nil, err
					}
					b, err := io.ReadAll(part)
					if err != nil {
						return nil, err
					}
					fields[part.FormName()] = b
				}
				if string(fields["chat_id"]) != "123" || string(fields["message_thread_id"]) != "77" || string(fields["caption"]) != caption || !bytes.Equal(fields[field], contents[0]) {
					t.Error("media, profile caption and routing were not included together")
				}
				var markup struct {
					Buttons [][]map[string]string `json:"inline_keyboard"`
				}
				if err := json.Unmarshal(fields["reply_markup"], &markup); err != nil || len(markup.Buttons) != 1 || len(markup.Buttons[0]) != 1 || markup.Buttons[0][0]["callback_data"] != "next" {
					t.Error("card lost its inline keyboard")
				}
				return albumTestResponse(r, 200, `{"ok":true,"result":{"message_id":42,"chat":{"id":123}}}`), nil
			})
			var message SentMessage
			var err error
			if document {
				message, err = tg.SendDocumentCard(context.Background(), 123, 77, paths[0], caption, keyboard)
			} else {
				message, err = tg.SendPhotoCard(context.Background(), 123, 77, paths[0], caption, keyboard)
			}
			if err != nil || message.ID != 42 || calls != 1 {
				t.Fatalf("one media card failed: calls=%d message=%#v error=%v", calls, message, err)
			}
		})
	}
}
