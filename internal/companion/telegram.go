package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Telegram struct {
	Token, Base string
	HTTP        *http.Client
}
type TelegramError struct {
	Code        int
	Description string
	RetryAfter  int
}

func (e *TelegramError) Error() string {
	return fmt.Sprintf("Telegram HTTP %d: %s", e.Code, e.Description)
}

type TGUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	IsBot    bool   `json:"is_bot"`
}
type TGChat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	IsForum  bool   `json:"is_forum"`
	Username string `json:"username"`
}
type Document struct {
	FileID   string `json:"file_id"`
	UniqueID string `json:"file_unique_id"`
	Size     int64  `json:"file_size"`
	Name     string `json:"file_name"`
}
type SentMessage struct {
	ID       int64     `json:"message_id"`
	ThreadID int64     `json:"message_thread_id"`
	Document *Document `json:"document"`
	Chat     TGChat    `json:"chat"`
}
type Message struct {
	ID            int64  `json:"message_id"`
	ThreadID      int64  `json:"message_thread_id"`
	From          TGUser `json:"from"`
	Chat          TGChat `json:"chat"`
	Text          string `json:"text"`
	ForwardOrigin *struct {
		Type   string `json:"type"`
		Sender TGUser `json:"sender_user"`
	} `json:"forward_origin"`
}
type Callback struct {
	ID      string  `json:"id"`
	From    TGUser  `json:"from"`
	Data    string  `json:"data"`
	Message Message `json:"message"`
}
type Update struct {
	ID       int64     `json:"update_id"`
	Message  *Message  `json:"message"`
	Callback *Callback `json:"callback_query"`
}

func NewTelegram(c Config) *Telegram {
	return &Telegram{Token: c.Token, Base: strings.TrimRight(c.TelegramBase, "/"), HTTP: &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (t *Telegram) decode(res *http.Response, out any) error {
	defer res.Body.Close()
	var envelope struct {
		OK          bool            `json:"ok"`
		Result      json.RawMessage `json:"result"`
		Code        int             `json:"error_code"`
		Description string          `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 16<<20)).Decode(&envelope); err != nil {
		return errors.New("Telegram response interrupted or invalid")
	}
	if !envelope.OK {
		return &TelegramError{envelope.Code, envelope.Description, envelope.Parameters.RetryAfter}
	}
	if out != nil {
		return json.Unmarshal(envelope.Result, out)
	}
	return nil
}
func (t *Telegram) Call(ctx context.Context, method string, payload, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, "POST", t.Base+"/bot"+t.Token+"/"+method, bytes.NewReader(b))
	if err != nil {
		return errors.New("invalid Telegram request")
	}
	r.Header.Set("Content-Type", "application/json")
	res, err := t.HTTP.Do(r)
	if err != nil {
		return fmt.Errorf("Telegram transport: %w", redactedTransportError(err))
	}
	return t.decode(res, out)
}
func (t *Telegram) Upload(ctx context.Context, method string, chat, thread int64, path, caption, field string) (SentMessage, error) {
	var out SentMessage
	file, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return out, err
	}
	limit := int64(49 << 20)
	if field == "photo" {
		limit = 9 << 20
	}
	if info.Size() > limit {
		return out, errors.New("whole file exceeds Telegram upload limit")
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	multipartWriter := multipart.NewWriter(writer)
	go func() {
		fail := func(e error) { _ = writer.CloseWithError(e) }
		if err := multipartWriter.WriteField("chat_id", strconv.FormatInt(chat, 10)); err != nil {
			fail(err)
			return
		}
		if thread > 0 {
			if err := multipartWriter.WriteField("message_thread_id", strconv.FormatInt(thread, 10)); err != nil {
				fail(err)
				return
			}
		}
		if caption != "" {
			if err := multipartWriter.WriteField("caption", caption); err != nil {
				fail(err)
				return
			}
		}
		part, err := multipartWriter.CreateFormFile(field, filepath.Base(path))
		if err == nil {
			_, err = io.Copy(part, file)
		}
		if err == nil {
			err = multipartWriter.Close()
		}
		_ = writer.CloseWithError(err)
	}()
	r, err := http.NewRequestWithContext(ctx, "POST", t.Base+"/bot"+t.Token+"/"+method, reader)
	if err != nil {
		reader.CloseWithError(err)
		return out, err
	}
	r.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	// Upload timeout is independent of the long-poll client's timeout.
	uploadClient := *t.HTTP
	uploadClient.Timeout = 3 * time.Minute
	res, err := uploadClient.Do(r)
	if err != nil {
		reader.CloseWithError(err)
		return out, fmt.Errorf("Telegram upload: %w", redactedTransportError(err))
	}
	err = t.decode(res, &out)
	return out, err
}
func (t *Telegram) SendDocument(ctx context.Context, chat, thread int64, path, caption string) (SentMessage, error) {
	return t.Upload(ctx, "sendDocument", chat, thread, path, caption, "document")
}
func (t *Telegram) SendPhoto(ctx context.Context, chat, thread int64, path, caption string) (SentMessage, error) {
	return t.Upload(ctx, "sendPhoto", chat, thread, path, caption, "photo")
}
func (t *Telegram) SendText(ctx context.Context, chat, thread int64, text string, keyboard any) (SentMessage, error) {
	var out SentMessage
	p := map[string]any{"chat_id": chat, "text": text, "link_preview_options": map[string]bool{"is_disabled": true}}
	if thread > 0 {
		p["message_thread_id"] = thread
	}
	if keyboard != nil {
		p["reply_markup"] = keyboard
	}
	err := t.Call(ctx, "sendMessage", p, &out)
	return out, err
}
func (t *Telegram) Download(ctx context.Context, fileID, path string) error {
	var f struct {
		Path string `json:"file_path"`
		Size int64  `json:"file_size"`
	}
	if err := t.Call(ctx, "getFile", map[string]string{"file_id": fileID}, &f); err != nil {
		return err
	}
	if f.Size > 20<<20 {
		return errors.New("file requires manual Telegram download or a Local Bot API server")
	}
	u, err := url.Parse(t.Base + "/file/bot" + t.Token + "/" + f.Path)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	res, err := t.HTTP.Do(r)
	if err != nil {
		return redactedTransportError(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New("Telegram download failed")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, io.LimitReader(res.Body, 21<<20))
	return err
}
