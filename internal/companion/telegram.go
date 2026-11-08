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
	"unicode/utf8"
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
type MessageEntity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

func validMessageEntities(text string, entities []MessageEntity) error {
	for _, e := range entities {
		if e.Type != "expandable_blockquote" || e.Offset < 0 || e.Length <= 0 || e.Offset > textUnits(text)-e.Length {
			return errors.New("invalid Telegram message entity")
		}
	}
	return nil
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
	return t.upload(ctx, method, chat, thread, path, caption, field, nil)
}
func (t *Telegram) upload(ctx context.Context, method string, chat, thread int64, path, caption, field string, keyboard any, extra ...map[string]string) (SentMessage, error) {
	var out SentMessage
	var markup []byte
	if keyboard != nil {
		var err error
		markup, err = json.Marshal(keyboard)
		if err != nil {
			return out, err
		}
	}
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
		for _, fields := range extra {
			for key, value := range fields {
				if err := multipartWriter.WriteField(key, value); err != nil {
					fail(err)
					return
				}
			}
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
		if markup != nil {
			if err := multipartWriter.WriteField("reply_markup", string(markup)); err != nil {
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
func (t *Telegram) SendPhotoCard(ctx context.Context, chat, thread int64, path, caption string, keyboard any, entities ...[]MessageEntity) (SentMessage, error) {
	if len(entities) > 0 && len(entities[0]) > 0 {
		if err := validMessageEntities(caption, entities[0]); err != nil {
			return SentMessage{}, err
		}
		data, err := json.Marshal(entities[0])
		if err != nil {
			return SentMessage{}, err
		}
		return t.upload(ctx, "sendPhoto", chat, thread, path, caption, "photo", keyboard, map[string]string{"caption_entities": string(data)})
	}
	return t.upload(ctx, "sendPhoto", chat, thread, path, caption, "photo", keyboard)
}
func (t *Telegram) SendDocumentCard(ctx context.Context, chat, thread int64, path, caption string, keyboard any) (SentMessage, error) {
	return t.upload(ctx, "sendDocument", chat, thread, path, caption, "document", keyboard)
}

// EditDocument replaces the ZIP in its existing post, keeping topic ordering.
// Unknown transport outcomes are returned for reconciliation, never retried.
func (t *Telegram) EditDocument(ctx context.Context, chat, messageID int64, path, caption string, keyboard ...any) (SentMessage, error) {
	if chat == 0 || messageID <= 0 {
		return SentMessage{}, errors.New("invalid document edit target")
	}
	media, err := json.Marshal(map[string]string{"type": "document", "media": "attach://archive", "caption": caption})
	if err != nil {
		return SentMessage{}, err
	}
	var markup any
	if len(keyboard) > 0 {
		markup = keyboard[0]
	}
	m, err := t.upload(ctx, "editMessageMedia", chat, 0, path, "", "archive", markup, map[string]string{"message_id": strconv.FormatInt(messageID, 10), "media": string(media)})
	if err != nil {
		return m, err
	}
	if m.ID != messageID || m.Chat.ID != chat || m.Document == nil || m.Document.FileID == "" {
		return m, errors.New("edited document receipt unconfirmed")
	}
	return m, nil
}

// SendAlbum uploads local photos once. A transport error leaves delivery unknown.
func (t *Telegram) SendAlbum(ctx context.Context, chat, thread int64, paths []string, caption string, entities ...[]MessageEntity) (out []SentMessage, retErr error) {
	if len(paths) < 2 || len(paths) > 10 {
		return nil, errors.New("Telegram photo album requires 2 to 10 files")
	}
	if len(entities) > 0 {
		if err := validMessageEntities(caption, entities[0]); err != nil {
			return nil, err
		}
	}
	if utf8.RuneCountInString(caption) > 1024 {
		return nil, errors.New("Telegram album caption exceeds 1024 characters")
	}
	files := make([]*os.File, 0, len(paths))
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	sizes := make([]int64, 0, len(paths))
	media := make([]struct {
		Type     string          `json:"type"`
		Media    string          `json:"media"`
		Caption  string          `json:"caption,omitempty"`
		Entities []MessageEntity `json:"caption_entities,omitempty"`
	}, len(paths))
	for i, path := range paths {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Telegram album photo %d is not a readable local file", i+1)
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("Telegram album photo %d cannot be opened", i+1)
		}
		files = append(files, file)
		info, err = file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 9<<20 {
			return nil, fmt.Errorf("Telegram album photo %d must be a regular file of at most 9 MiB", i+1)
		}
		sizes = append(sizes, info.Size())
		media[i].Type = "photo"
		media[i].Media = "attach://photo" + strconv.Itoa(i)
	}
	media[0].Caption = caption
	if len(entities) > 0 {
		media[0].Entities = entities[0]
	}
	mediaJSON, err := json.Marshal(media)
	if err != nil {
		return nil, errors.New("invalid Telegram album media")
	}
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	mw := multipart.NewWriter(writer)
	r, err := http.NewRequestWithContext(uploadCtx, "POST", t.Base+"/bot"+t.Token+"/sendMediaGroup", reader)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, errors.New("invalid Telegram album request")
	}
	r.Header.Set("Content-Type", mw.FormDataContentType())
	written := make(chan error, 1)
	go func() {
		err := func() error {
			if err := mw.WriteField("chat_id", strconv.FormatInt(chat, 10)); err != nil {
				return err
			}
			if thread > 0 {
				if err := mw.WriteField("message_thread_id", strconv.FormatInt(thread, 10)); err != nil {
					return err
				}
			}
			if err := mw.WriteField("media", string(mediaJSON)); err != nil {
				return err
			}
			for i, file := range files {
				part, err := mw.CreateFormFile("photo"+strconv.Itoa(i), filepath.Base(paths[i]))
				if err != nil {
					return err
				}
				if _, err = io.CopyN(part, file, sizes[i]); err != nil {
					return err
				}
			}
			return mw.Close()
		}()
		_ = writer.CloseWithError(err)
		written <- err
	}()
	defer func() {
		cancel()
		_ = reader.Close()
		if err := <-written; retErr == nil && err != nil {
			retErr = errors.New("Telegram album upload interrupted; delivery outcome unknown")
		}
	}()
	uploadClient := *t.HTTP
	uploadClient.Timeout = 3 * time.Minute
	res, err := uploadClient.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("Telegram album transport: %w", ctx.Err())
		}
		return nil, errors.New("Telegram album transport failed; delivery outcome unknown")
	}
	if err = t.decode(res, &out); err != nil {
		var rejected *TelegramError
		if errors.As(err, &rejected) {
			description := rejected.Description
			if t.Token != "" {
				description = strings.ReplaceAll(description, t.Token, "[redacted]")
				description = strings.ReplaceAll(description, url.QueryEscape(t.Token), "[redacted]")
			}
			return out, &TelegramError{Code: rejected.Code, Description: description, RetryAfter: rejected.RetryAfter}
		}
		return out, errors.New("Telegram album response interrupted or invalid; delivery outcome unknown")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 || len(out) != len(paths) {
		return out, errors.New("Telegram album response incomplete; delivery outcome unknown")
	}
	seen := make(map[int64]bool, len(out))
	for _, message := range out {
		if message.ID <= 0 || message.Chat.ID != chat || seen[message.ID] {
			return out, errors.New("Telegram album message receipt invalid; delivery outcome unknown")
		}
		seen[message.ID] = true
	}
	return out, nil
}

func (t *Telegram) SendText(ctx context.Context, chat, thread int64, text string, keyboard any, entities ...[]MessageEntity) (SentMessage, error) {
	var out SentMessage
	p := map[string]any{"chat_id": chat, "text": text, "link_preview_options": map[string]bool{"is_disabled": true}}
	if len(entities) > 0 && len(entities[0]) > 0 {
		if err := validMessageEntities(text, entities[0]); err != nil {
			return out, err
		}
		p["entities"] = entities[0]
	}
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
