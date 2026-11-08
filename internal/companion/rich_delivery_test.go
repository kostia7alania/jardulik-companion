package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func richDeliveryReceipt(photos int) json.RawMessage {
	blocks := []any{}
	for i := 0; i < photos; i++ {
		blocks = append(blocks, map[string]any{"type": "photo", "photo": []any{map[string]any{"file_id": fmt.Sprintf("fixture_photo_%d", i), "width": 900, "height": 1200}}})
	}
	raw, _ := json.Marshal(map[string]any{"blocks": blocks})
	return raw
}

type richDeliveryRequest struct {
	Chat, Thread int64
	Content      json.RawMessage
	Files        int
}

func readRichDeliveryRequest(t *testing.T, r *http.Request) richDeliveryRequest {
	t.Helper()
	var out richDeliveryRequest
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		out.Chat, _ = strconv.ParseInt(r.FormValue("chat_id"), 10, 64)
		out.Thread, _ = strconv.ParseInt(r.FormValue("message_thread_id"), 10, 64)
		out.Content = json.RawMessage(r.FormValue("rich_message"))
		out.Files = len(r.MultipartForm.File)
	} else {
		var body struct {
			Chat   int64           `json:"chat_id"`
			Thread int64           `json:"message_thread_id"`
			Rich   json.RawMessage `json:"rich_message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		out.Chat, out.Thread, out.Content = body.Chat, body.Thread, body.Rich
	}
	return out
}

func TestRichDeliveryPrivateOneMessagePreservesFullJSONAndActiveCard(t *testing.T) {
	s, p := cardFixture(t, 3)
	s.Config.RichCards = true
	s.Media = NewMediaManager(s.Store, s.Config)
	ctx := context.Background()
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "fixture", UserID: 101, Generation: "rich-session", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	p.Profile["opaque"] = strings.Repeat("source 🙂 ", 700)
	raw, err := json.Marshal(p.Profile)
	if err != nil {
		t.Fatal(err)
	}
	p.RawJSON = string(raw)
	if _, err := s.Store.DB.ExecContext(ctx, "UPDATE versions SET raw_json=? WHERE id=?", p.RawJSON, p.VersionID); err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		t.Fatal(err)
	}
	var methods []string
	var request richDeliveryRequest
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := filepath.Base(r.URL.Path)
		methods = append(methods, method)
		if method != "sendRichMessage" {
			t.Fatalf("extra or fallback message: %s", method)
		}
		request = readRichDeliveryRequest(t, r)
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":300,"chat":{"id":1},"rich_message":%s}}`, richDeliveryReceipt(3))
	})}
	if err := s.Deliver(ctx, 1, p); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(methods, []string{"sendRichMessage"}) || request.Chat != 1 || request.Thread != 0 || request.Files != 3 {
		t.Fatalf("wrong rich delivery: %v %+v", methods, request)
	}
	encodedJSON, _ := json.Marshal(pretty.String())
	if !bytes.Contains(request.Content, encodedJSON) || !bytes.Contains(request.Content, []byte(`"expandable_blockquote"`)) {
		t.Fatal("complete source JSON missing from rich card")
	}
	marker, err := loadRichCardMarker(ctx, s.Store, 1, 300)
	if err != nil || marker == nil || marker.OwnerID != 1 || marker.VersionID != p.VersionID || marker.ThreadID != 0 || len(marker.PhotoFileIDs) != 3 {
		t.Fatalf("rich receipt lost provenance: %+v %v", marker, err)
	}
	active, err := s.activeKeyboardCard(ctx, 1, 301)
	if err != nil || active.VersionID != p.VersionID {
		t.Fatalf("rich delivery did not activate keyboard target: %v", err)
	}
	var state, ids string
	if err := s.Store.DB.QueryRow(`SELECT state,message_ids FROM deliveries WHERE owner_id=1 ORDER BY id DESC LIMIT 1`).Scan(&state, &ids); err != nil || state != "sent" || ids != "[300]" {
		t.Fatalf("delivery receipt lost: %q %q %v", state, ids, err)
	}
	// Explicit JSON access remains available even when it already fits in the
	// rich message. Large JSON is still delivered whole as one document.
	var document []byte
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if filepath.Base(r.URL.Path) != "sendDocument" {
			t.Fatal("full JSON action did not use a whole document")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		defer r.MultipartForm.RemoveAll()
		file, _, err := r.FormFile("document")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		document, err = io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":301,"chat":{"id":1},"document":{"file_id":"fixture_json","file_unique_id":"fixture_json_unique","file_size":%d}}}`, len(document))
	})}
	s.sourceJSON(ctx, 1, 1, p.VersionID)
	if string(document) != pretty.String() {
		t.Fatal("JSON document action changed or truncated source")
	}
}

func TestRichDeliveryNeverFallsBackAfterTelegramError(t *testing.T) {
	for _, archive := range []bool{false, true} {
		for _, outcome := range []string{"unknown", "400", "unconfirmed"} {
			t.Run(fmt.Sprintf("archive%t-%s", archive, outcome), func(t *testing.T) {
				c, store, a, p, jobs := archivePresentationFixture(t)
				c.RichCards, a.Config.RichCards = true, true
				tg := NewTelegram(c)
				a.TG = tg
				s := &Service{Config: c, Store: store, TG: tg}
				calls := 0
				tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
					calls++
					if filepath.Base(r.URL.Path) != "sendRichMessage" {
						t.Fatal("ambiguous rich send fell back to another Telegram method")
					}
					request := readRichDeliveryRequest(t, r)
					if outcome == "unknown" {
						return nil, errors.New("mock response lost")
					}
					body := `{"ok":false,"error_code":400,"description":"mock rich rejection"}`
					if outcome == "unconfirmed" {
						body = fmt.Sprintf(`{"ok":true,"result":{"message_id":300,"chat":{"id":%d},"message_thread_id":%d}}`, request.Chat, request.Thread)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				ctx := context.Background()
				var err error
				if archive {
					_, err = a.sendCurrentProfileCard(ctx, p, jobs, 55, func([]int64) error { return nil })
				} else {
					_, err = s.sendCurrentProfileCard(ctx, 123, p)
				}
				if err == nil || calls != 1 {
					t.Fatalf("unexpected fallback/retry: err=%v calls=%d", err, calls)
				}
			})
		}
	}
}

func TestRichDeliveryTooLargeFallsBackBeforeSending(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, p := cardFixture(t, 1)
			s.Config.RichCards = enabled
			p.Profile["opaque"] = strings.Repeat("a", 33000)
			raw, err := json.Marshal(p.Profile)
			if err != nil {
				t.Fatal(err)
			}
			p.RawJSON = string(raw)
			var methods []string
			s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				methods = append(methods, filepath.Base(r.URL.Path))
				if filepath.Base(r.URL.Path) != "sendPhoto" {
					t.Fatal("oversized rich content was sent before safe fallback")
				}
				_, _ = io.Copy(io.Discard, r.Body)
				fmt.Fprint(w, `{"ok":true,"result":{"message_id":300,"chat":{"id":1}}}`)
			})}
			ids, err := s.sendCurrentProfileCard(context.Background(), 1, p)
			if err != nil || !reflect.DeepEqual(ids, []int64{300}) || !reflect.DeepEqual(methods, []string{"sendPhoto"}) {
				t.Fatalf("preflight fallback failed: ids=%v methods=%v err=%v", ids, methods, err)
			}
		})
	}
}

func TestRichDeliveryArchiveSavesIDsBeforeMarkerFailure(t *testing.T) {
	for _, failMarker := range []bool{false, true} {
		t.Run(fmt.Sprint(failMarker), func(t *testing.T) {
			c, store, a, p, jobs := archivePresentationFixture(t)
			a.Config.RichCards = true
			a.TG = NewTelegram(c)
			if failMarker {
				if _, err := store.DB.Exec(`CREATE TRIGGER reject_rich_marker BEFORE INSERT ON meta WHEN NEW.key LIKE 'rich_card:%' BEGIN SELECT RAISE(ABORT,'fixture marker failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			a.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if filepath.Base(r.URL.Path) != "sendRichMessage" {
					t.Fatal("unexpected archive fallback")
				}
				request := readRichDeliveryRequest(t, r)
				if request.Chat != c.BackupChatID || request.Thread != 55 || request.Files != 2 {
					t.Fatalf("wrong archive rich destination: %+v", request)
				}
				fmt.Fprintf(w, `{"ok":true,"result":{"message_id":300,"chat":{"id":%d},"message_thread_id":55,"rich_message":%s}}`, c.BackupChatID, richDeliveryReceipt(2))
			})}
			var saved []int64
			ids, err := a.sendCurrentProfileCard(context.Background(), p, jobs, 55, func(ids []int64) error {
				saved = append([]int64(nil), ids...)
				return nil
			})
			if (err != nil) != failMarker || calls != 1 || !reflect.DeepEqual(ids, []int64{300}) || !reflect.DeepEqual(saved, ids) {
				t.Fatalf("confirmed archive receipt lost: ids=%v saved=%v calls=%d err=%v", ids, saved, calls, err)
			}
			marker, loadErr := loadRichCardMarker(context.Background(), store, c.BackupChatID, 300)
			if loadErr != nil || failMarker && marker != nil {
				t.Fatalf("failed marker persisted: %+v %v", marker, loadErr)
			}
			if !failMarker && (marker == nil || marker.OwnerID != 123 || marker.VersionID != p.VersionID || marker.ThreadID != 55 || len(marker.PhotoFileIDs) != 2) {
				t.Fatalf("archive marker lost provenance: %+v", marker)
			}
		})
	}
}

// Cancel only once the complete ACK was decoded, before the wrapper records it.
type richDeliveryCancelOnClose struct {
	io.Reader
	cancel context.CancelFunc
}

func (b richDeliveryCancelOnClose) Close() error {
	b.cancel()
	return nil
}

func TestRichDeliveryPrivateConfirmedACKSurvivesCancellation(t *testing.T) {
	s, p := cardFixture(t, 1)
	s.Config.RichCards = true
	s.Media = NewMediaManager(s.Store, s.Config)
	if err := s.Store.SaveSession(context.Background(), 1, Session{Token: "fixture", UserID: 101, Generation: "rich-cancel-session"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if filepath.Base(r.URL.Path) != "sendRichMessage" {
			t.Fatal("unexpected fallback after confirmed rich send")
		}
		_ = readRichDeliveryRequest(t, r)
		body := fmt.Sprintf(`{"ok":true,"result":{"message_id":300,"chat":{"id":1},"rich_message":%s}}`, richDeliveryReceipt(1))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: richDeliveryCancelOnClose{strings.NewReader(body), cancel}}, nil
	})
	err := s.Deliver(ctx, 1, p)
	if err != nil || calls != 1 || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("confirmed private delivery lost: err=%v calls=%d ctx=%v", err, calls, ctx.Err())
	}
	var state, ids string
	if err := s.Store.DB.QueryRow(`SELECT state,message_ids FROM deliveries WHERE owner_id=1 ORDER BY id DESC LIMIT 1`).Scan(&state, &ids); err != nil || state != "sent" || ids != "[300]" {
		t.Fatalf("confirmed private ACK not durable: state=%s ids=%s err=%v", state, ids, err)
	}
	var delivered int
	if err := s.Store.DB.QueryRow(`SELECT delivered FROM queue WHERE owner_id=1 AND profile_id=?`, p.ID).Scan(&delivered); err != nil || delivered != 1 {
		t.Fatalf("confirmed private card remains eligible for automatic redelivery: delivered=%d err=%v", delivered, err)
	}
	marker, err := loadRichCardMarker(context.Background(), s.Store, 1, 300)
	if err != nil || marker == nil || marker.VersionID != p.VersionID || marker.OwnerID != 1 {
		t.Fatalf("private marker lost on cancellation: %+v %v", marker, err)
	}
}

func TestRichDeliveryArchiveConfirmedACKSurvivesCancellation(t *testing.T) {
	for _, failMarker := range []bool{false, true} {
		t.Run(fmt.Sprintf("markerFailure%t", failMarker), func(t *testing.T) {
			c, store, a, p, _ := archivePresentationFixture(t)
			a.Config.RichCards = true
			a.TG = NewTelegram(c)
			if failMarker {
				if _, err := store.DB.Exec(`CREATE TRIGGER reject_cancelled_rich_marker BEFORE INSERT ON meta WHEN NEW.key LIKE 'rich_card:%' BEGIN SELECT RAISE(ABORT,'fixture marker failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var methods []string
			a.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				method := filepath.Base(r.URL.Path)
				methods = append(methods, method)
				var body string
				switch method {
				case "sendDocument":
					if err := r.ParseMultipartForm(1 << 20); err != nil {
						t.Fatal(err)
					}
					defer r.MultipartForm.RemoveAll()
					file, _, err := r.FormFile("document")
					if err != nil {
						t.Fatal(err)
					}
					size, err := io.Copy(io.Discard, file)
					file.Close()
					if err != nil {
						t.Fatal(err)
					}
					body = fmt.Sprintf(`{"ok":true,"result":{"message_id":100,"document":{"file_id":"whole-cancel-file","file_unique_id":"whole-cancel-unique","file_size":%d}}}`, size)
				case "sendRichMessage":
					request := readRichDeliveryRequest(t, r)
					if request.Chat != c.BackupChatID || request.Thread != 55 {
						t.Fatal("wrong rich destination")
					}
					body = fmt.Sprintf(`{"ok":true,"result":{"message_id":300,"chat":{"id":%d},"message_thread_id":55,"rich_message":%s}}`, c.BackupChatID, richDeliveryReceipt(2))
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: richDeliveryCancelOnClose{strings.NewReader(body), cancel}}, nil
				default:
					t.Fatalf("unexpected cancellation fallback: %s", method)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			err := a.mirrorVersion(ctx, 123, p.VersionID)
			if !errors.Is(ctx.Err(), context.Canceled) || !reflect.DeepEqual(methods, []string{"sendDocument", "sendRichMessage"}) {
				t.Fatalf("wrong cancellation boundary: methods=%v ctx=%v err=%v", methods, ctx.Err(), err)
			}
			var state, ids string
			if scanErr := store.DB.QueryRow(`SELECT state,message_ids FROM archive_versions WHERE version_id=?`, p.VersionID).Scan(&state, &ids); scanErr != nil || ids != "[300]" {
				t.Fatalf("confirmed archive ID lost: state=%s ids=%s err=%v", state, ids, scanErr)
			}
			if failMarker && (err == nil || state != "uncertain") {
				t.Fatalf("failed marker not durably uncertain: state=%s err=%v", state, err)
			}
			if !failMarker && (err != nil || state != "sent") {
				t.Fatalf("confirmed ACK did not finish durably: state=%s err=%v", state, err)
			}
			marker, markerErr := loadRichCardMarker(context.Background(), store, c.BackupChatID, 300)
			if markerErr != nil || failMarker && marker != nil || !failMarker && (marker == nil || marker.OwnerID != 123 || marker.VersionID != p.VersionID || marker.ThreadID != 55) {
				t.Fatalf("archive marker cancellation durability: marker=%+v err=%v", marker, markerErr)
			}
			var zipID int64
			if err := store.DB.QueryRow(`SELECT message_id FROM archive_receipts WHERE file_id='whole-cancel-file'`).Scan(&zipID); err != nil || zipID != 100 {
				t.Fatalf("source ZIP receipt lost: id=%d err=%v", zipID, err)
			}
		})
	}
}
