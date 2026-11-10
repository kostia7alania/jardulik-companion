package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func assertProfileCopyMarkup(t *testing.T, markup any, id int64) {
	t.Helper()
	raw, err := json.Marshal(markup)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Rows [][]map[string]json.RawMessage `json:"inline_keyboard"`
	}
	if err = json.Unmarshal(raw, &decoded); err != nil || len(decoded.Rows) != 1 || len(decoded.Rows[0]) != 1 {
		t.Fatal("legacy copy action must be one explicit clipboard button", string(raw), err)
	}
	button := decoded.Rows[0][0]
	var text string
	var copy struct {
		Text string `json:"text"`
	}
	if err = json.Unmarshal(button["text"], &text); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(button["copy_text"], &copy); err != nil {
		t.Fatal(err)
	}
	if len(button) != 2 || text != "📋 "+strconv.FormatInt(id, 10) || copy.Text != strconv.FormatInt(id, 10) {
		t.Fatal("copy button must copy only the upstream userId", string(raw))
	}
}

func TestLegacyProfileCopyMarkupUsesClipboardAction(t *testing.T) {
	assertProfileCopyMarkup(t, legacyProfileCopyMarkup(115), 115)
	for _, id := range []int64{0, -1} {
		if legacyProfileCopyMarkup(id) != nil {
			t.Fatal("invalid userId became a copy action")
		}
	}
}

func TestCaptionMaintenanceLegacyArchiveAddsCopyWithoutChangingCard(t *testing.T) {
	for _, photos := range []bool{true, false} {
		t.Run(fmt.Sprintf("photos=%t", photos), func(t *testing.T) {
			s, p, _, _ := captionRefreshFixture(t)
			ctx := context.Background()
			if !photos {
				for _, query := range []string{
					"DELETE FROM media_jobs",
					"UPDATE archive_versions SET message_ids='[101]'",
					"UPDATE archive_profile_repairs SET new_message_ids='[101]'",
					"UPDATE deliveries SET message_ids='[201]'",
				} {
					if _, err := s.Store.DB.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			w := captionMaintenanceWorker{s, time.Now}
			archive, err := w.claim(ctx)
			if err != nil || archive == nil || archive.kind != "archive" {
				t.Fatal("confirmed archive job missing", err)
			}
			method, payload, err := w.prepare(ctx, archive)
			if err != nil || payload["message_id"] != int64(101) || payload["chat_id"] != s.Config.BackupChatID {
				t.Fatal("copy action left the known card or targeted the original ZIP", method, payload, err)
			}
			assertProfileCopyMarkup(t, payload["reply_markup"], p.ID)
			jobs, err := s.Store.MediaJobs(ctx, archive.owner, p.VersionID)
			if err != nil {
				t.Fatal(err)
			}
			limit, textKey, entitiesKey, wantMethod := 1024, "caption", "caption_entities", "editMessageCaption"
			if !photos {
				limit, textKey, entitiesKey, wantMethod = 4096, "text", "entities", "editMessageText"
			}
			card, err := s.Archiver.profileCardContent(p, unavailableMediaCount(jobs), 55, limit, !photos, selectedProfilePhotos(jobs))
			if err != nil || method != wantMethod || payload[textKey] != card.Text || !reflect.DeepEqual(payload[entitiesKey], card.Entities) {
				t.Fatal("adding clipboard action changed the caption, raw JSON or entities", err)
			}
			private, err := w.claim(ctx)
			if err != nil || private == nil || private.kind != "private" {
				t.Fatal("confirmed private job missing", err)
			}
			_, privatePayload, err := w.prepare(ctx, private)
			if err != nil || privatePayload["message_id"] != int64(201) || privatePayload["reply_markup"] != nil {
				t.Fatal("copy maintenance overwrote the private reaction/contact keyboard", err)
			}
			var raw string
			if err := s.Store.DB.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&raw); err != nil || raw != p.RawJSON {
				t.Fatal("copy maintenance changed source JSON", err)
			}
		})
	}
}

func TestCaptionMaintenanceRichKeepsInlineCopyWithoutLegacyKeyboard(t *testing.T) {
	s, p, _, _ := captionRefreshFixture(t)
	ctx := context.Background()
	if _, err := s.Store.DB.ExecContext(ctx, "UPDATE archive_versions SET message_ids='[101]'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.ExecContext(ctx, "UPDATE archive_profile_repairs SET new_message_ids='[101]'"); err != nil {
		t.Fatal(err)
	}
	maintenanceRichMarker(t, s, s.Config.BackupChatID, 101, 123, p.VersionID, 55, []string{"AgACsynthetic1", "AgACsynthetic2"})
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	w := captionMaintenanceWorker{s, time.Now}
	job, err := w.claim(ctx)
	if err != nil || job == nil {
		t.Fatal(err)
	}
	method, payload, err := w.prepare(ctx, job)
	if err != nil || method != "editMessageText" || payload["message_id"] != int64(101) || payload["reply_markup"] != nil {
		t.Fatal("rich card gained a redundant legacy keyboard", err)
	}
	parts := payload["rich_message"].(map[string]any)["blocks"].([]any)[1].(map[string]any)["text"].([]any)
	button := parts[2].(map[string]any)["button"].(map[string]any)
	if button["copy_text"].(map[string]any)["text"] != strconv.FormatInt(p.ID, 10) {
		t.Fatal("rich card lost its inline source-userId clipboard action")
	}
}
