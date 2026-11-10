package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAnalysisPostMaintenanceEditsKnownMessageAndKeepsRaw(t *testing.T) {
	f := newAnalysisPostFixture(t, "1. **Тема🙂**: текст\nОценка совместимости — 75%.")
	f.ready(t)
	s := f.service
	ctx := context.Background()
	if ran, err := s.deliverAnalysisPost(ctx); err != nil || !ran {
		t.Fatal(ran, err)
	}
	var message int64
	if err := s.Store.DB.QueryRow("SELECT message_id FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&message); err != nil {
		t.Fatal(err)
	}
	old := []byte(`{"text":"От: own · 101\n**старый текст**"}`)
	if _, err := s.Store.DB.Exec("UPDATE analysis_posts SET payload_json=? WHERE analysis_id=?", old, f.analysis.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	// This contract exercises the saved GPT receipt after profile formatting.
	if _, err := s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done' WHERE kind<>'analysis'"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM caption_maintenance_jobs WHERE revision=? AND kind='analysis'", captionDesignRevision).Scan(&count); err != nil || count != 1 {
		t.Fatal("maintenance duplicated", count, err)
	}
	if err := s.Store.SetMeta(ctx, "archive_next_send_ns", "0"); err != nil {
		t.Fatal(err)
	}
	edits := 0
	s.TG = s.Archiver.TG
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, "/editMessageText") {
			t.Fatal("maintenance attempted a new post", r.URL.Path)
		}
		var body struct {
			Chat     int64           `json:"chat_id"`
			ID       int64           `json:"message_id"`
			Text     string          `json:"text"`
			Entities []MessageEntity `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Chat != -100123 || body.ID != message || strings.Contains(body.Text, "От:") || strings.Contains(body.Text, "**") || !strings.Contains(body.Text, "reported-model") || !strings.Contains(body.Text, "🕒 ") {
			t.Fatal("new presentation not applied", body)
		}
		quote, bold := false, false
		for _, entity := range body.Entities {
			quote = quote || entity.Type == "expandable_blockquote"
			bold = bold || entity.Type == "bold"
		}
		if !quote || !bold || validMessageEntities(body.Text, body.Entities) != nil {
			t.Fatal("nested formatting missing", body.Entities)
		}
		edits++
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":55,"chat":{"id":-100123}}}`, message)), nil
	})
	w := captionMaintenanceWorker{s, func() time.Time { return f.now }}
	if ran, err := w.step(ctx); err != nil || !ran || edits != 1 {
		t.Fatal("known GPT post was not edited", ran, err, edits)
	}
	var state string
	if err := s.Store.DB.QueryRow("SELECT state FROM caption_maintenance_jobs WHERE revision=? AND kind='analysis'", captionDesignRevision).Scan(&state); err != nil || state != "done" {
		t.Fatal(state, err)
	}
	var saved analysisPostPayload
	var payload, raw []byte
	var sameID int64
	if err := s.Store.DB.QueryRow("SELECT payload_json,message_id FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&payload, &sameID); err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(payload, &saved) != nil || sameID != message || strings.Contains(saved.Text, "От:") {
		t.Fatal("receipt payload not updated")
	}
	if err := s.Store.DB.QueryRow("SELECT response_body FROM profile_analyses WHERE id=?", f.analysis.ID).Scan(&raw); err != nil || !bytes.Equal(raw, f.analysis.ResponseBody) {
		t.Fatal("raw source changed", err)
	}
}

func TestAnalysisPostMaintenanceRejectsDifferentOwnerThreadAndAccount(t *testing.T) {
	f := newAnalysisPostFixture(t, "**Тема**: текст")
	f.ready(t)
	s := f.service
	ctx := context.Background()
	if ran, err := s.deliverAnalysisPost(ctx); err != nil || !ran {
		t.Fatal(ran, err)
	}
	var message int64
	if err := s.Store.DB.QueryRow("SELECT message_id FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&message); err != nil {
		t.Fatal(err)
	}
	job := captionMaintenanceJob{kind: "analysis", receipt: f.analysis.ID, owner: 1, version: f.analysis.VersionID, chat: -100123, thread: 55, source: digest(f.analysis.ResponseBody)}
	for _, field := range []string{"owner", "chat", "thread", "source", "message"} {
		copy := job
		id := message
		switch field {
		case "owner":
			copy.owner = 2
		case "chat":
			copy.chat = -100456
		case "thread":
			copy.thread = 56
		case "source":
			copy.source = "other"
		case "message":
			id++
		}
		if _, _, err := s.prepareAnalysisPostEdit(ctx, &copy, []int64{id}); !errors.Is(err, errCaptionMaintenanceReceipt) {
			t.Fatal("forged receipt accepted", field, err)
		}
	}
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "different", UserID: 999, Generation: "different"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.prepareAnalysisPostEdit(ctx, &job, []int64{message}); !errors.Is(err, errCaptionMaintenanceReceipt) {
		t.Fatal("previous account accepted", err)
	}
}
