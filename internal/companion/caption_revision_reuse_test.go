package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func captionRevisionAnalysisFixture(t *testing.T) (*analysisPostFixture, captionMaintenanceWorker) {
	t.Helper()
	f := newAnalysisPostFixture(t, "**Тема🙂**: полный текст\nОценка совместимости — 75%.")
	f.ready(t)
	s := f.service
	ctx := context.Background()
	if ran, err := s.deliverAnalysisPost(ctx); err != nil || !ran {
		t.Fatal(ran, err)
	}
	s.TG = s.Archiver.TG
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done' WHERE kind<>'analysis'"); err != nil {
		t.Fatal(err)
	}
	w := captionMaintenanceWorker{s, time.Now}
	s.Archiver.now = w.now
	return f, w
}

func TestCaptionRevisionReuseConfirmedAnalysisWithoutPacingOrAttempt(t *testing.T) {
	f, w := captionRevisionAnalysisFixture(t)
	s := f.service
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	before := threadNoticeRowHash(t, s.Store, "SELECT * FROM analysis_posts WHERE analysis_id=?", f.analysis.ID)
	sourceBefore := threadNoticeRowHash(t, s.Store, "SELECT * FROM profile_analyses WHERE id=?", f.analysis.ID)
	future := fmt.Sprint(time.Now().Add(time.Hour).UnixNano())
	if err := s.Store.SetMeta(ctx, "archive_next_send_ns", future); err != nil {
		t.Fatal(err)
	}
	calls := 0
	maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
		calls++
		return maintenanceSuccess(s, in), nil
	})
	if ran, err := w.step(ctx); err != nil || !ran || calls != 0 {
		t.Fatal("unchanged confirmed analysis made an HTTP request", ran, err, calls)
	}
	var state string
	var attempts int
	if err := s.Store.DB.QueryRow("SELECT state,attempts FROM caption_maintenance_jobs WHERE kind='analysis'").Scan(&state, &attempts); err != nil || state != "done" || attempts != 0 {
		t.Fatal("no-op consumed an attempt or stayed pending", state, attempts, err)
	}
	if s.Store.Meta(ctx, "archive_next_send_ns") != future {
		t.Fatal("no-op reserved a Telegram slot")
	}
	if before != threadNoticeRowHash(t, s.Store, "SELECT * FROM analysis_posts WHERE analysis_id=?", f.analysis.ID) || sourceBefore != threadNoticeRowHash(t, s.Store, "SELECT * FROM profile_analyses WHERE id=?", f.analysis.ID) {
		t.Fatal("no-op changed confirmed payload or raw source")
	}
}

func TestCaptionRevisionReuseRequiresExactPayloadAndConfirmedDestination(t *testing.T) {
	for _, mode := range []string{"old_text", "old_entities", "old_markup", "missing_receipt", "foreign_receipt", "uncertain", "foreign_destination"} {
		t.Run(mode, func(t *testing.T) {
			f, w := captionRevisionAnalysisFixture(t)
			s := f.service
			ctx := context.Background()
			switch mode {
			case "old_text", "old_entities", "old_markup":
				var raw []byte
				if err := s.Store.DB.QueryRow("SELECT payload_json FROM analysis_posts WHERE analysis_id=?", f.analysis.ID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var payload map[string]any
				if err := json.Unmarshal(raw, &payload); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "old_text":
					payload["text"] = "old text"
				case "old_entities":
					payload["entities"] = []any{}
				case "old_markup":
					payload["reply_markup"] = map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "old", "callback_data": "old:1"}}}}
				}
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.Store.DB.Exec("UPDATE analysis_posts SET payload_json=? WHERE analysis_id=?", raw, f.analysis.ID); err != nil {
					t.Fatal(err)
				}
			case "missing_receipt":
				if _, err := s.Store.DB.Exec("UPDATE analysis_posts SET receipt_json=X'' WHERE analysis_id=?", f.analysis.ID); err != nil {
					t.Fatal(err)
				}
			case "foreign_receipt":
				if _, err := s.Store.DB.Exec(`UPDATE analysis_posts SET receipt_json='{"message_id":501,"chat":{"id":-100999},"message_thread_id":55}' WHERE analysis_id=?`, f.analysis.ID); err != nil {
					t.Fatal(err)
				}
			case "uncertain":
				if _, err := s.Store.DB.Exec("UPDATE analysis_posts SET state='uncertain' WHERE analysis_id=?", f.analysis.ID); err != nil {
					t.Fatal(err)
				}
			case "foreign_destination":
				if _, err := s.Store.DB.Exec("UPDATE analysis_posts SET chat_id=-100999 WHERE analysis_id=?", f.analysis.ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Store.SetMeta(ctx, "archive_next_send_ns", "0"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
				calls++
				return maintenanceSuccess(s, in), nil
			})
			if ran, err := w.step(ctx); err != nil || !ran {
				t.Fatal(ran, err)
			}
			var state string
			var attempts int
			if err := s.Store.DB.QueryRow("SELECT state,attempts FROM caption_maintenance_jobs WHERE kind='analysis'").Scan(&state, &attempts); err != nil {
				t.Fatal(err)
			}
			if mode == "uncertain" || mode == "foreign_destination" {
				if state != "failed" || calls != 0 {
					t.Fatal("unconfirmed source/destination was marked done", state, calls)
				}
			} else if state != "done" || calls != 1 || attempts != 1 {
				t.Fatal("changed or unproven payload bypassed normal edit", state, calls, attempts)
			}
		})
	}
}

func TestCaptionRevisionTargetEditsOnlyNewestOwnedArchiveProfile(t *testing.T) {
	f := newArchiveChangeFixture(t)
	s := f.service
	ctx := context.Background()
	first := archiveChangePublish(t, f, 123, Profile{"userId": 7, "name": "Target old"}, "2026-11-08 10:00:00", 55)
	latest := archiveChangePublish(t, f, 123, Profile{"userId": 7, "name": "Target current"}, "2026-11-08 11:00:00", 55)
	archiveChangePublish(t, f, 123, Profile{"userId": 8, "name": "Other"}, "2026-11-08 10:00:00", 55)
	archiveChangePublish(t, f, 456, Profile{"userId": 7, "name": "Foreign observer"}, "2026-11-08 10:00:00", 55)
	calls := 0
	maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
		calls++
		if in.Chat != s.Config.BackupChatID || in.Message != 1000+latest {
			t.Fatal("target API edited another receipt", in.Message)
		}
		return maintenanceSuccess(s, in), nil
	})
	s.Archiver.now = time.Now
	if ran, err := s.RefreshProfileCaption(ctx, 7); err != nil || !ran || calls != 1 {
		t.Fatal("targeted card did not edit", ran, err, calls)
	}
	var state string
	if err := s.Store.DB.QueryRow("SELECT state FROM caption_maintenance_jobs WHERE kind='archive' AND version_id=?", latest).Scan(&state); err != nil || state != "done" {
		t.Fatal(state, err)
	}
	for _, query := range []string{
		"SELECT COUNT(*) FROM caption_maintenance_jobs WHERE kind='archive' AND version_id=" + fmt.Sprint(first) + " AND state='pending'",
		"SELECT COUNT(*) FROM caption_maintenance_jobs WHERE kind='archive' AND owner_id=456 AND state='pending'",
	} {
		if syncPublicationCount(t, s.Store, query) != 1 {
			t.Fatal("target API claimed another owner/version")
		}
	}
	// Failed jobs are not resurrected by an explicit target request.
	if _, err := s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='failed' WHERE kind='archive' AND owner_id=123"); err != nil {
		t.Fatal(err)
	}
	if ran, err := s.RefreshProfileCaption(ctx, 7); err != nil || ran || calls != 1 {
		t.Fatal("target API reset failed work", ran, err, calls)
	}
	if ran, err := s.RefreshProfileCaption(ctx, 0); err == nil || ran {
		t.Fatal("invalid target accepted", ran, err)
	}
}
