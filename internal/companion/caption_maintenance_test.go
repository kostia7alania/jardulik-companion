package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type maintenanceInput struct {
	Chat    int64           `json:"chat_id"`
	Message int64           `json:"message_id"`
	Caption string          `json:"caption"`
	Text    string          `json:"text"`
	Rich    json.RawMessage `json:"rich_message"`
}

func maintenanceTelegram(t *testing.T, s *Service, call func(*http.Request, maintenanceInput) (string, error)) {
	t.Helper()
	tg := NewTelegram(s.Config)
	tg.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		method := filepath.Base(r.URL.Path)
		if method != "editMessageCaption" && method != "editMessageText" {
			t.Fatalf("maintenance attempted %s", method)
		}
		var input maintenanceInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		mu := s.userLock(123)
		if !mu.TryLock() {
			t.Fatal("caption edit held the private-chat lock")
		}
		mu.Unlock()
		check, cancel := context.WithTimeout(r.Context(), time.Second)
		var one int
		err := s.Store.DB.QueryRowContext(check, "SELECT 1").Scan(&one)
		cancel()
		if err != nil {
			t.Fatal("caption edit held a SQL transaction", err)
		}
		body, err := call(r, input)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	s.TG, s.Archiver.TG = tg, tg
}
func maintenanceSuccess(s *Service, input maintenanceInput) string {
	thread := int64(0)
	if input.Chat == s.Config.BackupChatID {
		thread = 55
	}
	return fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"chat":{"id":%d},"message_thread_id":%d}}`, input.Message, input.Chat, thread)
}
func maintenanceState(t *testing.T, s *Service, kind string) (string, int64) {
	t.Helper()
	var state string
	var next int64
	if err := s.Store.DB.QueryRow("SELECT state,next_attempt FROM caption_maintenance_jobs WHERE kind=? ORDER BY id LIMIT 1", kind).Scan(&state, &next); err != nil {
		t.Fatal(err)
	}
	return state, next
}
func TestCaptionMaintenanceDurableOnceOnlyEditsConfirmedCards(t *testing.T) {
	s, p, jobs, zip := captionRefreshFixture(t)
	before := map[string]string{}
	for _, path := range append([]string{zip}, jobs[0].Path, jobs[1].Path, jobs[2].Path) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = digest(data)
	}
	var raw string
	if err := s.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, s.Store, "caption_maintenance_jobs") != 2 {
		t.Fatal("revision scheduled duplicate or missing receipts")
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	w := captionMaintenanceWorker{s, func() time.Time { return now }}
	var messages []int64
	maintenanceTelegram(t, s, func(_ *http.Request, input maintenanceInput) (string, error) {
		if (input.Chat == s.Config.BackupChatID && input.Message != 101) || (input.Chat == 123 && input.Message != 201) || (input.Chat != s.Config.BackupChatID && input.Chat != 123) {
			t.Fatal("edited outside the confirmed caption receipts")
		}
		for _, command := range []string{"/like_", "/json_", "/text_", "/fav_"} {
			if strings.Contains(input.Caption, command) {
				t.Fatal("maintenance retained obsolete album commands")
			}
		}
		messages = append(messages, input.Message)
		return maintenanceSuccess(s, input), nil
	})
	for i := 0; i < 2; i++ {
		if did, err := w.step(context.Background()); err != nil || !did {
			t.Fatal("maintenance did not advance", did, err)
		}
		now = now.Add(5 * time.Second)
	}
	if fmt.Sprint(messages) != "[101 201]" {
		t.Fatal("maintenance edited a ZIP or non-caption album item", messages)
	}
	if did, err := w.step(context.Background()); err != nil || did {
		t.Fatal("completed receipt repeated", did, err)
	}
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if watchCount(t, s.Store, "caption_maintenance_jobs") != 2 {
		t.Fatal("completed revision was rescheduled")
	}
	for path, sha := range before {
		data, err := os.ReadFile(path)
		if err != nil || digest(data) != sha {
			t.Fatal("maintenance changed original bytes")
		}
	}
	var after string
	if err := s.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&after); err != nil || raw != after {
		t.Fatal("maintenance changed raw source JSON")
	}
}
func TestCaptionMaintenanceRestartAndUnknownEditRetryAreIdempotent(t *testing.T) {
	s, _, _, _ := captionRefreshFixture(t)
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	w := captionMaintenanceWorker{s, func() time.Time { return now }}
	if job, err := w.claim(context.Background()); err != nil || job == nil {
		t.Fatal("claim failed", err)
	}
	restarted := captionMaintenanceWorker{s, func() time.Time { return now }}
	if err := restarted.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	calls := 0
	maintenanceTelegram(t, s, func(_ *http.Request, input maintenanceInput) (string, error) {
		calls++
		if input.Message != 101 {
			t.Fatal("retry changed its confirmed message")
		}
		if calls == 1 {
			return "", errors.New("synthetic unknown transport outcome")
		}
		return `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`, nil
	})
	if did, err := restarted.step(context.Background()); err != nil || !did {
		t.Fatal(did, err)
	}
	state, next := maintenanceState(t, s, "archive")
	if state != "retry" || next <= now.UnixMilli() {
		t.Fatal("unknown edit was not durably deferred")
	}
	now = time.UnixMilli(next).Add(time.Second)
	if did, err := restarted.step(context.Background()); err != nil || !did {
		t.Fatal(did, err)
	}
	state, _ = maintenanceState(t, s, "archive")
	if state != "done" || calls != 2 {
		t.Fatal("idempotent edit retry not confirmed", state, calls)
	}
}
func TestCaptionMaintenanceIsolatesMissingAndForeignReceipts(t *testing.T) {
	for _, mutation := range []string{"DELETE FROM archive_receipts", "UPDATE archive_receipts SET chat_id=-100999", "UPDATE archive_versions SET message_ids='[11,102]'"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, _, _ := captionRefreshFixture(t)
			if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
			w := captionMaintenanceWorker{s, func() time.Time { return now }}
			calls := 0
			maintenanceTelegram(t, s, func(_ *http.Request, input maintenanceInput) (string, error) {
				calls++
				if input.Chat != 123 || input.Message != 201 {
					t.Fatal("invalid receipt reached Telegram")
				}
				return maintenanceSuccess(s, input), nil
			})
			if did, err := w.step(context.Background()); err != nil || !did {
				t.Fatal(did, err)
			}
			state, _ := maintenanceState(t, s, "archive")
			if state != "failed" || calls != 0 {
				t.Fatal("invalid receipt was not isolated")
			}
			now = now.Add(5 * time.Second)
			if did, err := w.step(context.Background()); err != nil || !did || calls != 1 {
				t.Fatal("bad receipt blocked another owner/post", did, err, calls)
			}
		})
	}
}
func TestCaptionMaintenancePersistsTelegramRetryAfterAcrossJobs(t *testing.T) {
	s, _, _, _ := captionRefreshFixture(t)
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	w := captionMaintenanceWorker{s, func() time.Time { return now }}
	calls := 0
	maintenanceTelegram(t, s, func(_ *http.Request, input maintenanceInput) (string, error) {
		calls++
		if calls == 1 {
			return `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":28}}`, nil
		}
		return maintenanceSuccess(s, input), nil
	})
	if did, err := w.step(context.Background()); err != nil || !did {
		t.Fatal(did, err)
	}
	state, next := maintenanceState(t, s, "archive")
	if state != "retry" || next != now.Add(28*time.Second).UnixMilli() {
		t.Fatal("server retry_after was not preserved")
	}
	now = now.Add(10 * time.Second)
	if did, err := w.step(context.Background()); err != nil || did || calls != 1 {
		t.Fatal("another edit bypassed shared flood wait", did, err, calls)
	}
	now = now.Add(19 * time.Second)
	if did, err := w.step(context.Background()); err != nil || !did || calls != 2 {
		t.Fatal("retry did not resume after its checkpoint", did, err, calls)
	}
	state, _ = maintenanceState(t, s, "archive")
	if state != "done" {
		t.Fatal("resumed edit not confirmed")
	}
}

func maintenanceRichMarker(t *testing.T, s *Service, chat, message, owner, version, thread int64, refs []string) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"owner_id": owner, "version_id": version, "thread_id": thread, "photo_file_ids": refs})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetMeta(context.Background(), fmt.Sprintf("rich_card:%d:%d", chat, message), string(data)); err != nil {
		t.Fatal(err)
	}
}
func TestCaptionMaintenanceRichSingleMessageUsesConfirmedFileIDs(t *testing.T) {
	s, p, _, _ := captionRefreshFixture(t)
	for _, query := range []string{"UPDATE archive_versions SET message_ids='[101]'", "UPDATE archive_profile_repairs SET new_message_ids='[101]'", "UPDATE deliveries SET message_ids='[201]'"} {
		if _, err := s.Store.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	refs := []string{"AgACsynthetic1", "AgACsynthetic2"}
	maintenanceRichMarker(t, s, s.Config.BackupChatID, 101, 123, p.VersionID, 55, refs)
	maintenanceRichMarker(t, s, 123, 201, 123, p.VersionID, 0, refs)
	if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	w := captionMaintenanceWorker{s, func() time.Time { return now }}
	calls := 0
	maintenanceTelegram(t, s, func(r *http.Request, input maintenanceInput) (string, error) {
		calls++
		if filepath.Base(r.URL.Path) != "editMessageText" || !json.Valid(input.Rich) || input.Caption != "" || input.Text != "" {
			t.Fatal("rich maintenance attempted a legacy caption or upload")
		}
		if strings.Contains(string(input.Rich), "attach://") || !strings.Contains(string(input.Rich), refs[0]) || !strings.Contains(string(input.Rich), refs[1]) {
			t.Fatal("rich edit did not reuse all confirmed photo IDs")
		}
		return maintenanceSuccess(s, input), nil
	})
	for i := 0; i < 2; i++ {
		if did, err := w.step(context.Background()); err != nil || !did {
			t.Fatal(did, err)
		}
		now = now.Add(5 * time.Second)
	}
	for _, kind := range []string{"archive", "private"} {
		state, _ := maintenanceState(t, s, kind)
		if state != "done" {
			t.Fatal("one-message rich inventory was rejected as an album", kind, state)
		}
	}
	if calls != 2 {
		t.Fatal("rich maintenance changed its message count")
	}
}
func TestCaptionMaintenanceInvalidRichMarkerNeverFallsBackToAlbum(t *testing.T) {
	for _, test := range []struct {
		name  string
		owner int64
		refs  []string
	}{
		{"foreign owner", 456, []string{"AgACsynthetic1", "AgACsynthetic2"}},
		{"missing refs", 123, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, p, _, _ := captionRefreshFixture(t)
			maintenanceRichMarker(t, s, s.Config.BackupChatID, 101, test.owner, p.VersionID, 55, test.refs)
			if err := s.ScheduleCaptionMaintenance(context.Background()); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
			w := captionMaintenanceWorker{s, func() time.Time { return now }}
			maintenanceTelegram(t, s, func(_ *http.Request, _ maintenanceInput) (string, error) {
				t.Fatal("invalid rich marker fell back to editing an album")
				return "", nil
			})
			if did, err := w.step(context.Background()); err != nil || !did {
				t.Fatal(did, err)
			}
			state, _ := maintenanceState(t, s, "archive")
			if state != "failed" {
				t.Fatal("invalid rich marker was not isolated", state)
			}
		})
	}
}

func noticeScopeMarkExistingDesign(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetMeta(context.Background(), "caption_design_revision", captionDesignRevision); err != nil {
		t.Fatal(err)
	}
}

func TestCaptionNoticeScopeEnrollsLateSentChangeWithoutResettingOtherKinds(t *testing.T) {
	s, j := threadChangeMaintenanceFixture(t)
	ctx := context.Background()
	noticeScopeMarkExistingDesign(t, s)
	if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET message_id=3034 WHERE version_id=?", j.receipt); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"archive", "private", "presence"} {
		if _, err := s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id,state,attempts,next_attempt,error)
 VALUES(?,?,77,123,?,'[42]',?,55,'done',4,12345,'preserve')`, captionDesignRevision, kind, j.version, s.Config.BackupChatID); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]string{}
	for _, kind := range []string{"archive", "private", "presence"} {
		before[kind] = threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE kind=?", kind)
	}
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	var state, inventory string
	if err := s.Store.DB.QueryRow("SELECT state,inventory FROM caption_maintenance_jobs WHERE revision=? AND kind='change' AND receipt_id=?", captionDesignRevision, j.receipt).Scan(&state, &inventory); err != nil || state != "pending" || inventory != "[3034]" {
		t.Fatalf("late confirmed notice missed: %s %s %v", state, inventory, err)
	}
	for kind, hash := range before {
		if hash != threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE kind=?", kind) {
			t.Fatalf("scoped pass changed %s", kind)
		}
	}
	if s.Store.Meta(ctx, "caption_design_revision") != captionDesignRevision || s.Store.Meta(ctx, "caption_notice_revision") != captionNoticeRevision {
		t.Fatal("scoped pass changed global revision or failed its own marker")
	}
	if _, err := s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done',attempts=3,next_attempt=54321,error='done-marker' WHERE kind='change'"); err != nil {
		t.Fatal(err)
	}
	after := threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE kind='change'")
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if after != threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE kind='change'") || watchCount(t, s.Store, "caption_maintenance_jobs") != 4 {
		t.Fatal("repeated scoped schedule requeued or duplicated completed notice")
	}
}

func captionNoticeAnalysisReceipt(t *testing.T) (*analysisPostFixture, int64) {
	t.Helper()
	f := newAnalysisPostFixture(t, "1. Тема\nОценка совместимости 75%.")
	f.ready(t)
	s := f.service
	if err := s.seedAnalysisPosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE analysis_posts SET state='sent',chat_id=?,thread_id=55,message_id=700,response_sha=? WHERE analysis_id=?", s.Config.BackupChatID, digest(f.analysis.ResponseBody), f.analysis.ID); err != nil {
		t.Fatal(err)
	}
	noticeScopeMarkExistingDesign(t, s)
	result, err := s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,source_sha,chat_id,thread_id,state,attempts,next_attempt,error)
 VALUES(?,'analysis',?,?,?,'[700]',?,?,55,'done',5,99999,'old-design')`, captionDesignRevision, f.analysis.ID, s.Config.OwnerID, f.analysis.VersionID, digest(f.analysis.ResponseBody), s.Config.BackupChatID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return f, id
}

func TestCaptionNoticeScopeRequeuesDoneAnalysisExactlyOnce(t *testing.T) {
	f, id := captionNoticeAnalysisReceipt(t)
	s := f.service
	ctx := context.Background()
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	var state, source string
	var attempts, next int64
	if err := s.Store.DB.QueryRow("SELECT state,attempts,next_attempt,source_sha FROM caption_maintenance_jobs WHERE id=?", id).Scan(&state, &attempts, &next, &source); err != nil || state != "pending" || attempts != 0 || next != 0 || source != digest(f.analysis.ResponseBody) {
		t.Fatal("done GPT receipt was not requeued with unchanged source", err)
	}
	if f.calls != 0 {
		t.Fatal("scheduling sent or edited a message")
	}
	if _, err := s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done',attempts=2,error='' WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	after := threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE id=?", id)
	if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if after != threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE id=?", id) {
		t.Fatal("scoped marker did not protect completed GPT job")
	}
}

func TestCaptionNoticeScopeDoesNotCrossOwnerOrCurrentSource(t *testing.T) {
	for _, bad := range []string{"job_owner", "job_source", "post_source", "response_body", "session", "live_owner"} {
		t.Run(bad, func(t *testing.T) {
			f, id := captionNoticeAnalysisReceipt(t)
			s := f.service
			var err error
			switch bad {
			case "job_owner":
				_, err = s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET owner_id=456 WHERE id=?", id)
			case "job_source":
				_, err = s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET source_sha='different-source' WHERE id=?", id)
			case "post_source":
				_, err = s.Store.DB.Exec("UPDATE analysis_posts SET response_sha='different-source' WHERE analysis_id=?", f.analysis.ID)
			case "response_body":
				_, err = s.Store.DB.Exec("UPDATE profile_analyses SET response_body=? WHERE id=?", []byte(`{"content":"other confirmed object"}`), f.analysis.ID)
			case "session":
				_, err = s.Store.DB.Exec("UPDATE sessions SET upstream_id=upstream_id+1 WHERE owner_id=?", s.Config.OwnerID)
			case "live_owner":
				if err = s.Store.User(context.Background(), 456, "test"); err == nil {
					_, err = s.Store.DB.Exec("UPDATE analysis_posts SET owner_id=456 WHERE analysis_id=?", f.analysis.ID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before := threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE id=?", id)
			if err = s.ScheduleCaptionMaintenance(context.Background()); err != nil {
				t.Fatal(err)
			}
			if before != threadNoticeRowHash(t, s.Store, "SELECT * FROM caption_maintenance_jobs WHERE id=?", id) || watchCount(t, s.Store, "caption_maintenance_jobs") != 1 || f.calls != 0 {
				t.Fatal("scoped schedule retargeted stale or other-owner source")
			}
		})
	}
}
