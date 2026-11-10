package companion

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestCaptionFormatEditsExistingMessagesDuringCollection(t *testing.T) {
	for _, stage := range []string{"search", "refresh", "draining", "no-cycle"} {
		t.Run(stage, func(t *testing.T) {
			s, p, _, _ := captionRefreshFixture(t)
			ctx := context.Background()
			if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "format-source", Token: "fixture"}); err != nil {
				t.Fatal(err)
			}
			cycle := syncPublicationCycle(t, s, 123)
			if stage == "no-cycle" {
				if _, err := s.Store.DB.Exec("DELETE FROM sync_cycles WHERE id=?", cycle.ID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage=? WHERE id=?", stage, cycle.ID); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.SetMeta(ctx, "caption_design_revision", ""); err != nil {
				t.Fatal(err)
			}
			if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			var before string
			if err := s.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			calls := 0
			maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
				calls++
				if in.Message != 101 || in.Chat != s.Config.BackupChatID {
					t.Fatal("format upgrade changed the destination or message ID", in)
				}
				return maintenanceSuccess(s, in), nil
			})
			w := captionMaintenanceWorker{s, time.Now}
			if did, err := w.step(ctx); err != nil || !did || calls != 1 {
				t.Fatal("existing message format waited for collection", did, err, calls)
			}
			var after string
			if err := s.Store.DB.QueryRow("SELECT raw_json FROM versions WHERE id=?", p.VersionID).Scan(&after); err != nil || after != before {
				t.Fatal("format upgrade changed the source snapshot", err)
			}
		})
	}
}

func TestCaptionFormatDefersPausedOrBlockedSource(t *testing.T) {
	for _, mode := range []string{"paused", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _, _ := captionRefreshFixture(t)
			ctx := context.Background()
			if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "format-source", Token: "fixture"}); err != nil {
				t.Fatal(err)
			}
			cycle := syncPublicationCycle(t, s, 123)
			if mode == "paused" {
				if _, err := s.Store.DB.Exec("UPDATE users SET paused=1 WHERE id=123"); err != nil {
					t.Fatal(err)
				}
			} else if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='blocked' WHERE id=?", cycle.ID); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.SetMeta(ctx, "caption_design_revision", ""); err != nil {
				t.Fatal(err)
			}
			if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			calls := 0
			maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
				calls++
				return maintenanceSuccess(s, in), nil
			})
			w := captionMaintenanceWorker{s, time.Now}
			if _, err := w.step(ctx); err != nil || calls != 0 {
				t.Fatal("paused source edited a message", err, calls)
			}
			var attempts int
			if err := s.Store.DB.QueryRow("SELECT COALESCE(SUM(attempts),0) FROM caption_maintenance_jobs").Scan(&attempts); err != nil || attempts != 0 {
				t.Fatal("deferred formatting consumed a request attempt", err, attempts)
			}
		})
	}
}

func TestCaptionFormatPrioritizesLatestPublishedProfile(t *testing.T) {
	s, old, _, _ := captionRefreshFixture(t)
	ctx := context.Background()
	updated := old.Profile
	updated["name"] = "Updated"
	ids, err := s.Store.ArchiveViewed(ctx, 123, []Profile{updated}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	latest := ids[0]
	if _, err = s.Store.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha)
 SELECT ?,owner_id,profile_id,'latest-fingerprint','sent',thread_id,'[301,302]','latest-source' FROM archive_versions WHERE version_id=?`, latest, old.VersionID); err != nil {
		t.Fatal(err)
	}
	if err = s.ScheduleCaptionMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	w := captionMaintenanceWorker{s, time.Now}
	j, err := w.claim(ctx)
	if err != nil || j == nil || j.kind != "archive" || j.version != latest {
		t.Fatal("historical formatting delayed the latest published profile", j, err)
	}
}
