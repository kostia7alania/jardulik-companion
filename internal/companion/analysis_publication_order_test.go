package companion

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func analysisOrderFixture(t *testing.T, changes int) (*analysisPostFixture, SyncCycle, []int64) {
	t.Helper()
	f := newAnalysisPostFixture(t, "initial response")
	s := f.service
	s.Store.cfg.BackupChatID = s.Config.BackupChatID
	s.Store.cfg.BackupThreadID = s.Config.BackupThreadID
	ctx := context.Background()
	versions := []int64{f.analysis.VersionID}
	for i := 0; i < changes; i++ {
		p, err := s.Store.Profile(ctx, 1, 7)
		if err != nil {
			t.Fatal(err)
		}
		p.Profile["description"] = fmt.Sprintf("meaningful profile text %d", i+1)
		ids, err := s.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser")
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, ids[0])
		f.analysis = f.save(t, 1, 7, fmt.Sprintf("meaningful response %d", i+1), f.now.Add(time.Duration(i+1)*time.Minute))
	}
	f.ready(t)
	for i, version := range versions {
		sha := fmt.Sprintf("order-source-%d", version)
		if _, err := s.Store.DB.Exec("UPDATE archive_versions SET source_sha=? WHERE version_id=?", sha, version); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at)
 VALUES(?,'profile_version','source.zip',1,-100123,55,?,'source-file','unique',1,?)`, sha, 100+i, f.now.Add(time.Duration(i)*time.Minute).UTC().Format(time.RFC3339)); err != nil {
			t.Fatal(err)
		}
	}
	c := syncPublicationCycle(t, s, 1)
	p, err := s.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	syncPublicationCohort(t, s, c, p)
	syncPublicationAnalysisFrontier(t, s, c, f.analysis)
	syncPublicationPublish(t, s, &c)
	return f, c, versions
}

func TestAnalysisPublicationOrderInitialDoesNotWaitForMissingChangeJob(t *testing.T) {
	f, _, _ := analysisOrderFixture(t, 0)
	if worked, err := f.service.deliverAnalysisPost(context.Background()); err != nil || !worked || f.calls != 1 {
		t.Fatal("initial result waited for a nonexistent change", worked, err, f.calls)
	}
	if n := syncPublicationCount(t, f.service.Store, "SELECT COUNT(*) FROM archive_change_posts"); n != 0 {
		t.Fatal("initial publication created a fake change", n)
	}
}

func TestAnalysisPublicationOrderWaitsForEveryFrozenChangeThenPostsOnce(t *testing.T) {
	f, c, versions := analysisOrderFixture(t, 2)
	ctx := context.Background()
	s := f.service
	p, err := s.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	before := p.RawJSON
	wait := func() {
		t.Helper()
		before := f.calls
		if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked || f.calls != before {
			t.Fatal("GPT crossed unfinished change notices", worked, err, f.calls)
		}
	}
	wait()
	if n := syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM archive_change_posts WHERE state='pending'"); n != 2 {
		t.Fatal("eligible changes were not seeded before claim", n)
	}
	if worked, err := s.deliverArchiveChangePost(ctx); err != nil || !worked {
		t.Fatal(worked, err)
	}
	// Newest-first change delivery must not release GPT while an older frozen change is pending.
	if state := func() string {
		var state string
		if err := s.Store.DB.QueryRow("SELECT state FROM archive_change_posts WHERE version_id=?", versions[1]).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}(); state != "pending" {
		t.Fatal(state)
	}
	wait()
	if worked, err := s.deliverArchiveChangePost(ctx); err != nil || !worked {
		t.Fatal(worked, err)
	}
	if worked, err := s.deliverAnalysisPost(ctx); err != nil || !worked || f.calls != 3 {
		t.Fatal("settled profile stream did not release new result", worked, err, f.calls)
	}
	for _, body := range f.bodies[:2] {
		if !strings.HasPrefix(body.Text, "✏️ Изменения анкеты") {
			t.Fatal("result preceded profile change")
		}
	}
	if !strings.HasPrefix(f.bodies[2].Text, "🧩 Совместимость") || !strings.Contains(f.bodies[2].Text, "meaningful response 2") {
		t.Fatal("wrong final source result")
	}
	if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked || f.calls != 3 {
		t.Fatal("same source analysis posted twice", worked, err)
	}
	after, err := s.Store.Version(ctx, 1, p.VersionID)
	if err != nil || after.RawJSON != before {
		t.Fatal("sender changed source JSON", err)
	}
	// Photo-only reuse keeps the same analysis ID and must not repost its full body.
	if _, err = s.Store.DB.Exec("UPDATE sync_cycles SET stage='complete' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	p.Profile["photos"] = []string{"https://media.jardulik.app/photo-only.jpg"}
	if _, err = s.Store.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "GET /user/getUser"); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked || f.calls != 3 {
		t.Fatal("photo-only reuse duplicated the old full GPT post", worked, err)
	}
	if n := syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM actions"); n != 0 {
		t.Fatal("publication created a reaction")
	}
}

func TestAnalysisPublicationOrderRejectsUnresolvedAndMismatchedChanges(t *testing.T) {
	cases := []string{"pending", "preparing", "sending", "error", "uncertain", "sent_without_id", "foreign_owner", "wrong_profile", "wrong_thread", "unknown_skip"}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			f, _, versions := analysisOrderFixture(t, 1)
			s := f.service
			ctx := context.Background()
			if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked {
				t.Fatal(worked, err)
			}
			state := mode
			query := "UPDATE archive_change_posts SET state=? WHERE version_id=?"
			args := []any{state, versions[1]}
			switch mode {
			case "sent_without_id":
				query = "UPDATE archive_change_posts SET state='sent',message_id=0 WHERE version_id=?"
				args = []any{versions[1]}
			case "foreign_owner":
				query = "UPDATE archive_change_posts SET state='sent',message_id=500,owner_id=2 WHERE version_id=?"
				args = []any{versions[1]}
			case "wrong_profile":
				query = "UPDATE archive_change_posts SET state='sent',message_id=500,profile_id=999 WHERE version_id=?"
				args = []any{versions[1]}
			case "wrong_thread":
				query = "UPDATE archive_change_posts SET state='sent',message_id=500,thread_id=999 WHERE version_id=?"
				args = []any{versions[1]}
			case "unknown_skip":
				query = "UPDATE archive_change_posts SET state='skipped',message_id=0,error='not_proven' WHERE version_id=?"
				args = []any{versions[1]}
			}
			if _, err := s.Store.DB.Exec(query, args...); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked || f.calls != 0 {
					t.Fatal("unconfirmed change was hidden or retried", worked, err, f.calls)
				}
			}
		})
	}
}

func TestAnalysisPublicationOrderAllowsKnownNoopAndIgnoredCandidates(t *testing.T) {
	for _, mode := range []string{"skipped", "ignored"} {
		t.Run(mode, func(t *testing.T) {
			f, c, versions := analysisOrderFixture(t, 1)
			s := f.service
			ctx := context.Background()
			if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked {
				t.Fatal(worked, err)
			}
			if mode == "skipped" {
				if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET state='skipped',message_id=0,error='no_profile_changes' WHERE version_id=?", versions[1]); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.Store.DB.Exec("UPDATE sync_publication_items SET disposition='ignored',reason='no_profile_changes' WHERE cycle_id=? AND kind='change'", c.ID); err != nil {
					t.Fatal(err)
				}
			}
			if worked, err := s.deliverAnalysisPost(ctx); err != nil || !worked || f.calls != 1 {
				t.Fatal("proven no-op blocked result", worked, err, f.calls)
			}
		})
	}
}

func TestAnalysisPublicationOrderOlderUncertainLayoutStillBlocksLatestResult(t *testing.T) {
	f, _, versions := analysisOrderFixture(t, 1)
	s := f.service
	ctx := context.Background()
	if _, err := s.Store.DB.Exec("UPDATE archive_versions SET state='uncertain' WHERE version_id=?", versions[0]); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.deliverAnalysisPost(ctx); err != nil || worked || f.calls != 0 {
		t.Fatal("older frozen uncertain layout was ignored", worked, err)
	}
}

func TestAnalysisPublicationOrderRechecksAfterPacingBeforeFirstHTTP(t *testing.T) {
	f, _, _ := analysisOrderFixture(t, 1)
	s := f.service
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.initArchiveChangePosts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.enqueueArchiveChangePosts(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET state='sent',message_id=400"); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var ticks atomic.Int64
	s.Archiver.now = func() time.Time {
		if ticks.Add(1) == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return time.Now()
	}
	if err := s.Store.SetMeta(ctx, "archive_next_send_ns", strconv.FormatInt(time.Now().Add(40*time.Millisecond).UnixNano(), 10)); err != nil {
		t.Fatal(err)
	}
	type result struct {
		worked bool
		err    error
	}
	done := make(chan result, 1)
	go func() { worked, err := s.deliverAnalysisPost(ctx); done <- result{worked, err} }()
	select {
	case <-entered:
	case r := <-done:
		t.Fatal("pacing not reached", r)
	case <-ctx.Done():
		t.Fatal("pacing timed out")
	}
	if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET state='uncertain'"); err != nil {
		t.Fatal(err)
	}
	close(release)
	r := <-done
	if !r.worked || r.err == nil || f.calls != 0 || f.state(t, f.analysis.ID) != "pending" {
		t.Fatal("change changed during pacing but GPT was sent", r, f.calls)
	}
}

func TestAnalysisPublicationOrderRechecksAfterConfirmed429WithoutBlindReplay(t *testing.T) {
	f, _, _ := analysisOrderFixture(t, 1)
	s := f.service
	ctx := context.Background()
	if err := s.initArchiveChangePosts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.enqueueArchiveChangePosts(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET state='sent',message_id=400"); err != nil {
		t.Fatal(err)
	}
	f.respond = func(r *http.Request) (*http.Response, error) {
		if _, err := s.Store.DB.Exec("UPDATE archive_change_posts SET state='uncertain'"); err != nil {
			t.Fatal(err)
		}
		return albumTestResponse(r, 429, `{"ok":false,"error_code":429,"parameters":{"retry_after":1}}`), nil
	}
	if worked, err := s.deliverAnalysisPost(ctx); !worked || err == nil || f.calls != 1 || f.state(t, f.analysis.ID) != "pending" {
		t.Fatal("429 retry skipped the refreshed change guard", worked, err, f.calls)
	}
	if worked, err := s.deliverAnalysisPost(ctx); worked || err != nil || f.calls != 1 {
		t.Fatal("unknown prerequisite was blindly replayed", worked, err, f.calls)
	}
}

func TestAnalysisPublicationOrderMissingZIPCannotMasqueradeAsInitial(t *testing.T) {
	for _, mode := range []string{"missing", "wrong_chat", "invalid_date", "invalid_inventory", "zip_is_card", "invalid_original_date"} {
		t.Run(mode, func(t *testing.T) {
			f, _, versions := analysisOrderFixture(t, 1)
			s := f.service
			version := versions[1]
			var query string
			args := []any{version}
			switch mode {
			case "missing":
				query = "DELETE FROM archive_receipts WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)"
			case "wrong_chat":
				query = "UPDATE archive_receipts SET chat_id=-100999 WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)"
			case "invalid_date":
				query = "UPDATE archive_receipts SET sent_at='invalid' WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)"
			case "invalid_inventory":
				query = "UPDATE archive_versions SET message_ids='[0]' WHERE version_id=?"
			case "zip_is_card":
				query = "UPDATE archive_receipts SET message_id=40 WHERE source_sha=(SELECT source_sha FROM archive_versions WHERE version_id=?)"
			case "invalid_original_date":
				if _, err := s.Store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at)
 VALUES('old-zip','profile_version','source.zip',1,-100123,55,101,'old-source-file','old-unique',1,'invalid')`); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Store.DB.Exec(`INSERT INTO archive_zip_repairs(version_id,state,old_source_sha,old_filename,old_message_id) VALUES(?,'sent','old-zip','source.zip',101)`, version); err != nil {
					t.Fatal(err)
				}
			}
			if query != "" {
				if _, err := s.Store.DB.Exec(query, args...); err != nil {
					t.Fatal(err)
				}
			}
			if worked, err := s.deliverAnalysisPost(context.Background()); err != nil || worked || f.calls != 0 {
				t.Fatal("unproven publication passed as initial/no-op", worked, err, f.calls)
			}
			if state := f.state(t, f.analysis.ID); state != "pending" {
				t.Fatal("unpublished result state changed", state)
			}
		})
	}
}

func TestAnalysisPublicationOrderDuplicateNeedsConfirmedOriginalZIP(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			f, _, versions := analysisOrderFixture(t, 0)
			s := f.service
			other := compatibilityAdd(t, s, 2, 7, 23)
			if _, err := s.Store.DB.Exec("UPDATE archive_versions SET state='duplicate' WHERE version_id=?", versions[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec("DELETE FROM archive_profile_repairs WHERE version_id=?", versions[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,message_ids,source_sha)
 SELECT id,owner_id,profile_id,fingerprint,'sent',55,'[99]','other-source' FROM versions WHERE id=?`, other.VersionID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.DB.Exec("INSERT INTO archive_profile_repairs(version_id,state,old_message_ids,new_message_ids) VALUES(?,'sent','[]','[99]')", other.VersionID); err != nil {
				t.Fatal(err)
			}
			if valid {
				if _, err := s.Store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at)
 VALUES('other-source','profile_version','source.zip',1,-100123,55,200,'other-source-file','other-unique',1,'2026-11-08 10:00:00')`); err != nil {
					t.Fatal(err)
				}
			}
			worked, err := s.deliverAnalysisPost(context.Background())
			if err != nil || worked != valid || f.calls != map[bool]int{true: 1, false: 0}[valid] {
				t.Fatal("global duplicate proof did not preserve owned result ordering", worked, err, f.calls)
			}
		})
	}
}
