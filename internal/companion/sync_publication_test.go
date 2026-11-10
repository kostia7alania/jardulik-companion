package companion

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func syncPublicationCycle(t *testing.T, s *Service, owner int64) SyncCycle {
	t.Helper()
	ctx := context.Background()
	s.Config.SyncCycleEnabled = true
	s.Store.cfg.SyncCycleEnabled = true
	s.Store.cfg.OwnerID = s.Config.OwnerID
	if s.Archiver != nil {
		s.Archiver.Config.SyncCycleEnabled = true
	}
	if err := s.Store.InitSyncCycles(ctx); err != nil {
		t.Fatal(err)
	}
	ss, err := s.Store.Session(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Hour)
	c := SyncCycle{ID: fmt.Sprintf("publication-%d", owner), OwnerID: owner, UserID: ss.UserID, Generation: ss.Generation, Stage: "draining", StartedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(), NextCycle: now.Add(24 * time.Hour).UnixMilli()}
	if _, err = s.Store.DB.Exec(`INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES(?,?,?,?,?,'fixture',?,?,?)`, c.ID, c.OwnerID, c.UserID, c.Generation, c.Stage, c.StartedAt, c.UpdatedAt, c.NextCycle); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetMeta(ctx, "caption_design_revision", captionDesignRevision); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetMeta(ctx, "caption_notice_revision", captionNoticeRevision); err != nil {
		t.Fatal(err)
	}
	return c
}
func syncPublicationCohort(t *testing.T, s *Service, c SyncCycle, p StoredProfile) {
	t.Helper()
	if _, err := s.Store.DB.Exec("INSERT INTO sync_refresh_items(cycle_id,profile_id,state,version_id,observed_at_ms) VALUES(?,?,'available',?,?)", c.ID, p.ID, p.VersionID, c.StartedAt+1); err != nil {
		t.Fatal(err)
	}
}
func syncPublicationPublish(t *testing.T, s *Service, c *SyncCycle) {
	t.Helper()
	if err := s.Store.FreezeSyncPublication(context.Background(), *c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='publishing' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	c.Stage = "publishing"
}
func syncPublicationCount(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSyncPublicationEmptyAndPopulatedManifestsStayFrozen(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			c := syncPublicationCycle(t, f.s, 123)
			if populated {
				p := f.add(t, 7, 24, "First", "")
				syncPublicationCohort(t, f.s, c, p)
			}
			if err := f.s.Store.FreezeSyncPublication(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			before := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=?", c.ID)
			if populated && before != 2 {
				t.Fatal("missing version/change frontier", before)
			}
			later := f.add(t, 8, 25, "Later", "")
			syncPublicationCohort(t, f.s, c, later)
			if err := f.s.Store.FreezeSyncPublication(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			if after := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=?", c.ID); after != before {
				t.Fatal("freeze silently enrolled a later profile", before, after)
			}
			if n := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM sync_publication_manifests WHERE cycle_id=?", c.ID); n != 1 {
				t.Fatal("empty frontier has no durable marker")
			}
		})
	}
}

func TestSyncPublicationCompletesOnlyExactConfirmedReceiptsAndFinalIndexes(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	syncPublicationPublish(t, f.s, &c)
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("unpublished final index counted complete", ready, err)
	}
	if len(f.operations) != 0 {
		t.Fatal("completion performed HTTP")
	}
	for range 3 {
		f.step(t)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || !ready {
		t.Fatal("confirmed finite frontier did not complete", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE archive_versions SET state='uncertain' WHERE version_id=?", p.VersionID); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("uncertain archive treated as success", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE archive_versions SET state='sent' WHERE version_id=?", p.VersionID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE profile_index_pages SET sent_sha='changed'"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("wrong index payload receipt treated as success", ready, err)
	}
}

func TestSyncPublicationRejectsIncompleteForeignAndMutatedArchiveEvidence(t *testing.T) {
	for _, mutation := range []string{
		"UPDATE archive_receipts SET chat_id=-100999",
		"UPDATE archive_receipts SET file_id=''",
		"UPDATE archive_profile_repairs SET state='uncertain'",
		"UPDATE archive_profile_repairs SET new_message_ids='[2]'",
		"UPDATE archive_versions SET message_ids='[907]'",
		"UPDATE archive_versions SET owner_id=999",
		"UPDATE versions SET raw_json=json_set(raw_json,'$.name','mutated')",
	} {
		t.Run(mutation, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			p := f.add(t, 7, 24, "First", "")
			c := syncPublicationCycle(t, f.s, 123)
			syncPublicationCohort(t, f.s, c, p)
			syncPublicationPublish(t, f.s, &c)
			if _, err := f.s.Store.DB.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
				t.Fatal("invalid publication evidence was accepted", ready, err)
			}
			if len(f.operations) != 0 {
				t.Fatal("receipt verification made HTTP calls")
			}
		})
	}
}

func TestSyncPublicationDuplicateNeedsOriginalWholeZIPAndLayout(t *testing.T) {
	f := newProfileIndexesFixture(t)
	original := f.add(t, 7, 24, "First", "")
	ctx := context.Background()
	s := f.s.Store
	if err := s.User(ctx, 2, "fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSession(ctx, 2, Session{UserID: 222, Generation: "other", Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Archive(ctx, 2, []Profile{original.Profile}, "GET /user/getUser")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := s.Version(ctx, 2, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(`INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id) VALUES(?,2,7,'fixture','duplicate',107)`, duplicate.VersionID); err != nil {
		t.Fatal(err)
	}
	c := syncPublicationCycle(t, f.s, 2)
	syncPublicationCohort(t, f.s, c, duplicate)
	syncPublicationPublish(t, f.s, &c)
	if ready, err := f.s.syncCyclePublicationComplete(ctx, c); err != nil || !ready {
		t.Fatal("valid shared archive duplicate rejected", ready, err)
	}
	if _, err = s.DB.Exec("DELETE FROM archive_receipts"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(ctx, c); err != nil || ready {
		t.Fatal("duplicate with no confirmed original receipt accepted", ready, err)
	}
}

func TestSyncPublicationSharedBarrierAcrossStoreHandles(t *testing.T) {
	s, now := syncFixture(t)
	ctx := context.Background()
	other, err := OpenStore(s.Store.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	release, locked, err := other.trySyncPublicationLock()
	if err != nil || !locked {
		t.Fatal(locked, err)
	}
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || started {
		release()
		t.Fatal("source cycle crossed in-flight poster barrier", started, err)
	}
	if syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM sync_cycles") != 0 {
		release()
		t.Fatal("cycle state changed while other role held barrier")
	}
	release()
	if started, err := s.startSyncCycle(ctx, 1, *now); err != nil || !started {
		t.Fatal("source did not progress after poster persisted outcome", started, err)
	}
}

func TestSyncPublicationStagesGateAllBackgroundPostersWithoutHTTP(t *testing.T) {
	for _, stage := range []string{"search", "refresh", "draining", "blocked", "no-cycle"} {
		t.Run(stage, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			f.add(t, 7, 24, "First", "")
			c := syncPublicationCycle(t, f.s, 123)
			if stage == "no-cycle" {
				if _, err := f.s.Store.DB.Exec("DELETE FROM sync_cycles"); err != nil {
					t.Fatal(err)
				}
			} else if _, err := f.s.Store.DB.Exec("UPDATE sync_cycles SET stage=? WHERE id=?", stage, c.ID); err != nil {
				t.Fatal(err)
			}
			for _, poster := range []struct {
				name string
				fn   func(context.Context) (bool, error)
			}{
				{"analysis", f.s.deliverAnalysisPost}, {"change", f.s.deliverArchiveChangePost}, {"index", f.s.deliverProfileIndex},
			} {
				if did, err := poster.fn(context.Background()); err != nil || did {
					t.Fatalf("%s crossed %s barrier: %v %v", poster.name, stage, did, err)
				}
			}
			if len(f.operations) != 0 {
				t.Fatal("collection phase made Telegram requests")
			}
		})
	}
}

func TestSyncPublicationRechecksBindingAndPhaseAfterPacing(t *testing.T) {
	for _, mutation := range []string{"phase", "generation"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, _, _ := captionRefreshFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "caption-cycle", Token: "fixture"}); err != nil {
				t.Fatal(err)
			}
			c := syncPublicationCycle(t, s, 123)
			syncPublicationPublish(t, s, &c)
			if err := s.Store.SetMeta(ctx, "caption_design_revision", ""); err != nil {
				t.Fatal(err)
			}
			if err := s.ScheduleCaptionMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			w := captionMaintenanceWorker{s, time.Now}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			s.Archiver.now = func() time.Time {
				once.Do(func() {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
					}
				})
				return time.Now()
			}
			if err := s.Store.SetMeta(ctx, "archive_next_send_ns", strconv.FormatInt(time.Now().Add(40*time.Millisecond).UnixNano(), 10)); err != nil {
				t.Fatal(err)
			}
			calls := 0
			maintenanceTelegram(t, s, func(_ *http.Request, in maintenanceInput) (string, error) {
				calls++
				return maintenanceSuccess(s, in), nil
			})
			type result struct {
				did bool
				err error
			}
			done := make(chan result, 1)
			go func() { did, err := w.step(ctx); done <- result{did, err} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("pacing not reached")
			}
			if mutation == "phase" {
				if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='blocked' WHERE id=?", c.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "reconnected", Token: "fixture"}); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			r := <-done
			if r.err != nil || !r.did || calls != 0 {
				t.Fatal("source/phase changed during pacing but HTTP attempted", r, calls)
			}
			var attempts int
			var state string
			if err := s.Store.DB.QueryRow("SELECT attempts,state FROM caption_maintenance_jobs WHERE kind='archive'").Scan(&attempts, &state); err != nil {
				t.Fatal(err)
			}
			if attempts != 0 || state != "retry" {
				t.Fatal("deferred publication consumed edit budget", attempts, state)
			}
		})
	}
}

func TestSyncPublicationCurrentSessionMustMatchFrozenSource(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	syncPublicationPublish(t, f.s, &c)
	if err := f.s.Store.SaveSession(context.Background(), 123, Session{UserID: 999, Generation: c.Generation, Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Store.requireSyncPublication(context.Background(), 123); !errors.Is(err, errSyncPublicationDeferred) {
		t.Fatal("current source switch accepted", err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("old upstream was marked complete", ready, err)
	}
}

func syncPublicationAnalysisFrontier(t *testing.T, s *Service, c SyncCycle, a ProfileAnalysis) {
	t.Helper()
	ctx := context.Background()
	if err := s.Store.InitCompatibility(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(compatibilityCycleSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO compatibility_cycle_scans(cycle_id,owner_id,upstream_id,generation,own_hash,final_pass,sealed,updated_at_ms) VALUES(?,?,?,?,'own',1,1,?)`, c.ID, c.OwnerID, c.UserID, c.Generation, c.StartedAt); err != nil {
		t.Fatal(err)
	}
	result, err := s.Store.DB.Exec(`INSERT INTO compatibility_pairs(owner_id,from_user_id,target_user_id,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?)`, c.OwnerID, c.UserID, a.ProfileID, c.StartedAt, c.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	result, err = s.Store.DB.Exec(`INSERT INTO compatibility_jobs(pair_id,generation,version_id,own_hash,target_hash,input_hash,filter_hash,own_profile,target_profile,state,analysis_id,created_at_ms,updated_at_ms) VALUES(?,?,?,'own','target','input','archive-adult-v1',?,?,'succeeded',?,?,?)`, pair, c.Generation, a.VersionID, a.OwnProfile, a.TargetProfile, a.ID, c.StartedAt, c.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	job, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec(`INSERT INTO compatibility_cycle_targets(cycle_id,profile_id,version_id,input_hash,job_id,state,final_member,updated_at_ms) VALUES(?,?,?,'input',?,'succeeded',1,?)`, c.ID, a.ProfileID, a.VersionID, job, c.StartedAt); err != nil {
		t.Fatal(err)
	}
}

func TestSyncPublicationPinsExactCachedAnalysisDespiteLaterQueueAndCapture(t *testing.T) {
	f := newAnalysisPostFixture(t, "saved whole response")
	f.ready(t)
	s := f.service
	s.Store.cfg.BackupChatID = s.Config.BackupChatID
	s.Store.cfg.BackupThreadID = s.Config.BackupThreadID
	ctx := context.Background()
	if _, err := s.Store.DB.Exec("UPDATE archive_versions SET source_sha='cached-analysis-zip' WHERE version_id=?", f.analysis.VersionID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size,sent_at)
 VALUES('cached-analysis-zip','profile_version','source.zip',1,-100123,55,100,'source-file','unique',1,?)`, f.now.UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	c := syncPublicationCycle(t, s, 1)
	p, err := s.Store.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	syncPublicationCohort(t, s, c, p)
	// Reusing an exact valid same-account cache across reconnects is allowed.
	if _, err = s.Store.DB.Exec("UPDATE profile_analyses SET generation='old-cache-generation' WHERE id=?", f.analysis.ID); err != nil {
		t.Fatal(err)
	}
	syncPublicationAnalysisFrontier(t, s, c, f.analysis)
	later := f.save(t, 1, 7, "later unplanned response", f.now.Add(time.Hour))
	syncPublicationPublish(t, s, &c)
	if n := syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=? AND kind='analysis' AND item_id=?", c.ID, f.analysis.ID); n != 1 {
		t.Fatal("exact cached analysis missing")
	}
	if n := syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=? AND kind='analysis' AND item_id=?", c.ID, later.ID); n != 0 {
		t.Fatal("frontier silently selected newest capture")
	}
	// A later foreground version and historical pending outside the frozen set must not starve it.
	if _, err = s.Store.Archive(ctx, 1, []Profile{{"userId": 7, "age": 24, "name": "Later foreground"}}, "test"); err != nil {
		t.Fatal(err)
	}
	if did, err := s.deliverAnalysisPost(ctx); err != nil || !did || f.calls != 1 {
		t.Fatal("frozen analysis blocked by outside frontier work", did, err, f.calls)
	}
	if state := f.state(t, f.analysis.ID); state != "sent" {
		t.Fatal(state)
	}
	if state := f.state(t, later.ID); state != "pending" {
		t.Fatal("out-of-frontier analysis was posted", state)
	}
	if did, err := s.deliverAnalysisPost(ctx); err != nil || did || f.calls != 1 {
		t.Fatal("out-of-frontier result repeated", did, err)
	}
}

func TestSyncPublicationPresenceRequiresMatchingReceiptAndFrozenEvent(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	if _, err := f.s.Store.DB.Exec(profilePresenceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(`INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at) VALUES(123,7,'unavailable',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(`INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,version_id) VALUES(123,7,1,'unavailable',CURRENT_TIMESTAMP,?)`, p.VersionID); err != nil {
		t.Fatal(err)
	}
	syncPublicationPublish(t, f.s, &c)
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("pending presence counted complete", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE profile_presence_events SET state='sent',message_id=55,chat_id=?,thread_id=107", f.s.Config.BackupChatID); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("final indexes not required", ready, err)
	}
	for range 3 {
		f.step(t)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || !ready {
		t.Fatal("confirmed presence not accepted", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE profile_presence_events SET reason='different-source'"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("changed event with old receipt accepted", ready, err)
	}
}

func TestSyncPublicationChangeMustBeExplicitlySentOrSkipped(t *testing.T) {
	f := newArchiveChangeFixture(t)
	s := f.service
	ctx := context.Background()
	if err := s.Store.User(ctx, 123, "fixture"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "changes", Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	archiveChangePublish(t, f, 123, Profile{"userId": 99, "age": 24, "name": "Before"}, "2026-11-08 10:00:00", 55)
	version := archiveChangePublish(t, f, 123, Profile{"userId": 99, "age": 24, "name": "After"}, "2026-11-08 11:00:00", 55)
	p, err := s.Store.Version(ctx, 123, version)
	if err != nil {
		t.Fatal(err)
	}
	c := syncPublicationCycle(t, s, 123)
	syncPublicationCohort(t, s, c, p)
	syncPublicationPublish(t, s, &c)
	if ready, err := s.syncCyclePublicationComplete(ctx, c); err != nil || ready {
		t.Fatal("unseeded change absence was treated complete", ready, err)
	}
	if state := archiveChangeState(t, f, version); state != "pending" {
		t.Fatal("change candidate not materialized", state)
	}
	if _, err = s.Store.DB.Exec("UPDATE archive_change_posts SET state='uncertain'"); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.syncCyclePublicationComplete(ctx, c); err != nil || ready {
		t.Fatal("unknown notice was treated success", ready, err)
	}
	if len(f.calls) != 0 {
		t.Fatal("completion published or retried unknown notice")
	}
	if err = s.Store.User(ctx, 2, "other tenant"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.DB.Exec("UPDATE archive_change_posts SET state='sent',message_id=321,owner_id=2"); err != nil {
		t.Fatal(err)
	}
	if ready, err := s.syncCyclePublicationComplete(ctx, c); err != nil || ready {
		t.Fatal("foreign change row was treated as initial/no-op", ready, err)
	}
}

func TestSyncPublicationFrozenIndexPlanIgnoresLaterManualScoreUntilComplete(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	syncPublicationPublish(t, f.s, &c)
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal(ready, err)
	}
	hashes := func() string {
		t.Helper()
		var h string
		if err := f.s.Store.DB.QueryRow("SELECT group_concat(desired_sha,';') FROM profile_index_pages").Scan(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	before := hashes()
	f.analysis(t, p, "Итоговая оценка совместимости: 95%", time.Now())
	ss, err := f.s.Store.Session(context.Background(), 123)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE profile_index_topics SET next_refresh=0"); err != nil {
		t.Fatal(err)
	}
	if err = f.s.refreshProfileIndexPlan(context.Background(), ss); err != nil {
		t.Fatal(err)
	}
	if hashes() != before {
		t.Fatal("later manual score replaced frozen payloads")
	}
	for range 3 {
		f.step(t)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || !ready {
		t.Fatal("frozen index frontier became unfulfillable", ready, err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE sync_cycles SET stage='complete' WHERE id=?", c.ID); err != nil {
		t.Fatal(err)
	}
	if err = f.s.refreshProfileIndexPlan(context.Background(), ss); err != nil {
		t.Fatal(err)
	}
	if hashes() == before {
		t.Fatal("normal index refresh stayed frozen after cycle completion")
	}
}

func TestSyncPublicationCollectionGatesArchiveAndPresence(t *testing.T) {
	t.Run("archive", func(t *testing.T) {
		c, store, a, _, _ := archivePresentationFixture(t)
		s := &Service{Config: c, Store: store, Archiver: a}
		ctx := context.Background()
		if err := store.SaveSession(ctx, 123, Session{UserID: 5696, Generation: "archive", Token: "fixture"}); err != nil {
			t.Fatal(err)
		}
		cycle := syncPublicationCycle(t, s, 123)
		if _, err := store.DB.Exec("UPDATE sync_cycles SET stage='search' WHERE id=?", cycle.ID); err != nil {
			t.Fatal(err)
		}
		tg := NewTelegram(c)
		a.TG = tg
		tg.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
			t.Fatal("archive published during collection")
			return nil, nil
		})
		if err := a.processOne(ctx); err != nil {
			t.Fatal(err)
		}
		if n := syncPublicationCount(t, store, "SELECT COUNT(*) FROM archive_versions"); n != 0 {
			t.Fatal("archive claimed while collection was incomplete", n)
		}
	})
	t.Run("presence", func(t *testing.T) {
		f := newPresenceFixture(t)
		presenceTopic(t, f)
		presenceObserve(t, f, false)
		s := &Service{Config: f.watch.Archiver.Config, Store: f.store, Archiver: f.watch.Archiver}
		c := syncPublicationCycle(t, s, 1)
		if _, err := f.store.DB.Exec("UPDATE sync_cycles SET stage='refresh' WHERE id=?", c.ID); err != nil {
			t.Fatal(err)
		}
		f.watch.Archiver.TG.HTTP.Transport = archiveRoundTripper(func(*http.Request) (*http.Response, error) {
			t.Fatal("presence published during refresh")
			return nil, nil
		})
		if did, err := f.watch.deliverProfilePresence(context.Background()); err != nil || did {
			t.Fatal("presence crossed collection barrier", did, err)
		}
		if state := presenceEventState(t, f); state != "pending" {
			t.Fatal("collection changed presence outbox", state)
		}
	})
}

func TestSyncPublicationLatePresenceDefersUnsentNoticesWithoutExpandingFrontier(t *testing.T) {
	for _, availability := range []string{"available", "unavailable"} {
		t.Run(availability, func(t *testing.T) {
			f := newProfileIndexesFixture(t)
			p := f.add(t, 7, 24, "First", "")
			f.analysis(t, p, "Итоговая оценка совместимости: 95%", time.Now())
			a, err := f.s.Store.LatestProfileAnalysis(context.Background(), 123, 7)
			if err != nil || a == nil {
				t.Fatal(err)
			}
			c := syncPublicationCycle(t, f.s, 123)
			syncPublicationCohort(t, f.s, c, p)
			syncPublicationAnalysisFrontier(t, f.s, c, *a)
			syncPublicationPublish(t, f.s, &c)
			if _, err = f.s.Store.DB.Exec(`INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at) VALUES(123,7,?,2,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`, availability); err != nil {
				t.Fatal(err)
			}
			if _, err = f.s.Store.DB.Exec(`INSERT INTO profile_presence_events(owner_id,profile_id,transition,availability,observed_at,version_id) VALUES(123,7,2,?,CURRENT_TIMESTAMP,?)`, availability, p.VersionID); err != nil {
				t.Fatal(err)
			}
			if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
				t.Fatal("unconfirmed index pages skipped", ready, err)
			}
			var disposition, reason string
			if err = f.s.Store.DB.QueryRow("SELECT disposition,reason FROM sync_publication_items WHERE cycle_id=? AND kind='analysis'", c.ID).Scan(&disposition, &reason); err != nil {
				t.Fatal(err)
			}
			if disposition != "ignored" || reason == "" {
				t.Fatal("late source observation left an unfulfillable frontier", disposition, reason)
			}
			if n := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM sync_publication_items WHERE cycle_id=? AND kind='presence'", c.ID); n != 0 {
				t.Fatal("late event expanded frozen membership")
			}
			if did, err := f.s.deliverAnalysisPost(context.Background()); err != nil || did {
				t.Fatal("analysis published over a late presence event", did, err)
			}
			for range 3 {
				f.step(t)
			}
			if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || !ready {
				t.Fatal("late event permanently blocked frozen completion", ready, err)
			}
			if n := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM profile_presence_events WHERE state='pending' AND message_id=0"); n != 1 {
				t.Fatal("late event outbox was changed")
			}
			if n := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM analysis_posts WHERE analysis_id=? AND state='pending' AND message_id=0", a.ID); n != 1 {
				t.Fatal("analysis outbox/history was changed")
			}
		})
	}
}

func TestSyncPublicationLatePresenceNeverDisposesUnknownAnalysis(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	f.analysis(t, p, "Итоговая оценка совместимости: 95%", time.Now())
	a, err := f.s.Store.LatestProfileAnalysis(context.Background(), 123, 7)
	if err != nil || a == nil {
		t.Fatal(err)
	}
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	syncPublicationAnalysisFrontier(t, f.s, c, *a)
	syncPublicationPublish(t, f.s, &c)
	if _, err = f.s.Store.DB.Exec(`INSERT INTO profile_presence(owner_id,profile_id,availability,transition,changed_at,checked_at) VALUES(123,7,'unavailable',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE analysis_posts SET state='uncertain' WHERE analysis_id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("unknown request was masked by a later source observation", ready, err)
	}
	var disposition string
	if err = f.s.Store.DB.QueryRow("SELECT disposition FROM sync_publication_items WHERE cycle_id=? AND kind='analysis'", c.ID).Scan(&disposition); err != nil || disposition != "required" {
		t.Fatal("unknown send disposed", disposition, err)
	}
}

func TestSyncPublicationWaitsForScopedCurrentCaptionRevisionBeforeIndexFreeze(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 7, 24, "First", "")
	c := syncPublicationCycle(t, f.s, 123)
	syncPublicationCohort(t, f.s, c, p)
	syncPublicationPublish(t, f.s, &c)
	if _, err := f.s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	for _, j := range []struct {
		kind                    string
		receipt, owner, version int64
		revision                string
	}{
		{"private", 701, 123, 0, captionDesignRevision}, {"archive", 702, 123, p.VersionID, captionDesignRevision},
		{"archive", 703, 123, 999999, captionDesignRevision}, {"private", 704, 2, 0, captionDesignRevision}, {"private", 705, 123, 0, "older-revision"},
	} {
		if _, err := f.s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id) VALUES(?,?,?,?,?,'[42]',123,0)`, j.revision, j.kind, j.receipt, j.owner, j.version); err != nil {
			t.Fatal(err)
		}
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("current caption backlog skipped", ready, err)
	}
	if n := syncPublicationCount(t, f.s.Store, "SELECT COUNT(*) FROM sync_publication_indexes WHERE cycle_id=?", c.ID); n != 0 {
		t.Fatal("indexes frozen before caption confirmations")
	}
	if _, err := f.s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done' WHERE receipt_id=702"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("private backlog skipped", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='failed' WHERE receipt_id=701"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("failed caption counted as confirmed", ready, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE caption_maintenance_jobs SET state='done' WHERE receipt_id=701"); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || ready {
		t.Fatal("unconfirmed index pages skipped", ready, err)
	}
	for range 3 {
		f.step(t)
	}
	if ready, err := f.s.syncCyclePublicationComplete(context.Background(), c); err != nil || !ready {
		t.Fatal("unrelated owner/version/revision caption work blocked the cycle", ready, err)
	}
}

func TestSyncPublicationCaptionClaimKeepsPausedTenantsIndependent(t *testing.T) {
	f := newProfileIndexesFixture(t)
	s := f.s
	ctx := context.Background()
	primary := syncPublicationCycle(t, s, 123)
	if _, err := s.Store.DB.Exec("UPDATE sync_cycles SET stage='search' WHERE id=?", primary.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.User(ctx, 2, "second tenant"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveSession(ctx, 2, Session{UserID: 222, Generation: "second", Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	other := syncPublicationCycle(t, s, 2)
	syncPublicationPublish(t, s, &other)
	if _, err := s.Store.DB.Exec(captionMaintenanceSchema); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []int64{123, 2} {
		if _, err := s.Store.DB.Exec(`INSERT INTO caption_maintenance_jobs(revision,kind,receipt_id,owner_id,version_id,inventory,chat_id,thread_id) VALUES(?,'private',?,?,0,'[42]',?,0)`, captionDesignRevision, owner, owner, owner); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Store.DB.Exec("UPDATE users SET paused=1 WHERE id=123"); err != nil {
		t.Fatal(err)
	}
	w := captionMaintenanceWorker{s, time.Now}
	job, err := w.claim(ctx)
	if err != nil || job == nil || job.owner != 2 {
		t.Fatal("paused primary blocked an independent tenant", job, err)
	}
	if n := syncPublicationCount(t, s.Store, "SELECT COUNT(*) FROM caption_maintenance_jobs WHERE owner_id=123 AND state='pending' AND attempts=0"); n != 1 {
		t.Fatal("paused tenant was claimed")
	}
}
