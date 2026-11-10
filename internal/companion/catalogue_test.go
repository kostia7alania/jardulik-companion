package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCatalogueMigrationEditsExistingSlotsAndPreservesLegacyPages(t *testing.T) {
	f := newProfileIndexesFixture(t)
	f.s.Archiver.now = func() time.Time { return f.now }
	f.add(t, 10, 23, "Current", "2003-03-01")
	ctx := context.Background()
	ss, err := f.s.Store.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	var primary, age int64
	for _, entry := range []struct {
		kind   string
		thread int64
		id     *int64
	}{{profileIndexCompatibility, 3040, &primary}, {profileIndexAge, 3041, &age}} {
		receipt := fmt.Sprintf(`{"message_thread_id":%d}`, entry.thread)
		result, err := f.s.Store.DB.Exec(`INSERT INTO profile_index_topics(owner_id,upstream_id,generation,kind,chat_id,thread_id,state,receipt_json) VALUES(123,5696,?,?,?,?,'sent',?)`, ss.Generation, entry.kind, f.s.Config.BackupChatID, entry.thread, receipt)
		if err != nil {
			t.Fatal(err)
		}
		*entry.id, _ = result.LastInsertId()
	}
	for _, topic := range []int64{primary, age} {
		count := 2
		if topic == primary {
			count = 5
		}
		for page := 1; page <= count; page++ {
			label := fmt.Sprintf("Retained row %d/%d", topic, page)
			old := profileIndexPayload{Text: "Old title\n" + label, Entities: []MessageEntity{{Type: "text_link", Offset: textUnits("Old title\n"), Length: textUnits(label), URL: profileIndexCardURL(f.s.Config.BackupChatID, 110, 1010)}}}
			raw, sha, err := profileIndexPayloadBytes(old)
			if err != nil {
				t.Fatal(err)
			}
			message, state := topic*100+int64(page), "sent"
			if topic == primary && page >= 4 {
				message, state = 0, "pending"
			}
			if topic == primary && page == 5 {
				state = "uncertain"
			}
			if _, err = f.s.Store.DB.Exec(`INSERT INTO profile_index_pages(topic_id,page_no,message_id,state,payload_json,desired_sha,sent_sha,receipt_json) VALUES(?,?,?,?,?,?,?,?)`, topic, page, message, state, raw, sha, sha, fmt.Sprintf(`{"message_id":%d}`, message)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = f.s.refreshProfileIndexPlan(ctx, ss); err != nil {
		t.Fatal(err)
	}
	if f.count(t, "SELECT count(*) FROM profile_index_topics") != 2 || f.count(t, "SELECT count(*) FROM profile_index_pages") != 7 {
		t.Fatal("migration lost source records")
	}
	for _, entry := range []struct {
		topic  int64
		page   int
		prefix string
		legacy bool
	}{{primary, 1, "🧩 Совместимость", false}, {primary, 2, "🎂 Возраст", false}, {primary, 3, "🧩 Совместимость · архив", true}, {age, 1, "🎂 Возраст · архив", true}, {age, 2, "🎂 Возраст · архив", true}} {
		var message int64
		var raw []byte
		if err = f.s.Store.DB.QueryRow("SELECT message_id,payload_json FROM profile_index_pages WHERE topic_id=? AND page_no=?", entry.topic, entry.page).Scan(&message, &raw); err != nil {
			t.Fatal(err)
		}
		var payload profileIndexPayload
		if json.Unmarshal(raw, &payload) != nil || message != entry.topic*100+int64(entry.page) || !strings.HasPrefix(payload.Text, entry.prefix) || payload.Legacy != entry.legacy || validProfileIndexPayload(payload) != nil {
			t.Fatalf("wrong preserved page: %+v %d", payload, message)
		}
		if entry.legacy && (!strings.Contains(payload.Text, fmt.Sprintf("Retained row %d/%d", entry.topic, entry.page)) || payload.Entities[0].URL != forumThreadURL(f.s.Config.BackupChatID, 3040)) {
			t.Fatal("legacy body or catalogue pointer lost")
		}
	}
	if f.count(t, "SELECT count(*) FROM profile_index_pages WHERE topic_id=? AND page_no=4 AND message_id=0 AND state='error' AND error='unpublished_page_superseded'", primary) != 1 || f.count(t, "SELECT count(*) FROM profile_index_pages WHERE topic_id=? AND page_no=5 AND message_id=0 AND state='uncertain'", primary) != 1 {
		t.Fatal("unknown or unposted legacy state changed incorrectly")
	}
	for range 6 {
		f.step(t)
	}
	if strings.Join(f.operations, ",") != "editForumTopic,editMessageText,editMessageText,editMessageText,editMessageText,editMessageText" {
		t.Fatal("migration created or replayed posts", f.operations)
	}
	if worked, err := f.s.deliverProfileIndex(ctx); worked || err != nil {
		t.Fatal("unchanged migration replayed", worked, err)
	}
	if confirmed, err := f.s.catalogueTitleConfirmed(ctx, 123, 5696, ss.Generation); !confirmed || err != nil {
		t.Fatal("known title ACK not persisted", confirmed, err)
	}
}

func TestCatalogueRichDeliveryUsesRichMessageWithoutClassicText(t *testing.T) {
	f := newProfileIndexesFixture(t)
	f.s.Config.RichCards = true
	f.add(t, 10, 23, "Name", "2003-03-01")
	for range 3 {
		f.step(t)
	}
	if strings.Join(f.operations, ",") != "createForumTopic,sendRichMessage,sendRichMessage" {
		t.Fatal(f.operations)
	}
	for _, body := range f.bodies[1:] {
		if len(body["rich_message"]) == 0 || len(body["text"]) != 0 || len(body["entities"]) != 0 {
			t.Fatal("rich view used classic text")
		}
	}
}

func TestCatalogueKnownEditDuringSearchHoldsUnpublishedView(t *testing.T) {
	f := newProfileIndexesFixture(t)
	p := f.add(t, 10, 23, "Name", "2003-03-01")
	f.step(t) // Create Catalog.
	f.step(t) // Confirm only the compatibility view.
	cycle := syncPublicationCycle(t, f.s, 123)
	if _, err := f.s.Store.DB.Exec("UPDATE sync_cycles SET stage='search' WHERE id=?", cycle.ID); err != nil {
		t.Fatal(err)
	}
	f.analysis(t, p, "Оценка совместимости: 75%", f.now)
	f.now = f.now.Add(10 * time.Minute)
	f.step(t)
	before := len(f.operations)
	if worked, err := f.s.deliverProfileIndex(context.Background()); worked || err != nil || len(f.operations) != before {
		t.Fatal("unpublished view crossed the search barrier", worked, err, f.operations)
	}
	if strings.Join(f.operations, ",") != "createForumTopic,sendMessage,editMessageText" || f.count(t, "SELECT count(*) FROM profile_index_pages WHERE message_id=0 AND state='pending'") != 1 {
		t.Fatal("known edit changed the new-send frontier", f.operations)
	}
}

func TestCatalogueFrozenLegacyAgeCreateCompletesOriginalManifest(t *testing.T) {
	f := newProfileIndexesFixture(t)
	f.s.Archiver.now = func() time.Time { return f.now }
	cycle := syncPublicationCycle(t, f.s, 123)
	syncPublicationPublish(t, f.s, &cycle)
	ctx := context.Background()
	for _, kind := range []string{profileIndexCompatibility, profileIndexAge} {
		pages, err := renderProfileIndexPagesMode(kind, nil, f.s.Config.BackupChatID, false)
		if err != nil {
			t.Fatal(err)
		}
		result, err := f.s.Store.DB.Exec(`INSERT INTO profile_index_topics(owner_id,upstream_id,generation,kind,chat_id) VALUES(123,5696,?,?,?)`, cycle.Generation, kind, f.s.Config.BackupChatID)
		if err != nil {
			t.Fatal(err)
		}
		topic, _ := result.LastInsertId()
		raw, sha, err := profileIndexPayloadBytes(pages[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.s.Store.DB.Exec("INSERT INTO profile_index_pages(topic_id,page_no,payload_json,desired_sha) VALUES(?,1,?,?)", topic, raw, sha); err != nil {
			t.Fatal(err)
		}
		if _, err = f.s.Store.DB.Exec("INSERT INTO sync_publication_indexes(cycle_id,topic_id,page_no,payload_sha) VALUES(?,?,1,?)", cycle.ID, topic, sha); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.s.Store.DB.Exec("UPDATE sync_publication_manifests SET indexes_frozen=1 WHERE cycle_id=?", cycle.ID); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		f.step(t)
	}
	if strings.Join(f.operations, ",") != "createForumTopic,createForumTopic,sendMessage,sendMessage" {
		t.Fatal("legacy frozen frontier changed", f.operations)
	}
	var title string
	if json.Unmarshal(f.bodies[1]["name"], &title) != nil || title != profileIndexTitle(profileIndexAge) {
		t.Fatal("legacy age topic became a second Catalog", title)
	}
	if ready, err := f.s.syncPublicationIndexesComplete(ctx, cycle, true); !ready || err != nil {
		t.Fatal("legacy frozen manifest could not complete", ready, err)
	}
	if f.count(t, "SELECT count(*) FROM sync_publication_indexes WHERE cycle_id=?", cycle.ID) != 2 {
		t.Fatal("frozen pages silently migrated")
	}
}

func TestCatalogueStagingNeverReplaysUnknownSendingPage(t *testing.T) {
	f := newProfileIndexesFixture(t)
	ctx := context.Background()
	ss, err := f.s.Store.Session(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.seedProfileIndexTopics(ctx, ss); err != nil {
		t.Fatal(err)
	}
	var topic int64
	if err = f.s.Store.DB.QueryRow("SELECT id FROM profile_index_topics").Scan(&topic); err != nil {
		t.Fatal(err)
	}
	original := profileIndexPayload{Text: "Original unknown request", Entities: []MessageEntity{}}
	raw, sha, err := profileIndexPayloadBytes(original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec("INSERT INTO profile_index_pages(topic_id,page_no,state,payload_json,desired_sha) VALUES(?,1,'sending',?,?)", topic, raw, sha); err != nil {
		t.Fatal(err)
	}
	if err = f.s.refreshProfileIndexPlan(ctx, ss); err != nil {
		t.Fatal(err)
	}
	if f.count(t, "SELECT count(*) FROM profile_index_pages WHERE topic_id=? AND page_no=1 AND message_id=0 AND state='sending' AND desired_sha=?", topic, sha) != 1 {
		t.Fatal("unknown sending request reset by layout refresh")
	}
}

func TestCatalogueUnavailableScoreFallbackRequiresCurrentSourceReceipt(t *testing.T) {
	f := newProfileIndexesFixture(t)
	ctx := context.Background()
	if err := f.s.Store.InitSyncCycles(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{10, 11, 12, 13} {
		p := f.add(t, id, 23, "Name", "2003-03-01")
		f.analysis(t, p, "Оценка совместимости: 70%", f.now)
		f.analysis(t, p, "Профиль недоступен", f.now.Add(time.Minute))
		if _, err := f.s.Store.DB.Exec(`INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(123,?,'unavailable','now','now')`, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, cycle := range []struct {
		id         string
		upstream   int
		generation string
		started    int64
	}{{"current", 5696, "index-generation", 100}, {"foreign", 777, "index-generation", 200}, {"stale-generation", 5696, "old-generation", 300}} {
		if _, err := f.s.Store.DB.Exec(`INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES(?,123,?,?,'complete','fixture',?,1,1)`, cycle.id, cycle.upstream, cycle.generation, cycle.started); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		cycle string
		id    int
		state string
		at    int
	}{{"current", 10, "unavailable", 100}, {"current", 11, "available", 101}, {"foreign", 12, "unavailable", 201}, {"stale-generation", 13, "unavailable", 301}} {
		if _, err := f.s.Store.DB.Exec(`INSERT INTO sync_refresh_items(cycle_id,profile_id,state,observed_at_ms) VALUES(?,?,?,?)`, item.cycle, item.id, item.state, item.at); err != nil {
			t.Fatal(err)
		}
	}
	scores, err := f.s.profileIndexScores(ctx, 123, 5696)
	if err != nil || len(scores) != 1 || scores[10] != 70 {
		t.Fatalf("fallback mixed sources or available ambiguity: %+v %v", scores, err)
	}
	if _, err = f.s.Store.DB.Exec(`INSERT INTO sync_cycles(id,owner_id,upstream_id,generation,stage,search_run_id,started_at_ms,updated_at_ms,next_cycle_ms) VALUES('new',123,5696,'index-generation','complete','fixture',400,400,400)`); err != nil {
		t.Fatal(err)
	}
	scores, err = f.s.profileIndexScores(ctx, 123, 5696)
	if err != nil || len(scores) != 0 {
		t.Fatal("old cycle negative result reused as current evidence", scores, err)
	}
}
