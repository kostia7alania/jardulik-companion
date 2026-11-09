package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func draftCacheFixture(t *testing.T) *autoFixture {
	t.Helper()
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	if err := s.SaveAccount(context.Background(), 1, Profile{"userId": int64(101), "age": int64(33), "minAge": int64(18), "maxAge": int64(25)}); err != nil {
		t.Fatal(err)
	}
	f.add(t, 1, 7, 23, false)
	if _, err := s.SetReactionMessagePolicy(context.Background(), 1, "off", nil, "fixture_user"); err != nil {
		t.Fatal(err)
	}
	return f
}
func draftCacheEntry(t *testing.T, f *autoFixture, id int64, state, message string) map[string]any {
	t.Helper()
	p, err := f.worker.Service.Store.Profile(context.Background(), 1, id)
	if err != nil {
		t.Fatal(err)
	}
	basis, err := reactionDraftBasis(p.Profile)
	if err != nil {
		t.Fatal(err)
	}
	evidence := []reactionDraftEvidence{}
	if state == "hold" {
		evidence = append(evidence, reactionDraftEvidence{Field: "name", Quote: p.Profile.String("name")})
	}
	return map[string]any{"profile_id": id, "basis": basis, "state": state, "message": message, "review": reactionDraftReview{Approved: state == "approved", Notes: "two passes reviewed", Evidence: evidence}, "provenance": map[string]any{"first_pass": map[string]string{"message": message}}}
}
func draftCacheBundle(t *testing.T, entries ...map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"schema": ReactionMessageDraftSchema, "owner_id": int64(1), "upstream_id": int64(101), "telegram_username": "fixture_user", "drafts": entries})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func enableDraftCache(t *testing.T, s *Store, handle string) {
	t.Helper()
	if _, err := s.ConfigureReactionMessagePolicy(context.Background(), 1, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: handle, PreferDrafts: true}); err != nil {
		t.Fatal(err)
	}
}
func prepareDraftCacheIntent(t *testing.T, f *autoFixture, owner, id int64) (string, string) {
	t.Helper()
	s := f.worker.Service.Store
	ctx := context.Background()
	p, err := s.Profile(ctx, owner, id)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := s.Session(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	action, err := s.ClaimOwnerAction(ctx, owner, id, f.now)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.prepareLikeMessage(ctx, owner, action, p, ss, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wire reactionMessageWire
	if err = json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	var mode string
	if err = s.DB.QueryRow("SELECT message_mode FROM reaction_message_intents WHERE action_id=?", action).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return wire.Message, mode
}

func TestReactionDraftImportIdempotentOffAndExactPersonalIntent(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	message := "  Привет, у меня есть вопрос @fixture_user  \n"
	raw := draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", message))
	for i := 0; i < 2; i++ {
		r, err := s.ImportReactionMessageDrafts(ctx, 1, raw)
		if err != nil || r.Approved != 1 || i == 0 && r.Imported != 1 || i == 1 && r.Unchanged != 1 {
			t.Fatal("import not idempotent", r, err)
		}
	}
	p, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.Mode != "off" || p.PreferDrafts || len(f.requests) != 0 {
		t.Fatal("import activated policy or used HTTP", p, err, f.requests)
	}
	enableDraftCache(t, s, "fixture_user")
	messages := captureReactionMessages(t, f)
	if _, err = f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); err != nil {
		t.Fatal(err)
	}
	if len(*messages) != 1 || (*messages)[0].Message != message {
		t.Fatal("personal text changed", *messages)
	}
	if _, err = f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); !errors.Is(err, ErrActionBlocked) || len(*messages) != 1 {
		t.Fatal("personal result replayed", err, *messages)
	}
	p, err = s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != 0 {
		t.Fatal("personal cache consumed birthday cursor", p, err)
	}
	var mode string
	var reserved int64
	if err = s.DB.QueryRow("SELECT message_mode,reserved_cursor FROM reaction_message_intents").Scan(&mode, &reserved); err != nil || mode != "personal" || reserved != -1 {
		t.Fatal("wrong personal intent", mode, reserved, err)
	}
	conflict := draftCacheEntry(t, f, 7, "approved", "Different @fixture_user")
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, conflict)); err == nil {
		t.Fatal("approved content was overwritten")
	}
}

func TestReactionDraftImportRollsBackWholeBatchAndBindingFailures(t *testing.T) {
	for _, kind := range []string{"last_stale", "owner", "source", "handle", "unknown_basis"} {
		t.Run(kind, func(t *testing.T) {
			f := draftCacheFixture(t)
			s := f.worker.Service.Store
			f.add(t, 1, 8, 24, false)
			first := draftCacheEntry(t, f, 7, "approved", "First @fixture_user")
			second := draftCacheEntry(t, f, 8, "approved", "Second @fixture_user")
			if kind == "last_stale" {
				second["basis"].(Profile)["description"] = "stale input"
			}
			if kind == "unknown_basis" {
				second["basis"].(Profile)["extra_source_field"] = "unsupported"
			}
			raw := draftCacheBundle(t, first, second)
			if kind == "owner" || kind == "source" || kind == "handle" {
				var body map[string]any
				_ = json.Unmarshal(raw, &body)
				switch kind {
				case "owner":
					body["owner_id"] = 2
				case "source":
					body["upstream_id"] = 999
				case "handle":
					body["telegram_username"] = "other_user"
				}
				raw, _ = json.Marshal(body)
			}
			if _, err := s.ImportReactionMessageDrafts(context.Background(), 1, raw); err == nil {
				t.Fatal("invalid bundle accepted", kind)
			}
			_ = s.initReactionMessages(context.Background())
			var count int
			if err := s.DB.QueryRow("SELECT count(*) FROM reaction_message_drafts").Scan(&count); err != nil || count != 0 {
				t.Fatal("invalid final item left a partial import", count, err)
			}
			if len(f.requests) != 0 {
				t.Fatal("import used HTTP")
			}
		})
	}
}

func TestReactionDraftFreshBindingAndBirthdayFallback(t *testing.T) {
	for _, kind := range []string{"content", "handle", "upstream", "owner", "volatile_only"} {
		t.Run(kind, func(t *testing.T) {
			f := draftCacheFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			personal := "Personal @fixture_user"
			if _, err := s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", personal))); err != nil {
				t.Fatal(err)
			}
			enableDraftCache(t, s, "fixture_user")
			owner := int64(1)
			handle := "fixture_user"
			switch kind {
			case "content", "volatile_only":
				p, err := s.Profile(ctx, 1, 7)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "content" {
					p.Profile["description"] = "new current profile"
				} else {
					p.Profile["distance"] = 234
					p.Profile["type"] = 1
					p.Profile["photos"] = []string{"https://example.test/new-photo"}
				}
				if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "draft changed fixture"); err != nil {
					t.Fatal(err)
				}
			case "handle":
				handle = "other_user"
				enableDraftCache(t, s, handle)
			case "upstream":
				ss, err := s.Session(ctx, 1)
				if err != nil {
					t.Fatal(err)
				}
				ss.UserID = 999
				ss.Generation = "replaced"
				if err = s.SaveSession(ctx, 1, ss); err != nil {
					t.Fatal(err)
				}
			case "owner":
				owner = 2
				f.add(t, 2, 7, 23, false)
				if _, err := s.ConfigureReactionMessagePolicy(ctx, 2, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: handle, PreferDrafts: true}); err != nil {
					t.Fatal(err)
				}
			}
			message, mode := prepareDraftCacheIntent(t, f, owner, 7)
			if kind == "volatile_only" {
				if message != personal || mode != "personal" {
					t.Fatal("irrelevant runtime fields invalidated semantic cache", message, mode)
				}
			} else if message == personal || mode != "birthday" || !strings.Contains(message, "@"+handle) {
				t.Fatal("stale or foreign draft bypassed fallback", message, mode)
			}
			if len(f.requests) != 0 {
				t.Fatal("preparation used HTTP")
			}
		})
	}
}

func TestReactionDraftPersistentHoldBlocksOffExplicitAndChangedContent(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	if _, err := s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "hold", ""))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetReactionMessagePolicy(ctx, 1, "off", nil, "other_user"); err != nil {
		t.Fatal(err)
	}
	p, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["name"] = "changed name"
	p.Profile["description"] = "edited bio"
	p.Profile["age"] = 24
	if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "hold changed content"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.worker.Service.Client.LikeWithMessageGuarded(ctx, 1, 7, "", nil); !errors.Is(err, ErrReactionMessageReviewHold) || len(f.sent) != 0 {
		t.Fatal("persistent hold bypassed by off/empty explicit/changed content", err, f.sent)
	}
	if _, err = s.SetReactionMessagePolicy(ctx, 1, "off", nil, "fixture_user"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", "Reviewed @fixture_user"))); !errors.Is(err, ErrReactionMessageReviewHold) {
		t.Fatal("approved import cleared a hold", err)
	}
	f.add(t, 2, 7, 23, false)
	if _, err := s.ConfigureReactionMessagePolicy(ctx, 2, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "fixture_user", PreferDrafts: true}); err != nil {
		t.Fatal(err)
	}
	if _, mode := prepareDraftCacheIntent(t, f, 2, 7); mode != "birthday" {
		t.Fatal("hold leaked to another owner")
	}
}

func TestReactionDraftCurrentMinorDOBWinsOverApprovedCache(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	f.add(t, 1, 7, 18, false)
	raw := draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", "Adult reviewed @fixture_user"))
	if _, err := s.ImportReactionMessageDrafts(ctx, 1, raw); err != nil {
		t.Fatal(err)
	}
	enableDraftCache(t, s, "fixture_user")
	p, err := s.Profile(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["birthday"] = time.Now().UTC().AddDate(-17, 0, 0).Format("2006-01-02")
	if _, err = s.ArchiveViewed(ctx, 1, []Profile{p.Profile}, "DOB conflict fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); !errors.Is(err, ErrReactionMessageTargetAge) || len(f.sent) != 0 {
		t.Fatal("personal cache bypassed current DOB guard", err, f.sent)
	}
	var intents int
	if err = s.DB.QueryRow("SELECT count(*) FROM reaction_message_intents").Scan(&intents); err != nil || intents != 0 {
		t.Fatal("minor DOB created an intent", intents, err)
	}
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, raw); err == nil {
		t.Fatal("import ignored current minor DOB")
	}
}

func TestReactionDraftLegacyMigrationAndBackupBeforeInit(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	_, err := s.DB.Exec(`CREATE TABLE reaction_message_settings(owner_id INTEGER PRIMARY KEY REFERENCES users(id),mode TEXT NOT NULL,templates_json TEXT NOT NULL,telegram_username TEXT NOT NULL,next_cursor INTEGER NOT NULL,revision INTEGER NOT NULL,updated_at TEXT);INSERT INTO reaction_message_settings VALUES(1,'birthday','[]','fixture_user',9,17,'old')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fullDurableStateRecords(ctx, s.DB); err != nil {
		t.Fatal("backup failed before lazy migration", err)
	}
	var prefer int
	if err = s.DB.QueryRow("SELECT count(*) FROM pragma_table_info('reaction_message_settings') WHERE name='prefer_drafts'").Scan(&prefer); err != nil || prefer != 0 {
		t.Fatal("backup mutated legacy schema", prefer, err)
	}
	other, err := OpenStore(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			p, e := st.ReactionMessagePolicy(ctx, 1)
			if e == nil && (p.Mode != "birthday" || p.NextCursor != 9 || p.Revision != 17 || p.PreferDrafts) {
				e = errors.New("migration changed existing policy")
			}
			errs <- e
		}(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReactionDraftBackupFingerprintAndExactSnapshot(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	raw := draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", "Backup @fixture_user"))
	before, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(before)
	if _, err = s.ImportReactionMessageDrafts(ctx, 1, raw); err != nil {
		t.Fatal(err)
	}
	after, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("draft-only import did not dirty backup")
	}
	enableDraftCache(t, s, "fixture_user")
	before, err = fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(before)
	if _, err = s.DB.Exec("UPDATE reaction_message_settings SET prefer_drafts=0 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	after, err = fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("prefer_drafts was excluded from backup fingerprint")
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.sqlite")
	if err = s.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite", "file:"+snapshot+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var original, copied []byte
	if err = s.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts").Scan(&original); err != nil {
		t.Fatal(err)
	}
	if err = restored.QueryRow("SELECT capture_json FROM reaction_message_drafts").Scan(&copied); err != nil || !bytes.Equal(original, copied) {
		t.Fatal("whole snapshot changed review provenance", err)
	}
}
