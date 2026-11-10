package companion

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

func TestReactionMessageIronMixOnlyAnchoredIntro(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"Лика", "Привет! Русский текст. @fixture_user", "Д\u04d5 бон хорз! Русский текст. @fixture_user"},
		{"инесса", "Инесса, привет!  Текст\n@fixture_user", "Инесса, д\u04d5 бон хорз!  Текст\n@fixture_user"},
		{"Kristina", "Привет, Kristina! Текст. @fixture_user", "Kristina, д\u04d5 бон хорз! Текст. @fixture_user"},
		{"Алана", "Алана, приветик! Текст. @fixture_user", "Алана, д\u04d5 бон хорз! Текст. @fixture_user"},
		{"Кристина", "Кристина, саламик) Текст. @fixture_user", "Кристина, д\u04d5 бон хорз! Текст. @fixture_user"},
		{"Ася", "Хотел сказать: «Ася, привет!» А тебе добрый вечер.", "Хотел сказать: «Ася, привет!» А тебе добрый вечер."},
		{"Ася", "Другое имя, привет! Текст.", "Другое имя, привет! Текст."},
		{"Ася", "Здравствуйте! Текст.", "Здравствуйте! Текст."},
		{"Ася", "", ""},
	} {
		if got := ironMixReactionMessage(tc.in, tc.name); got != tc.want {
			t.Fatalf("intro/body contract changed: %q != %q", got, tc.want)
		}
	}
}

func ironFixtureName(t *testing.T, f *autoFixture) {
	t.Helper()
	s := f.worker.Service.Store
	p, err := s.Profile(context.Background(), 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	p.Profile["name"] = "Лика"
	if _, err = s.ArchiveViewed(context.Background(), 1, []Profile{p.Profile}, "iron fixture"); err != nil {
		t.Fatal(err)
	}
}

func TestReactionMessageIronMixPersonalKeepsImmutableDraftAndExactIntent(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	ironFixtureName(t, f)
	original := "Лика, привет! Русский текст.\n@fixture_user"
	if _, err := s.ImportReactionMessageDrafts(ctx, 1, draftCacheBundle(t, draftCacheEntry(t, f, 7, "approved", original))); err != nil {
		t.Fatal(err)
	}
	var captureBefore []byte
	if err := s.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts").Scan(&captureBefore); err != nil {
		t.Fatal(err)
	}
	policy, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "fixture_user", PreferDrafts: true, IronMix: true})
	if err != nil {
		t.Fatal(err)
	}
	messages := captureReactionMessages(t, f)
	if _, err = f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); err != nil {
		t.Fatal(err)
	}
	want := "Лика, д\u04d5 бон хорз! Русский текст.\n@fixture_user"
	if len(*messages) != 1 || (*messages)[0].Message != want {
		t.Fatal("personal style not sent exactly", *messages)
	}
	var source string
	var after []byte
	if err = s.DB.QueryRow("SELECT message,capture_json FROM reaction_message_drafts").Scan(&source, &after); err != nil || source != original || !bytes.Equal(captureBefore, after) {
		t.Fatal("style mutated immutable draft", err)
	}
	var saved string
	var revision int64
	if err = s.DB.QueryRow("SELECT message,policy_revision FROM reaction_message_intents").Scan(&saved, &revision); err != nil || saved != want || revision != policy.Revision {
		t.Fatal("styled message or revision missing in intent", saved, revision, err)
	}
	other, err := s.ReactionMessagePolicy(ctx, 2)
	if err != nil || other.IronMix {
		t.Fatal("style leaked to another owner", other, err)
	}
}

func TestReactionMessageIronMixFallbackAndCustomPolicies(t *testing.T) {
	for _, mode := range []string{"birthday", "first", "rotate"} {
		t.Run(mode, func(t *testing.T) {
			f := draftCacheFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			ironFixtureName(t, f)
			original := "Лика, привет! Русский текст. @fixture_user"
			templates := []string{original}
			if mode == "birthday" {
				templates = nil
				p, err := s.Profile(ctx, 1, 7)
				if err != nil {
					t.Fatal(err)
				}
				original, _, err = birthdayReactionMessage(p.Profile, "fixture_user", 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			if !strings.HasPrefix(original, "Лика, привет!") {
				t.Fatal("fixture has no supported opening")
			}
			if _, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: mode, Templates: templates, TelegramUsername: "fixture_user", IronMix: true}); err != nil {
				t.Fatal(err)
			}
			messages := captureReactionMessages(t, f)
			if _, err := f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); err != nil {
				t.Fatal(err)
			}
			want := "Лика, д\u04d5 бон хорз!" + strings.TrimPrefix(original, "Лика, привет!")
			if len(*messages) != 1 || (*messages)[0].Message != want {
				t.Fatal("style changed body or did not reach intent", *messages)
			}
		})
	}
}

func TestReactionMessageIronMixExplicitAndOffStayExact(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "off", true: "explicit"}[explicit], func(t *testing.T) {
			f := draftCacheFixture(t)
			s := f.worker.Service.Store
			ctx := context.Background()
			if _, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "off", TelegramUsername: "fixture_user", IronMix: true}); err != nil {
				t.Fatal(err)
			}
			messages := captureReactionMessages(t, f)
			want := ""
			var err error
			if explicit {
				want = "Привет!  Точный текст\n@fixture_user "
				_, err = f.worker.Service.Client.LikeWithMessageGuarded(ctx, 1, 7, want, nil)
			} else {
				_, err = f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil)
			}
			if err != nil || len(*messages) != 1 || (*messages)[0].Message != want {
				t.Fatal("explicit/off message changed", err, *messages)
			}
		})
	}
}

func TestReactionMessageIronMixOverflowRollsBackBeforePost(t *testing.T) {
	f := draftCacheFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	template := "Привет!" + strings.Repeat("x", 493)
	if textUnits(template) != 500 {
		t.Fatal("fixture size")
	}
	if _, err := s.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "rotate", Templates: []string{template}, IronMix: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.worker.Service.Client.LikeGuarded(ctx, 1, 7, nil); !errors.Is(err, ErrInvalidReactionMessage) || len(f.sent) != 0 {
		t.Fatal("overflow reached POST", err, f.sent)
	}
	policy, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || policy.NextCursor != 0 {
		t.Fatal("overflow consumed cursor", policy, err)
	}
	for _, table := range []string{"actions", "reaction_message_intents"} {
		var count int
		if err = s.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("overflow left outgoing rows", table, count, err)
		}
	}
}

func TestReactionMessageIronMixLegacyMigrationAndBackup(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	_, err := s.DB.Exec(`CREATE TABLE reaction_message_settings(owner_id INTEGER PRIMARY KEY REFERENCES users(id),mode TEXT NOT NULL,templates_json TEXT NOT NULL,telegram_username TEXT NOT NULL,prefer_drafts INTEGER NOT NULL,next_cursor INTEGER NOT NULL,revision INTEGER NOT NULL,updated_at TEXT);INSERT INTO reaction_message_settings VALUES(1,'birthday','[]','fixture_user',1,9,17,'old')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fullDurableStateRecords(ctx, s.DB); err != nil {
		t.Fatal("legacy backup before migration failed", err)
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
			if e == nil && (p.IronMix || !p.PreferDrafts || p.NextCursor != 9 || p.Revision != 17) {
				e = errors.New("style migration changed existing policy")
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
	before, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(before)
	if _, err = s.DB.Exec("UPDATE reaction_message_settings SET iron_mix=1 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	after, err := fullDurableStateRecords(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(after)
	if reflect.DeepEqual(before, after) {
		t.Fatal("iron-only change did not dirty backup")
	}
	snapshot := filepath.Join(t.TempDir(), "iron.sqlite")
	if err = s.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+snapshot+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var enabled, prefer bool
	var cursor, revision int64
	if err = db.QueryRow("SELECT iron_mix,prefer_drafts,next_cursor,revision FROM reaction_message_settings WHERE owner_id=1").Scan(&enabled, &prefer, &cursor, &revision); err != nil || !enabled || !prefer || cursor != 9 || revision != 17 {
		t.Fatal("snapshot did not preserve style and old policy", enabled, prefer, cursor, revision, err)
	}
}
