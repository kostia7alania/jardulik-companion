package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type openingPostsFixture struct {
	s       *Service
	now     time.Time
	ticks   atomic.Int64
	methods []string
	texts   []openingPostPayload
	respond func(*http.Request, string, map[string]json.RawMessage) (*http.Response, error)
}

func newOpeningPostsFixture(t *testing.T) *openingPostsFixture {
	t.Helper()
	c, store := archiveTestStore(t)
	c.TelegramBase = "https://telegram.test"
	ctx := context.Background()
	if err := store.SaveSession(ctx, 123, Session{Token: "private-access", Refresh: "private-refresh", UserID: 5696, Generation: "opening-generation"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAccount(ctx, 123, Profile{"userId": 5696, "age": 33}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetReactionMessagePolicy(ctx, 123, "birthday", nil, "fixture_user"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConfigureReactionMessagePolicy(ctx, 123, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "fixture_user", PreferDrafts: true, IronMix: true}); err != nil {
		t.Fatal(err)
	}
	a := NewArchiver(c, store, NewTelegram(c), nil)
	s := &Service{Config: c, Store: store, Archiver: a}
	f := &openingPostsFixture{s: s, now: time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)}
	a.now = func() time.Time { return f.now.Add(time.Duration(f.ticks.Add(1)) * 10 * time.Second) }
	a.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		f.methods = append(f.methods, method)
		if method != "createForumTopic" && method != "sendMessage" && method != "editMessageText" {
			t.Fatalf("unexpected operation: %s", method)
		}
		var payload openingPostPayload
		if method != "createForumTopic" {
			encoded, _ := json.Marshal(body)
			if err := json.Unmarshal(encoded, &payload); err != nil {
				t.Fatal(err)
			}
			f.texts = append(f.texts, payload)
			if _, err := openingPostBytes(payload); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(payload.Text, "private-access") || strings.Contains(payload.Text, "private-device") {
				t.Fatal("private provenance exported")
			}
		}
		if f.respond != nil {
			return f.respond(r, method, body)
		}
		if method == "createForumTopic" {
			return albumTestResponse(r, 200, `{"ok":true,"result":{"message_thread_id":700}}`), nil
		}
		var message int64
		_ = json.Unmarshal(body["message_id"], &message)
		if message == 0 {
			message = 2000 + int64(len(f.methods))
		}
		return albumTestResponse(r, 200, fmt.Sprintf(`{"ok":true,"result":{"message_id":%d,"message_thread_id":700,"chat":{"id":-100123}}}`, message)), nil
	})
	if err := s.initOpeningPosts(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *openingPostsFixture) addDraft(t *testing.T, id int64, name, message string) []byte {
	t.Helper()
	ctx := context.Background()
	profile := Profile{"userId": id, "name": name, "age": 23, "height": 170, "birthday": "2003-04-10", "description": "люблю музыку"}
	if _, err := f.s.Store.Archive(ctx, 123, []Profile{profile}, "opening-fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec("INSERT OR REPLACE INTO mirror_topics(profile_id,thread_id) VALUES(?,?)", id, id+100); err != nil {
		t.Fatal(err)
	}
	basis, hash, err := reactionDraftContent(profile)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"profile_id": id, "basis": basis, "state": "approved", "message": message, "review": reactionDraftReview{Approved: true, Notes: "reviewed", Evidence: []reactionDraftEvidence{{Field: "description", Quote: "музыку"}}}, "provenance": map[string]any{"device": "private-device", "first_pass": message}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.s.Store.DB.Exec(`INSERT INTO reaction_message_drafts(owner_id,upstream_id,profile_id,telegram_username,content_hash,basis_hash,state,message,capture_json,capture_sha) VALUES(123,5696,?,'fixture_user',?,?,'approved',?,?,?)`, id, hash, reactionDraftBindingHash(123, 5696, id, "fixture_user", hash), message, raw, digest(raw))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func (f *openingPostsFixture) step(t *testing.T) {
	t.Helper()
	if ok, err := f.s.deliverOpeningPost(context.Background()); !ok || err != nil {
		t.Fatalf("delivery %v %v", ok, err)
	}
}
func (f *openingPostsFixture) refresh(t *testing.T) {
	t.Helper()
	if _, err := f.s.Store.DB.Exec("UPDATE opening_audit_topics SET next_refresh=0"); err != nil {
		t.Fatal(err)
	}
}
func (f *openingPostsFixture) scalar(t *testing.T, query string, args ...any) string {
	t.Helper()
	var value string
	if err := f.s.Store.DB.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (f *openingPostsFixture) addScore(t *testing.T, owner, upstream, target, observed int64, content string) {
	t.Helper()
	request, _ := json.Marshal(map[string]int64{"fromUserId": upstream, "targetUserId": target})
	response, _ := json.Marshal(map[string]string{"model": "fixture-model", "content": content})
	own, _ := json.Marshal(Profile{"userId": upstream, "age": 33})
	profile, _ := json.Marshal(Profile{"userId": target, "age": 23})
	if _, err := f.s.Store.SaveAnalysisCapture(context.Background(), owner, AnalysisCaptureInput{
		UpstreamID: upstream, ProfileID: target, Method: http.MethodPost, Path: "/GPT/checkCompatibility",
		ObservedAtMillis: observed, HTTPStatus: 200, State: "response_received", Generation: "saved-generation",
		RequestBody: request, ResponseBody: response, OwnProfile: own, TargetProfile: profile,
	}); err != nil {
		t.Fatal(err)
	}
}

func openingTestQuoted(t *testing.T, p openingPostPayload) string {
	t.Helper()
	var quoted string
	count := 0
	for _, entity := range p.Entities {
		if entity.Type != "expandable_blockquote" {
			continue
		}
		_, rest := telegramPrefix(p.Text, entity.Offset)
		quoted, _ = telegramPrefix(rest, entity.Length)
		if entity.Offset+entity.Length != textUnits(p.Text) {
			t.Fatal("text remained outside the folded body")
		}
		count++
	}
	if count != 1 {
		t.Fatalf("expected one folded body, got %d", count)
	}
	return quoted
}

func TestOpeningPostsPreviewUsesCurrentRulesWithoutSendingOrMutating(t *testing.T) {
	f := newOpeningPostsFixture(t)
	raw := f.addDraft(t, 10, "Ира😀", "Ира😀, привет! Какая музыка тебе нравится? @fixture_user")
	if err := f.s.Store.User(context.Background(), 456, "foreign"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Store.DB.Exec(`INSERT INTO reaction_message_drafts(owner_id,upstream_id,profile_id,telegram_username,content_hash,basis_hash,state,message,capture_json,capture_sha) SELECT 456,111,99,telegram_username,content_hash,basis_hash,state,'FOREIGN',capture_json,capture_sha FROM reaction_message_drafts LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	cursor := f.scalar(t, "SELECT next_cursor FROM reaction_message_settings WHERE owner_id=123")
	f.step(t)
	f.step(t)
	f.step(t)
	if strings.Join(f.methods, ",") != "createForumTopic,sendMessage,sendMessage" {
		t.Fatal(f.methods)
	}
	last := f.texts[len(f.texts)-1]
	if !strings.Contains(last.Text, "Ира😀, дӕ бон хорз! Какая музыка тебе нравится? @fixture_user") || !strings.Contains(last.Text, "Предпросмотр") || !strings.Contains(last.Text, "strict8") || !strings.Contains(last.Text, "музыку") {
		t.Fatal(last.Text)
	}
	var linked, folded bool
	for _, e := range last.Entities {
		linked = linked || e.Type == "text_link" && e.URL == "https://t.me/c/123/110/110"
		folded = folded || e.Type == "expandable_blockquote"
	}
	if !linked || !folded {
		t.Fatal(last.Entities)
	}
	heading := profileCardHeading(Profile{"userId": 10, "name": "Ира😀", "age": 23, "height": 170, "birthday": "2003-04-10"}, f.now)
	if !strings.HasPrefix(last.Text, heading+"\n\n") || !strings.Contains(openingTestQuoted(t, last), "Правило:") {
		t.Fatal("opening layout differs from the common card or leaks its rule", last.Text)
	}
	if cursor != f.scalar(t, "SELECT next_cursor FROM reaction_message_settings WHERE owner_id=123") || f.scalar(t, "SELECT count(*) FROM actions") != "0" || f.scalar(t, "SELECT count(*) FROM reaction_message_intents") != "0" {
		t.Fatal("preview changed outgoing state")
	}
	var stored []byte
	if err := f.s.Store.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts WHERE owner_id=123").Scan(&stored); err != nil || string(stored) != string(raw) {
		t.Fatal("raw source changed", err)
	}
	if f.scalar(t, "SELECT count(*) FROM opening_audit_posts") != "2" {
		t.Fatal("foreign draft entered audit")
	}
	if worked, err := f.s.deliverOpeningPost(context.Background()); worked || err != nil {
		t.Fatalf("duplicate post: %v %v", worked, err)
	}
}

func TestOpeningPostsFrozenIntentAndLegacyReceipt(t *testing.T) {
	f := newOpeningPostsFixture(t)
	f.addDraft(t, 19587, "Лена", "Лена, привет! Что слушаешь? @fixture_user")
	f.addDraft(t, 10, "Ира", "Ира, привет! Что слушаешь? @fixture_user")
	const frozen = "  Привет 😀 какой концерт запомнился? @old_handle \n"
	req, _ := json.Marshal(map[string]any{"fromUserId": 5696, "userId": 10, "reactionType": 3, "isDeleted": false, "message": frozen})
	r, err := f.s.Store.DB.Exec(`INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(123,10,'Like','succeeded','{"success":true}')`)
	if err != nil {
		t.Fatal(err)
	}
	action, _ := r.LastInsertId()
	if _, err = f.s.Store.DB.Exec(`INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reaction_type) VALUES(?,123,5696,10,'old-generation',?,?,'personal',2,-1,3)`, action, frozen, req); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec(debugAuditSchema); err != nil {
		t.Fatal(err)
	}
	legacy, _ := json.Marshal(map[string]any{"fromUserId": 5696, "userId": 19587, "reactionType": 3, "isDeleted": false, "message": "Лена, привет! Какой концерт? @fixture_user"})
	if _, err = f.s.Store.DB.Exec(`INSERT INTO debug_api_calls(owner_id,request_id,upstream_id,generation,path,target_id,reaction_type,request_body,response_body,http_status,state) VALUES(123,'legacy-opening-001',5696,'old-generation','/reactions/addNew',19587,3,?,'{"success":true}',200,'received')`, legacy); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	f.step(t)
	f.step(t)
	f.step(t)
	if !strings.Contains(f.texts[1].Text, frozen) || !strings.Contains(f.texts[1].Text, "подтверждена API") || strings.Contains(f.texts[1].Text, "дӕ бон хорз") {
		t.Fatal("intent was regenerated", f.texts[1].Text)
	}
	if !strings.Contains(f.texts[2].Text, "прежний debug SuperLike") || !strings.Contains(f.texts[2].Text, "Лена, привет!") {
		t.Fatal(f.texts[2].Text)
	}
	if f.scalar(t, "SELECT count(*) FROM opening_audit_posts WHERE kind IN ('intent','debug')") != "2" {
		t.Fatal("missing receipt posts")
	}
}

func TestOpeningPostsUnknownNewSendAndKnownEditRecovery(t *testing.T) {
	f := newOpeningPostsFixture(t)
	f.step(t)
	f.respond = func(r *http.Request, method string, body map[string]json.RawMessage) (*http.Response, error) {
		return nil, errors.New("transport closed")
	}
	if worked, err := f.s.deliverOpeningPost(context.Background()); !worked || err == nil {
		t.Fatalf("unknown send: %v %v", worked, err)
	}
	if f.scalar(t, "SELECT state FROM opening_audit_posts WHERE item_key='formula'") != "uncertain" {
		t.Fatal("unknown new send not quarantined")
	}
	f.refresh(t)
	if worked, err := f.s.deliverOpeningPost(context.Background()); worked || err != nil {
		t.Fatalf("unknown retried: %v %v", worked, err)
	}
	if _, err := f.s.Store.DB.Exec("UPDATE opening_audit_posts SET state='sending',message_id=55 WHERE item_key='formula'"); err != nil {
		t.Fatal(err)
	}
	if err := f.s.recoverOpeningPosts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.scalar(t, "SELECT state FROM opening_audit_posts WHERE item_key='formula'") != "pending" {
		t.Fatal("known edit did not recover")
	}
	f.respond = func(r *http.Request, method string, body map[string]json.RawMessage) (*http.Response, error) {
		if method != "editMessageText" {
			t.Fatal("known message replaced")
		}
		return albumTestResponse(r, 400, `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`), nil
	}
	f.step(t)
	if f.scalar(t, "SELECT state FROM opening_audit_posts WHERE item_key='formula'") != "sent" {
		t.Fatal("known edit receipt not saved")
	}
}

func TestOpeningPostsGenerationAndSourceGuardsBeforeOutbound(t *testing.T) {
	for _, change := range []string{"generation", "profile", "handle"} {
		t.Run(change, func(t *testing.T) {
			f := newOpeningPostsFixture(t)
			f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
			f.step(t)
			f.step(t)
			expected, err := f.s.Store.Session(context.Background(), 123)
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := f.s.claimOpeningPost(context.Background(), expected)
			if err != nil || !claimed {
				t.Fatal(claimed, err)
			}
			switch change {
			case "generation":
				expected2 := expected
				expected2.Generation = "new-generation"
				if err = f.s.Store.SaveSession(context.Background(), 123, expected2); err != nil {
					t.Fatal(err)
				}
			case "profile":
				if _, err = f.s.Store.Archive(context.Background(), 123, []Profile{{"userId": 10, "name": "Ира", "age": 23, "height": 170, "description": "changed"}}, "changed"); err != nil {
					t.Fatal(err)
				}
			case "handle":
				if _, err = f.s.Store.ConfigureReactionMessagePolicy(context.Background(), 123, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "other_user", PreferDrafts: true, IronMix: true}); err != nil {
					t.Fatal(err)
				}
			}
			if err = f.s.markOpeningPostSending(context.Background(), op, expected); !errors.Is(err, errOpeningPostBinding) {
				t.Fatalf("changed %s allowed: %v", change, err)
			}
			if len(f.methods) != 2 {
				t.Fatal("guard performed outbound")
			}
		})
	}
}

func TestOpeningPostsRefreshEditsKnownPreviewAndKeepsFrozenSources(t *testing.T) {
	f := newOpeningPostsFixture(t)
	f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
	f.step(t)
	f.step(t)
	f.step(t)
	before := f.scalar(t, "SELECT message_id FROM opening_audit_posts WHERE item_key='draft:10'")
	if _, err := f.s.Store.ConfigureReactionMessagePolicy(context.Background(), 123, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "fixture_user", PreferDrafts: true, IronMix: false}); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	f.step(t)
	f.step(t)
	if f.methods[len(f.methods)-1] != "editMessageText" || before != f.scalar(t, "SELECT message_id FROM opening_audit_posts WHERE item_key='draft:10'") {
		t.Fatal("refresh replaced known message")
	}
	if !strings.Contains(f.texts[len(f.texts)-1].Text, "Ира, привет!") {
		t.Fatal("current style not reflected")
	}
}

func TestOpeningPostsUnavailableItemDoesNotBlockLaterRecipients(t *testing.T) {
	for _, invalidBeforePlan := range []bool{false, true} {
		t.Run(fmt.Sprint(invalidBeforePlan), func(t *testing.T) {
			f := newOpeningPostsFixture(t)
			f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
			f.addDraft(t, 11, "Лена", "Лена, привет! Какая музыка? @fixture_user")
			if invalidBeforePlan {
				if _, err := f.s.Store.Archive(context.Background(), 123, []Profile{{"userId": 10, "name": "Ира", "age": "invalid", "height": 170}}, "bad-source"); err != nil {
					t.Fatal(err)
				}
			}
			f.step(t)
			f.step(t)
			if !invalidBeforePlan {
				if _, err := f.s.Store.DB.Exec("UPDATE opening_audit_topics SET next_refresh=9223372036854775807"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.Store.DB.Exec("DELETE FROM mirror_topics WHERE profile_id=10"); err != nil {
					t.Fatal(err)
				}
				if worked, err := f.s.deliverOpeningPost(context.Background()); !worked || !errors.Is(err, errOpeningPostSource) {
					t.Fatalf("lost source: %v %v", worked, err)
				}
				if f.scalar(t, "SELECT state FROM opening_audit_posts WHERE item_key='draft:10'") != "error" {
					t.Fatal("unavailable item kept pending")
				}
			}
			f.step(t)
			if !strings.Contains(f.texts[len(f.texts)-1].Text, "Лена") {
				t.Fatal("valid later recipient starved")
			}
		})
	}
}

func TestOpeningPostsStaleDraftShowsActualFallbackAndRecipe(t *testing.T) {
	f := newOpeningPostsFixture(t)
	original := f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
	changed := Profile{"userId": 10, "name": "Ира", "age": 23, "height": 170, "birthday": "2003-04-10", "description": "новое описание"}
	if _, err := f.s.Store.Archive(context.Background(), 123, []Profile{changed}, "changed"); err != nil {
		t.Fatal(err)
	}
	expected, err := f.s.Store.Session(context.Background(), 123)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.s.Store.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	item, err := f.s.openingPostItemTx(context.Background(), tx, expected, "draft:10")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(item.Payload.Text, "Запасной birthday") || !strings.Contains(item.Payload.Text, "Архив заготовки") || strings.Contains(item.Payload.Text, "Источник: approved") {
		t.Fatal(item.Payload.Text)
	}
	actual, _, err := birthdayReactionMessage(changed, "fixture_user", 0)
	if err != nil {
		t.Fatal(err)
	}
	actual = ironMixReactionMessage(actual, "Ира")
	if !strings.Contains(item.Payload.Text, actual) {
		t.Fatal("preview is not the actual fallback")
	}
	var recipe struct {
		Capture []byte                `json:"draft_capture_bytes"`
		Basis   Profile               `json:"current_basis"`
		Policy  ReactionMessagePolicy `json:"policy"`
	}
	if err = json.Unmarshal(item.Source, &recipe); err != nil || string(recipe.Capture) != string(original) || recipe.Basis.String("description") != "новое описание" || !recipe.Policy.IronMix {
		t.Fatal("incomplete preview recipe", err)
	}
}

func TestOpeningPostsRefreshRepairsThreadURLWithoutReposting(t *testing.T) {
	f := newOpeningPostsFixture(t)
	original := f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
	f.step(t)
	f.step(t)
	f.step(t)
	var messageID int64
	var stored, source []byte
	var sourceSHA string
	if err := f.s.Store.DB.QueryRow("SELECT message_id,payload_json,source_json,source_sha FROM opening_audit_posts WHERE item_key='draft:10'").Scan(&messageID, &stored, &source, &sourceSHA); err != nil {
		t.Fatal(err)
	}
	var old openingPostPayload
	if err := json.Unmarshal(stored, &old); err != nil {
		t.Fatal(err)
	}
	for i := range old.Entities {
		if old.Entities[i].Type == "text_link" {
			old.Entities[i].URL = "https://t.me/c/123/110"
		}
	}
	legacy, err := openingPostBytes(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.Store.DB.Exec("UPDATE opening_audit_posts SET payload_json=?,desired_sha=?,sent_sha=?,state='sent' WHERE item_key='draft:10'", legacy, digest(legacy), digest(legacy)); err != nil {
		t.Fatal(err)
	}
	before := len(f.methods)
	f.refresh(t)
	f.step(t)
	if len(f.methods) != before+1 || f.methods[len(f.methods)-1] != "editMessageText" {
		t.Fatal("URL repair reposted a message", f.methods)
	}
	var afterID int64
	var afterPayload, afterSource, afterCapture []byte
	var afterSourceSHA, sentSHA string
	if err = f.s.Store.DB.QueryRow("SELECT message_id,payload_json,source_json,source_sha,sent_sha FROM opening_audit_posts WHERE item_key='draft:10'").Scan(&afterID, &afterPayload, &afterSource, &afterSourceSHA, &sentSHA); err != nil {
		t.Fatal(err)
	}
	var current openingPostPayload
	if err = json.Unmarshal(afterPayload, &current); err != nil {
		t.Fatal(err)
	}
	linked := false
	for _, entity := range current.Entities {
		linked = linked || entity.Type == "text_link" && entity.URL == "https://t.me/c/123/110/110"
	}
	if !linked || afterID != messageID || current.Text != old.Text || sentSHA != digest(afterPayload) || sentSHA == digest(legacy) {
		t.Fatal("URL repair did not preserve the known post and text")
	}
	if string(afterSource) != string(source) || afterSourceSHA != sourceSHA {
		t.Fatal("URL repair changed source snapshot/hash")
	}
	if err = f.s.Store.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts WHERE owner_id=123").Scan(&afterCapture); err != nil || string(afterCapture) != string(original) {
		t.Fatal("URL repair changed raw draft", err)
	}
}

func TestOpeningPostsScoreEditKeepsMessageAndSource(t *testing.T) {
	f := newOpeningPostsFixture(t)
	raw := f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
	f.step(t)
	f.step(t)
	f.step(t)
	var id int64
	var source []byte
	var sourceSHA string
	if err := f.s.Store.DB.QueryRow("SELECT message_id,source_json,source_sha FROM opening_audit_posts WHERE item_key='draft:10'").Scan(&id, &source, &sourceSHA); err != nil {
		t.Fatal(err)
	}
	f.addScore(t, 123, 5696, 10, 1, "Совместимость: 0%")
	f.refresh(t)
	before := len(f.methods)
	f.step(t)
	last := f.texts[len(f.texts)-1]
	heading := strings.SplitN(last.Text, "\n", 2)[0]
	if !strings.HasSuffix(heading, " · 0%") || len(f.methods) != before+1 || f.methods[len(f.methods)-1] != "editMessageText" {
		t.Fatal("new score did not edit the known opening heading", f.methods, heading)
	}
	if quoted := openingTestQuoted(t, last); !strings.Contains(quoted, "Предпросмотр") || !strings.Contains(quoted, "Правило:") || !strings.Contains(quoted, "Основание:") || !strings.Contains(quoted, "Ира, дӕ бон хорз! Какая музыка?") {
		t.Fatal("folded body lost audit details", quoted)
	}
	linked := false
	for _, entity := range last.Entities {
		linked = linked || entity.Type == "text_link" && entity.Offset == 0 && entity.Length == textUnits(heading) && entity.URL == "https://t.me/c/123/110/110"
	}
	var afterID int64
	var afterSource, afterRaw []byte
	var afterSHA string
	if err := f.s.Store.DB.QueryRow("SELECT message_id,source_json,source_sha FROM opening_audit_posts WHERE item_key='draft:10'").Scan(&afterID, &afterSource, &afterSHA); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Store.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts WHERE owner_id=123").Scan(&afterRaw); err != nil {
		t.Fatal(err)
	}
	if !linked || afterID != id || string(afterSource) != string(source) || afterSHA != sourceSHA || string(afterRaw) != string(raw) || f.scalar(t, "SELECT count(*) FROM actions") != "0" {
		t.Fatal("score/layout edit changed the source, destination or outgoing state")
	}
}

func TestOpeningPostsUnavailableKeepsLastReadableOwnScore(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			f := newOpeningPostsFixture(t)
			f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
			f.addScore(t, 123, 5696, 10, 1, "Совместимость: 75%")
			f.addScore(t, 123, 5696, 10, 2, "Совместимость: 70%. Совместимость: 80%")
			// A different owner cannot supply the public audit's score.
			if err := f.s.Store.User(context.Background(), 456, "foreign"); err != nil {
				t.Fatal(err)
			}
			if err := f.s.Store.SaveSession(context.Background(), 456, Session{UserID: 999, Token: "foreign", Generation: "other"}); err != nil {
				t.Fatal(err)
			}
			f.addScore(t, 456, 999, 10, 3, "Совместимость: 99%")
			if unavailable {
				if _, err := f.s.Store.DB.Exec(profilePresenceSchema); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.Store.DB.Exec("INSERT INTO profile_presence(owner_id,profile_id,availability,changed_at,checked_at) VALUES(123,10,'unavailable','fixture','fixture')"); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := f.s.Store.DB.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			value, known, err := openingProfileScoreTx(context.Background(), tx, 123, 5696, 10)
			if err != nil || known != unavailable || known && value != 75 {
				t.Fatalf("availability=%v score=%d,%v err=%v", unavailable, value, known, err)
			}
		})
	}
}

func TestOpeningPostsOversizeRecipientDoesNotBlockLaterRecipients(t *testing.T) {
	f := newOpeningPostsFixture(t)
	original := f.addDraft(t, 10, strings.Repeat("И", 4200), "Привет! Какая музыка? @fixture_user")
	f.addDraft(t, 11, "Лена", "Лена, привет! Какая музыка? @fixture_user")
	f.step(t)
	f.step(t)
	f.step(t)
	last := f.texts[len(f.texts)-1]
	if !strings.Contains(last.Text, "Лена") || f.scalar(t, "SELECT count(*) FROM opening_audit_posts WHERE item_key='draft:10'") != "0" {
		t.Fatal("oversize heading starved the next recipient")
	}
	var raw []byte
	if err := f.s.Store.DB.QueryRow("SELECT capture_json FROM reaction_message_drafts WHERE owner_id=123 AND profile_id=10").Scan(&raw); err != nil || string(raw) != string(original) {
		t.Fatal("oversize presentation changed its archived source", err)
	}
}
