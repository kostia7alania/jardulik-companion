package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func keyboardFixture(t *testing.T) (*Service, *[]uxMessage, StoredProfile, keyboardSessionPin) {
	t.Helper()
	s, sent := uxFixture(t)
	uxProfiles(t, s, 1, 1)
	ctx := context.Background()
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "test-token", UserID: 101, Generation: "keyboard-session", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.Profile(ctx, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := s.keyboardSessionPin(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.setActiveCard(ctx, 1, p, pin, []int64{100, 101, 102}); err != nil {
		t.Fatal(err)
	}
	return s, sent, p, pin
}

func keyboardMessage(owner, id int64, text string) Message {
	return Message{ID: id, From: TGUser{ID: owner}, Chat: TGChat{ID: owner, Type: "private"}, Text: text}
}

func TestBotKeyboardMarkupStaysAvailableWithAlbums(t *testing.T) {
	b, err := json.Marshal(keyboardReplyMarkup())
	if err != nil {
		t.Fatal(err)
	}
	var markup struct {
		Keyboard   [][]struct{ Text string } `json:"keyboard"`
		Persistent bool                      `json:"is_persistent"`
		Resize     bool                      `json:"resize_keyboard"`
		Once       bool                      `json:"one_time_keyboard"`
	}
	if err := json.Unmarshal(b, &markup); err != nil {
		t.Fatal(err)
	}
	if !markup.Persistent || !markup.Resize || markup.Once || len(markup.Keyboard) != 2 || len(markup.Keyboard[0]) != 2 || len(markup.Keyboard[1]) != 2 {
		t.Fatalf("unexpected reply keyboard: %s", b)
	}
	want := []string{keyboardLike, keyboardNext, keyboardSave, keyboardMenu}
	for i, label := range want {
		if markup.Keyboard[i/2][i%2].Text != label {
			t.Fatalf("missing action %s", label)
		}
	}
}

func TestBotKeyboardActiveCardRejectsOldTapForeignOwnerAndChangedSession(t *testing.T) {
	s, _, p, pin := keyboardFixture(t)
	ctx := context.Background()
	if got, err := s.activeKeyboardCard(ctx, 1, 103); err != nil || got.VersionID != p.VersionID {
		t.Fatalf("confirmed card unavailable: %v", err)
	}
	for _, id := range []int64{1, 100, 101, 102} {
		if _, err := s.activeKeyboardCard(ctx, 1, id); err == nil {
			t.Fatalf("queued tap %d accepted for newer album", id)
		}
	}
	if _, err := s.activeKeyboardCard(ctx, 2, 103); err == nil {
		t.Fatal("foreign owner obtained card")
	}
	if err := s.setActiveCard(ctx, 2, p, pin, []int64{103}); err == nil {
		t.Fatal("foreign version became active")
	}
	if err := s.Store.SaveSession(ctx, 1, Session{Token: "new-token", UserID: 202, Generation: "new-login"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.activeKeyboardCard(ctx, 1, 104); !errors.Is(err, ErrReactionSessionChanged) {
		t.Fatalf("old session card survived reconnect: %v", err)
	}
	if err := s.setActiveCard(ctx, 1, p, pin, []int64{104}); !errors.Is(err, ErrReactionSessionChanged) {
		t.Fatalf("delivery from old session became active: %v", err)
	}
}

func TestBotKeyboardUnknownDeliveryClearsTargetAndUnconfirmedIDsStayInactive(t *testing.T) {
	s, _, p, pin := keyboardFixture(t)
	ctx := context.Background()
	if err := s.clearActiveCard(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int64{nil, {0}, {104, 0}} {
		if err := s.setActiveCard(ctx, 1, p, pin, ids); err == nil {
			t.Fatal("unconfirmed send activated a profile")
		}
	}
	if _, err := s.activeKeyboardCard(ctx, 1, 105); err == nil {
		t.Fatal("old card remained active after unknown send")
	}
}

func TestBotKeyboardSaveMenuAndDisabledLikeDoNotContactSource(t *testing.T) {
	s, sent, p, _ := keyboardFixture(t)
	ctx := context.Background()
	for _, text := range []string{keyboardSave, keyboardMenu, keyboardLike} {
		if !s.handleReplyKeyboardLocked(ctx, 1, keyboardMessage(1, 103, text)) {
			t.Fatalf("button not handled: %s", text)
		}
	}
	var favorites int
	if err := s.Store.DB.QueryRow(`SELECT COUNT(*) FROM favorites WHERE owner_id=1 AND profile_id=1`).Scan(&favorites); err != nil || favorites != 1 {
		t.Fatalf("save failed: count=%d err=%v", favorites, err)
	}
	if len(*sent) != 3 || !strings.Contains((*sent)[2].Text, "выключены") {
		t.Fatalf("disabled like advanced or failed to explain: %v", *sent)
	}
	menu := (*sent)[1]
	var actions []string
	for _, row := range menu.Markup.Rows {
		for _, button := range row {
			actions = append(actions, button.Data)
		}
	}
	for _, data := range []string{"p:1", fmt.Sprintf("tv:%d", p.VersionID), fmt.Sprintf("jv:%d", p.VersionID), "favorites", "likes", "matches", "history", "connect_help", "settings", "next"} {
		if !strings.Contains("|"+strings.Join(actions, "|")+"|", "|"+data+"|") {
			t.Fatalf("missing menu route %s in %v", data, actions)
		}
	}
	if got, err := s.activeKeyboardCard(ctx, 1, 104); err != nil || got.ID != p.ID {
		t.Fatal("disabled reaction changed active card")
	}
}

func TestBotKeyboardIgnoresForeignChatAndStaleActions(t *testing.T) {
	s, sent, _, _ := keyboardFixture(t)
	ctx := context.Background()
	for _, text := range []string{keyboardLike, keyboardSave, keyboardNext, keyboardMenu} {
		m := keyboardMessage(2, 103, text)
		if s.handleReplyKeyboardLocked(ctx, 1, m) {
			t.Fatal("foreign message accepted")
		}
		m = keyboardMessage(1, 103, text)
		m.Chat.Type = "group"
		if s.handleReplyKeyboardLocked(ctx, 1, m) {
			t.Fatal("group message accepted")
		}
	}
	if len(*sent) != 0 {
		t.Fatal("foreign requests produced messages")
	}
	for _, text := range []string{keyboardLike, keyboardSave} {
		s.handleReplyKeyboardLocked(ctx, 1, keyboardMessage(1, 101, text))
	}
	var count int
	if err := s.Store.DB.QueryRow(`SELECT COUNT(*) FROM favorites`).Scan(&count); err != nil || count != 0 {
		t.Fatal("old queued action saved newer card")
	}
	if s.handleReplyKeyboardLocked(ctx, 1, keyboardMessage(1, 103, "ordinary filter input")) {
		t.Fatal("keyboard consumed unrelated text")
	}
}

func TestBotKeyboardInstallationOnceIncludingUnknownSend(t *testing.T) {
	for _, outcome := range []string{"sent", "lost", "wrong-chat"} {
		t.Run(outcome, func(t *testing.T) {
			s, _, _, _ := keyboardFixture(t)
			calls := 0
			s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !strings.Contains(string(body["reply_markup"]), `"is_persistent":true`) {
					t.Fatal("installation omitted persistent keyboard")
				}
				if outcome == "lost" {
					return nil, errors.New("mock lost response")
				}
				chat := 1
				if outcome == "wrong-chat" {
					chat = 2
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"result":{"message_id":110,"chat":{"id":%d}}}`, chat)))}, nil
			})
			ctx := context.Background()
			err := s.installKeyboardOnceLocked(ctx, 1)
			if (outcome == "sent") != (err == nil) {
				t.Fatalf("delivery outcome %s: %v", outcome, err)
			}
			if err := s.installKeyboardOnceLocked(ctx, 1); err != nil {
				t.Fatal(err)
			}
			if err := s.bootstrapKeyboards(ctx); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("installation blindly retried %d times", calls)
			}
			var state string
			if err := s.Store.DB.QueryRow(`SELECT installation_state FROM bot_keyboard_state WHERE owner_id=1`).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if outcome == "sent" && state != "sent" || outcome != "sent" && state != "unknown" {
				t.Fatalf("unexpected receipt %q", state)
			}
		})
	}
}

func TestBotKeyboardBootstrapDoesNotDeadlockSingleConnectionAndDeleteCascades(t *testing.T) {
	s, sent, _, _ := keyboardFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.bootstrapKeyboards(ctx); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 || (*sent)[0].ChatID != 1 {
		t.Fatal("bootstrap did not target only connected owner")
	}
	if err := s.Store.DeleteUser(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.Store.DB.QueryRow(`SELECT COUNT(*) FROM bot_keyboard_state`).Scan(&count); err != nil || count != 0 {
		t.Fatal("deleted user's keyboard state survived")
	}
}

func TestBotKeyboardOnlyConfirmedNewLikeAdvances(t *testing.T) {
	for _, tc := range []struct {
		name, account, reaction string
		advance                 bool
	}{
		{"success", `{"userId":101,"freezeState":false}`, `{"IsSuccess":true}`, true},
		{"mutual", `{"userId":101,"freezeState":false}`, `{"IsSuccess":true,"IsMutual":true}`, true},
		{"unknown", `{"userId":101,"freezeState":false}`, "transport-error", false},
		{"quota", `{"userId":101,"freezeState":false}`, `{"IsSuccess":false,"MessageKind":-1,"HoursLeft":1}`, false},
		{"frozen", `{"userId":101,"freezeState":true}`, `{"IsSuccess":true}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := manualLikeFixture(t, tc.account, tc.reaction)
			ctx := context.Background()
			p, err := s.Store.Profile(ctx, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			pin, err := s.keyboardSessionPin(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.setActiveCard(ctx, 1, p, pin, []int64{100}); err != nil {
				t.Fatal(err)
			}
			s.Store.Delivered(ctx, 1, p.ID)
			original := s.Client.HTTP.Transport
			feeds, posts := 0, 0
			s.Client.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/user/getUsers" {
					feeds++
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[]`))}, nil
				}
				if r.Method == http.MethodPost {
					posts++
				}
				return original.RoundTrip(r)
			})
			if !s.handleReplyKeyboardLocked(ctx, 1, keyboardMessage(1, 101, keyboardLike)) {
				t.Fatal("like not handled")
			}
			if (feeds == 1) != tc.advance || feeds > 1 {
				t.Fatalf("wrong automatic next: feeds=%d advance=%t", feeds, tc.advance)
			}
			if tc.name == "success" || tc.name == "mutual" || tc.name == "unknown" {
				s.handleReplyKeyboardLocked(ctx, 1, keyboardMessage(1, 110, keyboardLike))
				if posts != 1 {
					t.Fatalf("repeated tap caused another reaction: %d", posts)
				}
			}
		})
	}
}

func TestBotKeyboardReconnectAfterSelectionCannotLikeFromNewAccount(t *testing.T) {
	s, sent, _, _ := keyboardFixture(t)
	s.Config.AllowReactions, s.Store.cfg.AllowReactions = true, true
	ctx := context.Background()
	changed := false
	// LikeGuarded reads its clock after the keyboard selected its target, but
	// before capturing the initial reaction session. Simulate an operator's
	// reconnect in this exact gap, where its own session guard cannot help.
	s.Client.Now = func() time.Time {
		if !changed {
			changed = true
			if err := s.Store.SaveSession(ctx, 1, Session{Token: "new-account", UserID: 202, Generation: "new-login", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
				t.Fatal(err)
			}
		}
		return time.Now()
	}
	m := keyboardMessage(1, 103, keyboardLike)
	s.Handle(ctx, Update{Message: &m})
	if !changed || len(*sent) != 1 || !strings.Contains((*sent)[0].Text, "Сессия Jardulik изменилась") {
		t.Fatalf("reconnect was not rejected: changed=%t messages=%v", changed, *sent)
	}
	var count int
	if err := s.Store.DB.QueryRow(`SELECT COUNT(*) FROM actions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("reconnect claimed a reaction: count=%d err=%v", count, err)
	}
}

func TestBotKeyboardAgeIndexIsOwnerOnly(t *testing.T) {
	s, sent, _, _ := keyboardFixture(t)
	s.Config.OwnerID = 1
	for _, owner := range []int64{1, 2} {
		m := keyboardMessage(owner, 103, keyboardMenu)
		s.Handle(context.Background(), Update{Message: &m})
		found := false
		for _, row := range (*sent)[len(*sent)-1].Markup.Rows {
			for _, button := range row {
				if button.Data == "ai:0" {
					found = true
					if button.Text != "📅 По возрасту" {
						t.Fatal("unexpected age index label")
					}
				}
			}
		}
		if found != (owner == s.Config.OwnerID) {
			t.Fatalf("age index visibility leaked: owner=%d found=%t", owner, found)
		}
	}
}

func TestBotKeyboardActualDeliveryClearsActiveCardOnUnknownResult(t *testing.T) {
	s, _, p, _ := keyboardFixture(t)
	ctx := context.Background()
	s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("mock delivery response lost")
	})
	if err := s.Deliver(ctx, 1, p); err == nil {
		t.Fatal("unknown delivery reported success")
	}
	if _, err := s.activeKeyboardCard(ctx, 1, 104); err == nil {
		t.Fatal("unknown delivery left the old generic-like target active")
	}
	var state string
	if err := s.Store.DB.QueryRow(`SELECT state FROM deliveries WHERE owner_id=1 ORDER BY id DESC LIMIT 1`).Scan(&state); err != nil || state != "unknown" {
		t.Fatalf("unknown delivery receipt changed: state=%s err=%v", state, err)
	}
}
