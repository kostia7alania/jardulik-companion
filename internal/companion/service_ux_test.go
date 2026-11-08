package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type uxMessage struct {
	ChatID int64  `json:"chat_id"`
	Text   string `json:"text"`
	Markup struct {
		Rows [][]struct {
			Text string `json:"text"`
			Data string `json:"callback_data"`
		} `json:"inline_keyboard"`
	} `json:"reply_markup"`
}

func uxFixture(t *testing.T) (*Service, *[]uxMessage) {
	t.Helper()
	store, c := testStore(t)
	c.OwnerID = 99
	for _, owner := range []int64{1, 2} {
		if err := store.User(context.Background(), owner, ""); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{Config: c, Store: store, TG: NewTelegram(c), Client: NewClient(store, c), Media: NewMediaManager(store, c)}
	var sent []uxMessage
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := filepath.Base(r.URL.Path)
		if method == "answerCallbackQuery" {
			_, _ = io.Copy(io.Discard, r.Body)
			fmt.Fprint(w, `{"ok":true,"result":true}`)
			return
		}
		if method != "sendMessage" {
			t.Fatalf("text-only UX unexpectedly sent %s", method)
		}
		var message uxMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatal(err)
		}
		sent = append(sent, message)
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":%d}}}`, 100+len(sent), message.ChatID)
	})}
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("read-only navigation made upstream request: %s %s", r.Method, r.URL.Path)
	})}
	return s, &sent
}

func uxSend(s *Service, owner int64, text string) {
	s.Handle(context.Background(), Update{Message: &Message{ID: 1, From: TGUser{ID: owner}, Chat: TGChat{ID: owner, Type: "private"}, Text: text}})
}
func uxTap(s *Service, owner int64, data string) {
	s.Handle(context.Background(), Update{Callback: &Callback{ID: "tap", From: TGUser{ID: owner}, Message: Message{Chat: TGChat{ID: owner, Type: "private"}}, Data: data}})
}
func uxProfiles(t *testing.T, s *Service, owner int64, count int) {
	t.Helper()
	var profiles []Profile
	for id := 1; id <= count; id++ {
		profiles = append(profiles, Profile{"userId": id, "name": fmt.Sprintf("Имя%d", id), "age": 23, "height": 171})
	}
	if _, err := s.Store.Archive(context.Background(), owner, profiles, "ux-test"); err != nil {
		t.Fatal(err)
	}
}

func TestUXHistoryAliasesAndPrivateOwnership(t *testing.T) {
	s, sent := uxFixture(t)
	uxProfiles(t, s, 1, 1)
	for _, cmd := range []string{"/history_1", "/history_1@our_bot", "/history 1"} {
		uxSend(s, 1, cmd)
		if !strings.Contains((*sent)[len(*sent)-1].Text, "Анкета #1:") {
			t.Fatalf("history command failed: %s", cmd)
		}
	}
	for _, cmd := range []string{"/history_0", "/history_-1", "/history_foo", "/history_9223372036854775808"} {
		before := len(*sent)
		uxSend(s, 1, cmd)
		if len(*sent) != before {
			t.Fatalf("malformed ID produced data: %s", cmd)
		}
	}
	uxSend(s, 2, "/history_1")
	if strings.Contains((*sent)[len(*sent)-1].Text, "Имя1") {
		t.Fatal("history alias leaked another tenant")
	}
	for _, data := range []string{"p:1", "f:1", "h:1", "t:1"} {
		before := len(*sent)
		uxTap(s, 2, data)
		for _, message := range (*sent)[before:] {
			if strings.Contains(message.Text, "Имя1") {
				t.Fatalf("callback %s leaked tenant data", data)
			}
		}
	}
	before := len(*sent)
	s.Handle(context.Background(), Update{Callback: &Callback{From: TGUser{ID: 2}, Message: Message{Chat: TGChat{ID: 1, Type: "private"}}, Data: "h:1"}})
	if len(*sent) != before {
		t.Fatal("foreign-chat callback was accepted")
	}
	var actions int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("navigation created a reaction action")
	}
}

func TestUXListsUseTextButtonsAndTenantScopedCursor(t *testing.T) {
	s, sent := uxFixture(t)
	uxProfiles(t, s, 1, 21)
	uxProfiles(t, s, 2, 3)
	uxSend(s, 1, "/history")
	first := (*sent)[len(*sent)-1]
	if first.Text != "История анкет\nВыбери анкету:" || len(first.Markup.Rows) != 11 || first.Markup.Rows[0][0].Data != "p:21" || first.Markup.Rows[0][1].Data != "h:21" || !strings.Contains(first.Markup.Rows[0][0].Text, "23/171") {
		t.Fatalf("unexpected first page: %+v", first)
	}
	next := first.Markup.Rows[10][0].Data
	uxTap(s, 1, next)
	second := (*sent)[len(*sent)-1]
	if len(second.Markup.Rows) != 11 || second.Markup.Rows[0][0].Data != "p:11" || second.Markup.Rows[10][0].Data != "lh:0" {
		t.Fatalf("second page wrong: %+v", second)
	}
	uxTap(s, 1, second.Markup.Rows[10][1].Data)
	third := (*sent)[len(*sent)-1]
	if len(third.Markup.Rows) != 2 || third.Markup.Rows[0][0].Data != "p:1" || len(third.Markup.Rows[1]) != 1 {
		t.Fatalf("last page wrong: %+v", third)
	}
	for id := 1; id <= 21; id++ {
		if err := s.Store.Favorite(context.Background(), 1, int64(id)); err != nil {
			t.Fatal(err)
		}
	}
	uxSend(s, 1, "/favorites")
	favorites := (*sent)[len(*sent)-1]
	if !strings.HasPrefix(favorites.Markup.Rows[10][0].Data, "lf:") {
		t.Fatal("favorites lost independent pagination")
	}
	// A cursor is only a boundary, never an authorization or a tenant identifier.
	page, err := s.Store.List(context.Background(), 2, false, 999999)
	if err != nil || len(page) != 3 {
		t.Fatalf("tenant cursor query failed: %d %v", len(page), err)
	}
	if _, err = s.Store.List(context.Background(), 1, false, -1); err == nil {
		t.Fatal("negative cursor accepted")
	}
	for _, cursor := range []string{"lh:-1", "lh:no", "lf:9223372036854775808"} {
		before := len(*sent)
		uxTap(s, 1, cursor)
		if len(*sent) != before {
			t.Fatal("malformed cursor produced a page")
		}
	}
	// Clicking a person, unlike listing, intentionally delivers their card.
	uxTap(s, 1, "p:1")
	if !strings.Contains((*sent)[len(*sent)-1].Text, "Имя1") {
		t.Fatal("person button did not open a card")
	}
}

func TestUXCursorDoesNotRepeatWhenNewProfilesArrive(t *testing.T) {
	s, _ := uxFixture(t)
	uxProfiles(t, s, 1, 11)
	first, err := s.Store.List(context.Background(), 1, false, 0)
	if err != nil || len(first) != 11 {
		t.Fatal(err)
	}
	if _, err = s.Store.Archive(context.Background(), 1, []Profile{{"userId": 999, "name": "Новая"}}, "new"); err != nil {
		t.Fatal(err)
	}
	second, err := s.Store.List(context.Background(), 1, false, first[9].Cursor)
	if err != nil || len(second) != 1 || second[0].ID != 1 {
		t.Fatalf("cursor repeated or skipped old page: %+v %v", second, err)
	}
}

func TestUXCursorDoesNotRepeatWhenCurrentVersionReverts(t *testing.T) {
	s, sent := uxFixture(t)
	uxProfiles(t, s, 1, 11)
	original := Profile{"userId": 1, "name": "Имя1", "age": 23, "height": 171}
	changed := Profile{"userId": 1, "name": "Изменено", "age": 23, "height": 171}
	if _, err := s.Store.Archive(context.Background(), 1, []Profile{changed}, "change"); err != nil {
		t.Fatal(err)
	}
	uxSend(s, 1, "/history")
	first := (*sent)[len(*sent)-1]
	seen := map[string]bool{}
	for _, row := range first.Markup.Rows[:10] {
		seen[row[0].Data] = true
	}
	if _, err := s.Store.Archive(context.Background(), 1, []Profile{original}, "revert"); err != nil {
		t.Fatal(err)
	}
	uxTap(s, 1, first.Markup.Rows[10][0].Data)
	next := (*sent)[len(*sent)-1]
	for _, row := range next.Markup.Rows {
		if !strings.HasPrefix(row[0].Data, "p:") {
			continue
		}
		if seen[row[0].Data] {
			t.Fatalf("reverting version repeated profile %s across pages", row[0].Data)
		}
		seen[row[0].Data] = true
	}
	if len(seen) != 11 {
		t.Fatalf("paging lost profiles after version reverted: %v", seen)
	}
}

func TestUXStatusShowsOnlyFreshPresentBalances(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		failed, balances bool
	}{
		{"balances", `{"userId":101,"fire":3,"clean":0}`, false, true},
		{"missing", `{"userId":101}`, false, false},
		{"invalid", `{"userId":101,"fire":null,"clean":false}`, false, false},
		{"stale", `{"userId":101,"fire":9,"clean":8}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sent := uxFixture(t)
			if err := s.Store.SaveSession(context.Background(), 1, Session{Token: "test-access", Refresh: "test-refresh", UserID: 101, Expires: time.Now().Add(time.Hour).Unix(), Account: json.RawMessage(tc.body)}); err != nil {
				t.Fatal(err)
			}
			s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || !strings.HasSuffix(r.URL.Path, "/user/getMe") {
					t.Fatalf("status made action request: %s %s", r.Method, r.URL.Path)
				}
				if tc.failed {
					w.WriteHeader(500)
					fmt.Fprint(w, `{}`)
				} else {
					fmt.Fprint(w, tc.body)
				}
			})}
			uxSend(s, 1, "/status")
			text := (*sent)[len(*sent)-1].Text
			if strings.Contains(text, "Огонь") != tc.balances || strings.Contains(text, "Чистый лист") != tc.balances {
				t.Fatalf("incorrect fresh balances: %s", text)
			}
			if tc.balances && (!strings.Contains(text, "🔥 Огонь: 3") || !strings.Contains(text, "Чистый лист: 0")) {
				t.Fatal("present zero balance was lost")
			}
			if !strings.Contains(text, "Отправлено лайков ботом: 0") || !strings.Contains(text, "Автолайки: выключены") || strings.Contains(text, "false") || strings.Contains(text, "суточ") {
				t.Fatalf("misleading action status: %s", text)
			}
		})
	}
}

func TestUXSupportAcknowledgesOnlyConfirmedOperatorDelivery(t *testing.T) {
	for _, mode := range []string{"sent", "unknown", "missing-id", "wrong-chat"} {
		t.Run(mode, func(t *testing.T) {
			s, sent := uxFixture(t)
			operatorCalls := 0
			s.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				var message uxMessage
				if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
					t.Fatal(err)
				}
				body := fmt.Sprintf(`{"ok":true,"result":{"message_id":1,"chat":{"id":%d}}}`, message.ChatID)
				if message.ChatID == s.Config.OwnerID {
					operatorCalls++
					switch mode {
					case "unknown":
						return nil, errors.New("response lost")
					case "missing-id":
						body = `{"ok":true,"result":{"chat":{"id":99}}}`
					case "wrong-chat":
						body = `{"ok":true,"result":{"message_id":1,"chat":{"id":3}}}`
					}
				}
				if message.ChatID == 1 {
					*sent = append(*sent, message)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			uxSend(s, 1, "/support вопрос")
			if operatorCalls != 1 || len(*sent) != 1 {
				t.Fatal("support retried or omitted acknowledgment")
			}
			if strings.Contains((*sent)[0].Text, "передано") != (mode == "sent") {
				t.Fatalf("false delivery confirmation: %s", (*sent)[0].Text)
			}
		})
	}
}

func TestUXWelcomeHelpAndConnectStayShortAndActionable(t *testing.T) {
	s, sent := uxFixture(t)
	for _, cmd := range []string{"/start", "/help", "/connect"} {
		uxSend(s, 1, cmd)
		message := (*sent)[len(*sent)-1]
		if len(message.Markup.Rows) == 0 || strings.Contains(message.Text, "Не присылай пароль") {
			t.Fatalf("onboarding missing buttons or repeats boilerplate: %s", cmd)
		}
	}
	if !strings.Contains(welcomeText, "Оператор имеет доступ") || !strings.Contains(welcomeText, "privacy.html") {
		t.Fatal("privacy disclosure removed")
	}
	for _, step := range []string{"1.", "2.", "3.", "Telegram Web", "#tgWebAppData="} {
		if !strings.Contains(connectHelp, step) {
			t.Fatalf("missing applicable connection instruction: %s", step)
		}
	}
	for _, data := range []string{"history", "favorites", "help", "connect_help"} {
		before := len(*sent)
		uxTap(s, 1, data)
		if len(*sent) != before+1 {
			t.Fatalf("menu callback failed: %s", data)
		}
	}
	uxSend(s, 1, "/autolikes 1")
	var actions int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("disabled autolikes sent an action")
	}
	s.Config.AllowReactions = true
	uxSend(s, 1, "/autolikes 0")
	if !strings.Contains((*sent)[len(*sent)-1].Text, "обычных лайков") || !strings.Contains((*sent)[len(*sent)-1].Text, strconv.Itoa(50)) {
		t.Fatal("series guard not explained")
	}
}
