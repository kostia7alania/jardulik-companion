package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

type settingsUXRequest struct {
	uxMessage
	MessageID int64 `json:"message_id"`
	Method    string
}

func settingsUXFixture(t *testing.T) (*autoFixture, *[]settingsUXRequest) {
	t.Helper()
	f := newAutoFixture(t)
	var requests []settingsUXRequest
	f.worker.Service.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request settingsUXRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		request.Method = filepath.Base(r.URL.Path)
		if request.Method != "sendMessage" && request.Method != "editMessageText" {
			t.Fatalf("unexpected menu request: %s", request.Method)
		}
		requests = append(requests, request)
		id := request.MessageID
		if id == 0 {
			id = 100
		}
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":%d}}}`, id, request.ChatID)
	})}
	return f, &requests
}

func settingsUXAction(t *testing.T, request settingsUXRequest, action string) string {
	t.Helper()
	for _, row := range request.Markup.Rows {
		for _, button := range row {
			if strings.HasSuffix(button.Data, ":"+action) {
				return button.Data
			}
		}
	}
	t.Fatalf("action %q not found in menu: %+v", action, request.Markup.Rows)
	return ""
}

func settingsUXTap(t *testing.T, s *Service, owner, messageID int64, data string) {
	t.Helper()
	if handled, err := s.HandleSettingsMessageCallback(context.Background(), owner, messageID, data); !handled || err != nil {
		t.Fatalf("tap %q: handled=%t err=%v", data, handled, err)
	}
}

func TestSettingsUXEditsOneMenuAndMarksSelectedValues(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if err := s.OpenSettings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	settingsUXAction(t, (*requests)[0], "automatic")
	settingsUXAction(t, (*requests)[0], "series")
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[0], "view:interval"))
	if !strings.Contains((*requests)[1].Text, "Интервал между лайками") {
		t.Fatal("interval controls lack their context")
	}
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[1], "interval:60"))
	selected := false
	for _, row := range (*requests)[2].Markup.Rows {
		for _, button := range row {
			selected = selected || button.Text == "✅ 60 с"
		}
	}
	if !selected {
		t.Fatal("chosen interval is not marked")
	}
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[2], "home"))
	for i, request := range *requests {
		want := "editMessageText"
		if i == 0 {
			want = "sendMessage"
		}
		if request.Method != want || (i > 0 && request.MessageID != 100) {
			t.Fatalf("navigation sent a new menu: %+v", request)
		}
	}
	j, err := f.worker.settings(ctx, 1)
	if err != nil || j.Mode != "off" || j.Interval != 60 || len(f.requests) != 0 {
		t.Fatalf("menu changed run or contacted source: %+v err=%v requests=%v", j, err, f.requests)
	}
}

func TestSettingsUXStaleStartRefreshesWithoutRunningAndPreservesCustomValues(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	j, _ := f.worker.settings(ctx, 1)
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "count", 7); err != nil {
		t.Fatal(err)
	}
	j, _ = f.worker.settings(ctx, 1)
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "cap", 137); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenSettings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	oldStart := settingsUXAction(t, (*requests)[0], "automatic")
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[0], "view:interval"))
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[1], "interval:60"))
	settingsUXTap(t, s, 1, 100, oldStart)
	j, _ = f.worker.settings(ctx, 1)
	if j.Mode != "off" || j.DailyCap != 137 || j.Count != 7 || !strings.Contains((*requests)[3].Text, "Для запуска нажми кнопку ещё раз") {
		t.Fatalf("stale start mutated settings: %+v, %s", j, (*requests)[3].Text)
	}
	settingsUXAction(t, (*requests)[3], "series")
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[3], "view:cap"))
	marked := false
	for _, row := range (*requests)[4].Markup.Rows {
		for _, button := range row {
			marked = marked || button.Text == "✅ 137 /24ч"
		}
	}
	if !marked {
		t.Fatal("custom cap disappeared from choices")
	}
	settingsUXTap(t, s, 1, 100, settingsUXAction(t, (*requests)[4], "home"))
	start := settingsUXAction(t, (*requests)[5], "automatic")
	settingsUXTap(t, s, 1, 100, start)
	active, _ := f.worker.settings(ctx, 1)
	settingsUXTap(t, s, 1, 100, start)
	after, _ := f.worker.settings(ctx, 1)
	if active.Mode != "automatic" || active.RunID != after.RunID || after.DailyCap != 137 || after.Count != 7 || after.Interval != 60 || len(f.requests) != 0 {
		t.Fatalf("start or repeated callback corrupted run: before=%+v after=%+v upstream=%v", active, after, f.requests)
	}
	for _, row := range (*requests)[6].Markup.Rows {
		for _, button := range row {
			if strings.HasSuffix(button.Data, ":automatic") || strings.HasSuffix(button.Data, ":series") {
				t.Fatal("active menu offers another start")
			}
		}
	}
}

func TestSettingsUXStaleSettingsAreReadOnlyButStopAlwaysWorks(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if err := f.worker.Start(ctx, 1, "automatic", 10); err != nil {
		t.Fatal(err)
	}
	j, _ := f.worker.settings(ctx, 1)
	old := j.Nonce
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "interval", 60); err != nil {
		t.Fatal(err)
	}
	settingsUXTap(t, s, 1, 100, "as:"+old+":cap:10")
	current, _ := f.worker.settings(ctx, 1)
	if current.DailyCap != j.DailyCap || current.Mode != "automatic" {
		t.Fatal("stale setting was applied")
	}
	settingsUXTap(t, s, 1, 100, "as:"+old+":off")
	stopped, _ := f.worker.settings(ctx, 1)
	if stopped.Mode != "off" || len(f.requests) != 0 || len(*requests) != 2 {
		t.Fatalf("stale Stop failed: %+v upstream=%v menus=%d", stopped, f.requests, len(*requests))
	}
	settingsUXTap(t, s, 2, 200, "as:"+stopped.Nonce+":automatic")
	other, _ := f.worker.settings(ctx, 2)
	if other.Mode != "off" || (*requests)[2].ChatID != 2 {
		t.Fatal("foreign nonce changed another owner's run")
	}
}

func TestSettingsUXEditFailureDoesNotDuplicateMenu(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		err  error
		want bool
	}{
		{name: "unchanged", body: `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`},
		{name: "rejected", body: `{"ok":false,"error_code":400,"description":"Bad Request: message to edit not found"}`, want: true},
		{name: "transport", err: errors.New("response lost"), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := settingsUXFixture(t)
			calls := 0
			f.worker.Service.TG.HTTP.Transport = archiveRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if filepath.Base(r.URL.Path) != "editMessageText" {
					t.Fatalf("edit failure sent another message: %s", r.URL.Path)
				}
				if tc.err != nil {
					return nil, tc.err
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})
			err := f.worker.Service.OpenSettingsMessage(context.Background(), 1, 100)
			if (err != nil) != tc.want || calls != 1 {
				t.Fatalf("edit: err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestSettingsUXBlockedSourceAndUserPauseHideStarts(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason, want string
		pause, connect            bool
	}{
		{name: "challenge", state: "paused", reason: "unexpected_response_or_challenge", want: "после необычного ответа", connect: true},
		{name: "expired", state: "reconnect_required", want: "Подключи аккаунт заново", connect: true},
		{name: "user pause", state: "connected", want: "Действия приостановлены", pause: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, requests := settingsUXFixture(t)
			s := f.worker.Service
			ctx := context.Background()
			s.Store.SessionState(ctx, 1, tc.state, tc.reason)
			if tc.pause {
				s.Store.Pause(ctx, 1, true)
			}
			if err := s.OpenSettingsMessage(ctx, 1, 100); err != nil {
				t.Fatal(err)
			}
			menu := (*requests)[0]
			if !strings.Contains(menu.Text, tc.want) {
				t.Fatalf("blocked source lacks clear explanation: %s", menu.Text)
			}
			connect := false
			for _, row := range menu.Markup.Rows {
				for _, button := range row {
					connect = connect || button.Data == "connect_help"
					if strings.HasSuffix(button.Data, ":automatic") || strings.HasSuffix(button.Data, ":series") {
						t.Fatal("blocked account offered Start")
					}
				}
			}
			if connect != tc.connect || len(f.requests) != 0 {
				t.Fatalf("wrong connect action or source requests: connect=%t source=%v", connect, f.requests)
			}
		})
	}
}

func TestSettingsUXShowsAppliedFiltersAndSelectedTapDoesNotRotateNonce(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	filters := DefaultFeedFilters()
	filters.AgeMin, filters.AgeMax, filters.HeightMin, filters.HeightMax = 18, 100, 150, 190
	filters.Country, filters.City = "Russia", "Vladikavkaz"
	raw, _ := json.Marshal(filters)
	if _, err := s.Store.DB.ExecContext(ctx, "UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SaveAccount(ctx, 1, Profile{"ageMin": 18, "ageMax": 25}); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenSettings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains((*requests)[0].Text, "18-100 лет · 150-190 см") || !strings.Contains((*requests)[0].Text, "Россия, Vladikavkaz") {
		t.Fatalf("menu did not show active filters: %s", (*requests)[0].Text)
	}
	before, _ := f.worker.settings(ctx, 1)
	settingsUXTap(t, s, 1, 100, "as:"+before.Nonce+":interval:"+fmt.Sprint(before.Interval))
	after, _ := f.worker.settings(ctx, 1)
	if before.Nonce != after.Nonce || after.Mode != "off" || len(f.requests) != 0 {
		t.Fatalf("selected option changed run or nonce: before=%+v after=%+v", before, after)
	}
}

func TestSettingsUXShowsEffectiveReactionLimits(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	j, err := f.worker.settings(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.worker.updateSetting(ctx, 1, j.Nonce, "cap", 500); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "first", Templates: []string{"Привет!"}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ cutoff, interval, cap int }{{23, 60, 100}, {112, 60, 100}, {0, 30, 500}} {
		if err := s.Store.SetSuperBeforeAge(ctx, 1, tc.cutoff); err != nil {
			t.Fatal(err)
		}
		if err := s.OpenSettings(ctx, 1); err != nil {
			t.Fatal(err)
		}
		menu := (*requests)[len(*requests)-1]
		labels := make(map[string]bool)
		for _, row := range menu.Markup.Rows {
			for _, button := range row {
				labels[button.Text] = true
			}
		}
		if !labels[fmt.Sprintf("⏱ Интервал · %d с", tc.interval)] || !labels[fmt.Sprintf("🧮 Мой лимит · %d /24ч", tc.cap)] {
			t.Fatalf("wrong limits in settings for cutoff %d: %v", tc.cutoff, labels)
		}
		wantStatus := fmt.Sprintf("интервал %d с; мой лимит %d /24ч", tc.interval, tc.cap)
		if status := s.AutolikeStatus(ctx, 1); !strings.Contains(status, wantStatus) {
			t.Fatalf("wrong limits in status for cutoff %d: %s", tc.cutoff, status)
		}
		settingsUXTap(t, s, 1, 100, settingsUXAction(t, menu, "view:cap"))
		capMenu := (*requests)[len(*requests)-1]
		if !strings.Contains(capMenu.Text, fmt.Sprintf("Мой лимит: %d лайков /24ч", tc.cap)) {
			t.Fatalf("wrong cap details for cutoff %d: %s", tc.cutoff, capMenu.Text)
		}
		settingsUXAction(t, capMenu, "cap:500")
	}
	if len(f.requests) != 0 {
		t.Fatalf("viewing limits contacted Jardulik: %v", f.requests)
	}
}

func TestSettingsUXSuccessfulReconnectDoesNotShowOldDisconnectAsCurrent(t *testing.T) {
	f, requests := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if _, err := f.worker.settings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.DisableAutolike(ctx, 1, "disconnected"); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenSettingsMessage(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	menu := (*requests)[0]
	if !strings.Contains(menu.Text, "Подключение восстановлено") || strings.Contains(menu.Text, "Нужно переподключение") {
		t.Fatal("healthy session shows an obsolete reconnect error", menu.Text)
	}
	settingsUXAction(t, menu, "automatic")
	state, err := f.worker.settings(ctx, 1)
	if err != nil || state.Mode != "off" || state.Reason != "disconnected" || len(f.requests) != 0 {
		t.Fatal("render changed the previous run or used source", err)
	}
}
