package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type filterMenuUXRequest struct {
	uxMessage
	MessageID int64 `json:"message_id"`
	Method    string
}

func filterMenuUXFixture(t *testing.T) (*Service, *[]filterMenuUXRequest, *int64, *[]string) {
	t.Helper()
	s, _, premium, source := filterFixture(t)
	var requests []filterMenuUXRequest
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request filterMenuUXRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		request.Method = filepath.Base(r.URL.Path)
		if request.Method != "sendMessage" && request.Method != "editMessageText" {
			t.Fatalf("unexpected filter menu request: %s", request.Method)
		}
		requests = append(requests, request)
		id := request.MessageID
		if id == 0 {
			id = 100
		}
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":%d}}}`, id, request.ChatID)
	})}
	return s, &requests, premium, source
}

func filterMenuUXTap(t *testing.T, s *Service, owner, messageID int64, action string) {
	t.Helper()
	state, err := s.Store.filterState(context.Background(), owner)
	if err != nil || state.Draft == nil {
		t.Fatalf("missing filter draft: %v", err)
	}
	if !s.FilterMessageCallback(context.Background(), owner, messageID, "flt:"+state.Draft.ID+":"+action) {
		t.Fatal("filter callback was not handled")
	}
}

func filterMenuUXOnlyEdits(t *testing.T, requests []filterMenuUXRequest, owner, messageID int64) {
	t.Helper()
	for _, request := range requests {
		if request.Method != "editMessageText" || request.MessageID != messageID || request.ChatID != owner {
			t.Fatalf("filter navigation created or edited another menu: %+v", request)
		}
	}
}

func TestFilterMenuUXGeographyAndApplyEditTheCheckedMessage(t *testing.T) {
	s, requests, _, source := filterMenuUXFixture(t)
	ctx := context.Background()
	if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	// Stored IDs only continue a text picker; a private callback always supplies its actual target.
	if _, err := s.Store.DB.ExecContext(ctx, `UPDATE feed_filters SET draft_json=json_set(draft_json,'$.messageId',777) WHERE owner_id=1`); err != nil {
		t.Fatal(err)
	}
	filterMenuUXTap(t, s, 1, 100, "edit:country")
	state, err := s.Store.filterState(ctx, 1)
	if err != nil || state.Draft.MessageID != 100 || state.Draft.Input != "country" {
		t.Fatalf("callback trusted stored message ID: %+v err=%v", state, err)
	}
	if !s.FilterText(ctx, 1, "Россия") {
		t.Fatal("country text ignored")
	}
	filterMenuUXTap(t, s, 1, 100, "edit:city")
	if !s.FilterText(ctx, 1, "Владикавказ") {
		t.Fatal("city text ignored")
	}
	filterMenuUXTap(t, s, 1, 100, "apply")
	state, err = s.Store.filterState(ctx, 1)
	if err != nil || state.Applied == nil || state.Applied.Country != "Russia" || state.Applied.City != "Vladikavkaz" || state.Draft != nil {
		t.Fatalf("geography draft not applied: %+v err=%v", state, err)
	}
	last := (*requests)[len(*requests)-1]
	if !strings.Contains(last.Text, "Автолайкер:") || !strings.Contains(last.Text, "Фильтры применены") {
		t.Fatalf("apply did not return to autoliker: %s", last.Text)
	}
	filterMenuUXOnlyEdits(t, *requests, 1, 100)
	for _, request := range *source {
		if strings.Contains(request, "getUsers") || strings.Contains(request, "reaction") {
			t.Fatalf("filter navigation started a run: %s", request)
		}
	}
	j, err := NewAutoliker(s).settings(ctx, 1)
	if err != nil || j.Mode != "off" {
		t.Fatalf("filters enabled autolikes: %+v err=%v", j, err)
	}
}

func TestFilterMenuUXCancelAndBackStopConsumingText(t *testing.T) {
	for _, action := range []string{"cancel", "settings"} {
		t.Run(action, func(t *testing.T) {
			s, requests, _, _ := filterMenuUXFixture(t)
			ctx := context.Background()
			if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
				t.Fatal(err)
			}
			filterMenuUXTap(t, s, 1, 100, "edit:country")
			filterMenuUXTap(t, s, 1, 100, action)
			state, err := s.Store.filterState(ctx, 1)
			if err != nil || state.Draft != nil || state.Applied != nil || s.FilterText(ctx, 1, "Россия") {
				t.Fatalf("leaving filters retained text input or applied values: %+v err=%v", state, err)
			}
			if !strings.Contains((*requests)[len(*requests)-1].Text, "Автолайкер:") {
				t.Fatal("leaving filters did not return to autoliker")
			}
			filterMenuUXOnlyEdits(t, *requests, 1, 100)
		})
	}
}

func TestFilterMenuUXStaleApplyRefreshesWithoutApplying(t *testing.T) {
	s, requests, _, source := filterMenuUXFixture(t)
	ctx := context.Background()
	if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
		t.Fatal(err)
	}
	state, _ := s.Store.filterState(ctx, 1)
	stale := "flt:" + state.Draft.ID + ":apply"
	filterMenuUXTap(t, s, 1, 100, "edit:age")
	if !s.FilterText(ctx, 1, "0 25") {
		t.Fatal("age input ignored")
	}
	if !s.FilterMessageCallback(ctx, 1, 100, stale) {
		t.Fatal("stale apply not handled")
	}
	state, err := s.Store.filterState(ctx, 1)
	if err != nil || state.Applied != nil || state.Draft == nil || state.Draft.Values.AgeMin != 0 || state.Draft.Values.AgeMax != 25 || state.Draft.Input != "" {
		t.Fatalf("stale apply committed or lost current values: %+v err=%v", state, err)
	}
	if !strings.Contains((*requests)[len(*requests)-1].Text, "нажми Применить ещё раз") || len(*source) != 1 {
		t.Fatal("stale apply did not refresh in place without source action")
	}
	filterMenuUXOnlyEdits(t, *requests, 1, 100)
}

func TestFilterMenuUXApplyStillRequiresPremiumAndOriginalSession(t *testing.T) {
	for _, changed := range []string{"premium", "session"} {
		t.Run(changed, func(t *testing.T) {
			s, requests, premium, _ := filterMenuUXFixture(t)
			ctx := context.Background()
			if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
				t.Fatal(err)
			}
			if changed == "premium" {
				*premium = 0
			} else if err := s.Store.SaveSession(ctx, 1, Session{Token: "new", Refresh: "new-r", UserID: 999, Generation: "new-login", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
				t.Fatal(err)
			}
			filterMenuUXTap(t, s, 1, 100, "apply")
			state, err := s.Store.filterState(ctx, 1)
			if err != nil || state.Applied != nil {
				t.Fatalf("apply bypassed account guard: %+v err=%v", state, err)
			}
			if !strings.Contains((*requests)[len(*requests)-1].Text, "Не удалось изменить фильтры") {
				t.Fatal("failed apply did not explain the result in the menu")
			}
			filterMenuUXOnlyEdits(t, *requests, 1, 100)
		})
	}
}

func TestFilterMenuUXRangeInputUpdatesOnlyDraft(t *testing.T) {
	for _, field := range []string{"age", "height"} {
		t.Run(field, func(t *testing.T) {
			s, requests, _, source := filterMenuUXFixture(t)
			ctx := context.Background()
			if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
				t.Fatal(err)
			}
			filterMenuUXTap(t, s, 1, 100, "edit:"+field)
			input := "0 25"
			if field == "height" {
				input = "150 180"
			}
			if !s.FilterText(ctx, 1, input) {
				t.Fatal("range input ignored")
			}
			state, err := s.Store.filterState(ctx, 1)
			if err != nil || state.Applied != nil || state.Draft == nil || state.Draft.Input != "" || len(*source) != 1 {
				t.Fatalf("range input applied or queried source: %+v err=%v", state, err)
			}
			f := state.Draft.Values
			if field == "age" && (f.AgeMin != 0 || f.AgeMax != 25) || field == "height" && (f.HeightMin != 150 || f.HeightMax != 180) {
				t.Fatalf("range input lost: %+v", f)
			}
			filterMenuUXOnlyEdits(t, *requests, 1, 100)
		})
	}
}

func TestFilterMenuUXInvalidRangeKeepsDraftAndPicker(t *testing.T) {
	for field, invalid := range map[string][]string{
		"age":    {"", "0-25", "25 0", "-1 25", "0 101", "0 +25", "0 x", "0 25 30"},
		"height": {"99 180", "150 251", "200 150"},
	} {
		t.Run(field, func(t *testing.T) {
			s, requests, _, source := filterMenuUXFixture(t)
			ctx := context.Background()
			if err := s.OpenFiltersMessage(ctx, 1, 100); err != nil {
				t.Fatal(err)
			}
			filterMenuUXTap(t, s, 1, 100, "edit:"+field)
			before, _ := s.Store.filterState(ctx, 1)
			for _, input := range invalid {
				if !s.FilterText(ctx, 1, input) {
					t.Fatalf("invalid range %q escaped the active picker", input)
				}
				state, err := s.Store.filterState(ctx, 1)
				if err != nil || state.Applied != nil || state.Draft == nil || state.Draft.Values != before.Draft.Values || state.Draft.Input != field {
					t.Fatalf("invalid range %q changed filters: %+v err=%v", input, state, err)
				}
			}
			if len(*source) != 1 {
				t.Fatal("invalid range queried source")
			}
			filterMenuUXOnlyEdits(t, *requests, 1, 100)
		})
	}
}

func TestFeedFiltersValidationAllowsGlobal111WithoutChangingPersonalDefaults(t *testing.T) {
	f := DefaultFeedFilters()
	f.AgeMin, f.AgeMax = 0, 111
	if err := f.validate(); err != nil {
		t.Fatal("global crawler bounds rejected", err)
	}
	f.AgeMax = 112
	if err := f.validate(); err == nil {
		t.Fatal("unbounded age maximum accepted")
	}
	if f := DefaultFeedFilters(); f.AgeMin != 18 || f.AgeMax != 60 {
		t.Fatal("global bound changed ordinary personal defaults")
	}
}
