package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func filterFixture(t *testing.T) (*Service, *[]uxMessage, *int64, *[]string) {
	t.Helper()
	s, messages := uxFixture(t)
	premium := int64(1)
	var requests []string
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		if err := s.Store.SaveSession(ctx, owner, Session{Token: "token", Refresh: "refresh", UserID: owner + 100, Expires: time.Now().Add(time.Hour).Unix(), Generation: fmt.Sprint(owner)}); err != nil {
			t.Fatal(err)
		}
		if err := s.Store.SaveAccount(ctx, owner, Profile{"userId": owner + 100, "type": 1, "minAge": 24, "maxAge": 77}); err != nil {
			t.Fatal(err)
		}
	}
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.String())
		switch {
		case r.URL.Host == "mini.jardulik.app":
			if r.URL.Scheme != "https" || r.Header.Get("Authorization") != "" || r.URL.Path != "/assets/RU-DtiNLDz2.js" {
				t.Fatalf("untrusted dictionary request: %s", r.URL)
			}
			raw, err := os.ReadFile(filepath.Join("testdata", "filters", "RU.js"))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = w.Write(raw)
		case r.URL.Path == "/user/getMe":
			fmt.Fprintf(w, `{"userId":%s,"type":%d,"minAge":24,"maxAge":77}`, r.URL.Query().Get("userId"), premium)
		case r.URL.Path == "/user/getUsers":
			fmt.Fprint(w, `[{"userId":13,"name":"Анкета"}]`)
		default:
			t.Fatalf("unexpected upstream action: %s %s", r.Method, r.URL)
		}
	})}
	return s, messages, &premium, &requests
}

func filterTap(t *testing.T, s *Service, owner int64, action string) {
	t.Helper()
	state, err := s.Store.filterState(context.Background(), owner)
	if err != nil || state.Draft == nil {
		t.Fatalf("missing draft: %v", err)
	}
	if !s.FilterCallback(context.Background(), owner, "flt:"+state.Draft.ID+":"+action) {
		t.Fatal("filter callback was not handled")
	}
}

func TestFiltersTenantScopePersistenceAndActualQuery(t *testing.T) {
	s, sent, premium, requests := filterFixture(t)
	ctx := context.Background()
	for _, owner := range []int64{1, 2} {
		if _, err := s.Store.Archive(ctx, owner, []Profile{{"userId": 11}, {"userId": 12}}, "filters-test"); err != nil {
			t.Fatal(err)
		}
	}
	s.Store.Delivered(ctx, 1, 11)
	if err := s.Store.Favorite(ctx, 1, 11); err != nil {
		t.Fatal(err)
	}
	if err := s.OpenFilters(ctx, 1); err != nil {
		t.Fatal(err)
	}
	state, _ := s.Store.filterState(ctx, 1)
	if state.Applied != nil || state.Draft.Values.AgeMin != 24 || state.Draft.Values.AgeMax != 77 {
		t.Fatal("opening filters silently changed legacy effective ages")
	}
	stale := state.Draft.ID
	if err := s.OpenFilters(ctx, 2); err != nil {
		t.Fatal(err)
	}
	s.FilterCallback(ctx, 2, "flt:"+stale+":reset")
	other, _ := s.Store.filterState(ctx, 2)
	if other.Draft.Values.AgeMax != 77 || other.Applied != nil {
		t.Fatal("cross-tenant button changed filters")
	}

	filterTap(t, s, 1, "edit:country")
	if !s.FilterText(ctx, 1, "Россия") {
		t.Fatal("country input ignored")
	}
	filterTap(t, s, 1, "edit:city")
	if !s.FilterText(ctx, 1, "Владикавказ") {
		t.Fatal("city input ignored")
	}
	filterTap(t, s, 1, "set:worldview:dont_tell")
	filterTap(t, s, 1, "set:goal:serious")
	cancelled := false
	s.runs.Store(int64(1), context.CancelFunc(func() { cancelled = true }))
	filterTap(t, s, 1, "apply")
	if !cancelled {
		t.Fatal("applying filters did not cancel the old run")
	}
	state, err := s.Store.filterState(ctx, 1)
	if err != nil || state.Applied == nil || state.Draft != nil || state.Applied.Country != "Russia" || state.Applied.City != "Vladikavkaz" || state.Applied.Worldview != "dont_tell" || state.Applied.AgeMax != 77 {
		t.Fatalf("canonical applied state lost: %+v %v", state, err)
	}
	if _, err := s.Store.Next(ctx, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old discovery queue survived apply: %v", err)
	}
	history, err := s.Store.List(ctx, 1, false, 0)
	if err != nil || len(history) != 2 {
		t.Fatal("filter apply hid archive history")
	}
	favorites, err := s.Store.List(ctx, 1, true, 0)
	if err != nil || len(favorites) != 1 || favorites[0].ID != 11 {
		t.Fatal("filter apply changed favorites")
	}
	if s.Store.Page(ctx, 1) != 1 {
		t.Fatal("feed page not reset")
	}
	if p, err := s.Store.Next(ctx, 2); err != nil || p.ID != 11 {
		t.Fatal("another tenant's queue changed")
	}

	if _, err := s.Client.Users(ctx, 1, 2); err != nil {
		t.Fatal(err)
	}
	last := (*requests)[len(*requests)-1]
	for _, value := range []string{"pageNumber=2", "pageSize=150", "all=0", "ageMin=24", "ageMax=77", "goal=serious", "worldview=dont_tell", "country=Russia", "city=Vladikavkaz"} {
		if !strings.Contains(last, value) {
			t.Fatalf("applied filter missing from HTTP query: %s", last)
		}
	}
	batch := []Profile{{"userId": 11}, {"userId": 12}, {"userId": 13}}
	if _, err := s.Store.Archive(ctx, 1, batch, "new-filtered-batch"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.RestoreFilteredBatch(ctx, 1, batch); err != nil {
		t.Fatal(err)
	}
	if p, err := s.Store.Next(ctx, 1); err != nil || p.ID != 12 {
		t.Fatal("matching previously pending card did not return, or delivered card repeated")
	}
	s.Store.Delivered(ctx, 1, 12)
	if _, err := s.Store.Archive(ctx, 1, batch, "repeated-filtered-batch"); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.RestoreFilteredBatch(ctx, 1, batch); err != nil {
		t.Fatal(err)
	}
	if p, err := s.Store.Next(ctx, 1); err != nil || p.ID != 13 {
		t.Fatal("already delivered card repeated after batch replay")
	}
	reopened, err := OpenStore(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	saved, err := reopened.filterState(ctx, 1)
	if err != nil || saved.Applied == nil || saved.Applied.City != "Vladikavkaz" {
		t.Fatal("filters lost after reopening database")
	}

	before := len(*requests)
	*premium = 0
	if _, err := s.Client.Users(ctx, 1, 3); !errors.Is(err, ErrFiltersPremium) {
		t.Fatalf("expired Premium accepted: %v", err)
	}
	if len(*requests) != before+1 || !strings.Contains((*requests)[before], "/user/getMe?") {
		t.Fatal("Premium denial still queried feed")
	}
	var actions int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM actions").Scan(&actions); err != nil || actions != 0 {
		t.Fatal("filter navigation created reactions")
	}
	for _, message := range *sent {
		for _, row := range message.Markup.Rows {
			for _, button := range row {
				if len(button.Data) > 64 {
					t.Fatalf("callback exceeds Telegram limit: %s", button.Data)
				}
			}
		}
	}
}

func TestFiltersResetCancelAndOtherProfilesAreExplicit(t *testing.T) {
	s, _, _, requests := filterFixture(t)
	ctx := context.Background()
	if _, err := s.Client.Users(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains((*requests)[len(*requests)-1], "ageMax=77") {
		t.Fatal("legacy defaults changed before apply")
	}
	if err := s.OpenFilters(ctx, 1); err != nil {
		t.Fatal(err)
	}
	state, _ := s.Store.filterState(ctx, 1)
	stale := state.Draft.ID
	filterTap(t, s, 1, "reset")
	state, _ = s.Store.filterState(ctx, 1)
	if state.Applied != nil || state.Draft.Values != DefaultFeedFilters() {
		t.Fatal("reset did not remain an explicit draft")
	}
	s.FilterCallback(ctx, 1, "flt:"+stale+":apply")
	state, _ = s.Store.filterState(ctx, 1)
	if state.Applied != nil {
		t.Fatal("stale keyboard applied new draft")
	}
	filterTap(t, s, 1, "cancel")
	state, _ = s.Store.filterState(ctx, 1)
	if state.Draft != nil || state.Applied != nil {
		t.Fatal("cancel changed applied filters")
	}
	if err := s.OfferOtherProfiles(ctx, 1); err != nil {
		t.Fatal(err)
	}
	state, _ = s.Store.filterState(ctx, 1)
	if state.Others || state.OfferID == "" {
		t.Fatal("empty-result offer silently widened feed")
	}
	s.FilterCallback(ctx, 2, "flt:others:"+state.OfferID)
	other, _ := s.Store.filterState(ctx, 2)
	if other.Others {
		t.Fatal("another tenant activated the offer")
	}
	s.FilterCallback(ctx, 1, "flt:others:"+state.OfferID)
	if _, err := s.Client.Users(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains((*requests)[len(*requests)-1], "all=1") {
		t.Fatal("explicit other-profiles selection not sent")
	}
	if err := s.OpenFilters(ctx, 1); err != nil {
		t.Fatal(err)
	}
	filterTap(t, s, 1, "reset")
	filterTap(t, s, 1, "apply")
	state, _ = s.Store.filterState(ctx, 1)
	if state.Others || state.Applied == nil || state.Applied.AgeMax != 60 {
		t.Fatal("new filters did not restore strict mode")
	}
}

func TestFilterCitySourceFixturesAndUnknownFormats(t *testing.T) {
	var assets map[string]string
	if err := json.Unmarshal(filterCityAssetsJSON, &assets); err != nil || len(assets) != 228 {
		t.Fatalf("pinned asset index: %d %v", len(assets), err)
	}
	for iso, want := range map[string]string{"RU": "Vladikavkaz", "XS": "Tskhinval", "XA": "Sukhumi", "GE": "Tbilisi", "AM": "Yerevan"} {
		raw, err := os.ReadFile(filepath.Join("testdata", "filters", iso+".js"))
		if err != nil {
			t.Fatal(err)
		}
		cities, err := decodeCityModule(raw)
		if err != nil {
			t.Fatalf("%s source module rejected: %v", iso, err)
		}
		found := false
		for _, city := range cities {
			if city.Name == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s source canonical city missing", iso)
		}
	}
	tooManyRows := `const a=[` + strings.Repeat(`"City",`, filterCityRows) + `"City"];export{a as default};`
	if _, err := decodeCityModule([]byte(tooManyRows)); err == nil {
		t.Fatal("city row limit was not enforced")
	}
	for _, source := range []string{
		`const a=fetch("https://other.invalid");export{a as default};`,
		`const a=[{name:"X"}];fetch("https://other.invalid");export{a as default};`,
		`const a=[{name:""}];export{a as default};`,
		`<html>challenge</html>`,
		strings.Repeat(" ", filterCityBytes+1),
	} {
		if _, err := decodeCityModule([]byte(source)); err == nil {
			t.Fatal("unknown or oversized city module accepted")
		}
	}
}

func TestFilterCityLookupBoundsCacheAndRedirect(t *testing.T) {
	s, _, _, _ := filterFixture(t)
	var assets map[string]string
	if err := json.Unmarshal(filterCityAssetsJSON, &assets); err != nil {
		t.Fatal(err)
	}
	countries := make([]string, 0, len(assets))
	for country := range assets {
		countries = append(countries, country)
	}
	sort.Strings(countries)
	calls := 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Host != "mini.jardulik.app" || r.URL.Scheme != "https" || r.Header.Get("Authorization") != "" || r.Context().Err() != nil {
			t.Fatal("dictionary URL or identity escaped boundary")
		}
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 11*time.Second {
			t.Fatal("dictionary request has no bounded deadline")
		}
		fmt.Fprint(w, `const a=[{name:"Example",ruName:"Пример"}];export{a as default};`)
	})}
	for _, country := range countries[:9] {
		if _, err := s.Client.filterCities(context.Background(), country); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.Client.cities.Values) != 8 {
		t.Fatal("city cache exceeded eight countries")
	}
	if _, err := s.Client.filterCities(context.Background(), countries[8]); err != nil {
		t.Fatal(err)
	}
	if calls != 9 {
		t.Fatal("cached city dictionary fetched again")
	}
	if _, err := s.Client.filterCities(context.Background(), countries[0]); err != nil {
		t.Fatal(err)
	}
	if calls != 10 {
		t.Fatal("oldest city dictionary was not evicted")
	}

	s.Client.cities.Values = nil
	s.Client.cities.Order = nil
	calls = 0
	s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "http://127.0.0.1/internal")
		w.WriteHeader(http.StatusFound)
	})}
	if _, err := s.Client.filterCities(context.Background(), "Russia"); err == nil || calls != 1 {
		t.Fatal("dictionary followed redirect or accepted it")
	}
	if _, err := s.Client.filterCities(context.Background(), "http://127.0.0.1"); err == nil || calls != 1 {
		t.Fatal("arbitrary dictionary origin accepted")
	}
}

func TestFiltersDiscoveryRejectsSessionChange(t *testing.T) {
	for _, stage := range []string{"getMe", "getUsers"} {
		t.Run(stage, func(t *testing.T) {
			s, _, _, _ := filterFixture(t)
			calls := 0
			s.Client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Query().Get("userId") != "101" {
					t.Fatal("request used another account's ID")
				}
				if r.URL.Path == "/user/"+stage {
					if err := s.Store.SaveSession(context.Background(), 1, Session{Token: "new", Refresh: "new-r", UserID: 999, Generation: "new-login", Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
						t.Fatal(err)
					}
				}
				if r.URL.Path == "/user/getMe" {
					fmt.Fprint(w, `{"userId":101,"type":1}`)
				} else {
					fmt.Fprint(w, `[{"userId":13}]`)
				}
			})}
			profiles, err := s.Client.Users(context.Background(), 1, 1)
			if err == nil || profiles != nil {
				t.Fatal("stale discovery response accepted after reconnect")
			}
			if stage == "getMe" && calls != 1 {
				t.Fatal("feed queried after stale account response")
			}
			current, err := s.Store.Session(context.Background(), 1)
			if err != nil || current.Generation != "new-login" || current.UserID != 999 {
				t.Fatal("new session overwritten by stale discovery")
			}
		})
	}
}

func TestFiltersManualDeliveryWhileHiddenDoesNotRequeue(t *testing.T) {
	s, _, _, _ := filterFixture(t)
	ctx := context.Background()
	batch := []Profile{{"userId": 11, "name": "Анкета"}}
	for _, owner := range []int64{1, 2} {
		if _, err := s.Store.Archive(ctx, owner, batch, "before-filter"); err != nil {
			t.Fatal(err)
		}
		if err := s.OpenFilters(ctx, owner); err != nil {
			t.Fatal(err)
		}
		filterTap(t, s, owner, "reset")
		filterTap(t, s, owner, "apply")
	}
	p, err := s.Store.Profile(ctx, 1, 11)
	if err != nil {
		t.Fatal(err)
	}
	// Opening this hidden card from history is still a confirmed manual delivery.
	if err := s.Deliver(ctx, 1, p); err != nil {
		t.Fatal(err)
	}
	var ownerMarkers, otherMarkers int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM feed_filter_pending WHERE owner_id=1 AND profile_id=11").Scan(&ownerMarkers); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM feed_filter_pending WHERE owner_id=2 AND profile_id=11").Scan(&otherMarkers); err != nil {
		t.Fatal(err)
	}
	if ownerMarkers != 0 || otherMarkers != 1 {
		t.Fatalf("manual delivery did not clear only its owner's marker: owner=%d other=%d", ownerMarkers, otherMarkers)
	}
	for _, owner := range []int64{1, 2} {
		if _, err := s.Store.Archive(ctx, owner, batch, "fresh-filtered-response"); err != nil {
			t.Fatal(err)
		}
		if err := s.Store.RestoreFilteredBatch(ctx, owner, batch); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Store.Next(ctx, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("already delivered hidden card was requeued: %v", err)
	}
	if p, err := s.Store.Next(ctx, 2); err != nil || p.ID != 11 {
		t.Fatalf("another owner's unseen card was not restored: %v", err)
	}
}

func TestFiltersManualDeliveryWithoutFilterTables(t *testing.T) {
	s, _ := uxFixture(t)
	ctx := context.Background()
	if _, err := s.Store.Archive(ctx, 1, []Profile{{"userId": 11}}, "manual-only"); err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.Profile(ctx, 1, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Deliver(ctx, 1, p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.Next(ctx, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("manual delivery without filters was not confirmed: %v", err)
	}
	var firstCard sql.NullString
	if err := s.Store.DB.QueryRow("SELECT first_card_at FROM users WHERE id=1").Scan(&firstCard); err != nil || !firstCard.Valid {
		t.Fatalf("first delivery was not recorded: %v", err)
	}
	var tables int
	if err := s.Store.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='feed_filter_pending'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("manual delivery unexpectedly requires filter schema: %d %v", tables, err)
	}
}
