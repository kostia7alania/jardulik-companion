package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestAutoOpenAgeFilterStillSkipsUnknownAndUnderageFavorites(t *testing.T) {
	for _, age := range []int64{0, 17, 18} {
		t.Run(fmt.Sprint(age), func(t *testing.T) {
			f := newAutoFixture(t)
			f.add(t, 1, 81, age, true)
			filters := DefaultFeedFilters()
			filters.AgeMin, filters.AgeMax = 0, 25
			raw, _ := json.Marshal(filters)
			if _, err := f.worker.Service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
				t.Fatal(err)
			}
			if err := f.worker.Start(context.Background(), 1, "series", 1); err != nil {
				t.Fatal(err)
			}
			f.step(t)
			want := 0
			if age >= 18 {
				want = 1
			}
			if len(f.sent) != want {
				t.Fatalf("age %d sent reactions: %v", age, f.sent)
			}
		})
	}
}

func TestAutoSendsSelectedZeroTo25Query(t *testing.T) {
	f := newAutoFixture(t)
	filters := DefaultFeedFilters()
	filters.AgeMin, filters.AgeMax = 0, 25
	raw, _ := json.Marshal(filters)
	if _, err := f.worker.Service.Store.DB.Exec("UPDATE feed_filters SET applied_json=? WHERE owner_id=1", string(raw)); err != nil {
		t.Fatal(err)
	}
	seen := false
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/user/getUsers" {
			seen = true
			if r.URL.Query().Get("ageMin") != "0" || r.URL.Query().Get("ageMax") != "25" {
				t.Fatal("auto did not use personal filter", r.URL.RawQuery)
			}
		}
	}
	if err := f.worker.Start(context.Background(), 1, "series", 1); err != nil {
		t.Fatal(err)
	}
	f.step(t)
	if !seen {
		t.Fatal("personal feed not requested")
	}
}
