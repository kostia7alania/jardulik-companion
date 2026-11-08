package companion

import "testing"

func TestAutoFiltersRequireKnownFieldsAndAllAppliedDimensions(t *testing.T) {
	f := DefaultFeedFilters()
	f.AgeMax = 25
	f.Goal = "serious"
	f.Children = "none"
	f.Worldview = "orthodox"
	f.Country = "Russia"
	f.City = "Vladikavkaz"
	p := Profile{"age": 23, "height": 170, "goal": "serious", "children": "none", "worldview": "orthodox", "country": "Russia", "city": "Vladikavkaz"}
	if !profileMatchesAutoFilters(p, f) {
		t.Fatal("valid source fields rejected")
	}
	for key := range p {
		copy := Profile{}
		for k, v := range p {
			copy[k] = v
		}
		delete(copy, key)
		if profileMatchesAutoFilters(copy, f) {
			t.Fatalf("unknown required %s matched", key)
		}
	}
	for key, value := range map[string]any{"age": 26, "height": 99, "goal": "friends", "city": "Other", "age-invalid": 2.5} {
		copy := Profile{}
		for k, v := range p {
			copy[k] = v
		}
		if key == "age-invalid" {
			key = "age"
		}
		copy[key] = value
		if profileMatchesAutoFilters(copy, f) {
			t.Fatalf("nonmatching %s accepted", key)
		}
	}
}
