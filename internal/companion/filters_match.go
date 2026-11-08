package companion

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

func autoInteger(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v || v < 0 || v > 1000 {
			return 0, false
		}
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	return 0, false
}

// Missing required fields are not a match. Only explicit favorites bypass filters.
func profileMatchesAutoFilters(p Profile, f FeedFilters) bool {
	if f.validate() != nil {
		return false
	}
	age, ok := autoInteger(p["age"])
	if !ok || age <= 0 || age > 100 || age < f.AgeMin || age > f.AgeMax {
		return false
	}
	height, ok := autoInteger(p["height"])
	if !ok || height < f.HeightMin || height > f.HeightMax {
		return false
	}
	for key, wanted := range map[string]string{"goal": f.Goal, "children": f.Children, "worldview": f.Worldview, "country": f.Country, "city": f.City} {
		if wanted == "" {
			continue
		}
		actual, ok := p[key].(string)
		if !ok || !strings.EqualFold(strings.TrimSpace(actual), strings.TrimSpace(wanted)) {
			return false
		}
	}
	return true
}
