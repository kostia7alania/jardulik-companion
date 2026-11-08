package companion

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrFiltersPremium = errors.New("фильтры доступны с Jardulik Premium")

// FeedFilters uses the same query values as Jardulik's public filter sheet.
type FeedFilters struct {
	AgeMin    int64  `json:"ageMin"`
	AgeMax    int64  `json:"ageMax"`
	HeightMin int64  `json:"heightMin"`
	HeightMax int64  `json:"heightMax"`
	Goal      string `json:"goal"`
	Children  string `json:"children"`
	Worldview string `json:"worldview"`
	Country   string `json:"country"`
	City      string `json:"city"`
}

func DefaultFeedFilters() FeedFilters {
	return FeedFilters{AgeMin: 18, AgeMax: 60, HeightMin: 100, HeightMax: 250}
}

func existingFeedFilters(account Profile) FeedFilters {
	f := DefaultFeedFilters()
	// Preserve the companion's previous effective query until its owner changes it.
	f.AgeMax = 80
	if n := number(account["minAge"]); n >= 18 {
		f.AgeMin = n
	}
	if n := number(account["maxAge"]); n >= 18 {
		f.AgeMax = n
	}
	return f
}

var filterOptions = map[string][]string{
	"goal":      {"family", "serious", "friends", "business"},
	"children":  {"none", "none_but_want", "have", "have_more"},
	"worldview": {"iron", "orthodox", "islam", "dont_tell"},
}

func (f FeedFilters) validate() error {
	if f.AgeMin < 18 || f.AgeMax < f.AgeMin || f.HeightMin < 100 || f.HeightMax > 250 || f.HeightMin > f.HeightMax {
		return errors.New("неверный диапазон фильтра")
	}
	for field, value := range map[string]string{"goal": f.Goal, "children": f.Children, "worldview": f.Worldview} {
		if value == "" {
			continue
		}
		found := false
		for _, option := range filterOptions[field] {
			found = found || value == option
		}
		if !found {
			return errors.New("неизвестное значение фильтра")
		}
	}
	if f.Country != "" {
		if _, ok := countryLabels[f.Country]; !ok {
			return errors.New("страна отсутствует в справочнике Jardulik")
		}
	}
	if f.City != "" && f.Country == "" {
		return errors.New("сначала выбери страну")
	}
	if utf8.RuneCountInString(f.City) > 160 || strings.IndexFunc(f.City, unicode.IsControl) >= 0 {
		return errors.New("неверное название города")
	}
	return nil
}

func (f FeedFilters) query(user int64, page int, others bool) url.Values {
	q := url.Values{
		"userId": {strconv.FormatInt(user, 10)}, "pageNumber": {strconv.Itoa(page)}, "pageSize": {"150"},
		"all": {"0"}, "ageMin": {strconv.FormatInt(f.AgeMin, 10)}, "ageMax": {strconv.FormatInt(f.AgeMax, 10)},
		"heightMin": {strconv.FormatInt(f.HeightMin, 10)}, "heightMax": {strconv.FormatInt(f.HeightMax, 10)},
		"goal": {f.Goal}, "children": {f.Children}, "worldview": {f.Worldview}, "country": {f.Country}, "city": {f.City},
	}
	if others {
		q.Set("all", "1")
	}
	return q
}

type filterDraft struct {
	ID         string       `json:"id"`
	Generation string       `json:"generation"`
	UserID     int64        `json:"userId"`
	Values     FeedFilters  `json:"values"`
	Input      string       `json:"input,omitempty"`
	Cities     []filterCity `json:"cities,omitempty"`
}

type filterState struct {
	Applied *FeedFilters
	Draft   *filterDraft
	Others  bool
	OfferID string
}

const filterSchema = `
CREATE TABLE IF NOT EXISTS feed_filters (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 applied_json TEXT, draft_json TEXT, include_others INTEGER NOT NULL DEFAULT 0,
 others_id TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS feed_filter_pending (
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 profile_id INTEGER NOT NULL, PRIMARY KEY(owner_id,profile_id)
);`

func (s *Store) filterState(ctx context.Context, owner int64) (filterState, error) {
	var state filterState
	if _, err := s.DB.ExecContext(ctx, filterSchema); err != nil {
		return state, err
	}
	var applied, draft sql.NullString
	err := s.DB.QueryRowContext(ctx, "SELECT applied_json,draft_json,include_others,others_id FROM feed_filters WHERE owner_id=?", owner).
		Scan(&applied, &draft, &state.Others, &state.OfferID)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if applied.Valid {
		if err = json.Unmarshal([]byte(applied.String), &state.Applied); err != nil {
			return state, errors.New("сохранённые фильтры повреждены")
		}
		if state.Applied != nil {
			if err = state.Applied.validate(); err != nil {
				return state, err
			}
		}
	}
	if draft.Valid {
		if err = json.Unmarshal([]byte(draft.String), &state.Draft); err != nil {
			return state, errors.New("черновик фильтров повреждён; открой /filters заново")
		}
	}
	return state, nil
}

func filterNonce() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *Store) saveFilterDraft(ctx context.Context, owner int64, draft *filterDraft) error {
	var raw any
	if draft != nil {
		id, err := filterNonce()
		if err != nil {
			return err
		}
		draft.ID = id
		b, err := json.Marshal(draft)
		if err != nil {
			return err
		}
		raw = string(b)
	}
	_, err := s.DB.ExecContext(ctx, "INSERT INTO feed_filters(owner_id,draft_json) VALUES(?,?) ON CONFLICT(owner_id) DO UPDATE SET draft_json=excluded.draft_json", owner, raw)
	return err
}

func (s *Store) applyFilterDraft(ctx context.Context, owner int64, draft filterDraft) error {
	if err := draft.Values.validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(draft.Values)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "UPDATE feed_filters SET applied_json=?,draft_json=NULL,include_others=0,others_id='' WHERE owner_id=? AND json_extract(draft_json,'$.id')=?", string(raw), owner, draft.ID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return errors.New("черновик устарел; открой /filters заново")
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO feed_filter_pending(owner_id,profile_id) SELECT owner_id,profile_id FROM queue WHERE owner_id=? AND delivered=0", owner); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE queue SET delivered=1 WHERE owner_id=? AND delivered=0", owner); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE users SET feed_page=1 WHERE id=?", owner); err != nil {
		return err
	}
	return tx.Commit()
}

// RestoreFilteredBatch is called after archiving a successful discovery response.
// Only cards invalidated before delivery can rejoin the new filtered queue.
func (s *Store) RestoreFilteredBatch(ctx context.Context, owner int64, profiles []Profile) error {
	if len(profiles) == 0 {
		return nil
	}
	if _, err := s.DB.ExecContext(ctx, filterSchema); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, profile := range profiles {
		id := profile.ID()
		if id <= 0 {
			return errors.New("invalid profile in discovery response")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE queue SET delivered=0 WHERE owner_id=? AND profile_id=? AND EXISTS(SELECT 1 FROM feed_filter_pending p WHERE p.owner_id=queue.owner_id AND p.profile_id=queue.profile_id)", owner, id); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM feed_filter_pending WHERE owner_id=? AND profile_id=?", owner, id); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE feed_filters SET others_id='' WHERE owner_id=?", owner); err != nil {
		return err
	}
	return tx.Commit()
}

//go:embed filter_city_assets.json
var filterCityAssetsJSON []byte

type filterCity struct {
	Name    string `json:"name"`
	Russian string `json:"ruName,omitempty"`
}

type filterCityCache struct {
	sync.Mutex
	Values map[string][]filterCity
	Order  []string
}

const filterCityBytes = 2 << 20
const filterCityRows = 50000

func (c *Client) filterCities(ctx context.Context, country string) ([]filterCity, error) {
	c.cities.Lock()
	cached, ok := c.cities.Values[country]
	c.cities.Unlock()
	if ok {
		return cached, nil
	}
	var assets map[string]string
	if json.Unmarshal(filterCityAssetsJSON, &assets) != nil {
		return nil, errors.New("справочник стран повреждён")
	}
	asset := assets[country]
	if asset == "" || strings.ContainsAny(asset, "/\\?#:") || !strings.HasSuffix(asset, ".js") {
		return nil, errors.New("для этой страны справочник городов недоступен; выбери Все города")
	}
	// Asset names come only from the pinned public build, never from user input.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://mini.jardulik.app/assets/"+asset, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/javascript")
	client := *c.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.New("не удалось загрузить справочник; выбери Все города или попробуй позже")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New("справочник городов недоступен; выбери Все города или попробуй позже")
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, filterCityBytes+1))
	if err != nil || len(raw) > filterCityBytes {
		return nil, errors.New("справочник городов превысил лимит; доступны Все города")
	}
	cities, err := decodeCityModule(raw)
	if err != nil {
		return nil, errors.New("формат справочника изменился; доступны Все города")
	}
	c.cities.Lock()
	defer c.cities.Unlock()
	if c.cities.Values == nil {
		c.cities.Values = make(map[string][]filterCity)
	}
	if existing, ok := c.cities.Values[country]; ok {
		return existing, nil
	}
	c.cities.Values[country] = cities
	c.cities.Order = append(c.cities.Order, country)
	if len(c.cities.Order) > 8 {
		delete(c.cities.Values, c.cities.Order[0])
		c.cities.Order = c.cities.Order[1:]
	}
	return cities, nil
}

// Decode only the two static data formats emitted by the checked build.
func decodeCityModule(raw []byte) ([]filterCity, error) {
	fail := errors.New("unknown city dictionary format")
	s := strings.TrimSpace(string(raw))
	if len(raw) > filterCityBytes || !strings.HasPrefix(s, "const ") {
		return nil, fail
	}
	name, value, ok := strings.Cut(strings.TrimPrefix(s, "const "), "=")
	if !ok || name == "" || strings.IndexFunc(name, func(r rune) bool {
		return !(r == '_' || r == '$' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
	}) >= 0 {
		return nil, fail
	}
	suffix := ";export{" + name + " as default};"
	if !strings.HasSuffix(value, suffix) {
		return nil, fail
	}
	value = strings.TrimSuffix(value, suffix)
	var data []byte
	if strings.HasPrefix(value, "JSON.parse(") && strings.HasSuffix(value, ")") {
		quoted := strings.TrimSuffix(strings.TrimPrefix(value, "JSON.parse("), ")")
		if len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
			return nil, fail
		}
		var escaped strings.Builder
		escaped.WriteByte('"')
		for i := 1; i < len(quoted)-1; i++ {
			switch quoted[i] {
			case '"':
				escaped.WriteString(`\"`)
			case '\\':
				i++
				if i >= len(quoted)-1 {
					return nil, fail
				}
				if quoted[i] == '\'' {
					escaped.WriteByte('\'')
				} else {
					escaped.WriteByte('\\')
					escaped.WriteByte(quoted[i])
				}
			case '\'':
				return nil, fail
			default:
				escaped.WriteByte(quoted[i])
			}
		}
		escaped.WriteByte('"')
		var text string
		if json.Unmarshal([]byte(escaped.String()), &text) != nil {
			return nil, fail
		}
		data = []byte(text)
	} else {
		var out strings.Builder
		for i := 0; i < len(value); {
			if value[i] == '"' {
				start := i
				i++
				for i < len(value) && value[i] != '"' {
					if value[i] == '\\' {
						i++
					}
					i++
				}
				if i >= len(value) {
					return nil, fail
				}
				i++
				out.WriteString(value[start:i])
			} else if value[i] >= 'A' && value[i] <= 'Z' || value[i] >= 'a' && value[i] <= 'z' {
				start := i
				for i < len(value) && (value[i] >= 'A' && value[i] <= 'Z' || value[i] >= 'a' && value[i] <= 'z') {
					i++
				}
				key := value[start:i]
				if key != "name" && key != "ruName" && key != "city" {
					return nil, fail
				}
				out.WriteByte('"')
				out.WriteString(key)
				out.WriteByte('"')
			} else {
				out.WriteByte(value[i])
				i++
			}
		}
		data = []byte(out.String())
	}
	var rows []json.RawMessage
	if json.Unmarshal(data, &rows) != nil || rows == nil || len(rows) > filterCityRows {
		return nil, fail
	}
	cities := make([]filterCity, 0, len(rows))
	for _, row := range rows {
		var city filterCity
		if len(row) > 0 && row[0] == '"' {
			if json.Unmarshal(row, &city.Name) != nil {
				return nil, fail
			}
		} else {
			var fields struct {
				Name    string `json:"name"`
				City    string `json:"city"`
				Russian string `json:"ruName"`
			}
			if json.Unmarshal(row, &fields) != nil {
				return nil, fail
			}
			city = filterCity{Name: fields.Name, Russian: fields.Russian}
			if city.Name == "" {
				city.Name = fields.City
			}
		}
		city.Name = strings.TrimSpace(city.Name)
		if city.Name == "" || utf8.RuneCountInString(city.Name) > 160 || utf8.RuneCountInString(city.Russian) > 160 || strings.IndexFunc(city.Name+city.Russian, unicode.IsControl) >= 0 {
			return nil, fail
		}
		cities = append(cities, city)
	}
	return cities, nil
}

func (s *Service) filterPremium(ctx context.Context, owner int64) (Profile, Session, error) {
	before, err := s.Store.Session(ctx, owner)
	if err != nil {
		return nil, Session{}, errors.New("сначала подключи свой Jardulik через /connect")
	}
	account, err := s.Client.Me(ctx, owner)
	if err != nil {
		return nil, Session{}, err
	}
	current, err := s.Store.Session(ctx, owner)
	if err != nil || before.Generation != current.Generation || before.UserID != current.UserID {
		return nil, Session{}, errors.New("сессия изменилась; открой /filters заново")
	}
	if number(account["type"]) != 1 {
		return nil, Session{}, ErrFiltersPremium
	}
	return account, current, nil
}

func (s *Service) OpenFilters(ctx context.Context, owner int64) error {
	account, session, err := s.filterPremium(ctx, owner)
	if err != nil {
		_ = s.say(ctx, owner, "Не удалось открыть фильтры: "+err.Error(), nil)
		return err
	}
	state, err := s.Store.filterState(ctx, owner)
	if err != nil {
		return err
	}
	values := existingFeedFilters(account)
	if state.Applied != nil {
		values = *state.Applied
	}
	draft := &filterDraft{Generation: session.Generation, UserID: session.UserID, Values: values}
	return s.showFilters(ctx, owner, draft, "", "")
}

func filterChoiceLabel(field, value string) string {
	if value == "" {
		return "Любое"
	}
	if value == "dont_tell" {
		return "Не указывать"
	}
	return profileLabels[field+"_"+value]
}

func filterCountryLabel(country string) string {
	if country == "" {
		return "Любая"
	}
	if label := countryLabels[country]; label != "" {
		return label
	}
	return country
}

func filterCityLabel(city filterCity) string {
	if city.Russian != "" {
		return city.Russian
	}
	return city.Name
}

type filterButton = map[string]string

func filterButtonFor(id, label, action string) filterButton {
	return filterButton{"text": label, "callback_data": "flt:" + id + ":" + action}
}

func (s *Service) showFilters(ctx context.Context, owner int64, draft *filterDraft, field, notice string) error {
	if err := s.Store.saveFilterDraft(ctx, owner, draft); err != nil {
		return err
	}
	button := func(label, action string) filterButton { return filterButtonFor(draft.ID, label, action) }
	f := draft.Values
	text := "Фильтры Jardulik\n"
	var rows [][]filterButton
	switch field {
	case "age", "height":
		min, max := f.AgeMin, f.AgeMax
		label, unit := "Возраст", "лет"
		if field == "height" {
			min, max, label, unit = f.HeightMin, f.HeightMax, "Рост", "см"
		}
		text += fmt.Sprintf("%s: %d-%d %s\n", label, min, max, unit)
		if field == "age" && (min > 60 || max > 60) {
			text += "Сейчас сохранён исходный диапазон. Кнопки используют диапазон Jardulik 18-60 лет.\n"
		}
		rows = [][]filterButton{
			{button("От -1", field+":min:-1"), button("От +1", field+":min:1")},
			{button("До -1", field+":max:-1"), button("До +1", field+":max:1")},
			{button("Любой", field+":any")},
		}
	case "goal", "children", "worldview":
		labels := map[string]string{"goal": "Цель знакомства", "children": "Дети", "worldview": "Мировоззрение"}
		text += labels[field] + "\n"
		rows = append(rows, []filterButton{button("Любое", "set:"+field+":")})
		for _, value := range filterOptions[field] {
			rows = append(rows, []filterButton{button(filterChoiceLabel(field, value), "set:"+field+":"+value)})
		}
	case "country":
		text += "Введи название страны по-русски или по-английски. Текущая: " + filterCountryLabel(f.Country) + "."
		rows = [][]filterButton{{button("Любая страна", "country:")}}
		for _, country := range []string{"Russia", "South Ossetia", "Abkhazia", "Georgia", "Armenia"} {
			rows = append(rows, []filterButton{button(filterCountryLabel(country), "country:"+country)})
		}
	case "city":
		text += "Страна: " + filterCountryLabel(f.Country) + ".\nВведи город по-русски или латиницей. Используется справочник Jardulik."
		rows = [][]filterButton{{button("Все города", "city:")}}
		for i, city := range draft.Cities {
			rows = append(rows, []filterButton{button(filterCityLabel(city), "pickcity:"+strconv.Itoa(i))})
		}
	default:
		text += fmt.Sprintf("Возраст: %d-%d лет\nРост: %d-%d см\nЦель: %s\nДети: %s\nМировоззрение: %s\nСтрана: %s\nГород: %s\n\nЭто черновик. Применить обновит выдачу. История и избранное сохранятся.",
			f.AgeMin, f.AgeMax, f.HeightMin, f.HeightMax, filterChoiceLabel("goal", f.Goal), filterChoiceLabel("children", f.Children), filterChoiceLabel("worldview", f.Worldview), filterCountryLabel(f.Country), func() string {
				if f.City == "" {
					return "Все города"
				}
				return f.City
			}())
		rows = [][]filterButton{
			{button("Возраст", "edit:age"), button("Рост", "edit:height")},
			{button("Цель", "edit:goal"), button("Дети", "edit:children")},
			{button("Мировоззрение", "edit:worldview")},
			{button("Страна", "edit:country"), button("Город", "edit:city")},
			{button("Применить", "apply"), button("Сбросить", "reset")},
			{button("Отмена", "cancel")},
		}
	}
	if field != "" {
		rows = append(rows, []filterButton{button("Назад", "back"), button("Отмена", "cancel")})
	}
	if notice != "" {
		text += "\n\n" + notice
	}
	return s.say(ctx, owner, text, map[string]any{"inline_keyboard": rows})
}

// FilterCallback is called only after Handle has checked the private owner chat.
func (s *Service) FilterCallback(ctx context.Context, owner int64, data string) bool {
	if !strings.HasPrefix(data, "flt:") {
		return false
	}
	handle := func() error {
		state, err := s.Store.filterState(ctx, owner)
		if err != nil {
			return err
		}
		parts := strings.Split(data, ":")
		if len(parts) == 3 && parts[1] == "others" {
			return s.acceptOtherProfiles(ctx, owner, parts[2], state)
		}
		if len(parts) < 3 || state.Draft == nil || parts[1] != state.Draft.ID {
			return errors.New("эта панель устарела; открой /filters заново")
		}
		draft := state.Draft
		switch parts[2] {
		case "cancel":
			if err := s.Store.saveFilterDraft(ctx, owner, nil); err != nil {
				return err
			}
			return s.say(ctx, owner, "Изменения фильтров отменены.", nil)
		case "apply":
			_, session, err := s.filterPremium(ctx, owner)
			if err != nil {
				return err
			}
			mu := s.Client.lock(owner)
			mu.Lock()
			err = func() error {
				defer mu.Unlock()
				current, err := s.Store.Session(ctx, owner)
				if err != nil || current.Generation != session.Generation || current.UserID != session.UserID || session.Generation != draft.Generation || session.UserID != draft.UserID {
					return errors.New("сессия изменилась; открой /filters заново")
				}
				s.cancelRun(owner)
				return s.Store.applyFilterDraft(ctx, owner, *draft)
			}()
			if err != nil {
				return err
			}
			return s.say(ctx, owner, "Фильтры применены. /next покажет новую выдачу. История и избранное сохранены.", nil)
		case "reset":
			draft.Values = DefaultFeedFilters()
			draft.Input, draft.Cities = "", nil
			return s.showFilters(ctx, owner, draft, "", "Сброшено к настройкам Jardulik. Нажми Применить для сохранения.")
		case "back":
			draft.Input, draft.Cities = "", nil
			return s.showFilters(ctx, owner, draft, "", "")
		case "edit":
			if len(parts) != 4 {
				return errors.New("неверная кнопка фильтра")
			}
			field := parts[3]
			switch field {
			case "age", "height", "goal", "children", "worldview", "country", "city":
			default:
				return errors.New("неизвестный фильтр")
			}
			draft.Input, draft.Cities = "", nil
			if field == "city" && draft.Values.Country == "" {
				field = "country"
			}
			if field == "country" || field == "city" {
				draft.Input = field
			}
			return s.showFilters(ctx, owner, draft, field, "")
		case "age", "height":
			field := parts[2]
			lower, upper := int64(18), int64(60)
			min, max := &draft.Values.AgeMin, &draft.Values.AgeMax
			if field == "height" {
				lower, upper, min, max = 100, 250, &draft.Values.HeightMin, &draft.Values.HeightMax
			}
			clamp := func(n int64) int64 {
				if n < lower {
					return lower
				}
				if n > upper {
					return upper
				}
				return n
			}
			if len(parts) == 4 && parts[3] == "any" {
				*min, *max = lower, upper
			} else {
				if len(parts) != 5 || (parts[4] != "-1" && parts[4] != "1") || (parts[3] != "min" && parts[3] != "max") {
					return errors.New("неверный диапазон")
				}
				step, _ := strconv.ParseInt(parts[4], 10, 64)
				*min, *max = clamp(*min), clamp(*max)
				if parts[3] == "min" {
					*min = clamp(*min + step)
					if *min > *max {
						*max = *min
					}
				} else {
					*max = clamp(*max + step)
					if *max < *min {
						*min = *max
					}
				}
			}
			return s.showFilters(ctx, owner, draft, field, "")
		case "set":
			if len(parts) != 5 {
				return errors.New("неверная кнопка фильтра")
			}
			f := draft.Values
			switch parts[3] {
			case "goal":
				f.Goal = parts[4]
			case "children":
				f.Children = parts[4]
			case "worldview":
				f.Worldview = parts[4]
			default:
				return errors.New("неизвестный фильтр")
			}
			if err := f.validate(); err != nil {
				return err
			}
			draft.Values, draft.Input = f, ""
			return s.showFilters(ctx, owner, draft, "", "")
		case "country":
			if len(parts) != 4 {
				return errors.New("неверная страна")
			}
			if parts[3] != "" {
				if _, ok := countryLabels[parts[3]]; !ok {
					return errors.New("страна отсутствует в справочнике Jardulik")
				}
			}
			draft.Values.Country, draft.Values.City = parts[3], ""
			draft.Input, draft.Cities = "", nil
			return s.showFilters(ctx, owner, draft, "", "")
		case "city":
			if len(parts) != 4 || parts[3] != "" {
				return errors.New("неверная кнопка города")
			}
			draft.Values.City, draft.Input, draft.Cities = "", "", nil
			return s.showFilters(ctx, owner, draft, "", "")
		case "pickcity":
			if len(parts) != 4 || draft.Input != "city" {
				return errors.New("неверная кнопка города")
			}
			i, err := strconv.Atoi(parts[3])
			if err != nil || i < 0 || i >= len(draft.Cities) {
				return errors.New("город отсутствует в результатах поиска")
			}
			draft.Values.City = draft.Cities[i].Name
			draft.Input, draft.Cities = "", nil
			return s.showFilters(ctx, owner, draft, "", "")
		default:
			return errors.New("неизвестная кнопка фильтра")
		}
	}
	if err := handle(); err != nil {
		_ = s.say(ctx, owner, "Не удалось изменить фильтры: "+err.Error(), nil)
	}
	return true
}

func filterSearch(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}

// FilterText consumes text only while this owner's geography picker is open.
func (s *Service) FilterText(ctx context.Context, owner int64, text string) bool {
	if strings.HasPrefix(strings.TrimSpace(text), "/") {
		return false
	}
	state, err := s.Store.filterState(ctx, owner)
	if err != nil || state.Draft == nil || state.Draft.Input == "" {
		return false
	}
	draft := state.Draft
	search := filterSearch(text)
	if search == "" || utf8.RuneCountInString(search) > 160 {
		_ = s.say(ctx, owner, "Введи короткое название страны или города.", nil)
		return true
	}
	if draft.Input == "country" {
		var countries []string
		for country, label := range countryLabels {
			if filterSearch(country) == search || filterSearch(label) == search {
				draft.Values.Country, draft.Values.City, draft.Input, draft.Cities = country, "", "", nil
				err = s.showFilters(ctx, owner, draft, "", "")
				if err != nil {
					_ = s.say(ctx, owner, err.Error(), nil)
				}
				return true
			}
			if strings.Contains(filterSearch(country), search) || strings.Contains(filterSearch(label), search) {
				countries = append(countries, country)
			}
		}
		sort.Strings(countries)
		notice := "Страна не найдена. Введи полное название из Jardulik."
		if len(countries) > 0 {
			var labels []string
			for _, country := range countries[:min(len(countries), 10)] {
				labels = append(labels, filterCountryLabel(country))
			}
			notice = "Уточни название: " + strings.Join(labels, ", ") + "."
		}
		err = s.showFilters(ctx, owner, draft, "country", notice)
	} else if draft.Input == "city" {
		var cities []filterCity
		cities, err = s.Client.filterCities(ctx, draft.Values.Country)
		if err != nil {
			err = s.showFilters(ctx, owner, draft, "city", err.Error())
		} else {
			draft.Cities = nil
			count := 0
			for _, city := range cities {
				if filterSearch(city.Name) == search || filterSearch(city.Russian) == search {
					draft.Values.City, draft.Input, draft.Cities = city.Name, "", nil
					err = s.showFilters(ctx, owner, draft, "", "")
					if err != nil {
						_ = s.say(ctx, owner, err.Error(), nil)
					}
					return true
				}
				if strings.Contains(filterSearch(city.Name), search) || strings.Contains(filterSearch(city.Russian), search) {
					count++
					if len(draft.Cities) < 10 {
						draft.Cities = append(draft.Cities, city)
					}
				}
			}
			notice := "Город не найден. Уточни название или выбери Все города."
			if count > 0 {
				notice = "Выбери город из справочника."
			}
			if count > 10 {
				notice = fmt.Sprintf("Найдено %d городов. Показаны первые 10; уточни название.", count)
			}
			err = s.showFilters(ctx, owner, draft, "city", notice)
		}
	}
	if err != nil {
		_ = s.say(ctx, owner, "Не удалось изменить фильтр: "+err.Error(), nil)
	}
	return true
}

// OfferOtherProfiles is called only after a successful empty discovery response.
func (s *Service) OfferOtherProfiles(ctx context.Context, owner int64) error {
	state, err := s.Store.filterState(ctx, owner)
	if err != nil {
		return err
	}
	if state.Others {
		return s.say(ctx, owner, "В этой выдаче больше новых анкет нет. Попробуй позже.", nil)
	}
	id, err := filterNonce()
	if err != nil {
		return err
	}
	_, err = s.Store.DB.ExecContext(ctx, "INSERT INTO feed_filters(owner_id,others_id) VALUES(?,?) ON CONFLICT(owner_id) DO UPDATE SET others_id=excluded.others_id", owner, id)
	if err != nil {
		return err
	}
	return s.say(ctx, owner, "По текущим условиям новых анкет не получено. Jardulik умеет дополнительно показывать остальных. Включить эту выдачу?", map[string]any{
		"inline_keyboard": [][]filterButton{{{"text": "Показать остальных", "callback_data": "flt:others:" + id}}},
	})
}

func (s *Service) acceptOtherProfiles(ctx context.Context, owner int64, id string, state filterState) error {
	if id == "" || id != state.OfferID {
		return errors.New("это предложение устарело; повтори /next")
	}
	if state.Applied != nil {
		if _, _, err := s.filterPremium(ctx, owner); err != nil {
			return err
		}
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "UPDATE feed_filters SET include_others=1,others_id='' WHERE owner_id=? AND others_id=?", owner, id)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return errors.New("это предложение устарело")
	}
	if _, err = tx.ExecContext(ctx, "UPDATE users SET feed_page=1 WHERE id=?", owner); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.say(ctx, owner, "Показ остальных включён. /next запросит новую пачку. Это не отправляет лайки.", nil)
}
