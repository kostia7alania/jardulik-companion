package companion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const broadCrawlAgeMin int64 = 0
const broadCrawlAgeMax int64 = 111

// UsersUnfilteredFrozen collects with broad age bounds and no mutable account preferences or other UI filters.
func (c *Client) UsersUnfilteredFrozen(ctx context.Context, owner int64, expected Session, page int, others bool) ([]Profile, error) {
	if page < 1 {
		return nil, errors.New("invalid discovery page")
	}
	if err := c.crawlPremium(ctx, owner, expected); err != nil {
		return nil, err
	}
	query := url.Values{
		"userId":     {strconv.FormatInt(expected.UserID, 10)},
		"pageNumber": {strconv.Itoa(page)},
		"pageSize":   {"150"},
		"all":        {"0"},
		"ageMin":     {strconv.FormatInt(broadCrawlAgeMin, 10)},
		"ageMax":     {strconv.FormatInt(broadCrawlAgeMax, 10)},
	}
	if others {
		query.Set("all", "1")
	}
	raw, err := c.requestSession(ctx, owner, http.MethodGet, "/user/getUsers?"+query.Encode(), nil, &expected)
	if err != nil {
		return nil, err
	}
	current, err := c.Store.Session(ctx, owner)
	if err != nil || current.State != "connected" || current.Generation != expected.Generation || current.UserID != expected.UserID {
		return nil, errors.New("сессия изменилась; старый ответ отменён")
	}
	if c.Store.Paused(ctx, owner) {
		return nil, errors.New("аккаунт на паузе; обход остановлен")
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, errors.New("Jardulik profile list format changed")
	}
	var profiles []Profile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&profiles); err != nil {
		return nil, errors.New("Jardulik profile list format changed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("Jardulik profile list has trailing data")
	}
	for _, profile := range profiles {
		rawID, exists := profile["userId"]
		if !exists {
			rawID = profile["id"]
		}
		id, err := strconv.ParseInt(valueString(rawID), 10, 64)
		if profile == nil || err != nil || id <= 0 {
			return nil, errors.New("Jardulik profile list has no valid profile ID")
		}
	}
	return profiles, nil
}
