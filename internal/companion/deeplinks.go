package companion

import (
	"net/url"
	"strconv"
)

// Launch through the official bot so Telegram signs initData for Jardulik.
const jardulikAppURL = "https://t.me/jardulik_bot?startapp"

func jardulikAppButton() map[string]string {
	return map[string]string{"text": "↗️ Открыть Jardulik", "url": jardulikAppURL}
}

// The profile route is the clean URL emitted by Jardulik's own bot, without launch credentials.
func jardulikProfileURL(id int64) string {
	if id <= 0 {
		return ""
	}
	return "https://mini.jardulik.app/mutuals?targetUserId=" + strconv.FormatInt(id, 10) + "&noCheck=1"
}

func validJardulikProfileURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "mini.jardulik.app" || u.User != nil || u.Path != "/mutuals" || u.RawPath != "" || u.Fragment != "" || u.ForceQuery {
		return false
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 2 || len(query["targetUserId"]) != 1 || len(query["noCheck"]) != 1 || query.Get("noCheck") != "1" {
		return false
	}
	id, err := strconv.ParseInt(query.Get("targetUserId"), 10, 64)
	return err == nil && id > 0 && query.Get("targetUserId") == strconv.FormatInt(id, 10) && raw == jardulikProfileURL(id)
}
