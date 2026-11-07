package companion

import "net/http"

// A stable reduced Android browser profile. Operators can supply a measured
// Telegram WebView UA; a header change does not reproduce Chromium's TLS stack.
const defaultUserAgent = "Mozilla/5.0 (Linux; Android 16; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Mobile Safari/537.36 Telegram-Android/13.2.1 (Vivo V2514; Android 16; SDK 36; HIGH)"

func upstreamHeaders(r *http.Request, ua string, image bool) {
	if ua == "" {
		ua = defaultUserAgent
	}
	r.Header.Set("User-Agent", ua)
	r.Header.Set("Accept-Language", "ru-RU,ru;q=0.9,en;q=0.8")
	r.Header.Set("Referer", "https://mini.jardulik.app/")
	r.Header.Set("Sec-Fetch-Site", "same-site")
	if image {
		r.Header.Set("Accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
		r.Header.Set("Sec-Fetch-Mode", "no-cors")
		r.Header.Set("Sec-Fetch-Dest", "image")
		if r.URL.Hostname() == "mini.jardulik.app" {
			r.Header.Set("Sec-Fetch-Site", "same-origin")
		}
	} else {
		r.Header.Set("Accept", "application/json, text/plain, */*")
		r.Header.Set("Origin", "https://mini.jardulik.app")
		r.Header.Set("Sec-Fetch-Mode", "cors")
		r.Header.Set("Sec-Fetch-Dest", "empty")
	}
}
