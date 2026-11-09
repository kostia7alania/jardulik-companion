package companion

// Launch through the official bot so Telegram signs initData for Jardulik.
const jardulikAppURL = "https://t.me/jardulik_bot?startapp"

func jardulikAppButton() map[string]string {
	return map[string]string{"text": "↗️ Открыть Jardulik", "url": jardulikAppURL}
}
