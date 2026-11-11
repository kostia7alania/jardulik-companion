package companion

import "strconv"

// CopyTextButton supplies an explicit clipboard action where legacy code text
// is only a font and does not reliably copy on every Telegram client.
func profileCopyButton(id int64) map[string]any {
	if id <= 0 {
		return nil
	}
	text := strconv.FormatInt(id, 10)
	return map[string]any{"text": "📋 " + text, "copy_text": map[string]string{"text": text}}
}

func legacyProfileCopyMarkup(id int64) any {
	button := profileCopyButton(id)
	if button == nil {
		return nil
	}
	return map[string]any{"inline_keyboard": [][]map[string]any{{button}}}
}
