package companion

import "strings"

// Only a known opening greeting changes; the shown name, Russian body and CTA stay exact.
func ironMixReactionMessage(message, name string) string {
	if strings.HasPrefix(message, "Привет!") {
		return "Дӕ бон хорз!" + message[len("Привет!"):]
	}
	if name == "" {
		return message
	}
	for _, ending := range []string{", привет!", ", приветик!", ", саламик)"} {
		shownName, tail, found := strings.Cut(message, ending)
		if found && strings.EqualFold(shownName, name) {
			return shownName + ", дӕ бон хорз!" + tail
		}
	}
	if strings.HasPrefix(message, "Привет, ") {
		shownName, tail, found := strings.Cut(message[len("Привет, "):], "!")
		if found && strings.EqualFold(shownName, name) {
			return shownName + ", дӕ бон хорз!" + tail
		}
	}
	return message
}
