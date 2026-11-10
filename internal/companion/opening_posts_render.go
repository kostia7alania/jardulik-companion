package companion

import (
	"fmt"
	"strings"
)

func openingFormulaPayload(policy ReactionMessagePolicy) openingPostPayload {
	header := "💌 Как выбирается открывалка"
	body := fmt.Sprintf("Текущие настройки: mode=%s; prefer_drafts=%t; iron_mix=%t; @%s.\nАвто: %s. Настройки не запускают отправку.\n\n1. Перед реакцией проверяются возраст 18..111, текущие фильтры, активный hold и привязка аккаунта. Подтверждённый DOB младше 18 блокирует отправку.\n2. При birthday + prefer_drafts сначала ищется approved по owner/account/target/handle и strict8: name, age, height, description, profession, city, country, languages.\n3. Нет свежего совпадения: библиотека %d текстов; variant = cursor %% 12. Предпросмотр cursor не двигает. first берёт первый собственный шаблон, rotate выбирает по cursor. off не добавляет текста.\n4. Календарная тема приблизительна: около границы ±2 дня, при неизвестной, неверной или будущей дате выбирается neutral. Это не расчёт характера или натальной карты.\n5. Номинальные начала: 20.01 Водолей; 19.02 Рыбы; 21.03 Овен; 20.04 Телец; 21.05 Близнецы; 21.06 Рак; 23.07 Лев; 23.08 Дева; 23.09 Весы; 23.10 Скорпион; 22.11 Стрелец; 22.12 Козерог.\n6. Iron заменяет только известное начало приветствия на дӕ бон хорз. Остальные слова и handle сохраняются.\n7. После выбора проверяются UTF-8 и предел 500 UTF-16. Точный outgoing intent сохраняется до POST. Неизвестный результат не повторяется вслепую.\n\nЭтот тред показывает сохранённые заготовки, текущий предпросмотр и отдельные факты отправки. Новые GPT/LLM-запросы и реакции здесь не выполняются.", policy.Mode, policy.PreferDrafts, policy.IronMix, policy.TelegramUsername, positiveRuleLabel(policy.SuperBeforeAge), len(birthdayGreetingTemplates))
	prefix := header + "\n\n"
	return openingPostPayload{Text: prefix + body, Entities: []MessageEntity{{Type: "bold", Offset: 0, Length: textUnits(header)}, {Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(body)}}}
}

func openingRecipientPayload(profile Profile, id int64, link, status, message, rule string, evidence []reactionDraftEvidence) openingPostPayload {
	name := birthdayGreetingName(profile)
	if name == "" {
		name = fmt.Sprintf("Анкета #%d", id)
	}
	heading := name
	if age, known := autoInteger(profile["age"]); known && age > 0 && age <= 111 {
		heading += fmt.Sprintf(", %d", age)
	}
	prefix := "💌 " + heading + "\n" + status + "\n\n"
	payload := openingPostPayload{Text: prefix, Entities: []MessageEntity{{Type: "bold", Offset: 0, Length: textUnits("💌 " + heading)}, {Type: "text_link", Offset: textUnits("💌 "), Length: textUnits(heading), URL: link}}}
	if message != "" {
		payload.Text += message + "\n\n"
		payload.Entities = append(payload.Entities, MessageEntity{Type: "expandable_blockquote", Offset: textUnits(prefix), Length: textUnits(message)})
	}
	payload.Text += "Правило: " + rule
	for i, source := range evidence {
		if i == 2 {
			break
		}
		quote := strings.Join(strings.Fields(source.Quote), " ")
		quote, rest := telegramPrefix(quote, 180)
		if rest != "" {
			quote += "…"
		}
		payload.Text += "\nОснование: " + source.Field + ": " + quote
	}
	return payload
}
