# Аудит открывалок

`Service.RunOpeningPosts(ctx)` готовит отдельный служебный тред `💌 Открывашки` в `BackupChatID`. Интеграция в poster runtime и публикация выполняются отдельно. Worker не запускает лайкер, не отправляет реакции Jardulik и не вызывает GPT/LLM. Pin через Bot API не выполняется; закрепление тредов остаётся отдельным действием владельца в Telegram.

В тред попадают только данные `Config.OwnerID` и его текущего Jardulik `upstream_id`. Другие пользователи Companion, даже с тем же target ID, не публикуются. Перед каждым outbound, включая повтор после подтверждённого 429, повторно проверяются текущие owner/account/session generation и привязка Telegram topic/message. Секреты сессии и device metadata в текстах не выводятся. Callback-кнопок с экспортом сырья нет.

## Что показывается

- Один пост с текущими правилами: режим, приоритет approved, strict8 hash, календарная таблица и её приблизительность, neutral около границ, Iron-приветствие, предел 500 UTF-16 и журнал исходящего запроса.
- Один пост предпросмотра на получателя с approved заготовкой. Заголовок использует общий нейтральный helper карточки: значок пола, имя/фамилия, возраст/рост, знак зодиака и Premium. Весь заголовок ведёт в существующий архивный тред; в этом персональном аудите к нему добавляется сохранённый процент совместимости текущего владельца, если он известен. Статус, буквальный выбранный текст, правило и до двух коротких source quotes целиком находятся в одной свёрнутой цитате. Если hold/возраст/выключенный режим блокируют выбор, сообщение не выдаётся за готовую отправку.
- Отдельные факты отправки из `reaction_message_intents` и `actions`: подтверждённый API успех или явно неизвестный исход. Текст буквально берётся из frozen `request_body` и сверяется с `message`, type и identity; текущий Iron или новый handle его не переписывают.
- Старые unlinked debug SuperLike учитываются только при настоящем `received`, HTTP 200 и успешном JSON-результате. Target 19587 не зашит в код и не считается отправленным без сохранённого подтверждения. Linked debug rows не дублируют intent.

Ссылка получателя явно задаёт forum thread: `https://t.me/c/<chat>/<thread>/<thread>`. При обновлении старого URL меняется только отображаемый payload. Worker исправляет уже опубликованный пост по прежнему message ID; текст открывалки, исходные bytes и source hash не меняются.

Процент читается из сохранённых `profile_analyses` внутри существующей SQL-транзакции, без нового GPT или запроса анкеты. Проверяются owner, текущий upstream и ID обоих снимков; используется общий разбор процента. Для доступной анкеты применяется последний успешный ответ: его неоднозначное число не заменяется старым. Если анкета сейчас недоступна, допускается последний прежний успешный ответ с читаемым процентом. Известный `0%` сохраняется. Добавление процента и изменение оформления не входят в source snapshot и не меняют его hash. Непомещающийся заголовок откладывает только его получателя, а не весь тред.

Предпросмотр использует те же read-only helpers, что выбор сообщения: `reactionDraftMessageTx`, `birthdayReactionMessage` и `ironMixReactionMessage`. `preparePositiveMessage` не вызывается. Cursor, actions, intents, оригинальные drafts, review и frozen requests не меняются. Предпросмотр не обещает прохождения всех текущих фильтров и разрешений авто; они проверяются обычным dispatch независимо.

Evidence проверяется через `parseReactionDraft`, `capture_sha`, strict8 и owner/account/handle binding. Целиком provenance, сведения об устройствах и имена неподтверждённых моделей не публикуются. Исходные bytes остаются в исходных таблицах; source snapshot предпросмотра сохраняет точные capture bytes, текущую strict8-базу, DOB для локальной проверки и использованный policy. Для fallback архивная заготовка подписывается отдельно от фактического текущего выбора. Подтверждение API Jardulik не означает, что получатель прочитал сообщение или получил его в Telegram.

## Хранение и доставка

Две новые таблицы не меняют схему profile indexes:

- `opening_audit_topics`: `id,owner_id,upstream_id,generation,chat_id,thread_id,state,next_attempt,next_refresh,receipt_json,error,updated_at`. Уникальный topic binding: owner/upstream/chat.
- `opening_audit_posts`: `topic_id,item_key,kind,profile_id,message_id,state,payload_json,desired_sha,sent_sha,source_json,source_sha,receipt_json,next_attempt,error,updated_at`. Ключ: topic/item_key. Kind: formula/draft/intent/debug.

`item_key` имеет вид `formula`, `draft:<profile_id>`, `intent:<action_id>` или `debug:<receipt_id>`. План обновляется раз в минуту; изменение текста редактирует только подтверждённый message ID. Сокращение набора не удаляет сообщения автоматически.

Новая публикация и edit используют общие `Archiver.mu` и `groupCall` с существующим групповым интервалом. `opening-posts.lock` исключает второго worker на одном компьютере. При создании topic проверяется, что возвращённый ID не совпал с общим backup thread, profile thread или известным utility topic. Для сообщения нужны точные chat/thread/message IDs. Полный rendered text и Telegram entities проверяются на 4096 UTF-16.

Подтверждённый429 допускает повтор с новым binding check. Неизвестный результат нового `createForumTopic` или `sendMessage` переходит в `uncertain` и вслепую не повторяется, даже после изменения source или рестарта. Для известного message ID сетевой сбой edit допускает повтор; `message is not modified` принимается только для этого ID. Ошибочная квитанция не подтверждает публикацию. Receipt сохраняется с отдельным контекстом после отмены исходного запроса.

При рестарте preparing возвращается в pending, sending без message ID становится uncertain. Известный edit возвращается в pending. Оператор сверяет uncertain с Telegram вручную до каких-либо исправлений receipt. Worker ничего не удаляет и не создаёт замену потерянному известному сообщению автоматически.

Opening audit использует уже сохранённые данные и не зависит от sync-cycle publication barrier для новых profile/GPT-пакетов. Отсутствующий recipient thread или повреждённый источник переводит только конкретный ранее staged item в `error/source_unavailable`. После восстановления источника план может вернуть его в работу. Пост правил и остальные получатели не ждут этот item.

## Подключение и проверка

Root runtime вызывает `RunOpeningPosts` в poster role. Обе таблицы должны быть включены в полный backup/restore перед live-включением; сами исходные drafts/intents/debug receipts уже остаются в своих существующих таблицах. `ArchivePaused` или несовпадение operator/chat конфигурации отключает worker.

Локальные тесты используют только fake Telegram transport. Они проверяют owner isolation, current preview и Iron без cursor mutation, буквальный frozen intent, настоящий legacy receipt, неизвестную новую отправку, безопасное редактирование известного ID и guard изменения generation/profile/handle перед outbound. Live-публикация этим изменением не подтверждается.
