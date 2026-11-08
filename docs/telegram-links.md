# Telegram-ссылки после взаимности

Проверено 9.11.2026 через аккаунт владельца в официальном @jardulik_bot и по публичной сборке index-BFeBP-cW.js.

1. Экран взаимностей получает список через GET /user/getReactUsers с isMutual=true.
2. Кнопка Telegram вызывает POST /telegram/getTG с внутренними fromUserId и targetUserId Jardulik. Это действие запрашивает отправку карточки, а не обычное чтение JSON анкеты.
3. Frontend не использует ответ как контактный URL: после запроса он закрывает Mini App. Контактная карточка видна в личном диалоге с официальным ботом.
4. Посмотреть анкету является web_app-кнопкой с URL вида https://mini.jardulik.app/mutuals?targetUserId=<id>&noCheck=1. Telegram добавляет свежие подписанные launch-параметры. noCheck=1 не доказывает взаимность; для маршрута mutuals frontend его не использует.
5. Открыть в Telegram содержит отдельную обычную ссылку https://t.me/<username>. Проверенный переход открывает личный диалог.

Связь ID Jardulik с Telegram numeric ID не выводится из номера анкеты или username. Шаблон start/startapp для произвольной анкеты в просмотренной сборке не подтверждён. Кнопка запуска от другого бота не эквивалентна запуску от официального Jardulik: подпись launch-данных относится к боту запуска.

## Хранение

profile_contact_evidence сохраняет контакт, точный URL исходной web_app-кнопки без launch-фрагмента, аккаунт Jardulik, профиль и locator исходного сообщения. Тип official_bot_ui означает операторское наблюдение в интерфейсе, а не криптографически проверенный ответ Jardulik API. data-mid хранится как непрозрачный DOM locator и не объявляется серверным Telegram message_id.

Наблюдение импортируется JSON через stdin:

```sh
jardulik import-contact < evidence.json
```

Команда доступна только настроенному оператору и проверяет текущий upstream аккаунт, доверенный source bot, URL и привязку профиля. Сетевых запросов она не делает. Повторный импорт идемпотентен; изменение URL сохраняется отдельной записью. Контакт показывается отдельной кнопкой в личке, только для соответствующего владельца и текущего аккаунта. Исходный JSON анкеты не меняется. Новые контактные данные включены в fingerprint суточного бэкапа.

Автоматического чтения личного диалога пользователя через Bot API здесь нет. Существующий MTProto helper авторизован как наш бот, не как пользователь. Импорт наблюдения не создаёт пользовательскую Telegram-сессию и не включает фоновый сбор контактов.

Источники: [публичная сборка](https://mini.jardulik.app/assets/index-BFeBP-cW.js), [bot deep links](https://core.telegram.org/bots/features#deep-linking), [direct Mini App links](https://core.telegram.org/bots/webapps#direct-link-mini-apps), [username links](https://core.telegram.org/api/links#public-username-links).
