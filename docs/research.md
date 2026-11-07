# Исследование Jardulik Companion

Состояние источников: 8 ноября 2026. Это исследование реализации и позиционирования; наличие кода не доказывает рабочую интеграцию или отсутствие серверного обнаружения.

## Jardulik

[Публичная frontend-сборка](https://mini.jardulik.app/assets/index-BFeBP-cW.js), SHA-256 `b8e7b1a5947c75272dcb5da9a882e577396b9650ad0bd4e6a837bd963e217fe1`, содержит обмен `initData` на `token/refreshToken`, Bearer-запросы и `PUT /appuser/refresh`. Проверенный JWT имел `exp - nbf = 3600 секунд`; срок refresh сервер в просмотренных ответах не раскрывал. У Telegram initData нет универсального TTL: backend выбирает допустимый возраст.

Основные пути: `/appuser/login/telegram`, `/appuser/refresh`, `/user/getMe`, `/user/getUsers`, `/user/getUser`, `/reactions/addNew`. В сборке не найдены известные строки CAPTCHA/fingerprint/`webdriver` и device headers в основном API-клиенте. Это ограниченный статический результат, а не доказательство отсутствия защиты на backend.

[Соглашение Jardulik](https://www.jardulik.app/agreement) описывает приглашение разработчиков армянского приложения. В сборке осталась ссылка `jerm.am`. Есть подтверждённая связь разработки с армянским проектом, но выпуск одного white-label продукта для множества этносов не установлен. Публичный исходный репозиторий не найден; проверенный URL source map возвращает HTML.

## Как другие получают доступ

1. [RockyRabbitBot: ручной initData](https://github.com/Freddywhest/RockyRabbitBot/blob/main/AddQueryId.md): пользователь извлекает `tgWebAppData` из iframe Telegram Web. Это путь без пароля, OTP и MTProto-сессии.
2. [BlumBot](https://github.com/Freddywhest/BlumBot/blob/5db989142121726ba1a620792cbda5efb4815544/bot/core/tapper.js#L120): GramJS session и `RequestWebView` для получения свежих данных. Код исторический, текущая работоспособность не проверялась.
3. [telegram-blum-auto](https://github.com/flaming-chameleon/telegram-blum-auto/blob/edcf44062a6211ded6988c718639f45d8b73f833/utils/blum.py#L233): пользовательская Pyrogram session, WebView URL, HTTP login и app refresh. Небезопасные настройки вроде `ssl=False` не заимствуются.
4. [MRKT-API](https://github.com/boostNT/MRKT-API/blob/580378a250a1f18fa75b7c31c4168e908e68fafc/README.md): ручной токен либо `RequestAppWebView` через пользовательскую сессию. Чужой заявленный TTL не переносится на Jardulik.
5. [Go auth example](https://github.com/mikiasyonas/Sample-Telegram-Mini-App-Backend/blob/305f114f88696977792be4572196f1887988f841/internal/handlers/auth.go#L31): проверка initData и выпуск отдельной app-сессии.

Наш бот использует ручное одноразовое подключение и refresh Jardulik. Созданный нами Mini App получил бы подпись для нашего bot ID и не смог бы авторизовать посетителя в Jardulik. Автоматическое получение новых чужих launch URL обычно требует пользовательской Telegram-сессии; собирать её в этой версии не требуется.

Основа проверки: [Telegram Ed25519 third-party validation](https://core.telegram.org/bots/webapps#validating-data-for-third-party-use). Доверенный ID источника закреплён отдельно, подтверждён данными Telegram и проверкой реального подписанного пакета. URL пользователя не загружается, повторяющиеся query keys отвергаются, raw JSON пользователя перед проверкой не пересериализуется.

## Рынок и семантика

Проверены шесть первичных сайтов:

| Сервис | Наблюдаемое позиционирование |
| --- | --- |
| [Jardulik](https://jardulik.app/) | Осетинские знакомства в Telegram |
| [Jerm](https://jerm.am/) | Telegram-знакомства армян по миру |
| [DateMe](https://dateme.am/) | Проверенные профили, discovery, мэтчи и чат |
| [Flirtu](https://flirtu.io/) | Знакомства внутри мессенджера |
| [Auto Swiper](https://auto-swiper.ch/) | Автоматизация свайпов в браузере и Android |
| [ChatMate](https://chatmate.cc/) | Автоматизация аккаунтов, переписок и свайпов |

На просмотренных страницах долговременный версионный архив всех возвращённых анкет Jardulik не выделяется как отдельный продукт. Это не доказательство отсутствия аналогов на рынке. У Auto Swiper и ChatMate не подтверждена интеграция с Jardulik.

Первичный кластер: Jardulik + анкеты/архив/история в Telegram. Широкий запрос про осетинские знакомства обычно предполагает самостоятельный dating-сервис; лендинг сразу обозначает Companion как независимый помощник.

### Что измерено и что неизвестно

Подключённых Keyword Planner, Wordstat, Ahrefs или Search Console нет. Месячная частотность, KD, CPC и трафик не подтверждены; поля в CSV оставлены пустыми, не нулевыми.

Google Autocomplete с `client=firefox&hl=ru` вернул пустые подсказки для пяти seeds: jardulik, жардулик, осетинские знакомства, автолайки телеграм знакомства, сохранить анкету знакомства. Контрольные погода, знакомства и telegram вернули по 10 подсказок. Это проверка доступного autocomplete, не измерение частотности и не доказательство нулевого спроса.

[Google Trends, Россия, 12 месяцев](https://trends.google.com/trends/explore?date=today%2012-m&geo=RU&q=Jardulik&hl=ru) не предоставил извлекаемый числовой ряд. [Trends нормализует относительный интерес](https://support.google.com/trends/answer/4365533?hl=ru), а не число поисков. После подтверждения собственности на сайт [Search Console](https://developers.google.com/search/docs/monitor-debug/search-console-start) сможет дать реальные показы и клики; сейчас этот источник не подключён.

Семантические кандидаты и их продуктовый приоритет находятся в `keywords.csv`. Приоритет отражает соответствие функции, не доказанный поисковый объём.

## Маркетинг и показатели

- Основная страница объясняет карточки, историю, ручное подключение и отдельную подписку Jardulik. Демонстрация полностью вымышленная.
- Одно содержательное руководство подключения вместо страниц под каждую вариацию ключа.
- В бот deep links добавляется общий источник `site`, `guide_connect` или `community`. Не называем источник `google`, если он не установлен.
- `/stats` измеряет уникальные start, успешные connect и первую подтверждённую доставку карточки. Это не посещения сайта и не прочтения человеком.
- Никаких внешних рекламных трекеров, расходов на рекламу или сообщений в сообщества без отдельного поручения.

## Telegram-архив

[Официальные forum docs](https://core.telegram.org/api/forum) не подтверждают заявленный предел в миллион тем. [createForumTopic](https://core.telegram.org/bots/api/methods/createforumtopic.md) требует `can_manage_topics`. [Bot FAQ](https://core.telegram.org/bots/faq) указывает лимиты всей группы, а не каждой темы. Архив использует одну очередь с интервалом не менее четырёх секунд и учитывает `retry_after`.

Cloud Bot API отправляет документы до 50 MB, но [getFile](https://core.telegram.org/bots/api/methods/getfile.md) скачивает только до 20 MB. Наш служебный ZIP ограничен 18 МиБ и проходит те же бюджеты, что restore. Большие целые документы требуют отдельного пути восстановления, а не скрытого дробления. [deleteMessage](https://core.telegram.org/bots/api/methods/deletemessage.md) обычно ограничен 48 часами, поэтому автоматическое удаление старых архивов не обещается.
