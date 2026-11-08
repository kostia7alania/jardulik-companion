# Релиз с Telegram transfer helper

Основной модуль остаётся Go + SQLite. MTProto-зависимости находятся только в отдельном модуле `tools/telegram-transfer`; его бинарник называется `telegram-transfer`.

CI отдельно выполняет `go test -race ./...` и `go vet ./...` в обоих модулях. Обе программы собираются вне VPS с `CGO_ENABLED=0 GOOS=linux GOARCH=amd64`. Версии checkout/setup-go закреплены существующими SHA; версия Go берётся из корневого `go.mod`, helper использует ту же версию.

`scripts/deploy.sh` требует чистый опубликованный `main`, выполняет проверки обоих модулей и упаковывает только `jardulik`, `telegram-transfer` и явный набор deployment-файлов. Ключи, SQLite, фотографии, API-пара и файл сессии в архив релиза не включаются.

`deploy/install.sh` получает SHA-256 обеих программ. Отсутствующая программа, symlink или неверный хэш останавливают установку. Резерв свободного диска учитывает размер обеих программ. Сначала обе программы копируются и проверяются во временном каталоге релиза; затем каталог атомарно получает имя commit SHA. `SHA256SUMS` содержит две строки.

Расположение после активации:

```text
/opt/jardulik-companion/releases/<commit>/jardulik
/opt/jardulik-companion/releases/<commit>/telegram-transfer
/opt/jardulik-companion/releases/<commit>/SHA256SUMS
/opt/jardulik-companion/current -> releases/<commit>
/opt/jardulik-companion/previous -> releases/<previous-commit>
```

Обе программы переключаются одним `current` symlink. Ошибка запуска основного сервиса возвращает всю предыдущую папку, поэтому новая и старая бинарки разных релизов не смешиваются. Для legacy-релиза до появления helper нормально отсутствие `telegram-transfer`: его старый основной процесс helper не использует. Новый `backup-daily` в старом бинарнике отсутствует и завершится ошибкой, не заменяя полную копию снимком только метаданных; после такого отката оператор отдельно проверяет совместимость backup unit. Автоматическая ротация старых релизов и Telegram-документов не добавляется.

Настройки остаются вне Git в защищённом environment file:

- `TELEGRAM_TRANSFER_HELPER` по умолчанию `/opt/jardulik-companion/current/telegram-transfer`.
- `TELEGRAM_TRANSFER_SESSION_FILE` по умолчанию `APP_DATA_DIR/telegram-bot-session.enc`.
- `TELEGRAM_API_ID` и `TELEGRAM_API_HASH` задаются парой; значения и bot token не передаются аргументами командной строки.
- Сессионный файл helper шифруется отдельным контекстом существующего `SESSION_KEY`.

Helper запускается только для отдельной передачи, не создаёт daemon или локальный Bot API сервер. Наличие и SHA бинарника не доказывают успешный MTProto-вход или отправку: для этого нужна отдельная согласованная живая проверка с настроенной API-парой.

`MemoryMax=128M` основного сервиса не меняется. Если helper запущен дочерним процессом, он разделяет cgroup-лимит родителя. Перед приёмкой измеряется совокупный `memory.peak` основного процесса и helper; отдельный маленький RSS бота не является проверкой общей памяти. Сборка helper на VPS не выполняется.

Локальная проверка скриптов без сервера:

```sh
bash -n deploy/install.sh scripts/deploy.sh
python3 scripts/test-deploy.py
```

Mock-проверки покрывают отказ для dirty main и непроверенного staging path, отдельные проверки nested module, остановку при ошибке helper build, отсутствие секретных файлов в пакете, проверку двух SHA и возврат полной пары при неудачном запуске. Запуск этих проверок не выполняет SSH, systemd или Telegram API в реальной среде.
