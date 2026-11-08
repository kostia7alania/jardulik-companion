# Telegram backup transfer

One-shot bot-only MTProto transport for whole encrypted Jardulik backups. The
main application keeps using the cloud Bot API. This helper never calls
`getUpdates`, `logOut`, QR/code authentication or user-session import.

The nested module pins `github.com/gotd/td v0.163.0`; the application's root
module retains its existing dependencies. Build outside the small VPS:

```sh
go test -race ./...
go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o telegram-transfer .
```

Protected environment, outside Git:

- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`: the owner's existing Telegram API app.
- `TELEGRAM_BOT_TOKEN`: the same bot used by the main application.
- `SESSION_KEY`: existing base64-encoded 32-byte key.
- `APP_DATA_DIR`: absolute private directory, mode 0700.
- `BACKUP_CHAT_ID`: allowlisted forum supergroup Bot API ID.
- `BACKUP_THREAD_ID`: allowlisted topic ID, including 1 for General.
- Optional `TELEGRAM_TRANSFER_SESSION_FILE`: path inside `APP_DATA_DIR`;
  default `telegram-bot-session.enc`.
- Optional `TELEGRAM_TRANSFER_BYTES_PER_SECOND`: upload bandwidth budget;
  `0` or unset means unlimited. `1048576` allows 1 MiB/s. Values range from
  0 to 1 GiB/s; settings that cannot fit the file within the 25-minute deadline
  are rejected before authorization. Downloads are unaffected.

No credential or destination override is accepted on the command line. The
helper encrypts its bot session using a domain-separated AES-256-GCM key,
binds it to bot ID and API app ID, writes it atomically as 0600, and locks it
against concurrent helper processes. Session decryption or identity mismatch
fails closed. This session is a bot authorization, not a Telegram user account.

The caller must durably save a unique signed nonzero `random_id` and the exact
encrypted file SHA-256 before starting an upload. Reuse that ID only for this
same operation and file; never assign a new ID to an uncertain retry.

```text
telegram-transfer upload --file /private/path/snapshot.zip.enc --random-id 123 --sha256 <encrypted-file-sha256> --size <bytes>
telegram-transfer download --receipt /private/path/receipt.json --output /private/path/new-copy.zip.enc --sha256 <encrypted-file-sha256> --size <bytes>
```

All paths must be absolute. Files are limited to 1 GiB. Transfers have a
25-minute deadline and use one upload worker with 512 KiB parts. Upload verifies
the file before transferring and hashes the actual streamed bytes before
`messages.sendMedia`. Download re-fetches the document by channel/message ID,
so an expired stored `file_reference` is not reused, then streams to a private
temporary file, checks exact size/SHA-256 and publishes without overwriting.

Stdout is one JSON object, at most 64 KiB:

```json
{"status":"sent","transport":"mtproto","chat_id":-1000000000042,"thread_id":9,"message_id":123,"document_id":456,"byte_size":60000000,"source_sha256":"<encrypted-file-sha256>","random_id":789}
```

IDs are signed JSON integers. Successful upload status is `sent`, after fresh
readback confirms bot sender, target, topic and document size. Download returns
`downloaded` with the same locator after checksum verification. This does not
claim Telegram server-byte verification of an upload. Bot API `file_id` and
`file_unique_id` are omitted until a separate compatibility check verifies them;
neither is invented from a temporary upload ID.

Failures return `failed` or `uncertain`, an allowlisted `error_code`, and optional
`retry_after` seconds. Any error after starting `sendMedia`, including a failed
readback, is `uncertain`: the caller must not blindly repeat it or mark sources
clean. Raw errors, tokens, API hashes, auth keys and source paths are not printed.

`NoUpdates: true` is mandatory. In the pinned SDK this selects
`ConnModeData`, which wraps RPC calls in `invokeWithoutUpdates`. Cloud-poller
coexistence, real bot authorization, actual upload/download and VPS memory still
require an authorized live acceptance; unit tests make no Telegram calls.
