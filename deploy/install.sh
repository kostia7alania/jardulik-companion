#!/usr/bin/env bash
set -Eeuo pipefail

die() { printf 'install: %s\n' "$*" >&2; exit 1; }

[[ $# -eq 3 ]] || die 'usage: install.sh <40-character-commit> <unpacked-release-directory> <binary-sha256>'
revision=$1
source_dir=$2
binary_sha=$3
[[ $revision =~ ^[0-9a-f]{40}$ ]] || die 'invalid commit SHA'
[[ $binary_sha =~ ^[0-9a-f]{64}$ ]] || die 'invalid binary SHA-256'
[[ -f $source_dir/jardulik && ! -L $source_dir/jardulik ]] || die 'release binary is missing or is a symlink'
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || die 'the release requires Linux x86_64'
[[ $EUID -eq 0 ]] || die 'run the installer as root'

for command in sha256sum systemctl systemd-run systemd-analyze useradd install mktemp flock python3 df stat awk; do
    command -v "$command" >/dev/null || die "required command is unavailable: $command"
done

base=/opt/jardulik-companion
data_dir=/var/lib/jardulik-companion
env_file=/etc/jardulik-companion.env
unit_names=(jardulik-companion.service jardulik-companion-backup.service jardulik-companion-backup.timer)

# Validate secrets and capacity before creating users, changing releases or restarting.
[[ -f $env_file && ! -L $env_file ]] || die "create $env_file before deploying"
[[ $(stat -c '%u:%a' "$env_file") == 0:600 ]] || die "$env_file must be root-owned with mode 0600"
python3 - "$env_file" <<'PY'
import base64, pathlib, re, sys
values = {}
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    key, sep, value = line.partition('=')
    if not sep or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
        raise SystemExit('install: environment file must contain plain KEY=value assignments')
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    if key in values:
        raise SystemExit('install: duplicate environment key: ' + key)
    values[key] = value
required = ('APP_DATA_DIR', 'SESSION_KEY', 'BACKUP_KEY', 'OWNER_TELEGRAM_ID',
            'TELEGRAM_BOT_TOKEN', 'JARDULIK_BOT_ID', 'BACKUP_CHAT_ID')
for key in required:
    if not values.get(key):
        raise SystemExit('install: required environment value is missing: ' + key)
for key in ('SESSION_KEY', 'BACKUP_KEY'):
    try:
        valid = len(base64.b64decode(values[key], validate=True)) == 32
    except ValueError:
        valid = False
    if not valid:
        raise SystemExit('install: ' + key + ' must be 32 bytes encoded as base64')
for key in ('OWNER_TELEGRAM_ID', 'JARDULIK_BOT_ID'):
    if not re.fullmatch(r'[1-9][0-9]*', values[key]):
        raise SystemExit('install: ' + key + ' must be a positive numeric ID')
if not re.fullmatch(r'-[1-9][0-9]*', values['BACKUP_CHAT_ID']):
    raise SystemExit('install: BACKUP_CHAT_ID must identify the configured private backup group')
if values.get('BACKUP_THREAD_ID') and not re.fullmatch(r'[1-9][0-9]*', values['BACKUP_THREAD_ID']):
    raise SystemExit('install: BACKUP_THREAD_ID must be a positive numeric ID')
if not re.fullmatch(r'[1-9][0-9]*:[A-Za-z0-9_-]+', values['TELEGRAM_BOT_TOKEN']):
    raise SystemExit('install: TELEGRAM_BOT_TOKEN has an invalid format')
if values['APP_DATA_DIR'] != '/var/lib/jardulik-companion':
    raise SystemExit('install: APP_DATA_DIR does not match the service data directory')
if values.get('ALLOW_REACTIONS', 'false').lower() != 'false':
    raise SystemExit('install: reactions must remain disabled for this deployment')
reserve = values.get('MIN_FREE_BYTES', '2147483648')
if not reserve.isdigit() or int(reserve) < 2147483648:
    raise SystemExit('install: MIN_FREE_BYTES must preserve at least 2 GiB')
PY

available_bytes=$(df -PB1 /opt | awk 'NR == 2 { print $4 }')
release_bytes=$(stat -c '%s' "$source_dir/jardulik")
reserve_bytes=$(awk -F= '/^[[:space:]]*MIN_FREE_BYTES=/ { value=$2; gsub(/[^0-9]/, "", value); found=1 } END { if (found) print value; else print "2147483648" }' "$env_file")
[[ $available_bytes =~ ^[0-9]+$ ]] || die 'could not read available disk space'
(( available_bytes >= reserve_bytes + release_bytes * 2 + 1048576 )) || die 'insufficient disk space to preserve the configured reserve'
printf '%s  %s\n' "$binary_sha" "$source_dir/jardulik" | sha256sum --check --status || die 'binary checksum mismatch'
for unit in "${unit_names[@]}"; do
    [[ -f $source_dir/deploy/$unit && ! -L $source_dir/deploy/$unit ]] || die "release unit is missing: $unit"
done

exec 9>/run/lock/jardulik-companion-deploy.lock
flock -n 9 || die 'another Jardulik deployment is running'

if id jardulik >/dev/null 2>&1; then
    [[ $(id -u jardulik) -lt 1000 && $(id -gn jardulik) == jardulik ]] || die 'the existing jardulik account is not the dedicated service account'
else
    useradd --system --user-group --home-dir "$data_dir" --no-create-home --shell /usr/sbin/nologin jardulik
fi
[[ ! -L $base && ! -L $data_dir ]] || die 'application directories must not be symlinks'
install -d -o root -g root -m 0755 "$base" "$base/releases"
install -d -o jardulik -g jardulik -m 0700 "$data_dir"

previous_target=''
if [[ -L $base/current ]]; then
    previous_target=$(readlink -f "$base/current")
    [[ $previous_target =~ ^/opt/jardulik-companion/releases/[0-9a-f]{40}$ && -x $previous_target/jardulik ]] || die 'the current release symlink has an unexpected target'
elif [[ -e $base/current ]]; then
    die 'the current release path is not a symlink'
fi

release_dir=$base/releases/$revision
candidate=''
cleanup_candidate() {
    if [[ -n $candidate && -d $candidate ]]; then rm -rf -- "$candidate"; fi
}
trap cleanup_candidate EXIT
if [[ -e $release_dir ]]; then
    [[ -d $release_dir && ! -L $release_dir ]] || die 'release path is not a regular directory'
    printf '%s  %s\n' "$binary_sha" "$release_dir/jardulik" | sha256sum --check --status || die 'the existing release has a different checksum'
else
    candidate=$(mktemp -d "$base/releases/.$revision.XXXXXX")
    install -o root -g root -m 0755 "$source_dir/jardulik" "$candidate/jardulik"
    printf '%s\n' "$revision" > "$candidate/VERSION"
    printf '%s  jardulik\n' "$binary_sha" > "$candidate/SHA256SUMS"
    chmod 0755 "$candidate"
    mv -T -- "$candidate" "$release_dir"
    candidate=''
fi

for unit in "${unit_names[@]}"; do
    install -o root -g root -m 0644 "$source_dir/deploy/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload

switched=false
rollback() {
    code=$?
    trap - ERR
    if $switched; then
        printf 'install: activation failed; restoring the previous release\n' >&2
        if [[ -n $previous_target ]]; then
            ln -s "$previous_target" "$base/.current.rollback.$$"
            mv -Tf -- "$base/.current.rollback.$$" "$base/current"
            if ! systemctl restart jardulik-companion.service; then
                printf 'install: previous release restored, but its service restart failed\n' >&2
            fi
        else
            systemctl disable --now jardulik-companion.service jardulik-companion-backup.timer || true
            rm -f -- "$base/current"
        fi
    fi
    exit "$code"
}
trap rollback ERR
ln -s "$release_dir" "$base/.current.$$"
mv -Tf -- "$base/.current.$$" "$base/current"
switched=true
systemd-analyze verify "${unit_names[@]/#/$source_dir/deploy/}"
systemctl restart jardulik-companion.service
sleep 2
systemctl is-active --quiet jardulik-companion.service
systemd-run --quiet --wait --pipe --collect \
    --unit="jardulik-companion-health-$$" \
    --property=User=jardulik --property=Group=jardulik \
    --property="EnvironmentFile=$env_file" \
    --property="WorkingDirectory=$data_dir" \
    --property=RuntimeMaxSec=30 \
    --property=MemoryMax=128M --property=TasksMax=32 \
    --setenv=GOMEMLIMIT=64MiB --setenv=GOMAXPROCS=1 \
    "$base/current/jardulik" status >/dev/null
systemctl is-active --quiet jardulik-companion.service
systemctl enable jardulik-companion.service
systemctl enable --now jardulik-companion-backup.timer
if [[ -n $previous_target && $previous_target != "$release_dir" ]]; then
    ln -s "$previous_target" "$base/.previous.$$"
    mv -Tf -- "$base/.previous.$$" "$base/previous"
fi
trap - ERR
printf 'Deployed %s; service active; daily Telegram backup timer enabled.\n' "$revision"
