#!/usr/bin/env bash
set -Eeuo pipefail

die() { printf 'install: %s\n' "$*" >&2; exit 1; }

[[ $# -eq 4 || $# -eq 5 ]] || die 'usage: install.sh <40-character-commit> <unpacked-release-directory> <binary-sha256> <helper-sha256> [--no-start]'
no_start=false
if [[ $# -eq 5 ]]; then
    [[ $5 == --no-start ]] || die 'invalid installation mode'
    no_start=true
fi
revision=$1
source_dir=$2
binary_sha=$3
helper_sha=$4
[[ $revision =~ ^[0-9a-f]{40}$ ]] || die 'invalid commit SHA'
[[ $binary_sha =~ ^[0-9a-f]{64}$ ]] || die 'invalid binary SHA-256'
[[ $helper_sha =~ ^[0-9a-f]{64}$ ]] || die 'invalid helper SHA-256'
[[ -f $source_dir/jardulik && ! -L $source_dir/jardulik ]] || die 'release binary is missing or is a symlink'
[[ -f $source_dir/telegram-transfer && ! -L $source_dir/telegram-transfer ]] || die 'release helper is missing or is a symlink'
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || die 'the release requires Linux x86_64'
[[ $EUID -eq 0 ]] || die 'run the installer as root'

for command in sha256sum systemctl systemd-run systemd-analyze useradd install mktemp flock python3 df stat awk; do
    command -v "$command" >/dev/null || die "required command is unavailable: $command"
done

base=/opt/jardulik-companion
data_dir=/var/lib/jardulik-companion
env_file=/etc/jardulik-companion.env
unit_dir=/etc/systemd/system
runtime_units=(jardulik-companion.service jardulik-companion-poster.service jardulik-companion-compatibility.service)
unit_names=("${runtime_units[@]}" jardulik-companion-backup.service jardulik-companion-backup.timer)

# BEGIN runtime unit helpers
runtime_info() {
    local details line load='' state='' pid=''
    details=$(systemctl show --property=LoadState --property=ActiveState --property=MainPID "$1") || true
    while IFS= read -r line; do
        case $line in
            LoadState=*) load=${line#*=} ;;
            ActiveState=*) state=${line#*=} ;;
            MainPID=*) pid=${line#*=} ;;
        esac
    done <<< "$details"
    [[ $load == loaded || $load == not-found ]] || return 1
    # A not-yet-installed service may omit service-specific properties.
    if [[ $load == not-found && $state == inactive && -z $pid ]]; then pid=0; fi
    [[ $pid =~ ^[0-9]+$ ]] || return 1
    if [[ $load == not-found ]]; then
        [[ $state == inactive && $pid == 0 ]] || return 1
    else
        [[ $state == active || $state == inactive || $state == failed ]] || return 1
    fi
    printf '%s %s %s\n' "$load" "$state" "$pid"
}
require_inactive() {
    local unit info load state pid
    for unit in "${runtime_units[@]}"; do
        info=$(runtime_info "$unit") || return 1
        read -r load state pid <<< "$info"
        if [[ $state != inactive || $pid != 0 ]]; then
            printf 'install: %s must already be inactive for --no-start/switch\n' "$unit" >&2
            return 1
        fi
    done
}
stop_runtime() {
    local unit info load state pid
    for unit in "${runtime_units[@]}"; do
        info=$(runtime_info "$unit") || return 1
        read -r load state pid <<< "$info"
        if [[ $load == loaded ]]; then systemctl stop "$unit" || return 1; fi
    done
    require_inactive
}
verify_running() {
    local unit info load state pid
    for unit in "${runtime_units[@]}"; do
        info=$(runtime_info "$unit") || return 1
        read -r load state pid <<< "$info"
        [[ $load == loaded && $state == active && $pid != 0 ]] || return 1
    done
}
# END runtime unit helpers

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
if values.get('COMPATIBILITY_ENABLED', 'false') not in ('true', 'false'):
    raise SystemExit('install: COMPATIBILITY_ENABLED must be true or false')
compatibility_interval = values.get('COMPATIBILITY_INTERVAL_SECONDS', '60')
if not re.fullmatch(r'[1-9][0-9]*', compatibility_interval) or int(compatibility_interval) > ((1 << 63) - 1) // 1000000000:
    raise SystemExit('install: COMPATIBILITY_INTERVAL_SECONDS must fit a positive Go duration')
reserve = values.get('MIN_FREE_BYTES', '2147483648')
if not reserve.isdigit() or int(reserve) < 2147483648:
    raise SystemExit('install: MIN_FREE_BYTES must preserve at least 2 GiB')
PY

available_bytes=$(df -PB1 /opt | awk 'NR == 2 { print $4 }')
release_bytes=$(( $(stat -c '%s' "$source_dir/jardulik") + $(stat -c '%s' "$source_dir/telegram-transfer") ))
reserve_bytes=$(awk -F= '/^[[:space:]]*MIN_FREE_BYTES=/ { value=$2; gsub(/[^0-9]/, "", value); found=1 } END { if (found) print value; else print "2147483648" }' "$env_file")
[[ $available_bytes =~ ^[0-9]+$ ]] || die 'could not read available disk space'
(( available_bytes >= reserve_bytes + release_bytes * 2 + 1048576 )) || die 'insufficient disk space to preserve the configured reserve'
printf '%s  %s\n' "$binary_sha" "$source_dir/jardulik" | sha256sum --check --status || die 'binary checksum mismatch'
printf '%s  %s\n' "$helper_sha" "$source_dir/telegram-transfer" | sha256sum --check --status || die 'helper checksum mismatch'
for unit in "${unit_names[@]}"; do
    [[ -f $source_dir/deploy/$unit && ! -L $source_dir/deploy/$unit ]] || die "release unit is missing: $unit"
done

exec 9>/run/lock/jardulik-companion-deploy.lock
flock -n 9 || die 'another Jardulik deployment is running'
if $no_start; then require_inactive; fi

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
    [[ -f $release_dir/jardulik && ! -L $release_dir/jardulik ]] || die 'the existing release binary is missing or is a symlink'
    [[ -f $release_dir/telegram-transfer && ! -L $release_dir/telegram-transfer ]] || die 'the existing release helper is missing or is a symlink'
    printf '%s  %s\n' "$binary_sha" "$release_dir/jardulik" | sha256sum --check --status || die 'the existing release has a different checksum'
    printf '%s  %s\n' "$helper_sha" "$release_dir/telegram-transfer" | sha256sum --check --status || die 'the existing release helper has a different checksum'
else
    candidate=$(mktemp -d "$base/releases/.$revision.XXXXXX")
    install -o root -g root -m 0755 "$source_dir/jardulik" "$candidate/jardulik"
    install -o root -g root -m 0755 "$source_dir/telegram-transfer" "$candidate/telegram-transfer"
    printf '%s  %s\n' "$binary_sha" "$candidate/jardulik" | sha256sum --check --status
    printf '%s  %s\n' "$helper_sha" "$candidate/telegram-transfer" | sha256sum --check --status
    printf '%s\n' "$revision" > "$candidate/VERSION"
    printf '%s  jardulik\n%s  telegram-transfer\n' "$binary_sha" "$helper_sha" > "$candidate/SHA256SUMS"
    chmod 0755 "$candidate"
    mv -T -- "$candidate" "$release_dir"
    candidate=''
fi

# BEGIN controlled activation
if $no_start; then require_inactive; fi
systemd-analyze verify "${unit_names[@]/#/$source_dir/deploy/}"
[[ ! -L $base/deployments ]] || die 'deployment state directory must not be a symlink'
install -d -o root -g root -m 0700 "$base/deployments"
state_dir=$(mktemp -d "$base/deployments/$revision.XXXXXX")
mkdir "$state_dir/units"
chmod 0700 "$state_dir" "$state_dir/units"
printf '%s\n' "$previous_target" > "$state_dir/previous-target"
for unit in "${unit_names[@]}"; do
    if [[ -e $unit_dir/$unit || -L $unit_dir/$unit ]]; then
        [[ -f $unit_dir/$unit && ! -L $unit_dir/$unit ]] || die "existing unit is not a regular file: $unit"
        cp -p -- "$unit_dir/$unit" "$state_dir/units/$unit"
    fi
    enabled=$(systemctl is-enabled "$unit" 2>/dev/null || true)
    case $enabled in
        enabled|enabled-runtime|disabled|static|indirect|not-found) ;;
        *) die "unsupported existing enablement state: $unit" ;;
    esac
    printf '%s\n' "$enabled" > "$state_dir/$unit.enabled"
done
for unit in "${runtime_units[@]}"; do
    info=$(runtime_info "$unit") || die "could not read runtime state: $unit"
    read -r load state pid <<< "$info"
    printf '%s\n' "$state" > "$state_dir/$unit.active"
done
timer_state=$(systemctl show --property=ActiveState --value jardulik-companion-backup.timer) || die 'could not read timer state'
printf '%s\n' "$timer_state" > "$state_dir/timer-active"

switched=false
units_changed=false
stopped=false
enabled_changed=false
rollback() {
    code=$?
    trap - ERR
    printf 'install: activation failed; restoring previous release and units (%s)\n' "$state_dir" >&2
    local unit before stop_ok=true
    if ! $no_start && $stopped; then
        if ! stop_runtime; then
            stop_ok=false
            printf 'install: runtime stop failed; previous roles will not be started\n' >&2
        fi
    fi
    if ! $no_start && $enabled_changed; then
        systemctl disable "${runtime_units[@]}" jardulik-companion-backup.timer >/dev/null 2>&1 || true
        if [[ $(cat "$state_dir/timer-active") != active ]]; then
            systemctl stop jardulik-companion-backup.timer || true
        fi
    fi
    if $units_changed; then
        for unit in "${unit_names[@]}"; do
            if [[ -f $state_dir/units/$unit ]]; then
                if ! cp -p -- "$state_dir/units/$unit" "$unit_dir/$unit"; then
                    stop_ok=false
                    printf 'install: could not restore previous unit: %s\n' "$unit" >&2
                fi
            else
                if ! rm -f -- "$unit_dir/$unit"; then
                    stop_ok=false
                    printf 'install: could not remove newly introduced unit: %s\n' "$unit" >&2
                fi
            fi
        done
    fi
    if $switched; then
        if [[ -n $previous_target ]]; then
            if ! ln -s "$previous_target" "$base/.current.rollback.$$" || ! mv -Tf -- "$base/.current.rollback.$$" "$base/current"; then
                stop_ok=false
                printf 'install: could not restore previous release symlink\n' >&2
            fi
        else
            if ! rm -f -- "$base/current"; then stop_ok=false; fi
        fi
    fi
    if ! systemctl daemon-reload; then stop_ok=false; fi
    if ! $no_start && $enabled_changed; then
        for unit in "${unit_names[@]}"; do
            before=$(cat "$state_dir/$unit.enabled")
            if [[ $before == enabled ]]; then systemctl enable "$unit" || true; fi
            if [[ $before == enabled-runtime ]]; then systemctl enable --runtime "$unit" || true; fi
        done
    fi
    if ! $no_start && $enabled_changed && $stop_ok && [[ $(cat "$state_dir/timer-active") == active ]]; then
        systemctl start jardulik-companion-backup.timer || printf 'install: prior backup timer restart failed\n' >&2
    fi
    if ! $no_start && $stopped && $stop_ok; then
        for unit in "${runtime_units[@]}"; do
            if [[ $(cat "$state_dir/$unit.active") == active ]]; then
                systemctl start "$unit" || printf 'install: prior role restart failed: %s\n' "$unit" >&2
            fi
        done
    fi
    exit "$code"
}
trap rollback ERR
if $no_start; then
    require_inactive
else
    stopped=true
    stop_runtime
fi
units_changed=true
for unit in "${unit_names[@]}"; do
    install -o root -g root -m 0644 "$source_dir/deploy/$unit" "$unit_dir/$unit"
done
ln -s "$release_dir" "$base/.current.$$"
mv -Tf -- "$base/.current.$$" "$base/current"
switched=true
systemctl daemon-reload
if $no_start; then
    require_inactive
else
    systemctl start "${runtime_units[@]}"
    sleep 2
    verify_running
    systemd-run --quiet --wait --pipe --collect \
        --unit="jardulik-companion-health-$$" \
        --property=User=jardulik --property=Group=jardulik \
        --property="EnvironmentFile=$env_file" \
        --property="WorkingDirectory=$data_dir" \
        --property=RuntimeMaxSec=30 \
        --property=MemoryMax=128M --property=TasksMax=32 \
        --setenv=GOMEMLIMIT=64MiB --setenv=GOMAXPROCS=1 \
        "$base/current/jardulik" status >/dev/null
    verify_running
    enabled_changed=true
    systemctl enable "${runtime_units[@]}"
    systemctl enable --now jardulik-companion-backup.timer
fi
if [[ -n $previous_target && $previous_target != "$release_dir" ]]; then
    ln -s "$previous_target" "$base/.previous.$$"
    mv -Tf -- "$base/.previous.$$" "$base/previous"
fi
trap - ERR
if $no_start; then
    printf 'Installed %s; all three roles remain stopped; previous units: %s\n' "$revision" "$state_dir"
else
    printf 'Deployed %s; bot/parser, poster and compatibility planner active; previous units: %s\n' "$revision" "$state_dir"
fi
# END controlled activation
