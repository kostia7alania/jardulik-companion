#!/usr/bin/env bash
set -euo pipefail

die() { printf 'deploy: %s\n' "$*" >&2; exit 1; }

[[ $# -le 1 ]] || die 'usage: scripts/deploy.sh [ssh-host-alias]'
deploy_host=${1:-sg-1gb}
[[ $deploy_host =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die 'invalid SSH host alias'
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
for command in git go ssh scp tar shasum mktemp; do
    command -v "$command" >/dev/null || die "required command is unavailable: $command"
done
git -C "$repo_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die 'the script must run from its Git checkout'
revision=$(git -C "$repo_dir" rev-parse --verify HEAD 2>/dev/null) || die 'create and commit the release before deploying'
[[ $(git -C "$repo_dir" branch --show-current) == main ]] || die 'deploy only a reviewed main commit'
[[ -z $(git -C "$repo_dir" status --porcelain) ]] || die 'commit all changes before deploying'
git -C "$repo_dir" fetch --no-tags origin
[[ $(git -C "$repo_dir" rev-parse origin/main) == "$revision" ]] || die 'local main must match origin/main before deploying'

cd "$repo_dir"
go test -race ./...
go vet ./...

staging_dir=$(mktemp -d "${TMPDIR:-/tmp}/jardulik-deploy.XXXXXX")
remote_stage=''
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=2)
cleanup() {
    rm -rf -- "$staging_dir"
    if [[ -n $remote_stage ]]; then
        ssh "${ssh_options[@]}" "$deploy_host" "rm -rf -- '$remote_stage'" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
mkdir "$staging_dir/release"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o "$staging_dir/release/jardulik" ./cmd/jardulik
mkdir "$staging_dir/release/deploy"
for file in install.sh jardulik-companion.service jardulik-companion-backup.service jardulik-companion-backup.timer; do
    cp "$repo_dir/deploy/$file" "$staging_dir/release/deploy/$file"
done
binary_sha=$(shasum -a 256 "$staging_dir/release/jardulik" | awk '{print $1}')
COPYFILE_DISABLE=1 tar -czf "$staging_dir/release.tar.gz" -C "$staging_dir/release" jardulik deploy
archive_sha=$(shasum -a 256 "$staging_dir/release.tar.gz" | awk '{print $1}')
stage_candidate=$(ssh "${ssh_options[@]}" "$deploy_host" 'mktemp -d /tmp/jardulik-deploy.XXXXXX')
[[ $stage_candidate =~ ^/tmp/jardulik-deploy\.[A-Za-z0-9]+$ ]] || die 'the server returned an unexpected staging path'
remote_stage=$stage_candidate
scp "${ssh_options[@]}" "$staging_dir/release.tar.gz" "$deploy_host:$remote_stage/release.tar.gz"
ssh "${ssh_options[@]}" "$deploy_host" bash -s -- "$remote_stage" "$revision" "$archive_sha" "$binary_sha" <<'REMOTE'
set -euo pipefail
stage=$1
revision=$2
archive_sha=$3
binary_sha=$4
printf '%s  %s\n' "$archive_sha" "$stage/release.tar.gz" | sha256sum --check --status
mkdir "$stage/unpacked"
tar --no-same-owner -xzf "$stage/release.tar.gz" -C "$stage/unpacked"
bash "$stage/unpacked/deploy/install.sh" "$revision" "$stage/unpacked" "$binary_sha"
REMOTE
