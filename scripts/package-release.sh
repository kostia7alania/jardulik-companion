#!/usr/bin/env bash
set -euo pipefail

die() { printf 'package-release: %s\n' "$*" >&2; exit 1; }

[[ $# -ge 2 ]] || die 'usage: scripts/package-release.sh VERSION OUTPUT_DIR [darwin-arm64|linux-amd64|linux-arm64 ...]'
version=$1
destination=$2
shift 2
[[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]] || die 'VERSION must be a tag such as v0.2.0 or v0.2.0-rc.1'
if [[ $# -eq 0 ]]; then
    set -- darwin-arm64 linux-amd64 linux-arm64
fi

for command in go tar shasum mktemp awk; do
    command -v "$command" >/dev/null || die "required command is unavailable: $command"
done

copy_notices() {
    local name=$1 source=$2 file found=false
    [[ -d $source ]] || die "license source is missing for $name"
    mkdir -p -- "$notices_dir/$name"
    for file in "$source"/{LICENSE,COPYING,NOTICE,COPYRIGHT,PATENTS}*; do
        [[ -f $file ]] || continue
        cp -- "$file" "$notices_dir/$name/"
        found=true
    done
    "$found" || die "no standard license file found for $name"
}

copy_module_notices() {
    local binary=$1 dependencies metadata name version source
    local modules=()
    dependencies=$(go version -m "$binary" | awk '$1 == "dep" {print $2}')
    [[ -n $dependencies ]] || return 0
    while IFS= read -r name; do
        modules+=("$name")
    done <<< "$dependencies"
    metadata=$(go list -mod=readonly -m -f '{{.Path}}|{{.Version}}|{{.Dir}}' "${modules[@]}")
    while IFS='|' read -r name version source; do
        copy_notices "$name@$version" "$source"
    done <<< "$metadata"
}

archives=()
for platform in "$@"; do
    case $platform in
        darwin-arm64|linux-amd64|linux-arm64) ;;
        *) die "unsupported platform: $platform" ;;
    esac
    archive="jardulik-$version-$platform.tar.gz"
    for existing in "${archives[@]:-}"; do
        [[ $existing != "$archive" ]] || die "duplicate platform: $platform"
    done
    archives+=("$archive")
done

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
[[ -f $repo_dir/LICENSE ]] || die 'LICENSE is missing'
mkdir -p -- "$destination"
output_dir=$(cd -- "$destination" && pwd -P)
[[ -z $(ls -A -- "$output_dir") ]] || die 'OUTPUT_DIR must be empty'
staging_dir=$(mktemp -d "$output_dir/.package.XXXXXX")
trap 'rm -rf -- "$staging_dir"' EXIT

for platform in "$@"; do
    printf 'Building Jardulik %s for %s\n' "$version" "$platform"
    payload_dir=$(mktemp -d "$staging_dir/payload.XXXXXX")
    notices_dir="$payload_dir/third-party-licenses"
    cp "$repo_dir/LICENSE" "$payload_dir/LICENSE"
    (
        export CGO_ENABLED=0 GOWORK=off
        export GOOS=${platform%-*} GOARCH=${platform#*-}
        cd -- "$repo_dir"
        go build -mod=readonly -trimpath -ldflags='-s -w' -o "$payload_dir/jardulik" ./cmd/jardulik
        copy_notices Go "$(go env GOROOT)"
        copy_module_notices "$payload_dir/jardulik"
        cd -- "$repo_dir/tools/telegram-transfer"
        go build -mod=readonly -trimpath -ldflags='-s -w' -o "$payload_dir/telegram-transfer" .
        copy_module_notices "$payload_dir/telegram-transfer"
    )
    COPYFILE_DISABLE=1 tar -czf "$staging_dir/jardulik-$version-$platform.tar.gz" \
        -C "$payload_dir" jardulik telegram-transfer LICENSE third-party-licenses
    rm -rf -- "$payload_dir"
done

(
    cd -- "$staging_dir"
    shasum -a 256 "${archives[@]}" > SHA256SUMS
    mv -- "${archives[@]}" SHA256SUMS "$output_dir/"
)
