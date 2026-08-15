#!/bin/sh
set -eu

destination="${EVENTFS_ARTIFACT_DIR:-.docker-artifacts}"
proxy="${EVENTFS_PROXY:-}"
commit="$(git rev-parse HEAD)"
version="${EVENTFS_VERSION:-sha-$(git rev-parse --short=7 HEAD)}"

if [ -e "$destination" ]; then
    echo "目标目录已存在，请先移走或删除：$destination" >&2
    exit 1
fi

if [ -n "$proxy" ]; then
    export HTTP_PROXY="$proxy"
    export HTTPS_PROXY="$proxy"
    export http_proxy="$proxy"
    export https_proxy="$proxy"
fi

run_id="${EVENTFS_RUN_ID:-}"
if [ -z "$run_id" ]; then
    branch="$(git branch --show-current)"
    run_id="$(gh run list \
        --workflow release.yml \
        --branch "$branch" \
        --commit "$commit" \
        --event workflow_dispatch \
        --status success \
        --limit 1 \
        --json databaseId \
        --jq '.[0].databaseId')"
fi
if [ -z "$run_id" ]; then
    echo "当前提交没有成功的 Release workflow_dispatch 运行" >&2
    exit 1
fi

gh run download "$run_id" \
    --name "eventfs-release-assets-$version" \
    --dir "$destination"

if command -v sha256sum >/dev/null 2>&1; then
    (cd "$destination" && sha256sum --check SHA256SUMS)
else
    (cd "$destination" && shasum -a 256 --check SHA256SUMS)
fi

case "$(uname -m)" in
    arm64 | aarch64)
        target="aarch64-unknown-linux-gnu"
        ;;
    x86_64 | amd64)
        target="x86_64-unknown-linux-gnu"
        ;;
    *)
        echo "不支持的 Docker 宿主架构：$(uname -m)" >&2
        exit 1
        ;;
esac

cp "$destination/eventfs-$version-$target.tar.gz" \
    "$destination/eventfs-linux-native.tar.gz"

echo "下载并校验完成：$destination/eventfs-linux-native.tar.gz"
