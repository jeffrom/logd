#!/bin/bash
set -euo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

if ! command -v goreleaser > /dev/null; then
    :
fi

set +u
do_release="${RELEASE:-}"
set -u

if [[ "$do_release" != "true" ]]; then
    echo "Dry running release. use RELEASE=true $0 to publish."
    set -x
    goreleaser release --snapshot --skip-publish --rm-dist
    exit
fi

set -x
goreleaser release
