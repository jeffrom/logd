#!/bin/bash
set -euxo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

if ! command -v go-mod-outdated > /dev/null; then
    GO111MODULE=off go get github.com/psampaz/go-mod-outdated
fi

IFS=" "
mods=($(GO111MODULE=on go list -f '{{join .Imports "\n"}}' ./... \
    | grep '\..*\/.*\/' \
    | grep -v jeffrom/logd \
    | sort -u \
    | tr '\n' ' '))

GO111MODULE=on go list -u -m -json "${mods[@]}" | go-mod-outdated
