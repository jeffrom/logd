#!/bin/bash
set -euo pipefail

if ! command -v git-chglog > /dev/null; then
    GO111MODULE=off go get github.com/git-chglog/git-chglog/cmd/git-chglog
fi

set +u
release="${RELEASE:-}"
set -u

if [[ "$release" == "true" ]]; then
    git-chglog --output CHANGELOG.md "$@"
else
    git-chglog "$@"
fi
