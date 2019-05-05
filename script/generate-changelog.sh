#!/bin/bash
set -euo pipefail

if ! command -v git-chglog > /dev/null; then
    GO111MODULE=off go get github.com/git-chglog/git-chglog/cmd/git-chglog
fi

git-chglog "$@"
