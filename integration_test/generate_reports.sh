#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"

generate_reports() {
    find "$TESTROOT/out" -name "*.cov.out" -print0 | xargs -0 gocovmerge > "$TESTROOT/out/all.cov.out"
    # find "$TESTROOT/out" -name "*.cov.out" -and -not -name "unit.cov.out" -and -not -name "all.cov.out" -print0 | xargs -0 gocovmerge > "$TESTROOT/out/integration.cov.out"
    # find "$TESTROOT/out" -name "logd.*.cov.out" -print0 | xargs -0 gocovmerge > "$TESTROOT/out/server.cov.out"
    # find "$TESTROOT/out" -name "log-cli.*.cov.out" -print0 | xargs -0 gocovmerge > "$TESTROOT/out/client.cov.out"

    go tool cover -html="$TESTROOT/out/all.cov.out" -o "$TESTROOT/out/all.cov.html"
    # go tool cover -html="$TESTROOT/out/integration.cov.out" -o "$TESTROOT/out/integration.cov.html"
    # go tool cover -html="$TESTROOT/out/server.cov.out" -o "$TESTROOT/out/server.cov.html"
    # go tool cover -html="$TESTROOT/out/client.cov.out" -o "$TESTROOT/out/client.cov.html"

    if [[ -s "$TESTROOT/out/unit.cov.out" ]]; then
        echo "total"
        go tool cover -func="$TESTROOT/out/unit.cov.out" | grep "total:"
    fi

    if [[ -s "$TESTROOT/out/server.cov.out" ]]; then
        echo "server"
        go tool cover -func="$TESTROOT/out/server.cov.out" | grep "total:"
    fi

    if [[ -s "$TESTROOT/out/client.cov.out" ]]; then
        echo "client"
        go tool cover -func="$TESTROOT/out/client.cov.out" | grep "total:"
    fi

    if [[ -s "$TESTROOT/out/integration.cov.out" ]]; then
        echo "total integration"
        go tool cover -func="$TESTROOT/out/integration.cov.out" | grep "total:"
    fi

    if [[ -s "$TESTROOT/out/all.cov.out" ]]; then
        echo "total"
        go tool cover -func="$TESTROOT/out/all.cov.out" | grep "total:"
    fi
}

generate_reports
