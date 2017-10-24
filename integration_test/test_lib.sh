#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"


run_test() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: run_test \$0"
        exit 1
    fi

    name=$(basename "$1")
    name="${name%.test.sh}"
    # name="${name#[0-9]*_}"

    testpath="$TESTROOT/out/$name"

    rm -rf "${testpath:?}"
    mkdir -p "$testpath"
    cd "$TESTROOT"
    cp "../logd_conf.yml" "${testpath}/logd_conf.yml"
    cd "$testpath"
}

finish_test() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: run_test \$0"
        exit 1
    fi

    name=$(basename "$1")
    name="${name%.test.sh}"
    # name="${name#[0-9]*_}"

    testpath="$TESTROOT/out/$name"
    cd "$testpath"

    while read -r p; do
        "$TESTROOT/../log-cli.test" -test.run=TestMain -integrationTest -- --host="127.0.0.1:$p" shutdown
    done <__logd_ports

    while read -r p; do
    # shellcheck disable=SC2009
        if ps -p "$p" | grep "logd.test"; then
            kill "$p"
        fi
    done <__logd_pids
}

wait_for_logd() {
    while true; do
        if grep "Serving at" "logd.$1.out" || test -s "logd.$1.err"; then
            break
        fi
        sleep 0.1
    done
}

LOG_CLI_RUNS=0
log_cli() {
    "$TESTROOT/../log-cli.test" -test.run=TestMain \
        -test.coverprofile="log-cli.${LOG_CLI_RUNS}.cov.out" \
        -integrationTest \
        -- "$@" \
        > log-cli.${LOG_CLI_RUNS}.out \
        2> log-cli.${LOG_CLI_RUNS}.err
    LOG_CLI_RUNS=$((LOG_CLI_RUNS+1))
}

LOGD_RUNS=0
logd() {
    "$TESTROOT/../logd.test" -test.run=TestMain \
        -test.coverprofile="logd.${LOGD_RUNS}.cov.out" \
        -integrationTest \
        -- "$@" \
        > logd.${LOGD_RUNS}.out \
        2> logd.${LOGD_RUNS}.err &

    # shellcheck disable=SC2034
    LOGD_TEST_PID=$!
    echo "$LOGD_TEST_PID" >> __logd_pids
    wait_for_logd $LOGD_RUNS
    __LOGD_PORT=$(grep "Serving at " logd.$LOGD_RUNS.out | sed -e 's/.*:\([0-9]*\)$/\1/')
    if [[ "$__LOGD_PORT" != "" ]]; then
        echo "$__LOGD_PORT" >> __logd_ports
    fi
    LOGD_RUNS=$((LOGD_RUNS+1))
}
