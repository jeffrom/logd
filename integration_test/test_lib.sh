#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"


get_incr_count() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: get_count tempfile"
        exit 1
    fi

    count=$(cat "$1")
    echo "$count"
    ((count++))

    echo "$count" > "$1"
}

run_test() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: run_test \$0"
        exit 1
    fi

    LOG_CLI_RUNCOUNT=$(tempfile --suffix logcli)
    echo "0" > "$LOG_CLI_RUNCOUNT"
    LOGD_RUNCOUNT=$(tempfile --suffix logd)
    echo "0" > "$LOGD_RUNCOUNT"

    name=$(basename "$1")
    name="${name%.test.sh}"
    # name="${name#[0-9]*_}"

    testpath="$TESTROOT/out/$name"

    rm -rf "${testpath:?}"
    mkdir -p "$testpath"
    cd "$TESTROOT"
    cp "../logd.conf.yml" "${testpath}/logd.conf.yml"
    cd "$testpath"
}

finish_test() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: finish_test \$0"
        exit 1
    fi

    rm -f "$LOG_CLI_RUNCOUNT"
    rm -f "$LOGD_RUNCOUNT"
    LOG_CLI_RUNCOUNT=""
    LOGD_RUNCOUNT=""

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
    n=0
    while true; do
        if (( n > 4 )); then
            waited=$(echo "scale=2; $n*0.1" | bc)
            echo "waited $waited secs for logd"
            exit 1
        fi
        if grep "Serving at" "logd.$1.out" || test -s "logd.$1.err"; then
            break
        fi

        sleep 0.1
        ((n++)) || true
    done
}

log_cli() {
    count=$(get_incr_count "$LOG_CLI_RUNCOUNT")
    "$TESTROOT/../log-cli.test" -test.run=TestMain \
        -test.coverprofile="log-cli.${count}.cov.out" \
        -integrationTest \
        -- "$@" \
        # | sed -e '/^PASS$/d; /^coverage: .*$/d' \
        > >(tee "log-cli.${count}.out") \
        2> >(tee "log-cli.${count}.err")
}

strip_test_output() {
    sed -e '/^PASS/d; /^coverage:/d'
}

logd() {
    count=$(get_incr_count "$LOGD_RUNCOUNT")

    "$TESTROOT/../logd.test" -test.run=TestMain \
        -test.coverprofile="logd.${count}.cov.out" \
        -integrationTest \
        -- "$@" \
        > >(tee "logd.${count}.out") \
        2> >(tee "logd.${count}.err") &

    # shellcheck disable=SC2034
    LOGD_TEST_PID=$!
    echo "$LOGD_TEST_PID" >> __logd_pids
    wait_for_logd "$count"
    __LOGD_PORT=$(grep "Serving at " "logd.$count.out" | sed -e 's/.*:\([0-9]*\)$/\1/')
    if [[ "$__LOGD_PORT" != "" ]]; then
        echo "$__LOGD_PORT" >> __logd_ports
    fi
}

failnow() {
    msg="FAILED"
    if [[ "x$1" != "x" ]]; then
        msg="FAILED: $1"
    fi
    printf "\n\n*\n**\n*** %s\n**\n*\n\n" "$msg"
    exit 1
}

assert_equal() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: assert_equal filea fileb"
        exit 1
    fi
    if [[ "x$2" == "x" ]]; then
        echo "usage: assert_equal filea fileb"
        exit 1
    fi

    diff -u "$1" "$2" || failnow "values were unequal"
}
