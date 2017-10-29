#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"


list_tests() {
    find "$TESTROOT" -name "[0-9]*_*.test.sh" | cat | sort
}

run_test() {
    if [[ "x$1" == "x" ]]; then
        echo "usage: run_test <test_file_path>"
        exit 1
    fi

    bn="$(basename "$1")"
    # shellcheck disable=SC2001
    test_num="$(echo "$bn" | sed -e 's/^\([0-9]*\)_.*/\1/')"
    # shellcheck disable=SC2001
    test_name="$(echo "$bn" | sed -e 's/^[0-9]*_\(.*\).test.sh/\1/')"
    echo "===TEST ${test_num} ${test_name}"

    "$1"
}

finish_all() {
    if killall logd.test; then
        echo "Extra logd.test instances laying around..."
    fi
    if killall log-cli.test; then
        echo "Extra log-cli.test instances laying around..."
    fi
}

run_all_tests() {
    trap finish_all EXIT

    echo "Running test suite"
    list_tests
    for testfile in $(list_tests); do
        if ! run_test "$testfile"; then
            exit 1
        fi
    done
    echo "Completed test suite"
}

run_all_tests
