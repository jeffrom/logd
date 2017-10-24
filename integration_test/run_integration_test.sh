#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"


list_tests() {
    find "$TESTROOT" -name "[0-9]*_*.test.sh"
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

run_all_tests() {
    echo "Running test suite"
    for testfile in $(list_tests); do
        run_test "$testfile"
    done
}

run_all_tests
