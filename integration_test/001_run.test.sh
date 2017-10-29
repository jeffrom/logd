#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"

# shellcheck disable=SC1091
# shellcheck source=test_lib.sh
source "$TESTROOT/test_lib.sh"


run_test "$0"
finish() {
    finish_test "$0"
}
trap finish EXIT


logd -v

log_cli ping
