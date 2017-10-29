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

log_cli -v write "first message"
echo "second message" | log_cli -v write
echo "fourth message" | log_cli -v write "third message"

actual=$(log_cli -v read | strip_test_output)

expected=$(cat <<EOF
1 first message
2 second message
3 third message
4 fourth message
EOF
)

assert_equal <(echo "$expected") <(echo "$actual")
