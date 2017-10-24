#!/bin/bash
set -euxo pipefail

TESTROOT="$( cd "$(dirname "$0")" ; pwd -P )"

cd "$TESTROOT"
make test.coverprofile
