#!/bin/bash
set -euo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

set -x

set +u
count="${COUNT:-1}"
set -u

docker build -t logd/ci --rm -f Dockerfile.ci . \
    && docker run --rm \
    -e COUNT="$count" \
    logd/ci "$@"
