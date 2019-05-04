#!/bin/bash
set -eo pipefail

tag="${TAG:-unstable}"

set -u

docker run --rm \
    --network=host \
    logd/log-cli:"$tag" "$@"
