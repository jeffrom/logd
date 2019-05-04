#!/bin/bash
set -exo pipefail

tag="${TAG:-unstable}"
workdir="${LOCAL_WORKDIR:-logs}"
image_workdir="${WORKDIR:-/opt/logd}"
set -u

docker run --rm \
    --volume "$workdir":"$image_workdir" \
    --publish 1774:1774 \
    logd/logd:"$tag" --workdir "$image_workdir" "$@"
