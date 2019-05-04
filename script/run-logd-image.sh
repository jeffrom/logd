#!/bin/bash
set -exo pipefail

tag="${TAG:-unstable}"
volume="${VOLUME:-logs}"
image_workdir="${WORKDIR:-/opt/logd}"
port="${PORT:-1774}"
http_port="${HTTP_PORT:-1775}"

set -u

docker run --rm \
    --volume "$volume":"$image_workdir" \
    --publish "$port":"$port" \
    --publish "$http_port":"$http_port" \
    logd/logd:"$tag" --workdir "$image_workdir" "$@"
