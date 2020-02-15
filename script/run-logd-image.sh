#!/bin/bash
set -exo pipefail

tag="${TAG:-unstable}"
volume="${VOLUME:-$PWD/logs}"
image_workdir="${WORKDIR:-/opt/logd}"
port="${PORT:-1774}"
http_port="${HTTP_PORT:-1775}"

set -u

# _tmpdir="$(mktemp -d logd.XXXXXX -p "${TMPDIR:-/tmp}")"
# mkdir "$_tmpdir"/logs
# tmpdir="${_tmpdir}/logs"

docker run --rm \
    --user "$(id -u):$(id -g)" \
    --volume "$volume":"$image_workdir" \
    --tmpfs /opt/tmp \
    --publish "$port":"$port" \
    --publish "$http_port":"$http_port" \
    logd:"$tag" --workdir "$image_workdir" "$@"
