#!/bin/bash
set -euxo pipefail


mkdir -p build

docker build -f Dockerfile.builder -t logd-builder:latest .

# last_build=$(docker images | grep ^logd-builder | awk '{ print ($3) }')

user=$(id -u)

docker run -i \
    -v "${PWD}/build":/go/src/github.com/jeffrom/logd/build \
    -u "$user" \
    logd-builder:latest

chown "$user":"$user" build/logd
chmod 755 build/logd

docker build -f Dockerfile.release -t logd:latest .

docker create --net host --publish 1774:1774 logd:latest
