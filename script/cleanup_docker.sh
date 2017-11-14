#!/bin/bash
set -euxo pipefail


docker rm "$(docker ps -a -q -f status=exited -f ancestor=logd -f ancestor=logd-builder)" || true
docker rmi "$(docker images -a -q -f dangling=true)" || true
docker volume rm "$(docker volume ls -q -f dangling=true)" || true
