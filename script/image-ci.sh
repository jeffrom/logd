#!/bin/bash
set -euo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

set -x

docker build -t logd/ci --rm -f Dockerfile.ci
docker run --rm logd/ci
