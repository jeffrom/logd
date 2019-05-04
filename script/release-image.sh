#!/bin/bash
set -eo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

do_release="${1:-}"

set -u

if [[ "$do_release" == "-h" || "$do_release" == "--help" ]]; then
    echo "Usage: $0 [release]"
    echo
    echo "providing the argument 'release' will push the tag in the VERSION file."
    exit 1
fi

if [[ "$do_release" != "release" ]]; then
    echo "Only pushing latest tag. Use \`$0 release\` to push a release."
fi

docker build --rm -t logd/logd:latest .
docker build --rm -t logd/log-cli:latest -f Dockerfile.cli .

if [[ "$do_release" == "release" ]]; then
    tag="$(cat VERSION)"
    docker tag logd/logd:latest logd/logd:"$tag"
    docker tag logd/log-cli:latest logd/log-cli:"$tag"

    docker push logd/logd:"$tag"
    docker push logd/log-cli:"$tag"
fi

docker push logd/logd:latest
docker push logd/log-cli:latest
