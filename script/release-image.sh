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

docker build -t logd/logd:latest .

if [[ "$do_release" == "release" ]]; then
    tag="$(cat VERSION)"
    docker tag logd/logd:latest logd/logd:"$tag"
    docker push logd/logd:"$tag"
fi

docker push logd/logd:latest
