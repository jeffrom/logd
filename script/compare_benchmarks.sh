#!/bin/bash
set -euxo pipefail


if ! git diff-index --quiet HEAD --; then
    echo "Please commit all changes before using this command."
    exit 1
fi

branch=$(git rev-parse --abbrev-ref HEAD)

make bench

if [[ "$branch" == "master" ]]; then
    git checkout HEAD^
else
    git checkout master
fi

finish() {
    git checkout -
}
trap finish EXIT

make bench

benchcmp report/bench.out report/bench.out.1 | tee report/benchcmp.out
