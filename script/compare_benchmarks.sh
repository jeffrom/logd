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

{
    head -n 1 report/bench.out.1
    head -n 1 report/bench.out
    echo "---"
    echo ""
} > report/benchcmp.out

# NOTE the first argument is the output of the SECOND most recent commit.
# that means, if on master, the second most recent commit in master. For all
# other branches, it means HEAD on master.
benchcmp report/bench.out report/bench.out.1 | tee -a report/benchcmp.out
