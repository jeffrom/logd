#!/bin/bash
set -euo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

set -x

if ! command -v benchcmp > /dev/null; then
    GO111MODULE=off go get golang.org/x/tools/cmd/benchcmp
fi

if ! command -v benchcmp > /dev/null; then
    GO111MODULE=off go get golang.org/x/perf/cmd/benchstat
fi

# TODO maybe should check if we're using a volume too
if grep "docker" /proc/1/cgroup > /dev/null; then
    echo "in a container, so cleaning git state. These changes will be undone:"
    git diff
    git reset --hard HEAD
fi

set +ux
if [[ -n "$CI" && "$CI" != "false" && "$CI" != "no" ]]; then
    echo "in CI, so cleaning git state. These changes will be undone:"
    git diff
    git reset --hard HEAD
fi
set -u

if ! git diff-index --quiet HEAD --; then
    echo "Please commit all changes before using this command. Changes:"
    git diff
    exit 1
fi

branch=$(git rev-parse --symbolic-full-name HEAD)
if [[ -n "$TRAVIS_BRANCH" ]]; then
    branch="$TRAVIS_BRANCH"
fi
echo "current branch is ${branch}"

set -x

PACKAGE=events BENCH=Full ./script/benchmark.sh

set +x 2> /dev/null
if [[ "${branch}" == "master" ]]; then
    # checkout previous commit on master
    echo "checking out previous commit because we're on master branch"
    git checkout HEAD^
else
    echo "checking out master because we're on another branch"
    git checkout master
fi
set -x

finish() {
    git checkout -- go.mod # temporary workaround for go 1.13
    git checkout -
}
trap finish EXIT

PACKAGE=events BENCH=Full ./script/benchmark.sh

{
    head -n 1 report/bench.out
    head -n 1 report/bench.out.1
    echo "---"
    echo ""
} > report/benchcmp.out

{
    head -n 1 report/bench.out
    head -n 1 report/bench.out.1
    echo "---"
    echo ""
} > report/benchstat.out

# NOTE the first argument is the output of the SECOND most recent commit.
# that means, if on master, the second most recent commit in master. For all
# other branches, it means HEAD on master. Basically, the "new" column will be
# the branch you just pushed, but the benchmarks run on the new branch first.
# This seemed nicest because if the benchmarks fail, you know immediately.
# Also, you usually want to see the new benchmarks first if you're watching.
benchcmp report/bench.out report/bench.out.1 | tee -a report/benchcmp.out
benchstat report/bench.out report/bench.out.1 | tee -a report/benchstat.out
