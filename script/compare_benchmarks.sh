#!/bin/bash
set -euxo pipefail

cd "$( cd "$(dirname "$0")" ; pwd )/../"

# TODO maybe should check if we're using a volume too
if grep "docker" /proc/1/cgroup > /dev/null; then
    echo "in a container, so cleaning git state. These changes will be undone:"
    git diff
    git reset --hard HEAD
fi

if ! git diff-index --quiet HEAD --; then
    echo "Please commit all changes before using this command."
    exit 1
fi

branch=$(git rev-parse --abbrev-ref HEAD)

./script/benchmark.sh

if [[ "$branch" == "master" ]]; then
    # checkout previous commit on master
    git checkout HEAD^
else
    git checkout master
fi

finish() {
    git checkout -
}
trap finish EXIT

./script/benchmark.sh

{
    head -n 1 report/bench.out
    head -n 1 report/bench.out.1
    echo "---"
    echo ""
} > report/benchcmp.out

# NOTE the first argument is the output of the SECOND most recent commit.
# that means, if on master, the second most recent commit in master. For all
# other branches, it means HEAD on master. Basically, the "new" column will be
# the branch you just pushed.
benchcmp report/bench.out report/bench.out.1 | tee -a report/benchcmp.out
