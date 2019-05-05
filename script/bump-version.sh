#!/bin/bash
set -euo pipefail

# Usage: ./script/bump-version.sh [patch|minor|major]
# see git-semver options

cd "$( cd "$(dirname "$0")" ; pwd )/../"

if ! command -v git-semver > /dev/null; then
    echo "git-semver is required: https://github.com/markchalloner/git-semver"
    exit 1
fi

branch=$(git symbolic-ref HEAD 2> /dev/null)
branch=${branch##refs/heads/}
branch=${branch:-HEAD}

if [[ "$branch" != "master" ]]; then
    echo "Not on master branch. Please checkout master and try again."
    exit 1
fi

if git status --porcelain | grep -v '^?? ' > /dev/null; then
    echo "Found uncommitted changes. Please commit and try again."
    exit 1
fi

set +u
release="${RELEASE:-}"
set -u

if [[ "$release" != "true" ]]; then
    echo "Running in dry-run. Wont commit changes. use RELEASE=true $0 to commit."
fi

# update VERSION file
args=($@)
if [[ ${#args[@]} -eq 0 ]]; then
    next=$(git-semver patch --dryrun)
else
    next=$(git-semver "$@" --dryrun)
fi

version="$(echo "$next" | sed -e 's/^v//' | tr -d '\n')"
printf "%s" "$version" > VERSION

# generate CHANGELOG
changelog=$(./script/generate-changelog.sh --config .chglog/config-commit.yml --next-tag v"$version" v"$version")

set -x
./script/generate-changelog.sh --output CHANGELOG.md --next-tag v"$version"
set +x

commit_message="logd v$version
$changelog
"

if [[ "$release" != "true" ]]; then
    echo "commit: $commit_message"
    exit
fi

git commit VERSION CHANGELOG.md --allow-empty -t <(echo -n "$commit_message")
git tag -a v"$version" -m "logd v$version"

echo "use git push --follow-tags to deploy the new version."
