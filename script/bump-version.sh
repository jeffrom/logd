#!/bin/bash
set -euo pipefail

# Usage: ./script/bump-version.sh [patch|minor|major]
# see semver options

cd "$( cd "$(dirname "$0")" ; pwd )/../"

if ! command -v semver > /dev/null; then
    echo "semver-tool is required: https://github.com/fsaintjacques/semver-tool"
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
    echo "Running in dry-run. Wont commit changes."
    echo "Use RELEASE=true $0 to commit."
fi

# get latest version
latest=$(git tag  -l --merged master --sort='-*authordate' | head -n1)
latest=$(git tag  -l --merged master --sort='-*authordate' | head -n1)
semver_parts=(${latest//./ })
major=${semver_parts[0]}
minor=${semver_parts[1]}
patch=${semver_parts[2]}
current_version=${major}.${minor}.${patch}

echo "Current version is $current_version"

# update VERSION file
args=($@)
if [[ ${#args[@]} -eq 0 ]]; then
    # shortsha=$(git rev-parse --short=9 --verify HEAD)
    next=$(semver bump patch "$current_version")
else
    next=$(semver bump "$@" "$current_version")
fi

version="$(echo "$next" | sed -e 's/^v//' | tr -d '\n')"

# generate CHANGELOG
changelog=$(./script/generate-changelog.sh --config .chglog/config-commit.yml --next-tag v"$version" v"$version")

commit_message="logd v$version
$changelog
"

if [[ "$release" != "true" ]]; then
    echo
    echo "dry run mode"
    echo "VERSION: $version"
    echo "commit: $commit_message"
    exit
fi

printf "%s" "$version" > VERSION

set -x
./script/generate-changelog.sh --output CHANGELOG.md --next-tag v"$version"
set +x

git commit VERSION CHANGELOG.md --allow-empty -t <(echo -n "$commit_message")
git tag -a v"$version" -m "logd v$version"

echo "use git push --follow-tags to deploy the new version."
