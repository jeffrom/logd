#!/bin/bash
set -eo pipefail

package="${1:-...}"
benchtime="${BENCHTIME:-1s}"
benchmarks="${BENCH:-.}"
go111module="${GO111MODULE:-on}"

if [[ ! -z "$CI" && "$CI" != "false" && "$CI" != "no" ]]; then
    go111module=on
fi
set -u

cd "$( cd "$(dirname "$0")" ; pwd )/../"

rotate() {
    fullpath="$1"
    filebase="$(basename "$1")"
    filedir="$(dirname "$1")"
    nums=()
    while read -r f; do
        num="${f##./${fullpath}.}"
        nums+=("$num")
    done < <(find "./${filedir}" -name "*.${filebase}.*")

    if [[ ${#nums[@]} -eq 0 ]]; then
        return
    fi

    IFS=$'\n' sorted=($(sort -r <<<"${nums[*]}"))
    # XXX this doesnt work in the CI container
    # unset IFS

    # echo $sorted
    for n in ${sorted[*]}; do
        next=$((n+1))
        mv "${fullpath}.$n" "${fullpath}.$next"
    done

    if [[ -e "${fullpath}" ]]; then
        mv "${fullpath}" "${fullpath}.1"
    fi
}

mkdir -p report

rotate "report/bench.out"
# rotate "report/cpu.pprof"
# rotate "report/mem.pprof"
# rotate "report/mutex.pprof"

set +e
{
    git log --oneline | head -n 1
    echo "---"
    echo
} > report/bench.out
set -e

set -x
GO111MODULE="$go111module" go test ./"$package" -run="^$" -bench="$benchmarks" \
    -benchmem \
    -benchtime="$benchtime" \
    | tee -a report/bench.out

    # -blockprofile=block.pprof \
    # -cpuprofile=cpu.pprof \
    # -memprofile=mem.pprof \
    # -mutexprofile=mutex.pprof \
    # -outputdir=report \
