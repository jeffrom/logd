#!/bin/bash
set -eo pipefail

package="${1:-...}"
benchtime="${BENCHTIME:-1s}"
benchmarks="${BENCH:-.}"
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

    if [[ ${#nums[@]} -gt 0 ]]; then
        IFS=$'\n' sorted=($(sort -r <<<"${nums[*]}"))
        # XXX this doesnt work in the CI container
        # unset IFS
    fi

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
go test ./"$package" -run="^$" -bench="$benchmarks" \
    -benchmem \
    -benchtime="$benchtime" \
    | tee -a report/bench.out

    # -blockprofile=block.pprof \
    # -cpuprofile=cpu.pprof \
    # -memprofile=mem.pprof \
    # -mutexprofile=mutex.pprof \
    # -outputdir=report \
