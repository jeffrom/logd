#!/bin/bash
set -euxo pipefail


rotate() {
    fullpath="$1"
    filebase="$(basename "$1")"
    filedir="$(dirname "$1")"
    nums=()
    while read -r f; do
        num="${f##./${fullpath}.}"
        nums+=("$num")
    done < <(find "./${filedir}" -name "${filebase}.*")

    IFS=$'\n' sorted=($(sort -r <<<"${nums[*]}"))
    unset IFS

    for n in ${sorted[*]}; do
        next=$((n+1))
        mv "${fullpath}.$n" "${fullpath}.$next"
    done

    if [[ -e "${fullpath}" ]]; then
        mv "${fullpath}" "${fullpath}.1"
    fi
}

rotate "report/bench.out"
rotate "report/cpu.pprof"
rotate "report/mem.pprof"
rotate "report/mutex.pprof"

# git rev-parse HEAD > report/bench.out
set +e
git log --oneline | head -n 1 > report/bench.out
set -e

go test -run="^$" -bench="${RUN:-.}" \
    -benchmem \
    -cpuprofile=cpu.pprof \
    -memprofile=mem.pprof \
    -mutexprofile=mutex.pprof \
    -outputdir=report \
    | tee -a report/bench.out
