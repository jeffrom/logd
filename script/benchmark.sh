#!/bin/bash
set -euxo pipefail


rotate() {
    nums=()
    while read -r f; do
        num="${f##./report/bench.out.}"
        nums+=("$num")
    done < <(find ./report -name "bench.out.*")

    IFS=$'\n' sorted=($(sort -r <<<"${nums[*]}"))
    unset IFS

    for n in ${sorted[*]}; do
        next=$((n+1))
        mv "report/bench.out.$n" "report/bench.out.$next"
    done

    if [[ -e "report/bench.out" ]]; then
        mv report/bench.out report/bench.out.1
    fi
}

rotate

go test -run="^$" -bench=. \
    -benchmem \
    -cpuprofile=cpu.pprof \
    -memprofile=mem.pprof \
    -mutexprofile=mutex.pprof \
    -outputdir=report \
    | tee report/bench.out

