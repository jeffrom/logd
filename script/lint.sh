#!/bin/bash
set -euxo pipefail

gometalinter --aggregate --vendored-linters --vendor --enable-all --tests \
    --warn-unmatched-nolint \
    --exclude='error return value not checked.*(Close|Log|Print).*\(errcheck\)$' \
    --exclude='.*_test\.go:.*error return value not checked.*\(errcheck\)$' \
    --exclude='duplicate of.*_test.go.*\(dupl\)$'
