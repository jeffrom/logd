
TMPDIR ?= /tmp
PKGS ?= $(shell go list ./...)
SHORT_PKGS ?= $(shell go list -f '{{.Name}}' ./... | grep -v main)
PKG_DIRS ?= $(shell go list -f '{{.Dir}}' ./...)
WITHOUT_APPTEST ?= $(shell go list -f '{{.Name}}' ./... | grep -v main | grep -v app$$)

GENERATED_FILES ?= __* testdata/*.actual.golden logd.test log-cli.test

.PHONY: all
all: build

.PHONY: clean
clean:
	@echo "Cleaning generated development files..."
	rm -f $(GENERATED_FILES)
	$(foreach pkg,$(PKG_DIRS),rm -f $(pkg)/testdata/*.actual.golden;)
	$(foreach pkg,$(SHORT_PKGS),rm -f $(pkg).test;)
	rm -rf __[0-9]*.log
	rm -rf $(TMPDIR)/logd-testdata*
	rm -rf $(TMPDIR)/logd-artifacts.log*
	rm -rf ./tmp
	test -d logs && find ./logs -not -name ".gitignore" -not -name "logs" -exec rm -rf {} \; || true
	rm -rf report/*

.PHONY: clean.reports
clean.reports:
	rm -rf integration_test/out/* report/*

.PHONY: clean.docker
clean.docker:
	./script/cleanup_docker.sh

.PHONY: ls.tmp
ls.tmp:
	@echo "Listing temporary files..."
	ls $(GENERATED_FILES)
	ls integration_tests/out

.PHONY: deps
deps:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	GO111MODULE=off go get golang.org/x/tools/cmd/benchcmp
	GO111MODULE=off go get github.com/AlekSi/gocoverutil
	mkdir -p report
	mkdir -p integration_test/out

.PHONY: build
build:
	GO111MODULE=on go install -v ./...

.PHONY: release
release: release.patch

.PHONY: release.prerelease
release.prerelease:
	RELEASE=true ./script/bump-version.sh

.PHONY: release.patch
release.patch:
	RELEASE=true ./script/bump-version.sh patch

.PHONY: release.minor
release.minor:
	RELEASE=true ./script/bump-version.sh minor

.PHONY: release.major
release.major:
	RELEASE=true ./script/bump-version.sh major

.PHONY: doc.serve
doc.serve:
	godoc -http=:6060 -goroot /usr/share/go

.PHONY: build.container
build.container:
	docker build -f Dockerfile -t logd:latest .

.PHONY: test
test: test.cover test.race

.PHONY: test.race
test.race:
	GO111MODULE=on go test -race $(PKGS)

.PHONY: test.cover
test.cover:
	go test -cover -coverpkg ./... ./...

.PHONY: test.coverprofile
test.coverprofile:
	mkdir -p report
	GO111MODULE=on gocoverutil -coverprofile=report/cov.out test -covermode=count ./...

.PHONY: test.golden
test.golden:
	go test -golden $(PKGS)
	cp testdata/events.file_partition_write.0.golden testdata/q.read_file_test_log.0
	cp testdata/events.file_partition_write.1.golden testdata/q.read_file_test_log.1
	cp testdata/events.file_partition_write.2.golden testdata/q.read_file_test_log.2
	cp testdata/events.file_partition_write.index.golden testdata/q.read_file_test_log.index

.PHONY: lint
lint:
	./script/lint.sh

.PHONY: lint.install
lint.install:
	GO111MODULE=on go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.16.0

.PHONY: bench
BENCH ?= .
bench:
	./script/benchmark.sh

.PHONY: benchcmp
benchcmp:
	benchcmp report/bench.out report/bench.out.1

.PHONY: bench.compare
bench.compare:
	BENCHTIME=5s ./script/compare_benchmarks.sh

.PHONY: bench.race
bench.race:
	RACE=true ./script/benchmark.sh

.PHONY: bench.ci
bench.ci: bench.race bench.compare

.PHONY: ci
ci: clean deps build test.coverprofile test.race bench.ci test.report.summary

.PHONY: ci.local
ci.local:
	./script/image-ci.sh

.PHONY: test.integration.compile
test.integration.compile:
	go test -c -o logd.test -covermode=count -coverpkg ./... ./cmd/logd
	go test -c -o log-cli.test -covermode=count -coverpkg ./... ./cmd/log-cli

.PHONY: test.integration
test.integration:
	mkdir -p report
	./integration_test/run_integration_test.sh

.PHONY: test.report
test.report:
	mkdir -p report
	./integration_test/generate_reports.sh

.PHONY: test.report.summary
test.report.summary:
	echo -n "total: "; go tool cover -func=report/cov.out | tail -n 1 | sed -e 's/\((statements)\|total:\)//g' | tr -s "[:space:]"

.PHONY: test.report.html
test.report.html:
	go tool cover -html=report/cov.out -o report/cov.html

.PHONY: report.depgraph
report.depgraph:
	go list ./... | grep -v cmd | xargs godepgraph -s -p "github.com/pkg/errors" | dot -Tpng -o godepgraph.png
