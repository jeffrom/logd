
PKGS ?= $(shell go list ./...)
SHORT_PKGS ?= $(shell go list -f '{{.Name}}' ./... | grep -v main)
PKG_DIRS ?= $(shell go list -f '{{.Dir}}' ./...)

GENERATED_FILES ?= __log* testdata/*.actual.golden logd.test log-cli.test *.pprof

.PHONY: all
all: build

.PHONY: clean
clean:
	@echo "Cleaning generated development files..."
	rm -f $(GENERATED_FILES)
	$(foreach pkg,$(PKG_DIRS),rm -f $(pkg)/testdata/*.actual.golden;)
	$(foreach pkg,$(SHORT_PKGS),rm -f $(pkg).test;)
	rm -rf integration_test/out/* report/*
	rm -rf /tmp/__logd-testdata__*

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
	@echo "Installing dep tool and dependencies..."
	dep version || go get -u github.com/golang/dep/cmd/dep
	dep ensure
	go get github.com/wadey/gocovmerge
	go get golang.org/x/tools/cmd/benchcmp
	mkdir -p report
	mkdir -p integration_test/out

.PHONY: deps.dep
deps.dep:
	@echo "Installing dep tool..."
	go get -u github.com/golang/dep/cmd/dep

.PHONY: build
build:
	go install -x -v ./...

.PHONY: build.container
build.container:
	./script/build_container.sh

.PHONY: test
test: test.cover test.race

.PHONY: test.race
test.race:
	go test -race $(PKGS)

.PHONY: test.cover
test.cover:
	go test -cover $(PKGS)

.PHONY: test.coverprofile
test.coverprofile:
	$(foreach pkg,$(SHORT_PKGS),go test -coverprofile=integration_test/out/unit.$(pkg).cov.out -covermode=count ./$(pkg);)

.PHONY: test.golden
test.golden:
	go test -golden $(PKGS)
	cp testdata/events.file_partition_write.0.golden testdata/q.read_file_test_log.0
	cp testdata/events.file_partition_write.1.golden testdata/q.read_file_test_log.1
	cp testdata/events.file_partition_write.2.golden testdata/q.read_file_test_log.2
	cp testdata/events.file_partition_write.index.golden testdata/q.read_file_test_log.index

.PHONY: lint
lint:
	gometalinter --aggregate --vendored-linters --vendor --enable-all $(PKGS)
	# ./script/lint.sh

.PHONY: lint.install
lint.install:
	go get github.com/alecthomas/gometalinter
	gometalinter --install

.PHONY: lint.update
lint.update:
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install --update

.PHONY: bench
BENCH ?= .
bench:
	mkdir -p report
	# go test -run="^$$" -bench=. -benchmem -cpuprofile=cpu.pprof -memprofile=mem.pprof -mutexprofile=mutex.pprof -outputdir=report | tee report/bench.out
	# ./script/benchmark.sh
	$(foreach pkg,$(SHORT_PKGS),go test -bench=$(BENCH) -cpuprofile=$(pkg).cpu.pprof -memprofile=$(pkg).mem.pprof -mutexprofile=$(pkg).mutex.pprof -outputdir=../report -benchmem -run="^$$" ./$(pkg) | tee -a report/$(pkg).bench.out;)

.PHONY: benchcmp
benchcmp:
	benchcmp report/bench.out report/bench.out.1

.PHONY: bench.ci
bench.ci:
	./script/compare_benchmarks.sh

.PHONY: ci
ci: clean deps lint.install test.coverprofile test.race test.integration.compile test.integration test.report lint test.report.summary

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
	go tool cover -func=integration_test/out/all.cov.out
