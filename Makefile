
PKGS ?= $(shell go list ./...)

GENERATED_FILES ?= __log* testdata/*.actual.golden logd.test log-cli.test *.pprof integration_test/out/* report/*

.PHONY: all
all: test.cover test build

.PHONY: clean
clean:
	@echo "Cleaning generated development files..."
	rm -f $(GENERATED_FILES)

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
	go install ./...

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
	go test -coverprofile=integration_test/out/unit.cov.out -covermode=count

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
	# $(foreach pkg,$(PKGS),go test -bench=$(BENCH) -cpuprofile=cpu.pprof -memprofile=mem.pprof -mutexprofile=mutex.pprof -outputdir=report/ -benchmem -run="^$$" $(pkg);)
	./script/benchmark.sh

.PHONY: benchcmp
benchcmp:
	benchcmp report/bench.out report/bench.out.1

.PHONY: ci
ci: clean deps lint.install test.coverprofile test.race test.integration.compile test.integration test.report lint test.report.summary

.PHONY: test.integration.compile
test.integration.compile:
	go test -c -o logd.test -covermode=count -coverpkg . ./cmd/logd
	go test -c -o log-cli.test -covermode=count -coverpkg . ./cmd/log-cli

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
