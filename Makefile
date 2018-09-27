
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
	rm -rf logs/*

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
	@echo "Installing dep tool and dependencies..."
	dep version || go get -u github.com/golang/dep/cmd/dep
	dep ensure -v
	go get github.com/wadey/gocovmerge
	go get golang.org/x/tools/cmd/benchcmp
	go get github.com/AlekSi/gocoverutil
	mkdir -p report
	mkdir -p integration_test/out

.PHONY: deps.dep
deps.dep:
	@echo "Installing dep tool..."
	go get -u github.com/golang/dep/cmd/dep

.PHONY: build
build:
	go install -x -v ./...

.PHONY: doc.serve
doc.serve:
	godoc -http=:6060 -goroot /usr/share/go

.PHONY: build.container
build.container:
	./script/build_container.sh

.PHONY: test
test: test.cover test.race

.PHONY: test.race
test.race:
	go test -race $(PKGS)

.PHONY: test.cover
# $(foreach pkg,$(WITHOUT_APPTEST),go test -outputdir=../report -cover ./$(pkg);)
test.cover:
	go test -cover -coverpkg ./... ./...

.PHONY: test.coverprofile
test.coverprofile:
	gocoverutil -coverprofile=cov.out test -covermode=count ./...

.PHONY: test.golden
test.golden:
	go test -golden $(PKGS)
	cp testdata/events.file_partition_write.0.golden testdata/q.read_file_test_log.0
	cp testdata/events.file_partition_write.1.golden testdata/q.read_file_test_log.1
	cp testdata/events.file_partition_write.2.golden testdata/q.read_file_test_log.2
	cp testdata/events.file_partition_write.index.golden testdata/q.read_file_test_log.index

.PHONY: lint
lint:
	gometalinter --aggregate --vendored-linters --vendor --enable-all $(SHORT_PKGS)
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
	# ./script/benchmark.sh
	# $(foreach pkg,$(SHORT_PKGS),go test -bench=$(BENCH) -cpuprofile=$(pkg).cpu.pprof -memprofile=$(pkg).mem.pprof -mutexprofile=$(pkg).mutex.pprof -outputdir=../report -benchmem -run="^$$" ./$(pkg) | tee -a report/$(pkg).bench.out;)
	$(foreach pkg,$(SHORT_PKGS),go test -bench=$(BENCH) -benchtime=2s -cpuprofile=$(pkg).cpu.pprof -outputdir=../report -benchmem -run="^$$" ./$(pkg) | tee -a report/$(pkg).bench.out;)

.PHONY: benchcmp
benchcmp:
	benchcmp report/bench.out report/bench.out.1

.PHONY: bench.ci
bench.ci:
	./script/compare_benchmarks.sh

.PHONY: bench.race
bench.race:
	go test ./... -run ^$$ -bench . -benchmem -benchtime 2s -race

.PHONY: ci
# ci: clean deps build lint.install test.coverprofile test.race test.integration.compile test.integration test.report lint test.report.summary
# ci: clean deps build test.coverprofile test.race bench.race test.report test.report.summary
ci: clean deps build test.coverprofile test.race bench.race test.report.summary

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
	echo -n "total: "; go tool cover -func=cov.out | tail -n 1 | sed -e 's/\((statements)\|total:\)//g' | tr -s "[:space:]"

.PHONY: test.report.html
test.report.html:
	go tool cover -html=cov.out -o cov.html

.PHONY: report.depgraph
report.depgraph:
	go list ./... | grep -v cmd | xargs godepgraph -s -p "github.com/pkg/errors" | dot -Tpng -o godepgraph.png
