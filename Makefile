
BENCHFLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS ?= $(go list ./...)

GENERATED_FILES ?= __log* testdata/*.actual.golden logd.test log-cli.test

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

.PHONY: deps
deps:
	@echo "Installing dep tool and dependencies..."
	dep version || go get -u github.com/golang/dep/cmd/dep
	dep ensure

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

.PHONY: test.golden
test.golden:
	go test -golden $(PKGS)

.PHONY: lint
lint:
	gometalinter --aggregate --vendored-linters --vendor --enable-all $(PKGS)

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
	@$(foreach pkg,$(PKGS),go test -bench=$(BENCH) -run="^$$" $(BENCH_FLAGS) $(pkg);)

.PHONY: ci
ci: deps lint.install test.cover test.race lint

.PHONY: integration
integration:
	mkdir -p report
	go test -c -o logd.test -covermode=count -coverpkg . ./cmd/logd
	go test -c -o log-cli.test -covermode=count -coverpkg . ./cmd/log-cli
