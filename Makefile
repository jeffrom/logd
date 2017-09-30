
BENCHFLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS ?= $(shell glide novendor)

GENERATED_FILES ?= __log* testdata/*.actual.golden logd.test log-cli.test

.PHONY: all
all: cover test

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
	@echo "Installing Glide and dependencies..."
	glide --version || go get -u -f github.com/Masterminds/glide
	glide install

.PHONY: test
test:
	go test -race $(PKGS)

.PHONY: test.golden
test.golden:
	go test -golden $(PKGS)

.PHONY: cover
cover:
	go test -cover $(PKGS)

.PHONY: lint
lint:
	gometalinter $(PKGS)

.PHONY: bench
BENCH ?= .
bench:
	@$(foreach pkg,$(PKGS),go test -bench=$(BENCH) -run="^$$" $(BENCH_FLAGS) $(pkg);)

.PHONY: integration
integration:
	mkdir -p report
	go test -c -o logd.test -covermode=count -coverpkg . ./cmd/logd
	go test -c -o log-cli.test -covermode=count -coverpkg . ./cmd/log-cli

