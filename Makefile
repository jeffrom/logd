
BENCHFLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS ?= $(shell glide novendor)

.PHONY: all
all: cover test

.PHONY: clean
clean:
	@echo "Cleaning generated development files..."
	rm -f __log*
	rm -f testdata/*.actual.golden
	rm -f logd.test
	rm -f log-cli.test

.PHONY: dependencies
dependencies:
	@echo "Installing Glide and dependencies..."
	glide --version || go get -u -f github.com/Masterminds/glide
	glide install

.PHONY: test
test:
	go test -race $(PKGS)

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

