
BENCHFLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS ?= $(shell glide novendor)

.PHONY: all
all: cover test

.PHONY: clean
clean:
	@echo "Cleaning generated development files..."
	rm -f __log*
	rm -f testdata/*.actual.golden

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
