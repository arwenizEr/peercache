GO      ?= go
PKGS    ?= ./...
# Bench flags are conservative on purpose: a benchmark that only runs once is a
# number nobody can compare against.
BENCHTIME ?= 1s
BENCHCOUNT ?= 5
TIMEOUT ?= 300s

.PHONY: all ci test test-race latency examples vet lint bench bench-l1-eval bench-save cover fuzz tidy tools fmt clean

all: fmt vet lint test-race

## ci runs exactly what the pipeline runs, so a green local run means a green CI run.
ci: vet lint test test-race latency examples bench

## examples runs every example to completion. Compiling them is not enough:
## each one asserts the behaviour it demonstrates, and an example that is only
## compiled is documentation that can quietly start lying. M1 broke the M0
## example and nothing noticed.
examples:
	$(GO) run ./examples/m0
	$(GO) run ./examples/m1
	$(GO) run ./examples/m2
	$(GO) run ./examples/m3
	$(GO) run ./examples/m4
	$(GO) run ./examples/m5
	$(GO) run ./examples/m6
	$(GO) run ./examples/m7

## test runs the suite without the race detector, which is the fast feedback loop.
test:
	$(GO) test $(PKGS) -timeout $(TIMEOUT)

## test-race is the gate. CLAUDE.md requires every concurrency-touching test to
## run under -race, and -count=2 catches state that leaks between runs.
test-race:
	$(GO) test $(PKGS) -race -count=2 -timeout $(TIMEOUT)

## latency gates the M1 p99 acceptance criterion, alone.
##
## It cannot share a machine with the rest of the suite. `go test ./...` runs
## packages concurrently, and from M4 that means measuring a sub-microsecond
## budget while gRPC servers and gossip clusters compete for the same cores —
## which reports the machine's load, not the cache's latency. Measured idle it
## is 478ns; measured alongside everything else, 1.87us.
latency:
	PEERCACHE_LATENCY_GATE=1 $(GO) test . -run 'TestLocalGetP99AtOneMillionKeys|TestRemoteGetThroughputAndLatency' -count=1 -p 1 -timeout $(TIMEOUT)
	@echo '--- async replication loss window (separate process: its cluster perturbs the budgets above) ---'
	PEERCACHE_LATENCY_GATE=1 $(GO) test . -run 'TestAsyncLossWindowOverGRPC' -count=1 -p 1 -timeout $(TIMEOUT)

vet:
	$(GO) vet $(PKGS)

## lint is staticcheck. Install it with `make tools`.
lint:
	staticcheck $(PKGS)

## bench runs every benchmark. Numbers belong in docs/benchmarks.md together
## with the machine they were measured on; a number without a machine is noise.
bench:
	$(GO) test $(PKGS) -run '^$$' -bench . -benchmem -benchtime $(BENCHTIME) -count $(BENCHCOUNT)

## fuzz exercises the frame parser, which is the only code here that interprets
## bytes it did not write. From M4 those bytes arrive over the network, and
## "no panic in library paths" needs evidence stronger than a handful of
## hand-written corrupt inputs.
FUZZTIME ?= 30s
fuzz:
	$(GO) test ./internal/codec -run '^$$' -fuzz FuzzUnframe -fuzztime $(FUZZTIME)
	$(GO) test ./internal/codec -run '^$$' -fuzz FuzzFrameRoundTrip -fuzztime $(FUZZTIME)
	$(GO) test ./internal/wire -run '^$$' -fuzz FuzzDecode -fuzztime $(FUZZTIME)

## bench-l1-eval re-runs the L1 cache-library comparison. It lives in a nested
## module so that three losing candidates never enter the shipping dependency
## graph, which also means `go test ./...` never sees it.
bench-l1-eval:
	cd internal/l1/eval && $(GO) test -run TestReport -v
	cd internal/l1/eval && $(GO) test -run '^$$' -bench . -benchmem -count $(BENCHCOUNT)

## bench-save writes the raw output for comparison with benchstat.
bench-save:
	$(GO) test $(PKGS) -run '^$$' -bench . -benchmem -benchtime $(BENCHTIME) -count $(BENCHCOUNT) | tee bench.txt

cover:
	$(GO) test $(PKGS) -covermode atomic -coverprofile coverage.out -timeout $(TIMEOUT)
	$(GO) tool cover -func coverage.out | tail -n 1

fmt:
	$(GO) fmt $(PKGS)

tidy:
	$(GO) mod tidy

## tools installs the linters CI expects.
tools:
	$(GO) install honnef.co/go/tools/cmd/staticcheck@latest
	$(GO) install golang.org/x/perf/cmd/benchstat@latest

clean:
	rm -f coverage.out bench.txt
