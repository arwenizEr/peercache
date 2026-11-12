GO      ?= go
PKGS    ?= ./...
# Bench flags are conservative on purpose: a benchmark that only runs once is a
# number nobody can compare against.
BENCHTIME ?= 1s
BENCHCOUNT ?= 5
TIMEOUT ?= 300s

# RACE_TIMEOUT is separate, and much larger, because `test-race` runs the suite
# twice under the race detector in one binary.
#
# The root package takes ~215s for one race pass on Machine A, so `-count=2`
# needed 439s when this was written and measured 527s later. The 300s this file used to share with the non-race gate was not being met:
# `make test-race` failed with `panic: test timed out after 5m0s`, mid-way
# through the second pass, with a test running normally at the moment the alarm
# fired; the gate had been unpassable on this machine for some time before
# anyone measured it.
#
# Raising it is not weakening the gate. A timeout here exists to turn a deadlock
# into a failure rather than a hung CI job, and 900s does that just as well as
# 300s did — what 300s had additionally become was an undeclared performance
# budget that the suite outgrew silently.
#
# The margin is 1.7x and was 2.0x before the simulation tests. Re-measure before assuming
# it, rather than after the gate stops passing; that is the mistake 300s was.
#
# Where the time goes is measured in docs/benchmarks.md, "What the test suite
# itself costs" — three tests are 81% of a race pass, their cost is inherent to
# running many nodes under the detector, and two of them are acceptance criteria.
# The figures live there rather than here so there is one copy to keep true.
RACE_TIMEOUT ?= 900s

.PHONY: all ci test test-race latency examples vet lint bench bench-l1-eval bench-save cover fuzz tidy tools fmt clean

all: fmt vet lint test-race

## ci runs exactly what the pipeline runs, so a green local run means a green CI run.
ci: vet lint test test-race latency examples bench

## examples runs every example to completion. Compiling them is not enough:
## each one asserts the behaviour it demonstrates, and an example that is only
## compiled is documentation that can quietly start lying. A change once broke
## the lifecycle example and nothing noticed.
examples:
	$(GO) run ./examples/lifecycle
	$(GO) run ./examples/namespace
	$(GO) run ./examples/groups
	$(GO) run ./examples/ownership
	$(GO) run ./examples/remote
	$(GO) run ./examples/replication
	$(GO) run ./examples/rolling-upgrade
	$(GO) run ./examples/observability
	$(GO) run ./examples/failure-guarantees

## test runs the suite without the race detector, which is the fast feedback loop.
test:
	$(GO) test $(PKGS) -timeout $(TIMEOUT)

## test-race is the gate. CLAUDE.md requires every concurrency-touching test to
## run under -race, and -count=2 catches state that leaks between runs.
test-race:
	$(GO) test $(PKGS) -race -count=2 -timeout $(RACE_TIMEOUT)

## latency gates the local p99 acceptance criterion, alone.
##
## It cannot share a machine with the rest of the suite. `go test ./...` runs
## packages concurrently, and that means measuring a sub-microsecond
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
## bytes it did not write. Those bytes arrive over the network, and
## "no panic in library paths" needs evidence stronger than a handful of
## hand-written corrupt inputs.
##
## It also covers two properties that are not about hostile input at all: the
## codecs must return the value they were given, and the partitioner must agree
## with itself, because ownership is derived from its output.
##
## Every pattern is anchored. `-fuzz` takes a regexp, and an unanchored name is
## a prefix that a later target can collide with — adding FuzzDecodeEveryMessage
## did exactly that to `-fuzz FuzzDecode`, which then refused to run at all.
## TestFuzzTargetsNamedInCIExist checks this file and the workflows for both
## failure modes, ambiguous and unmatched.
FUZZTIME ?= 30s
fuzz:
	$(GO) test ./internal/codec -run '^$$' -fuzz '^FuzzUnframe$$' -fuzztime $(FUZZTIME)
	$(GO) test ./internal/codec -run '^$$' -fuzz '^FuzzFrameRoundTrip$$' -fuzztime $(FUZZTIME)
	$(GO) test ./internal/codec -run '^$$' -fuzz '^FuzzCodecRoundTrip$$' -fuzztime $(FUZZTIME)
	$(GO) test ./internal/wire -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime $(FUZZTIME)
	$(GO) test ./internal/wire -run '^$$' -fuzz '^FuzzDecodeEveryMessage$$' -fuzztime $(FUZZTIME)
	$(GO) test . -run '^$$' -fuzz '^FuzzPartitionByPrefix$$' -fuzztime $(FUZZTIME)

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

## clean removes every artefact .gitignore names, and reads .gitignore to find
## out which those are rather than repeating the list. The two had already
## drifted: this recipe removed two of the seven patterns, which is how two 17 MB
## example binaries matched by `*.exe` sat in the repository root for a day.
## Being gitignored kept them out of the history and did nothing about them being
## there.
##
## It is a Go program rather than `rm -f` so that it does not depend on which
## shell make was invoked from — the glob in the old recipe did not make GNU make
## reach for SHELL, so `rm` had to be on PATH, and from PowerShell it is not.
## Portability, not repair: the recipe worked from Git Bash and on CI.
clean:
	$(GO) run ./scripts/clean
