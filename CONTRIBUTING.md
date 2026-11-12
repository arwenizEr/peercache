# Contributing

Thanks for looking. PeerCache is small on purpose and strict on purpose; this page
is the strict part. Most of it is enforced by tests in `discipline_test.go`, so a
change that breaks a rule usually fails CI with a message saying which one.

## Before you start

Open an issue for anything larger than a bug fix. The design decisions in
[`docs/decisions/`](docs/decisions) were argued out already, and a change that
reverses one — the ownership algorithm, the single activation per group, the
absence of durability — needs that argument first, not a pull request.

Things PeerCache will not do, whatever the patch looks like: cross-key or
cross-group transactions, serialised closures over the network, multi-region,
and writing to disk.

## Building and testing

Go 1.25 or later.

```sh
make test        # go test ./...
make test-race   # the gate: go test ./... -race -count=2
make vet lint    # go vet and staticcheck
make bench       # benchmarks
make fuzz        # every fuzz target for 30 seconds each
make examples    # builds and runs every example
```

On Windows without GNU make, `mingw32-make` works, or run the `go` commands
directly. `staticcheck` installs with
`go install honnef.co/go/tools/cmd/staticcheck@latest`.

A pull request is ready when `go test ./... -race` passes, `go vet` and
`staticcheck` are clean, and `gofmt -l .` prints nothing.

## Rules the code follows

- **Every concurrency-touching test runs under `-race`.**
- **No `time.Now`, `time.Sleep` or `context.WithTimeout` outside
  `internal/clock`.** Code takes a `clock.Clock`; tests use the fake one. A test
  that sleeps is a broken test: it is slow when it passes and flaky when it does
  not. The two packages that wrap libraries with their own wall-clock timers —
  `membership` and `transport` — are the exception, and the only one.
- **No `panic` on a library path, and no `log` package.** Errors are returned;
  diagnostics go to the `Logger` interface, which is a no-op by default.
- **Errors wrap with `%w` and are matched with `errors.Is` / `errors.As`.**
  Sentinel errors live in the root package. A new failure mode that a caller
  needs to act on gets a sentinel; one they only need to read does not.
- **Every exported symbol has a doc comment, and every package a `doc.go`.**
- **Context deadlines propagate** across the network and into group execution.
- **Dependencies are a budget**: memberlist, gRPC, one local cache library, one
  codec, compression, and test dependencies. Adding one needs a decision record
  in `docs/decisions/` first.
- **Correctness never depends on a message arriving.** Invalidations,
  replication tombstones and hand-offs are all best effort; whatever a lost
  message would break has to be held up by something else — a TTL, a version
  floor, a stamp comparison.

## Tests that prove something

A test should fail if the behaviour it names regresses. To check that it does,
`scripts/mutation-audit.sh` breaks the code on purpose — one entry per behaviour
— and confirms a named test notices:

```sh
ONLY='name of an entry' bash scripts/mutation-audit.sh   # one entry
bash scripts/mutation-audit.sh                           # all of them, ~10 minutes
```

It refuses a dirty tree, because it reverts with `git checkout`. If you add a
behaviour, add an entry for it; if you change code an entry mutates, update the
entry, or the audit reports it as skipped.

Benchmarks live next to the code they measure. If you change a hot path, run the
relevant benchmark before and after and put both numbers in the pull request.

## Commit messages

Say what changed and why, in the body if the subject cannot hold it. If a
change was found by a failing test, name the test.
