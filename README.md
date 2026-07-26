# PeerCache

An embedded distributed cache and stateful aggregation engine for Go services
deployed as N replicas.

Every pod embeds the library; there is no separate cache tier. Each key belongs
to exactly one pod, chosen by rendezvous hashing over the live membership. Reads
hit local memory when the data is owned locally or recently cached, and route to
the owner otherwise. Mutations and registered computations execute on the owner,
so a working set is never shipped to the caller.

```go
cache, err := peercache.New(
    peercache.WithNodeID(os.Getenv("POD_NAME")),
    peercache.WithBindAddr(os.Getenv("POD_IP") + ":7946"),
)
defer cache.Close(ctx)

// Computation moves to the data, not the other way round.
cache.RegisterGroupFunc("apply", func(ctx context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
    current, _ := s.Get("total")
    s.Set("total", add(current, arg))
    return nil, nil
})

_, err = cache.Group("tenant:acme").Call(ctx, "apply", payload)
```

---

## Guarantees

This section is the most useful documentation in the project, so it comes before
the feature list rather than after it.

- **Operations on a single group are serialized while that group has one
  activation.** Serialization is structural — one goroutine per activated group,
  fed by a bounded mailbox — which is why there is no lock API.
- **Membership is eventually consistent.** It is gossip based and therefore AP.
  A network partition can briefly produce two activations of the same group.
  Conflicting writes are *detected and counted*, not automatically merged. The
  window is narrowed by activation delay and detected via epochs. It is not
  eliminated, and no configuration setting will eliminate it.
- **State is not durable.** Ungraceful pod termination loses that pod's owned
  data and any replicas not yet caught up. Graceful shutdown transfers ownership
  and streams state. Every namespace must therefore either be reconstructible
  from a loader or tolerate loss.
- **Async replication has a data-loss window** averaging **≈0.1 ms**, p99 under
  1.5 ms, measured at RF=3 over gRPC (see `docs/benchmarks.md`). A write
  acknowledged inside it exists on one node only. `WithSyncReplication()` closes
  the window entirely, at the cost of bounding a group's write rate by its
  slowest replica.
- **A restarted node must call `CatchUp` before it serves traffic.** This is the
  sharpest edge in the system. Replication is a stream of writes, not a state
  transfer, so a node that restarts receives only the keys written *after* it
  came back — while the ownership ranking hands it back its keys immediately. It
  will answer "not found", with total confidence, for data sitting safely on the
  node that covered for it. In the chaos test that presented as 558 of 600 keys
  lost, none of them actually gone. Call `CatchUp` after readiness and before
  serving.
- **Reads of non-owned keys may be stale** by up to the configured L1 TTL.
  Invalidation broadcasts are a latency optimisation and may be lost;
  correctness never depends on one arriving.
- **Replication survives the loss of RF−1 nodes, and no more.** At RF=3, a
  chaos run of 30 kills and 30 restarts over 60 simulated seconds lost no
  synchronously written key. At RF=2, killing both holders of a key loses it —
  that is what RF=2 means, and it is asserted as a test rather than left as a
  caveat. Survivors are always correct: losing data is possible, corrupting it
  is not.
- **Failure detection must be faster than your failure rate.** A cluster losing
  a node every two seconds while taking three to notice never converges. This is
  a deployment constraint the library cannot fix.
- **Throughput degrades to single-pod capacity for a single hot group.** Group
  sharding for hot-key mitigation is deliberately not in v1.

The words "strong" and "quorum" do not appear in this API, deliberately.

### What this is not

- Not a database.
- Not a general distributed transaction system. There are no cross-group
  transactions, ever.
- Not a Redis replacement for pub/sub, streams, or Lua scripting.
- Not multi-region. Single Kubernetes cluster, single failure domain.

---

## Status

Built milestone by milestone. Each is complete, tested and benchmarked before
the next begins.

| | Milestone | Status |
|---|---|---|
| M0 | Test harness and skeleton | **complete** |
| M1 | Single-node core | **complete** |
| M2 | Group actor runtime | **complete** |
| M3 | Membership and ownership | **complete** |
| M4 | Transport and remote routing | **complete** |
| M5 | Replication and failure handling | **complete** |
| M6 | Rebalancing and Kubernetes lifecycle | not started |
| M7 | Observability and hardening | not started |

Methods belonging to a milestone that has not run return `ErrNotImplemented`.
That is a typed, testable declaration of scaffolding, not an accident.

---

## Repository layout

```
peercache.go      Cache construction, lifecycle
namespace.go      Namespace handle
group.go          Group handle
router.go         Queue-message routing with owner batching
types.go          Shared public types
errors.go         Sentinel errors
options.go        Functional options and partitioners

internal/
  core/           Type definitions shared by the public and internal packages
  clock/          Real and fake clock. Nothing else calls time.Now.
  testkit/        In-process cluster harness with fault injection
  store/          Sharded owned store, TTL, versions          (M1)
  l1/             Non-owned cache, TTL and version discipline (M1)
  l1/eval/        Cache-library comparison (nested module)    (M1)
  codec/          Codec registry, LZ4 framing                 (M1)
  singleflight/   Collapses concurrent loads of one key       (M1)
  actor/          Group activation, mailbox, lifecycle        (M2)
                  Records persist state; activations hold the goroutine
  ownership/      Rendezvous hashing, ring epochs             (M3)
  membership/     Gossip membership over memberlist           (M3)
  transport/      gRPC client and server                      (M4)
  replication/    Replica fan-out, promotion, catch-up        (M5)
  rebalance/      Move planning and streaming transfer        (M6)
  metrics/        Prometheus and OTel emitters                (M7)

proto/            gRPC service definitions                   (M4)
examples/         One runnable example per milestone
docs/             api.md, benchmarks.md, decisions/
```

---

## Testing

Everything is deterministic. Time only moves when a test moves it, randomness
comes from a seed that is printed on failure, and no test sleeps.

```bash
make test          # fast feedback
make test-race     # the gate: -race -count=2
make vet lint      # go vet and staticcheck
make examples      # run every example to completion; they assert what they show
make bench         # benchmarks, 5 runs
make fuzz          # fuzz the frame parser
make bench-l1-eval # re-run the cache-library comparison (nested module)

bash scripts/mutation-audit.sh   # break each invariant, check its test notices
make ci            # exactly what the pipeline runs
```

On Windows without GNU make, `mingw32-make` works, or run the `go` commands
directly.

To reproduce a failure:

```bash
PEERCACHE_SEED=1234 go test ./... -run TestThatFailed
```

### The harness

`internal/testkit` builds N in-process nodes over a fake network and a fake
clock, with fault injection for drop, delay, reorder, partition, isolation,
graceful kill, hard kill and revival. Teardown asserts three things a harness is
uniquely placed to check: no clock misuse, no timer left armed, and no leaked
goroutine.

Simulated time is free. The M0 acceptance scenario — five nodes, a 3/2
partition, an hour advanced on each side of a heal — runs in about **1.5 ms** of
wall clock against a 50 ms budget.

### Rules the tests enforce on themselves

`discipline_test.go` reads the repository as text and fails the build on:

- any `time.Sleep`, anywhere;
- any direct `time.Now`, `time.After`, `time.NewTimer`, `time.NewTicker` or
  `time.AfterFunc` outside `internal/clock`;
- any `context.WithTimeout` or `context.WithDeadline` outside `internal/clock`,
  because both read the wall clock and make deadline behaviour untestable;
- the `log` package;
- an `internal/` package importing the root package;
- a package without a `doc.go`;
- a leftover TODO marker.

---

## Documentation

- [`docs/api.md`](docs/api.md) — the public API, with runnable examples
- [`docs/benchmarks.md`](docs/benchmarks.md) — numbers, with the machine they
  were measured on
- [`docs/verification.md`](docs/verification.md) — which acceptance criteria
  were mutation-tested, and how each failed when the behaviour was broken on
  purpose
- [`docs/decisions/`](docs/decisions/) — one file per significant choice, plus
  `open.md` for everything still guessed at
- [`SPEC.md`](SPEC.md) — the build plan
- [`CLAUDE.md`](CLAUDE.md) — the non-negotiable constraints

---

## Requirements

Go 1.25 or later.

The floor was 1.23 until M3. Adding memberlist raised it: its transitive graph
(`x/net`, `x/sys`, `miekg/dns`) has moved past 1.23, and holding those back to
keep the older floor would mean pinning the modules that receive security
patches. [`docs/decisions/0013-go-floor-raised-to-1.25.md`](docs/decisions/0013-go-floor-raised-to-1.25.md)
records the trade.
