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
- **Async replication has a data-loss window** of approximately X ms under
  normal conditions. *(To be filled in from M5 measurements. Until then the
  honest answer is: unmeasured.)*
- **Reads of non-owned keys may be stale** by up to the configured L1 TTL.
  Invalidation broadcasts are a latency optimisation and may be lost;
  correctness never depends on one arriving.
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
| M2 | Group actor runtime | not started |
| M3 | Membership and ownership | not started |
| M4 | Transport and remote routing | not started |
| M5 | Replication and failure handling | not started |
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
- [`docs/decisions/`](docs/decisions/) — one file per significant choice, plus
  `open.md` for everything still guessed at
- [`SPEC.md`](SPEC.md) — the build plan
- [`CLAUDE.md`](CLAUDE.md) — the non-negotiable constraints

---

## Requirements

Go 1.23 or later.
