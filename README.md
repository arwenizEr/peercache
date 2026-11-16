# PeerCache

[![Go Reference](https://pkg.go.dev/badge/github.com/arwenizEr/peercache.svg)](https://pkg.go.dev/github.com/arwenizEr/peercache)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A shared, partitioned, in-memory state layer that lives inside your Go service.
No cache tier to run: every replica of the service embeds the library, and
together they hold the state.

- **Each key has one owner**, chosen by rendezvous hashing over the live
  members. Reads are served from local memory when this pod owns the key or
  cached it recently, and from the owner otherwise.
- **Computation moves to the data.** Writes and registered functions run on the
  owner, so a working set is never shipped to the caller.
- **Copies on other pods** (async or sync replication) survive pod loss, and
  graceful shutdown hands keys to their next owner.
- **Persistence is opt-in, per namespace**, to a local disk, a shared volume or
  S3.

```
go get github.com/arwenizEr/peercache
```

Requires Go 1.25 or later ([why](docs/decisions/0013-go-floor-raised-to-1.25.md)).

## When to use it

Use it for state that is hot, shared and rebuildable, where a network hop to
Redis on every access is the cost you want to remove: sessions, per-user or
per-tenant aggregates, rate-limit counters, workflow state.

Do not use it for:

- **A system of record.** Without persistence, state is lost when every pod
  holding it dies. With persistence there is still a loss window of one flush
  interval and no cross-key atomicity.
- **Transactions.** Nothing spans two keys or two groups.
- **Pub/sub, streams or scripting.** It is not a Redis replacement.
- **More than one cluster.** One Kubernetes cluster is one failure domain, and
  that is the scope.

## Contents

- [Quick start](#quick-start)
- [Groups: computation on the owner](#groups-computation-on-the-owner)
- [Running a cluster](#running-a-cluster)
- [Persistence](#persistence)
- [Guarantees](#guarantees)
- [Operating it](#operating-it)
- [Stability and verification](#stability-and-verification)
- [Development](#development)
- [Repository layout](#repository-layout)
- [Documentation](#documentation)

## Quick start

A single node needs only an ID. The same code works in a cluster; only the
construction changes.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arwenizEr/peercache"
)

type Session struct {
	UserID string
	Cart   []string
}

func main() {
	ctx := context.Background()

	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close(ctx)

	// A typed namespace encodes values with msgpack. Open every namespace at
	// startup, on every node, with the same options.
	sessions := peercache.NamespaceOf[Session](cache, "sessions",
		peercache.WithReplicationFactor(2))

	if err := sessions.Set(ctx, "s:123", Session{UserID: "u1", Cart: []string{"book"}}, 30*time.Minute); err != nil {
		log.Fatal(err)
	}
	s, err := sessions.Get(ctx, "s:123")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(s.UserID, s.Cart)
}
```

**Loaders.** A namespace with a loader rebuilds a missing key on its owner. The
load runs once, however many callers miss at the same moment:

```go
users := peercache.NamespaceOf[User](cache, "users",
	peercache.WithTypedLoader(func(ctx context.Context, key string) (User, time.Duration, error) {
		u, err := db.LoadUser(ctx, key)
		return u, 10 * time.Minute, err
	}))

u, err := users.GetOrLoad(ctx, "user:42")
```

**Atomic single-key operations.** `SetIfAbsent`, `CompareAndSwap`, `Increment`
and `Touch` each run on the key's owner as one step:

```go
hits, err := ns.Increment(ctx, "rl:user:7", 1, time.Minute) // a rate-limit window
```

**Raw bytes.** `cache.Namespace(name)` is the same API over `[]byte` values, for
callers that bring their own encoding.

**Errors.** A missing key is `peercache.ErrNotFound`. Match every error the
library returns with `errors.Is`.

## Groups: computation on the owner

A group is a virtual actor. It is addressable at any time, activated on its
owner when first called, and deactivated when idle. An active group runs on one
goroutine fed by a bounded mailbox, so calls on one group never overlap. That
is why there is no lock API.

Functions are registered by name at startup and called by name. Closures never
cross the network, so every pod must run the same binary and register the same
functions.

```go
err := peercache.RegisterTypedGroupFunc(cache, "add",
	func(ctx context.Context, s peercache.GroupState, n int) (int, error) {
		total := 0
		if b, ok := s.Get("total"); ok {
			total, _ = strconv.Atoi(string(b))
		}
		total += n
		s.Set("total", []byte(strconv.Itoa(total)))
		return total, nil
	})

// Runs on whichever pod owns "tenant:acme", wherever this call is made.
total, err := peercache.CallTyped[int, int](ctx, cache.Group("tenant:acme"), "add", 5)
```

`peercache.NewRouter` batches queue messages by owning group: ten thousand
messages across three owners become at most three requests per flush.

## Running a cluster

A cluster needs two more pieces: a peer transport (`transport`, gRPC) and a
membership source (`membership`, gossip over memberlist). The application
creates both, and each listens on its own port.

```go
import (
	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/membership"
	"github.com/arwenizEr/peercache/transport"
)

self := peercache.Node{ID: os.Getenv("NODE_ID"), Addr: os.Getenv("DATA_ADDR")}

l, err := net.Listen("tcp", self.Addr) // gRPC between caches
if err != nil {
	return err
}
tr, err := transport.New(transport.Options{Self: self})
if err != nil {
	return err
}
gossip, err := membership.New(membership.Options{
	Self:     self,
	BindAddr: os.Getenv("GOSSIP_ADDR"), // memberlist
})
if err != nil {
	return err
}

cache, err := peercache.New(
	peercache.WithNodeID(self.ID),
	peercache.WithTransport(tr),
	peercache.WithListener(l),
	peercache.WithMembership(gossip),
	peercache.WithSeeds(seeds...), // gossip addresses of existing peers
	peercache.WithActivationDelay(3*time.Second),
)
if err != nil {
	return err
}

// Open every namespace and register every group function here, then:
if err := cache.CatchUp(ctx); err != nil { // pull the replicas this node should hold
	return err
}
if err := cache.WaitReady(ctx); err != nil {
	return err
}
// Serve traffic. On shutdown (a PreStop hook), cache.Drain(ctx) hands this
// node's keys to their new owners before the pod stops.
```

In Kubernetes:

- seeds come from a headless service,
- the readiness probe reads `cache.Ready()`,
- the PreStop hook calls `Drain`.

[`deploy/`](deploy) has a Deployment and a StatefulSet with the probes, ports
and hooks filled in, and [`deploy/README.md`](deploy/README.md) lists the five
things that must be right. `transport.Options.TLS` turns on mutual TLS between
peers.

## Persistence

By default a namespace lives only in memory. `WithPersistence` writes it behind
to a blob store, so its keys survive the loss of every pod that held them:

```go
store, err := persist.Dir("/var/lib/peercache") // a volume every pod can reach
if err != nil {
	return err
}
orders := cache.Namespace("orders", peercache.WithPersistence(store))
```

- Writes are acknowledged first and flushed every 100 ms
  (`persist.WithFlushInterval`), so a crash loses at most the last interval.
  On `persist.Dir`, a write is durable at p99 105 ms after its acknowledgement.
  `Close` and `Drain` flush before they return.
- A node that comes to own keys it does not hold restores them from storage on
  first access. Calls to that partition wait for the restore, within their
  deadline.
- Backends: `persist.Dir` (local or shared disk), `persist.Memory` (tests), and
  `persist/s3` (S3, MinIO, R2, Ceph RGW). `persist/s3` is a separate module, so
  only programs that use it depend on the AWS SDK. On an object store, use a
  flush interval of a second or more; [`docs/api.md`](docs/api.md#persistence--withpersistence)
  has the request-cost table.

## Guarantees

These are the promises and their limits. Each one is covered by a named test
([`docs/verification.md`](docs/verification.md)), and the numbers come from
[`docs/benchmarks.md`](docs/benchmarks.md).

### Consistency

- **Calls on one group are serialized while it has one activation.** This is
  structural: one goroutine per active group.
- **Membership is eventually consistent (AP).** A network partition can briefly
  produce two activations of one group. Conflicting writes are detected via
  epochs and counted, not merged. Activation delay narrows the window; no
  setting closes it.
- **Reads of keys owned elsewhere may be stale** by up to the L1 TTL.
  Invalidation messages only make updates show up sooner; correctness never
  depends on one arriving. With `WithReplicaReads`, replicas answer reads too,
  and staleness grows by the replication delay. That delay is measured, not
  bounded.
- **TTLs hold across failover, drain and catch-up.** Replicas store the owner's
  deadline, not a duration, so a key expires when it was meant to wherever it
  ends up.

The words "strong" and "quorum" are absent from the API on purpose.

### Durability and replication

- **Without persistence, a pod killed ungracefully loses the keys it owned**,
  except those with a current replica elsewhere. Graceful shutdown hands them
  over. A namespace without persistence must be rebuildable from a loader or
  tolerate loss.
- **Replication survives the loss of RF−1 nodes, and no more.** At RF=3, a run
  of 30 kills and 30 restarts lost no synchronously written key. At RF=2,
  killing both holders of a key loses it. Survivors are never corrupted.
- **Async replication has a loss window**: about 0.1 ms on average, p99 under
  1.5 ms at RF=3 over gRPC. A write acknowledged inside it exists on one node.
  `WithSyncReplication()` closes the window while the writer can see its
  replicas, and limits a group's write rate to what its slowest replica can
  take.
- **A synchronous write can be acknowledged with fewer copies than asked for.**
  A node cut off from the cluster eventually considers itself owner of
  everything. It writes the one copy it can reach and returns `nil`.
  `Stats().DegradedWrites` (`peercache_degraded_writes_total`) is the only
  signal: **alert on any non-zero value**. If the writer still sees its
  replicas but cannot reach them, the write fails instead.
  `WithRequireReplicationFactor()` refuses these writes with
  `ErrUnderReplicated`, giving up availability during a partition, and on any
  cluster smaller than the factor, in exchange for a `nil` that always means the
  factor was reached. Off by default.

### Membership changes

- **A restarted node must call `CatchUp` before serving.** Replication streams
  writes; it does not transfer state. A restarted node gets its keys back in the
  ranking at once but holds only keys written after it returned, so without
  catch-up it answers "not found" for data that is safe on another node.
  `Ready()` stays false until `CatchUp` has run, so a readiness probe keeps
  traffic away.
- **Keys move with their copies.** When the ranking reassigns a key, the node
  holding a copy hands it to the new holders before dropping its own, and keeps
  it if it cannot place it. `ReplicasRehomed` and `ReplicasRetained` count the
  moves.
- **While a change settles, the replication factor is not yet restored.** A key
  can be readable from one node before RF nodes hold it again, and losing RF−1
  nodes inside that window can lose it. `CatchUp` closes the window.
  `Ready().ReplicasCurrent` (`peercache_replicas_current`) reports when this node
  has caught up under its current view.

### Overload and slow nodes

- **With `WithMemoryBudget`, a node over budget sheds data rather than being
  OOM-killed.** It sheds down to nine tenths: L1 first, then replicas, then
  owned keys. Shedding owned keys is data loss, counted in `Stats().ShedOwned`;
  in a persistent namespace a shed key stays in storage and comes back on its
  next access. The budget stops failover cascades, where survivors inherit a
  dead node's share and die in turn. Off by default.
- **With `WithJoinProbation`, a crashlooping pod moves no keys.** A node stays
  out of the ranking until it has been up for the given time. A cold start,
  where every node is young, ranks them all. Off by default.
- **A slow node slows only its own keys**: reads it owns and writes that
  replicate to it synchronously. Everything else completes without it.
- **A single hot group is limited to one pod's throughput.** Groups are not
  sharded.

## Operating it

Four rules. The library enforces or reports each one, but cannot fix them for
you:

1. **Open every namespace, with the same options, on every node, before joining
   the cluster.** A peer's request for a namespace this node has not opened is
   refused with `ErrActivationPending` and counted in
   `Stats().UnopenedNamespaceRefusals`.
2. **Call `CatchUp` before serving, and wire readiness to `Ready()`.**
3. **Alert on `DegradedWrites` and `UnopenedNamespaceRefusals`.**
4. **Detect failures faster than they happen.** A cluster that loses a node
   every two seconds but takes three to notice never converges.

**Observability.** PeerCache imports no metrics client. `cache.MetricsHandler()`
serves Prometheus text, `cache.Metrics()` returns plain values for any other
backend, and `cache.Stats()` is the same data as a struct. Along with throughput
and latency it exports the correctness counters (split-brain detections,
stale-epoch refusals, version conflicts, degraded writes) because a gap in a
guarantee that is not counted looks the same as no gap.
`cache.DebugHandler()` serves a live dashboard, and the `otel` module bridges
the series to OpenTelemetry. [`docs/api.md`](docs/api.md) lists every series and
which ones to alert on.

## Stability and verification

Stable since `v1.0.0`. [Decision 0042](docs/decisions/0042-what-v1-promises.md)
spells out what that means:

- The API you call only grows in minor releases.
- The interfaces you implement (`Transport`, `Logger`, `persist.BlobStore`, …)
  keep their method sets until v2.
- The interfaces the library returns (`Cache`, `Namespace`, …) may gain
  methods, so test with `peercachetest` rather than your own fake.
- Two adjacent minor releases run in one cluster and read each other's
  persisted data.
- Debug JSON fields and metric names are never renamed or removed.

Besides unit and integration tests, the library is checked by:

- a linearizability checker over group histories,
- a deterministic fault simulation (partitions, hard kills, revivals) over
  thousands of seeds,
- fuzzing of every protocol message,
- chaos and rolling-upgrade tests,
- scenarios on a kind cluster,
- a mutation audit that breaks each guarded behaviour on purpose and checks
  that a named test fails.

[`docs/verification.md`](docs/verification.md) maps every acceptance criterion in
[`docs/design.md`](docs/design.md) to the test that proves it.

## Development

Tests are deterministic. Time moves only when a test moves it, randomness comes
from a seed printed on failure, and no test sleeps.

```bash
make test          # fast feedback
make test-race     # the gate: -race -count=2
make vet lint      # go vet and staticcheck
make examples      # run every example; each asserts what it shows
make bench         # benchmarks, 5 runs
make fuzz          # fuzz the frame parser, the decoder, the codecs, the partitioner
make bench-l1-eval # re-run the cache-library comparison (nested module)
make ci            # what the pipeline runs

bash scripts/mutation-audit.sh   # break each invariant, check a test notices
```

On Windows without GNU make, use `mingw32-make` or run the `go` commands
directly.

The long runs live in `.github/workflows/nightly.yml`: two thousand simulation
seeds, repeated linearizability checks, an hour of soak, ten minutes per fuzz
target and the mutation audit. A failure prints its seed, so it reproduces with
one command:

```bash
PEERCACHE_SEED=1234 go test ./... -run TestThatFailed
```

`internal/testkit` runs N nodes in one process over a fake network and a fake
clock, and can drop, delay, reorder, partition, isolate, kill (gracefully or
not) and revive them. Simulated time costs nothing: five nodes, a 3/2 partition
and an hour on each side of the heal take about **1.5 ms** of wall clock.

`tests/repo/discipline_test.go` reads the repository as text and fails on
`time.Sleep`; on `time.Now`, timers, tickers or `context.WithTimeout` outside
`internal/clock`; on the `log` package; on an `internal/` package importing the
root; on a package without a `doc.go`; on an undocumented exported symbol; on a
dependency outside the budget; and on a leftover TODO marker.
[`CONTRIBUTING.md`](CONTRIBUTING.md) has the rest.

## Repository layout

The root directory is the public API. Go does not let one package span
directories, so everything a caller imports is here, everything else is under
`internal/`, and the tests are under `tests/`. The only test files in the root
are godoc examples, benchmarks and a fuzz target.

<details>
<summary>Every file and package</summary>

```
doc.go            Package documentation
peercache.go      Cache construction, lifecycle
namespace.go      Namespace handle
ignoredopts.go    Warning for options given to an already open namespace
batchload.go      BatchGetOrLoad: one request per owner, one batch load per owner
refresh.go        WithRefreshAhead: background reloads of keys read near expiry
trace.go          WithTracer: spans around peer requests and loads
cluster.go        DescribeCluster: every member's view, replica digests compared
group.go          Group handle
types.go          Shared public types
errors.go         Sentinel errors
options.go        Functional options and partitioners
context.go        Nil-context guard at every API boundary
typed.go          NamespaceOf[T]: values as a Go type
typed_group.go    Typed group function arguments and results
atomic.go         SetIfAbsent, CompareAndSwap, Increment and Touch, from the
                  method to the owner's store

router.go         Queue-message routing with owner batching
remote.go         Owner lookup and the remote read/write path
handler.go        Server side: dispatches an inbound RPC
replicate.go      Replica fan-out, sync and async
promote.go        Reconciles held state against membership
catchup.go        Pulls the replicas this node should hold
invalidate.go     Best-effort L1 invalidation broadcast
drain.go          Hands work away without closing
handoff.go        Moves replicas on a membership change rather
                  than dropping them
ready.go          Readiness, its three conditions, and whether
                  replicas are current for this view
observe.go        Stats and the metric series they export
describe.go       Describe, DescribeKey and the debug handler
pressure.go       The memory budget and the order it sheds in
probation.go      Keeps young nodes out of the ranking
persistence.go    Write-behind flush of persistent namespaces to a blob store

membership/       Gossip membership over memberlist (public)
transport/        gRPC client and server, optional mTLS (public)
persist/          Blob-store interface for persistent namespaces, the
                  memory and directory backends, and the contract suite (public)
persist/s3/       S3 and S3-compatible backend, a nested module (public)
otel/             OpenTelemetry bridge for the metric series, a nested
                  module (public)
peercachetest/    An in-process cluster for applications' own failover
                  tests: simulated network, membership and clock (public)

internal/
  core/           Type definitions shared by the public and internal packages
  clock/          Real and fake clock. Nothing else calls time.Now.
  hlc/            Hybrid logical clock ordering persistent writes
  digest/         Running per-partition digests of what a store holds
  pager/          Sorted key lists for catch-up and hand-off passes, sorted once per pass
  debugui/        The live dashboard DebugHandler serves at /ui
  hotkeys/        Sampled Space-Saving estimate of the most accessed keys
  testkit/        In-process cluster harness with fault injection
  store/          Sharded owned store, TTL, versions
  l1/             Non-owned cache, TTL and version discipline
  l1/eval/        Cache-library comparison (nested module)
  codec/          Codec registry, LZ4 framing
  singleflight/   Collapses concurrent loads of one key
  actor/          Group activation, mailbox, lifecycle
  ownership/      Rendezvous hashing, membership epochs
  wire/           Protocol messages, bounded decoders, frame
                  validation. The only code here that interprets
                  bytes it did not write.
  replication/    Replica store, version stamps, conflict detection
  segment/        Storage format of persistent namespaces, bounded decoding
  rebalance/      Move planning
  metrics/        Metric collection, text format, histograms

tests/            Behavioural, cluster, acceptance, chaos and simulation tests,
                  against the public API and the in-process harness
tests/repo/       Tests that read the repository as text: engineering rules,
                  docs and CI agree with the code, an external module builds
proto/            gRPC service definitions
examples/         Runnable programs, one per feature: lifecycle, namespace,
                  groups, ownership, remote, replication, rolling-upgrade,
                  observability, failure-guarantees, and shop (the kind
                  scenarios)
deploy/           Kubernetes manifests and the operational guide
scripts/          mutation-audit.sh, clean/ (the `make clean` implementation)
docs/             design.md, api.md, benchmarks.md, verification.md, decisions/
.github/          ci.yml per commit, nightly.yml for the long runs
```

</details>

## Documentation

- [pkg.go.dev](https://pkg.go.dev/github.com/arwenizEr/peercache): API reference
- [`docs/api.md`](docs/api.md): the public API, with runnable examples and the
  metrics to alert on
- [`docs/guide.tr.md`](docs/guide.tr.md): a Turkish guide with nine use cases
  (sessions, rate limits, read-through, groups, workflows, queues, orders,
  hot keys, idempotency), failure scenarios and operations
- [`deploy/README.md`](deploy/README.md): running it in Kubernetes
- [`docs/design.md`](docs/design.md): the design and the order it was built in
- [`docs/benchmarks.md`](docs/benchmarks.md): numbers, with the machine they
  were measured on
- [`docs/verification.md`](docs/verification.md): how each acceptance criterion
  is proven, and how each test failed when the behaviour was broken on purpose
- [`docs/decisions/`](docs/decisions/): one record per significant choice, plus
  `open.md` for open questions
- [`CHANGELOG.md`](CHANGELOG.md), [`CONTRIBUTING.md`](CONTRIBUTING.md),
  [`SECURITY.md`](SECURITY.md)

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
