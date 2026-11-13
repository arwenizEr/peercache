# PeerCache

[![Go Reference](https://pkg.go.dev/badge/github.com/arwenizEr/peercache.svg)](https://pkg.go.dev/github.com/arwenizEr/peercache)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An embedded, partitioned, in-memory state layer for Go services that run as
several replicas — with no separate cache tier.

Every pod embeds the library. Each key belongs to exactly one pod, chosen by
rendezvous hashing over the live membership. Reads hit local memory when the
data is owned locally or recently cached, and go to the owner otherwise.
Mutations and registered computations run on the owner, so a working set is
never shipped to the caller: computation moves to the data.

It is meant for state that is hot, shared and rebuildable — sessions, per-user
or per-tenant aggregates, rate-limit counters, workflow state — where a network
hop to Redis on every access is the cost you want to remove.

```
go get github.com/arwenizEr/peercache
```

Go 1.25 or later.

---

## Contents

- [Quick start](#quick-start)
- [Groups: computation on the owner](#groups-computation-on-the-owner)
- [Running a cluster](#running-a-cluster)
- [Guarantees](#guarantees)
- [What this is not](#what-this-is-not)
- [Observability](#observability)
- [Status](#status)
- [Repository layout](#repository-layout)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

---

## Quick start

A single node needs nothing but an ID. Everything below works unchanged when the
cache is part of a cluster; only construction differs.

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

A namespace with a loader reconstructs missing keys on their owner, once,
however many callers miss at the same moment:

```go
users := peercache.NamespaceOf[User](cache, "users",
	peercache.WithTypedLoader(func(ctx context.Context, key string) (User, time.Duration, error) {
		u, err := db.LoadUser(ctx, key)
		return u, 10 * time.Minute, err
	}))

u, err := users.GetOrLoad(ctx, "user:42")
```

For one key that must change atomically, `SetIfAbsent`, `CompareAndSwap`,
`Increment` and `Touch` run on the key's owner as a single step:

```go
hits, err := ns.Increment(ctx, "rl:user:7", 1, time.Minute) // a rate-limit window
```

`cache.Namespace(name)` is the same API over `[]byte` values, for callers that
bring their own encoding. Missing keys are `peercache.ErrNotFound`; every error
the library returns is matched with `errors.Is`.

## Groups: computation on the owner

A group is a virtual actor: addressable at all times, activated lazily on its
owner, deactivated when idle. While active it runs on a single goroutine, fed by
a bounded mailbox, so every call on one group is serialized — which is why there
is no lock API.

Functions are registered by name at startup and called by name; closures never
cross the network. Every pod must run the same binary and register the same
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

`peercache.NewRouter` batches queue messages by owning group, so ten thousand
messages across three owners become at most three requests per flush.

## Running a cluster

A cluster needs a peer transport (gRPC) and a membership authority (gossip over
memberlist). Both are public packages so the application wires them, and both
listen on their own port.

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

// Open namespaces and register group functions here, then:
if err := cache.CatchUp(ctx); err != nil { // pull the replicas this node should hold
	return err
}
if err := cache.WaitReady(ctx); err != nil {
	return err
}
// ...serve traffic. On shutdown (a PreStop hook), cache.Drain(ctx) hands this
// node's keys to their new owners before the pod stops.
```

In Kubernetes, seeds come from a headless service, readiness comes from
`cache.Ready()`, and the PreStop hook calls `Drain`. [`deploy/`](deploy) has a
Deployment and a StatefulSet with the probes, ports and hooks filled in, and its
[README](deploy/README.md) explains the five things that must be right.
`transport.Options.TLS` enables mutual TLS between peers.

---

## Guarantees

This section is the most useful documentation in the project, so it comes before
everything else that follows.

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
  from a loader or tolerate loss. Opt-in persistence to disk or object storage
  is designed (`docs/decisions/0029-persistence-as-a-segment-log.md`) and being
  built as M14; until it ships, this holds for every namespace.
- **Async replication has a data-loss window** averaging **≈0.1 ms**, p99 under
  1.5 ms, measured at RF=3 over gRPC (see `docs/benchmarks.md`). A write
  acknowledged inside it exists on one node only. `WithSyncReplication()` closes
  the window while the writer can see its replicas, at the cost of bounding a
  group's write rate by its slowest replica.
- **A synchronous write can still be acknowledged below its replication factor.**
  Replication targets come from the ranking over the *writer's own membership
  view*, so a node cut off from the cluster eventually detects every peer as
  failed, ranks itself the owner of everything, places the one copy it can reach
  and returns `nil`. The caller asked for three copies, got one, and cannot tell.
  The window lasts as long as the partition. `Stats().DegradedWrites` and
  `peercache_degraded_writes_total` count it and are the only signal that
  separates it from a healthy write — **alert on any non-zero value**. The
  neighbouring case is correct and worth knowing: while the writer still *sees*
  its replicas but cannot reach them, the write fails rather than being
  acknowledged. A namespace opened with `WithRequireReplicationFactor()` refuses
  these writes with `ErrUnderReplicated` instead — trading availability during a
  partition, and on any cluster smaller than the factor, for a `nil` that always
  means the factor was reached. Off by default.
- **With a memory budget, a node over it sheds data rather than being
  OOM-killed.** `WithMemoryBudget` caps the owned, replica and L1 tiers. Over it,
  the node sheds to nine tenths: L1 first, then replicas, then owned data — the
  last of which is data loss, by design, counted in `Stats().ShedOwned`. It is
  what stops a failover cascade, where survivors inherit a dead node's share,
  exceed their limits and die in turn. A shed key that is written again is never
  served at its older value after a failover; that holds whether or not the
  removal sent to its replicas arrives. The trade is data for liveness: in the
  cascade test, three deaths of five at RF=2 left two survivors under budget
  and 1152 of 1500 keys readable, where without a budget all 1500 survived on
  nodes holding 1.9× the budget — which in a real pod is three OOM kills. No
  budget is the default, and then nothing is shed.
- **With join probation, a crashlooping pod moves no keys.** `WithJoinProbation`
  keeps a node out of the ownership ranking until it has been up for the given
  time, so a pod that restarts every few seconds is never ranked and its
  restarts cause no rebalancing and no "not found" for data the healthy nodes
  hold. A new pod carries no share for that long; a cold start, where every node
  is young, ranks them all. Off by default.
- **A slow node slows only its own keys.** A node whose every message is
  delayed — a CPU-throttled pod — delays reads of the keys it owns and writes
  that replicate to it synchronously, and nothing else: reads of keys owned
  elsewhere and asynchronous writes complete without waiting on it. Whether it is
  judged dead is memberlist's call, made with a 500 ms probe timeout and a
  suspicion window of seconds; the test harness does not model that part.
- **Every namespace must be opened on every node, with the same options, before
  serving.** A namespace not opened locally is created on the first routed write
  with *default* options — replication factor 1 — and options cannot be changed
  afterwards. That node then silently replicates nothing for it. A five-node
  cluster where one node had opened the namespace placed 17% of the copies RF=3
  implies, with every write reporting success. `Stats().ImplicitNamespaces`
  counts the misconfiguration; any non-zero value deserves an alert.
- **A restarted node must call `CatchUp` before it serves traffic.** This is the
  sharpest edge in the system. Replication is a stream of writes, not a state
  transfer, so a node that restarts receives only the keys written *after* it
  came back — while the ownership ranking hands it back its keys immediately. It
  will answer "not found", with total confidence, for data sitting safely on the
  node that covered for it. In the chaos test that presented as 558 of 600 keys
  lost, none of them actually gone. Readiness stays false until `CatchUp` has
  run, so a readiness probe wired to `Ready()` keeps traffic away until then.
- **A key's TTL holds across failover, drain and catch-up.** Replicas hold the
  owner's deadline, not a duration, so a promoted, drained or caught-up key
  expires when it was written to, and a key that expired on its owner does not
  come back on the node that replaces it. Expired replicas are reclaimed on the
  cache's tick.
- **Reads of non-owned keys may be stale** by up to the configured L1 TTL.
  Invalidation broadcasts are a latency optimisation and may be lost;
  correctness never depends on one arriving. A namespace opened with
  `WithReplicaReads` lets replicas answer too, and its reads may be stale by up
  to the L1 TTL plus the replication delay — measured, not bounded.
- **Replication survives the loss of RF−1 nodes, and no more.** At RF=3, a
  chaos run of 30 kills and 30 restarts over 60 simulated seconds lost no
  synchronously written key. At RF=2, killing both holders of a key loses it —
  that is what RF=2 means, and it is asserted as a test rather than left as a
  caveat. Survivors are always correct: losing data is possible, corrupting it
  is not.
- **That guarantee describes a cluster at rest, not one still settling.** A
  membership change — a node joining, a partition healing — moves keys, and until
  the copies have been restored a key can be *readable* while fewer than RF nodes
  hold it. Readable and replicated are not the same condition: one node answering
  is enough for the first and not for the second. Losing RF−1 nodes during that
  window can therefore lose a key that would have survived once the cluster had
  settled.

  `CatchUp` is what closes the window, which is why it is an explicit call. The
  operational rule is the same one restarts already have: after a membership
  change, catch up before you rely on the replication factor again.
  `Ready().ReplicasCurrent`, exported as `peercache_replicas_current`, says when
  a node has done so under the view it holds now. It sits beside readiness rather
  than inside it, because a node can serve correctly long before the cluster is
  back at its factor.
- **A membership change moves replicas rather than discarding them.** When the
  ranking reassigns a key, the node holding a copy hands it to the nodes that now
  rank for it before dropping its own; if it cannot place it anywhere, it keeps
  it. Dropping on the local ranking alone lost keys whenever a partition healed,
  because the copies had been placed at positions computed without the absent
  node — 20 of 94 acknowledged writes in the reduced case. `ReplicasRehomed` and
  `ReplicasRetained` account for every moved copy.
- **Failure detection must be faster than your failure rate.** A cluster losing
  a node every two seconds while taking three to notice never converges. This is
  a deployment constraint the library cannot fix.
- **Throughput degrades to single-pod capacity for a single hot group.** Group
  sharding for hot-key mitigation is not implemented.

The words "strong" and "quorum" do not appear in this API, deliberately.

## What this is not

- Not a database. Nothing is written to disk.
- Not a general distributed transaction system. There are no cross-key or
  cross-group transactions.
- Not a Redis replacement for pub/sub, streams, or Lua scripting.
- Not multi-region. Single Kubernetes cluster, single failure domain.

---

## Observability

PeerCache imports no metrics client library. `cache.MetricsHandler()` serves the
Prometheus text format, `cache.Metrics()` returns plain values for any other
backend, and `cache.Stats()` is the same data as a struct. Beside the usual
throughput and latency series it exports the correctness counters — split-brain
detections, stale-epoch refusals, version conflicts, degraded writes, implicit
namespaces — because a window in a guarantee that is not counted cannot be told
apart from one that does not exist. [`docs/api.md`](docs/api.md) lists every
series and which ones to alert on.

## Status

Pre-1.0: the API is complete for what is described here, and may still change
before `v1.0.0`.

Beyond unit and integration tests, the library is checked by a linearizability
checker over group histories, a deterministic fault simulation (partitions, hard
kills, revivals) run across thousands of seeds nightly, fuzzing of every protocol
message, chaos and rolling-upgrade tests, and a mutation audit that breaks each
guarded behaviour on purpose and confirms a named test fails.
[`docs/verification.md`](docs/verification.md) maps every acceptance criterion
in [`docs/design.md`](docs/design.md) to the test that proves it.

---

## Repository layout

The root directory is the public API. Go does not allow one package to span
directories, so everything a caller imports lives here, everything else lives
under `internal/`, and the tests live under `tests/`. The only test files in the
root are godoc examples, benchmarks and a fuzz target, which belong next to the
code they document or measure.

```
doc.go            Package documentation
peercache.go      Cache construction, lifecycle
namespace.go      Namespace handle
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
pressure.go       The memory budget and the order it sheds in
probation.go      Keeps young nodes out of the ranking

membership/       Gossip membership over memberlist (public)
transport/        gRPC client and server, optional mTLS (public)

internal/
  core/           Type definitions shared by the public and internal packages
  clock/          Real and fake clock. Nothing else calls time.Now.
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
  rebalance/      Move planning
  metrics/        Metric collection, text format, histograms

tests/            Behavioural, cluster, acceptance, chaos and simulation tests,
                  against the public API and the in-process harness
tests/repo/       Tests that read the repository as text: engineering rules,
                  docs and CI agree with the code, an external module builds
proto/            gRPC service definitions
examples/         Runnable programs, one per feature: lifecycle, namespace,
                  groups, ownership, remote, replication, rolling-upgrade,
                  observability, failure-guarantees
deploy/           Kubernetes manifests and the operational guide
scripts/          mutation-audit.sh, clean/ (the `make clean` implementation)
docs/             design.md, api.md, benchmarks.md, verification.md, decisions/
.github/          ci.yml per commit, nightly.yml for the long runs
```

---

## Development

Everything is deterministic. Time only moves when a test moves it, randomness
comes from a seed that is printed on failure, and no test sleeps.

```bash
make test          # fast feedback
make test-race     # the gate: -race -count=2
make vet lint      # go vet and staticcheck
make examples      # run every example to completion; they assert what they show
make bench         # benchmarks, 5 runs
make fuzz          # fuzz the frame parser, the decoder, the codecs, the partitioner
make bench-l1-eval # re-run the cache-library comparison (nested module)

bash scripts/mutation-audit.sh   # break each invariant, check its test notices
make ci            # exactly what the pipeline runs
```

The long-running verification is nightly rather than per-commit, in
`.github/workflows/nightly.yml`: two thousand simulation seeds, repeated
linearizability checks, an hour of soak, ten minutes per fuzz target, and the
mutation audit. A failure there prints the seed or commits the crasher, so it
reproduces with one command:

```bash
PEERCACHE_SEED=1234 go test ./... -run TestThatFailed
```

On Windows without GNU make, `mingw32-make` works, or run the `go` commands
directly.

`internal/testkit` builds N in-process nodes over a fake network and a fake
clock, with fault injection for drop, delay, reorder, partition, isolation,
graceful kill, hard kill and revival. Simulated time is free: five nodes, a 3/2
partition and an hour advanced on each side of a heal run in about **1.5 ms** of
wall clock.

`tests/repo/discipline_test.go` reads the repository as text and fails the build on any
`time.Sleep`; any direct `time.Now`, `time.After`, timer, ticker or
`context.WithTimeout` outside `internal/clock`; the `log` package; an
`internal/` package importing the root package; a package without a `doc.go`;
an undocumented exported symbol; a dependency outside the budget; and a leftover
TODO marker. [`CONTRIBUTING.md`](CONTRIBUTING.md) has the rest.

---

## Documentation

- [pkg.go.dev](https://pkg.go.dev/github.com/arwenizEr/peercache) — API reference
- [`docs/api.md`](docs/api.md) — the public API, with runnable examples and the
  metrics to alert on
- [`deploy/README.md`](deploy/README.md) — running it in Kubernetes
- [`docs/design.md`](docs/design.md) — the design and the order it was built in
- [`docs/benchmarks.md`](docs/benchmarks.md) — numbers, with the machine they
  were measured on
- [`docs/verification.md`](docs/verification.md) — how each acceptance criterion
  is proven, and how each test failed when the behaviour was broken on purpose
- [`docs/decisions/`](docs/decisions/) — one record per significant choice, plus
  `open.md` for the questions still open
- [`CHANGELOG.md`](CHANGELOG.md), [`CONTRIBUTING.md`](CONTRIBUTING.md),
  [`SECURITY.md`](SECURITY.md)

Go 1.25 is the floor because memberlist's transitive graph (`x/net`, `x/sys`,
`miekg/dns`) has moved past older releases, and holding those back would mean
pinning the modules that receive security patches; see
[`docs/decisions/0013-go-floor-raised-to-1.25.md`](docs/decisions/0013-go-floor-raised-to-1.25.md).

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
