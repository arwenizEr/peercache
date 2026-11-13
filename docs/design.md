# PeerCache — Design

Embedded distributed L1 cache and stateful aggregation engine for Go.

This is the design the library was built from. Section 0 states the principles
it does not compromise on. Section 4 lists the milestones in
the order they were built, each with the acceptance criteria its tests prove
(`docs/verification.md` maps every criterion to a named test). Decisions taken
along the way are recorded in [`decisions/`](decisions).

---

## 0. Principles

These were argued out before any code was written, and the rest of this
document, the code and the decision records assume them. Records in
`decisions/` cite them as "principle N".

### What this is not

- Not a database. State is **lossy by default** (principle 4), and persistence,
  where a namespace opts in, has a measured loss window and no cross-key
  atomicity.
- Not a general distributed transaction system. There are no cross-group
  transactions.
- Not a Redis replacement for pub/sub, streams, or Lua scripting.
- Not multi-region. Single Kubernetes cluster, single failure domain.

### Design principles

1. **Ownership is rendezvous (HRW) hashing**, not a consistent hash ring. One
   algorithm, one place, `internal/ownership`. The sorted node list is cached
   and recomputed only on membership change.

2. **Groups are virtual actors.** A group is addressable at all times but
   *activated* lazily on its owner and *deactivated* after idle timeout. All
   operations on an activated group run on a single goroutine, serialized
   through a bounded mailbox. This is why there is no lock API.

3. **Remote code is registered, never serialized.** Closures do not cross the
   network. Functions and loaders are registered by name at startup; RPCs carry
   `(name, []byte args)`. All pods run the same binary — this is assumed.

4. **Durability: none by default; opt-in persistence per namespace.** The owned
   store is memory-only unless a namespace is opened with `WithPersistence`.
   Without it, graceful shutdown transfers ownership and streams state and
   ungraceful termination loses it, so such a namespace must be reconstructible
   from a loader or tolerate loss. With it, owners flush coalesced records as
   immutable segments to a pluggable blob store — memory, local disk, or a cloud
   object store through a separate module — and the loss window is the flush
   interval, measured. Changed by the project owner on 2026-11-13; see
   `decisions/0029-persistence-as-a-segment-log.md`.

5. **Consistency ceiling is per-group serializability**, provided by the single
   activation. Membership is gossip-based and therefore AP: a network partition
   can produce two activations of one group. This window is *narrowed* by
   activation delay and *detected* via epochs, not eliminated. The words
   "strong" and "quorum" are not used in docs or API names.

6. **Non-owned L1 entries are TTL-bounded and version-stamped.** Invalidation
   broadcast is a latency optimization only. Correctness never depends on an
   invalidation message arriving.

7. **Replication is async by default**, configurable per namespace. The data
   loss window is a documented, measured number — not an unknown.

### Engineering rules

- Go 1.25+ (it was 1.23 until memberlist's dependencies moved past it; see
  `decisions/0013-go-floor-raised-to-1.25.md`). Generics where they remove
  reflection, not for decoration.
- Dependency budget: `memberlist`, `grpc`, a local cache library, a codec, and
  test deps. Adding anything else requires a decision record first.
- Every exported symbol has a doc comment. Every package has a `doc.go`.
- **Every concurrency-touching test runs under `-race`.**
- No `time.Now()` outside `internal/clock`. No `time.Sleep` in tests, ever —
  use the fake clock. A sleeping test is a broken test.
- No `panic` in library code paths. No `log` package; a `Logger` interface with
  a no-op default.
- Errors wrap with `%w` and are checked with `errors.Is/As`. Sentinel errors
  live in the root package.
- Context deadlines propagate across RPC boundaries and into group execution.
- Benchmarks live next to the code they measure and are run in CI for
  regression, not just written once.

### Definition of done

A milestone in section 4 is complete when:

- `go test ./... -race` passes.
- Its acceptance criteria are each covered by a named test that would fail if
  the behavior regressed.
- `go vet` and `staticcheck` are clean.
- Its benchmarks exist and their numbers are recorded in `docs/benchmarks.md`
  with the machine spec.
- Public API additions are in `docs/api.md` with a runnable example.

When a decision is genuinely ambiguous, the question goes in
`decisions/open.md` and the simplest option that can be reversed later is
picked. Nothing touching correctness under failure is guessed silently.

---

## 1. Problem statement

A Go service deployed as N replicas in Kubernetes needs shared mutable state —
session data, per-user aggregates, tenant counters, workflow state — with
sub-millisecond access. The conventional answer is Redis: a separate cluster, a
network hop on every access, an operational burden, and a serialization tax on
every read.

PeerCache removes the separate tier. Every pod embeds the library. Each key
belongs to exactly one pod. Reads hit local memory when the data is local or
recently cached; otherwise they route to the owner. Mutations and computations
execute on the owner, so the working set is never shipped to the caller.

The API surface a user sees should be roughly as small as a `map` plus a
`Group` handle.

---

## 2. Repository layout

```
peercache/
  peercache.go           Cache construction, lifecycle, options
  namespace.go           Namespace handle: typed Get/Set/GetOrLoad
  group.go               Group handle: State, Call, Keys, Flush
  router.go              Queue-message routing with owner batching
  errors.go              Sentinel errors
  options.go             Functional options

  internal/
    clock/               Real + fake clock. Nothing else calls time.Now.
    ownership/           Rendezvous hashing, ring epochs, ownership table
    membership/          MembershipAuthority iface; gossip impl (memberlist)
    store/               Owned store, replica store, shard maps
    l1/                  Local cache wrapper, TTL + version discipline
    actor/               Group activation, mailbox, lifecycle, timers
    transport/           gRPC client + server, connection pool, mTLS
    replication/         Replica fan-out, promotion, catch-up
    rebalance/           Partition move planning + streaming transfer
    codec/               Codec registry, compression
    metrics/             Prometheus + OTel emitters
    testkit/             In-process cluster harness, network fault injection

  proto/                 gRPC service definitions
  examples/              Runnable examples, one per milestone
  docs/
    api.md
    benchmarks.md
    decisions/           One file per significant choice + open.md
```

---

## 3. Core interfaces

Write these first, with compiling no-op implementations, before any logic.
Signatures may evolve, but changes go in `docs/decisions/`.

### 3.1 Cache and namespaces

```go
type Cache interface {
    Namespace(name string, opts ...NamespaceOption) Namespace
    Group(id string) Group
    RegisterGroupFunc(name string, fn GroupFunc) error
    Stats() Stats
    Close(ctx context.Context) error
}

type Namespace interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
    BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)
    BatchSet(ctx context.Context, kv map[string][]byte, ttl time.Duration) error
    GetOrLoad(ctx context.Context, key string) ([]byte, error)
}
```

`GetOrLoad` has no loader argument. The loader is registered on the namespace at
construction, so it exists on every pod by name and the owner can execute it.
Concurrent misses on the owner collapse via singleflight.

```go
type Loader func(ctx context.Context, key string) (val []byte, ttl time.Duration, err error)

func WithLoader(l Loader) NamespaceOption
func WithPartitioner(p Partitioner) NamespaceOption   // see 3.4
func WithReplicationFactor(rf int) NamespaceOption    // 1..3
func WithCodec(name string) NamespaceOption
func WithL1TTL(d time.Duration) NamespaceOption       // non-owned copies
```

### 3.2 Groups

A group is a named collection of keys guaranteed to share an owner, and the unit
of activation and serialization.

```go
type Group interface {
    ID() string
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, val []byte) error
    Delete(ctx context.Context, key string) error
    Keys(ctx context.Context) ([]string, error)
    Items(ctx context.Context) (map[string][]byte, error)
    Call(ctx context.Context, fn string, arg []byte) ([]byte, error)
    Flush(ctx context.Context) error
}
```

There is no `Lock`. Serialization is structural.

### 3.3 Registered group functions

```go
type GroupFunc func(ctx context.Context, s GroupState, arg []byte) ([]byte, error)

// GroupState is only valid for the duration of one turn. It must not escape.
type GroupState interface {
    ID() string
    Get(key string) ([]byte, bool)
    Set(key string, val []byte)
    Delete(key string)
    Keys() []string

    ScheduleTimer(name string, after time.Duration, fn string, arg []byte)
    CancelTimer(name string)
    Emit(event Event)
}
```

`GroupState` methods take no context and return no error: they are pure local
memory access inside the actor turn. This is the whole point of moving
computation to the data. Enforce non-escape with a generation counter checked on
every call, returning a panic-free error path in the calling layer if violated.

### 3.4 Pluggable partitioning

```go
type Partitioner interface {
    // GroupKey maps a cache key to the group whose owner holds it.
    GroupKey(key string) string
}

func PartitionByKey() Partitioner                     // default
func PartitionByPrefix(sep string) Partitioner        // "user:123/cart" -> "user:123"
func PartitionByFunc(f func(string) string) Partitioner
```

For queue workloads, the same idea typed over messages:

```go
type Router[T any] interface {
    Submit(ctx context.Context, msgs ...T) error
}

func NewRouter[T any](c Cache, groupOf func(T) string, handler string) Router[T]
```

`Submit` groups messages by owner, issues one RPC per owner, and the owner
applies all messages for a group inside a single actor turn. This is the batch
affinity requirement, and it is a first-class API rather than an internal
optimization.

### 3.5 Membership

```go
type MembershipAuthority interface {
    Self() Node
    Members() ([]Node, Epoch)
    Watch() <-chan MembershipChange
    Join(seeds []string) error
    Leave(ctx context.Context) error
}
```

Two implementations. `gossip` (memberlist) is the default and is AP. A `raft`
implementation may be added later for clusters that need a single authoritative
ring epoch; the interface exists so that swap is not a rewrite.

### 3.6 Clock and transport (for testability)

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
    NewTimer(d time.Duration) Timer
}

type Transport interface {
    Dial(node Node) (PeerClient, error)
    Serve(l net.Listener, h Handler) error
    Close() error
}
```

Both are injected at construction. The test harness supplies fakes.

---

## 4. Milestones

Each milestone ends with working, tested, benchmarked code. Do not begin the
next until the previous is done per section 0.

### M0 — Test harness and skeleton

Nothing distributed. This exists so every later milestone is provable.

- `internal/clock` with real and fake implementations. Fake clock supports
  `Advance(d)` and deterministic timer firing.
- `internal/testkit`: spin up N in-process nodes sharing a fake transport.
  Fault injection: drop, delay, reorder, partition into arbitrary groups, kill
  a node with and without graceful shutdown.
- All core interfaces from §3 defined and compiling with no-op implementations.
- Deterministic seeded randomness throughout; a failing test prints a seed that
  reproduces it.

**Acceptance:** a test can create a 5-node in-process cluster, partition it
3/2, advance a fake clock by an hour, heal the partition, and assert on
membership — all in under 50ms of wall clock and with zero `time.Sleep`.

### M1 — Single-node core

- Sharded owned store. 256 shards default, `xxhash` key hashing, per-shard
  `RWMutex`. Values stored as `[]byte`.
- L1 cache wrapper over a local cache library. **Evaluate at least two
  candidates under this workload and record the comparison** — do not assume
  the first one is best.
- Namespace API fully working locally: Get/Set/Delete/Exists/Batch/GetOrLoad
  with singleflight.
- Codec registry: JSON and MsgPack. Compression: LZ4, applied above a
  configurable size threshold.

**Acceptance:** local Get p99 under 1µs at 1M keys; correct TTL expiry under
fake clock; singleflight proven by a loader that counts invocations under 1000
concurrent misses and reports exactly 1.

### M2 — Group actor runtime

Still single-node. This is the hardest correctness work; do it before adding
networking.

- Activation directory, bounded mailbox per group, single-goroutine turn loop.
- Idle deactivation with configurable timeout, plus a global cap on active
  groups with LRU deactivation under pressure.
- `RegisterGroupFunc` + `Call` end to end.
- Per-group timers on the injected clock, cancelled on deactivation and
  re-registered on activation.
- Lifecycle events: OnActivate, OnDeactivate, OnCreate, OnUpdate, OnDelete,
  OnEvict.
- Backpressure: full mailbox returns `ErrGroupOverloaded` rather than blocking
  indefinitely; caller deadline is respected.

**Acceptance:** 10k concurrent `Call`s across 1k groups produce serialized,
correct results under `-race`; memory for 1M *addressable* / 20k *active*
groups stays inside budget, measured and recorded; a group that deactivates and
reactivates preserves state and pending timers.

### M3 — Membership and ownership

- Rendezvous hashing over member set, epoch-stamped.
- Gossip membership via memberlist, with node metadata carrying epoch.
- Ownership table with fast `Owner(groupKey) (Node, Epoch)`.
- Activation guard: a node refuses to activate a group whose epoch is stale, and
  waits a configurable `activationDelay` after a membership change before
  activating groups it newly owns.

**Acceptance:** in a 7-node harness cluster, ownership of 100k group keys is
within 5% of uniform; removing one node moves approximately 1/7 of keys and no
more; a partitioned minority stops activating new groups within
`activationDelay`.

### M4 — Transport and remote routing

- gRPC service per `proto/`, connection pool, keepalive, mTLS optional but
  implemented.
- Remote Get/Set/Delete/Batch, remote `Call`, request deadline propagation.
- Non-owned L1 population with mandatory short TTL and owner-issued version.
- Best-effort invalidation broadcast, explicitly documented as an optimization.
- `Router[T]` with owner-side batching.

**Acceptance:** remote Get p99 under 2ms and p999 under 10ms at 50k rps in a
3-node real-network test; a router submitting 10k messages across 3 owners
issues at most 3 RPCs per flush window; killing the transport mid-request
surfaces a deadline error, never a hang.

### M5 — Replication and failure handling

- RF 1/2/3, async fan-out by default, sync mode available.
- Replica promotion on owner failure, driven by membership change + epoch bump.
- Catch-up sync for a returning node.
- Measured and documented data-loss window for async mode.

**Acceptance:** killing an owner ungracefully causes promotion and resumed
service within a stated bound; a chaos test that kills a random node every 2
seconds for 60 seconds loses no key that was written with sync replication;
split-brain writes are detected and counted in metrics.

### M6 — Rebalancing and Kubernetes lifecycle

- Move planning: compute only the affected partitions, never a full reshuffle.
- Streaming transfer with backpressure and resumability.
- PreStop hook: drain, deactivate groups, transfer ownership, then exit.
- Readiness gate that fails until the node has joined and settled.
- Manifests for Deployment and StatefulSet with headless service.

**Acceptance:** a simulated rolling upgrade of a 9-node cluster under sustained
write load completes with zero failed client requests and moves no more than
the theoretically required key count ±10%.

### M7 — Observability and hardening

- Prometheus and OTel: the metrics from the original spec, plus correctness
  metrics — split-brain detections, stale-epoch activation rejections, version
  conflicts, mailbox rejections, transfer failures.
- pprof endpoints, structured logging via the `Logger` interface.
- Load and soak test: 24h at target throughput, memory flat. *(Run for one hour
  instead, by decision: 728M operations with the live heap flat and goroutines
  23 → 23. A leak per operation shows within that; one that grows with wall
  time would show in the hour the nightly job runs. See docs/benchmarks.md.)*
- `docs/api.md` complete, examples runnable, README with an honest guarantees
  section including the failure modes.

### M9 — Resilience under resource pressure

Added after M8. The failure modes a Kubernetes operator actually sees —
OOMKilled, CrashLoopBackOff, CPU throttling — each turn one pod's problem into
the cluster's unless the library contains it.

- **Memory budget.** A per-cache byte budget covering the owned stores, the
  replica store and the L1 tier. Over budget, entries are shed in that order of
  preference: L1 first, then replicas, then owned data — sampled, not LRU, so
  the read path pays nothing. State is lossy by default (principle 4), so
  shedding is legitimate; it is never silent, and every tier has a counter.
- **No stale resurrection.** Shedding an owned key in a replicated namespace
  sheds its replicas too, as a replicated deletion. Dropping only the owner's
  copy would leave replicas holding an older value that a later write could not
  displace and a later failover would promote.
- **Catch-up respects the budget.** A node over budget stops pulling rather than
  pulling and shedding in a loop, and reports the catch-up as incomplete.
- **Crashloop containment.** A node is not ranked by its peers until it has
  declared itself rankable, so a pod that joins and dies repeatedly does not
  repeatedly take ownership of keys it never holds.
- **CPU starvation.** A throttled node is slow, not dead: it must not flap the
  membership, and callers of other nodes must not wait on it.

**Acceptance:** in a 5-node harness with a per-node budget, killing nodes one at
a time until two remain never takes any node's accounted memory above its
budget by more than one shedding step, and every shed is counted by tier; a key
shed by its owner and then rewritten is never served at an older value, before
or after a failover; a node restarted every 2 seconds for 60 seconds causes no
key held by the healthy nodes to be reported missing; a node whose every message
is delayed by 200 ms produces no split-brain detections and no change in p99 for
keys owned elsewhere.

### M10 — Typed namespaces

Section 2 described the namespace as "typed Get/Set/GetOrLoad" and
section 3.1 defined it in `[]byte`; open question 12 has carried the gap since
M1, and `WithCodec` has validated a name and encoded nothing.

- `NamespaceOf[T](cache, name, opts...)` returns a `TypedNamespace[T]`: the same
  operations as `Namespace`, in `T`, plus `Raw()` for the byte view underneath.
  A function, because Go interfaces cannot have type parameters.
- Values are encoded with the namespace's codec: `WithCodec`, or msgpack when
  none is named — msgpack because the JSON codec silently rewrites invalid UTF-8
  (open question 41), and a default must not corrupt data.
- `WithTypedLoader[T]` supplies a loader that returns `T`. It is a namespace
  option like `WithLoader`, and conflicts with it.
- Mismatches fail loudly, not as corrupt decodes: a name opened with two
  different `T`, or a typed loader whose `T` is not the namespace's, is
  `ErrInvalidOption`. A value that will not encode or decode is `ErrCodec`.
- Everything below the value — routing, replication, L1, TTL, the memory
  budget — is unchanged, because the typed view encodes to the same bytes.

**Acceptance:** a typed value set on one node is read back equal on every other
node of a three-node cluster, through Get, BatchGet and GetOrLoad with a typed
loader; a string holding invalid UTF-8 round-trips intact under the default
codec; opening one name with two types is ErrInvalidOption, and a value that
fails to decode is ErrCodec; the typed path adds no more than one encode or
decode to the cost of the byte path, measured.

### M11 — Linux and the pre-v1 API review

The suite had only ever run on one Windows machine. The first run on a Linux CI
runner failed four ways, none of them in the library: a test that needed a warm
module cache, a latency budget held to on a shared runner, a race pass that did
not fit a hosted runner's timeout twice, and three tests whose idle timer could
be re-armed after the clock had moved. Each is fixed for the reason it failed.

Before an API freezes, what it promises and cannot keep comes out: `WithBindAddr`,
which configured nothing that was dialled; `ErrStateEscaped` and `EventEvict`,
which nothing returned or emitted; and `PressureCheckInterval`, a tuning detail.
The tests moved out of the root directory into `tests/` and `tests/repo/`.

### M12 — Atomic operations on namespace keys

Conditional writes on one key, executed on its owner under the store's lock and
replicated like `Set`. See `decisions/0027-atomic-key-operations.md`.

- `SetIfAbsent`, `CompareAndSwap`, `Increment` and `Touch` on `Namespace`;
  `SetIfAbsent` and `Touch` on `TypedNamespace[T]`.
- `Increment` stores a base-10 integer, keeps an existing key's deadline, and
  reports `ErrNotAnInteger` for a value that does not parse or a sum that
  overflows.
- Only refusals that guarantee nothing was applied are retried. Nothing else is,
  because none of these but `Touch` is idempotent.

**Acceptance:** a thousand concurrent increments of one key from every node of a
three-node cluster return a thousand distinct results and leave it at a
thousand; of a hundred concurrent SetIfAbsent calls exactly one stores, and
every node reads its value; CompareAndSwap with a stale old value changes
nothing, and on a compressed namespace a matching one succeeds; Increment of a
non-integer or an overflowing sum is ErrNotAnInteger and leaves the value; a
touched key outlives its original deadline on its owner and on the replica
promoted after the owner dies; an increment the owner applied before its reply
was lost is not applied again; a local Increment costs no more than twice a
local Set, measured.

### M13 — Hot-key reads

Concurrent remote misses of one key on one node share one fetch, always; a
namespace may opt in to having its keys' replicas answer reads, bounded by the
L1 TTL. See `decisions/0028-hot-key-reads.md`.

- Remote `Get` misses coalesce under their own single-flight, run under the
  starting caller's deadline; a waiter whose own context is alive never inherits
  another's deadline.
- `WithReplicaReads()` picks one holder per (node, key) by rendezvous hash. A
  replica serves only a copy younger than the L1 TTL, only while it ranks as a
  replica under its own view, and never reports a key absent; the reader caches
  the copy for what is left of the TTL.
- In those namespaces an invalidation leaves an L1 floor rather than deleting,
  so a replica behind the write cannot refill the old value.

**Acceptance:** a thousand concurrent reads of one key owned elsewhere cost its
owner one RPC; a caller that quits a shared fetch, or whose deadline ends it,
does not fail the others; with replica reads at RF 3 reads spread across every
holder and fewer than half reach an owner; a replica copy one TTL old, a replica
that no longer ranks, a replica without the key and a peer without the method
all send the read to the owner, and none produces ErrNotFound; a replica answer
below an invalidation floor is discarded; a self-replica answers without an RPC;
a write whose replication and invalidations are all lost is read everywhere
once one TTL has passed, and a replica copy is cached only for what is left of
its TTL; replica reads at RF 1 or without an L1 tier are ErrInvalidOption; the
coalesced and replica-read paths are benchmarked.

### M14 — Opt-in persistence

A namespace may be opened with `WithPersistence(store)`. Owners flush coalesced
records, ordered by a hybrid logical clock, as immutable segments to a
`BlobStore`; a node that acquires a group it does not hold restores that group's
partition by a last-writer-wins merge; a per-partition compactor folds segments
into snapshots. See `decisions/0029-persistence-as-a-segment-log.md`, which
supersedes `0026`.

Built in parts: **M14a** the design and this section; **M14b** the blob store
interface, `persist.Memory`, the order carried through every write path, flush,
restore and compaction; **M14c** `persist.Dir` and the measured loss window;
**M14d** `persist/s3` as a separate module.

M14b and M14c are built. M14d's module is a criterion of its own part and is
added here when it is.

**Acceptance:** killing every holder of a key restores it on its new owner at
its last flushed value; a whole cluster stopped and started on the same store
reads every flushed key from every node, and neither a deleted nor an expired
key comes back; a restore merges every snapshot, so a record left in an older
one by a concurrent compactor is not lost, and a snapshot that fails to write
deletes nothing; a promoted replica's next write outranks everything its dead
owner flushed; a shed owned key is restored on its next access; an unavailable
store fails no write, keeps one pending record per key, and loses nothing once
it returns, and a restore that cannot read it serves memory and retries; a
namespace without persistence performs within noise of before, benchmarked;
`persist.Dir` meets the blob-store contract, writes data, then renames, then
syncs the directory, and a Put interrupted at any step leaves a whole object;
the loss window on it is measured.

---

## 5. Deliberately deferred

Record these in `docs/decisions/open.md` and do not build them in v1:

- Per-write synchronous persistence (a WAL fsynced on every `Set`); M14's is a
  write-behind segment log with a measured window
- Persisting group state
- Cross-group transactions
- Multi-region / multi-cluster
- Raft-backed membership
- Group sharding for hot-key mitigation (document the limitation instead)
- Protobuf and CBOR codecs beyond the registry hook
- Snappy/Zstd beyond the compression interface

---

## 6. Guarantees to state honestly in the README

Write this section early and keep it accurate as the implementation evolves.
It is the most useful documentation in the project.

- Operations on a single group are serialized while that group has one
  activation.
- Membership is eventually consistent. A network partition can briefly produce
  two activations of the same group; conflicting writes are detected but not
  automatically merged.
- State is not durable by default. Ungraceful pod termination loses that pod's
  owned data and any replicas not yet caught up. A namespace with persistence
  loses only what was written in the last flush interval, which is measured, and
  resolves conflicting writes from a partition by last writer wins rather than
  detecting them.
- Async replication has a data-loss window averaging approximately 0.1 ms under
  normal conditions, with a p99 under 1.5 ms (measured at M5; see
  docs/benchmarks.md). A write acknowledged in that window exists on one node
  only. Synchronous replication closes it entirely, at the cost of bounding a
  group's write rate by its slowest replica.
- Reads of non-owned keys may be stale by up to the configured L1 TTL. In a
  namespace with replica reads, by up to the L1 TTL plus the replication delay,
  which is measured, not bounded.
- Throughput degrades to single-pod capacity for a single hot group.