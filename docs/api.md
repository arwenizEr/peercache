# Public API

Every symbol here exists, compiles and is implemented. `ErrNotImplemented` no
longer marks unbuilt scaffolding — it now reports only an RPC method this binary
does not recognise, which is what a peer running a newer build looks like during
a rolling upgrade, or a cache asked to reach another node with no transport
configured.

Every public symbol below has a runnable example in `example_test.go`, compiled
and output-checked by `go test`:

| Example | Covers |
|---|---|
| `Example` | construction, `Set`, `Get`, `ErrNotFound` |
| `ExampleNamespace_Set` | TTL expiry under an injected clock; non-positive TTL |
| `ExampleNamespace_Get` | the aliasing contract — `Set` copies, `Get` does not |
| `ExampleNamespace_BatchGet` | batch semantics; absent keys omitted |
| `ExampleWithLoader` | `GetOrLoad`, loader registration, `ErrNoLoader` |
| `ExampleWithLoaderTimeout` | the load is bounded by the namespace, not the caller |
| `ExampleWithCompression` | opt-in compression, threshold, unknown compressor rejected |
| `ExampleWithShards` | per-namespace shard count |
| `ExampleWithRequireReplicationFactor` | a synchronous write below its factor: counted by default, refused on request, held either way |
| `ExamplePartitionByPrefix` | key-to-group mapping |
| `ExampleCache_RegisterGroupFunc` | registration, duplicates, unknown names |
| `ExampleNewRouter` | queue-workload routing |
| `ExampleCache_WriteMetrics` | the metrics surface; text exposition format, no client library |

End-to-end programs, one per milestone: `go run ./examples/m0` (skeleton and
lifecycle), `m1` (the local data path), `m2` (the group actor runtime), `m3` (a
real three-node cluster over loopback gossip), `m4` (remote routing), `m5`
(replication and failure), `m6` (rebalancing and drain), `m7` (the scrape
endpoint and the counters that matter) and `m8` (a partition heal that moves
replicas rather than dropping them, and the synchronous write acknowledged below
its replication factor). Each one asserts what it demonstrates and all are run by
CI.

**Milestone status:** all seven complete. Namespaces are fully functional on
a single node; groups run as virtual actors with a single goroutine each,
bounded mailboxes, idle deactivation, an active cap, per-group timers and
lifecycle events; and nodes form a gossip cluster in which every node computes
the same owner for a key without coordinating.

Ownership is now invisible to callers: a read, a write or a `Call` for data
owned elsewhere crosses the network and comes back, and the only way to tell is
latency. Non-owned values are cached under a mandatory TTL, loaders run on the
owner so a cold key costs the cluster one load rather than one per pod, and
`Router` turns a flush of messages into one request per owner.

Keys are replicated to the runners-up in the same ranking that chose their
owner, so losing a pod promotes a replica rather than losing its data. Fan-out
is asynchronous by default with a measured ≈0.1 ms exposure window, and
`WithSyncReplication()` closes that window while the writer can see its replicas,
at the cost of the group's write rate. `WithRequireReplicationFactor()` makes a
synchronous write that cannot reach its factor fail instead of succeed.

A node can now be added or removed on purpose: `Drain` hands its keys to their
new owners before it stops, `Ready` keeps traffic off a node that has joined but
cannot yet serve, and planning moves only the share the ranking reassigns.
`deploy/` has manifests wiring both to Kubernetes probes.

Every counter the earlier milestones built is exported: `MetricsHandler` serves
40 series in the Prometheus text exposition format, and PeerCache imports no
metrics client library to do it. Latency histograms are available behind
`WithLatencyHistograms()` and off by default, for a reason that is a measurement
rather than an opinion — see Observability below.

What is *not* true: there is no tracing, no per-namespace label breakdown, and
no durability. The last of those is a design decision, not a gap.

---

## Construction

```go
func New(opts ...Option) (Cache, error)
```

Options are validated once, at construction, so a misconfiguration surfaces at
startup rather than on the first request. `WithNodeID` is required.

| Option | Meaning |
|---|---|
| `WithNodeID(string)` | Cluster-unique, stable for the process lifetime. Ownership is derived from it, so a restart under a new ID moves that node's share of the keyspace. **Required.** |
| `WithBindAddr(string)` | Address peers dial. May be empty for an in-process transport. |
| `WithSeeds(...string)` | Addresses used to join an existing cluster. Empty means form a cluster of one. |
| `WithClock(Clock)` | Time source. Defaults to the real clock. |
| `WithLogger(Logger)` | Defaults to `NopLogger`, which discards everything. |
| `WithTransport(Transport)` | Peer fabric. Nil means single-node, no networking — a supported configuration, not an error. |
| `WithMembership(MembershipAuthority)` | Nil means single-node. |
| `WithGroupMailboxSize(int)` | Queue depth per group before `ErrGroupOverloaded`. Default 64. |
| `WithGroupIdleTimeout(time.Duration)` | How long an activated group waits before releasing its goroutine. Default 30s. |
| `WithMaxActiveGroups(int)` | Cap on activated groups, LRU-evicted. Default 20,000; negative means unlimited. |
| `WithEventHandler(func(Event))` | Receives lifecycle events. Must not block. |

```go
cache, err := peercache.New(
    peercache.WithNodeID("pod-0"),
    peercache.WithBindAddr("10.0.0.1:7946"),
)
if err != nil {
    return err
}
defer cache.Close(ctx)
```

---

## Cache

```go
type Cache interface {
    Namespace(name string, opts ...NamespaceOption) Namespace
    Group(id string) Group
    RegisterGroupFunc(name string, fn GroupFunc) error

    CatchUp(ctx context.Context) error   // M5 — see "CatchUp — required after a restart"
    Drain(ctx context.Context) error     // M6 — the PreStop hook
    Ready() ReadyState                   // M6 — the readiness probe
    WaitReady(ctx context.Context) error

    Metrics() []Metric                   // M7 — see "Observability"
    WriteMetrics(w io.Writer) error
    MetricsHandler() http.Handler
    RemoteLatency() (p50, p99, p999 uint64)

    Stats() Stats
    Close(ctx context.Context) error
}
```

`Namespace` and `Group` return handles, not data, and are cheap enough to call
on a hot path.

`Close` with a live context drains gracefully — deactivate groups, transfer
ownership, stop. If the context expires first, shutdown becomes ungraceful and
this node's owned state is lost, which is the documented consequence of having
no durability. `Close` is idempotent, and every operation afterwards reports
`ErrClosed`.

---

## Namespace

```go
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

A missing key returns `ErrNotFound`, never a nil value, so that a key holding
zero bytes stays distinguishable from a key holding nothing.

Batch calls group keys by owner and issue one request per owner, so cost scales
with the number of distinct owners rather than the number of keys. They are not
atomic across owners, and no cross-group transaction exists to make them so.

### Namespace options

| Option | Default | Meaning |
|---|---|---|
| `WithLoader(Loader)` | none | Reconstructs a missing value. Without it, `GetOrLoad` returns `ErrNoLoader`. |
| `WithPartitioner(Partitioner)` | `PartitionByKey()` | Maps keys to groups. Must be pure and identical on every pod. |
| `WithReplicationFactor(int)` | 1 | 1 to 3. Outside that range is `ErrInvalidOption`. |
| `WithCodec(string)` | none | Names a registered codec: `json` or `msgpack`. Unknown names fail at construction. |
| `WithL1TTL(time.Duration)` | 5s | Upper bound on staleness for non-owned copies. |
| `WithCompression(name string, threshold int)` | off | Compresses stored values at or above `threshold` bytes. `lz4` is registered. |
| `WithLoaderTimeout(time.Duration)` | 30s | Bounds one loader invocation. |
| `WithShards(int)` | 256 | Independently locked partitions, rounded up to a power of two. |

`Namespace` cannot return an error, so an invalid option is reported by every
operation on the returned handle. Ignoring it would hide the misconfiguration
until production.

Calling `Namespace` twice with the same name returns the same handle; the first
call's options win. Two handles to one namespace would mean two stores.

### Compression is off by default

Reading a 4 KiB value costs 72 ns uncompressed and 5.0 µs with LZ4 — seventy
times more, plus an allocation. That is a good trade for large, cold,
compressible values and a bad one for a forty-byte counter, and only the
application knows which it has. See
[`docs/decisions/0008-compression-is-opt-in.md`](decisions/0008-compression-is-opt-in.md).

### Aliasing

`Set` copies the value it is given, so a caller may reuse its buffer. `Get`
returns the stored slice **without copying**, so a caller must not modify it.

Copying on read was measured at 2.6x the cost and one allocation per read, which
would also undo the allocation-free property the latency tail depends on. See
[`docs/decisions/0007-get-returns-the-stored-slice.md`](decisions/0007-get-returns-the-stored-slice.md).

### GetOrLoad has no loader argument

```go
profiles := cache.Namespace("profiles",
    peercache.WithLoader(func(ctx context.Context, key string) ([]byte, time.Duration, error) {
        return db.LoadProfile(ctx, key)
    }),
)

val, err := profiles.GetOrLoad(ctx, "user:123")
```

The loader is registered on the namespace so that it exists by name on every
pod and the key's owner can execute it locally, instead of shipping the miss
back to the caller. Concurrent misses for one key collapse into a single
invocation: a thousand concurrent misses on a cold key invoke the loader exactly
once, and the other 999 calls attach to that load.

The returned `ttl` is applied exactly as `Set` would apply it — a non-positive
value means the loaded value never expires on its own.

Three rules about the load itself, each of which exists because the obvious
alternative is wrong:

- **The load does not inherit the caller's context.** Misses collapse, so taking
  whichever caller arrived first as the bound would let a caller with 5 ms left
  abort a load that 999 callers with a second left were depending on. The load
  runs with cancellation stripped and the namespace's `WithLoaderTimeout` as its
  deadline. Request-scoped values still propagate.
- **A caller whose context expires stops waiting; the load continues** for
  everyone else.
- **A panicking loader becomes `ErrLoaderPanic` for every waiter**, rather than a
  panic in a thousand unrelated goroutines. A loader is application code.

### L1 TTL is a correctness bound

Not a tuning knob. Invalidation broadcasts are a latency optimisation and may be
lost; this TTL is the only thing that turns an unbounded stale read into a
bounded one. Longer means faster and staler, and the staleness is what a caller
has to reason about.

---

## Group

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

A group is a named collection of keys guaranteed to share an owner, and the unit
of activation and serialization. It is addressable at all times, activated
lazily on its owner, and deactivated after an idle period.

**There is no `Lock`, and there will not be one.** Serialization is structural:
it comes from the group having exactly one goroutine while activated, not from
callers agreeing to take a lock. A lock API here would be a *distributed* lock
API, with all the liveness problems that implies, layered on a membership system
that is explicitly AP.

`Keys` and `Items` are snapshots taken inside one turn: internally consistent,
stale the instant they are returned. Prefer `Call` to compute over a large group
in place rather than shipping it to the caller.

`Items` is the one read in this library that **copies**. Every other read hands
back the stored slice, but this snapshot outlives its turn, so aliasing would let
a caller mutate state that a later turn owns, with no lock between them.

### Activation, deactivation and cost

A group is *addressable* always and *active* only while it holds a goroutine.

- First use activates it: a mailbox, a goroutine, an idle timer.
- `WithGroupIdleTimeout` of no work releases the goroutine; `Flush` does it on
  demand; exceeding `WithMaxActiveGroups` evicts the least recently activated.
- **Deactivation preserves state and pending timers.** Only the goroutine goes;
  the next operation reactivates and re-arms the timer schedule.

Measured: an addressable group costs about 167 B, an active one about 1.1 KiB of
heap plus a goroutine stack. That fifty-fold ratio is why the two are separate —
a million addressable groups is 160 MiB, while a million *active* would be
gigabytes of stack. See
[`docs/decisions/0010-records-and-activations.md`](decisions/0010-records-and-activations.md).

A group with pending timers is exempt from *idle* deactivation, because nothing
else would wake it and its timer would silently never fire. The active cap can
still evict it, after which its timers lie dormant until it is next called.

The cap is a settled bound, not an instantaneous one: eviction is signalled
after the call that tripped it is already enqueued, so the active count may sit
slightly above the cap while victims finish their turns.

### Backpressure

A full mailbox returns `ErrGroupOverloaded` **immediately** — it does not wait
for room. The caller's deadline governs waiting for the *result*, not for a slot.

The error is retryable and retrying is what a caller is expected to do: a
thousand concurrent callers against a mailbox of 64 will see rejections and still
produce an exactly correct total once they retry. `Stats().Rejections` rising is
flow control working, not a fault. See
[`docs/decisions/0011-mailbox-rejects-immediately.md`](decisions/0011-mailbox-rejects-immediately.md).

A single group is a serialization point: one goroutine, one turn at a time, about
1.2 µs per call. No mailbox tuning changes that ceiling.

---

## Registered group functions

```go
type GroupFunc func(ctx context.Context, s GroupState, arg []byte) ([]byte, error)

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

```go
err := cache.RegisterGroupFunc("increment", func(ctx context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
    current, _ := s.Get("count")
    s.Set("count", increment(current, arg))
    return nil, nil
})

_, err = cache.Group("tenant:acme").Call(ctx, "increment", []byte("1"))
```

Registration happens once, at startup. Closures never cross the network: an RPC
carries the registered name and opaque argument bytes, and every pod is assumed
to run the same binary. Registering a name twice returns
`ErrDuplicateRegistration`; calling a name this node does not have returns
`ErrFuncNotRegistered`, which during a rolling upgrade is a real and expected
condition.

`GroupState` methods take no context and return no error because they are plain
local map accesses on the group's single goroutine. There is nothing to fail and
nothing to wait for.

**A `GroupState` is valid for one turn only.** Capturing one in a goroutine and
using it later is a use-after-free in all but name. The implementation stamps
each state with a generation and turns a violation into `ErrStateEscaped` —
detected, not panicked, and not silently corrupting.

---

## Partitioning

```go
type Partitioner interface {
    GroupKey(key string) string
}

func PartitionByKey() Partitioner                  // default
func PartitionByPrefix(sep string) Partitioner     // "user:123/cart" -> "user:123"
func PartitionByFunc(f func(string) string) Partitioner
```

Must be pure and identical on every pod. Two nodes disagreeing about a key's
group is indistinguishable from a split brain, and considerably harder to
diagnose.

`PartitionByPrefix("")` and `PartitionByFunc(nil)` fall back to
`PartitionByKey()` rather than failing on the first `Get`.

---

## Router

```go
type Router[T any] interface {
    Submit(ctx context.Context, msgs ...T) error
}

func NewRouter[T any](c Cache, groupOf func(T) string, handler string) Router[T]
```

```go
router := peercache.NewRouter(cache, func(e Event) string { return e.TenantID }, "apply")
err := router.Submit(ctx, events...)
```

`Submit` groups messages by owner, issues one request per owner, and the owner
applies every message for a group inside a single actor turn. This is batch
affinity as a first-class API rather than an internal optimisation, because the
difference between one RPC and ten thousand is not an implementation detail.

Submitting no messages succeeds: the caller's postcondition already holds. There
is no cross-owner atomicity and no rollback.

---

## Transport

Supply a transport and a listener and the cache serves peers on it:

```go
l, err := net.Listen("tcp", ":7947")
tr, err := transport.New(transport.Options{
    Self: Node{ID: "pod-0", Addr: l.Addr().String()},
    TLS:  &transport.TLSConfig{CertFile: ..., KeyFile: ..., CAFile: ...},
})
cache, err := peercache.New(
    peercache.WithTransport(tr),
    peercache.WithListener(l),
    peercache.WithMembership(gossip),
)
```

`New` does not return until the transport is answering, because a node that is
a cluster member but not yet serving owns part of the keyspace it will refuse
to answer for — which every peer reads as a broken node rather than a starting
one.

**The gossip address and the data address are different sockets.** memberlist
binds its own port for SWIM; the transport binds another for gRPC. Pass the
data address as `Node.Addr`; peers learn it through gossip metadata. Getting
this wrong makes the cluster dial memberlist expecting gRPC.

mTLS is off by default and real when switched on: both sides present a
certificate and both verify one. See
`docs/decisions/0014-grpc-without-protobuf.md` for why the transport is gRPC
but the codec is not protobuf.

### `Router`

```go
router := peercache.NewRouter(cache, func(o Order) string { return o.TenantID }, "apply")
err := router.Submit(ctx, orders...)
```

`Submit` groups messages by group key, then groups those by owner, and issues
one request per owner. Ten thousand messages across three owners cost three
requests, not ten thousand.

The registered handler receives **the whole batch for one group**, not one
message — that is what lets a group's messages be applied in a single turn.
Unpack it with `peercache.Batch[T]`:

```go
cache.RegisterGroupFunc("apply", func(ctx context.Context, s GroupState, arg []byte) ([]byte, error) {
    orders, err := peercache.Batch[Order](arg)
    ...
})
```

Failures are reported per group. There is no cross-group atomicity and no
rollback, so a group that failed is named and the ones beside it have already
applied.

---

## Replication

```go
ns := cache.Namespace("orders",
    peercache.WithReplicationFactor(3),
    peercache.WithSyncReplication(),
)
```

Replicas are the runners-up in the rendezvous ranking that chose the owner, so
promotion needs no election: when a node dies, the node that ranked second ranks
first everywhere at once, computed from the same membership.

`WithReplicationFactor` takes 1 to 3. The default is 1 — owner only, no copies.

`WithSyncReplication` makes a write wait for its replicas. Without it, fan-out is
asynchronous and a write is exposed for ≈0.1 ms on average (p99 1.5 ms) during
which it exists on one node. With it, the window closes and the group's write
rate becomes its slowest replica's round trip, because the fan-out holds the
group's single activation.

**One case it does not cover.** Replication targets come from the ranking over
this node's own membership view, so a node cut off from the cluster eventually
detects every peer as failed, ranks itself the owner of everything, places the
one copy it can reach and returns `nil`. A caller that asked for three copies got
one and cannot tell; the window lasts as long as the partition does.
`Stats().DegradedWrites` and `peercache_degraded_writes_total` count exactly
this and are the only thing that distinguishes it from a healthy write — alert
on any non-zero value. While the writer still *sees* its replicas but cannot
reach them, the write fails instead, which is the guarantee working. See
`docs/decisions/open.md` question 37 for why this is counted by default rather
than refused: refusing converts an available system into an unavailable one during a
partition, which is an AP/CP choice that should be made deliberately rather than
as a side effect.

`WithRequireReplicationFactor` makes that choice for one namespace:

```go
ns := cache.Namespace("orders",
    peercache.WithReplicationFactor(3),
    peercache.WithSyncReplication(),
    peercache.WithRequireReplicationFactor(),
)
```

A synchronous write that cannot be placed on three nodes then fails with
`ErrUnderReplicated` rather than succeeding with fewer, so `nil` always means the
factor was reached from where the writer stood. It is counted in
`Stats().UnderReplicatedRefusals` instead of `DegradedWrites`. The copies that
could be placed are placed first, and the owner keeps its copy — a refused
synchronous write is reported, not rolled back, exactly as when a replica does
not answer. `Set` and `Delete` are idempotent, so retrying is safe.

The costs, stated before anyone turns it on: a cluster smaller than the factor
refuses every write in that namespace — one node alone in development, or a pod
that has started and not yet joined — and an isolated node stops accepting those
writes until it can see enough peers again. It does not make writes agree across
nodes: the two-activation window of a partition is unchanged. It requires
`WithSyncReplication`; without it the namespace reports `ErrInvalidOption`. Off
by default, because the default should not turn a partition into an outage
without being asked.

### `CatchUp` — required after a restart

```go
// After the readiness gate, before serving traffic.
if err := cache.CatchUp(ctx); err != nil {
    log.Warn("catch-up incomplete", "error", err)
}
```

**A restarted node that skips this will report keys as missing when they are
not.** Replication carries writes, not state, so a returning node receives only
what is written after it comes back — while the ownership ranking gives it back
its keys immediately. It then answers `ErrNotFound` for data that exists
elsewhere in the cluster.

It is explicit rather than automatic because a starting node is usually also
serving traffic, and a bulk transfer at that moment competes with the requests
it was started to answer. The application decides when it can afford it.

Safe to call at any time and alongside live traffic: incoming pages go through
the same version check as replicated writes, so a page that arrives after a
newer write is discarded rather than applied.

#### It is also what restores the replication factor after any membership change

The heading says "after a restart" because that is the case that bites hardest,
but the need is more general. Any membership change moves keys: a node joining, a
partition healing, a rebalance. Until the copies have been placed, a key can be
**readable while fewer than RF nodes hold it** — one node answering is enough to
be readable and is not enough to survive a failure.

So `Replication survives the loss of RF−1 nodes` describes a cluster at rest.
Inside the settling window it does not hold, and no error distinguishes the two
states: a read succeeds either way.

If you depend on the replication factor after a topology change — draining a
node, scaling up, recovering from a partition — call `CatchUp` and let it finish
first. `Ready().ReplicasCurrent` says when it has, on each node, for the view
that node holds now.

`CatchUp` returns an error wrapping every peer it could not pull from, and keeps
going past each one: one unreachable peer is no reason to skip the others. From
M5 until after M8 it returned `nil` regardless — the error was collected into
a variable that was never assigned — so a caller checking it was told catch-up
had finished by a node that had reached nobody.

### Conflicts

`Stats().SplitBrainDetections` counts writes a replica saw from two different
nodes under one membership epoch. One group has one owner, so this cannot happen
unless two nodes believed they owned the same key — the partition case. It is
counted and never merged; choosing between conflicting writes is the
application's business.

---

## Lifecycle

Two calls, both intended for Kubernetes probes. See `deploy/README.md`.

### `Ready` — the readiness probe

```go
st := cache.Ready()
if !st.Ready {
    http.Error(w, st.String(), http.StatusServiceUnavailable)
    return
}
```

False until the node has joined, its view has been stable for longer than the
activation delay, and `CatchUp` has completed at least once. Permanently false
once `Drain` begins.

This exists because membership and readiness are different facts. A node becomes
an owner the instant it joins — peers recompute the ranking immediately — but
holds nothing until catch-up runs, and in between answers `ErrNotFound` for data
that exists elsewhere. The gate keeps clients away until the two agree.

`ReadyState` is a struct rather than a bool because the remedies differ: waiting
for gossip needs patience, an uncaught-up node needs `CatchUp`, a draining one
needs to be left alone. `String()` is written for a probe response body.

#### `ReplicasCurrent` — beside readiness, not inside it

`Ready` answers whether this node can serve. `ReplicasCurrent` answers whether it
has pulled, from every peer in its view and without a failure, the replicas it
ranks for under the membership it holds *now*. It goes false on every membership
change the node observes and true again only when a `CatchUp` that began under
the new view returns `nil`.

A ready node serves every request correctly while `ReplicasCurrent` is false;
what it cannot yet promise is that losing another node loses nothing. That is a
question about replacing the *next* pod, so it does not gate this one's traffic.
Gate the rolling update on it instead: the replication factor is back once every
node reports `ReplicasCurrent` under the same membership. One node reporting it
says only that this node has done its part.

Also in `Stats().ReplicasCurrent`, and exported as the gauge
`peercache_replicas_current` (1 or 0), so the rollout can wait on
`min(peercache_replicas_current) == 1` across the pods.

### `Drain` — the PreStop hook

```go
ctx, cancel := context.WithTimeout(r.Context(), 50*time.Second)
defer cancel()
err := cache.Drain(ctx)
```

Marks the node unready, announces its departure so peers stop routing to it,
pushes the keys it owns to their new owners, and deactivates its groups — in
that order, because every other order leaves a window where requests arrive at a
node that has already given away the ability to serve them.

Idempotent, and *not* `Close`: a drained cache still answers peers that have not
yet noticed. Call `Close` after. `terminationGracePeriodSeconds` must exceed how
long a drain takes.

At RF≥2 the hand-off is mostly a latency optimisation — replication and
promotion already cover the data. At RF=1 it is the only thing between a
graceful shutdown and losing that node's entire share.

---

## Membership, transport, clock

```go
type MembershipAuthority interface {
    Self() Node
    Members() ([]Node, Epoch)
    Watch() <-chan MembershipChange
    Join(seeds []string) error
    Leave(ctx context.Context) error
}

type Transport interface {
    Dial(node Node) (PeerClient, error)
    Serve(l net.Listener, h Handler) error
    Close() error
}

type Clock interface {
    Now() time.Time
    Since(t time.Time) time.Duration
    After(d time.Duration) <-chan time.Time
    NewTimer(d time.Duration) Timer
    AfterFunc(d time.Duration, f func()) Timer
    NewTicker(d time.Duration) Ticker
    Sleep(ctx context.Context, d time.Duration) error
}
```

`Clock` is wider than SPEC.md section 3.6 specifies; see
`docs/decisions/0002-clock-interface.md`. `PeerClient` and `Handler` are defined
by this project, not by the spec; see `docs/decisions/open.md`.

`Watch` returns one channel per call, buffered, with non-blocking sends. A
watcher that stops reading loses changes rather than stalling membership.

### Ownership

Supply a `MembershipAuthority` with `WithMembership` and the cache derives
ownership from it: rendezvous hashing over the current view, stamped with the
view's epoch. Without one, the node is a cluster of one and owns everything,
which is the correct answer rather than a special case.

`github.com/peercache/peercache/internal/membership` provides a gossip-backed
implementation over hashicorp/memberlist.

```go
gossip, err := membership.New(membership.Options{
    Self:     Node{ID: "pod-0"},
    BindAddr: "0.0.0.0:7946",
})
cache, err := peercache.New(
    peercache.WithMembership(gossip),
    peercache.WithActivationDelay(3 * time.Second),
)
```

Ownership is recomputed on every membership change, and every node reaches the
same conclusion from the same view without coordinating. Removing a node moves
only that node's keys; the survivors do not trade keys with each other.

### `WithActivationDelay`

After a membership change, a node defers activating groups it has *just*
acquired, for this long, and returns `ErrActivationPending` for them meanwhile.
Groups it already held are unaffected — the deferral is confined to the part of
the keyspace that actually moved.

The default is 3 seconds. It is a guess about how long the previous owner takes
to notice it lost the group, and it narrows the double-activation window without
closing it: membership is gossip based and eventually consistent, so there is no
delay that makes two owners impossible. A negative value disables deferral
entirely, which is only sane on a single node.

Note the limit precisely: the guard runs on *activation*. A group that is
already active on a node that has just lost it keeps running there until it goes
idle. Graceful handoff is M6. See `docs/decisions/0012-the-activation-guard.md`.

`Stats().DeferredActivations`, `Stats().RefusedActivations` and
`Stats().StaleEpochRefusals` count these decisions. A spike in deferrals after a change that returns to zero is healthy;
sustained refusals under stable membership mean callers disagree with this node
about who owns what.

---

## Observability

```go
Metrics() []Metric
WriteMetrics(w io.Writer) error
MetricsHandler() http.Handler
RemoteLatency() (p50, p99, p999 uint64)
```

PeerCache imports no metrics client library. It exports values and lets the
application choose the instrument — the reasoning, and what it costs, is in
`docs/decisions/0017-metrics-without-a-client-library.md`.

```go
mux.Handle("/metrics", cache.MetricsHandler())
```

That is the whole integration. The handler writes the Prometheus text exposition
format with the content type a scraper expects, and 40 series come out of it.
`WriteMetrics` is the same output to any `io.Writer`; the two are tested to
produce identical bytes.

### The metrics that are worth alerting on

Most of the 40 are ordinary levels and rates. These eleven are not — each counts a
window this system is documented as having, and a non-zero value means the
window was entered.

| Series | Non-zero means |
|---|---|
| `peercache_split_brain_detections_total` | A replica saw writes for one key from two nodes under one membership epoch. Two nodes believed they owned it. This is the AP consequence in CLAUDE.md §5 actually happening. |
| `peercache_stale_epoch_refusals_total` | A caller's view was older than this node's and the operation was refused. Sustained under stable membership means callers disagree about ownership. |
| `peercache_implicit_namespaces_total` | A routed write created a namespace this node had never opened, so it got default options — replication factor 1. **Alert on this being non-zero.** It is a silent misconfiguration that looks exactly like a data-loss bug; it cost most of M6 to diagnose. |
| `peercache_degraded_writes_total` | A **synchronous** write was acknowledged holding fewer copies than its namespace's replication factor asked for. **Alert on this being non-zero.** It means a caller was told a write reached RF nodes when it reached fewer, and it is the only signal that distinguishes the two. Replication targets come from this node's own membership view, so a node cut off from the cluster ranks itself the owner of everything, places the one copy it can reach and returns nil. See open question 37. |
| `peercache_under_replicated_refusals_total` | A synchronous write was refused with `ErrUnderReplicated` in a namespace opened with `WithRequireReplicationFactor`. The caller was told, so nothing is silent — but a sustained rate means a node is cut off or the cluster is smaller than the factor, and every write to that namespace is failing. |
| `peercache_replications_failed_total` | A replica write did not land. Under async replication the caller was told the write succeeded. |
| `peercache_replications_dropped_total` | A replica write was never attempted, because the queue was full. |
| `peercache_replicas_retained_total` | A replica this node stopped ranking for could not be handed to the nodes that now rank for it, so it was kept. Memory rather than data loss — the safe direction — but sustained under stable membership it means hand-offs are failing and the cluster is not converging. |
| `peercache_catchup_failures_total` | A restarted node could not pull the keys it now ranks for and may be serving misses for data that exists. |
| `peercache_mailbox_rejections_total` | A group's mailbox was full and the call was refused with `ErrGroupOverloaded`. Backpressure working, not a fault — but it is the first sign of saturation and it arrives before the latency does. |
| `peercache_version_conflicts_total` | A replicated copy was rejected because a newer one was already held. Distinct from split brain: this is one owner's writes arriving out of order. A steady low rate is normal under async replication; a rising one means reordering under load. |

All ten are counters, so `rate()` and `increase()` work on them. That is
deliberate and tested: `TestCorrectnessMetricsAreCounters` fails if one is
reclassified as a gauge, because "did this happen in the last hour" is a rate
question and a gauge cannot answer it.

These are the five SPEC.md names as correctness metrics, plus the five this
implementation found it needed — the last two added in M8, when a partition heal
turned out to move replicas that the reconciler had been discarding.

Transfer failures are covered by `peercache_replications_failed_total`: the drain handoff reuses the replication
path rather than inventing a transfer protocol, so a failed transfer *is* a
failed replication, and a second counter could only disagree with the first.

### Latency histograms are off by default

```go
peercache.New(..., peercache.WithLatencyHistograms())
```

Off unless asked for, and instrumenting only the remote path. An observation
costs 15.8 ns uncontended and 36.9 ns under concurrency, allocation-free —
about 7% of the 515 ns local read path, which would be a real tax for no
information: the platform's monotonic clock resolves to roughly 500 µs, so
timing a sub-microsecond operation records zero. Against a 1.7 ms remote round
trip the same observation is immeasurable.

`RemoteLatency` returns the three percentiles directly, in nanoseconds, for
callers that want a number rather than a scrape:

```go
p50, p99, p999 := cache.RemoteLatency()
```

Every value is a bucket's **upper bound**, so it over-reports by at most one
bucket width. Rounding up is deliberate and tested by `TestQuantileRoundsUp`: an
under-reported percentile lets a breached latency budget pass unnoticed, which
is the one error a latency metric must not make. Zero means nothing has been
observed, which is distinguishable from a real measurement because no operation
takes zero nanoseconds.

### What is not exported

No per-namespace or per-key labels. The aggregate is what operators alert on,
and a cardinality explosion is a production incident in its own right. A
namespace breakdown would need an explicit option and is not built.

---

## Errors

Test with `errors.Is`. Errors wrap their cause with `%w`.

| Sentinel | Meaning | Retryable |
|---|---|---|
| `ErrNotImplemented` | Scaffolding for a milestone that has not run. | no |
| `ErrNotFound` | Key or group does not exist. | no |
| `ErrClosed` | The cache has been closed. | no |
| `ErrGroupOverloaded` | Mailbox full; rejected rather than queued. | yes, after backoff |
| `ErrStaleEpoch` | Ownership was resolved under a view this node has already replaced. | yes, after re-resolving |
| `ErrNotOwner` | This node does not own the key's group. | yes, after re-resolving |
| `ErrActivationPending` | This node owns the group but acquired it within `WithActivationDelay`. | yes, here, shortly |
| `ErrStateEscaped` | A `GroupState` was used outside its turn. | no — it is a bug |
| `ErrNoLoader` | `GetOrLoad` without `WithLoader`. | no |
| `ErrLoaderPanic` | A registered loader panicked. | no — it is a bug |
| `ErrGroupFuncPanic` | A registered group function panicked. The group keeps its state and stays usable. | no — it is a bug |
| `ErrLoaderFailed` | A load could not complete, e.g. the loader exited without returning. | no |
| `ErrFuncNotRegistered` | Unknown function name. | during a rolling upgrade, yes |
| `ErrDuplicateRegistration` | Name registered twice. | no |
| `ErrInvalidOption` | Option outside its range. | no |
| `ErrNodeUnreachable` | Transport could not deliver. | yes |
| `ErrUnderReplicated` | A synchronous write could not reach its replication factor, in a namespace opened with `WithRequireReplicationFactor`. The owner holds the value. | yes, once this node sees enough peers |

`ErrGroupOverloaded` is backpressure working as designed. Blocking indefinitely
would convert one slow group into a cluster-wide stall.

`ErrStaleEpoch`, `ErrNotOwner` and `ErrActivationPending` are all about
ownership and they are retryable in different directions. `ErrStaleEpoch` means
*your view is out of date* — re-resolve, you may be at the wrong node. It is
raised when a caller's epoch is older than this node's; when it is *newer*, the
answer is `ErrActivationPending` instead, because it is this node that is behind
and it will catch up. Until M4 nothing carries an epoch across a boundary, so in
M3 this error is reachable only through `Guard.CanActivateAt` directly.

`ErrNotOwner` and `ErrActivationPending` are both about ownership and they are
retryable in opposite directions. `ErrNotOwner` means *go somewhere else* —
re-resolve and ask the owner. `ErrActivationPending` means *ask again here* —
this node is the owner and is deliberately waiting for the previous one to
notice. Treating them as one error would send a caller away from the node that
is about to be able to serve it. Until M4's router exists, both are surfaced to
the caller directly.

An expired context is reported ahead of anything else: an expired deadline is
the caller's answer, whatever the callee would otherwise have said.
