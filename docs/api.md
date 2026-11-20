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
| `ExampleRegisterTypedGroupFunc` | a group function with a typed argument and result; a call with the wrong type refused |
| `ExampleNamespaceOf` | typed values, a typed loader, a missing key, a second type on one name refused |
| `ExampleNamespace_Set` | TTL expiry under an injected clock; non-positive TTL |
| `ExampleNamespace_Get` | the aliasing contract — `Set` copies, `Get` does not |
| `ExampleNamespace_BatchGet` | batch semantics; absent keys omitted |
| `ExampleWithRefreshAhead` | a hot key reloaded before it expires, the caller never waiting |
| `ExampleWithTTLJitter` | a burst of writes with one TTL expiring spread out, none past the TTL |
| `ExampleWithBatchLoader` | `BatchGetOrLoad`: one loader call for every missing key, missing rows omitted |
| `ExampleNamespace_Increment` | a counter in a window, `SetIfAbsent` claiming once, `CompareAndSwap` refusing a stale value |
| `ExampleWithLoader` | `GetOrLoad`, loader registration, `ErrNoLoader` |
| `ExampleWithLoaderTimeout` | the load is bounded by the namespace, not the caller |
| `ExampleWithNegativeCaching` | a missing key loaded once, and a Delete forgetting the absence |
| `ExampleWithCompression` | opt-in compression, threshold, unknown compressor rejected |
| `ExampleWithShards` | per-namespace shard count |
| `ExampleWithL1MaxBytes` | the non-owned tier bounded in bytes, and zero refused |
| `ExampleWithCatchUpRate` | the catch-up rate configured, a negative one rejected |
| `ExampleWithJoinProbation` | probation configuration, a single node unaffected, a negative value rejected |
| `ExampleWithMemoryBudget` | shedding to stay within a byte budget, counted |
| `ExampleWithKeyTrace` | a key's history recorded with its writer and order, an untraced key with none |
| `ExampleWithRequireReplicationFactor` | a synchronous write below its factor: counted by default, refused on request, held either way |
| `ExamplePartitionByPrefix` | key-to-group mapping |
| `ExampleCache_RegisterGroupFunc` | registration, duplicates, unknown names |
| `ExampleNewRouter` | queue-workload routing |
| `ExampleCache_Describe` | the introspection surface: node, namespaces, one key's placement |
| `ExampleWithCatchUpGate` | a cache that holds its peers until its first catch-up; alone, ready at once |
| `ExampleWithPersistence` | write, close, reopen over the same store: restored, and a delete stays deleted |
| `ExampleWithReplicaReads` | the configurations replica reads refuse, and their counters |
| `ExampleCache_WriteMetrics` | the metrics surface; text exposition format, no client library |

End-to-end programs, each run with `go run ./examples/<name>`: `lifecycle`
(construction, validation and shutdown), `namespace` (the local data path),
`groups` (the group actor runtime), `ownership` (a real three-node cluster over
loopback gossip), `remote` (remote routing), `replication` (replication and
failure), `rolling-upgrade` (rebalancing and drain), `observability` (the scrape
endpoint and the counters that matter) and `failure-guarantees` (a partition heal
that moves replicas rather than dropping them, and the synchronous write
acknowledged below its replication factor). Each one asserts what it demonstrates
and all are run by CI.

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

Every counter is exported: `MetricsHandler` serves
66 series in the Prometheus text exposition format, and PeerCache imports no
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

    CatchUp(ctx context.Context) error   // see "CatchUp — required after a restart"
    Drain(ctx context.Context) error     // the PreStop hook
    Ready() ReadyState                   // the readiness probe
    WaitReady(ctx context.Context) error

    Metrics() []Metric                   // see "Observability"
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
    BatchGetOrLoad(ctx context.Context, keys []string) (map[string][]byte, error)
    BatchSet(ctx context.Context, kv map[string][]byte, ttl time.Duration) error
    GetOrLoad(ctx context.Context, key string) ([]byte, error)

    SetIfAbsent(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error)
    CompareAndSwap(ctx context.Context, key string, old, new []byte, ttl time.Duration) (bool, error)
    Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)
    Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
```

A missing key returns `ErrNotFound`, never a nil value, so that a key holding
zero bytes stays distinguishable from a key holding nothing.

`BatchGet` groups keys by owner and issues one request per owner, so its cost
scales with the number of distinct owners rather than the number of keys.
`BatchSet` routes each pair to its owner as `Set` would, one request per key; it
is not yet coalesced per owner. Neither is atomic across owners, and no
cross-group transaction exists to make them so.

### Conditional writes and counters

`SetIfAbsent`, `CompareAndSwap`, `Increment` and `Touch` each run on the key's
owner as one step under the store's lock, so no other write to the key can land
between the check and the write, and then replicate exactly as `Set` does. For
logic over several keys, use a group; these are for one key.

```go
// A lock, a lease, an idempotency key: exactly one caller creates it.
created, err := ns.SetIfAbsent(ctx, "job:42/claim", []byte(podName), 30*time.Second)

// Optimistic update: write only if nobody changed it since it was read.
cur, _ := ns.Get(ctx, "config")
swapped, err := ns.CompareAndSwap(ctx, "config", cur, next, 0)

// A rate-limit window: the TTL starts with the first hit and is not extended.
hits, err := ns.Increment(ctx, "rl:user:7:"+minute, 1, time.Minute)

// A session kept alive by activity.
alive, err := ns.Touch(ctx, "session:abc", 30*time.Minute)
```

`Increment` stores the value as base-10 text, so `Get` reads it and `Set` can
initialise it. A missing key counts as zero. A value that is not an integer, or
a sum that would overflow `int64`, is `ErrNotAnInteger` and changes nothing.

**Only `Touch` is safe to retry.** The cache itself re-resolves the owner once
after `ErrNotOwner`, `ErrStaleEpoch` or `ErrActivationPending`, because those are
answered before anything is applied. After any other error — `ErrNodeUnreachable`,
a deadline — the owner may already have applied the operation, and the cache
does not retry it: an `Increment` retried after a lost reply counts twice. A
caller that must retry should make the operation idempotent itself, for instance
by `SetIfAbsent` on a request ID.

The guarantees are `Set`'s. An asynchronously replicated counter can lose
increments acknowledged inside the replication window when its owner dies, and
a partition with two activations can accept increments on both sides — the
replica counts that as a split-brain detection and does not add them together.
`TypedNamespace[T]` has `SetIfAbsent` and `Touch`; it has no `CompareAndSwap`,
because equal values of `T` need not encode to equal bytes.

### Namespace options

Options apply when a namespace is first opened; the first call wins, and a
later `Namespace(name, opts...)` returns the open namespace unchanged. If
that later call's options would have changed it — a different `WithL1TTL`, a
loader for a namespace opened without one — the cache logs
`namespace options ignored: the namespace is already open` at Warn, once per
namespace, with the option names. Repeating the original options, a subset of
them, or none is silent. A namespace a peer's request opened first, with
defaults, is the usual way to meet this warning: open every namespace before
joining (open question 26).

| Option | Default | Meaning |
|---|---|---|
| `WithLoader(Loader)` | none | Reconstructs a missing value. Without it, `GetOrLoad` returns `ErrNoLoader`. |
| `WithPartitioner(Partitioner)` | `PartitionByKey()` | Maps keys to groups. Must be pure and identical on every pod. |
| `WithReplicationFactor(int)` | 1 | 1 to 3. Outside that range is `ErrInvalidOption`. |
| `WithCodec(string)` | `msgpack` | The codec a `NamespaceOf` view encodes values with: `json` or `msgpack`. Unknown names fail at construction. The byte API ignores it. |
| `WithTypedLoader(func(ctx, key) (T, time.Duration, error))` | none | A loader returning `T`, for `NamespaceOf`. Conflicts with `WithLoader`. |
| `WithL1TTL(time.Duration)` | 5s | Upper bound on staleness for non-owned copies. |
| `WithL1MaxBytes(int64)` | 64 MiB | Bounds the memory of the namespace's non-owned copies on each node: key, value and a fixed 192-byte overhead per copy, measured. Past it copies are evicted (round trips, never data); a copy larger than the bound is not cached. Zero or negative is `ErrInvalidOption`. Eviction is asynchronous, so a write burst can overshoot briefly; leave headroom. |
| `WithPersistence(persist.BlobStore)` | off | Writes the namespace behind to a blob store so keys survive losing every holder. See below. |
| `WithReplicaReads()` | off | Lets a key's replicas answer reads of it, bounded by the L1 TTL. Needs RF ≥ 2 and an L1 tier. See below. |
| `WithCompression(name string, threshold int)` | off | Compresses stored values at or above `threshold` bytes. `lz4` is registered. |
| `WithLoaderTimeout(time.Duration)` | 30s | Bounds one loader invocation. |
| `WithNegativeCaching(time.Duration)` | off | The owner remembers a loader's `ErrNotFound` for the TTL and answers `GetOrLoad` without loading. Any write of the key forgets it. Negative is `ErrInvalidOption`. See below. |
| `WithRefreshAhead(time.Duration)` | off | A `GetOrLoad` hit on the owner with less than this left of its lifetime returns at once and reloads the key in the background. Needs a loader. See below. |
| `WithTTLJitter(float64)` | 0 | Shortens every TTL the owner assigns by up to this fraction, fixed per key, so keys written together do not expire together. Never lengthens. Outside `[0, 1)` is `ErrInvalidOption`. Decision 0037. |
| `WithBatchLoader(BatchLoader)` | none | Loads every key a `BatchGetOrLoad` owner is missing in one call. Without it, `BatchGetOrLoad` uses the `WithLoader` loader per key. See below. |
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

### BatchGetOrLoad: one query for many misses

```go
rows := cache.Namespace("products",
    peercache.WithBatchLoader(func(ctx context.Context, keys []string) (map[string][]byte, time.Duration, error) {
        return db.LoadProducts(ctx, keys) // SELECT ... WHERE id IN (...)
    }),
)
page, err := rows.BatchGetOrLoad(ctx, productIDs)
```

Keys go to their owners as `BatchGet`'s do, one request per owner. Each owner
answers what it holds and calls the batch loader **once** with every key it is
missing, so a page that misses fifty keys over three owners costs three
requests and three queries, not fifty of each.

- A key absent from the loader's map does not exist and is omitted from the
  result, as `BatchGet` omits absent keys.
- One TTL applies to every value a call returns.
- Each value is stored only if no write landed while the loader ran; the write
  wins and is what the caller gets.
- A failing owner's keys are missing and its error is joined into the returned
  error, beside every other owner's keys. There is no atomicity across keys.
- Without `WithBatchLoader` the namespace's per-key loader runs for each missing
  key on its owner, through `GetOrLoad`'s single-flight: the round trips are
  saved, the queries are not. With no loader at all, `ErrNoLoader`.
- A batch load does not join a concurrent `GetOrLoad` of one of its keys: both
  may query, and the first store wins. Never a wrong value; occasionally one
  extra query for a key hot enough to be in flight.
- During a rolling upgrade a peer on an earlier release answers a load request
  as a `BatchGet`: what it holds, nothing loaded.

Decision 0035.

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

### Refreshing a key before it expires

```go
prices := cache.Namespace("prices",
    peercache.WithLoader(loadPrice), // returns a 60s TTL
    peercache.WithRefreshAhead(10*time.Second),
)
```

Without it, a popular loaded key is reloaded when it expires, and every caller
of it waits for that load: single-flight makes it one query, not one wait. With
it, a `GetOrLoad` on the key's owner that finds it with less than the window
left returns the current value at once and reloads the key in the background;
the next read gets the new value with a fresh lifetime. A key nobody reads in
its last ten seconds is not reloaded — it expires, as before.

- **One reload at a time**, in the key's single-flight: hits during it do not
  start another, and a caller that misses because the key expired meanwhile
  joins it.
- **A write wins.** A `Set`, `Delete` or atomic operation that lands while the
  loader runs is newer than what it read; the reload's value is dropped.
- **A failed reload keeps the value** until its own expiry, so a database
  restart at the wrong moment no longer empties the hottest keys. Counted in
  `Stats().RefreshFailures`; completed reloads in `Refreshes`.
- **Owner only, `GetOrLoad` only.** `Get` never loads. Non-owners see the new
  value after their L1 copy's TTL, as they see every write.
- `Close` waits for a reload in flight; the loader's own timeout bounds it.

Decision 0036.

### Remembering that a key does not exist

```go
users := cache.Namespace("users",
    peercache.WithLoader(loadUser), // returns peercache.ErrNotFound for no such row
    peercache.WithNegativeCaching(30*time.Second),
)
```

Without it, a stream of lookups for keys that do not exist — deleted users, bots
probing IDs — reaches the database once per request: single-flight collapses
only *concurrent* misses. With it, when the loader returns an error matching
`ErrNotFound`, the key's owner remembers the absence for the TTL and answers
`GetOrLoad` with `ErrNotFound` without calling the loader. Probes through any
pod reach that owner, so the database is asked once per key per TTL. Each saved
load is counted in `Stats().NegativeHits`.

- **Only `ErrNotFound` is remembered.** Any other loader error — the database
  down — is returned and forgotten, so an outage is never served as "not found".
- **An absence is not a value.** `Get`, `Exists`, `Keys`, replication,
  hand-off and persistence never see it.
- **Any write of the key forgets it**: `Set`, `Delete`, `Increment` and the
  other atomic operations. `Delete` is how an application that just created
  the row says "load this again". A write that lands while the loader runs wins
  over the loader's "not found".
- **It is bounded and owner-local:** at most 256 absences per shard, dropped
  when full (one more load, never a wrong answer), not replicated, and lost when
  the key changes owner.

Decision 0034.

### L1 TTL is a correctness bound

Not a tuning knob. Invalidation broadcasts are a latency optimisation and may be
lost; this TTL is the only thing that turns an unbounded stale read into a
bounded one. Longer means faster and staler, and the staleness is what a caller
has to reason about.

A copy never outlives its key: a key with less than an L1 TTL left is cached
elsewhere only for what it has left, so a session that expires on its owner is
gone everywhere at the same instant.

### Persistence — `WithPersistence`

Off by default; a namespace without it is memory-only, exactly as before. With
it (`docs/decisions/0029-persistence-as-a-segment-log.md`):

- **Write-behind.** Each write is recorded in memory and flushed every 100 ms
  (`persist.WithFlushInterval` changes it) as **one** immutable log object per
  flush, whatever partitions it touches. A key written a thousand times between
  flushes costs one record. A write is acknowledged before its flush: a crash
  loses at most the last interval, and a graceful `Close` or drain flushes
  first.
- **Split.** Every 10 s a namespace's splitter — the rendezvous owner of a
  synthetic key — moves the logs into per-partition segments, and deletes the
  logs only once those are durable (`docs/decisions/0030-one-object-per-flush.md`).
- **Restore.** A node that comes to own keys it does not hold — at start, or
  because every holder of them died — restores their partition on first access,
  keeping whichever of memory and storage is newer. Reads and writes of that
  partition wait for it, within their deadline.
- **Order.** Writes carry a hybrid logical clock through replication, catch-up
  and promotion, so a replica that takes over continues the key's history. Two
  writes to one key by owners that never exchanged an order, closer together
  than their clocks' skew, may be restored either way; writes from a network
  partition resolve last writer wins. Neither is merged.
- **Compaction.** Each partition's compactor — the rendezvous owner of a
  synthetic key — folds segments into snapshots once eight have collected, and
  drops deletions an hour after they were written.
- **Shedding.** Under `WithMemoryBudget`, an owned key may be dropped from
  memory; it is never dropped from storage, and comes back on its next access.
- **An unavailable store** never fails a write: records wait, one per key, and
  are counted against the memory budget. A restore that cannot read the store
  serves the partition from memory and retries a second later.

Backends implement `persist.BlobStore`, four methods with an atomic `Put`, and
run the contract in `persist/persisttest`. Two ship in the core module:

- `persist.Dir(root)` keeps each object as a file: written to a temporary file,
  fsynced, renamed into place, directory fsynced. A crash at any point leaves
  the old object or the new one, and its leftovers are cleaned up when the
  directory is next opened. It is one process's store; for a key to survive
  the loss of its node, every node must reach the same files — a shared volume.
  On Windows a directory cannot be fsynced, so whether a rename survives a power
  loss at once is the file system's journal's guarantee, not this package's.
- `persist.Memory()` survives nothing and is for tests.

And one in its own module, so that the AWS SDK is a dependency only of programs
that use it:

- `github.com/arwenizEr/peercache/persist/s3` — `s3.New(client, bucket, prefix)`
  over an aws-sdk-go-v2 S3 client, for S3 and S3-compatible stores (MinIO, R2,
  Ceph RGW). Several namespaces or clusters can share a bucket under different
  prefixes. Use `persist.WithFlushInterval(time.Second)` or longer; see the
  table below.

The loss window on `persist.Dir`, measured on Machine A: a write is durable at
most one flush interval plus one durable write after it is acknowledged — p99
104–107 ms against the 100 ms interval.
`PersistPending`, `PersistFlushes`, `PersistRecordsWritten`, `PersistFailures`,
`PersistRestored`, `PersistRestoreFailures`, `PersistCompactions`,
`PersistCompactFailures`, `PersistSplits` and `PersistSplitFailures` report it.

**Choosing a flush interval.** Each flush is one request per node; each split at
most 64 per cluster. On a local disk the default 100 ms is cheap. On an object
store that charges per request, a second is the usual choice:

| flush interval | requests per loaded node | loss window | ≈ S3 Standard PUTs per node per month |
|---:|---:|---:|---:|
| 100 ms | 10/s | ~100 ms | $130 |
| 1 s | 1/s | ~1 s | $13 |
| 5 s | 0.2/s | ~5 s | $2.60 |

Plus at most 6.4 split requests a second per cluster (≤ $83 a month). Prices
are list prices at the time of writing; check them. Before decision 0030 a
loaded node wrote one object per partition per flush — 640 requests a second,
about $8,000 a month per node — which is why a flush is one object.

Measured on Machine A: a persistent `Set` costs about twice a plain one
(851 ns against 430), and a namespace without persistence is within noise of
before M14.

### Hot keys — coalesced misses and `WithReplicaReads`

Two things keep one popular key from flattening its owner
(`docs/decisions/0028-hot-key-reads.md`).

**Concurrent misses share one fetch, always.** When a key's L1 copy expires or
is invalidated, every goroutine on a pod that reads it in that instant used to
send its own request: a thousand callers were a thousand RPCs, measured. Now
they share one, and `Stats().CoalescedReads` counts the ones that joined. The
shared fetch runs under the deadline of the caller that started it, so a waiter
with time left does not inherit that caller's failure: it fetches again under
its own.

**`WithReplicaReads()` spreads refills across the key's holders, on request.**
Each pod always asks the same holder for a given key — the owner or one of its
replicas, picked by rendezvous hash — so a key's readers divide among all `RF`
of them. A pod that is itself a replica reads its own copy with no round trip.
At RF 3, 200 remote reads sent 67 to owners instead of 200.

What a reader may see is stated, not hoped for:

- A replica answers only with a copy it received **less than one L1 TTL ago**,
  and the reader caches it only for what is left of that TTL. A value read this
  way is stale by at most **the L1 TTL plus the replication delay** — and that
  delay is measured (`docs/benchmarks.md`), not bounded.
- A replica that lacks the key, holds an older copy, or no longer ranks as a
  replica **declines**, and the read goes to the owner within the same
  deadline. A replica never reports a key absent; only the owner knows that.
- An invalidation leaves a *floor* in the reader's L1, so a replica that has
  not yet received the write cannot put the old value back. A lost invalidation
  leaves no floor and the TTL bounds the copy, as it always does.
- A key quiet for longer than the TTL is always read from its owner: replicas do
  not revalidate. `ReplicaReadsReferred` rising while `ReplicaReadsServed` holds
  steady is what that looks like.

It applies to `Get` only. `GetOrLoad`, `BatchGet`, `Exists` and writes go to the
owner, and a hot *group* is still bounded by its single activation.

---

### Typed namespaces — `NamespaceOf[T]`

```go
type User struct {
    Name  string
    Admin bool
}

users := peercache.NamespaceOf[User](cache, "users",
    peercache.WithReplicationFactor(2),
    peercache.WithTypedLoader(func(ctx context.Context, id string) (User, time.Duration, error) {
        return db.LoadUser(ctx, id)
    }),
)
u, err := users.Get(ctx, "42")             // User
err = users.Set(ctx, "42", u, time.Hour)
all, err := users.BatchGet(ctx, ids)       // map[string]User
raw := users.Raw()                         // the []byte Namespace underneath
```

A function, not a method, because Go interfaces cannot have type parameters.
`TypedNamespace[T]` has the operations of `Namespace`, in `T`, and the same
guarantees: the view encodes to bytes, so routing, replication, the L1 tier,
TTLs and the memory budget see exactly what they always did.

**The codec** is `WithCodec`'s, or MessagePack when none is named
(`DefaultTypedCodec`). Not JSON by default, because the JSON codec replaces
invalid UTF-8 in a string with U+FFFD and reports success (open question 41) — a
string field carrying arbitrary bytes would come back different with nothing to
say so. Name `json` if values must be readable in a dump. Options, the codec
included, must be the same on every pod; a node encoding with one codec and
another decoding with a different one is a type mismatch by another name.

**Mismatches are errors, not decodes.** One name holds one type's encoding, so
opening it again with a different `T` makes every operation on the later handle
return `ErrInvalidOption`; the first handle is unaffected. A `WithTypedLoader`
whose `T` is not the namespace's is `ErrInvalidOption` too — options are untyped,
so the compiler cannot catch it. A value that will not encode, or bytes under a
key that will not decode as `T`, is `ErrCodec`. A missing key is the zero `T` and
`ErrNotFound`.

**The cost is the codec.** On Machine A a typed `Get` of a four-field struct
takes 487 ns where the byte path takes 37 ns: the difference is one MessagePack
decode, 554 ns measured alone. A typed `Set` is 841 ns against 351 ns plus a
456 ns encode. For a read-heavy hot path where nanoseconds matter, keep a
`[]byte` value and decode once; `Raw()` gives the byte view of the same data.
See `docs/benchmarks.md`.

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

**`Call` works from any node; the rest work on the owner.** `Get`, `Set`,
`Delete`, `Keys` and `Items` act on the group's state where it lives and return
`ErrNotOwner` anywhere else, and `Flush` on a non-owner does nothing. That is the
design rather than a gap: moving a group's state to its caller is what groups
exist to avoid, so a caller elsewhere sends the computation with `Call`. Unlike
a `Namespace`, a `Group`'s direct accessors are for code already on the owner.

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
each state with a generation; a method called on a stale state does nothing and
increments `Stats().StateEscapes` — detected, not panicked, and not silently
corrupting. There is no error for it: the goroutine misusing the state is not
inside a call, so there is no caller to return one to.

---

### Typed group functions

```go
err := peercache.RegisterTypedGroupFunc(cache, "addToCart",
    func(ctx context.Context, s peercache.GroupState, item Item) (Cart, error) {
        // s is the group's []byte state, as for any group function.
        ...
    })
cart, err := peercache.CallTyped[Item, Cart](ctx, cache.Group("user:42"), "addToCart", item)
```

`RegisterGroupFunc` and `Group.Call` with the argument and result encoded by
`DefaultTypedCodec`. The rules are the same — registered once, at startup,
identically on every pod; one name, one function, typed or not — and the state
stays byte-valued, because a group's keys are the function's own business.

Every pod runs the same binary, so the local registration says what a function
takes and returns: `CallTyped` with other types, or on a function registered
untyped, is `ErrInvalidOption` before anything is sent. An argument that does not
decode inside the function's turn, or a result that does not decode at the
caller, is `ErrCodec` — which survives the gRPC hop like the other sentinels. An
error the function returns comes back as it is.

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

**mTLS covers the data plane only; gossip is a separate socket.** SWIM messages
carry every node's ID and data address, and an unauthenticated gossip port lets
anything that can reach it join the membership, be ranked as an owner, and be
sent writes — which mTLS then refuses, so the keys it ranks for fail rather than
leak, but the cluster is still disrupted. Encrypt gossip with memberlist's own
keyring, through the membership options' `Config`:

```go
cfg := memberlist.DefaultLANConfig()
cfg.SecretKey = gossipKey // 16, 24 or 32 bytes: AES-128, -192 or -256, from a Secret
gossip, err := membership.New(membership.Options{Self: self, Config: cfg})
```

Every message is then encrypted and verified, and a node without the key cannot
join (`TestEncryptedGossipAdmitsOnlyTheKeyHolders`). To rotate, install the new
key on every node's `cfg.Keyring` (`AddKey`) before making it primary
(`UseKey`), then remove the old one. On the data plane, a node dialling a peer
verifies the peer's certificate against the CA *and* its name against the
peer's node ID (or `ServerName`); a node serving a request verifies the
caller's certificate against the CA only. Any workload holding a certificate
from the same CA can therefore call a node, so give the cache its own CA, or
issue its certificates from one no other workload uses.

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
// Once the view has settled. Ready() stays false until this has run, so this
// is what opens the readiness gate — do not wait for the gate first.
for !cache.Ready().Settled {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(250 * time.Millisecond):
    }
}
if err := cache.CatchUp(ctx); err != nil {
    log.Warn("catch-up incomplete", "error", err)
}
```

The order is join, settle, `CatchUp`, ready. Waiting for `Ready()` before calling
`CatchUp` waits forever: its `Reason` stays "replicas not yet pulled; call
CatchUp". Calling it before the view settles is safe but may pull from a view
that is about to change; call it again after a membership change (see below).

**A restarted node that skips this will report keys as missing when they are
not.** Replication carries writes, not state, so a returning node receives only
what is written after it comes back — while the ownership ranking gives it back
its keys immediately. It then answers `ErrNotFound` for data that exists
elsewhere in the cluster.

It pulls two things from every peer: the keys that peer owns and this node
replicates, and the peer's replica copies of keys this node owns. The second
matters when a node comes back faster than its peers' failure detector — a
killed pod restarted by its kubelet in a second or two. Nobody declared it dead,
so nobody was promoted in its place, and the newest copies of what it owns are
replicas on its peers.

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
going past each one: one unreachable peer is no reason to skip the others.

### Conflicts

`Stats().SplitBrainDetections` counts forks a replica saw in a key's history.
Every replicated write carries the order of the value it replaced; a replica
holding a copy from one writer that receives a copy from another, where neither
replaced the other's write, has seen two writers each unaware of the other. One
group has one owner, so that means two nodes believed they owned the same key —
the partition case — or a new owner wrote over a value it never received. It is
counted and never merged; choosing between conflicting writes is the
application's business.

It is detection, not proof of absence: a copy that carries no history (a first
write, a key relayed by its owner's catch-up, a peer on an earlier release) is
not judged. It used to count "two writers in one epoch", and epochs are per view,
so after a rolling restart it counted handovers by the tens of thousands.

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

#### `WithCatchUpGate` — holding peers until caught up

```go
cache, err := peercache.New(
    peercache.WithMembership(gossip),
    peercache.WithTransport(tr),
    peercache.WithCatchUpGate(),
)
```

Readiness keeps *clients* away from a node that has not caught up. It does not
keep *peers* away: a restarted node owns its keys from the moment it joins, and
every other node forwards writes for them to it at once. Applied before catch-up
has brought the newest values back, such a write lands on an empty or
persisted-but-older base — an `Increment` counts from the wrong value, a
`SetIfAbsent` succeeds over a key that exists — and the catch-up that follows
discards it as older than the copy it brings. Under kill -9 on kind that was 1
to 3 lost decrements on 5 of 50 stock counters.

With the gate, until this node's first `CatchUp` completes, its peers'
operations on keys it owns, in namespaces with a replication factor of 2 or
more, are answered with `ErrActivationPending`. Nothing is applied; the
forwarding node retries once and returns the error, which an application treats
as refused-before-applied and retries. This node's own calls are not held —
they are the application's, and `Ready` already says when to make them.

Turn it on wherever the application calls `CatchUp` before serving, as the
deployment in `deploy/` does. It is off by default because a node whose
application never calls `CatchUp` would refuse its peers indefinitely.

#### `WithCatchUpRate` — how fast, not just when

```go
cache, err := peercache.New(
    peercache.WithMembership(gossip),
    peercache.WithTransport(tr),
    peercache.WithCatchUpRate(20_000), // entries per second, across all peers
)
```

The application decides *when* `CatchUp` runs; this bounds *how fast*. Without
it, a node joining at peak traffic has every peer serve it 512-entry pages as
fast as they can, competing with their own requests. With it, after each page
`CatchUp` waits as long as that page's entries take at the rate, so the average
over the whole catch-up stays at or under it. The node stays unready while it
catches up, so choose a rate that still finishes within the rollout's patience:
20,000 a second is a million keys in under a minute. The wait is on the cache's
clock and ends at the context's deadline. Zero, the default, does not pace; a
negative rate is `ErrInvalidOption`.

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

### Mixed releases during a rolling upgrade

A rolling upgrade runs two releases in one cluster. Adjacent releases
interoperate: every change runs in CI beside the previous release tag, over
real gossip and gRPC, writing and reading each other's keys, batches and
group calls and draining across releases (`scripts/crossversion.sh`, decision
0041). The same run checks persisted storage: a cold start of either release
reads back what a cluster of both wrote. Two releases apart is not promised;
upgrade one release at a time.

What a mixed cluster cannot do is use what the earlier release lacks: a
function only the new binary registers fails with `ErrFuncNotRegistered` on an
old owner, an RPC an old peer does not know with `ErrNotImplemented`, and a
batch load is answered by an old owner as a plain `BatchGet`.

---

### `WithMemoryBudget` — shedding instead of being OOM-killed

```go
// A 4 GiB container, run with GOMEMLIMIT=3600MiB.
cache, err := peercache.New(
    peercache.WithNodeID(podName),
    peercache.WithMemoryBudget(2 << 30), // half the container limit
)
```

Caps the bytes the owned stores, the replica store and the L1 tier may hold:
keys and values, plus a per-entry overhead measured at a map's worst point,
just after it doubles — 192 bytes an owned entry, 240 a replica, 192 an L1
copy (`TestAccountedBytesCoverTheLiveHeap`). The overhead is charged per slot a
map keeps — a replica namespace's shard, an owned store's shard — since a Go map
gives none back when entries leave it; a map is rebuilt once it holds under
three quarters of its slots, and after every shed, which is what frees them. Activated groups are not counted —
`WithMaxActiveGroups` bounds those — and neither are RPC buffers or catch-up
pages in flight. What a persistent namespace's restores and compactions have
read from storage and not yet applied, the logs kept for them, and restored
keys waiting to be sent to their replicas are counted: on kind a failover's
restores held 150 to 200 MB of them, and the restore backlog 80 MB, outside a
budget that read 440 MB of 512 when its pods were OOM-killed. Off by default.

It bounds the data, not the process. Without a memory limit the Go runtime
lets the heap reach twice its live size before collecting: a pod budgeted at
512 MiB under a 1 GiB limit, without `GOMEMLIMIT`, was OOM-killed (open
question 55). Before 1.1.2 the overheads were estimates, 96 and 128 bytes,
that read about two thirds of the heap: on kind at 3.7 million keys, a pod
budgeted at half its 1 GiB limit, with `GOMEMLIMIT` set, never shed and was
OOM-killed once its replicas were complete. Run with `GOMEMLIMIT` at about nine
tenths of the container limit and the budget at about half of it. `New` logs a
warning when a budget is set and the process has no memory limit.

Over budget, the node sheds down to nine tenths of it, cheapest first. Before
any tier it drops the sorted key lists it keeps for the catch-up and hand-off
passes it serves, which are counted too (16 bytes a key) and which each pass's
next page rebuilds:

| Tier | What shedding costs | Counted in |
|---|---|---|
| L1, whole | round trips for keys owned elsewhere; never data | `ShedL1` |
| replicas | that key's replication factor, until it is next written or caught up | `ShedReplicas` |
| owned data | the key, which a loader rebuilds | `ShedOwned` |

A replica of a write to a persistent namespace is not shed while the write may
still be in its owner's memory alone: for one flush interval and the flush
timeout, 10 s, after the write, by the owner's write order. Until the owner
flushes, that copy is the write's only other holder, and shedding it would let
an owner killed in the window take an acknowledged write with it. Copies of
older writes, which catch-up and restores bring, are shed as before. What this
keeps is bounded by the write rate: one window's writes
(`TestAnUnflushedWriteSurvivesItsReplicaNodeShedding`, open question 60).

The budget is checked every 64 writes, on any write of
64 KiB or more, and once a second, so the accounted size can run past it by
about 64 entries between checks. `Stats().MemoryUsed` and
`peercache_memory_used_bytes` report the accounted size with or without a
budget, which is how to size one.

**A shed key is never served at an older value.** Shedding an owned key in a
replicated namespace sends its replicas the same removal a `Delete` sends, which
frees their memory too. If that message is lost, the owned store's version floor
still makes a later rewrite of the key outrank the replica's copy, so a failover
cannot promote the value that was shed over the one written after it.

`CatchUp` checks the budget before every page and stops with
`ErrOverMemoryBudget` instead of pulling what it would only shed;
`ReplicasCurrent` then stays false, which is the truth.

The case it exists for is a failover cascade: a node dies, its share moves to
the survivors, and a survivor sized for its steady state goes over its container
limit, is killed, and passes its share on. With a budget it sheds and keeps
serving.

**What that costs, measured.** `TestAFailoverCascadeStaysWithinBudget` kills
three of five nodes, one at a time, at RF=2, with a working set twice what two
survivors' budgets hold, and catches up after each death:

| | no budget | budget of half the data |
|---|---|---|
| each survivor's accounted memory at the end | ~956 KB, 1.9× the budget | ~453 KB, under it |
| keys readable at the end | 1500 of 1500 | 1152 of 1500 |

Without a budget nothing is lost — in the harness, where nothing is killed for
using memory. In a pod whose limit is near the budget, that column is three OOM
kills. With one, every survivor stays up and about a quarter of the keys are
gone: a shed replica leaves its key at one copy, and when the next node to die
held that copy, the key goes with it. No owned key was shed. Size the budget for
the failures you want to survive without loss; the budget decides what happens
past that.

### `WithJoinProbation` — containing a crashloop

```go
cache, err := peercache.New(
    peercache.WithNodeID(podName),
    peercache.WithJoinProbation(30 * time.Second),
)
```

Keeps a node out of the ownership ranking until it has been up for the given
time. Without it, every restart of a pod in CrashLoopBackOff is a join: every
peer ranks it at once, a share of the keyspace moves to a node holding nothing,
reads of that share report `ErrNotFound` for data the healthy nodes have, and
the share moves back when it dies. With a probation longer than the pod lives,
its restarts move nothing. `TestACrashloopingNodeIsNeverRanked` restarts a node
every 2 seconds for 30 rounds and counts zero missing reads.

The rule is a pure function of the member list and the time, so every node
applies it alike:

- a node is ranked once it has been up for the probation, measured from the
  start time its membership authority publishes (the built-in gossip and the
  test harness both publish one);
- a node that publishes none — a peer on an older build — is ranked, so a
  rolling upgrade is not held up;
- if no member has been up that long, which is a cold start, all are ranked.

A young node applies it to itself as well: it does not rank itself while its
peers do not, so it never acts as owner of keys they route elsewhere. It serves
by routing until its probation ends, and then joins like any other node — catch
up before relying on it.

The cost is that a new pod takes no load for that long. `Stats().MembersOnProbation`
and `peercache_members_on_probation` count the nodes waiting; one that keeps
reappearing there is a crashlooping pod. Clocks need to be roughly synchronised,
as Kubernetes nodes are; skew moves the moment a node is ranked by that much on
some peers, which the activation delay absorbs. Off by default.

### A slow node — CPU throttling

A pod at its CPU limit is throttled by the kernel in each scheduling period, and
from outside it looks slow: every message to it answers late. The library keeps
that lateness from spreading:

- A read goes to the key's owner and nowhere else, so reads of keys the slow
  node does not own never wait on it.
- Asynchronous replication is queued, so a write whose replica is the slow node
  returns without waiting. **Synchronous** replication does wait — that is what
  it is for — so a namespace with `WithSyncReplication` writes at the pace of its
  slowest replica, as documented under Replication.
- A slow owner fills its groups' mailboxes, and callers get `ErrGroupOverloaded`
  rather than queueing behind it without bound; deadlines propagate, so a caller
  is never held past its own.

`TestAStarvedNodeSlowsOnlyItsOwnKeys` delays every message to and from one node
of five by 200 ms and checks that reads of keys owned elsewhere, and
asynchronous writes, complete without the fake clock moving at all, while a read
of the slow node's own key does not complete until it moves past a round trip.
No split-brain is detected.

**What the harness cannot show** is the failure detector, because its
membership follows reachability rather than probe timing. In production that is
memberlist's: the default LAN profile probes every second with a 500 ms timeout,
suspects a node for `4·log(N+1)` seconds before declaring it dead, and backs its
own probing off by up to 8× when the local node is the one struggling. A node
answering in 200 ms is inside all of that. One throttled hard enough to miss
probes for the whole suspicion window is indistinguishable from a dead one, and
is treated as one.

Go 1.25 sets `GOMAXPROCS` from the container's CPU limit, so the runtime does not
schedule more parallelism than the quota allows. Prefer a CPU request with no
limit, or a generous one: throttling costs latency everywhere, and gossip's
latency is what the failure detector reads.

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

`Clock` is wider than docs/design.md section 3.6 specifies; see
`docs/decisions/0002-clock-interface.md`. `PeerClient` and `Handler` are defined
by this project, not by the spec; see `docs/decisions/open.md`.

`Watch` returns one channel per call, buffered, with non-blocking sends. A
watcher that stops reading loses changes rather than stalling membership.

### Ownership

Supply a `MembershipAuthority` with `WithMembership` and the cache derives
ownership from it: rendezvous hashing over the current view, stamped with the
view's epoch. Without one, the node is a cluster of one and owns everything,
which is the correct answer rather than a special case.

`github.com/arwenizEr/peercache/membership` provides a gossip-backed
implementation over hashicorp/memberlist, and
`github.com/arwenizEr/peercache/transport` the gRPC transport. Both are public
packages, so an application imports them directly.
`TestAnExternalModuleCanBuildACluster` compiles this page's wiring as a separate
module, so the documented way to build a cluster is checked from outside this
repository.

`WithSeeds` hands its addresses to the authority's `Join` once the transport is
serving, and `New` fails if the join does. Before, nothing read it.

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

It holds for every caller, this node's own included. Until 0.2.1 a caller on
the new owner itself skipped it: an `Increment` there ran before the replica
copy was promoted, counted from nothing, and the promotion then discarded the
copy as older (open question 56). A caller that retries for longer than the
delay, as the rolling-upgrade criterion's client does, sees no failure.

The default is 3 seconds. It is a guess about how long the previous owner takes
to notice it lost the group, and it narrows the double-activation window without
closing it: membership is gossip based and eventually consistent, so there is no
delay that makes two owners impossible. A negative value disables deferral
entirely, which is only sane on a single node.

Note the limit precisely: the guard runs on *activation*. A group that is
already active on a node that has just lost it keeps running there until it goes
idle; `Drain` is the graceful hand-off. See `docs/decisions/0012-the-activation-guard.md`.

`Stats().DeferredActivations`, `Stats().RefusedActivations` and
`Stats().StaleEpochRefusals` count these decisions. A spike in deferrals after a change that returns to zero is healthy;
sustained refusals under stable membership mean callers disagree with this node
about who owns what.

---

## Testing an application: `peercachetest`

```go
func TestACartSurvivesItsOwnerCrashing(t *testing.T) {
    c := peercachetest.New(t, 3, peercachetest.WithCacheOptions(appOptions...))
    for i := 0; i < c.Size(); i++ {
        c.Cache(i).Namespace("carts", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
    }
    _ = c.Cache(0).Namespace("carts").Set(ctx, "cart:7", cart, 0)

    c.Kill(c.Owner("carts", "cart:7"))

    got, err := c.Cache(1).Namespace("carts").Get(ctx, "cart:7") // promoted replica answers
}
```

An application's own code — group functions, loaders, how it reacts to
`ErrNotOwner` or a lost key — is worth testing under failover, and a cluster of
real pods is the wrong tool for a unit test. `peercachetest.New(t, n)` builds
`n` real caches in the test's process, connected by a simulated network and a
simulated membership on one fake clock, and closes them when the test ends.

| Method | Does |
|---|---|
| `Cache(i)` | node `i`'s `peercache.Cache` |
| `Owner(ns, key)` | the index of the node owning `key` |
| `Advance(d)`, `Clock()` | move the clock; nothing else does, so TTLs and timeouts wait for the test |
| `Kill(i)` | crash node `i` — no hand-off, memory lost — and wait until the survivors have reacted |
| `Restart(i)` | bring it back empty under the same identity; the test calls `CatchUp` |
| `Partition(groups...)`, `Heal()` | cut the network between groups of nodes, and restore it |
| `Settle()` | wait, without moving the clock, until membership and replication have caught up |

`Kill`, `Restart`, `Partition` and `Heal` advance the clock past failure
detection themselves, so the next line of the test sees the cluster as the
application would after the event. `WithCacheOptions` and `WithNodeOptions`
pass the options an application uses in production; the harness supplies node
IDs, the clock, membership and transport. PeerCache's own failover tests are
built on the same pieces. The package's tests are its examples
(`peercachetest/cluster_test.go`).

## Observability

### Hot keys — `WithHotKeyTracking`

```go
catalog := cache.Namespace("catalog", peercache.WithHotKeyTracking(20))
```

Each owner estimates the keys it serves most — its own operations and its
peers' forwarded ones — and reports the top n in `Describe` and on the
dashboard, merged across the cluster. Sampled one access in 16 into a fixed set
of counters (Space-Saving), halved every minute so the list is about now; a
cached read costs ~6 ns more. **It names keys**, the one thing the rest of the
debug handler never does, so enable it only on namespaces whose keys may be
read by whoever reaches the debug port. Decision 0040.

### The dashboard

Mount the debug handler (`admin.Handle("/debug/peercache/", http.StripPrefix("/debug/peercache", cache.DebugHandler()))`)
and open `/debug/peercache/ui`: this node's counters with rates, every member's
view and whether they agree, the namespaces, the replica check on demand and a
key lookup, refreshed every two seconds. It is one embedded page reading the
handler's JSON routes (`/`, `/stats`, `/cluster`, `/key`) and loads nothing from
anywhere else, so it works on an internal port with no internet access. On
Kubernetes: `kubectl port-forward pod/<pod> 8080` and open it locally.

### The whole cluster — `DescribeCluster`

```go
d := cache.DescribeCluster(ctx, true) // or GET /debug/peercache/cluster?digests=1
if !d.MembersAgree { log.Print(d.Disagreements) }
for _, p := range d.OutOfStep { log.Printf("%s partition %d: %d copies of %d", p.Namespace, p.Partition, p.HeldCopies, p.WantCopies) }
```

`Describe` is one node's belief; during an incident the questions are about the
cluster. `DescribeCluster` asks every member of this node's view for its
`Describe` (each bounded by `DescribeTimeout`) and reports them side by side,
with `MembersAgree` and, when not, what each node sees. A member it cannot reach
is reported with the error.

With digests it also checks the replicas. Each store keeps, on every write, a
running digest per partition of the keyspace (64, by key hash): a count and a
sum of hashes of each key with its version. Reading it costs the same whatever
the store holds. In step, the copies held across the cluster are exactly the
owned keys times `copies - 1`; `OutOfStep` lists the partitions where they are
not, with each node's owned and held counts, so the short node shows. It needs
every member's answer. Replication in flight, and keys around their TTL, can
show briefly; read twice to tell lag from loss. It detects, never repairs, and
like the rest of the debug handler holds no key and no value. Decision 0039.

### Tracing — `WithTracer`

```go
cache, err := peercache.New(...,
    peercache.WithTracer(pcotel.Tracer(otel.GetTracerProvider(), otel.GetTextMapPropagator())),
)
```

The hop to a key's owner happens inside the application's request, and without
tracing it is invisible there. With a tracer, every peer request is a span on
the calling node (`peercache.rpc <method>`, a client span) and handling it a
span on the serving node (`peercache.serve <method>`, a server span), and every
loader call — per key, batch or refresh — a `peercache.load` span on the owner.
Each is a child of the span the caller's context carries; trace context crosses
to the peer in request headers (`peercache-trace-…`), so a `Get` served by
another pod, and the database query its loader ran, appear in the caller's
trace.

Attributes name the nodes (`peercache.node`, `peercache.peer`), the namespace
and, for a load, how many keys — never a key or a value. Local operations are
not traced: a span would cost more than the read. The default tracer does
nothing. `peercache.Tracer` is four methods, so any tracing system can be
adapted; the `otel` module's `Tracer(provider, propagator)` adapts
OpenTelemetry. Decision 0038.


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
format with the content type a scraper expects, and 66 series come out of it.
`WriteMetrics` is the same output to any `io.Writer`; the two are tested to
produce identical bytes.

### Introspection — `Describe`, `DescribeKey`, `DebugHandler`

```go
Describe() Description
DescribeKey(namespace, key string) (KeyPlacement, error)
DebugHandler() http.Handler
```

Metrics are aggregate; these answer the questions an operator asks once
something is already wrong (`docs/decisions/0031-introspection.md`):

- `Describe` — this node, its view of the members and its epoch, its
  readiness, and per namespace its options and counts: owned, replica and L1
  entries, and for a persistent namespace its flush interval, pending records,
  flushes, failures, restores, compactions and splits.
- `DescribeKey` — a key's group key, owner and ranked holders as this node
  sees them, this node's role (`owner`, `replica`, `none`), and whether it holds
  the key in its owned store, as a replica copy, or in L1.
- `DebugHandler` — the same as JSON: `GET /`, `GET /namespaces`,
  `GET /key?namespace=…&key=…`. Other methods are 405; a missing parameter
  is 400 and an unopened namespace 404, with a JSON error body.

**No value ever appears**, and a key only when the caller names it. **There is
no authentication**: mount it on an internal admin port, as one would
`net/http/pprof`.

```go
admin.Handle("/debug/peercache/", http.StripPrefix("/debug/peercache", cache.DebugHandler()))
```

A description reads each field on its own, so one taken during a membership
change can mix two moments; it is a node's belief, not a consensus.

Every JSON route names its fields in camelCase — `/stats` answers
`{"self": {"id": …, "addr": …}, "members": 3, "ownedKeys": …}` — and
`TestDebugHandlerJSONNamesAreCamelCase` keeps a new field from going out under
its Go name.

### A key's history — `WithKeyTrace`, `KeyHistory`

```go
func WithKeyTrace(namespace, prefix string) Option
KeyHistory(namespace, key string) []KeyEvent
```

Metrics count events; a correctness incident is a sequence of them on one key
across several nodes. `WithKeyTrace` records, for the keys of a namespace that
start with a prefix, every change to what this node holds: a write or delete
the owner applied, a replica copy stored, refused or in conflict (a fork), a
promotion, a demotion, a hand-off, a restore from storage, a shed of the owned
value or of a replica copy, a read-back, a catch-up install. Each `KeyEvent` carries this node's clock time,
the write's order and writer, the stored version, and the value's length and a
hash — never the value (`docs/decisions/0043-key-trace.md`).

The newest 64 events per key are kept, for at most 4,096 keys per node. Keys
that are not traced cost one check per change, and a cache without the option
records nothing. The debug handler serves a key's history at
`GET /keys?namespace=…&key=…` as `{"node": …, "events": […]}`; a key's whole
history is the union over the nodes, merged by time, and the order and writer
on each event, not the times, say which value is newer.

```go
cache, _ := peercache.New(peercache.WithNodeID("pod-0"),
	peercache.WithKeyTrace("stock", "item-"))
stock := cache.Namespace("stock")
_, _ = stock.Increment(ctx, "item-7", -1, 0)
for _, e := range cache.KeyHistory("stock", "item-7") {
	fmt.Println(e.Kind, e.Writer) // write pod-0
}
```

Runnable: `ExampleWithKeyTrace`.

### The metrics that are worth alerting on

Most of the 66 are ordinary levels and rates. These twelve are not — each counts a
window this system is documented as having, and a non-zero value means the
window was entered.

| Series | Non-zero means |
|---|---|
| `peercache_split_brain_detections_total` | A replica saw writes for one key from two nodes under one membership epoch. Two nodes believed they owned it. This is the AP consequence of gossip membership actually happening. |
| `peercache_stale_epoch_refusals_total` | A caller's view was older than this node's and the operation was refused. Sustained under stable membership means callers disagree about ownership. |
| `peercache_implicit_namespaces_total` | A routed write created a namespace this node had never opened, so it got default options — replication factor 1. **Alert on this being non-zero.** Since v0.4 nothing on the request paths creates one (see the next row); the series stays, at zero. |
| `peercache_unopened_namespace_refusals_total` | A peer's request named a namespace this node's application has not opened, and was refused with `ErrActivationPending`, which the peer asks again for. A few while a node starts are normal. **Alert on a rate that does not fall:** this node's application never opens a namespace its peers use, and its share of that namespace cannot be served. Open every namespace, with its options, before joining. |
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

These are the five docs/design.md names as correctness metrics, plus the five this
implementation found it needed — the last two added when a partition heal turned
out to move replicas that the reconciler had been discarding.

`peercache_restore_replications_dropped_total` is not among them on purpose. It
counts keys a restore brought back from storage that were never sent to their
replicas, and it stays zero unless four million restored keys are waiting to
go at once: a restored key waits in a backlog of its own, apart from live
writes, for as long as the network takes. That backlog is kept apart so that a
restart's burst cannot reach `peercache_replications_dropped_total`, which
stays the one to alert on. Before 1.1.2 it held 1,024 and dropped the rest,
on the reasoning that a restored key's replicas still held it; after a rolling
restart they do not, because they restarted too.

`peercache_promotion_scans_total` is not among them either; it is a health
signal, not a correctness one. It counts passes that ranked every replica of a
namespace for ownership, which a node runs on a membership change, on every
tick for ten seconds after one, and at the end of a catch-up. Under stable
membership it is flat even while `peercache_promotion_rounds_total` rises: a
round that runs only to hand off copies that arrived out of place does not
scan. A rising rate under stable membership means the view is not settling.

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

### OpenTelemetry — `github.com/arwenizEr/peercache/otel`

A nested module, so the OpenTelemetry API is a dependency only of programs that
use it (`docs/decisions/0032-opentelemetry-bridge.md`):

```go
reg, err := pcotel.Register(otel.Meter("peercache"), cache)
if err != nil { ... }
defer reg.Unregister() // before cache.Close
```

One observable instrument per series — counter or gauge, as the series is —
with its help text as the description, and every observation carries
`peercache.node`. A counter's name drops its trailing `_total`, because
OpenTelemetry's Prometheus exporter appends one; through that exporter the
names come out as `MetricsHandler` serves them. It needs Go 1.26, as
OpenTelemetry v1.47 does. Latency histograms are not bridged.

### What is not exported

No per-namespace or per-key labels. The aggregate is what operators alert on,
and a cardinality explosion is a production incident in its own right. A
namespace breakdown would need an explicit option and is not built.

---

## Errors

Test with `errors.Is`. Errors wrap their cause with `%w`.

| Sentinel | Meaning | Retryable |
|---|---|---|
| `ErrNotImplemented` | An RPC method this binary does not recognise (a newer peer during a rolling upgrade), or a cache with no transport asked to reach another node. | no |
| `ErrNotFound` | Key or group does not exist. | no |
| `ErrClosed` | The cache has been closed. | no |
| `ErrGroupOverloaded` | Mailbox full; rejected rather than queued. | yes, after backoff |
| `ErrStaleEpoch` | Ownership was resolved under a view this node has already replaced. | yes, after re-resolving |
| `ErrNotOwner` | This node does not own the key's group, or is draining and has handed it on. | yes, after re-resolving |
| `ErrActivationPending` | This node owns the group but acquired it within `WithActivationDelay`. | yes, here, shortly |
| `ErrNoLoader` | `GetOrLoad` without `WithLoader`. | no |
| `ErrLoaderPanic` | A registered loader panicked. | no — it is a bug |
| `ErrGroupFuncPanic` | A registered group function panicked. The group keeps its state and stays usable. | no — it is a bug |
| `ErrLoaderFailed` | A load could not complete, e.g. the loader exited without returning. | no |
| `ErrFuncNotRegistered` | Unknown function name. | during a rolling upgrade, yes |
| `ErrDuplicateRegistration` | Name registered twice. | no |
| `ErrInvalidOption` | Option outside its range. | no |
| `ErrNodeUnreachable` | Transport could not deliver. | yes, except after `SetIfAbsent`, `CompareAndSwap` or `Increment`: the owner may have applied it |
| `ErrNotAnInteger` | `Increment` of a value that is not a base-10 `int64`, or a sum that overflows. The value is unchanged. | no |
| `ErrCodec` | A typed value would not encode, or stored bytes would not decode as the namespace's type — or a typed group function's argument or result would not. | no — the bytes will not change |
| `ErrOverMemoryBudget` | `CatchUp` stopped pulling because this node is over its `WithMemoryBudget`. | yes, once memory is available |
| `ErrUnderReplicated` | A synchronous write was applied on its owner but is held by fewer nodes than its factor: a replica did not take the copy, or — with `WithRequireReplicationFactor` — this node sees too few peers. The write happened, and its result (`Increment`'s value, `SetIfAbsent`'s `true`) is returned beside the error. | `Set`/`Delete` yes; not `SetIfAbsent`, `CompareAndSwap` or `Increment` — already applied |

**A peer's own failure is not `ErrNodeUnreachable`.** When the peer answered and
the answer was a failure — a loader whose database is down, say — the error
carries the peer's message and no sentinel. `ErrNodeUnreachable` means the peer
did not answer, so a backend outage behind one owner does not look like a
network fault.

`ErrGroupOverloaded` is backpressure working as designed. Blocking indefinitely
would convert one slow group into a cluster-wide stall.

`ErrStaleEpoch`, `ErrNotOwner` and `ErrActivationPending` are all about
ownership and they are retryable in different directions. `ErrStaleEpoch` means
*your view is out of date* — re-resolve, you may be at the wrong node. It is
raised when a caller's epoch is older than this node's; when it is *newer*, the
answer is `ErrActivationPending` instead, because it is this node that is behind
and it will catch up.

`ErrNotOwner` and `ErrActivationPending` are both about ownership and they are
retryable in opposite directions. `ErrNotOwner` means *go somewhere else* —
re-resolve and ask the owner. `ErrActivationPending` means *ask again here* —
this node is the owner and is deliberately waiting for the previous one to
notice. Treating them as one error would send a caller away from the node that
is about to be able to serve it. The remote data path re-resolves the owner
once on either error, and on `ErrStaleEpoch`, before surfacing it to the caller.

An expired context is reported ahead of anything else: an expired deadline is
the caller's answer, whatever the callee would otherwise have said.
