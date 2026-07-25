# Public API

Every symbol here exists and compiles today. Anything belonging to a milestone
that has not run yet returns `ErrNotImplemented`, which is a typed, testable
declaration of scaffolding rather than a comment nobody greps for.

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
| `ExamplePartitionByPrefix` | key-to-group mapping |
| `ExampleCache_RegisterGroupFunc` | registration, duplicates, unknown names |
| `ExampleNewRouter` | queue-workload routing |

End-to-end programs: `go run ./examples/m0` (skeleton and lifecycle),
`./examples/m1` (the local data path), `./examples/m2` (the group actor runtime)
and `./examples/m3` (a real three-node cluster over loopback gossip). Each one
asserts what it demonstrates and all are run by CI.

**Milestone status:** M0 through M3 complete. Namespaces are fully functional on
a single node; groups run as virtual actors with a single goroutine each,
bounded mailboxes, idle deactivation, an active cap, per-group timers and
lifecycle events; and nodes form a gossip cluster in which every node computes
the same owner for a key without coordinating.

What is *not* yet true: there is no transport between nodes. A call for a group
this node does not own returns `ErrNotOwner` rather than being forwarded. M4
adds the network hop and the router that makes ownership invisible to callers.

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

`Stats().DeferredActivations` and `Stats().RefusedActivations` count these
decisions. A spike in deferrals after a change that returns to zero is healthy;
sustained refusals under stable membership mean callers disagree with this node
about who owns what.

---

## Errors

Test with `errors.Is`. Errors wrap their cause with `%w`.

| Sentinel | Meaning | Retryable |
|---|---|---|
| `ErrNotImplemented` | Scaffolding for a milestone that has not run. | no |
| `ErrNotFound` | Key or group does not exist. | no |
| `ErrClosed` | The cache has been closed. | no |
| `ErrGroupOverloaded` | Mailbox full; rejected rather than queued. | yes, after backoff |
| `ErrStaleEpoch` | Attempted under an older membership epoch. | yes, after re-resolving |
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

`ErrGroupOverloaded` is backpressure working as designed. Blocking indefinitely
would convert one slow group into a cluster-wide stall.

`ErrNotOwner` and `ErrActivationPending` are both about ownership and they are
retryable in opposite directions. `ErrNotOwner` means *go somewhere else* —
re-resolve and ask the owner. `ErrActivationPending` means *ask again here* —
this node is the owner and is deliberately waiting for the previous one to
notice. Treating them as one error would send a caller away from the node that
is about to be able to serve it. Until M4's router exists, both are surfaced to
the caller directly.

An expired context is reported ahead of anything else: an expired deadline is
the caller's answer, whatever the callee would otherwise have said.
