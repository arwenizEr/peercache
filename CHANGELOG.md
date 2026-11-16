# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until `v1.0.0`, a minor version may
change the public API.

## [Unreleased]

### Changed

- Asynchronous replication sends up to eight keys' copies at once, instead of
  one at a time per node. A node's asynchronous replication rate was one over
  the round trip; on kind at about 140,000 orders a minute the queue in front
  of it overflowed and dropped some 250,000 replica writes a minute
  (`Stats().ReplicationsDropped`), each a write acknowledged and never copied.
  Over gRPC on loopback a copy now drains in about 38 µs rather than 351 µs.
  One key's copies still travel one at a time and in order. The queue's total
  bound is unchanged at 4,096, now split evenly across the senders.
- A persistent namespace restores up to eight partitions at once after a
  restart or membership change, instead of one at a time. A caller of a
  partition near the back of the queue waited for every restore in front of
  it; with 50,000 keys and 2 ms of storage latency, every partition now
  answers in 0.29 s instead of 2.4 s. At the peak eight partitions' records
  are in memory together rather than one.

## [1.0.0] - 2026-11-16

The first stable release. From here the promises in
`docs/decisions/0042-what-v1-promises.md` hold: the API you call is additive in
minor releases, the interfaces you implement are frozen until v2, adjacent
minor releases interoperate on the wire and in storage, and debug JSON fields
and metric names are never renamed or removed. Checked for this release: the
full mutation audit (every entry caught), the race-enabled suite, the
cross-version job against v0.3.0, and all six M16 scenarios on kind with no
acknowledged write lost.

### Changed

- A peer's request for a namespace this node's application has not opened is
  refused with `ErrActivationPending`, instead of creating the namespace with
  default options. Created that way it was fixed at replication factor 1 with
  no persistence; on kind a pod that joined before opening its namespaces lost
  its share of every acknowledged order. Refusals are counted
  (`Stats().UnopenedNamespaceRefusals`,
  `peercache_unopened_namespace_refusals_total`). Open every namespace before
  joining; a node that never opens one its peers use refuses its share of it.
  Open question 32.

### Added

- `Namespace(name, opts...)` on an already open namespace logs a warning,
  once per namespace, when its options would have changed it, naming the
  options that were ignored. The first call still wins, as before; repeating
  its options, a subset of them, or none stays silent. Open question 26: this
  was silent and once made a benchmark measure local hits as remote reads.
- `docs/decisions/0042-what-v1-promises.md`: what stops changing at v1.0 and
  how each promise is enforced. The interfaces an application implements are
  frozen by `TestImplementedInterfacesAreFrozen`; the ones the library returns
  say in their documentation that they may gain methods.

## [0.3.0] - 2026-11-16

A minor version because the public API and the debug handler's JSON change:
two exported variables became functions, and every debug response is now
camelCase. Wire and persisted formats are unchanged — `make crossversion`
runs v0.2.2 and this release in one cluster, both ways.

### Fixed

- A freshly started node's first gossip message advertised no RPC address:
  `membership.New` set it only after memberlist had built that message, so
  a peer that joined before the node's first republish dialled the gossip
  port for data and failed with `error reading server preface: EOF` until a
  later membership change corrected it. Tests that retried around the
  failure no longer do.

### Changed

- `transport.Methods` and `peercachetest.Epoch` are functions, `Methods()` and
  `Epoch()`, instead of exported variables: an importer could assign to them,
  and appending to `transport.Methods` before `transport.New` changed what the
  server registered. A repository test now keeps public packages from
  exporting any variable but an error sentinel.
- `ReadyState`'s JSON field names are camelCase (`ready`, `reason`,
  `caughtUp`, …) like the rest of `Description`'s, instead of Go's field names.
  The debug handler's `/` and `/cluster` responses change accordingly.
- `Stats` and `Node` have json tags too, so the debug handler's `/stats` is
  camelCase (`ownedKeys`, `self.id`, …) instead of Go's field names; it was
  the last PascalCase response. A test now walks every type the handler
  serialises and fails on a field with no camelCase tag, and another checks
  that each dashboard tile reads a field `/stats` has.

## [0.2.2] - 2026-11-15

### Fixed

- `BatchGetOrLoad` on a namespace with only `WithBatchLoader` failed the whole
  call with `ErrNoLoader` when an owner reported a key as no longer its own:
  the moved keys were asked for again one at a time through `GetOrLoad`, which
  needs a per-key loader. They are now asked for again as one batch, once; a
  key that moves again is reported with `ErrNotOwner`.

## [0.2.1] - 2026-11-15

Fixes for four ways v0.2.0 could lose acknowledged writes, all found on the
kind cluster — over accumulated data, through rolling restarts, and with a
pod OOM-killed — and each covered by a test that failed before. With them,
every M16 scenario holds every invariant, the partition included
(`docs/m16-results.md`). Two behaviour changes: the activation delay now holds
for a node's own callers as well as its peers', and a persistent partition
that could not be restored refuses operations on keys this node does not hold
rather than answering them from memory.

### Changed

- `WithMemoryBudget`'s guidance: about half the container limit, with
  `GOMEMLIMIT` at about nine tenths of it — not three quarters. The budget
  counts the data tiers; on kind the heap in use was about 1.55 times that, and
  without a memory limit the runtime lets the heap reach twice its live size.
  `New` now warns when a budget is set and the process has no memory limit.
  The shop's manifest sets `GOMEMLIMIT`.

### Changed

- With a persistent namespace's partition unable to restore from storage, a
  read or write of a key this node does not hold in memory now returns
  `ErrActivationPending` instead of being answered from memory as absent.
  Keys memory holds are still served. Log objects' names carry a random
  suffix, so a writer whose clock repeats an order cannot replace an earlier
  log. Open question 57.

### Fixed

- A write to a persistent namespace that waited for its key's partition to
  restore was applied even if the key had changed owner meanwhile, on the store
  the key had just left: an Increment counted from nothing and replicated to
  the new owner with a newer order than the real count. On kind about one
  rolling restart in six reset a stock counter. An owner now checks ownership
  again just before applying — a read-modify-write under the shard lock
  demotion takes. Open question 58.
- A restore failed whenever a log it had listed was split and deleted before it
  read it, which on a busy cluster was most of the time; a returning owner
  then served its partition from empty memory and counted stock from zero. A
  restore now lists again and reads only what is new until nothing has
  vanished. Failed restores are logged. Open question 57.
- Acknowledged read-modify-write operations were lost around ownership
  changes. A caller on a key's new owner skipped the activation delay that
  peers' requests wait out: its `Increment`, `SetIfAbsent` or compare-and-set
  ran before the replica copy was promoted, on an owned store without it, and
  the promotion then discarded the copy as older. And a drained node, whose
  own view still named it the owner of keys it had handed on, applied its own
  callers' and unaware peers' writes where nobody would read them. On kind,
  every stock counter one pod owned ended thousands of decrements off over a
  rolling restart. Local callers now wait out the activation delay, and a
  draining node refuses writes and reads with `ErrNotOwner`. Open question 56.
- A persistent namespace stopped flushing while it restored a partition from
  storage: the restore held the flush lock for its whole read, and after every
  membership change every partition restores again. Acknowledged writes piled
  up unflushed for the length of the restores, and a crash in them lost all
  of them — on kind, about 54,000 orders when a pod was OOM-killed during a
  rolling restart over ~600,000 orders a pod. The loss window is one flush
  interval again: a restore no longer takes the flush lock, and lays over
  storage what is pending, what a flush is writing and what flushes wrote
  while it read. Open question 55.
- Compaction and log splitting ran on the flusher's goroutine, so each held
  flushes off while it read storage. On kind with a million orders, pending
  records rose tenfold at every compaction tick, once a minute. They now run
  on a goroutine of their own.

## [0.2.0] - 2026-11-15

Validated on a five-pod kind cluster under kill, rolling-restart and partition
scenarios (`docs/m16-results.md`), and run beside 0.1.0 over real gRPC. It
removes two exported symbols (see Removed); a minor version may do that
before 1.0.

### Added

- A cross-release CI job: `scripts/crossversion.sh` runs this tree beside the
  previous release tag over real gossip and gRPC — keys, batches, group calls
  and a drain in both directions — so a wire change that breaks a rolling
  upgrade fails the pull request. Adjacent releases are what is supported.
  Decision 0041.
- `WithHotKeyTracking(n)`: owners estimate their n most accessed keys (sampled
  Space-Saving, halved every minute) and report them by name in `Describe` and
  on the dashboard. Opt-in per namespace because it names keys, the one
  exception to decision 0031. Decision 0040.
- A live dashboard at the debug handler's `/ui`: this node's counters with
  rates, every member's view and agreement, namespaces, the replica check on
  demand and a key lookup. One embedded page reading the handler's own JSON;
  loads nothing from elsewhere. Also `/stats`, `Stats` as JSON.
- `Cache.DescribeCluster` and the debug handler's `/cluster` route: every
  member's `Describe`, whether their member sets agree, and — with digests —
  the partitions whose replica copies are out of step with their owners. Both
  stores keep a running per-partition digest on every write, so the check
  costs the same whatever they hold. New peer method `Describe`. Decision 0039.
- `WithTracer` and `peercache.Tracer`: client and server spans around every
  peer request and spans around loads, children of the caller's span, with
  trace context carried in transport headers. `otel.Tracer(provider,
  propagator)` adapts OpenTelemetry. Spans never carry keys or values.
  Decision 0038.
- `WithRefreshAhead(window)`: a `GetOrLoad` hit on a key's owner with less than
  `window` of its lifetime left returns the current value and reloads the key
  once in the background, storing only if no write landed meanwhile; a failed
  reload keeps the value. `Stats().Refreshes`/`RefreshFailures`, two metric
  series (63). Decision 0036.
- `WithTTLJitter(fraction)`: the owner shortens every TTL it assigns by up to
  `fraction`, by an amount fixed per key, so keys written together do not
  expire together. Never lengthens; replicas and persistence carry the owner's
  deadline. Decision 0037.
- `WithCatchUpRate(entriesPerSecond)`: `CatchUp` waits after each page as long
  as its entries take at the rate, so a node joining at peak traffic does not
  have every peer serve it at full speed.
- `peercachetest`: an in-process cluster for an application's own tests — real
  caches on a simulated network, membership and fake clock, with `Kill`,
  `Restart`, `Partition`, `Heal`, `Advance`, `Settle` and `Owner`.
- `WithL1MaxBytes`: a namespace's non-owned tier is bounded in bytes, each
  copy charged its key, value and a fixed overhead, the same charge the L1
  bytes gauge and the memory budget use. Decision 0033.
- `WithNegativeCaching(ttl)`: a key's owner remembers that the namespace's
  loader returned `ErrNotFound` and answers `GetOrLoad` of it without loading
  until the TTL passes or the key is written. The absence is not a value:
  nothing but `GetOrLoad` sees it. `Stats().NegativeHits`,
  `peercache_negative_hits_total` and `Describe`'s `absences`. Decision 0034.
- `Namespace.BatchGetOrLoad` and `WithBatchLoader`: keys go to their owners one
  request per owner, and each owner loads every key it is missing in one batch
  loader call, or per key with the ordinary loader. Wire: `BatchGetRequest`
  field 4 `load` and `BatchGetResponse` field 2 `misrouted`, both readable by
  and from the previous release. Decision 0035.
- `WithCatchUpGate`: a node refuses its peers' operations on keys it owns, in
  namespaces with replicas, with `ErrActivationPending` until its first
  `CatchUp` completes. Readiness keeps clients away from a restarted node but
  not peers, whose forwarded writes landed on a base catch-up had not yet
  restored and were then discarded as older. Opt-in. Found by M16's kill -9
  on kind (open question 49).
- `github.com/arwenizEr/peercache/otel`, an OpenTelemetry bridge in its own
  module: one observable instrument per metric series, with the node as an
  attribute and counters named so the Prometheus exporter does not double their
  `_total`. Decision 0032.
- Conditional writes on one key, run on its owner under the store's lock:
  `Namespace.SetIfAbsent`, `CompareAndSwap`, `Increment` and `Touch`, and
  `SetIfAbsent` and `Touch` on `TypedNamespace[T]`. Decision 0027.
- `ErrNotAnInteger`, from `Increment`.
- `WithReplicaReads`: a namespace's replicas may answer reads of its keys,
  bounded by the L1 TTL plus the replication delay. New `ReplicaGet` RPC; a peer
  on an older build answers `ErrNotImplemented` and the read goes to the owner.
  Decision 0028.
- `Stats.CoalescedReads`, `ReplicaReadsServed`, `ReplicaReadsReferred` and
  `ReplicaReadsFloored`, and their four metric series.
- Opt-in persistence: `WithPersistence(persist.BlobStore)` writes a namespace
  behind to a blob store as immutable segments, restores keys whose every
  holder died, and compacts segments into snapshots. New package `persist`
  (the `BlobStore` interface, `Memory`, and the `persisttest` contract suite);
  eight `Persist*` stats and metric series. Design principle 4 changed to allow
  it. Decision 0029, superseding 0026.
- `persist.Dir`, a durable file-system backend: temporary file, fsync, rename,
  directory fsync; crash leftovers cleaned on open; names encoded so any
  namespace is a legal file name on every platform.
- `persist.WithFlushInterval`, and one object per flush: a flush writes a single
  log object, which a per-namespace splitter moves into partition segments
  (decision 0030). `PersistSplits` and `PersistSplitFailures`.
- `github.com/arwenizEr/peercache/persist/s3`, an S3 and S3-compatible backend
  in its own module, so the AWS SDK enters only programs that choose it.
- A key a persistent namespace restores from storage is sent to its replicas
  again, instead of sitting at a replication factor of one until its next
  write.
- `Close` cancels and waits for restores still in flight; one reading a store
  that no longer answered could outlive the cache and hold up its final flush.
- A persistent write allocates 437 bytes in 3 allocations instead of 705 in 4:
  the dirty map holds records by value and is recycled between flushes.
- `Cache.Describe`, `DescribeKey` and `DebugHandler`: what a node believes —
  its view, readiness, each namespace's options and counts, a key's placement —
  as Go values and as JSON, never including a value. Decision 0031.

### Changed

- The non-owned tier is bounded at 64 MiB per namespace and node by default,
  instead of 100,000 entries whatever their size: under the old bound a
  namespace of 1 MiB values could cache 100 GB of copies. A namespace of
  small values (key and value under about 540 bytes) now holds more copies than before,
  one of large values fewer. `WithL1MaxBytes` sets it.
- A synchronous write applied on its owner whose replication then fails now
  returns an error wrapping `ErrUnderReplicated` — written, held below its
  factor — and is counted in `DegradedWrites`, instead of an error that could
  not be told apart from a refusal and lost its identity across a forwarding
  hop. Found by M16's real-world run (open question 37, option 2).
- The result of a held write now survives forwarding too. A forwarded
  `Increment`, `SetIfAbsent`, `CompareAndSwap` or `Touch` that came back with
  `ErrUnderReplicated` returned a zero result — `0`, `false` — for a write that
  had happened. The owner now sends the result and the report in the response
  body. A peer on the previous release ignores the new field, and sees the
  write as an ordinary success, during a rolling upgrade. Found by M16.
- Tests live in `tests/` and `tests/repo/` instead of the root directory.
- Concurrent `Get` misses of one key owned elsewhere share one request to its
  owner instead of sending one each.
- A replicated write ranks its replicas without copying or cloning the
  membership: 4x faster at 32 nodes, with a third of the allocations.

### Removed

- `WithBindAddr`. It set a field nothing dialled. `Stats().Self` now comes from
  the membership authority, which carries the address peers actually use.
- `ErrStateEscaped`, which nothing returned. A `GroupState` used outside its
  turn is still caught and counted in `Stats().StateEscapes`.
- `EventEvict`, which nothing emitted.
- `PressureCheckInterval` from the public API; the interval is unchanged.

### Fixed

- A promoted key lived longer than its TTL by however long the promotion had
  taken to reach it: the remaining lifetime was taken against one reading of
  the clock and turned back into a deadline against a later one. Promotion now
  installs the owner's deadline as it is (`Store.RestoreAt`). Found when CI,
  re-enabled after three days off, failed `TestADrainedKeyKeepsItsRemainingTTL`
  on a 4-vCPU runner (locally 1 run in 40 at `-cpu 2`).

- A node that lost a key's ownership kept the value in its owned store, outside
  replication, hand-off and eviction; when ownership came back to it after the
  newer holders died it served the overwritten value. Reconciliation now
  demotes such keys to replica copies, which the existing rules keep, hand off
  or drop. Found by the dashboard's replica check on kind.

- Catch-up and hand-off cost O(n² log n) in the keys paged: every 512-entry
  page scanned and sorted every key above its cursor. A full pass over 400,000
  keys took 57 s and allocated 13.7 GB on the serving node, once per requester.
  A pass's sorted key list is now built once and kept for its duration
  (`internal/pager`, at most 16 passes, dropped after a minute idle), so a page
  costs its own size: 400,000 keys in 172 ms, a million in 0.7 s. No wire
  change; a requester on an earlier release pages the same way.

- A non-owner's L1 copy of a key with a TTL could outlive the key by up to
  the namespace's L1 TTL: the owner never filled the remaining lifetime its
  responses carry, so a session expired on its owner stayed readable through
  other nodes for up to five seconds. Owners now report it on Get, load, batch
  and replica reads, and the reader caches no longer than it. Found writing
  `peercachetest`.

- `SplitBrainDetections` counted handovers. It tested "two writers in one
  epoch", and epochs are per view, so after one rolling restart on kind it
  reported tens of thousands of splits that never happened. It now counts forks
  in a key's history: every replicated write carries the order of the value it
  replaced (`ReplicateRequest.Prev`, relayed by catch-up and hand-off), and a
  replica reports two writers neither of which replaced the other's write.
  Found by M16 (open question 50).
- A node restarted faster than failure detection — a SIGKILLed pod its kubelet
  restarts in a second or two — lost every key it owned, even at a synchronous
  replication factor of 3. Its peers never declared it dead, so nobody was
  promoted in its place, and `CatchUp` pulled only the keys its peers *own*.
  It now also pulls its peers' replica copies of the keys it owns. Found by
  M16's kill -9 on kind (open question 49).

- Replica copies are ordered by the hybrid logical clock, in every namespace,
  instead of by a stamp whose epoch dominated. Epochs are per view, so after a
  rolling restart a key's returning owner held a smaller epoch than the interim
  owner it had caught up from, its replicas refused its writes as older, and a
  synchronous write at RF 3 was lost once the owner and the interim owner
  failed. On kind, version conflicts after one rolling restart grew with each
  pod's epoch, to 1.1 million. Found by M16 (open question 50). A peer on an
  earlier release sends no order for a non-persistent namespace; its copies
  fall back to the stamp.

- An idle node spent about a quarter of a core reconciling its replicas: every
  second it ranked each one with an allocating call, and the garbage that made
  was most of the rest. On M16's kind cluster, at 160,000 replicas a node, that
  was 28% of a core with no traffic at all. Reconciliation and the catch-up
  sender now rank without allocating.

- A rebalance with more displaced replicas than the hand-off queue holds no
  longer cycles the excess through the replica store every second. Each round
  drained every orphan, queued 4,096 and put the rest back; after one rolling
  restart on kind, one pod put 10.5 million copies back to rehome 97,000, at a
  core and a quarter. A round now takes only what the queue has room for. Found
  by M16.

- A group already active on a node that then lost it — a peer joined and
  outranks it, as in every rolling restart — kept serving that node's `Get`,
  `Set`, `Delete`, `Keys` and `Items`, and any `Call` a lagging peer forwarded
  to it, while the new owner activated a second copy. Ownership was checked only
  at activation. It is now checked before every turn; a group that has moved
  answers `ErrNotOwner` and its activation is released, keeping its state.
  Found by M16.
- `docs/api.md` told a restarted node to call `CatchUp` "after the readiness
  gate", but `Ready()` stays false until `CatchUp` has run, so an application
  following it never became ready. The order is join, settle, `CatchUp`, ready.
  Found by M16's real-world run.
- Tests that passed only on a warm module cache or a fast machine: the
  external-module build, three idle-deactivation tests, and the remote latency
  gate on shared CI runners (`PEERCACHE_LATENCY_GATE=shared`).
- A synchronous write that could not reach a replica returned before sending
  its invalidation, leaving other pods on the old value until their L1 TTL.
  `Set`, `Delete` and the atomic operations now invalidate either way.
- `Group.Call` could return an internal "record removed" error when concurrent
  callers on a node that does not own the group were refused at once.

## [0.1.0] - 2026-11-12

The first release. Everything below is new.

### Cache and namespaces

- `New` with functional options; a single node needs only `WithNodeID`.
- Byte-valued namespaces (`Cache.Namespace`): `Get`, `Set`, `Delete`, `Exists`,
  `BatchGet`, `BatchSet`, `GetOrLoad`, with per-key TTL.
- Typed namespaces (`NamespaceOf[T]`), msgpack by default, with `WithCodec` to
  choose another codec and `WithTypedLoader` for typed loaders.
- Loaders run on the key's owner, collapsed so that concurrent misses on one key
  cause one load (`WithLoader`, `WithLoaderTimeout`).
- Opt-in LZ4 compression above a threshold (`WithCompression`).
- Partitioners that keep related keys on one owner (`PartitionByPrefix`,
  `PartitionByFunc`).

### Groups

- Groups as virtual actors: activated lazily on their owner, deactivated when
  idle, one goroutine and a bounded mailbox per activation.
- Functions registered by name (`RegisterGroupFunc`, `RegisterTypedGroupFunc`)
  and called on the owner from any node (`Group.Call`, `CallTyped`).
- Group timers and lifecycle events.
- `NewRouter`, which batches queue messages so a flush issues at most one
  request per owner.

### Cluster

- Ownership by rendezvous hashing over the live membership, with an activation
  delay and epoch checks around ownership changes.
- `membership`: gossip membership over memberlist.
- `transport`: gRPC transport with deadline propagation and optional mutual TLS.
- Replication factor 1 to 3 per namespace, asynchronous by default
  (`WithReplicationFactor`, `WithSyncReplication`,
  `WithRequireReplicationFactor`).
- Promotion of replicas when an owner fails, `CatchUp` for restarted nodes,
  replica hand-off on membership changes, and `Drain` for graceful shutdown.
- Readiness (`Ready`, `WaitReady`) that stays false until a node has caught up.
- A per-node memory budget that sheds L1, then replicas, then owned data
  (`WithMemoryBudget`), and join probation for crashlooping pods
  (`WithJoinProbation`).

### Operations

- Prometheus text-format metrics without a client library (`MetricsHandler`,
  `WriteMetrics`, `Metrics`), including correctness counters for split-brain
  detections, stale epochs, version conflicts, degraded and implicit-namespace
  writes.
- `Stats` for the same data as a struct.
- Kubernetes manifests (Deployment and StatefulSet) and an operational guide in
  `deploy/`.

[Unreleased]: https://github.com/arwenizEr/peercache/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/arwenizEr/peercache/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/arwenizEr/peercache/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/arwenizEr/peercache/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/arwenizEr/peercache/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/arwenizEr/peercache/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/arwenizEr/peercache/releases/tag/v0.1.0
