# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until `v1.0.0`, a minor version may
change the public API.

## [Unreleased]

### Added

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

[Unreleased]: https://github.com/arwenizEr/peercache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/arwenizEr/peercache/releases/tag/v0.1.0
