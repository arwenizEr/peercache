# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until `v1.0.0`, a minor version may
change the public API.

## [Unreleased]

### Added

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
