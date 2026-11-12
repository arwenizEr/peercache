# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until `v1.0.0`, a minor version may
change the public API.

## [Unreleased]

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
