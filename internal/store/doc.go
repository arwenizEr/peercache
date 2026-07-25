// Package store holds the data this node is responsible for.
//
// Two stores, with different rules:
//
//   - The owned store holds keys this node owns. It is sharded (256 shards by
//     default, xxhash over the key, one RWMutex per shard) so that unrelated
//     keys do not contend. Values are opaque bytes.
//   - The replica store holds copies of keys owned elsewhere, maintained by
//     internal/replication, and is never served as authoritative.
//
// Neither is durable. Memory only, by decision (CLAUDE.md, decision 4):
// graceful shutdown transfers state to the new owner, and ungraceful
// termination loses it. Every namespace must therefore either be
// reconstructible from a loader or tolerate loss.
//
// Milestone: M1. This package is empty until then.
package store
