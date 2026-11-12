// Package peercache is an embedded distributed cache and stateful aggregation
// engine for Go services deployed as N replicas.
//
// Every pod embeds the library; there is no separate cache tier. Each key
// belongs to exactly one pod, chosen by rendezvous hashing over the live
// membership. Reads hit local memory when the data is owned locally or recently
// cached, and route to the owner otherwise. Mutations and registered
// computations execute on the owner, so a working set is never shipped to the
// caller.
//
// # What it guarantees
//
// Read this before the API. The honest list is short:
//
//   - Operations on a single group are serialized while that group has one
//     activation.
//   - Membership is eventually consistent. A network partition can briefly
//     produce two activations of the same group; conflicting writes are
//     detected but not automatically merged.
//   - State is not durable. Ungraceful termination loses that pod's owned data
//     and any replicas not yet caught up.
//   - Reads of non-owned keys may be stale by up to the configured L1 TTL.
//   - Throughput degrades to single-pod capacity for a single hot group.
//
// The words "strong" and "quorum" do not appear in this API, deliberately.
//
// # Status
//
// Every feature is built. [ErrNotImplemented] does not mean "not written
// yet": it reports an RPC method this binary does
// not recognise — which is what a peer running a newer build looks like during a
// rolling upgrade — or a cache asked to reach another node with no transport
// configured.
package peercache
