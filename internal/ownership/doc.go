// Package ownership decides which node owns a group key.
//
// The algorithm is rendezvous (highest random weight) hashing over the live
// member set, not a consistent hash ring. This is settled (CLAUDE.md, decision
// 1) and is not to be relitigated in code. Rendezvous gives near-uniform
// distribution without virtual nodes, and removing one node of N moves
// approximately 1/N of the keyspace and nothing else.
//
// The sorted node list is cached and recomputed only on membership change,
// because the hot path is a lookup, not a rebuild.
//
// Every answer is stamped with the [core.Epoch] it was computed under, so that
// a caller acting on it later can tell whether the world moved underneath.
//
// Milestone: M3. This package is empty until then.
package ownership
