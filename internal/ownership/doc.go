// Package ownership decides which node owns a group key.
//
// The algorithm is rendezvous hashing — also called highest random weight — over
// the live member set, not a consistent hash ring. This is settled (CLAUDE.md,
// decision 1) and is not to be relitigated in code.
//
// # Why rendezvous rather than a ring
//
// Both give near-uniform distribution and both move roughly 1/N of the keyspace
// when one of N nodes leaves. Rendezvous gets there without virtual nodes:
// there is no ring to build, no replica count to tune, and no arbitrary
// constant deciding how lumpy the distribution is. The cost is that a lookup is
// O(N) rather than O(log N), which for a single Kubernetes cluster — tens of
// nodes, not thousands — is a handful of multiplications.
//
// The property that matters is not uniformity but *stability*: when a node
// leaves, only the keys it owned may move, and every other key must stay
// exactly where it was. A scheme that reshuffled unrelated keys would turn one
// pod restart into a cluster-wide cache miss. [Table] is tested against that
// directly.
//
// # Epochs
//
// Every answer carries the [core.Epoch] it was computed under. Membership is
// gossip based and therefore AP, so two nodes can hold different views at the
// same instant; comparing epochs establishes staleness and never consensus. An
// answer without its epoch is not usable for anything that outlives the call,
// which is why [Table.Owner] returns them together.
//
// # The activation guard
//
// [Guard] is the part that turns ownership into a decision. It refuses to
// activate a group this node does not own, and defers activating groups it has
// *just* acquired until a configurable delay has passed, so that the previous
// owner has time to notice the change. That window is narrowed, not eliminated
// — see docs/decisions/0012-the-activation-guard.md.
package ownership
