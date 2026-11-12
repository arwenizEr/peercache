// Package membership answers who is in the cluster right now.
//
// It implements [core.MembershipAuthority] over hashicorp/memberlist, a SWIM
// gossip implementation. Node metadata carries the epoch, so a peer's staleness
// is visible without an extra round trip.
//
// # This is AP, and the consequence is stated rather than hidden
//
// Under a partition each side converges on a view containing only itself, and
// neither side blocks. Two live views can therefore each believe they own the
// same group. That window is narrowed by the activation delay in
// internal/ownership and detected through epochs. It is not eliminated, and no
// configuration setting will eliminate it.
//
// Nothing above this package may assume a single authoritative epoch. The
// interface exists so that a CP implementation can be substituted later without
// a rewrite, not because one is planned.
//
// # Testing
//
// Gossip converges in wall-clock time, driven by memberlist's own timers, which
// no injected clock reaches. So the tests here are integration tests over
// loopback and are skipped under -short; they poll rather than sleep, but they
// are honestly real-time.
//
// The logic that depends on membership — ownership, the activation guard — is
// tested deterministically against internal/testkit's in-process authority
// instead. That split is deliberate: the part with interesting behaviour gets
// exact tests, and the part that is mostly adaptation gets a slow one.
package membership
