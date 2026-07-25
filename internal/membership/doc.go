// Package membership answers who is in the cluster right now.
//
// It implements [core.MembershipAuthority]. The default implementation is
// gossip over hashicorp/memberlist and is AP: under a partition each side
// converges on a view containing only itself, and neither side blocks. Node
// metadata carries the epoch, so a peer's staleness is visible without an extra
// round trip.
//
// The consequence is stated plainly rather than hidden: a partition can produce
// two live views that each believe they own the same group. That window is
// narrowed by activation delay and detected through epochs. It is not
// eliminated, and no configuration setting will eliminate it (CLAUDE.md,
// decision 5).
//
// The interface exists so that a CP implementation can be substituted later
// without a rewrite. Nothing above this package may assume a single
// authoritative epoch.
//
// Milestone: M3. This package is empty until then. The test harness supplies
// its own membership implementation for M0; see internal/testkit.
package membership
