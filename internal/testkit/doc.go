// Package testkit is the in-process cluster harness.
//
// It exists so that every later milestone is provable without a container, a
// port, or a sleep. A test builds N nodes over a fake [Network], injects faults
// — drop, delay, reorder, partition, graceful kill, hard kill, revival — and
// drives the whole thing with a fake clock. An hour of simulated time costs
// microseconds, and a test that passes once passes every time.
//
// # Layering
//
// This package deliberately depends only on internal/core and internal/clock.
// It does not import the root peercache package. That keeps it usable from the
// root package's own tests without an import cycle, and it forces the harness
// to be written against the same interfaces the real implementations satisfy
// rather than against a concrete Cache.
//
// # What the membership implementation here is, and is not
//
// [Membership] is a reachability oracle, not a failure detector. It does not
// gossip, it does not send heartbeats, and it is not a preview of the M3
// implementation. It asks the [Network] whether a peer is reachable, and models
// detection latency with a single timer per observed peer, armed at the moment
// reachability changes.
//
// The reason is arithmetic. A heartbeat-based detector doing one round per
// interval performs (elapsed / interval) x N^2 message deliveries, so advancing
// an hour across five nodes at a one-second interval is seventy-two thousand
// goroutine spawns — which blows the harness's entire wall-clock budget on
// simulating silence. Event-driven detection makes an idle hour genuinely free,
// which is the property the M0 acceptance criterion is really asking for.
//
// When M3 lands, the real memberlist-backed authority runs on this same harness
// unchanged; it satisfies the same interface and consumes the same transport.
//
// # Determinism
//
// Randomness comes from [Seed], which prints the seed on failure and accepts
// PEERCACHE_SEED to reproduce. Probabilistic faults draw from that generator,
// so a sequential test is reproducible. Concurrent callers draw in whatever
// order they are scheduled, so a test that needs exact reproducibility under
// concurrency should use the deterministic faults — [Network.Partition],
// [Network.Isolate], a drop rate of 0 or 1 — rather than a fractional rate.
//
// # Lock ordering
//
// [Membership] may call into [Network]; the reverse never happens while the
// network holds its own lock, because change notifications are dispatched after
// unlocking. The order is therefore Membership then Network, and nothing may
// invert it.
package testkit
