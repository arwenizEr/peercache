//go:build race

package peercache_test

// raceEnabled reports whether the binary was built with the race detector.
//
// Latency budgets cannot be measured under it. The detector intercepts every
// memory access and typically costs an order of magnitude, so a test that
// asserted a sub-microsecond p99 under -race would either fail honestly or,
// worse, be relaxed until it passed and stop meaning anything.
//
// Correctness tests still run under -race; only the latency gate is skipped.
const raceEnabled = true
