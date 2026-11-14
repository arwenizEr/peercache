//go:build race

package segment_test

// raceEnabled reports whether the binary was built with the race detector,
// which allocates on its own behalf and makes allocation counts unreliable.
const raceEnabled = true
