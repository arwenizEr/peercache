//go:build !race

package actor_test

// raceEnabled reports whether the binary was built with the race detector.
// See race_on_test.go for why the memory measurements consult it.
const raceEnabled = false
