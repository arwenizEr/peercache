//go:build !race

package tests

// raceEnabled reports whether the binary was built with the race detector.
// See race_on_test.go for why the latency gate consults it.
const raceEnabled = false
