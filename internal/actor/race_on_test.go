//go:build race

package actor_test

// raceEnabled reports whether the binary was built with the race detector.
//
// Memory measurements consult it: the detector allocates shadow memory for
// every access, so a heap figure taken under it describes the detector rather
// than the runtime. Correctness tests still run under -race; only the
// measurements are skipped.
const raceEnabled = true
