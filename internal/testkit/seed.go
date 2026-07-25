package testkit

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

// SeedEnv is the environment variable that pins the seed for a run.
const SeedEnv = "PEERCACHE_SEED"

// Seed returns the random seed for this test and arranges for it to be printed
// if the test fails.
//
// A randomised test that does not tell you how to reproduce its failure is
// worse than a deterministic one, because it converts a real bug into a shrug.
// The contract here is: every run is different, every failure is reproducible.
//
//	PEERCACHE_SEED=1234 go test ./... -run TestThatFailed
//
// The seed is drawn from crypto/rand rather than the clock, both because the
// clock is off limits outside internal/clock and because two tests starting in
// the same millisecond would otherwise share a seed.
func Seed(tb testing.TB) int64 {
	tb.Helper()

	var seed int64
	if raw, ok := os.LookupEnv(SeedEnv); ok {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			tb.Fatalf("%s=%q is not a valid int64: %v", SeedEnv, raw, err)
		}
		seed = parsed
	} else {
		var buf [8]byte
		if _, err := crand.Read(buf[:]); err != nil {
			tb.Fatalf("testkit: cannot read entropy for seed: %v", err)
		}
		seed = int64(binary.LittleEndian.Uint64(buf[:]) >> 1)
	}

	tb.Cleanup(func() {
		if tb.Failed() {
			tb.Logf("reproduce with %s=%d", SeedEnv, seed)
		}
	})
	return seed
}

// Rand returns a generator seeded by [Seed]. Use it for anything a test varies:
// key names, message order, which node to kill.
func Rand(tb testing.TB) *rand.Rand {
	tb.Helper()
	return rand.New(rand.NewSource(Seed(tb)))
}
