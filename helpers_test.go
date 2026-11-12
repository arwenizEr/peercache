package peercache_test

import "strconv"

// key is the key benchmarks write: shaped like a real session key so its hash
// and length are representative. tests/namespace_test.go uses the same shape.
func key(i int) string { return "user:" + strconv.Itoa(i) + "/session" }

// shuffledIndices returns 0..n-1 in a fixed pseudo-random order, so a read
// benchmark does not walk the keyspace in insertion order. Deterministic: the
// same n gives the same order on every run. tests/acceptance_test.go has the
// same generator.
func shuffledIndices(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	state := uint64(0x9E3779B97F4A7C15)
	for i := n - 1; i > 0; i-- {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		j := int(state % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}
	return out
}
