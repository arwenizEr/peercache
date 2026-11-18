package store_test

import (
	"fmt"
	"testing"
)

// BenchmarkSetDeleteChurn writes and deletes keys over a store of 100,000, the
// path that now charges and rebuilds spare slots.
func BenchmarkSetDeleteChurn(b *testing.B) {
	s, _ := newStore(b)
	const n = 100_000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("order:%016d", i)
		s.Set(keys[i], make([]byte, 48), 0)
	}
	val := make([]byte, 48)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%n]
		s.Delete(k)
		s.Set(k, val, 0)
	}
}

// BenchmarkOwnedShed sheds a tenth of 100,000 entries, the store refilled
// outside the timer.
func BenchmarkOwnedShed(b *testing.B) {
	const n = 100_000
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s, _ := newStore(b)
		for j := 0; j < n; j++ {
			s.Set(fmt.Sprintf("order:%016d", j), make([]byte, 48), 0)
		}
		target := s.Bytes() / 10
		b.StartTimer()
		s.Shed(target, nil)
	}
}
