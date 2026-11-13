package hlc_test

import (
	"testing"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/hlc"
)

// BenchmarkNext is the cost a persistent namespace adds to every write.
func BenchmarkNext(b *testing.B) {
	c := hlc.New(clock.Real())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Next()
	}
}

// BenchmarkNextParallel is every writer on a node contending for one clock.
func BenchmarkNextParallel(b *testing.B) {
	c := hlc.New(clock.Real())
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Next()
		}
	})
}
