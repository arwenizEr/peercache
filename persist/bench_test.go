package persist_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache/persist"
)

// BenchmarkDirPut is one durable write of a segment-sized object: temporary
// file, fsync, rename, directory sync. It is the I/O term of the loss window,
// on top of waiting for the next flush.
func BenchmarkDirPut(b *testing.B) {
	for _, size := range []int{4 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			s, err := persist.Dir(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			data := make([]byte, size)
			ctx := context.Background()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Put(ctx, fmt.Sprintf("ns/p%03d/seg-%d.bin", i%256, i), data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDirPutParallel is BenchmarkDirPut from many goroutines at once: how
// far durable writes overlap on this file system, which is what a flush's
// parallel segment writes depend on.
func BenchmarkDirPutParallel(b *testing.B) {
	s, err := persist.Dir(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, 4<<10)
	ctx := context.Background()
	var n atomic.Int64
	b.SetParallelism(1) // GOMAXPROCS goroutines
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := n.Add(1)
			if err := s.Put(ctx, fmt.Sprintf("ns/p%03d/seg-%d.bin", i%256, i), data); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
