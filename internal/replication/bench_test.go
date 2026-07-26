package replication_test

import (
	"fmt"
	"testing"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/replication"
)

// The replica store sits on the write path of every replicated namespace: an
// owner's fan-out becomes a Put on each replica, so its cost is paid per key per
// replica. Promotion and eviction are paid per membership change instead, which
// is why they are allowed to be much more expensive.

func BenchmarkPut(b *testing.B) {
	s := replication.NewStore()
	value := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put("ns", "hot-key", replication.Entry{
			Value: value,
			Stamp: core.Stamp{Epoch: 1, Version: uint64(i + 1)},
			From:  "owner",
		})
	}
}

// BenchmarkPutDistinctKeys is the realistic shape: a fan-out touches many keys,
// not one. It also grows the map, which BenchmarkPut deliberately does not.
func BenchmarkPutDistinctKeys(b *testing.B) {
	s := replication.NewStore()
	value := make([]byte, 256)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put("ns", keys[i%len(keys)], replication.Entry{
			Value: value,
			Stamp: core.Stamp{Epoch: 1, Version: uint64(i/len(keys) + 1)},
			From:  "owner",
		})
	}
}

// BenchmarkPutRejected measures the path taken when an out-of-order copy
// arrives, which under asynchronous replication is not rare. It must be cheap:
// rejecting a stale copy should cost less than accepting a fresh one, or a
// reordering network would be more expensive than an orderly one.
func BenchmarkPutRejected(b *testing.B) {
	s := replication.NewStore()
	value := make([]byte, 256)
	s.Put("ns", "k", replication.Entry{
		Value: value,
		Stamp: core.Stamp{Epoch: 9, Version: 1000},
		From:  "owner",
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put("ns", "k", replication.Entry{
			Value: value,
			Stamp: core.Stamp{Epoch: 1, Version: 1},
			From:  "owner",
		})
	}
}

func BenchmarkGet(b *testing.B) {
	s := replication.NewStore()
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		s.Put("ns", keys[i], replication.Entry{
			Value: make([]byte, 256),
			Stamp: core.Stamp{Epoch: 1, Version: 1},
			From:  "owner",
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Get("ns", keys[i%len(keys)]); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkDrain is promotion: paid once per membership change, over every key
// this node holds as a replica. It is allowed to be expensive — a failure is
// rare and the alternative is a check on every read — but it must not be so
// expensive that a large replica set makes a node unresponsive while it
// reconciles.
func BenchmarkDrain(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := replication.NewStore()
				for k := 0; k < size; k++ {
					s.Put("ns", fmt.Sprintf("key-%d", k), replication.Entry{
						Value: make([]byte, 256),
						Stamp: core.Stamp{Epoch: 1, Version: 1},
						From:  "owner",
					})
				}
				b.StartTimer()

				// Half the keys promote, which is the worst case
				// for a drain: it must both collect and delete.
				s.Drain("ns", func(key string) bool {
					return len(key)%2 == 0
				})
			}
		})
	}
}

// BenchmarkEvict is the other half of reconciliation, on the same schedule.
func BenchmarkEvict(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := replication.NewStore()
				for k := 0; k < size; k++ {
					s.Put("ns", fmt.Sprintf("key-%d", k), replication.Entry{
						Value: make([]byte, 256),
						Stamp: core.Stamp{Epoch: 1, Version: 1},
						From:  "owner",
					})
				}
				b.StartTimer()

				s.Evict("ns", func(key string) bool {
					return len(key)%2 == 0
				})
			}
		})
	}
}

func BenchmarkStampComparison(b *testing.B) {
	older := core.Stamp{Epoch: 4, Version: 900}
	newer := core.Stamp{Epoch: 5, Version: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !newer.NewerThan(older) {
			b.Fatal("ordering is wrong")
		}
	}
}
