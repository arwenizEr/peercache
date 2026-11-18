package replication_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/replication"
)

// fillSweep files n replicas in namespace "ns", every expiredEvery-th of them
// already past its deadline at now (none when expiredEvery is 0), and returns
// how many are expired.
func fillSweep(s *replication.Store, n, expiredEvery int, now int64) int {
	expired := 0
	for i := 0; i < n; i++ {
		e := replication.Entry{
			Value:     []byte("v"),
			Stamp:     core.Stamp{Epoch: 1, Version: 1},
			From:      "owner",
			ExpiresAt: now + int64(1e12),
		}
		if expiredEvery > 0 && i%expiredEvery == 0 {
			e.ExpiresAt = now - 1
			expired++
		}
		s.Put("ns", fmt.Sprintf("k%d", i), e)
	}
	return expired
}

// TestASweepOfALargeNamespaceVisitsAShareOfIt: a sweep of a namespace with
// nothing expired visits about a thirty-second of it, not all of it.
//
// It visited every replica every second with the namespace's lock held. On
// kind, at 1.35 million replicas a pod, that was a quarter of the pod's CPU,
// and every copy arriving meanwhile waited on the lock.
func TestASweepOfALargeNamespaceVisitsAShareOfIt(t *testing.T) {
	const n = 64 * replication.SweepChunk
	s := replication.NewStore()
	const now = int64(1e15)
	fillSweep(s, n, 0, now)

	removed, visited := s.SweepExpired(now)
	if removed != 0 {
		t.Fatalf("removed %d live replicas", removed)
	}
	share := n / replication.SweepShare
	if visited < share || visited > share+replication.SweepChunk {
		t.Fatalf("a sweep of %d replicas, none expired, visited %d; want its share, %d, to within a chunk",
			n, visited, share)
	}
}

// TestASweepGoesOnWhileItFindsExpiredReplicas: when most of what a sweep
// visits has expired it goes on past its share, up to a quarter of the
// namespace, so a burst of expiries is reclaimed in a few sweeps rather than
// thirty.
func TestASweepGoesOnWhileItFindsExpiredReplicas(t *testing.T) {
	const n = 64 * replication.SweepChunk
	s := replication.NewStore()
	const now = int64(1e15)
	fillSweep(s, n, 1, now) // every one expired

	removed, visited := s.SweepExpired(now)
	if most := n / 4; visited != most || removed != most {
		t.Fatalf("a sweep of %d replicas, all expired, removed %d and visited %d; want a quarter, %d, of each",
			n, removed, visited, most)
	}
}

// TestRepeatedSweepsReclaimEveryExpiredReplica: a sample per sweep still
// finds every expired replica, in a few dozen sweeps.
func TestRepeatedSweepsReclaimEveryExpiredReplica(t *testing.T) {
	const n = 64 * replication.SweepChunk
	s := replication.NewStore()
	const now = int64(1e15)
	expired := fillSweep(s, n, 10, now)

	total := 0
	for i := 0; i < 400 && total < expired; i++ {
		removed, _ := s.SweepExpired(now)
		total += removed
	}
	if total != expired {
		t.Fatalf("400 sweeps reclaimed %d of %d expired replicas", total, expired)
	}
	if got := s.Len(); got != n-expired {
		t.Fatalf("%d replicas held after the sweeps, want the %d live ones", got, n-expired)
	}
}

// TestASmallNamespaceIsSweptWhole: under SweepWholeBelow, one sweep reclaims
// everything expired, as every sweep once did.
func TestASmallNamespaceIsSweptWhole(t *testing.T) {
	const n = replication.SweepWholeBelow - 1
	s := replication.NewStore()
	const now = int64(1e15)
	expired := fillSweep(s, n, 3, now)

	if removed, visited := s.SweepExpired(now); removed != expired || visited != n {
		t.Fatalf("one sweep of %d replicas removed %d of %d expired, visiting %d", n, removed, expired, visited)
	}
}

// BenchmarkSweepExpired is one sweep of a namespace of a million replicas with
// nothing expired, the steady state of a pod on kind past 3 million keys. It
// is the time a sweep spends; with the sample, most of it is spent with the
// lock released between chunks.
func BenchmarkSweepExpired(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("replicas=%d", n), func(b *testing.B) {
			s := replication.NewStore()
			const now = int64(1e15)
			fillSweep(s, n, 0, now)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.SweepExpired(now)
			}
		})
	}
}
