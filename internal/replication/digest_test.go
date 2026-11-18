package replication_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/digest"
	"github.com/arwenizEr/peercache/internal/replication"
)

// TestTheReplicaDigestFollowsEveryWritePath is the replica store's half of
// the store test of the same idea: every path that changes the copies held —
// Put, Delete, DeleteUnlessNewer, Drain, DrainUpTo, Evict, EvictWhere, Shed —
// in a random mix, then the running digest against one recomputed from what
// the store serves.
func TestTheReplicaDigestFollowsEveryWritePath(t *testing.T) {
	s := replication.NewStore()
	rng := rand.New(rand.NewSource(1))
	const ns = "n"
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(200)) }
	var version uint64
	for i := 0; i < 20000; i++ {
		k := key()
		switch rng.Intn(9) {
		case 0, 1, 2:
			version++
			s.Put(ns, k, replication.Entry{Value: []byte("v"), Stamp: core.Stamp{Epoch: 1, Version: version}, From: "o", Order: version})
		case 3:
			s.Delete(ns, k)
		case 4:
			s.DeleteUnlessNewer(ns, k, core.Stamp{Epoch: 1, Version: version}, version)
		case 5:
			s.DrainUpTo(ns, 3, func(string) bool { return rng.Intn(2) == 0 })
		case 6:
			s.Evict(ns, func(string) bool { return rng.Intn(4) != 0 })
		case 7:
			s.EvictWhere(ns, func(string, replication.Entry) bool { return rng.Intn(4) != 0 })
		case 8:
			s.Shed(s.Bytes()/4, nil)
		}
	}
	var want digest.Set
	for _, k := range s.Keys(ns) {
		if e, ok := s.Get(ns, k); ok {
			want.Add(digest.KeyOf(k), e.Stamp.Version)
		}
	}
	if s.Digest(ns) != want.Snapshot() {
		t.Fatal("the replica digest differs from one recomputed from the copies held")
	}
}
