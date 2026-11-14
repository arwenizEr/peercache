package store_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/digest"
	"github.com/arwenizEr/peercache/internal/store"
)

// recompute builds the digest from what the store serves, the slow way.
func recompute(s *store.Store) [digest.Parts]digest.Pair {
	var d digest.Set
	after := ""
	for {
		page, more := s.Snapshot(after, 256, nil)
		for _, e := range page {
			d.Add(digest.KeyOf(e.Key), e.Version)
			after = e.Key
		}
		if !more || len(page) == 0 {
			return d.Snapshot()
		}
	}
}

// TestTheRunningDigestFollowsEveryWritePath drives the store through every
// path that changes what it holds — Put, SetIfAbsent, Restore, Remove, Update,
// PutIfVersion, MergeIfNewer, Shed, Sweep — in a random mix, and checks the
// running digest against one recomputed from scratch. A path that changed an
// entry without the digest seeing it would make the two differ.
func TestTheRunningDigestFollowsEveryWritePath(t *testing.T) {
	s, fc := newStore(t)
	rng := rand.New(rand.NewSource(1))
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(200)) }
	for i := 0; i < 20000; i++ {
		k := key()
		switch rng.Intn(10) {
		case 0:
			s.Put(k, []byte("v"), 0)
		case 1:
			s.Put(k, []byte("v"), time.Duration(1+rng.Intn(5))*time.Second)
		case 2:
			s.SetIfAbsent(k, []byte("v"), 0)
		case 3:
			s.Restore(k, []byte("r"), 0, uint64(rng.Intn(50)), uint64(rng.Intn(50)))
		case 4:
			s.Remove(k)
		case 5:
			_, _ = s.Update(k, func([]byte, bool) (store.Mutation, bool, error) {
				return store.Mutation{Value: []byte("u")}, true, nil
			})
		case 6:
			if _, v, ok := s.Get(k); ok {
				s.PutIfVersion(k, []byte("p"), 0, v)
			}
		case 7:
			s.MergeIfNewer(k, []byte("m"), 0, uint64(1<<40+i), rng.Intn(2) == 0)
		case 8:
			s.Shed(s.Bytes()/2, func(string, uint64) {})
		case 9:
			fc.Advance(time.Second)
			s.Sweep(s.Shards())
		}
	}
	// Expired entries are in the running digest until swept, and the slow
	// way skips them; sweep everything first.
	fc.Advance(time.Minute)
	s.Sweep(s.Shards())
	if got, want := s.Digest(), recompute(s); got != want {
		t.Fatal("the running digest differs from one recomputed from the store's entries")
	}
}
