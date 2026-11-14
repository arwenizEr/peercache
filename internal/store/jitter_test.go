package store_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/store"
)

// TestJitterShortensByAnAmountFixedPerKey: every deadline lies in
// [ttl*(1-jitter), ttl], a key rewritten gets the same one, and different keys
// get different ones.
func TestJitterShortensByAnAmountFixedPerKey(t *testing.T) {
	s, fc := newStore(t, func(o *store.Options) { o.TTLJitter = 0.5 })
	const ttl = 100 * time.Second
	now := fc.Now().UnixNano()
	seen := map[int64]bool{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%d", i)
		w := s.Put(key, []byte("v"), ttl)
		got := time.Duration(w.ExpiresAt - now)
		if got < ttl/2 || got > ttl {
			t.Fatalf("%s lives %v of a %v TTL with jitter 0.5", key, got, ttl)
		}
		if again := s.Put(key, []byte("v2"), ttl); again.ExpiresAt != w.ExpiresAt {
			t.Fatalf("%s rewritten got deadline %d, then %d; the jitter must be fixed per key", key, w.ExpiresAt, again.ExpiresAt)
		}
		seen[w.ExpiresAt] = true
	}
	if len(seen) < 190 {
		t.Fatalf("200 keys got %d distinct deadlines; jitter is not spreading them", len(seen))
	}
}

// TestRestoreKeepsTheDeadlineItWasGiven: a promoted or caught-up copy carries
// the owner's deadline; jittering it again would shorten it on every move.
func TestRestoreKeepsTheDeadlineItWasGiven(t *testing.T) {
	s, fc := newStore(t, func(o *store.Options) { o.TTLJitter = 0.9 })
	s.Restore("k", []byte("v"), 100*time.Second, 1, 1)
	fc.Advance(99 * time.Second)
	if _, _, ok := s.Get("k"); !ok {
		t.Fatal("a restored key was jittered: it expired before the deadline it carried")
	}
}

// TestWithoutJitterTTLsAreExact is the default.
func TestWithoutJitterTTLsAreExact(t *testing.T) {
	s, fc := newStore(t)
	w := s.Put("k", []byte("v"), time.Minute)
	if got := time.Duration(w.ExpiresAt - fc.Now().UnixNano()); got != time.Minute {
		t.Fatalf("deadline %v, want exactly a minute", got)
	}
}
