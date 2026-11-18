package store_test

import (
	"testing"
	"time"
)

// TestPeekCountsNoRead: Peek is the node's own background look at an entry —
// a restored key on its way to its replicas — and must not show in the read
// counters an operator reads as traffic. It also hides an expired entry, as
// every read does.
func TestPeekCountsNoRead(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v"), time.Minute)
	before := s.Stats()

	val, version, expiresAt, _, ok := s.Peek("k")
	if !ok || string(val) != "v" || version == 0 || expiresAt == 0 {
		t.Fatalf("Peek(k) = %q, %d, %d, %v", val, version, expiresAt, ok)
	}
	if _, _, _, _, ok := s.Peek("absent"); ok {
		t.Fatal("Peek found a key never set")
	}
	if after := s.Stats(); after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatalf("Peek moved the read counters: hits %d -> %d, misses %d -> %d",
			before.Hits, after.Hits, before.Misses, after.Misses)
	}

	fc.Advance(2 * time.Minute)
	if _, _, _, _, ok := s.Peek("k"); ok {
		t.Fatal("Peek returned an expired entry")
	}
}
