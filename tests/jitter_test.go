package tests

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// TestTTLJitterSpreadsAWarmUp writes a thousand keys in one instant with one
// TTL — half by Set, half by Increment — in a namespace with jitter 0.5. None
// may expire before half the TTL or outlive it, and they must not all expire
// together: about half are gone three quarters of the way through.
func TestTTLJitterSpreadsAWarmUp(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("warm", peercache.WithTTLJitter(0.5))
	ctx := context.Background()
	const n, ttl = 1000, 100 * time.Second
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		var err error
		if i%2 == 0 {
			err = ns.Set(ctx, key, []byte("v"), ttl)
		} else {
			_, err = ns.Increment(ctx, key, 1, ttl)
		}
		if err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	alive := func() (set, inc int) {
		for i := 0; i < n; i++ {
			if ok, _ := ns.Exists(ctx, fmt.Sprintf("k%d", i)); ok {
				if i%2 == 0 {
					set++
				} else {
					inc++
				}
			}
		}
		return
	}
	fc.Advance(ttl/2 - time.Nanosecond)
	if s, i := alive(); s+i != n {
		t.Fatalf("%d of %d keys expired before half their TTL", n-s-i, n)
	}
	fc.Advance(ttl / 4)
	s, i := alive()
	for name, got := range map[string]int{"Set": s, "Increment": i} {
		if math.Abs(float64(got)-n/4) > n/10 {
			t.Fatalf("%d of %d %s keys alive at 3/4 of the TTL, want about half: expiries are not spread", got, n/2, name)
		}
	}
	fc.Advance(ttl / 4)
	if s, i := alive(); s+i != 0 {
		t.Fatalf("%d keys outlived their TTL", s+i)
	}
}

// TestTTLJitterMustBeAFraction: jitter of 1 or more could make a TTL zero or
// negative.
func TestTTLJitterMustBeAFraction(t *testing.T) {
	c, _ := newCache(t)
	for i, j := range []float64{-0.1, 1, 1.5, math.NaN()} {
		ns := c.Namespace(fmt.Sprintf("bad%d", i), peercache.WithTTLJitter(j))
		if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
			t.Fatalf("WithTTLJitter(%v): Get = %v, want ErrInvalidOption", j, err)
		}
	}
}
