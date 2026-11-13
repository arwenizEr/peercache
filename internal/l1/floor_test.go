package l1_test

import (
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
)

// Floors exist for replica reads (docs/decisions/0028): an invalidation for
// stamp S must stop this node refilling the key from a replica that has not
// yet received S, without stopping it refilling from the owner.

func TestAFloorReplacesAnOlderCopyAndReadsAsAMiss(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("old"), core.Stamp{Version: 1}, time.Minute)

	if !c.Floor("k", core.Stamp{Version: 2}, time.Second) {
		t.Fatal("Floor over an older copy reported that it set nothing")
	}
	if v, _, ok := c.Get("k"); ok {
		t.Fatalf("Get after a floor = %q; a floor holds no value and must read as a miss", v)
	}
}

func TestAFloorIsNotSetWithoutACopy(t *testing.T) {
	c, _ := newCache(t)
	// A node that holds nothing for a key has not read it within a TTL, and
	// a floor for every invalidated key on every node would fill the tier
	// with markers for keys nobody here reads.
	if c.Floor("absent", core.Stamp{Version: 2}, time.Second) {
		t.Fatal("Floor of an absent key set one")
	}
	if c.Len() != 0 {
		t.Fatalf("tier holds %d entries after a no-op floor", c.Len())
	}
}

func TestAFloorDoesNotReplaceANewerCopy(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("new"), core.Stamp{Version: 5}, time.Minute)

	if c.Floor("k", core.Stamp{Version: 5}, time.Second) {
		t.Fatal("a floor at the resident stamp replaced the copy")
	}
	if c.Floor("k", core.Stamp{Version: 4}, time.Second) {
		t.Fatal("a floor below the resident stamp replaced the copy")
	}
	if v, _, ok := c.Get("k"); !ok || string(v) != "new" {
		t.Fatalf("the newer copy was lost: %q ok=%v", v, ok)
	}
}

func TestAFloorRefusesOlderPutsAndAdmitsItsOwnStamp(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("v1"), core.Stamp{Version: 1}, time.Minute)
	c.Floor("k", core.Stamp{Version: 3}, time.Minute)

	if c.Put("k", []byte("v2"), core.Stamp{Version: 2}, time.Minute) {
		t.Fatal("a Put older than the floor was admitted")
	}
	// The owner answers with exactly the stamp the invalidation carried. If
	// that were refused, every read would go to the owner until the floor
	// expired, which is the cost the floor exists to avoid.
	if !c.Put("k", []byte("v3"), core.Stamp{Version: 3}, time.Minute) {
		t.Fatal("a Put at the floor's stamp was refused")
	}
	if v, _, ok := c.Get("k"); !ok || string(v) != "v3" {
		t.Fatalf("Get = %q ok=%v, want v3", v, ok)
	}
}

func TestBelowReportsAnythingNewerResident(t *testing.T) {
	c, _ := newCache(t)
	if c.Below("absent", core.Stamp{Version: 1}) {
		t.Fatal("Below of an absent key reported true")
	}

	c.Put("k", []byte("v1"), core.Stamp{Version: 1}, time.Minute)
	c.Floor("k", core.Stamp{Version: 3}, time.Minute)
	if !c.Below("k", core.Stamp{Version: 2}) {
		t.Fatal("a stamp under a live floor was not reported below it")
	}
	if c.Below("k", core.Stamp{Version: 3}) {
		t.Fatal("the floor's own stamp was reported below it")
	}

	c.Put("v", []byte("v7"), core.Stamp{Version: 7}, time.Minute)
	if !c.Below("v", core.Stamp{Version: 6}) {
		t.Fatal("a stamp under a resident copy was not reported below it")
	}
}

func TestAFloorExpires(t *testing.T) {
	c, clk := newCache(t)
	c.Put("k", []byte("v1"), core.Stamp{Version: 1}, time.Minute)
	c.Floor("k", core.Stamp{Version: 3}, time.Second)

	clk.Advance(time.Second)
	if c.Below("k", core.Stamp{Version: 2}) {
		t.Fatal("an expired floor still refused an older stamp")
	}
	if !c.Put("k", []byte("v2"), core.Stamp{Version: 2}, time.Minute) {
		t.Fatal("an expired floor still refused a Put")
	}
}
