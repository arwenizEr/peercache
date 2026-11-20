package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// stalledStore, while holding, holds every Put until released, then fails it: a store that does
// not answer for the length of a flush timeout and then times out.
type stalledStore struct {
	persist.BlobStore
	entered chan struct{}
	release chan struct{}
	holding atomic.Bool
}

var errStalled = errors.New("store did not answer")

func (s *stalledStore) Put(ctx context.Context, name string, data []byte) error {
	if !s.holding.Load() {
		return s.BlobStore.Put(ctx, name, data)
	}
	s.entered <- struct{}{}
	<-s.release
	return errStalled
}

// TestAFlushThatCannotWriteIsStillChargedToTheBudget: the records a flush is
// writing, and the segment it encoded from them, are memory until the write
// ends, and a failed write puts them back rather than copying them. The budget
// counted pending records only: the moment a flush took them it read zero, and
// stayed there for as long as the store did not answer. On kind, a pod cut off
// from storage by a partition was OOM-killed at 1 GiB with its budget reading
// 575 MB of 512, 426 MB of the heap in its flush.
func TestAFlushThatCannotWriteIsStillChargedToTheBudget(t *testing.T) {
	const writes = 2000
	store := &stalledStore{BlobStore: persist.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
	store.holding.Store(true)
	c, fc := newCache(t, peercache.WithMemoryBudget(1<<30))
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	ctx := context.Background()

	idle := c.Stats().MemoryUsed
	for i := 0; i < writes; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("o:%05d", i), make([]byte, 64), 0); err != nil {
			t.Fatal(err)
		}
	}
	written := c.Stats().MemoryUsed
	// What the store holds stays in it; what is pending is the part a
	// flush takes, and each record costs at least its value.
	owned := written - idle
	if owned < writes*64 {
		t.Fatalf("%d writes of 64 bytes raised the budget by %d", writes, owned)
	}

	fc.Advance(flushInterval)
	<-store.entered
	during := c.Stats().MemoryUsed
	if during < written {
		t.Errorf("budget read %d while a flush of %d records was writing, %d before it: "+
			"the batch and its segment were charged to nothing", during, writes, written)
	}

	store.release <- struct{}{} // the attempt fails; its records go back
	waitFor(t, "the failed flush", func() bool { return c.Stats().PersistFailures >= 1 })
	if got := c.Stats().PersistPending; got != writes {
		t.Fatalf("PersistPending = %d after the failed flush, want %d", got, writes)
	}
	after := c.Stats().MemoryUsed
	if after < written {
		t.Errorf("budget read %d after a failed flush put %d records back, %d before it", after, writes, written)
	}

	// The next attempt takes them again; let it fail too, and stop.
	fc.Advance(flushInterval)
	<-store.entered
	store.release <- struct{}{}
	waitFor(t, "the second failed flush", func() bool { return c.Stats().PersistFailures >= 2 })
	store.holding.Store(false) // storage is back; Close's flush writes
}
