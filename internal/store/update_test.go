package store_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/store"
)

// TestUpdateSeesTheLiveValueAndStoresTheMutation covers the three shapes every
// conditional operation is built from: absent, present, and expired, which is
// absent to every reader and must be absent here too.
func TestUpdateSeesTheLiveValueAndStoresTheMutation(t *testing.T) {
	s, fc := newStore(t)

	var sawOK bool
	res, err := s.Update("k", func(cur []byte, ok bool) (store.Mutation, bool, error) {
		sawOK = ok
		return store.Mutation{Value: []byte("v1"), TTL: time.Minute}, true, nil
	})
	if err != nil || !res.Wrote || sawOK {
		t.Fatalf("Update on an absent key = %+v, %v (saw ok=%v)", res, err, sawOK)
	}
	if got, _, ok := s.Get("k"); !ok || string(got) != "v1" {
		t.Fatalf("Get after Update = %q, %v", got, ok)
	}

	var saw string
	res2, err := s.Update("k", func(cur []byte, ok bool) (store.Mutation, bool, error) {
		saw = string(cur)
		return store.Mutation{Value: []byte("v2"), KeepDeadline: true}, true, nil
	})
	if err != nil || saw != "v1" || res2.Version != res.Version+1 {
		t.Fatalf("second Update saw %q, version %d after %d, err %v", saw, res2.Version, res.Version, err)
	}
	if res2.ExpiresAt != res.ExpiresAt || res.ExpiresAt == 0 {
		t.Fatalf("KeepDeadline moved the deadline from %d to %d", res.ExpiresAt, res2.ExpiresAt)
	}

	fc.Advance(2 * time.Minute)
	if _, err := s.Update("k", func(cur []byte, ok bool) (store.Mutation, bool, error) {
		if ok {
			t.Errorf("an expired entry was presented as live: %q", cur)
		}
		return store.Mutation{}, false, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestUpdateThatDeclinesOrFailsChangesNothing: a refusal leaves value and
// version exactly as they were, and an error from fn is returned as it is.
func TestUpdateThatDeclinesOrFailsChangesNothing(t *testing.T) {
	s, _ := newStore(t)
	v := s.Set("k", []byte("orig"), 0)

	res, err := s.Update("k", func([]byte, bool) (store.Mutation, bool, error) {
		return store.Mutation{Value: []byte("ignored")}, false, nil
	})
	if err != nil || res.Wrote {
		t.Fatalf("a declined Update = %+v, %v", res, err)
	}
	boom := errors.New("boom")
	if _, err := s.Update("k", func([]byte, bool) (store.Mutation, bool, error) {
		return store.Mutation{Value: []byte("ignored")}, true, boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Update returned %v, want fn's error", err)
	}
	got, ver, _ := s.Get("k")
	if string(got) != "orig" || ver != v {
		t.Fatalf("after a declined and a failed Update: %q at version %d, want %q at %d", got, ver, "orig", v)
	}
}

// TestUpdateIsAtomicUnderContention is the property every operation built on
// it depends on: read-modify-write with no lost update.
func TestUpdateIsAtomicUnderContention(t *testing.T) {
	s, _ := newStore(t)
	const workers, each = 16, 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := s.Update("counter", func(cur []byte, ok bool) (store.Mutation, bool, error) {
					n := 0
					if ok {
						n, _ = strconv.Atoi(string(cur))
					}
					return store.Mutation{Value: []byte(strconv.Itoa(n + 1))}, true, nil
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, _, _ := s.Get("counter")
	if string(got) != strconv.Itoa(workers*each) {
		t.Fatalf("counter = %s after %d increments", got, workers*each)
	}
}

// TestUpdateAfterDeleteStartsAboveTheFloor: a key recreated by Update follows
// Set's rule, so a replica still holding the deleted version cannot outrank it.
func TestUpdateAfterDeleteStartsAboveTheFloor(t *testing.T) {
	s, _ := newStore(t)
	s.Set("k", []byte("a"), 0)
	v := s.Set("k", []byte("b"), 0)
	s.Delete("k")
	res, err := s.Update("k", func([]byte, bool) (store.Mutation, bool, error) {
		return store.Mutation{Value: []byte("c")}, true, nil
	})
	if err != nil || res.Version <= v {
		t.Fatalf("recreated at version %d, want above the deleted %d (%v)", res.Version, v, err)
	}
}

// TestUpdateAccountsBytes keeps the memory budget honest: an Update that
// changes a value's size changes the store's accounted size by the same amount.
func TestUpdateAccountsBytes(t *testing.T) {
	s, _ := newStore(t)
	s.Set("k", []byte("12"), 0)
	before := s.Bytes()
	if _, err := s.Update("k", func([]byte, bool) (store.Mutation, bool, error) {
		return store.Mutation{Value: []byte("123456")}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := s.Bytes() - before; got != 4 {
		t.Fatalf("accounted size moved by %d for a value 4 bytes longer", got)
	}
}
