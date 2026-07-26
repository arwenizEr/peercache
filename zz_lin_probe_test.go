package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/testkit"
)

// Probe: same workload, but every client talks to the OWNER of the key.
// If this is linearizable and the unpinned version is not, the difference is
// non-owned L1 reads, which are documented as TTL-stale.
func TestZZProbePinnedToOwner(t *testing.T) {
	seed := testkit.Seed(t)
	f := newFabric(t, 3)
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("linp", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	const full = "hot/k0"
	owner := f.ownerOf(t, full)
	t.Logf("owner of %q is %s", full, owner)

	var lc linClock
	hist := &linHistory{}
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(client)))
			ns := openNS(owner) // pinned
			for i := 0; i < 30; i++ {
				if r.Intn(2) == 0 {
					val := fmt.Sprintf("c%d-%d", client, i)
					call := lc.now()
					err := ns.Set(context.Background(), full, []byte(val), 0)
					ret := lc.now()
					if retryable(err) || err != nil {
						continue
					}
					hist.add(porcupine.Operation{ClientId: client,
						Input: linInput{op: "set", key: 0, value: val},
						Call:  call, Output: linOutput{}, Return: ret})
					continue
				}
				call := lc.now()
				got, err := ns.Get(context.Background(), full)
				ret := lc.now()
				if retryable(err) {
					continue
				}
				out := linOutput{}
				if errors.Is(err, peercache.ErrNotFound) {
				} else if err != nil {
					continue
				} else {
					out.found, out.value = true, string(got)
				}
				hist.add(porcupine.Operation{ClientId: client,
					Input: linInput{op: "get", key: 0},
					Call:  call, Output: out, Return: ret})
			}
		}(c)
	}
	wg.Wait()
	ops := hist.sorted()
	t.Logf("PINNED-TO-OWNER: %d ops, linearizable=%v", len(ops),
		porcupine.CheckOperations(linModel, ops))
}
