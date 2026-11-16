package tests

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// heldReplicas is a transport that holds every Replicate request it sends
// until released, and records how many are in flight at once, overall and per
// key.
type heldReplicas struct {
	core.Transport

	mu      sync.Mutex
	gate    chan struct{}
	perKey  map[string]int
	keyPeak int

	inFlight atomic.Int64
	peak     atomic.Int64
}

func newHeldReplicas(tr core.Transport) *heldReplicas {
	return &heldReplicas{Transport: tr, perKey: map[string]int{}}
}

// hold makes every later Replicate wait until open.
func (h *heldReplicas) hold() {
	h.mu.Lock()
	h.gate = make(chan struct{})
	h.mu.Unlock()
}

// open releases every held Replicate and stops holding.
func (h *heldReplicas) open() {
	h.mu.Lock()
	if h.gate != nil {
		close(h.gate)
		h.gate = nil
	}
	h.mu.Unlock()
}

// Ready forwards the wrapped transport's readiness; see countingTransport.
func (h *heldReplicas) Ready() <-chan struct{} {
	if r, ok := h.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

func (h *heldReplicas) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := h.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &heldClient{PeerClient: inner, parent: h}, nil
}

type heldClient struct {
	core.PeerClient
	parent *heldReplicas
}

func (c *heldClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method != transport.MethodReplicate {
		return c.PeerClient.Invoke(ctx, method, req)
	}
	var r wire.ReplicateRequest
	if err := wire.Decode(req, &r); err != nil {
		return nil, err
	}
	h := c.parent
	key := r.Namespace + "/" + r.Key

	h.mu.Lock()
	gate := h.gate
	h.perKey[key]++
	if h.perKey[key] > h.keyPeak {
		h.keyPeak = h.perKey[key]
	}
	h.mu.Unlock()
	if n := h.inFlight.Add(1); n > h.peak.Load() {
		h.peak.Store(n)
	}
	defer func() {
		h.inFlight.Add(-1)
		h.mu.Lock()
		h.perKey[key]--
		h.mu.Unlock()
	}()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

func (h *heldReplicas) peakForOneKey() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keyPeak
}

// newHeldReplicaFabric is a three-node fabric whose first node's Replicate
// requests go through a heldReplicas, with an asynchronous RF=2 namespace
// "async" open everywhere.
func newHeldReplicaFabric(t *testing.T) (*fabric, *heldReplicas) {
	t.Helper()
	var held *heldReplicas
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1, withWrappers(
		func(id string, tr core.Transport) core.Transport {
			if id != "n0" {
				return tr
			}
			held = newHeldReplicas(tr)
			return held
		}, nil))
	for _, id := range f.ids {
		f.caches[id].Namespace("async", peercache.WithReplicationFactor(2))
	}
	return f, held
}

// TestAsyncFanOutRunsInParallel: asynchronous replication sends several keys'
// copies at once rather than one after another.
//
// The fan-out was one goroutine per node sending one request at a time, so a
// node's asynchronous replication rate was one over the round trip. On kind,
// at about 140,000 orders a minute, the 4,096-entry queue in front of it
// overflowed and dropped some 250,000 replica writes a minute: writes that
// reported success and were never copied. Here the writer's Replicate
// requests are held where they stand, so the ones in flight are counted.
func TestAsyncFanOutRunsInParallel(t *testing.T) {
	f, held := newHeldReplicaFabric(t)
	ctx := context.Background()
	writer := f.ids[0]
	ns := f.caches[writer].Namespace("async")

	held.hold()
	defer held.open()
	written := 0
	for i := 0; written < 64 && i < 5000; i++ {
		key := fmt.Sprintf("par/%d", i)
		if f.probeOwns(writer, key) != nil {
			continue
		}
		if err := ns.Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
		written++
	}
	if written < 64 {
		t.Fatalf("%s owns only %d keys; the test needs more", writer, written)
	}
	waitFor(t, "two keys' replica writes to be in flight at once", func() bool {
		return held.inFlight.Load() >= 2
	})
}

// TestAsyncFanOutKeepsOneKeysWritesInOrder: running the fan-out in parallel
// must not let two writes of one key travel at once, or the older could land
// last. A replica rejects an older copy and counts a version conflict, so the
// value would be right but the count would be noise, and a delete overtaken
// by the write before it would leave a stale copy until the next write.
func TestAsyncFanOutKeepsOneKeysWritesInOrder(t *testing.T) {
	f, held := newHeldReplicaFabric(t)
	ctx := context.Background()
	writer := f.ids[0]
	ns := f.caches[writer].Namespace("async")

	var key string
	for i := 0; i < 5000; i++ {
		if k := fmt.Sprintf("order/%d", i); f.probeOwns(writer, k) == nil {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatalf("%s owns no key", writer)
	}

	held.hold()
	const writes = 32
	for i := 0; i < writes; i++ {
		if err := ns.Set(ctx, key, []byte(fmt.Sprint(i)), 0); err != nil {
			t.Fatalf("Set #%d: %v", i, err)
		}
	}
	waitFor(t, "the first write's copy to be in flight", func() bool {
		return held.inFlight.Load() >= 1
	})
	// Give any other worker every chance to pick up a later write of the
	// same key while the first is held.
	for i := 0; i < 10_000; i++ {
		runtime.Gosched()
	}
	if n := held.peakForOneKey(); n != 1 {
		t.Fatalf("%d writes of %s were in flight at once; one key's copies must travel in order", n, key)
	}
	held.open()

	p, err := f.caches[writer].DescribeKey("async", key)
	if err != nil || len(p.Holders) != 2 {
		t.Fatalf("DescribeKey: %v, %v", p.Holders, err)
	}
	replica := p.Holders[1]
	want := fmt.Sprint(writes - 1)
	waitFor(t, "the replica to hold the last write", func() bool {
		for _, e := range catchUpPage(t, f, writer, replica, "async", 2, true) {
			if e.Key == key {
				return string(e.Value) == want
			}
		}
		return false
	})
	if c := f.caches[replica].Stats().VersionConflicts; c != 0 {
		t.Fatalf("the replica counted %d version conflicts; one key's copies arrived out of order", c)
	}
}
