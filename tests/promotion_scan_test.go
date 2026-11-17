package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// TestAStrayTickDoesNotScanForPromotions: on a settled cluster, a tick that
// runs only to hand on an out-of-place copy does not rank every replica the
// node holds for ownership.
//
// It did. On kind, a pod left with 117,000 out-of-place copies after a rolling
// restart could hand off only a queue's worth a tick, so every tick for minutes
// was dirty, and every one ranked all 1.3 million replicas the pod held for
// ownership with the namespace's replica lock held: a third of a core, and a
// stall for every copy arriving meanwhile. Throughput past 3 million keys fell
// to a quarter. A replica becomes a node's to own only when ownership moves,
// and that is a view change, which scans.
func TestAStrayTickDoesNotScanForPromotions(t *testing.T) {
	f := quietFabric(t)
	ctx := context.Background()

	const key = "stray-scan"
	p, err := f.caches[f.ids[0]].DescribeKey("quiet", key)
	if err != nil || len(p.Holders) != 2 {
		t.Fatalf("DescribeKey: %v, %v", p.Holders, err)
	}
	owner := p.Holders[0]
	var stray string
	for _, id := range f.ids {
		if !slices.Contains(p.Holders, id) {
			stray = id
		}
	}

	sendStray(t, f, owner, stray, key, "handed on")
	held := f.caches[stray].Stats()

	f.clk.Advance(time.Second)
	f.settle(t)
	waitForCache(t, "the stray copy to leave "+stray, func() bool {
		return f.caches[stray].Stats().ReplicaKeys < held.ReplicaKeys
	})

	after := f.caches[stray].Stats()
	if after.PromotionRounds == held.PromotionRounds {
		t.Fatalf("%s handed the copy on without a reconciliation round; the test is not exercising a dirty tick", stray)
	}
	if n := after.PromotionScans - held.PromotionScans; n != 0 {
		t.Fatalf("%s ran %d promotion scans on a dirty tick under a settled view; "+
			"it ranked every replica it holds to hand on one copy", stray, n)
	}
	got, err := f.caches[owner].Namespace("quiet").Get(ctx, key)
	if err != nil || string(got) != "handed on" {
		t.Fatalf("the owner %s reads %q, %v", owner, got, err)
	}
}

// TestANamespaceOpenedLateOnAQuietClusterPromotesItsReplicas: copies filed for
// a namespace a node has not opened, on a settled cluster, are promoted when it
// opens the namespace, if they are of keys it owns.
//
// Ticks under a settled view skip the promotion scan, and nothing else moves
// these: the replicate handler promotes an owned copy as it lands only in a
// namespace that exists. So a namespace must scan once under each epoch,
// however the tick that reaches it came to run.
func TestANamespaceOpenedLateOnAQuietClusterPromotesItsReplicas(t *testing.T) {
	f := quietFabric(t)
	ctx := context.Background()

	const name, key = "late", "late-key"
	// Ownership is by key, whatever the namespace, and "late" is open nowhere
	// yet, so the placement is read from "quiet".
	p, err := f.caches[f.ids[0]].DescribeKey("quiet", key)
	if err != nil || len(p.Holders) == 0 {
		t.Fatalf("DescribeKey: %v, %v", p.Holders, err)
	}
	owner := p.Holders[0]
	var sender string
	for _, id := range f.ids {
		if id != owner {
			sender = id
			break
		}
	}

	// A copy of a key owner owns, in a namespace owner has never opened:
	// filed in its replica store and left there.
	sendCopy(t, f, sender, owner, name, key, "promoted late")
	if f.caches[owner].Stats().ReplicaKeys == 0 {
		t.Fatalf("%s did not file the copy", owner)
	}

	ns := f.caches[owner].Namespace(name, peercache.WithReplicationFactor(2))
	f.clk.Advance(time.Second)
	f.settle(t)
	waitForCache(t, "the late namespace's copy to be promoted on "+owner, func() bool {
		return f.caches[owner].Stats().KeysPromoted > 0
	})
	got, err := ns.Get(ctx, key)
	if err != nil || string(got) != "promoted late" {
		t.Fatalf("the owner %s reads %q, %v after opening %q; its replica was never promoted", owner, got, err, name)
	}
}

// sendCopy files a copy of key in namespace at to, as if from had replicated
// it there.
func sendCopy(t *testing.T, f *fabric, from, to, namespace, key, value string) {
	t.Helper()
	client, err := f.net.Transport(f.nodes[from]).Dial(f.nodes[to])
	if err != nil {
		t.Fatal(err)
	}
	req, err := wire.Encode(&wire.ReplicateRequest{
		Namespace: namespace,
		Key:       key,
		Value:     []byte(value),
		Stamp:     core.Stamp{Epoch: uint64(f.caches[from].Stats().Epoch), Version: 1},
		From:      from,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Invoke(context.Background(), transport.MethodReplicate, req); err != nil {
		t.Fatalf("Replicate to %s: %v", to, err)
	}
}
