package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// writeOwned writes n keys, each on its own owner, skipping keys skip owns.
func writeOwned(t *testing.T, f *fabric, ns string, n int, skip string) {
	t.Helper()
	for i, written := 0, 0; written < n; i++ {
		key := fmt.Sprintf("row/%04d", i)
		owner := f.ownerOf(t, key)
		if owner == skip {
			continue
		}
		if err := f.caches[owner].Namespace(ns).Set(context.Background(), key, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		written++
	}
}

// TestInStepReplicasMatchTheirOwners: three nodes, RF 2, synchronous — no
// partition is out of step, and every node agrees on the members.
func TestInStepReplicasMatchTheirOwners(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		f.caches[id].Namespace("rows", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	f.settle(t)
	writeOwned(t, f, "rows", 300, "")

	d := f.caches[f.ids[0]].DescribeCluster(context.Background(), true)
	if !d.MembersAgree || len(d.Nodes) != 3 {
		t.Fatalf("membersAgree=%v over %d nodes: %v", d.MembersAgree, len(d.Nodes), d.Disagreements)
	}
	if !d.Digests {
		t.Fatal("digests were asked for and every member answered, but none were compared")
	}
	if len(d.OutOfStep) != 0 {
		t.Fatalf("in-step replicas reported out of step: %+v", d.OutOfStep)
	}
}

// TestAReplicaThatMissedWritesIsReported: one node is cut off while keys it
// replicates are written asynchronously on their owners; the partitions
// holding them are out of step, short by the copies it missed.
func TestAReplicaThatMissedWritesIsReported(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		f.caches[id].Namespace("rows", peercache.WithReplicationFactor(2))
	}
	f.settle(t)
	victim := f.ids[2]
	f.net.Isolate(victim)
	writeOwned(t, f, "rows", 300, victim)
	f.settle(t)
	f.net.Rejoin(victim)

	d := f.caches[f.ids[0]].DescribeCluster(context.Background(), true)
	if len(d.OutOfStep) == 0 {
		t.Fatal("a replica that missed writes is not reported")
	}
	var missing uint64
	for _, pc := range d.OutOfStep {
		if pc.HeldCopies >= pc.WantCopies {
			t.Fatalf("an out-of-step partition is not short: %+v", pc)
		}
		missing += pc.WantCopies - pc.HeldCopies
	}
	if missing == 0 {
		t.Fatalf("no copies reported missing: %+v", d.OutOfStep)
	}
}

// TestAnUnreachableMemberIsReported: a member that cannot be reached is
// reported with its error, the cluster is not reported as agreeing, and no
// digest conclusion is drawn without it.
func TestAnUnreachableMemberIsReported(t *testing.T) {
	f := newFabric(t, 3)
	gone := f.ids[2]
	f.net.Isolate(gone)
	d := f.caches[f.ids[0]].DescribeCluster(context.Background(), true)
	if d.MembersAgree || d.Digests {
		t.Fatalf("membersAgree=%v digests=%v with a member unreachable", d.MembersAgree, d.Digests)
	}
	for _, n := range d.Nodes {
		if n.ID == gone && (n.Error == "" || n.Description != nil) {
			t.Fatalf("the unreachable member is reported as %+v", n)
		}
		if n.ID != gone && n.Description == nil {
			t.Fatalf("a reachable member has no description: %+v", n)
		}
	}
}

// TestAMemberThatNeverAnswersCostsOnlyTheTimeout: a request that is lost, not
// refused, ends at DescribeTimeout on the cache's clock instead of holding the
// caller forever.
func TestAMemberThatNeverAnswersCostsOnlyTheTimeout(t *testing.T) {
	f := newFabric(t, 3)
	silent := f.ids[2]
	f.net.SetDropRate(f.ids[0], silent, 1)
	done := make(chan peercache.ClusterDescription, 1)
	go func() { done <- f.caches[f.ids[0]].DescribeCluster(context.Background(), false) }()
	var d peercache.ClusterDescription
	advanceUntilRecv(t, f, done, &d)
	for _, n := range d.Nodes {
		if n.ID == silent && n.Error == "" {
			t.Fatalf("the silent member is reported as %+v", n)
		}
	}
}

// TestTheClusterViewNamesNoKey: the whole report, as the debug handler serves
// it, contains no key and no value.
func TestTheClusterViewNamesNoKey(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		f.caches[id].Namespace("rows", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	f.settle(t)
	if err := f.caches[f.ids[0]].Namespace("rows").Set(context.Background(), "secret-key-7", []byte("secret-value"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	body, err := json.Marshal(f.caches[f.ids[1]].DescribeCluster(context.Background(), true))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, s := range []string{"secret-key-7", "secret-value"} {
		if strings.Contains(string(body), s) {
			t.Fatalf("the cluster view contains %q: %s", s, body)
		}
	}
}

// TestLoadedKeysAreMarkedNotMissing: loaded values live on their owner only,
// so a namespace with a loader holds fewer copies than owned keys times
// copies-1. Its partitions are reported as Loaded, so the shortfall reads as
// expected, while a namespace without a loader carries no such mark.
func TestLoadedKeysAreMarkedNotMissing(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		f.caches[id].Namespace("profiles", peercache.WithReplicationFactor(2),
			peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) { return []byte("p"), 0, nil }))
	}
	f.settle(t)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("u:%d", i)
		if _, err := f.caches[f.ownerOf(t, key)].Namespace("profiles").GetOrLoad(context.Background(), key); err != nil {
			t.Fatalf("GetOrLoad: %v", err)
		}
	}
	d := f.caches[f.ids[0]].DescribeCluster(context.Background(), true)
	if len(d.OutOfStep) == 0 {
		t.Fatal("loaded keys have no replicas, yet nothing was reported short")
	}
	for _, pc := range d.OutOfStep {
		if !pc.Loaded || pc.HeldCopies > pc.WantCopies {
			t.Fatalf("a partition short only by loaded keys is reported as %+v", pc)
		}
	}
}
