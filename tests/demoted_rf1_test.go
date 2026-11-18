package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestADemotedOwnerAtReplicationFactorOneDoesNotResurrectAnOverwrittenValue is
// TestADemotedOwnerDoesNotResurrectAnOverwrittenValue for a namespace that
// replicates nothing. Such a namespace has no replicas on any node, and
// reconciliation used to visit only the namespaces this node held replicas
// of, so its owned store was never demoted: a node that lost a key kept it
// and, when ownership came back after the new owner died, served the value
// the new owner had overwritten (docs/decisions/open.md, question 59).
func TestADemotedOwnerAtReplicationFactorOneDoesNotResurrectAnOverwrittenValue(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		f.caches[id].Namespace("probe", peercache.WithReplicationFactor(2))
		return f.caches[id].Namespace("sessions")
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)

	const key = "session-1"
	p, err := f.caches[f.ids[0]].DescribeKey("probe", key)
	if err != nil {
		t.Fatalf("DescribeKey: %v", err)
	}
	n, x := p.Holders[0], p.Holders[1]

	// N away: X owns the key and writes v1.
	f.hardKill(t, n)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 1)
	f.settle(t)
	if err := open(x).Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("Set v1: %v", err)
	}

	// N returns and owns the key, and overwrites it.
	f.revive(t, n)
	open(n)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.convergeTo(t, 2)
	f.settle(t)
	if err := open(n).Set(ctx, key, []byte("v2"), 0); err != nil {
		t.Fatalf("Set v2: %v", err)
	}

	// N dies. Ownership returns to X.
	f.hardKill(t, n)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 1)
	f.settle(t)

	got, err := open(x).Get(ctx, key)
	if err == nil && string(got) == "v1" {
		t.Fatalf("%s served v1, a value overwritten by v2 on the owner that died: an undemoted owner kept it and brought it back", x)
	}
	if err != nil && !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
}
