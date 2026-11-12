package peercache_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// joinRecorder records what Join was asked to do and can be told to fail.
type joinRecorder struct {
	core.MembershipAuthority
	mu    sync.Mutex
	calls [][]string
	fail  error
}

func (j *joinRecorder) Join(seeds []string) error {
	j.mu.Lock()
	j.calls = append(j.calls, slices.Clone(seeds))
	fail := j.fail
	j.mu.Unlock()
	if fail != nil {
		return fail
	}
	return j.MembershipAuthority.Join(seeds)
}

func newSeedNode(t *testing.T, rec *joinRecorder, opts ...peercache.Option) (peercache.Cache, error) {
	t.Helper()
	clk := clock.NewFake(clock.Epoch)
	net := testkit.NewNetwork(clk, 1)
	t.Cleanup(func() { _ = net.Close() })
	info := core.Node{ID: "n0", Addr: "n0:7946"}
	rec.MembershipAuthority = testkit.NewMembership(info, net, clk, testkit.NewRegistry(), testkit.MembershipConfig{})
	return peercache.New(append([]peercache.Option{
		peercache.WithNodeID("n0"),
		peercache.WithClock(clk),
		peercache.WithMembership(rec),
		peercache.WithTransport(net.Transport(info)),
	}, opts...)...)
}

// TestWithSeedsJoinsThroughTheMembership: WithSeeds was documented as "the
// addresses used to join an existing cluster" and was read by nothing. New now
// hands them to the membership authority's Join, once.
func TestWithSeedsJoinsThroughTheMembership(t *testing.T) {
	rec := &joinRecorder{}
	c, err := newSeedNode(t, rec, peercache.WithSeeds("10.0.0.2:7946", "10.0.0.3:7946"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := [][]string{{"10.0.0.2:7946", "10.0.0.3:7946"}}
	if !slices.EqualFunc(rec.calls, want, slices.Equal[[]string]) {
		t.Fatalf("Join was called with %v, want %v", rec.calls, want)
	}
}

// TestNoSeedsMeansNoJoin keeps a node that is meant to wait to be joined from
// calling Join with nothing.
func TestNoSeedsMeansNoJoin(t *testing.T) {
	rec := &joinRecorder{}
	c, err := newSeedNode(t, rec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 0 {
		t.Fatalf("Join was called %d times with no seeds configured", len(rec.calls))
	}
}

// TestAFailedJoinFailsNew: a node told to join a cluster and unable to is not a
// working node, and returning one would leave it serving as a cluster of one.
func TestAFailedJoinFailsNew(t *testing.T) {
	errNoSeed := errors.New("no seed answered")
	rec := &joinRecorder{fail: errNoSeed}
	c, err := newSeedNode(t, rec, peercache.WithSeeds("10.0.0.2:7946"))
	if err == nil {
		_ = c.Close(context.Background())
		t.Fatal("New returned a cache although joining its seeds failed")
	}
	if !errors.Is(err, errNoSeed) {
		t.Fatalf("New returned %v; want the join failure wrapped", err)
	}
}

// TestSeedsWithoutMembershipAreRejected: seeds with no membership authority
// have nothing to join through, which is a configuration mistake.
func TestSeedsWithoutMembershipAreRejected(t *testing.T) {
	_, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithSeeds("10.0.0.2:7946"))
	if !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New with seeds and no membership returned %v, want ErrInvalidOption", err)
	}
}
