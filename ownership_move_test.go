package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/testkit"
)

// The retry a data path makes when ownership moves under it — ask, be told
// ErrNotOwner, re-resolve, ask the new owner — had never run in a test: a
// coverage report showed the branch that follows a changed answer at zero.
// Reaching it needs a caller whose view is stale at the first attempt and fresh
// at the second, which ordinary gossip never arranges on purpose. These tests
// arrange it.

// frozenMembership passes the underlying view through until frozen, then serves
// the view it had at that moment until thawed.
type frozenMembership struct {
	core.MembershipAuthority

	mu       sync.Mutex
	frozen   bool
	members  []core.Node
	epoch    core.Epoch
	watchers []chan core.MembershipChange
	started  bool
}

func (m *frozenMembership) Members() ([]core.Node, core.Epoch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return m.members, m.epoch
	}
	return m.MembershipAuthority.Members()
}

func (m *frozenMembership) Watch() <-chan core.MembershipChange {
	ch := make(chan core.MembershipChange, 64)
	m.mu.Lock()
	m.watchers = append(m.watchers, ch)
	start := !m.started
	m.started = true
	m.mu.Unlock()
	if start {
		inner := m.MembershipAuthority.Watch()
		go func() {
			for change := range inner {
				m.mu.Lock()
				if !m.frozen {
					m.broadcastLocked(change)
				}
				m.mu.Unlock()
			}
		}()
	}
	return ch
}

func (m *frozenMembership) broadcastLocked(change core.MembershipChange) {
	for _, w := range m.watchers {
		select {
		case w <- change:
		default:
		}
	}
}

func (m *frozenMembership) freeze() {
	members, epoch := m.MembershipAuthority.Members()
	m.mu.Lock()
	m.frozen, m.members, m.epoch = true, members, epoch
	m.mu.Unlock()
}

func (m *frozenMembership) thaw() {
	m.mu.Lock()
	m.frozen = false
	m.broadcastLocked(core.MembershipChange{Kind: core.ChangeUpdate})
	m.mu.Unlock()
}

// thawingTransport thaws a membership the first time a call through it is
// answered ErrNotOwner, and waits for the cache to see the thawed view before
// returning the error — so the caller's retry runs against a fresh ranking.
type thawingTransport struct {
	core.Transport
	onNotOwner func()
	fired      atomic.Bool
}

// Ready forwards the wrapped transport's readiness. New waits on it before
// returning; a wrapper that hides it lets New return before the node is
// serving, the fabric then advances the clock past the join delay with this
// node's endpoint not yet registered, and the node is never admitted. That made
// these tests fail in most runs without -race and in none with it.
func (tr *thawingTransport) Ready() <-chan struct{} {
	if r, ok := tr.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	ready := make(chan struct{})
	close(ready)
	return ready
}

func (tr *thawingTransport) Dial(node core.Node) (core.PeerClient, error) {
	c, err := tr.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &thawingClient{PeerClient: c, tr: tr}, nil
}

type thawingClient struct {
	core.PeerClient
	tr *thawingTransport
}

func (c *thawingClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	out, err := c.PeerClient.Invoke(ctx, method, req)
	if errors.Is(err, core.ErrNotOwner) && c.tr.fired.CompareAndSwap(false, true) {
		c.tr.onNotOwner()
	}
	return out, err
}

// staleCaller builds four nodes, kills n3 and lets the rest converge on three,
// then freezes n0's view and revives n3. Afterwards n1 and n2 rank four nodes
// and n0 still ranks three; the first ErrNotOwner n0 receives thaws it.
func staleCaller(t *testing.T, register func(peercache.Cache)) (*fabric, *thawingTransport, func(string) peercache.Namespace) {
	t.Helper()
	frozen := &frozenMembership{}
	thawing := &thawingTransport{}
	f := newFabricFull(t, 4, false, testkit.MembershipConfig{}, -1,
		withWrappers(
			func(id string, tr core.Transport) core.Transport {
				if id != "n0" {
					return tr
				}
				thawing.Transport = tr
				return thawing
			},
			func(id string, m core.MembershipAuthority) core.MembershipAuthority {
				if id != "n0" {
					return m
				}
				frozen.MembershipAuthority = m
				return frozen
			}))
	thawing.onNotOwner = func() {
		frozen.thaw()
		waitForCache(t, "n0 to rank the revived node", func() bool {
			return f.caches["n0"].Stats().Members == 4
		})
	}

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("moved", peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
		register(f.caches[id])
	}
	f.settle(t)

	f.hardKill(t, "n3")
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 3)
	f.settle(t)

	frozen.freeze()
	f.revive(t, "n3")
	openNS("n3")
	register(f.caches["n3"])
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	waitForCache(t, "n1 and n2 to rank the revived node", func() bool {
		return f.caches["n1"].Stats().Members == 4 && f.caches["n2"].Stats().Members == 4
	})
	if got := f.caches["n0"].Stats().Members; got != 3 {
		t.Fatalf("n0 ranks %d members; the test needs it stale at 3", got)
	}
	return f, thawing, openNS
}

// keysMovedToN3 lists keys n3 owns in the four-node view.
//
// Some of them were n0's own in the three-node view n0 still holds, and for
// those n0 writes locally and never asks anyone — so a test tries them in turn
// until one is sent to a stale owner and comes back ErrNotOwner.
func keysMovedToN3(t *testing.T, f *fabric, prefix string) []string {
	t.Helper()
	var out []string
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%s/%d", prefix, i)
		if err := f.probeOwns("n3", key); err == nil {
			out = append(out, key)
		}
	}
	if len(out) == 0 {
		t.Fatal("no key among 500 moved to n3")
	}
	return out
}

// TestASetFollowsOwnershipThatMovedUnderIt: n0 sends a write to the owner its
// stale view names, is told ErrNotOwner, re-resolves against the view it now
// has, and delivers the write to the new owner. The caller sees success.
func TestASetFollowsOwnershipThatMovedUnderIt(t *testing.T) {
	f, thawing, openNS := staleCaller(t, func(peercache.Cache) {})

	key := ""
	for _, candidate := range keysMovedToN3(t, f, "moved") {
		if err := openNS("n0").Set(context.Background(), candidate, []byte("arrived"), 0); err != nil {
			t.Fatalf("Set %s from a stale caller: %v", candidate, err)
		}
		if thawing.fired.Load() {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("no write met ErrNotOwner, so the retry this test is about did not run")
	}
	got, err := openNS("n3").Get(context.Background(), key)
	if err != nil || string(got) != "arrived" {
		t.Fatalf("the new owner n3 holds %q, %v; the retried write did not reach it", got, err)
	}
}

// TestAGroupCallFollowsOwnershipThatMovedUnderIt is the same for Group.Call.
func TestAGroupCallFollowsOwnershipThatMovedUnderIt(t *testing.T) {
	register := func(c peercache.Cache) {
		_ = c.RegisterGroupFunc("whoami", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
			s.Set("seen", []byte("yes"))
			return []byte(s.ID()), nil
		})
	}
	f, thawing, _ := staleCaller(t, register)

	group := ""
	var out []byte
	for _, candidate := range keysMovedToN3(t, f, "group") {
		var err error
		out, err = f.caches["n0"].Group(candidate).Call(context.Background(), "whoami", nil)
		if err != nil {
			t.Fatalf("Call on %s from a stale caller: %v", candidate, err)
		}
		if thawing.fired.Load() {
			group = candidate
			break
		}
	}
	if group == "" {
		t.Fatal("no call met ErrNotOwner, so the retry this test is about did not run")
	}
	if string(out) != group {
		t.Fatalf("Call returned %q, want the group ID %q", out, group)
	}
	keys, err := f.caches["n3"].Group(group).Keys(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatalf("the new owner n3 holds keys %v, %v for %s; the retried call did not run there", keys, err, group)
	}
}
