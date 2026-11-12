package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
	"github.com/peercache/peercache/transport"
)

// These tests exist because the M7 mutation audit found that they did not.
//
// The audit script had a truncated entry — an orphaned argument list with no
// `mutate` command in front of it — which in a script running under `set -u`
// but not `set -e` is a command that fails with "not found" and lets the run
// continue. So the M6 acceptance criterion appeared in the audit source and was
// never actually audited.
//
// When the missing mutation was reconstructed and run, two things that looked
// covered were not:
//
//   - Deleting the handoff from [peercache.Cache.Drain] entirely left
//     TestRollingUpgradeLosesNoRequests passing. That test runs at replication
//     factor 2 with synchronous replication, so the replicas already hold every
//     key and losing the transfer costs nothing it measures. It proves
//     replication works. It says nothing about Drain.
//
//   - Reversing steps 2 and 3 — transferring before announcing departure, which
//     the thirty-line comment at the top of drain.go calls "the whole design" —
//     also left it passing, for the same reason.
//
// The fix is these two tests, at replication factor 1, where nothing else can
// stand in for the handoff.

const drainKeys = 300

func drainDetection() testkit.MembershipConfig {
	return testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}
}

// TestDrainHandsOffKeysNothingElseWouldSave is the test the audit was missing.
//
// Replication factor 1 is the point. With replicas, a key survives a departure
// whether or not Drain transfers anything, and every assertion about the
// transfer is really an assertion about replication. With one copy, the handoff
// is the only thing standing between a drained node and data loss.
func TestDrainHandsOffKeysNothingElseWouldSave(t *testing.T) {
	f := newFabricFull(t, 3, false, drainDetection(), ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("drain", peercache.WithReplicationFactor(1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	written := make(map[string]string, drainKeys)
	for i := 0; i < drainKeys; i++ {
		key := fmt.Sprintf("drain/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := writeWithRetry(t, f, openNS, key, val); err != nil {
			t.Fatalf("seeding %q: %v", key, err)
		}
		written[key] = val
	}

	// Everything is readable before the drain, so a failure afterwards is
	// attributable to the drain and not to a seeding bug.
	survivor := f.ids[1]
	for key, want := range written {
		got, err := readWithRetryFrom(t, f, openNS, survivor, key)
		if err != nil {
			t.Fatalf("before drain, %q: %v", key, err)
		}
		if got != want {
			t.Fatalf("before drain, %q = %q, want %q", key, got, want)
		}
	}

	victim := f.ids[0]
	ownedByVictim := f.caches[victim].Stats().OwnedKeys
	if ownedByVictim == 0 {
		t.Fatalf("the drained node owns nothing; the test would pass vacuously")
	}

	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain(%s): %v", victim, err)
	}

	// The survivors must agree the victim is gone before its keys can be
	// read from their new owners.
	waitForMembers(t, f, []string{f.ids[1], f.ids[2]}, 2)
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	var lost []string
	for key, want := range written {
		got, err := readWithRetryFrom(t, f, openNS, survivor, key)
		if err != nil || got != want {
			lost = append(lost, key)
		}
	}

	if len(lost) > 0 {
		t.Fatalf("%d of %d keys did not survive a graceful drain at replication "+
			"factor 1 (the drained node owned %d of them). Drain's handoff is the "+
			"only thing that saves these; example: %q",
			len(lost), len(written), ownedByVictim, lost[0])
	}
}

// TestDrainAnnouncesDepartureBeforeTransferring pins the step order that
// drain.go's comment calls the whole design.
//
// If the transfer runs first, it computes each key's new owner from a ranking
// that still contains the departing node — so the "new owner" is frequently the
// departing node itself, and the transfer is a no-op that looks like a success.
// The data is then lost when the process exits, with nothing having reported an
// error.
//
// This is checkable only by watching the sequence of two calls to two different
// collaborators, which is why the fabric grew wrappers.
func TestDrainAnnouncesDepartureBeforeTransferring(t *testing.T) {
	rec := &drainRecorder{
		leaveAt:     make(map[string]int),
		replicateAt: make(map[string]int),
	}

	f := newFabricFull(t, 3, false, drainDetection(), ownership.DefaultActivationDelay,
		withWrappers(
			func(id string, tr core.Transport) core.Transport {
				return &recordingTransport{Transport: tr, id: id, rec: rec}
			},
			func(id string, m core.MembershipAuthority) core.MembershipAuthority {
				return &recordingMembership{MembershipAuthority: m, id: id, rec: rec}
			},
		))
	ctx := context.Background()

	// Replication factor 1 again, for a second reason: it means the only
	// Replicate calls this cluster ever makes are the handoff's. At a higher
	// factor the ordinary write path would fire them too and the recorder
	// could not tell the two apart.
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("order", peercache.WithReplicationFactor(1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	for i := 0; i < drainKeys; i++ {
		if err := writeWithRetry(t, f, openNS,
			fmt.Sprintf("order/%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}

	victim := f.ids[0]
	if n := rec.replicateSeq(victim); n != 0 {
		t.Fatalf("the victim replicated before draining (at %d); replication "+
			"factor 1 should produce no Replicate calls at all, so this test "+
			"can no longer distinguish the handoff from ordinary writes", n)
	}

	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain(%s): %v", victim, err)
	}

	leave := rec.leaveSeq(victim)
	replicate := rec.replicateSeq(victim)

	if leave == 0 {
		t.Fatal("Drain did not announce departure at all")
	}
	if replicate == 0 {
		t.Fatal("Drain announced departure but transferred nothing; at replication " +
			"factor 1 the handoff is the only copy of those keys")
	}
	if leave > replicate {
		t.Fatalf("Drain transferred (call %d) before announcing departure (call %d). "+
			"The transfer then picks new owners from a ranking that still contains "+
			"this node, so keys are handed to the node that is leaving and lost on "+
			"exit — with every step reporting success", replicate, leave)
	}
}

// drainRecorder records the order of the collaborator calls Drain makes.
//
// One monotonic counter shared by both collaborators, because the question is
// which happened first and separate counters could not answer it.
type drainRecorder struct {
	mu          sync.Mutex
	seq         int
	leaveAt     map[string]int
	replicateAt map[string]int // first Replicate only
}

func (r *drainRecorder) recordLeave(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	if _, seen := r.leaveAt[id]; !seen {
		r.leaveAt[id] = r.seq
	}
}

func (r *drainRecorder) recordReplicate(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	if _, seen := r.replicateAt[id]; !seen {
		r.replicateAt[id] = r.seq
	}
}

func (r *drainRecorder) leaveSeq(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaveAt[id]
}

func (r *drainRecorder) replicateSeq(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.replicateAt[id]
}

type recordingMembership struct {
	core.MembershipAuthority
	id  string
	rec *drainRecorder
}

func (m *recordingMembership) Leave(ctx context.Context) error {
	m.rec.recordLeave(m.id)
	return m.MembershipAuthority.Leave(ctx)
}

type recordingTransport struct {
	core.Transport
	id  string
	rec *drainRecorder
}

func (t *recordingTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &recordingClient{PeerClient: inner, id: t.id, rec: t.rec}, nil
}

// Ready forwards the wrapped transport's readiness signal.
//
// Without this the wrapper silently opts its node out of the startup wait and
// the cluster never converges. countingTransport carries the same method for
// the same reason; the comment is repeated rather than referenced because the
// failure it prevents is a hang with no message.
func (t *recordingTransport) Ready() <-chan struct{} {
	if r, ok := t.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

type recordingClient struct {
	core.PeerClient
	id  string
	rec *drainRecorder
}

func (c *recordingClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method == transport.MethodReplicate {
		c.rec.recordReplicate(c.id)
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

// waitForMembers blocks until every listed node sees want members.
func waitForMembers(t *testing.T, f *fabric, ids []string, want int) {
	t.Helper()
	const maxYields = 10_000_000
	for i := 0; ; i++ {
		agreed := true
		for _, id := range ids {
			if f.caches[id].Stats().Members != want {
				agreed = false
				break
			}
		}
		if agreed {
			return
		}
		if i > maxYields {
			var seen []string
			for _, id := range ids {
				seen = append(seen, fmt.Sprintf("%s=%d", id, f.caches[id].Stats().Members))
			}
			t.Fatalf("membership did not converge to %d: %v", want, seen)
		}
		runtime.Gosched()
	}
}

// readWithRetryFrom reads a key from one node, retrying the errors that mean
// "ask again" rather than "it is gone".
//
// Distinguishing the two is the whole point: a test that retried every error
// would turn a lost key into a timeout, and a test that retried none would turn
// an ordinary ownership handover into a failure.
func readWithRetryFrom(t *testing.T, f *fabric, ns func(string) peercache.Namespace,
	id, key string) (string, error) {
	t.Helper()
	const maxYields = 2_000_000
	var last error
	for i := 0; i < maxYields; i++ {
		val, err := ns(id).Get(context.Background(), key)
		if err == nil {
			return string(val), nil
		}
		last = err
		if !errors.Is(err, peercache.ErrNotOwner) &&
			!errors.Is(err, peercache.ErrActivationPending) &&
			!errors.Is(err, peercache.ErrStaleEpoch) {
			return "", err
		}
		runtime.Gosched()
	}
	return "", fmt.Errorf("%q never became readable: %w", key, last)
}
