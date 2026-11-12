package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// Per-group serializability was once only asserted by construction: there is
// one goroutine per activation, therefore operations are serialized. The tests
// backing it checked that concurrent callers produced correct totals.
//
// A correct total is weaker than it looks. A cache that dropped one update and
// duplicated another produces the same number. Those tests showed that nothing
// was obviously broken.
//
// This records real histories and checks them with Porcupine, which asks the
// actual question: could every observed operation have happened in some
// sequential order consistent with real time? See
// docs/decisions/0018-porcupine-for-linearizability.md.
//
// # What is checked, and what is deliberately not
//
// The first draft of this file drove namespace reads from all three nodes and
// reported NOT LINEARIZABLE against a cluster with no faults injected at all.
// That result was true, and it was about a guarantee nobody ever made: reading
// a key through a Namespace from a node that does not own it is served by the
// L1 tier, and the design says those copies are TTL-bounded and may be stale.
//
// Pinning the same workload to the owner made it linearizable, which locates the
// boundary exactly:
//
//   - Group operations are linearizable from any node. They route to the owner
//     and execute in its turn loop with no cache in front of them. That is the
//     per-group guarantee the single activation provides, and
//     TestGroupHistoriesAreLinearizable checks it.
//   - Namespace operations are linearizable at the owner and boundedly stale
//     elsewhere. TestOwnerLocalHistoriesAreLinearizable checks the first half.
//
// # Why no network faults here
//
// Under the fake clock a dropped message is never delivered and a delayed one
// waits for an Advance that a concurrent client cannot issue, so an operation
// without a deadline hangs — which the first draft did for eight minutes. Fault
// schedules belong in the deterministic simulation, where time is driven on
// purpose rather than by whichever goroutine happens to run. The concurrency
// here comes from real scheduler interleaving across clients and nodes.

// linKeys is the model's key space. Small on purpose: the interesting
// interleavings are per group, and a wider key space costs search time without
// finding anything new.
const linKeys = 4

// linState is the modelled group state: one value per key, "" meaning absent.
// An array rather than a map so it is comparable and cheap to copy.
type linState [linKeys]string

type linInput struct {
	op    string // "get" | "set" | "delete"
	key   int
	value string
}

type linOutput struct {
	value string
	found bool
}

// linModel is the sequential specification a real history must be explainable
// by.
var linModel = porcupine.Model{
	Init: func() any { return linState{} },

	Step: func(state, input, output any) (bool, any) {
		st := state.(linState)
		in := input.(linInput)
		out := output.(linOutput)

		switch in.op {
		case "set":
			st[in.key] = in.value
			return true, st
		case "delete":
			st[in.key] = ""
			return true, st
		case "get":
			want := st[in.key]
			if want == "" {
				return !out.found, st
			}
			return out.found && out.value == want, st
		default:
			return false, st
		}
	},

	Equal: func(a, b any) bool { return a.(linState) == b.(linState) },

	DescribeOperation: func(input, output any) string {
		in := input.(linInput)
		out := output.(linOutput)
		switch in.op {
		case "set":
			return fmt.Sprintf("set(k%d, %s)", in.key, in.value)
		case "delete":
			return fmt.Sprintf("delete(k%d)", in.key)
		default:
			if !out.found {
				return fmt.Sprintf("get(k%d) -> <absent>", in.key)
			}
			return fmt.Sprintf("get(k%d) -> %s", in.key, out.value)
		}
	},

	DescribeState: func(state any) string {
		st := state.(linState)
		var sb strings.Builder
		for i, v := range st {
			if i > 0 {
				sb.WriteString(" ")
			}
			if v == "" {
				fmt.Fprintf(&sb, "k%d=-", i)
			} else {
				fmt.Fprintf(&sb, "k%d=%s", i, v)
			}
		}
		return sb.String()
	},
}

// linClock stamps call and return points.
//
// An atomic counter rather than a clock. CONTRIBUTING.md forbids time.Now outside
// internal/clock, and the platform's monotonic clock resolves to roughly 500 µs
// — larger than most operations being ordered. A counter cannot fail to
// distinguish two events that really did happen in order.
type linClock struct{ n atomic.Int64 }

func (c *linClock) now() int64 { return c.n.Add(1) }

type linHistory struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (h *linHistory) add(op porcupine.Operation) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

func (h *linHistory) sorted() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]porcupine.Operation, len(h.ops))
	copy(out, h.ops)
	sort.Slice(out, func(i, j int) bool { return out[i].Call < out[j].Call })
	return out
}

// TestGroupHistoriesAreLinearizable is the headline consistency check: the per-group
// guarantee, exercised from every node at once.
func TestGroupHistoriesAreLinearizable(t *testing.T) {
	seed := testkit.Seed(t)
	t.Logf("seed %d", seed)

	f := newFabric(t, 3)
	f.settle(t)

	for _, id := range f.ids {
		if err := f.caches[id].RegisterGroupFunc("linset", linSetFunc); err != nil {
			t.Fatalf("RegisterGroupFunc(linset) on %s: %v", id, err)
		}
		if err := f.caches[id].RegisterGroupFunc("linget", linGetFunc); err != nil {
			t.Fatalf("RegisterGroupFunc(linget) on %s: %v", id, err)
		}
	}

	const (
		clients = 6
		perCli  = 40
		groupID = "lin-group"
	)
	var lc linClock
	hist := &linHistory{}

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(client)*7919))
			// A different node per client on purpose: the claim is that
			// where you stand does not change what you observe.
			group := f.caches[f.ids[client%len(f.ids)]].Group(groupID)

			for i := 0; i < perCli; i++ {
				key := r.Intn(linKeys)
				switch r.Intn(3) {
				case 0, 1:
					val := fmt.Sprintf("c%d-%d", client, i)
					arg := fmt.Sprintf("k%d=%s", key, val)
					call := lc.now()
					_, err := group.Call(context.Background(), "linset", []byte(arg))
					ret := lc.now()
					if retryable(err) {
						continue
					}
					if err != nil {
						t.Errorf("client %d: Call(linset): %v", client, err)
						return
					}
					hist.add(porcupine.Operation{
						ClientId: client,
						Input:    linInput{op: "set", key: key, value: val},
						Call:     call, Output: linOutput{}, Return: ret,
					})
				default:
					call := lc.now()
					got, err := group.Call(context.Background(), "linget",
						[]byte(fmt.Sprintf("k%d", key)))
					ret := lc.now()
					if retryable(err) {
						continue
					}
					if err != nil {
						t.Errorf("client %d: Call(linget): %v", client, err)
						return
					}
					out := linOutput{}
					if len(got) > 0 {
						out.found, out.value = true, string(got)
					}
					hist.add(porcupine.Operation{
						ClientId: client,
						Input:    linInput{op: "get", key: key},
						Call:     call, Output: out, Return: ret,
					})
				}
			}
		}(c)
	}
	wg.Wait()

	ops := hist.sorted()
	if len(ops) < clients*perCli/3 {
		t.Fatalf("only %d of %d operations produced a definite answer; the history is "+
			"too thin to prove anything about ordering", len(ops), clients*perCli)
	}

	result, info := porcupine.CheckOperationsVerbose(linModel, ops, 30*time.Second)
	switch result {
	case porcupine.Ok:
		t.Logf("%d group operations across %d clients on %d nodes: linearizable",
			len(ops), clients, len(f.ids))
	case porcupine.Illegal:
		t.Fatalf("NOT LINEARIZABLE: %d group operations could not be explained by any "+
			"sequential order respecting real time. A group has one activation and one "+
			"mailbox, so this is precisely the guarantee the actor runtime exists to "+
			"provide.\nseed %d\n%s", len(ops), seed, describeLinFailure(ops, info))
	case porcupine.Unknown:
		t.Fatalf("the linearizability check timed out after 30s on %d operations; "+
			"this is not a pass", len(ops))
	}
}

// linSetFunc and linGetFunc run inside the turn, which is the point: the
// serialization under test is the runtime's, not the test's.
func linSetFunc(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
	key, val, ok := strings.Cut(string(arg), "=")
	if !ok {
		return nil, fmt.Errorf("malformed set arg %q", arg)
	}
	s.Set(key, []byte(val))
	return nil, nil
}

func linGetFunc(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
	val, ok := s.Get(string(arg))
	if !ok {
		return nil, nil
	}
	return val, nil
}

// TestOwnerLocalHistoriesAreLinearizable checks the namespace path where the
// guarantee applies: at the owner.
//
// Away from the owner, reads come from the L1 tier and are permitted to be
// stale, so this pins every client to the owning node. That is not the test
// dodging a hard case — it is the test being about the case the guarantee
// covers.
func TestOwnerLocalHistoriesAreLinearizable(t *testing.T) {
	seed := testkit.Seed(t)
	t.Logf("seed %d", seed)

	f := newFabric(t, 3)
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("linown", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	const full = "hot/k0"
	owner := f.ownerOf(t, full)
	t.Logf("owner of %q is %s", full, owner)

	const (
		clients = 8
		perCli  = 30
	)
	var lc linClock
	hist := &linHistory{}

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(client)))
			ns := openNS(owner)
			for i := 0; i < perCli; i++ {
				if r.Intn(2) == 0 {
					val := fmt.Sprintf("c%d-%d", client, i)
					call := lc.now()
					err := ns.Set(context.Background(), full, []byte(val), 0)
					ret := lc.now()
					if retryable(err) {
						continue
					}
					if err != nil {
						t.Errorf("Set: %v", err)
						return
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
				switch {
				case errors.Is(err, peercache.ErrNotFound):
				case err != nil:
					t.Errorf("Get: %v", err)
					return
				default:
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
	if len(ops) == 0 {
		t.Fatal("no operations recorded")
	}
	if !porcupine.CheckOperations(linModel, ops) {
		t.Fatalf("NOT LINEARIZABLE at the owner: %d operations, seed %d.\n"+
			"Every operation here executed on the node that owns the key, so there is "+
			"no L1 staleness to explain it away.\n%s", len(ops), seed, dumpOps(ops))
	}
	t.Logf("%d owner-local operations across %d clients: linearizable", len(ops), clients)
}

// TestTheCheckerRejectsAViolation is the control.
//
// A linearizability checker that passes everything is worse than none: it is a
// green light with no bulb behind it. This feeds the model histories that are
// definitely not linearizable and requires it to say so. Without this, the tests
// above prove only that Porcupine was called.
func TestTheCheckerRejectsAViolation(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: linInput{op: "set", key: 0, value: "a"},
			Call: 1, Output: linOutput{}, Return: 2},
		// Reads "b", which nobody ever wrote.
		{ClientId: 1, Input: linInput{op: "get", key: 0},
			Call: 3, Output: linOutput{found: true, value: "b"}, Return: 4},
	}
	if porcupine.CheckOperations(linModel, ops) {
		t.Fatal("the model accepted a read of a value that was never written; " +
			"it would accept anything, and the tests above prove nothing")
	}

	// Subtler: a value that was written, but read after a later write had
	// already returned.
	stale := []porcupine.Operation{
		{ClientId: 0, Input: linInput{op: "set", key: 0, value: "a"},
			Call: 1, Output: linOutput{}, Return: 2},
		{ClientId: 0, Input: linInput{op: "set", key: 0, value: "b"},
			Call: 3, Output: linOutput{}, Return: 4},
		{ClientId: 1, Input: linInput{op: "get", key: 0},
			Call: 5, Output: linOutput{found: true, value: "a"}, Return: 6},
	}
	if porcupine.CheckOperations(linModel, stale) {
		t.Fatal("the model accepted a read of a value overwritten by a write that had " +
			"already returned; real-time precedence is not being enforced")
	}

	// The control's control: a genuinely linearizable history must pass, or
	// the rejections above prove only that the model rejects everything.
	good := []porcupine.Operation{
		{ClientId: 0, Input: linInput{op: "set", key: 0, value: "a"},
			Call: 1, Output: linOutput{}, Return: 2},
		{ClientId: 1, Input: linInput{op: "get", key: 0},
			Call: 3, Output: linOutput{found: true, value: "a"}, Return: 4},
	}
	if !porcupine.CheckOperations(linModel, good) {
		t.Fatal("the model rejected a plainly linearizable history; it rejects " +
			"everything, which makes the failures above meaningless")
	}

	// Concurrent operations must stay legal: a read overlapping a write may
	// return either value. A model that demanded one of them would reject
	// correct histories and make every real run a coin toss.
	concurrent := []porcupine.Operation{
		{ClientId: 0, Input: linInput{op: "set", key: 0, value: "a"},
			Call: 1, Output: linOutput{}, Return: 10},
		{ClientId: 1, Input: linInput{op: "get", key: 0},
			Call: 2, Output: linOutput{found: false}, Return: 9},
	}
	if !porcupine.CheckOperations(linModel, concurrent) {
		t.Fatal("the model rejected a read that overlapped a write and saw the old " +
			"state; concurrent operations may linearize in either order")
	}
}

// retryable reports whether an error means "ask again" rather than "this is the
// answer".
//
// Operations returning one of these never enter the history. That is not
// leniency: the caller genuinely does not know whether the operation took
// effect, so admitting it would force the model to guess, and a checker that
// guesses is a checker that can be argued with.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, peercache.ErrNotOwner) ||
		errors.Is(err, peercache.ErrActivationPending) ||
		errors.Is(err, peercache.ErrStaleEpoch) ||
		errors.Is(err, peercache.ErrNodeUnreachable) ||
		errors.Is(err, peercache.ErrGroupOverloaded)
}

func dumpOps(ops []porcupine.Operation) string {
	var sb strings.Builder
	for i, op := range ops {
		if i > 40 {
			fmt.Fprintf(&sb, "... and %d more\n", len(ops)-i)
			break
		}
		fmt.Fprintf(&sb, "  [%d..%d] client %d: %s\n",
			op.Call, op.Return, op.ClientId,
			linModel.DescribeOperation(op.Input, op.Output))
	}
	return sb.String()
}

func describeLinFailure(ops []porcupine.Operation, info porcupine.LinearizationInfo) string {
	_ = info
	return dumpOps(ops)
}
