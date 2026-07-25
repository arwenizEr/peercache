package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/peercache/peercache"
)

// newCache builds a cache with the minimum viable configuration.
func newCache(t *testing.T, opts ...peercache.Option) peercache.Cache {
	t.Helper()
	c, err := peercache.New(append([]peercache.Option{peercache.WithNodeID("n0")}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestNewRequiresNodeID(t *testing.T) {
	if _, err := peercache.New(); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New without a node ID = %v, want ErrInvalidOption", err)
	}
}

func TestNewRejectsNilClockAndLogger(t *testing.T) {
	tests := map[string]peercache.Option{
		"clock":  peercache.WithClock(nil),
		"logger": peercache.WithLogger(nil),
	}
	for name, opt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := peercache.New(peercache.WithNodeID("n0"), opt)
			if !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("New with a nil %s = %v, want ErrInvalidOption", name, err)
			}
		})
	}
}

func TestNewIgnoresNilOptions(t *testing.T) {
	// A nil option in a slice built by a caller's helper should not be a
	// crash. There are no panics in library paths.
	if _, err := peercache.New(nil, peercache.WithNodeID("n0"), nil); err != nil {
		t.Fatalf("New with nil options = %v, want nil", err)
	}
}

func TestStatsReportsSelf(t *testing.T) {
	c := newCache(t, peercache.WithBindAddr("n0:7946"))
	stats := c.Stats()
	if stats.Self.ID != "n0" || stats.Self.Addr != "n0:7946" {
		t.Fatalf("Stats().Self = %v, want n0(n0:7946)", stats.Self)
	}
	if stats.Members != 1 {
		t.Fatalf("Stats().Members = %d, want 1", stats.Members)
	}
}

// TestNamespaceOperationsAreNotImplemented enumerates the whole Namespace
// surface. The point is not the error value but the guarantee that every method
// in SPEC.md section 3.1 exists, compiles and is reachable, so that M1 replaces
// behaviour rather than inventing signatures.
func TestNamespaceOperationsAreNotImplemented(t *testing.T) {
	c := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	tests := map[string]func() error{
		"Get":      func() error { _, err := ns.Get(ctx, "k"); return err },
		"Set":      func() error { return ns.Set(ctx, "k", []byte("v"), time.Minute) },
		"Delete":   func() error { return ns.Delete(ctx, "k") },
		"Exists":   func() error { _, err := ns.Exists(ctx, "k"); return err },
		"BatchGet": func() error { _, err := ns.BatchGet(ctx, []string{"k"}); return err },
		"BatchSet": func() error { return ns.BatchSet(ctx, map[string][]byte{"k": nil}, time.Minute) },
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, peercache.ErrNotImplemented) {
				t.Fatalf("%s = %v, want ErrNotImplemented", name, err)
			}
		})
	}
}

func TestGetOrLoadWithoutLoaderReportsErrNoLoader(t *testing.T) {
	c := newCache(t)
	ns := c.Namespace("sessions")
	if _, err := ns.GetOrLoad(context.Background(), "k"); !errors.Is(err, peercache.ErrNoLoader) {
		t.Fatalf("GetOrLoad = %v, want ErrNoLoader", err)
	}
}

func TestGetOrLoadWithLoaderIsNotImplemented(t *testing.T) {
	c := newCache(t)
	loader := func(context.Context, string) ([]byte, time.Duration, error) { return nil, 0, nil }
	ns := c.Namespace("sessions", peercache.WithLoader(loader))
	if _, err := ns.GetOrLoad(context.Background(), "k"); !errors.Is(err, peercache.ErrNotImplemented) {
		t.Fatalf("GetOrLoad = %v, want ErrNotImplemented", err)
	}
}

// TestNamespaceOptionValidationSurfaces covers the awkward corner of SPEC.md
// section 3.1: Namespace cannot return an error, so a bad option has to surface
// somewhere. Ignoring it would hide the misconfiguration until production.
func TestNamespaceOptionValidationSurfaces(t *testing.T) {
	c := newCache(t)
	tests := map[string]peercache.NamespaceOption{
		"replication factor too high": peercache.WithReplicationFactor(4),
		"replication factor zero":     peercache.WithReplicationFactor(0),
		"non-positive L1 TTL":         peercache.WithL1TTL(0),
		"nil partitioner":             peercache.WithPartitioner(nil),
	}
	for name, opt := range tests {
		t.Run(name, func(t *testing.T) {
			// Distinct names: namespace handles are memoized, so
			// reusing one here would test the first subtest's
			// configuration three more times.
			ns := c.Namespace("bad-"+name, opt)
			if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("Get = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestNamespaceAcceptsValidReplicationFactors(t *testing.T) {
	c := newCache(t)
	for rf := 1; rf <= 3; rf++ {
		ns := c.Namespace(fmt.Sprintf("ok-rf%d", rf), peercache.WithReplicationFactor(rf))
		if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrNotImplemented) {
			t.Fatalf("replication factor %d = %v, want ErrNotImplemented", rf, err)
		}
	}
}

// TestNamespaceHandleIsMemoized pins the documented contract that the first
// call wins. Two handles to one namespace with different replication factors
// and different staleness bounds is a data-loss bug wearing a
// convenience-feature costume.
func TestNamespaceHandleIsMemoized(t *testing.T) {
	c := newCache(t)
	first := c.Namespace("sessions", peercache.WithReplicationFactor(2))
	second := c.Namespace("sessions", peercache.WithReplicationFactor(9))

	if first != second {
		t.Fatal("Namespace returned a different handle for the same name")
	}
	// If the second call had won, its invalid replication factor would
	// surface here.
	if _, err := second.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrNotImplemented) {
		t.Fatalf("Get = %v, want ErrNotImplemented; the later options were applied", err)
	}
}

func TestGroupOperationsAreNotImplemented(t *testing.T) {
	c := newCache(t)
	g := c.Group("user:123")
	ctx := context.Background()

	if g.ID() != "user:123" {
		t.Fatalf("ID() = %q, want user:123", g.ID())
	}

	tests := map[string]func() error{
		"Get":    func() error { _, err := g.Get(ctx, "k"); return err },
		"Set":    func() error { return g.Set(ctx, "k", []byte("v")) },
		"Delete": func() error { return g.Delete(ctx, "k") },
		"Keys":   func() error { _, err := g.Keys(ctx); return err },
		"Items":  func() error { _, err := g.Items(ctx); return err },
		"Flush":  func() error { return g.Flush(ctx) },
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, peercache.ErrNotImplemented) {
				t.Fatalf("%s = %v, want ErrNotImplemented", name, err)
			}
		})
	}
}

func TestRegisterGroupFunc(t *testing.T) {
	c := newCache(t)
	fn := func(context.Context, peercache.GroupState, []byte) ([]byte, error) { return nil, nil }

	if err := c.RegisterGroupFunc("increment", fn); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	if err := c.RegisterGroupFunc("increment", fn); !errors.Is(err, peercache.ErrDuplicateRegistration) {
		t.Fatalf("duplicate registration = %v, want ErrDuplicateRegistration", err)
	}
	if err := c.RegisterGroupFunc("", fn); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("empty name = %v, want ErrInvalidOption", err)
	}
	if err := c.RegisterGroupFunc("nilfn", nil); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("nil function = %v, want ErrInvalidOption", err)
	}
}

// TestCallDistinguishesUnregisteredFromUnimplemented matters more than it
// looks: during a rolling upgrade a missing registration is a real, expected
// condition, and it must not be confused with scaffolding that has not been
// built yet.
func TestCallDistinguishesUnregisteredFromUnimplemented(t *testing.T) {
	c := newCache(t)
	g := c.Group("user:123")
	ctx := context.Background()

	if _, err := g.Call(ctx, "missing", nil); !errors.Is(err, peercache.ErrFuncNotRegistered) {
		t.Fatalf("Call of an unregistered function = %v, want ErrFuncNotRegistered", err)
	}

	fn := func(context.Context, peercache.GroupState, []byte) ([]byte, error) { return nil, nil }
	if err := c.RegisterGroupFunc("known", fn); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	if _, err := g.Call(ctx, "known", nil); !errors.Is(err, peercache.ErrNotImplemented) {
		t.Fatalf("Call of a registered function = %v, want ErrNotImplemented", err)
	}
}

func TestClosedCacheReportsErrClosed(t *testing.T) {
	c, err := peercache.New(peercache.WithNodeID("n0"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ns := c.Namespace("sessions")
	g := c.Group("user:123")
	ctx := context.Background()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent: a shutdown path that can be reached twice must
	// not fail the second time.
	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace.Get after Close = %v, want ErrClosed", err)
	}
	if _, err := ns.GetOrLoad(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace.GetOrLoad after Close = %v, want ErrClosed", err)
	}
	if _, err := g.Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Group.Get after Close = %v, want ErrClosed", err)
	}
	if _, err := g.Call(ctx, "anything", nil); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Group.Call after Close = %v, want ErrClosed", err)
	}
	fn := func(context.Context, peercache.GroupState, []byte) ([]byte, error) { return nil, nil }
	if err := c.RegisterGroupFunc("late", fn); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("RegisterGroupFunc after Close = %v, want ErrClosed", err)
	}
}

// TestCancelledContextIsReportedFirst pins a rule that has to hold for every
// milestone: an expired deadline is the caller's answer, whatever the callee
// would otherwise have said.
func TestCancelledContextIsReportedFirst(t *testing.T) {
	c := newCache(t)
	ns := c.Namespace("sessions")
	g := c.Group("user:123")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := ns.Get(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Get = %v, want context.Canceled", err)
	}
	if err := ns.Set(ctx, "k", nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Set = %v, want context.Canceled", err)
	}
	if _, err := g.Call(ctx, "anything", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Group.Call = %v, want context.Canceled", err)
	}
}

func TestRouterSubmit(t *testing.T) {
	c := newCache(t)
	router := peercache.NewRouter(c, func(s string) string { return s }, "apply")
	ctx := context.Background()

	// Submitting nothing succeeds: the caller's postcondition already holds.
	if err := router.Submit(ctx); err != nil {
		t.Fatalf("Submit of no messages = %v, want nil", err)
	}
	if err := router.Submit(ctx, "a", "b"); !errors.Is(err, peercache.ErrNotImplemented) {
		t.Fatalf("Submit = %v, want ErrNotImplemented", err)
	}

	bad := peercache.NewRouter[string](c, nil, "apply")
	if err := bad.Submit(ctx, "a"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("Submit with no groupOf = %v, want ErrInvalidOption", err)
	}
}

func TestPartitioners(t *testing.T) {
	tests := []struct {
		name string
		p    peercache.Partitioner
		key  string
		want string
	}{
		{"by key", peercache.PartitionByKey(), "user:123/cart", "user:123/cart"},
		{"by prefix", peercache.PartitionByPrefix("/"), "user:123/cart", "user:123"},
		{"by prefix, first separator only", peercache.PartitionByPrefix("/"), "a/b/c", "a"},
		{"by prefix, separator absent", peercache.PartitionByPrefix("/"), "standalone", "standalone"},
		{"by prefix, leading separator", peercache.PartitionByPrefix("/"), "/leading", ""},
		{"by prefix, empty separator", peercache.PartitionByPrefix(""), "a/b", "a/b"},
		{"by func", peercache.PartitionByFunc(func(string) string { return "fixed" }), "anything", "fixed"},
		{"by func, nil", peercache.PartitionByFunc(nil), "anything", "anything"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.GroupKey(tc.key); got != tc.want {
				t.Fatalf("GroupKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestPartitionersAreStable is the property that actually matters: two pods
// must derive the same group from the same key, or the cluster has a split
// brain that no epoch check will catch.
func TestPartitionersAreStable(t *testing.T) {
	p := peercache.PartitionByPrefix("/")
	first := p.GroupKey("user:123/cart")
	for i := 0; i < 100; i++ {
		if got := p.GroupKey("user:123/cart"); got != first {
			t.Fatalf("GroupKey is not stable: %q then %q", first, got)
		}
	}
}

// TestSentinelErrorsAreDistinct guards against two sentinels being accidentally
// bound to the same object, which would make errors.Is silently over-match.
func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := map[string]error{
		"ErrNotImplemented":        peercache.ErrNotImplemented,
		"ErrNotFound":              peercache.ErrNotFound,
		"ErrClosed":                peercache.ErrClosed,
		"ErrGroupOverloaded":       peercache.ErrGroupOverloaded,
		"ErrStaleEpoch":            peercache.ErrStaleEpoch,
		"ErrNotOwner":              peercache.ErrNotOwner,
		"ErrStateEscaped":          peercache.ErrStateEscaped,
		"ErrNoLoader":              peercache.ErrNoLoader,
		"ErrFuncNotRegistered":     peercache.ErrFuncNotRegistered,
		"ErrDuplicateRegistration": peercache.ErrDuplicateRegistration,
		"ErrInvalidOption":         peercache.ErrInvalidOption,
		"ErrNodeUnreachable":       peercache.ErrNodeUnreachable,
	}
	for nameA, a := range sentinels {
		if a == nil {
			t.Fatalf("%s is nil", nameA)
		}
		for nameB, b := range sentinels {
			if nameA == nameB {
				continue
			}
			if errors.Is(a, b) {
				t.Fatalf("%s matches %s; sentinels must be distinct", nameA, nameB)
			}
		}
	}
}
