package peercache_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
)

// Example shows the whole M0 surface: construct, configure a namespace,
// register a group function, and shut down.
//
// Every data-path call reports ErrNotImplemented until M1 and M2 land. The
// shape is what is being fixed here; the behaviour follows.
func Example() {
	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithBindAddr("10.0.0.1:7946"),
	)
	if err != nil {
		fmt.Println("construction failed:", err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	sessions := cache.Namespace("sessions",
		peercache.WithL1TTL(2*time.Second),
		peercache.WithReplicationFactor(2),
	)

	ctx := context.Background()
	if err := sessions.Set(ctx, "user:123", []byte("cart=3"), time.Hour); err != nil {
		fmt.Println("set failed:", err)
		return
	}

	val, err := sessions.Get(ctx, "user:123")
	if err != nil {
		fmt.Println("get failed:", err)
		return
	}
	fmt.Printf("Get: %s\n", val)

	// A missing key is ErrNotFound, never a nil value: a key may legitimately
	// hold zero bytes.
	_, err = sessions.Get(ctx, "user:999")
	fmt.Println("Missing:", errors.Is(err, peercache.ErrNotFound))

	fmt.Println("Self:", cache.Stats().Self)
	// Output:
	// Get: cart=3
	// Missing: true
	// Self: pod-0(10.0.0.1:7946)
}

// ExampleWithLoader shows why GetOrLoad has no loader argument. The loader is
// registered on the namespace, so it exists by name on every pod and the key's
// owner can run it locally instead of shipping the miss back to the caller.
// Concurrent misses for one key collapse into a single invocation.
func ExampleWithLoader() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	var loaderCalls int
	profiles := cache.Namespace("profiles",
		peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
			loaderCalls++
			return []byte("loaded:" + key), time.Minute, nil
		}),
	)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		val, err := profiles.GetOrLoad(ctx, "user:123")
		if err != nil {
			fmt.Println("load failed:", err)
			return
		}
		if i == 0 {
			fmt.Printf("GetOrLoad: %s\n", val)
		}
	}
	// The first call loads; the rest are served from memory.
	fmt.Println("loader calls:", loaderCalls)

	// Without a loader, GetOrLoad says so rather than returning nothing.
	bare := cache.Namespace("bare")
	_, err = bare.GetOrLoad(ctx, "user:123")
	fmt.Println("no loader:", errors.Is(err, peercache.ErrNoLoader))
	// Output:
	// GetOrLoad: loaded:user:123
	// loader calls: 1
	// no loader: true
}

// ExampleNamespace_Set shows TTL expiry driven by an injected clock, which is
// how every timeout in this library is meant to be tested: no sleeping, and the
// same behaviour every run.
func ExampleNamespace_Set() {
	fake := clock.NewFake(clock.Epoch)
	cache, err := peercache.New(peercache.WithNodeID("pod-0"), peercache.WithClock(fake))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	sessions := cache.Namespace("sessions")
	ctx := context.Background()

	if err := sessions.Set(ctx, "user:123", []byte("cart=3"), time.Minute); err != nil {
		fmt.Println(err)
		return
	}

	fake.Advance(59 * time.Second)
	_, err = sessions.Get(ctx, "user:123")
	fmt.Println("after 59s:", err == nil)

	fake.Advance(2 * time.Second)
	_, err = sessions.Get(ctx, "user:123")
	fmt.Println("after 61s:", errors.Is(err, peercache.ErrNotFound))

	// A non-positive TTL means the value does not expire on its own. It can
	// still be lost: nothing here is durable.
	if err := sessions.Set(ctx, "forever", []byte("v"), 0); err != nil {
		fmt.Println(err)
		return
	}
	fake.Advance(365 * 24 * time.Hour)
	_, err = sessions.Get(ctx, "forever")
	fmt.Println("after a year, no TTL:", err == nil)
	// Output:
	// after 59s: true
	// after 61s: true
	// after a year, no TTL: true
}

// ExampleNamespace_Get shows the aliasing contract, which is the sharpest edge
// in this API: Set copies, Get does not.
func ExampleNamespace_Get() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	ns := cache.Namespace("values")
	ctx := context.Background()

	// Set copies, so the caller may reuse its buffer immediately.
	buf := []byte("original")
	if err := ns.Set(ctx, "k", buf, 0); err != nil {
		fmt.Println(err)
		return
	}
	copy(buf, "MUTATED!")

	stored, err := ns.Get(ctx, "k")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("reusing the write buffer is safe: %s\n", stored)

	// Get does NOT copy. Writing through the returned slice corrupts the
	// store for every later reader. Copy it if you need to change it.
	mine := make([]byte, len(stored))
	copy(mine, stored)
	mine[0] = 'O'
	fmt.Printf("copy before modifying: %s, store still holds %s\n", mine, stored)
	// Output:
	// reusing the write buffer is safe: original
	// copy before modifying: Original, store still holds original
}

// ExampleNamespace_BatchGet shows batch semantics: keys are grouped by owner
// and issued as one request per owner, and absent keys are omitted rather than
// reported as errors.
func ExampleNamespace_BatchGet() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	counters := cache.Namespace("counters")
	ctx := context.Background()

	if err := counters.BatchSet(ctx, map[string][]byte{
		"tenant:acme":   []byte("41"),
		"tenant:globex": []byte("7"),
	}, time.Hour); err != nil {
		fmt.Println(err)
		return
	}

	got, err := counters.BatchGet(ctx, []string{"tenant:acme", "tenant:globex", "tenant:missing"})
	if err != nil {
		fmt.Println(err)
		return
	}

	// Sorted, because map iteration order is not defined.
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s = %s\n", k, got[k])
	}
	fmt.Println("asked for 3, received:", len(got))
	// Output:
	// tenant:acme = 41
	// tenant:globex = 7
	// asked for 3, received: 2
}

// ExampleWithCompression shows opt-in compression above a size threshold.
//
// It is off by default: a decompress costs roughly seventy times a plain read,
// which is the wrong default for a cache whose premise is sub-microsecond local
// reads. Values below the threshold are stored verbatim, transparently.
func ExampleWithCompression() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	blobs := cache.Namespace("blobs", peercache.WithCompression("lz4", 1024))
	ctx := context.Background()

	large := []byte(strings.Repeat("peercache-", 500))
	small := []byte("well under the threshold")

	for name, payload := range map[string][]byte{"large": large, "small": small} {
		if err := blobs.Set(ctx, name, payload, time.Hour); err != nil {
			fmt.Println(err)
			return
		}
	}

	got, err := blobs.Get(ctx, "large")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("large round-trips intact:", bytes.Equal(got, large))

	got, err = blobs.Get(ctx, "small")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("small round-trips intact:", bytes.Equal(got, small))

	// An unregistered compressor is rejected at construction, so a typo
	// fails at startup rather than on the first large value.
	broken := cache.Namespace("broken", peercache.WithCompression("lz5", 1024))
	_, err = broken.Get(ctx, "anything")
	fmt.Println("unknown compressor rejected:", errors.Is(err, peercache.ErrInvalidOption))
	// Output:
	// large round-trips intact: true
	// small round-trips intact: true
	// unknown compressor rejected: true
}

// ExampleWithLoaderTimeout shows that a load is bounded by the namespace, not
// by whichever caller happened to arrive first.
//
// Concurrent misses collapse into one load. If that load inherited the first
// caller's deadline, a caller with milliseconds left could abort a load that
// hundreds of callers with seconds left were depending on.
func ExampleWithLoaderTimeout() {
	fake := clock.NewFake(clock.Epoch)
	cache, err := peercache.New(peercache.WithNodeID("pod-0"), peercache.WithClock(fake))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	deadline := make(chan error, 1)
	entered := make(chan struct{}, 1)

	profiles := cache.Namespace("profiles",
		peercache.WithLoaderTimeout(30*time.Second),
		peercache.WithLoader(func(loadCtx context.Context, _ string) ([]byte, time.Duration, error) {
			entered <- struct{}{}
			<-loadCtx.Done() // the namespace's deadline, not the caller's
			deadline <- loadCtx.Err()
			return nil, 0, loadCtx.Err()
		}))

	result := make(chan error, 1)
	go func() {
		_, err := profiles.GetOrLoad(context.Background(), "user:123")
		result <- err
	}()

	<-entered
	fake.BlockUntil(2) // the cache's sweep ticker, plus the loader's deadline
	fake.Advance(30 * time.Second)

	fmt.Println("loader stopped on its own deadline:", errors.Is(<-deadline, context.DeadlineExceeded))
	fmt.Println("caller saw it too:", errors.Is(<-result, context.DeadlineExceeded))
	// Output:
	// loader stopped on its own deadline: true
	// caller saw it too: true
}

// ExampleWithShards shows the per-namespace shard count. The default of 256 is
// well above any realistic core count; lower it only for a namespace known to
// hold very few keys, where 256 mostly-empty maps are pure overhead.
func ExampleWithShards() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	// Rounded up to a power of two, so shard selection is a mask rather than
	// a division.
	tiny := cache.Namespace("feature-flags", peercache.WithShards(3))
	ctx := context.Background()

	if err := tiny.Set(ctx, "dark-mode", []byte("on"), 0); err != nil {
		fmt.Println(err)
		return
	}
	got, err := tiny.Get(ctx, "dark-mode")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("dark-mode = %s\n", got)
	// Output:
	// dark-mode = on
}

// ExamplePartitionByPrefix shows how keys are grouped so that related keys
// share an owner and can be updated in a single actor turn.
func ExamplePartitionByPrefix() {
	p := peercache.PartitionByPrefix("/")
	for _, key := range []string{"user:123/cart", "user:123/profile", "user:456/cart"} {
		fmt.Printf("%s -> %s\n", key, p.GroupKey(key))
	}
	// Output:
	// user:123/cart -> user:123
	// user:123/profile -> user:123
	// user:456/cart -> user:456
}

// ExampleCache_RegisterGroupFunc shows the primary API: a computation that runs
// on the owner, inside the group's turn, with exclusive access to its state and
// no locking.
func ExampleCache_RegisterGroupFunc() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	increment := func(_ context.Context, s peercache.GroupState, delta []byte) ([]byte, error) {
		// s is plain local memory on the group's single goroutine. No
		// context, no error, no lock — that is the point of moving the
		// computation to the data. Nothing here can race, because nothing
		// else runs on this group while this turn does.
		var n uint64
		if cur, ok := s.Get("count"); ok {
			n = binary.BigEndian.Uint64(cur)
		}
		n += binary.BigEndian.Uint64(delta)

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		s.Set("count", buf)
		return buf, nil
	}

	if err := cache.RegisterGroupFunc("increment", increment); err != nil {
		fmt.Println(err)
		return
	}
	// Registration happens once, at startup, and is deterministic across
	// pods because every pod runs the same binary.
	err = cache.RegisterGroupFunc("increment", increment)
	fmt.Println("duplicate rejected:", errors.Is(err, peercache.ErrDuplicateRegistration))

	one := make([]byte, 8)
	binary.BigEndian.PutUint64(one, 1)

	ctx := context.Background()
	group := cache.Group("user:123")
	for i := 0; i < 3; i++ {
		if _, err := group.Call(ctx, "increment", one); err != nil {
			fmt.Println(err)
			return
		}
	}
	total, err := group.Call(ctx, "increment", one)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("count after four calls:", binary.BigEndian.Uint64(total))

	_, err = group.Call(ctx, "unknown", nil)
	fmt.Println("unknown function:", errors.Is(err, peercache.ErrFuncNotRegistered))
	// Output:
	// duplicate rejected: true
	// count after four calls: 4
	// unknown function: true
}

// ExampleNewRouter shows the queue-workload API. Submit groups messages by
// owner and issues one request per owner, so cost scales with the number of
// distinct owners rather than the number of messages.
func ExampleNewRouter() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	type Event struct {
		TenantID string
		Amount   int
	}

	// The handler receives a whole group's batch, not one message, which is
	// what lets a group's messages be applied in a single turn.
	totals := map[string]int{}
	if err := cache.RegisterGroupFunc("apply", func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
		events, err := peercache.Batch[Event](arg)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			totals[s.ID()] += e.Amount
		}
		return nil, nil
	}); err != nil {
		fmt.Println(err)
		return
	}

	router := peercache.NewRouter(cache, func(e Event) string { return e.TenantID }, "apply")

	// Submitting nothing succeeds: the caller's postcondition already holds.
	fmt.Println("empty submit:", router.Submit(context.Background()) == nil)

	err = router.Submit(context.Background(),
		Event{TenantID: "acme", Amount: 1},
		Event{TenantID: "acme", Amount: 2},
		Event{TenantID: "globex", Amount: 3},
	)
	fmt.Println("Submit:", err == nil)
	fmt.Println("acme:", totals["acme"])
	fmt.Println("globex:", totals["globex"])
	// Output:
	// empty submit: true
	// Submit: true
	// acme: 3
	// globex: 3
}

// ExampleCache_WriteMetrics shows the observability surface added in M7.
//
// PeerCache imports no metrics client library; it exports values and renders
// the Prometheus text exposition format itself. The reasoning is in
// docs/decisions/0017-metrics-without-a-client-library.md.
func ExampleCache_WriteMetrics() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		panic(err)
	}
	defer cache.Close(context.Background())

	ctx := context.Background()
	orders := cache.Namespace("orders")
	if err := orders.Set(ctx, "order/1", []byte("paid"), 0); err != nil {
		panic(err)
	}
	if _, err := orders.Get(ctx, "order/1"); err != nil {
		panic(err)
	}
	if _, err := orders.Get(ctx, "order/absent"); !errors.Is(err, peercache.ErrNotFound) {
		panic(err)
	}

	var buf bytes.Buffer
	if err := cache.WriteMetrics(&buf); err != nil {
		panic(err)
	}

	// Print a few series rather than all 38, so the example stays readable.
	want := map[string]bool{
		"peercache_members":                      true,
		"peercache_owned_keys":                   true,
		"peercache_hits_total":                   true,
		"peercache_misses_total":                 true,
		"peercache_split_brain_detections_total": true,
		"peercache_implicit_namespaces_total":    true,
	}
	for _, line := range strings.Split(buf.String(), "\n") {
		if name, _, ok := strings.Cut(line, " "); ok && want[name] {
			fmt.Println(line)
		}
	}

	// The same output over HTTP, with the content type a scraper expects:
	//
	//	mux.Handle("/metrics", cache.MetricsHandler())

	// The two counters printed as zero are the point of the exercise. Both
	// report a window this system is documented as having, and an operator
	// alerts on them leaving zero — implicit_namespaces in particular, which
	// is a silent misconfiguration that looks exactly like a data-loss bug.

	// Series come out sorted by name, which is why this block is alphabetical
	// rather than in the order the fields are declared.

	// Output:
	// peercache_hits_total 1
	// peercache_implicit_namespaces_total 0
	// peercache_members 1
	// peercache_misses_total 1
	// peercache_owned_keys 1
	// peercache_split_brain_detections_total 0
}

// ExampleWithRequireReplicationFactor shows what the option changes, on the
// smallest cluster that exposes it: one node, a namespace asking for three
// copies.
//
// Without the option the write succeeds holding one copy, and only
// Stats().DegradedWrites says so. With it the write fails with
// ErrUnderReplicated — and the value is still held, because a refused
// synchronous write is reported rather than rolled back.
func ExampleWithRequireReplicationFactor() {
	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()
	ctx := context.Background()

	lenient := cache.Namespace("lenient",
		peercache.WithReplicationFactor(3),
		peercache.WithSyncReplication())
	strict := cache.Namespace("strict",
		peercache.WithReplicationFactor(3),
		peercache.WithSyncReplication(),
		peercache.WithRequireReplicationFactor())

	fmt.Println("lenient Set:", lenient.Set(ctx, "k", []byte("v"), 0))
	fmt.Println("degraded writes:", cache.Stats().DegradedWrites)

	err = strict.Set(ctx, "k", []byte("v"), 0)
	fmt.Println("strict Set refused:", errors.Is(err, peercache.ErrUnderReplicated))
	held, _ := strict.Get(ctx, "k")
	fmt.Printf("still held locally: %s\n", held)

	// Asynchronous replication cannot know how many copies landed, so asking
	// it to refuse is a configuration error.
	async := cache.Namespace("async",
		peercache.WithReplicationFactor(3),
		peercache.WithRequireReplicationFactor())
	fmt.Println("without sync:", errors.Is(async.Set(ctx, "k", nil, 0), peercache.ErrInvalidOption))
	// Output:
	// lenient Set: <nil>
	// degraded writes: 1
	// strict Set refused: true
	// still held locally: v
	// without sync: true
}

// ExampleWithMemoryBudget shows a node shedding rather than growing without
// bound: a megabyte written into a 64 KiB budget leaves the node near the
// budget, with the shed counted.
func ExampleWithMemoryBudget() {
	const budget = 64 << 10
	cache, err := peercache.New(peercache.WithNodeID("pod-0"), peercache.WithMemoryBudget(budget))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	ns := cache.Namespace("sessions")
	for i := 0; i < 1000; i++ {
		_ = ns.Set(context.Background(), fmt.Sprintf("user:%d", i), make([]byte, 1024), 0)
	}

	st := cache.Stats()
	// The budget is checked every PressureCheckInterval writes, so the node
	// can be up to that many entries past it between checks.
	fmt.Println("near the budget:", st.MemoryUsed < budget+peercache.PressureCheckInterval*2048)
	fmt.Println("owned keys shed:", st.ShedOwned > 0)
	fmt.Println("keys still held:", st.OwnedKeys > 0)
	// Output:
	// near the budget: true
	// owned keys shed: true
	// keys still held: true
}

// ExampleWithJoinProbation shows the option's configuration and its two
// boundaries. A cache without a membership authority is a cluster of one, so
// there is nobody to keep waiting and the probation has nothing to do; a
// negative probation is a configuration error. Its effect on a real cluster is
// what TestACrashloopingNodeIsNeverRanked demonstrates.
func ExampleWithJoinProbation() {
	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithJoinProbation(30*time.Second),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	fmt.Println("members on probation:", cache.Stats().MembersOnProbation)
	fmt.Println("ready:", cache.Ready().Ready)

	_, err = peercache.New(peercache.WithNodeID("pod-1"), peercache.WithJoinProbation(-time.Second))
	fmt.Println("negative rejected:", errors.Is(err, peercache.ErrInvalidOption))
	// Output:
	// members on probation: 0
	// ready: true
	// negative rejected: true
}

// ExampleNamespaceOf shows the typed view: values in and out as a Go type, a
// loader that returns one, and the errors that keep a type mismatch from
// passing for data.
func ExampleNamespaceOf() {
	type Profile struct {
		Name  string
		Admin bool
	}

	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()
	ctx := context.Background()

	profiles := peercache.NamespaceOf[Profile](cache, "profiles",
		peercache.WithTypedLoader(func(_ context.Context, id string) (Profile, time.Duration, error) {
			// Stands in for a database read on the key's owner.
			return Profile{Name: "loaded:" + id}, time.Minute, nil
		}))

	if err := profiles.Set(ctx, "alice", Profile{Name: "Alice", Admin: true}, time.Hour); err != nil {
		fmt.Println(err)
		return
	}
	alice, _ := profiles.Get(ctx, "alice")
	fmt.Printf("set and got: %+v\n", alice)

	bob, _ := profiles.GetOrLoad(ctx, "bob")
	fmt.Printf("loaded: %+v\n", bob)

	_, err = profiles.Get(ctx, "carol")
	fmt.Println("missing:", errors.Is(err, peercache.ErrNotFound))

	// The same name opened as another type is refused rather than decoded.
	_, err = peercache.NamespaceOf[int](cache, "profiles").Get(ctx, "alice")
	fmt.Println("wrong type:", errors.Is(err, peercache.ErrInvalidOption))
	// Output:
	// set and got: {Name:Alice Admin:true}
	// loaded: {Name:loaded:bob Admin:false}
	// missing: true
	// wrong type: true
}

// ExampleRegisterTypedGroupFunc shows a group function with a typed argument and
// result. The group's state stays []byte; only what crosses the call is typed.
func ExampleRegisterTypedGroupFunc() {
	type Deposit struct{ Amount int }
	type Balance struct{ Total int }

	cache, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = cache.Close(context.Background()) }()

	err = peercache.RegisterTypedGroupFunc(cache, "deposit",
		func(_ context.Context, s peercache.GroupState, d Deposit) (Balance, error) {
			var total int
			if raw, ok := s.Get("total"); ok {
				total = int(binary.BigEndian.Uint64(raw))
			}
			total += d.Amount
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(total))
			s.Set("total", buf)
			return Balance{Total: total}, nil
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx := context.Background()
	account := cache.Group("account:42")
	for _, amount := range []int{100, 250} {
		if _, err := peercache.CallTyped[Deposit, Balance](ctx, account, "deposit", Deposit{amount}); err != nil {
			fmt.Println(err)
			return
		}
	}
	b, _ := peercache.CallTyped[Deposit, Balance](ctx, account, "deposit", Deposit{50})
	fmt.Println("balance:", b.Total)

	_, err = peercache.CallTyped[string, Balance](ctx, account, "deposit", "fifty")
	fmt.Println("wrong argument type:", errors.Is(err, peercache.ErrInvalidOption))
	// Output:
	// balance: 400
	// wrong argument type: true
}
