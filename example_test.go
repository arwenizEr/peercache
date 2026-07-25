package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/peercache/peercache"
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

	_, err = sessions.Get(context.Background(), "user:123")
	fmt.Println("Get:", errors.Is(err, peercache.ErrNotImplemented))

	fmt.Println("Self:", cache.Stats().Self)
	// Output:
	// Get: true
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

	profiles := cache.Namespace("profiles",
		peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
			return []byte("loaded:" + key), time.Minute, nil
		}),
	)

	_, err = profiles.GetOrLoad(context.Background(), "user:123")
	fmt.Println("loader registered:", !errors.Is(err, peercache.ErrNoLoader))

	// Without a loader, GetOrLoad says so rather than returning nothing.
	bare := cache.Namespace("bare")
	_, err = bare.GetOrLoad(context.Background(), "user:123")
	fmt.Println("no loader:", errors.Is(err, peercache.ErrNoLoader))
	// Output:
	// loader registered: true
	// no loader: true
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

	increment := func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		// s is plain local memory on the group's single goroutine. No
		// context, no error, no lock — that is the point of moving the
		// computation to the data.
		current, _ := s.Get("count")
		s.Set("count", append(current, '.'))
		return nil, nil
	}

	if err := cache.RegisterGroupFunc("increment", increment); err != nil {
		fmt.Println(err)
		return
	}
	// Registration happens once, at startup, and is deterministic across
	// pods because every pod runs the same binary.
	err = cache.RegisterGroupFunc("increment", increment)
	fmt.Println("duplicate rejected:", errors.Is(err, peercache.ErrDuplicateRegistration))

	_, err = cache.Group("user:123").Call(context.Background(), "increment", nil)
	fmt.Println("Call:", errors.Is(err, peercache.ErrNotImplemented))

	_, err = cache.Group("user:123").Call(context.Background(), "unknown", nil)
	fmt.Println("unknown function:", errors.Is(err, peercache.ErrFuncNotRegistered))
	// Output:
	// duplicate rejected: true
	// Call: true
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

	router := peercache.NewRouter(cache, func(e Event) string { return e.TenantID }, "apply")

	// Submitting nothing succeeds: the caller's postcondition already holds.
	fmt.Println("empty submit:", router.Submit(context.Background()) == nil)

	err = router.Submit(context.Background(),
		Event{TenantID: "acme", Amount: 1},
		Event{TenantID: "acme", Amount: 2},
		Event{TenantID: "globex", Amount: 3},
	)
	fmt.Println("Submit:", errors.Is(err, peercache.ErrNotImplemented))
	// Output:
	// empty submit: true
	// Submit: true
}
