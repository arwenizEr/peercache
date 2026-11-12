// Command lifecycle demonstrates a cache's lifecycle: construction, option
// validation, registration, and shutdown.
//
// Nothing distributed happens here, and nothing is meant to. Configuration is
// validated at startup rather than on the first request, and this shows what
// that looks like. The data paths are in ./examples/namespace and
// ./examples/groups.
//
// Run it with:
//
//	go run ./examples/lifecycle
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/arwenizEr/peercache"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	// Configuration is validated once, here, so a misconfiguration surfaces
	// at startup rather than on the first request.
	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithBindAddr("127.0.0.1:7946"),
	)
	if err != nil {
		return fmt.Errorf("constructing the cache: %w", err)
	}
	defer func() { _ = cache.Close(context.Background()) }()

	fmt.Printf("node %v, %d member(s) in view\n", cache.Stats().Self, cache.Stats().Members)

	// A namespace is a keyspace with its own loader, partitioner,
	// replication factor and L1 TTL.
	sessions := cache.Namespace("sessions",
		// Grouping decides ownership, so it has no observable effect
		// until there is a cluster to own anything.
		peercache.WithPartitioner(peercache.PartitionByPrefix("/")),
		peercache.WithReplicationFactor(2),
		peercache.WithL1TTL(2*time.Second),
		peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
			return []byte("reconstructed:" + key), time.Minute, nil
		}),
	)

	ctx := context.Background()
	if err := sessions.Set(ctx, "user:123/cart", []byte("3"), time.Minute); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	fmt.Println("namespace constructed and usable; see examples/namespace for its data path")

	// Registered functions are how computation moves to the data. Closures
	// never cross the network; an RPC carries the name and argument bytes.
	increment := func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
		current, _ := s.Get("count")
		return append(current, arg...), nil
	}
	if err := cache.RegisterGroupFunc("increment", increment); err != nil {
		return fmt.Errorf("registering increment: %w", err)
	}
	if err := cache.RegisterGroupFunc("increment", increment); !errors.Is(err, peercache.ErrDuplicateRegistration) {
		return fmt.Errorf("expected ErrDuplicateRegistration, got %w", err)
	}
	fmt.Println("group function registered; duplicate registration rejected")

	group := cache.Group("user:123")
	if _, err := group.Call(ctx, "increment", []byte("+1")); err != nil {
		return fmt.Errorf("calling increment: %w", err)
	}
	if _, err := group.Call(ctx, "unregistered", nil); !errors.Is(err, peercache.ErrFuncNotRegistered) {
		return fmt.Errorf("expected ErrFuncNotRegistered, got %w", err)
	}
	fmt.Println("group function invoked; unknown names report ErrFuncNotRegistered")

	// Bad configuration is reported rather than ignored, even though
	// Namespace cannot return an error.
	broken := cache.Namespace("broken", peercache.WithReplicationFactor(9))
	if _, err := broken.Get(ctx, "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		return fmt.Errorf("expected ErrInvalidOption, got %w", err)
	}
	fmt.Println("invalid namespace options surface on every operation")

	if err := cache.Close(ctx); err != nil {
		return fmt.Errorf("closing: %w", err)
	}
	if _, err := sessions.Get(ctx, "user:123/cart"); !errors.Is(err, peercache.ErrClosed) {
		return fmt.Errorf("expected ErrClosed after Close, got %w", err)
	}
	fmt.Println("after Close, every operation reports ErrClosed")

	return nil
}
