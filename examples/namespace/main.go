// Command namespace demonstrates the local namespace API.
//
// Everything here runs on a single node with no networking. Namespaces are
// fully functional: Get, Set, Delete, Exists, the batch forms, and GetOrLoad
// with singleflight collapsing. Groups are in ./examples/groups.
//
// Run it with:
//
//	go run ./examples/namespace
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithBindAddr("127.0.0.1:7946"),
	)
	if err != nil {
		return fmt.Errorf("constructing the cache: %w", err)
	}
	defer func() { _ = cache.Close(context.Background()) }()
	ctx := context.Background()

	if err := basics(ctx, cache); err != nil {
		return err
	}
	if err := batches(ctx, cache); err != nil {
		return err
	}
	if err := loading(ctx, cache); err != nil {
		return err
	}
	if err := compression(ctx, cache); err != nil {
		return err
	}

	stats := cache.Stats()
	fmt.Printf("\nstats: %d keys resident, %d hits, %d misses, %d loads (%d collapsed)\n",
		stats.OwnedKeys, stats.Hits, stats.Misses, stats.Loads, stats.CollapsedLoads)
	return nil
}

func basics(ctx context.Context, cache peercache.Cache) error {
	sessions := cache.Namespace("sessions")

	if err := sessions.Set(ctx, "user:123", []byte("cart=3"), time.Hour); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	val, err := sessions.Get(ctx, "user:123")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	fmt.Printf("get user:123 -> %s\n", val)

	// A missing key is ErrNotFound, never a nil value: a key may legitimately
	// hold zero bytes, and (nil, nil) would make the two indistinguishable.
	if _, err := sessions.Get(ctx, "user:999"); !errors.Is(err, peercache.ErrNotFound) {
		return fmt.Errorf("expected ErrNotFound, got %w", err)
	}
	fmt.Println("get user:999 -> not found, as distinct from an empty value")

	if err := sessions.Set(ctx, "user:empty", []byte{}, 0); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	if empty, err := sessions.Get(ctx, "user:empty"); err != nil || len(empty) != 0 {
		return fmt.Errorf("a zero-length value did not round trip: %q %w", empty, err)
	}
	fmt.Println("get user:empty -> a zero-length value, which is not absence")

	// Deleting an absent key is not an error: the caller's postcondition
	// already holds.
	if err := sessions.Delete(ctx, "never-existed"); err != nil {
		return fmt.Errorf("delete of an absent key: %w", err)
	}
	return nil
}

func batches(ctx context.Context, cache peercache.Cache) error {
	counters := cache.Namespace("counters",
		// Grouping decides which node owns a key, so on a single node it
		// changes nothing visible. It is set here because the
		// configuration is validated at startup and because changing it
		// later moves keys between owners, which is a decision worth
		// making before there is data.
		peercache.WithPartitioner(peercache.PartitionByPrefix("/")),
	)

	kv := map[string][]byte{
		"tenant:acme/orders":   []byte("41"),
		"tenant:acme/refunds":  []byte("2"),
		"tenant:globex/orders": []byte("7"),
	}
	if err := counters.BatchSet(ctx, kv, time.Hour); err != nil {
		return fmt.Errorf("batch set: %w", err)
	}

	got, err := counters.BatchGet(ctx, []string{
		"tenant:acme/orders",
		"tenant:acme/refunds",
		"tenant:acme/missing", // omitted from the result rather than reported
	})
	if err != nil {
		return fmt.Errorf("batch get: %w", err)
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	fmt.Printf("\nbatch get of 3 keys returned %d (absent keys are omitted, not errors)\n", len(got))
	fmt.Printf("  %s\n", strings.Join(keys, ", "))
	return nil
}

func loading(ctx context.Context, cache peercache.Cache) error {
	const callers = 1000

	var calls atomic.Int64
	var arrived atomic.Int64

	profiles := cache.Namespace("profiles",
		peercache.WithLoaderTimeout(5*time.Second),
		peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
			calls.Add(1)
			// Stand-in for a slow database query. Holding the load open
			// until every caller has arrived is what makes this a
			// demonstration of collapsing rather than of a loader that
			// happened to finish before the others got going.
			for arrived.Load() < callers {
				runtime.Gosched()
			}
			return []byte("profile of " + key), 10 * time.Minute, nil
		}),
	)

	// A thousand goroutines all miss the same cold key. The loader runs once.
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arrived.Add(1)
			if _, err := profiles.GetOrLoad(ctx, "user:123"); err != nil {
				fmt.Fprintln(os.Stderr, "load failed:", err)
			}
		}()
	}
	wg.Wait()

	stats := cache.Stats()
	fmt.Printf("\n%d concurrent misses on one cold key invoked the loader %d time(s); "+
		"%d calls collapsed onto it\n", callers, calls.Load(), stats.CollapsedLoads)

	val, err := profiles.GetOrLoad(ctx, "user:123")
	if err != nil {
		return fmt.Errorf("get or load: %w", err)
	}
	fmt.Printf("  value: %s\n", val)

	// Without a loader, GetOrLoad says so rather than returning nothing.
	if _, err := cache.Namespace("bare").GetOrLoad(ctx, "k"); !errors.Is(err, peercache.ErrNoLoader) {
		return fmt.Errorf("expected ErrNoLoader, got %w", err)
	}
	fmt.Println("  a namespace without a loader reports ErrNoLoader")
	return nil
}

func compression(ctx context.Context, cache peercache.Cache) error {
	// Compression is off by default: it costs roughly seventy times more on
	// the read path, which is the wrong default for a cache whose premise is
	// sub-microsecond local reads. Opt in per namespace, above a threshold.
	blobs := cache.Namespace("blobs", peercache.WithCompression("lz4", 1024))

	payload := []byte(strings.Repeat("peercache-", 2000))
	if err := blobs.Set(ctx, "document", payload, time.Hour); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	got, err := blobs.Get(ctx, "document")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	fmt.Printf("\ncompressed namespace round-tripped %d bytes intact: %v\n",
		len(payload), string(got) == string(payload))

	// Values below the threshold are stored uncompressed, transparently.
	if err := blobs.Set(ctx, "small", []byte("tiny"), time.Hour); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	small, err := blobs.Get(ctx, "small")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	fmt.Printf("  a %d-byte value below the threshold round-tripped too: %s\n", len("tiny"), small)
	return nil
}
