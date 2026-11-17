package tests

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// BenchmarkStrayBacklogOverGRPC is a node handing on a backlog of
// out-of-place copies: three caches over real gRPC on loopback, an RF=2
// namespace, b.N copies filed at the one node that ranks for none of them.
// ns/op is the time from the first copy sent to the last handed on, over b.N.
// It includes up to a second of waiting for the next tick, and the queue takes
// at most 4,096 copies a tick, so it is read at tens of thousands of copies.
//
// A rolling restart on kind left a pod with 117,000 such copies, and it handed
// them on at about 400 a second, one after another.
func BenchmarkStrayBacklogOverGRPC(b *testing.B) {
	if testing.Short() {
		b.Skip("binds loopback sockets")
	}
	nodes := newGRPCCluster(b, 3)
	for _, n := range nodes {
		n.cache.Namespace("backlog", peercache.WithReplicationFactor(2))
	}
	stray := nodes[2]

	type copyOf struct{ key, owner string }
	copies := make([]copyOf, 0, b.N)
	for i := 0; len(copies) < b.N; i++ {
		k := fmt.Sprintf("stray%d/%d", b.N, i)
		p, err := stray.cache.DescribeKey("backlog", k)
		if err != nil {
			b.Fatal(err)
		}
		if p.Holders[0] != stray.id && p.Holders[1] != stray.id {
			copies = append(copies, copyOf{k, p.Holders[0]})
		}
	}

	// A client of its own, so the copies arrive as any peer's would.
	tr, err := transport.New(transport.Options{Self: core.Node{ID: "bench-sender", Addr: "127.0.0.1:1"}})
	if err != nil {
		b.Fatal(err)
	}
	client, err := tr.Dial(stray.info)
	if err != nil {
		b.Fatal(err)
	}
	val := make([]byte, 64)
	rehomed := func() int64 { return stray.cache.Stats().ReplicasRehomed }
	base := rehomed()

	b.ResetTimer()
	var wg sync.WaitGroup
	work := make(chan copyOf)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				req, err := wire.Encode(&wire.ReplicateRequest{
					Namespace: "backlog", Key: c.key, Value: val,
					Stamp: core.Stamp{Epoch: 1, Version: 1}, From: c.owner,
				})
				if err != nil {
					b.Error(err)
					return
				}
				if _, err := client.Invoke(context.Background(), transport.MethodReplicate, req); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	for _, c := range copies {
		work <- c
	}
	close(work)
	wg.Wait()

	for rehomed()-base < int64(b.N) {
		runtime.Gosched()
	}
	b.StopTimer()
}
