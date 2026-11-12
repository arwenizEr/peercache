package wire_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/wire"
)

// The message codec sits on every remote operation, twice — once to encode the
// request and once to decode the response — so its cost is paid per hop and is
// worth knowing separately from the network's.

func BenchmarkEncodeGetRequest(b *testing.B) {
	req := &wire.GetRequest{Namespace: "orders", Key: "tenant-42/order-99", Epoch: 7}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := wire.Encode(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeGetRequest(b *testing.B) {
	encoded, err := wire.Encode(&wire.GetRequest{
		Namespace: "orders", Key: "tenant-42/order-99", Epoch: 7,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out wire.GetRequest
		if err := wire.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeGetResponse uses a realistic value size. The payload dominates
// once it is more than a few hundred bytes, which is the point: the envelope
// should not be what a remote read costs.
func BenchmarkEncodeGetResponse(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			resp := &wire.GetResponse{
				Found: true,
				Value: make([]byte, size),
				Stamp: wire.Stamp{Epoch: 3, Version: 12},
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := wire.Encode(resp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeBatchGetResponse is the bounded decoder from bounded.go, which
// appends rather than pre-allocating. Appending costs a few reallocations that
// a trusted length would avoid; this measures what that safety is worth.
func BenchmarkDecodeBatchGetResponse(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			entries := make([]wire.BatchEntry, 0, n)
			for i := 0; i < n; i++ {
				entries = append(entries, wire.BatchEntry{
					Key:   fmt.Sprintf("key-%d", i),
					Value: make([]byte, 64),
					Stamp: wire.Stamp{Epoch: 1, Version: uint64(i)},
				})
			}
			encoded, err := wire.Encode(&wire.BatchGetResponse{Entries: entries})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out wire.BatchGetResponse
				if err := wire.Decode(encoded, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEncodeSubmitRequest is the router's flush payload: many groups, each
// with several messages. It is the largest message this protocol builds.
func BenchmarkEncodeSubmitRequest(b *testing.B) {
	const groups, perGroup = 100, 20
	batches := make([]wire.GroupBatch, 0, groups)
	for g := 0; g < groups; g++ {
		msgs := make([][]byte, 0, perGroup)
		for m := 0; m < perGroup; m++ {
			msgs = append(msgs, make([]byte, 32))
		}
		batches = append(batches, wire.GroupBatch{
			Group:    fmt.Sprintf("tenant-%d", g),
			Messages: msgs,
		})
	}
	req := &wire.SubmitRequest{FuncName: "apply", Batches: batches, Epoch: 5}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := wire.Encode(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStampNewerThan(b *testing.B) {
	a := wire.Stamp{Epoch: 4, Version: 900}
	c := wire.Stamp{Epoch: 5, Version: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !c.NewerThan(a) {
			b.Fatal("ordering is wrong")
		}
	}
}
