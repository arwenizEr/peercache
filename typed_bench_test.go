package peercache_test

import (
	"context"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/codec"
)

// session is a typical small cached value: a few fields, under 100 bytes encoded.
type session struct {
	UserID  string   `json:"user_id"`
	Expires int64    `json:"expires"`
	Roles   []string `json:"roles"`
	Cart    int      `json:"cart"`
}

var benchSession = session{UserID: "user-123456", Expires: 1_700_000_000, Roles: []string{"admin", "beta"}, Cart: 3}

// M10's criterion is that the typed path costs the byte path plus one encode or
// decode. These four measure the three terms: typed, the byte path on the same
// stored bytes, and the codec on its own.

func BenchmarkTypedGet(b *testing.B) {
	ns := peercache.NamespaceOf[session](benchCache(b), "typed")
	ctx := context.Background()
	if err := ns.Set(ctx, "k", benchSession, time.Hour); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Get(ctx, "k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedGetBytePath(b *testing.B) {
	ns := peercache.NamespaceOf[session](benchCache(b), "typed")
	ctx := context.Background()
	if err := ns.Set(ctx, "k", benchSession, time.Hour); err != nil {
		b.Fatal(err)
	}
	raw := ns.Raw()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := raw.Get(ctx, "k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedDecodeOnly(b *testing.B) {
	c, err := codec.Get(peercache.DefaultTypedCodec)
	if err != nil {
		b.Fatal(err)
	}
	data, err := c.Marshal(benchSession)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s session
		if err := c.Unmarshal(data, &s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedSet(b *testing.B) {
	ns := peercache.NamespaceOf[session](benchCache(b), "typed")
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.Set(ctx, ks[i%len(ks)], benchSession, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedSetBytePath(b *testing.B) {
	c, err := codec.Get(peercache.DefaultTypedCodec)
	if err != nil {
		b.Fatal(err)
	}
	data, err := c.Marshal(benchSession)
	if err != nil {
		b.Fatal(err)
	}
	raw := peercache.NamespaceOf[session](benchCache(b), "typed").Raw()
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := raw.Set(ctx, ks[i%len(ks)], data, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedEncodeOnly(b *testing.B) {
	c, err := codec.Get(peercache.DefaultTypedCodec)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Marshal(benchSession); err != nil {
			b.Fatal(err)
		}
	}
}
