package tests

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// user is a value with every shape a codec has to get right: strings, numbers,
// bytes, a slice, a map, a nested struct, and a pointer that may be nil.
type user struct {
	ID      string            `json:"id"`
	Age     int               `json:"age"`
	Score   float64           `json:"score"`
	Avatar  []byte            `json:"avatar"`
	Tags    []string          `json:"tags"`
	Prefs   map[string]string `json:"prefs"`
	Address address           `json:"address"`
	Manager *address          `json:"manager"`
}

type address struct {
	City string `json:"city"`
	Zip  int    `json:"zip"`
}

type order struct {
	Total int `json:"total"`
}

func sampleUser(id string) user {
	return user{
		ID: id, Age: 41, Score: 9.75,
		Avatar:  []byte{0, 1, 2, 255},
		Tags:    []string{"admin", "beta"},
		Prefs:   map[string]string{"theme": "dark"},
		Address: address{City: "İzmir", Zip: 35000},
	}
}

// TestTypedValuesRoundTripAcrossNodes is the typed API's first criterion: a value set on
// one node reads back equal on every node, through Get and BatchGet.
func TestTypedValuesRoundTripAcrossNodes(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	open := func(id string) peercache.TypedNamespace[user] {
		return peercache.NamespaceOf[user](f.caches[id], "users",
			peercache.WithReplicationFactor(2), peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)

	want := map[string]user{}
	var keys []string
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("users/%d", i)
		want[key] = sampleUser(key)
		keys = append(keys, key)
		if err := open(f.ids[i%3]).Set(ctx, key, want[key], 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	for _, via := range f.ids {
		for key, w := range want {
			got, err := open(via).Get(ctx, key)
			if err != nil {
				t.Fatalf("Get %s via %s: %v", key, via, err)
			}
			if !reflect.DeepEqual(got, w) {
				t.Fatalf("Get %s via %s = %+v, want %+v", key, via, got, w)
			}
		}
		batch, err := open(via).BatchGet(ctx, append(keys, "users/absent"))
		if err != nil {
			t.Fatalf("BatchGet via %s: %v", via, err)
		}
		if !reflect.DeepEqual(batch, want) {
			t.Fatalf("BatchGet via %s returned %d entries that do not equal the %d written",
				via, len(batch), len(want))
		}
	}
}

// TestTypedBatchSetAndDelete covers the remaining writes.
func TestTypedBatchSetAndDelete(t *testing.T) {
	c, _ := newCache(t)
	ns := peercache.NamespaceOf[order](c, "orders")
	ctx := context.Background()

	if err := ns.BatchSet(ctx, map[string]order{"a": {1}, "b": {2}}, 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := ns.Exists(ctx, "a"); err != nil || !ok {
		t.Fatalf("Exists(a) = %v, %v", ok, err)
	}
	if err := ns.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Get(ctx, "a"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get after Delete: %v", err)
	}
	if got, err := ns.Get(ctx, "b"); err != nil || got.Total != 2 {
		t.Fatalf("Get(b) = %+v, %v", got, err)
	}
}

// TestTypedGetOrLoadAcrossNodes: a typed loader runs on the owner, its value
// crosses the hop encoded, and the caller receives a T.
func TestTypedGetOrLoadAcrossNodes(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	var loads atomic.Int64
	loader := peercache.WithTypedLoader(func(_ context.Context, key string) (user, time.Duration, error) {
		loads.Add(1)
		return sampleUser(key), 0, nil
	})
	open := func(id string) peercache.TypedNamespace[user] {
		return peercache.NamespaceOf[user](f.caches[id], "loaded", loader, peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)

	via := f.ids[0]
	key := ""
	for i := 0; i < 100 && key == ""; i++ {
		if k := fmt.Sprintf("loaded/%d", i); f.ownerOf(t, k) != via {
			key = k
		}
	}
	for i := 0; i < 3; i++ {
		got, err := open(via).GetOrLoad(ctx, key)
		if err != nil {
			t.Fatalf("GetOrLoad %s via %s: %v", key, via, err)
		}
		if !reflect.DeepEqual(got, sampleUser(key)) {
			t.Fatalf("GetOrLoad %s = %+v", key, got)
		}
	}
	if n := loads.Load(); n != 1 {
		t.Fatalf("the typed loader ran %d times for one key; the owner should have kept it", n)
	}
}

// TestInvalidUTF8SurvivesTheDefaultCodec is why the default is msgpack. JSON
// replaces invalid UTF-8 in a string with U+FFFD and reports success (open
// question 41); a default codec must not do that.
func TestInvalidUTF8SurvivesTheDefaultCodec(t *testing.T) {
	type blob struct{ S string }
	c, _ := newCache(t)
	ctx := context.Background()
	in := blob{S: "\xff\xfe valid after"}

	def := peercache.NamespaceOf[blob](c, "bytes-in-a-string")
	if err := def.Set(ctx, "k", in, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := def.Get(ctx, "k"); err != nil || got != in {
		t.Fatalf("under the default codec %q came back as %q (%v)", in.S, got.S, err)
	}

	// And JSON, named explicitly, still does what question 41 says. Pinned so
	// that a change in either direction is noticed.
	js := peercache.NamespaceOf[blob](c, "json", peercache.WithCodec("json"))
	if err := js.Set(ctx, "k", in, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := js.Get(ctx, "k"); err != nil || got == in {
		t.Fatalf("JSON round-tripped invalid UTF-8 intact (%q, %v); question 41 has changed — update it", got.S, err)
	}
}

// TestOneNameTwoTypesIsInvalid: one name, one store, one encoding. A second
// handle decoding the same bytes as a different type would return garbage, or
// zero values, as success.
func TestOneNameTwoTypesIsInvalid(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()
	users := peercache.NamespaceOf[user](c, "shared")
	if err := users.Set(ctx, "k", sampleUser("k"), 0); err != nil {
		t.Fatal(err)
	}

	orders := peercache.NamespaceOf[order](c, "shared")
	if _, err := orders.Get(ctx, "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("a second type on the same name returned %v, want ErrInvalidOption", err)
	}
	if err := orders.Set(ctx, "k", order{1}, 0); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("Set through the mistyped handle returned %v, want ErrInvalidOption", err)
	}
	// The first handle is unaffected.
	if got, err := users.Get(ctx, "k"); err != nil || got.ID != "k" {
		t.Fatalf("the original handle broke: %+v, %v", got, err)
	}
	// Opening it again with the right type is fine.
	if _, err := peercache.NamespaceOf[user](c, "shared").Get(ctx, "k"); err != nil {
		t.Fatalf("reopening with the same type: %v", err)
	}
}

// TestATypedLoaderOfTheWrongTypeIsInvalid catches WithTypedLoader[A] on a
// NamespaceOf[B], which the compiler cannot: options are untyped.
func TestATypedLoaderOfTheWrongTypeIsInvalid(t *testing.T) {
	c, _ := newCache(t)
	ns := peercache.NamespaceOf[order](c, "wrong-loader",
		peercache.WithTypedLoader(func(context.Context, string) (user, time.Duration, error) {
			return user{}, 0, nil
		}))
	if _, err := ns.GetOrLoad(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("a user loader on an order namespace returned %v, want ErrInvalidOption", err)
	}
}

// TestTypedLoaderAndLoaderConflict: two loaders for one namespace is a mistake.
func TestTypedLoaderAndLoaderConflict(t *testing.T) {
	c, _ := newCache(t)
	ns := peercache.NamespaceOf[order](c, "two-loaders",
		peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) { return nil, 0, nil }),
		peercache.WithTypedLoader(func(context.Context, string) (order, time.Duration, error) { return order{}, 0, nil }))
	if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("WithLoader and WithTypedLoader together returned %v, want ErrInvalidOption", err)
	}
}

// TestAnUndecodableValueIsErrCodec: bytes that are not a T — written through
// Raw, or by a pod running another build — are reported, not zero-filled.
func TestAnUndecodableValueIsErrCodec(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()
	ns := peercache.NamespaceOf[order](c, "garbled")
	if err := ns.Raw().Set(ctx, "k", []byte{0xc1, 0xc1, 0xc1}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrCodec) {
		t.Fatalf("Get of bytes that are not an order returned %v, want ErrCodec", err)
	}
	if _, err := ns.BatchGet(ctx, []string{"k"}); !errors.Is(err, peercache.ErrCodec) {
		t.Fatalf("BatchGet of bytes that are not an order returned %v, want ErrCodec", err)
	}
}

// TestAValueThatWillNotEncodeIsErrCodec.
func TestAValueThatWillNotEncodeIsErrCodec(t *testing.T) {
	type bad struct{ C chan int }
	c, _ := newCache(t)
	ns := peercache.NamespaceOf[bad](c, "unencodable", peercache.WithCodec("json"))
	if err := ns.Set(context.Background(), "k", bad{C: make(chan int)}, 0); !errors.Is(err, peercache.ErrCodec) {
		t.Fatalf("Set of a value JSON cannot encode returned %v, want ErrCodec", err)
	}
}

// TestAMissingTypedKeyIsZeroAndNotFound.
func TestAMissingTypedKeyIsZeroAndNotFound(t *testing.T) {
	c, _ := newCache(t)
	got, err := peercache.NamespaceOf[user](c, "empty").Get(context.Background(), "nope")
	if !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get of a missing key returned %v, want ErrNotFound", err)
	}
	if !reflect.DeepEqual(got, user{}) {
		t.Fatalf("Get of a missing key returned %+v alongside the error, want the zero value", got)
	}
}

// TestTypedSetKeepsItsTTL: the typed view adds encoding and nothing else.
func TestTypedSetKeepsItsTTL(t *testing.T) {
	c, fc := newCache(t)
	ns := peercache.NamespaceOf[order](c, "ttl")
	if err := ns.Set(context.Background(), "k", order{7}, time.Second); err != nil {
		t.Fatal(err)
	}
	fc.Advance(2 * time.Second)
	if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a typed key with a 1s TTL is still served after 2s: %v", err)
	}
}
