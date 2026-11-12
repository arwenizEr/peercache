package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/peercache/peercache"
)

type cartItem struct {
	SKU   string `json:"sku"`
	Count int    `json:"count"`
}

type cart struct {
	Items map[string]int `json:"items"`
	Total int            `json:"total"`
}

var errOutOfStock = errors.New("out of stock")

// addToCart is the typed function under test: it keeps a cart in the group's
// state and returns it. The state stays []byte — only the argument and result
// are typed.
func addToCart(_ context.Context, s peercache.GroupState, item cartItem) (cart, error) {
	if item.SKU == "sold-out" {
		return cart{}, errOutOfStock
	}
	var c cart
	if raw, ok := s.Get("cart"); ok {
		if err := decodeCart(raw, &c); err != nil {
			return cart{}, err
		}
	}
	if c.Items == nil {
		c.Items = map[string]int{}
	}
	c.Items[item.SKU] += item.Count
	c.Total += item.Count
	s.Set("cart", encodeCart(c))
	return c, nil
}

// The state is stored in a trivial format of the test's own; how a function
// encodes its state is its business.
func encodeCart(c cart) []byte { return []byte(fmt.Sprint(c.Total)) }
func decodeCart(b []byte, c *cart) error {
	_, err := fmt.Sscan(string(b), &c.Total)
	return err
}

func registerCart(t *testing.T, c peercache.Cache) {
	t.Helper()
	if err := peercache.RegisterTypedGroupFunc(c, "addToCart", addToCart); err != nil {
		t.Fatalf("RegisterTypedGroupFunc: %v", err)
	}
}

// TestATypedGroupFuncRunsOnTheOwnerAcrossNodes: argument and result cross the
// network encoded and arrive as the types both sides named.
func TestATypedGroupFuncRunsOnTheOwnerAcrossNodes(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		registerCart(t, f.caches[id])
	}
	f.settle(t)

	via := f.ids[0]
	group := ""
	for i := 0; i < 100 && group == ""; i++ {
		if g := fmt.Sprintf("user:%d", i); f.ownerOf(t, g) != via {
			group = g
		}
	}

	ctx := context.Background()
	var got cart
	var err error
	for i, item := range []cartItem{{"apple", 2}, {"pear", 1}, {"apple", 3}} {
		got, err = peercache.CallTyped[cartItem, cart](ctx, f.caches[via].Group(group), "addToCart", item)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	// Items is rebuilt per call from the stored total only, so the last call
	// reports one SKU; the total is what persisted across calls.
	if got.Total != 6 || !reflect.DeepEqual(got.Items, map[string]int{"apple": 3}) {
		t.Fatalf("after three calls the cart is %+v", got)
	}
}

// TestATypedGroupFuncReturnsTheFunctionsError: an error from the function is
// the caller's error, not a decode failure.
func TestATypedGroupFuncReturnsTheFunctionsError(t *testing.T) {
	c, _ := newCache(t)
	registerCart(t, c)
	_, err := peercache.CallTyped[cartItem, cart](context.Background(), c.Group("g"), "addToCart", cartItem{SKU: "sold-out"})
	if !errors.Is(err, errOutOfStock) {
		t.Fatalf("the function's error came back as %v", err)
	}
}

// TestCallTypedWithTheWrongTypesIsInvalid: every pod runs the same binary, so
// the local registration says what the function takes and returns, and a call
// naming other types is a mistake to report, not a decode to attempt.
func TestCallTypedWithTheWrongTypesIsInvalid(t *testing.T) {
	c, _ := newCache(t)
	registerCart(t, c)
	ctx := context.Background()

	if _, err := peercache.CallTyped[string, cart](ctx, c.Group("g"), "addToCart", "apple"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("a call with the wrong argument type returned %v, want ErrInvalidOption", err)
	}
	if _, err := peercache.CallTyped[cartItem, int](ctx, c.Group("g"), "addToCart", cartItem{"a", 1}); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("a call with the wrong result type returned %v, want ErrInvalidOption", err)
	}

	// A function registered untyped is not callable typed.
	if err := c.RegisterGroupFunc("plain", func(context.Context, peercache.GroupState, []byte) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := peercache.CallTyped[cartItem, cart](ctx, c.Group("g"), "plain", cartItem{}); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("CallTyped on an untyped function returned %v, want ErrInvalidOption", err)
	}
}

// TestATypedGroupFuncRejectsAnUndecodableArgument: a raw Call with bytes that
// are not the function's argument type is ErrCodec, across the hop.
func TestATypedGroupFuncRejectsAnUndecodableArgument(t *testing.T) {
	f := newFabric(t, 2)
	for _, id := range f.ids {
		registerCart(t, f.caches[id])
	}
	f.settle(t)
	via := f.ids[0]
	group := ""
	for i := 0; i < 100 && group == ""; i++ {
		if g := fmt.Sprintf("user:%d", i); f.ownerOf(t, g) != via {
			group = g
		}
	}
	_, err := f.caches[via].Group(group).Call(context.Background(), "addToCart", []byte{0xc1, 0xc1})
	if !errors.Is(err, peercache.ErrCodec) {
		t.Fatalf("a raw call with garbage to a typed function returned %v, want ErrCodec", err)
	}
}

// TestTypedRegistrationIsStillUnique: one name, one function, typed or not.
func TestTypedRegistrationIsStillUnique(t *testing.T) {
	c, _ := newCache(t)
	registerCart(t, c)
	if err := peercache.RegisterTypedGroupFunc(c, "addToCart", addToCart); !errors.Is(err, peercache.ErrDuplicateRegistration) {
		t.Fatalf("a second typed registration returned %v, want ErrDuplicateRegistration", err)
	}
	if err := c.RegisterGroupFunc("addToCart", func(context.Context, peercache.GroupState, []byte) ([]byte, error) {
		return nil, nil
	}); !errors.Is(err, peercache.ErrDuplicateRegistration) {
		t.Fatalf("an untyped registration over a typed one returned %v, want ErrDuplicateRegistration", err)
	}
}
