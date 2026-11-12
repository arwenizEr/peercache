package peercache

import (
	"context"
	"fmt"
	"reflect"

	"github.com/arwenizEr/peercache/internal/codec"
)

// typedSignature is what a typed group function takes and returns, recorded at
// registration so that a call naming other types is refused rather than
// attempted.
type typedSignature struct {
	arg, result reflect.Type
}

// RegisterTypedGroupFunc registers fn under name, with its argument and result
// encoded by [DefaultTypedCodec]. It is [Cache.RegisterGroupFunc] with the
// encoding done for you, and shares its rules: registration happens once, at
// startup, identically on every pod, and a name taken by any function — typed
// or not — is [ErrDuplicateRegistration].
//
// Only the argument and result are typed. [GroupState] stays byte-valued: a
// group's keys are the function's own business, and a typed state would need a
// type per key.
//
// Inside the function's turn, an argument that does not decode as A is
// [ErrCodec], and so is a result that does not encode. An error fn returns is
// returned to the caller as it is.
func RegisterTypedGroupFunc[A, R any](c Cache, name string,
	fn func(ctx context.Context, s GroupState, arg A) (R, error)) error {
	enc, err := codec.Get(DefaultTypedCodec)
	if err != nil {
		return err
	}
	wrapped := func(ctx context.Context, s GroupState, raw []byte) ([]byte, error) {
		var arg A
		if err := enc.Unmarshal(raw, &arg); err != nil {
			return nil, fmt.Errorf("peercache: group function %q: decoding its argument as %v: %v: %w",
				name, reflect.TypeFor[A](), err, ErrCodec)
		}
		result, err := fn(ctx, s, arg)
		if err != nil {
			return nil, err
		}
		out, err := enc.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("peercache: group function %q: encoding its result: %v: %w", name, err, ErrCodec)
		}
		return out, nil
	}
	if err := c.RegisterGroupFunc(name, wrapped); err != nil {
		return err
	}
	if cc, ok := c.(*cache); ok {
		cc.typedFuncs.Store(name, typedSignature{arg: reflect.TypeFor[A](), result: reflect.TypeFor[R]()})
	}
	return nil
}

// CallTyped invokes the typed group function name on g with arg, and returns
// its result as R. It is [Group.Call] with the encoding done for you.
//
// Every pod runs the same binary, so this node's registration says what the
// function takes and returns. A function not registered with
// [RegisterTypedGroupFunc], or registered with other types, is
// [ErrInvalidOption] before anything is sent. A result that does not decode as R
// is [ErrCodec]. Otherwise the errors are those of Group.Call.
//
// A function rather than a method because Go methods cannot have type
// parameters.
func CallTyped[A, R any](ctx context.Context, g Group, name string, arg A) (R, error) {
	var zero R
	if gg, ok := g.(*group); ok {
		v, found := gg.cache.typedFuncs.Load(name)
		if !found {
			return zero, fmt.Errorf("peercache: group function %q is not registered as typed on this node: %w",
				name, ErrInvalidOption)
		}
		sig := v.(typedSignature)
		if sig.arg != reflect.TypeFor[A]() || sig.result != reflect.TypeFor[R]() {
			return zero, fmt.Errorf("peercache: group function %q takes %v and returns %v; called with %v and %v: %w",
				name, sig.arg, sig.result, reflect.TypeFor[A](), reflect.TypeFor[R](), ErrInvalidOption)
		}
	}
	enc, err := codec.Get(DefaultTypedCodec)
	if err != nil {
		return zero, err
	}
	raw, err := enc.Marshal(arg)
	if err != nil {
		return zero, fmt.Errorf("peercache: group function %q: encoding the argument: %v: %w", name, err, ErrCodec)
	}
	out, err := g.Call(ctx, name, raw)
	if err != nil {
		return zero, err
	}
	var result R
	if err := enc.Unmarshal(out, &result); err != nil {
		return zero, fmt.Errorf("peercache: group function %q: decoding its result as %v: %v: %w",
			name, reflect.TypeFor[R](), err, ErrCodec)
	}
	return result, nil
}
