package peercache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/arwenizEr/peercache/internal/codec"
	"github.com/arwenizEr/peercache/internal/core"
)

// DefaultTypedCodec is the codec a [TypedNamespace] encodes with when the
// namespace names none with [WithCodec].
//
// MessagePack, not JSON. The JSON codec replaces invalid UTF-8 in a string with
// U+FFFD and reports success (docs/decisions/open.md question 41), so a value
// whose string field carried arbitrary bytes would come back different and
// nothing would say so. A default must not do that; JSON remains available by
// name for anyone who wants values readable in a dump.
const DefaultTypedCodec = codec.NameMsgPack

// TypedNamespace is a [Namespace] whose values are T, encoded with the
// namespace's codec. Obtain one with [NamespaceOf].
//
// The operations, their errors and their guarantees are those of [Namespace]:
// the typed view encodes to bytes and everything below the value — routing,
// replication, the L1 tier, TTLs, the memory budget — sees the same bytes it
// always did. A missing key is the zero T and [ErrNotFound]. A value that will
// not encode, or stored bytes that will not decode as a T, is [ErrCodec].
//
// TypedNamespace is implemented by this module only and may gain methods in a minor
// release (docs/decisions/0042-what-v1-promises.md). A test that needs one
// should run a real cache with package peercachetest rather than fake this
// interface.
type TypedNamespace[T any] interface {
	// Get returns the value under key.
	Get(ctx context.Context, key string) (T, error)
	// Set stores val under key for ttl.
	Set(ctx context.Context, key string, val T, ttl time.Duration) error
	// Delete removes key.
	Delete(ctx context.Context, key string) error
	// Exists reports whether key is present.
	Exists(ctx context.Context, key string) (bool, error)
	// SetIfAbsent stores val under key for ttl unless a live value is
	// there, and reports whether it stored. See [Namespace.SetIfAbsent].
	//
	// There is no typed CompareAndSwap: equal values of T need not encode to
	// equal bytes — msgpack writes a map in iteration order — so a typed
	// comparison would be a byte comparison wearing a type. Use Raw and
	// compare bytes explicitly if that is what is wanted.
	SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error)
	// Touch gives key a new lifetime without changing its value. See
	// [Namespace.Touch].
	Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// BatchGet returns the values present among keys; absent keys are
	// omitted.
	BatchGet(ctx context.Context, keys []string) (map[string]T, error)
	// BatchGetOrLoad returns the values for keys, loading missing ones on
	// their owners. See [Namespace.BatchGetOrLoad].
	BatchGetOrLoad(ctx context.Context, keys []string) (map[string]T, error)
	// BatchSet stores every entry of kv for ttl.
	BatchSet(ctx context.Context, kv map[string]T, ttl time.Duration) error
	// GetOrLoad returns the value under key, loading it on the owner with
	// the namespace's loader if it is missing.
	GetOrLoad(ctx context.Context, key string) (T, error)
	// Raw returns the byte-valued namespace underneath.
	Raw() Namespace
}

// NamespaceOf returns the namespace called name, viewed as holding T.
//
// It is [Cache.Namespace] with an encoding: the same name gives the same
// namespace, and the first call's options win. Opening one name with two
// different types is reported as [ErrInvalidOption] by every operation on the
// later handle — the bytes stored under the name are one type's encoding, and
// decoding them as another would return garbage, or zero values, as success.
//
// A function rather than a method because Go interfaces cannot have type
// parameters.
func NamespaceOf[T any](c Cache, name string, opts ...NamespaceOption) TypedNamespace[T] {
	raw := c.Namespace(name, opts...)
	t := &typedNamespace[T]{raw: raw}

	ns, ok := raw.(*namespace)
	if !ok {
		// A Cache this package did not build — a test double, say. It
		// has no options to consult, so it gets the default codec and no
		// type checks.
		t.codec, t.err = codec.Get(DefaultTypedCodec)
		return t
	}
	if ns.err != nil {
		t.err = ns.err
		return t
	}
	t.codec = ns.valueCodec
	want := reflect.TypeFor[T]()
	if err := ns.bindValueType(want); err != nil {
		t.err = err
		return t
	}
	if spec := ns.opts.TypedLoader; spec != nil && spec.ValueType() != want {
		t.err = fmt.Errorf("peercache: namespace %q: its typed loader returns %v, not %v: %w",
			name, spec.ValueType(), want, ErrInvalidOption)
	}
	return t
}

// WithTypedLoader registers a loader that returns T, for [NamespaceOf]. It is
// [WithLoader] with the encoding done for you, and the two conflict: a
// namespace given both reports [ErrInvalidOption].
//
// Options are untyped, so the compiler cannot check that T matches the
// namespace it is given to. NamespaceOf does: a loader of one type on a
// namespace of another is [ErrInvalidOption].
func WithTypedLoader[T any](load func(ctx context.Context, key string) (T, time.Duration, error)) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.TypedLoader = typedLoader[T]{load: load} }
}

// typedLoader adapts a typed load function to the byte-valued [core.Loader]
// once the namespace's codec is known.
type typedLoader[T any] struct {
	load func(ctx context.Context, key string) (T, time.Duration, error)
}

func (l typedLoader[T]) ValueType() reflect.Type { return reflect.TypeFor[T]() }

func (l typedLoader[T]) Loader(encode func(any) ([]byte, error)) core.Loader {
	return func(ctx context.Context, key string) ([]byte, time.Duration, error) {
		v, ttl, err := l.load(ctx, key)
		if err != nil {
			return nil, 0, err
		}
		b, err := encode(v)
		if err != nil {
			return nil, 0, fmt.Errorf("peercache: encoding the loaded value for %q: %v: %w", key, err, ErrCodec)
		}
		return b, ttl, nil
	}
}

// typedNamespace implements [TypedNamespace].
type typedNamespace[T any] struct {
	raw   Namespace
	codec codec.Codec
	err   error
}

func (t *typedNamespace[T]) Raw() Namespace { return t.raw }

func (t *typedNamespace[T]) encode(key string, v T) ([]byte, error) {
	b, err := t.codec.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("peercache: encoding %q with %s: %v: %w", key, t.codec.Name(), err, ErrCodec)
	}
	return b, nil
}

func (t *typedNamespace[T]) decode(key string, b []byte) (T, error) {
	var v T
	if err := t.codec.Unmarshal(b, &v); err != nil {
		var zero T
		return zero, fmt.Errorf("peercache: decoding %q with %s: %v: %w", key, t.codec.Name(), err, ErrCodec)
	}
	return v, nil
}

func (t *typedNamespace[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	if t.err != nil {
		return zero, t.err
	}
	b, err := t.raw.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	return t.decode(key, b)
}

func (t *typedNamespace[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	if t.err != nil {
		return t.err
	}
	b, err := t.encode(key, val)
	if err != nil {
		return err
	}
	return t.raw.Set(ctx, key, b, ttl)
}

func (t *typedNamespace[T]) Delete(ctx context.Context, key string) error {
	if t.err != nil {
		return t.err
	}
	return t.raw.Delete(ctx, key)
}

func (t *typedNamespace[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	if t.err != nil {
		return false, t.err
	}
	b, err := t.encode(key, val)
	if err != nil {
		return false, err
	}
	return t.raw.SetIfAbsent(ctx, key, b, ttl)
}

func (t *typedNamespace[T]) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if t.err != nil {
		return false, t.err
	}
	return t.raw.Touch(ctx, key, ttl)
}

func (t *typedNamespace[T]) Exists(ctx context.Context, key string) (bool, error) {
	if t.err != nil {
		return false, t.err
	}
	return t.raw.Exists(ctx, key)
}

func (t *typedNamespace[T]) BatchGet(ctx context.Context, keys []string) (map[string]T, error) {
	if t.err != nil {
		return nil, t.err
	}
	raw, err := t.raw.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(raw))
	for key, b := range raw {
		v, err := t.decode(key, b)
		if err != nil {
			return nil, err
		}
		out[key] = v
	}
	return out, nil
}

func (t *typedNamespace[T]) BatchGetOrLoad(ctx context.Context, keys []string) (map[string]T, error) {
	if t.err != nil {
		return nil, t.err
	}
	raw, err := t.raw.BatchGetOrLoad(ctx, keys)
	out := make(map[string]T, len(raw))
	for key, b := range raw {
		v, derr := t.decode(key, b)
		if derr != nil {
			return out, errors.Join(err, derr)
		}
		out[key] = v
	}
	return out, err
}

func (t *typedNamespace[T]) BatchSet(ctx context.Context, kv map[string]T, ttl time.Duration) error {
	if t.err != nil {
		return t.err
	}
	// Encode everything before writing anything, so that one value that
	// will not encode does not leave the batch half written.
	raw := make(map[string][]byte, len(kv))
	for key, v := range kv {
		b, err := t.encode(key, v)
		if err != nil {
			return err
		}
		raw[key] = b
	}
	return t.raw.BatchSet(ctx, raw, ttl)
}

func (t *typedNamespace[T]) GetOrLoad(ctx context.Context, key string) (T, error) {
	var zero T
	if t.err != nil {
		return zero, t.err
	}
	b, err := t.raw.GetOrLoad(ctx, key)
	if err != nil {
		return zero, err
	}
	return t.decode(key, b)
}
