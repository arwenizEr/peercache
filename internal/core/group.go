package core

import (
	"context"
	"reflect"
	"strings"
	"time"
)

// GroupState is the actor-turn view of a group's memory.
//
// The methods take no context and return no error because they are plain local
// map access executing on the group's single activation goroutine. That is the
// entire payoff of moving computation to the data.
//
// A GroupState is valid only for the duration of one turn. Retaining it and
// using it later is a use-after-free in all but name. The implementation
// carries a generation counter, and a method called on a stale state does
// nothing and increments Stats().StateEscapes.
//
// It is deliberately not an error return. The goroutine holding an escaped
// state is not inside a Call, so there is no caller to return one to — see
// internal/actor/state.go. [ErrStateEscaped] exists for callers that matched on
// it and is never returned by this package; the counter is the signal.
type GroupState interface {
	ID() string
	Get(key string) ([]byte, bool)
	Set(key string, val []byte)
	Delete(key string)
	Keys() []string

	// ScheduleTimer arms a named timer that, when it fires, invokes the
	// registered function fn with arg as another turn on this group.
	// Re-arming an existing name replaces it.
	ScheduleTimer(name string, after time.Duration, fn string, arg []byte)

	// CancelTimer cancels a named timer. Cancelling an unknown name is a
	// no-op.
	CancelTimer(name string)

	// Emit publishes a lifecycle event. Delivery is best effort.
	Emit(event Event)
}

// GroupFunc is a function registered by name and executed on the group's owner.
// Closures never cross the network: an
// RPC carries the registered name and opaque argument bytes.
//
// The function runs inside the group's turn, so it has exclusive access to s.
// It must respect ctx: the caller's deadline is propagated into the turn, and a
// function that ignores it holds the group's single goroutine hostage.
type GroupFunc func(ctx context.Context, s GroupState, arg []byte) ([]byte, error)

// Loader reconstructs a namespace value that is not resident.
//
// It is registered on the namespace at construction rather than passed to
// GetOrLoad, so that it exists by name on every pod and the owner can execute
// it locally. Concurrent misses for one key collapse into a single invocation
// via singleflight.
//
// The returned ttl is applied exactly as Set would apply it: a non-positive
// value means the loaded value does not expire on its own.
type Loader func(ctx context.Context, key string) (val []byte, ttl time.Duration, err error)

// TypedLoaderSpec is a loader that returns a typed value, held in namespace
// options until the namespace's codec is known.
//
// It exists because core does not know about codecs, and an option cannot know
// which codec it will end up encoding with: WithCodec may come after it. The
// namespace builds the byte-valued Loader from it once all options are applied.
type TypedLoaderSpec interface {
	// ValueType is the type the loader returns.
	ValueType() reflect.Type
	// Loader returns the byte-valued loader, encoding with encode.
	Loader(encode func(any) ([]byte, error)) Loader
}

// Partitioner maps a cache key to the group key that decides its owner.
//
// It must be pure and identical on every pod: two nodes disagreeing about a
// key's group is indistinguishable from a split brain.
type Partitioner interface {
	GroupKey(key string) string
}

// PartitionerFunc adapts a function to [Partitioner].
type PartitionerFunc func(key string) string

// GroupKey implements [Partitioner].
func (f PartitionerFunc) GroupKey(key string) string { return f(key) }

// PartitionByKey makes every key its own group. This is the default: maximum
// spread, no batch affinity.
func PartitionByKey() Partitioner {
	return PartitionerFunc(func(key string) string { return key })
}

// PartitionByPrefix groups keys by the portion before the first occurrence of
// sep, so "user:123/cart" and "user:123/profile" share an owner.
//
// A key that does not contain sep is its own group. An empty sep would make
// every key its own group by accident, so it is treated as [PartitionByKey].
func PartitionByPrefix(sep string) Partitioner {
	if sep == "" {
		return PartitionByKey()
	}
	return PartitionerFunc(func(key string) string {
		if i := strings.Index(key, sep); i >= 0 {
			return key[:i]
		}
		return key
	})
}

// PartitionByFunc groups keys with an arbitrary pure function. A nil function
// falls back to [PartitionByKey] rather than failing at the first Get.
func PartitionByFunc(f func(string) string) Partitioner {
	if f == nil {
		return PartitionByKey()
	}
	return PartitionerFunc(f)
}
