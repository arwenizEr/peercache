// Package store holds the data this node is responsible for.
//
// [Store] is the owned store: keys this node owns, held in memory and served
// authoritatively. It is sharded — 256 shards by default, selected by xxhash
// over the key, one RWMutex each — so that unrelated keys do not contend.
// Values are opaque bytes.
//
// # Expiry
//
// A key's deadline is computed from the injected clock at Set and enforced on
// every read. Reads are authoritative: an expired key is never served, whatever
// the sweeper is doing. The background sweeper exists only to reclaim memory
// from keys nobody reads any more, and it works incrementally — a few shards
// per tick — so that expiry never turns into a periodic stall proportional to
// the size of the keyspace.
//
// That split matters. If the sweeper were authoritative, correctness would
// depend on it having run recently, which is the same shape of mistake as
// depending on an invalidation message arriving.
//
// # Aliasing
//
// [Store.Set] copies the value it is given, so a caller may reuse its buffer.
// [Store.Get] returns the stored slice without copying, so a caller must not
// modify it. The asymmetry is deliberate and is what makes a sub-microsecond
// read possible; see docs/decisions/0007-get-returns-the-stored-slice.md for
// the measurement behind it.
//
// # Durability
//
// None. Memory only, by decision (CLAUDE.md, decision 4): graceful shutdown
// transfers state to the new owner, and ungraceful termination loses it. Every
// namespace must therefore either be reconstructible from a loader or tolerate
// loss.
//
// The replica store, which holds copies of keys owned elsewhere, arrives in M5.
package store
