// Package core holds the type definitions shared by the public peercache
// package and every internal implementation package.
//
// # Why this package exists
//
// SPEC.md §3 defines the public API in the root package. Several of those
// definitions are concrete types — [Node], [Event], [Epoch] — that appear in the
// signatures of interfaces the internal packages must implement. Go's structural
// interface satisfaction does not help there: an internal package cannot declare
// its own compatible Transport, because Dial takes a named struct type, and two
// identically shaped structs in different packages are distinct types.
//
// If the root package owned those definitions, then internal/transport,
// internal/membership and internal/actor would all have to import the root
// package, while the root package's own constructor imports them back. That is
// an import cycle, and it appears the moment M2 begins.
//
// So the definitions live here, and the root package re-exports them as type
// aliases. An alias is not a wrapper: peercache.Node and core.Node are the same
// type, so the public API is exactly what SPEC.md §3 specifies and callers can
// never tell the difference. The rule this buys is worth stating plainly:
//
//	No package under internal/ may import the root peercache package.
//
// The full public documentation lives on the aliases in the root package, which
// is the surface users read. Comments here stay short and describe invariants
// rather than usage. See docs/decisions/0001-core-types-location.md.
package core
