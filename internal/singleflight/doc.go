// Package singleflight collapses concurrent loads of the same key into one.
//
// A thousand pods' worth of traffic arriving on one cold key must not become a
// thousand database queries. [Group] guarantees that at most one load per key
// runs at a time, and that every concurrent caller receives that load's result.
//
// # Why this is not golang.org/x/sync/singleflight
//
// Two reasons, in order of weight:
//
//  1. That package re-panics in every waiter when the shared call panics. A
//     loader is application code; a bug in it must not become a panic storm in
//     unrelated goroutines. Here a panicking loader becomes [core.ErrLoaderPanic]
//     for everyone who was waiting, which is a result they can handle.
//  2. The loader has to run under a context detached from whichever caller
//     happened to arrive first. Otherwise the leader's deadline silently
//     becomes everyone's deadline, and a caller with 5ms left can abort a load
//     that nine hundred callers with a second left were depending on.
//
// See docs/decisions/0006-hand-rolled-singleflight.md.
package singleflight
