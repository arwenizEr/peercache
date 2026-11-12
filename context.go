package peercache

import "context"

// ctxOrBackground replaces a nil context with [context.Background].
//
// # Why this exists
//
// Every context-taking method in this package starts by asking the context
// something — usually ctx.Err(), to report an already-expired deadline ahead of
// doing any work. [context.Context] is an interface, so a nil one makes that
// first question a nil-pointer dereference: the library panics inside the
// caller's goroutine, with a stack that points at peercache rather than at the
// call site that passed nil.
//
// The library does not panic, and that rule carves out no exception for
// caller error. A panic crossing an API boundary takes down a
// process that could have carried on, and an embedded library shares that
// process with an application that has its own opinion about how to fail.
//
// # Why Background rather than an error
//
// A nil context carries no deadline, no cancellation and no values, which is
// exactly what [context.Background] is. Treating the two as the same thing
// makes the call do what its author plainly meant.
//
// The alternative — returning a typed error — was rejected because it converts
// a harmless call into a failure that every caller must now handle, on a path
// where there is nothing to handle: the operation was going to succeed. It
// would also make Close(nil) a no-op that leaves goroutines running, which
// turns a cosmetic mistake into a leak.
//
// This is not an endorsement. Go's own guidance is not to pass nil; a caller
// that does has a bug. But the bug is theirs to find, and a returned error or a
// clean shutdown finds it just as well as a panic and costs a great deal less.
// See docs/decisions/open.md question 34.
func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
