// Package persisttest is the contract every persist.BlobStore must meet.
//
// A backend module runs it from its own tests:
//
//	func TestContract(t *testing.T) {
//		persisttest.Run(t, func(t *testing.T) persist.BlobStore { return newStore(t) })
//	}
//
// Each subtest gets a fresh, empty store from the factory. The suite checks what
// the cache relies on — atomic replacement, isolation from the caller's slices,
// sorted prefix listing, idempotent deletion, honoured contexts — and nothing a
// backend could reasonably do differently.
package persisttest
