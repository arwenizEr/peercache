package peercache_test

import (
	"strings"
	"testing"

	"github.com/arwenizEr/peercache"
)

// FuzzPartitionByPrefix asserts the four properties routing depends on.
//
// The partitioner is not a formatting helper. Its output is the group key, and
// the group key is what [peercache.Cache] hashes to choose an owner, what the
// router batches by, and what the actor runtime serializes on. A key whose group
// is computed differently on two nodes is a key with two owners, which is the
// one failure mode this library says it prevents.
//
// The properties, and what each one protects:
//
//   - The group is a prefix of the key. Anything else means the partitioner is
//     synthesising a group key rather than selecting one, and a group that is
//     not a substring of some key cannot be reasoned about from a key alone.
//   - Grouping is idempotent. `groupKeyFor` is applied wherever a group is
//     needed, and nothing tracks whether a given string has been through it
//     already. If GroupKey(GroupKey(k)) differed from GroupKey(k), applying it
//     twice would move a key to another owner.
//   - It is deterministic across instances. Two nodes construct their own
//     partitioners from the same option; they must agree.
//   - It never panics. CLAUDE.md forbids panics in library paths, and this one
//     runs on every routed operation with a key the application chose.
//
// Run longer with:
//
//	go test . -run '^$' -fuzz '^FuzzPartitionByPrefix$' -fuzztime 60s
func FuzzPartitionByPrefix(f *testing.F) {
	f.Add("user:123/cart", "/")
	f.Add("user:123", "/")
	f.Add("", "/")
	// An empty separator is documented as falling back to PartitionByKey.
	f.Add("user:123/cart", "")
	// A separator at the very start and at the very end.
	f.Add("/leading", "/")
	f.Add("trailing/", "/")
	// A multi-byte separator, and one longer than the key.
	f.Add("a::b::c", "::")
	f.Add("ab", "abcdef")
	// Bytes that are not valid UTF-8, which a key is allowed to contain.
	f.Add("\xff\xfe/rest", "\xff")
	// A separator that is itself the whole key.
	f.Add("/", "/")

	f.Fuzz(func(t *testing.T, key, sep string) {
		p := peercache.PartitionByPrefix(sep)
		group := p.GroupKey(key)

		if !strings.HasPrefix(key, group) {
			t.Fatalf("GroupKey(%q) with sep %q = %q, which is not a prefix of the key",
				key, sep, group)
		}
		if again := p.GroupKey(group); again != group {
			t.Fatalf("grouping is not idempotent: GroupKey(%q) = %q, GroupKey(%q) = %q",
				key, group, group, again)
		}
		if other := peercache.PartitionByPrefix(sep).GroupKey(key); other != group {
			t.Fatalf("two partitioners built from sep %q disagree about %q: %q and %q",
				sep, key, group, other)
		}

		if sep == "" {
			// Documented: an empty separator would make every key its
			// own group by accident, so it is PartitionByKey instead.
			if group != key {
				t.Fatalf("an empty separator must be the identity, but %q became %q",
					key, group)
			}
			return
		}
		// The group is everything before the first separator, so it cannot
		// contain one. This is what makes the idempotence above hold rather
		// than being a coincidence of the current implementation, and it is
		// checked separately so that a regression says which of the two
		// broke.
		if strings.Contains(group, sep) {
			t.Fatalf("GroupKey(%q) with sep %q = %q, which still contains the separator",
				key, sep, group)
		}
		if strings.Contains(key, sep) {
			if want := key[:strings.Index(key, sep)]; group != want {
				t.Fatalf("GroupKey(%q) with sep %q = %q, want everything before "+
					"the first separator, %q", key, sep, group, want)
			}
		} else if group != key {
			t.Fatalf("a key with no separator is its own group, but %q became %q",
				key, group)
		}
	})
}
