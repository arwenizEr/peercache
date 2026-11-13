package persisttest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache/persist"
)

// Run executes the contract against stores made by newStore.
func Run(t *testing.T, newStore func(t *testing.T) persist.BlobStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("PutThenGet", func(t *testing.T) {
		s := newStore(t)
		if err := s.Put(ctx, "ns/p000/seg-1-n0.bin", []byte("hello")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := s.Get(ctx, "ns/p000/seg-1-n0.bin")
		if err != nil || string(got) != "hello" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("EmptyObject", func(t *testing.T) {
		s := newStore(t)
		if err := s.Put(ctx, "empty", nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := s.Get(ctx, "empty")
		if err != nil || len(got) != 0 {
			t.Fatalf("Get of an empty object = %q, %v", got, err)
		}
	})

	t.Run("MissingIsErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Get(ctx, "absent"); !errors.Is(err, persist.ErrNotFound) {
			t.Fatalf("Get of an absent object = %v, want ErrNotFound", err)
		}
	})

	t.Run("PutReplaces", func(t *testing.T) {
		s := newStore(t)
		for _, v := range []string{"first", "second, longer than the first", "3"} {
			if err := s.Put(ctx, "k", []byte(v)); err != nil {
				t.Fatalf("Put(%q): %v", v, err)
			}
		}
		if got, _ := s.Get(ctx, "k"); string(got) != "3" {
			t.Fatalf("after three Puts Get = %q, want the last", got)
		}
	})

	t.Run("CallerSlicesAreNotShared", func(t *testing.T) {
		s := newStore(t)
		in := []byte("original")
		if err := s.Put(ctx, "k", in); err != nil {
			t.Fatal(err)
		}
		copy(in, "MUTATED!")
		out, _ := s.Get(ctx, "k")
		if string(out) != "original" {
			t.Fatalf("changing the slice passed to Put changed the object: %q", out)
		}
		copy(out, "MUTATED!")
		if again, _ := s.Get(ctx, "k"); string(again) != "original" {
			t.Fatalf("changing the slice Get returned changed the object: %q", again)
		}
	})

	t.Run("ListIsSortedAndFiltered", func(t *testing.T) {
		s := newStore(t)
		names := []string{"a/p001/x", "a/p000/z", "a/p000/y", "b/p000/x", "a/p0000"}
		for _, n := range names {
			if err := s.Put(ctx, n, []byte(n)); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.List(ctx, "a/p000/")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		want := []string{"a/p000/y", "a/p000/z"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("List(a/p000/) = %v, want %v", got, want)
		}
		all, _ := s.List(ctx, "")
		if !sort.StringsAreSorted(all) || len(all) != len(names) {
			t.Fatalf("List(\"\") = %v; want all %d names, sorted", all, len(names))
		}
	})

	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		s := newStore(t)
		_ = s.Put(ctx, "k", []byte("v"))
		for i := 0; i < 2; i++ {
			if err := s.Delete(ctx, "k"); err != nil {
				t.Fatalf("Delete #%d: %v", i+1, err)
			}
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, persist.ErrNotFound) {
			t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
		}
		if names, _ := s.List(ctx, ""); len(names) != 0 {
			t.Fatalf("List after Delete = %v", names)
		}
	})

	t.Run("CancelledContextIsHonoured", func(t *testing.T) {
		s := newStore(t)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := s.Put(cctx, "k", []byte("v")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put on a cancelled context = %v", err)
		}
		if _, err := s.Get(cctx, "k"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Get on a cancelled context = %v", err)
		}
		if _, err := s.List(cctx, ""); !errors.Is(err, context.Canceled) {
			t.Fatalf("List on a cancelled context = %v", err)
		}
		if err := s.Delete(cctx, "k"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Delete on a cancelled context = %v", err)
		}
	})

	// A reader racing a writer sees one whole object or the other, never a
	// mixture. Run under -race, it also checks the backend's locking.
	t.Run("ConcurrentPutsAreAtomic", func(t *testing.T) {
		s := newStore(t)
		a := bytes.Repeat([]byte{'a'}, 64<<10)
		b := bytes.Repeat([]byte{'b'}, 32<<10)
		if err := s.Put(ctx, "k", a); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					v := a
					if (i+w)%2 == 0 {
						v = b
					}
					if err := s.Put(ctx, "k", v); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
				}
			}(w)
		}
		for i := 0; i < 200; i++ {
			got, err := s.Get(ctx, "k")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(got, a) && !bytes.Equal(got, b) {
				t.Fatalf("Get returned %d bytes that are neither object", len(got))
			}
		}
		wg.Wait()
	})
}
