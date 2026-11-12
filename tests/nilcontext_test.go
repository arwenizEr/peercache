//lint:file-ignore SA1012 Passing a nil context is the subject of these tests.
// SA1012 says not to pass a nil Context, and it is right — that is why this
// file exists. Callers do it anyway, and once every one of them crashed the
// process. Suppressing the check here is not disagreement with it; it is the
// only way to test what happens when somebody ignores it.

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// TestNilContextDoesNotPanic covers every exported method that takes a context.
//
// This was found by accident, by a throwaway program that wrote
// `defer c.Close(nil)`. The process died inside the actor runtime, on the
// ctx.Err() that every one of these methods starts with — a nil
// [context.Context] is a nil interface, so the first question asked of it is a
// nil-pointer dereference.
//
// Library code paths never panic, and the rule makes no exception for
// caller error, which is the right call for something embedded: a panic here
// takes down an application that had a working cache and a cosmetic mistake.
//
// The list below is deliberately exhaustive rather than representative. A guard
// applied to seventeen of eighteen entry points is not a guard, and the missing
// one would be found the same way this was — in production, by whoever wrote
// the eighteenth call.
func TestNilContextDoesNotPanic(t *testing.T) {
	newCache := func(t *testing.T) peercache.Cache {
		t.Helper()
		c, err := peercache.New(peercache.WithNodeID("nilctx"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return c
	}

	// Cache-level methods, each on its own cache because two of them shut it
	// down and the rest would then be testing ErrClosed instead.
	cacheOps := map[string]func(peercache.Cache) error{
		"CatchUp":   func(c peercache.Cache) error { return c.CatchUp(nil) },
		"Drain":     func(c peercache.Cache) error { return c.Drain(nil) },
		"WaitReady": func(c peercache.Cache) error { return c.WaitReady(nil) },
		"Close":     func(c peercache.Cache) error { return c.Close(nil) },
	}
	for name, op := range cacheOps {
		t.Run("Cache."+name, func(t *testing.T) {
			c := newCache(t)
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Cache.%s(nil) panicked: %v", name, r)
				}
				_ = c.Close(context.Background())
			}()
			// The error is not asserted on: some of these legitimately
			// fail on a single node. Not panicking is the contract.
			_ = op(c)
		})
	}

	nsOps := map[string]func(peercache.Namespace) error{
		"Get":    func(n peercache.Namespace) error { _, err := n.Get(nil, "k"); return err },
		"Set":    func(n peercache.Namespace) error { return n.Set(nil, "k", []byte("v"), 0) },
		"Delete": func(n peercache.Namespace) error { return n.Delete(nil, "k") },
		"Exists": func(n peercache.Namespace) error { _, err := n.Exists(nil, "k"); return err },
		"BatchGet": func(n peercache.Namespace) error {
			_, err := n.BatchGet(nil, []string{"k"})
			return err
		},
		"BatchSet": func(n peercache.Namespace) error {
			return n.BatchSet(nil, map[string][]byte{"k": []byte("v")}, time.Minute)
		},
		"GetOrLoad": func(n peercache.Namespace) error { _, err := n.GetOrLoad(nil, "k"); return err },
	}
	c := newCache(t)
	defer c.Close(context.Background())
	ns := c.Namespace("nilctx")
	for name, op := range nsOps {
		t.Run("Namespace."+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Namespace.%s(nil) panicked: %v", name, r)
				}
			}()
			_ = op(ns)
		})
	}

	groupOps := map[string]func(peercache.Group) error{
		"Get":    func(g peercache.Group) error { _, err := g.Get(nil, "k"); return err },
		"Set":    func(g peercache.Group) error { return g.Set(nil, "k", []byte("v")) },
		"Delete": func(g peercache.Group) error { return g.Delete(nil, "k") },
		"Keys":   func(g peercache.Group) error { _, err := g.Keys(nil); return err },
		"Items":  func(g peercache.Group) error { _, err := g.Items(nil); return err },
		"Call":   func(g peercache.Group) error { _, err := g.Call(nil, "fn", nil); return err },
		"Flush":  func(g peercache.Group) error { return g.Flush(nil) },
	}
	grp := c.Group("nilctx-group")
	for name, op := range groupOps {
		t.Run("Group."+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Group.%s(nil) panicked: %v", name, r)
				}
			}()
			_ = op(grp)
		})
	}
}

// TestNilContextBehavesLikeBackground checks that the normalisation is
// transparent, not merely non-fatal.
//
// A guard that swallowed the operation would also stop the panic, and would be
// far worse: writes that silently do nothing are the failure mode this project
// has spent the most time chasing.
func TestNilContextBehavesLikeBackground(t *testing.T) {
	c, err := peercache.New(peercache.WithNodeID("nilctx2"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(context.Background())
	ns := c.Namespace("nilctx2")

	if err := ns.Set(nil, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set(nil): %v", err)
	}
	got, err := ns.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get after Set(nil): %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get after Set(nil) = %q, want %q; the nil-context guard must "+
			"normalise the context, not skip the work", got, "v")
	}
}
