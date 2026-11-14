package membership_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/membership"
)

// newKeyedGossip starts a gossip node whose memberlist encrypts and verifies
// every message with key, through Options.Config — the path docs/api.md tells
// an operator to take.
func newKeyedGossip(t *testing.T, id string, key []byte) *membership.Gossip {
	t.Helper()
	g, err := testkit.RetryBind(func() (*membership.Gossip, error) {
		cfg := localConfig(id)
		cfg.SecretKey = key
		return membership.New(membership.Options{Self: core.Node{ID: id}, Config: cfg})
	})
	if err != nil {
		t.Fatalf("membership.New(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestEncryptedGossipAdmitsOnlyTheKeyHolders: two nodes sharing a key form a
// cluster; a node with another key cannot join it. mTLS covers the data plane
// only, and gossip carries every node's data address, so this is how an
// operator keeps a stranger out of the membership.
func TestEncryptedGossipAdmitsOnlyTheKeyHolders(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and waits on real gossip")
	}
	key := bytes.Repeat([]byte{7}, 32)
	a := newKeyedGossip(t, "a", key)
	b := newKeyedGossip(t, "b", key)
	if err := b.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("a node holding the key could not join: %v", err)
	}
	awaitReal(t, "a and b to see each other", 10*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 2
	})

	stranger := newKeyedGossip(t, "stranger", bytes.Repeat([]byte{9}, 32))
	if err := stranger.Join([]string{a.LocalAddr()}); err == nil {
		t.Fatal("a node with another key joined an encrypted cluster")
	}
	if members, _ := a.Members(); len(members) != 2 {
		t.Fatalf("the cluster has %d members after a stranger tried to join, want 2", len(members))
	}
}
