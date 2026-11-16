package membership

import (
	"sync"
	"testing"

	"github.com/hashicorp/memberlist"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestTheFirstAliveMessageAdvertisesTheRPCAddress pins what a node tells the
// cluster before it has republished anything.
//
// memberlist asks the delegate for metadata while Create builds the local
// node's first alive message, and a peer that joins before the first republish
// learns the node from that message. If the RPC address is not in it, the peer
// falls back to the gossip address and dials the SWIM port expecting gRPC —
// "error reading server preface: EOF" — until a later republish corrects it.
// That was the first-RPC failure CI kept hitting against freshly started peers.
func TestTheFirstAliveMessageAdvertisesTheRPCAddress(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds a loopback socket")
	}
	self := core.Node{ID: "node-a", Addr: "10.0.0.1:9999"}
	var seen firstAlive
	g, err := testkit.RetryBind(func() (*Gossip, error) {
		seen.reset()
		cfg := testkit.LocalGossipConfig(self.ID)
		cfg.Alive = &seen
		return New(Options{Self: self, Config: cfg})
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	raw, ok := seen.get()
	if !ok {
		t.Fatal("memberlist reported no alive message for the local node")
	}
	got := decodeNodeMeta(string(raw))
	if got.RPCAddr != self.Addr {
		t.Fatalf("first alive message advertises RPC address %q, want %q", got.RPCAddr, self.Addr)
	}
	if got.Started == 0 {
		t.Fatal("first alive message carries no start time")
	}
}

// firstAlive records the metadata of the first alive message memberlist
// processes, which for a node that has joined nobody is its own bootstrap one.
type firstAlive struct {
	mu   sync.Mutex
	meta []byte
	ok   bool
}

func (f *firstAlive) NotifyAlive(n *memberlist.Node) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ok {
		f.meta, f.ok = append([]byte(nil), n.Meta...), true
	}
	return nil
}

func (f *firstAlive) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.meta, f.ok = nil, false
}

func (f *firstAlive) get() ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.meta, f.ok
}
