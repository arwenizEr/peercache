package core

import (
	"context"
	"fmt"
	"maps"
)

// Epoch is a monotonically increasing membership version. Ownership decisions
// are stamped with the epoch they were made under so that a node returning from
// a partition can recognise its own conclusions as stale.
//
// Epochs are per-view, not globally agreed: SPEC.md §3.5 makes gossip
// membership AP, so two nodes can hold different epochs at the same instant.
// Comparing epochs establishes staleness, never consensus.
type Epoch uint64

// Node identifies one member of the cluster.
//
// ID must be stable for the lifetime of a process and unique across the
// cluster; ownership hashing depends on it. Addr is the transport address peers
// dial. Meta carries small, gossip-sized annotations such as the node's epoch.
type Node struct {
	ID   string
	Addr string
	Meta map[string]string
}

// Clone returns a deep copy, so that a caller cannot mutate a node held inside
// a membership view by writing to its Meta map.
func (n Node) Clone() Node {
	out := Node{ID: n.ID, Addr: n.Addr}
	if n.Meta != nil {
		out.Meta = maps.Clone(n.Meta)
	}
	return out
}

// IsZero reports whether n names no node. Ownership lookups return a zero Node
// alongside an error rather than a sentinel "unknown" member.
func (n Node) IsZero() bool { return n.ID == "" }

// String implements [fmt.Stringer].
func (n Node) String() string {
	if n.Addr == "" {
		return n.ID
	}
	return fmt.Sprintf("%s(%s)", n.ID, n.Addr)
}

// ChangeKind classifies a [MembershipChange].
type ChangeKind int

const (
	// ChangeJoin means a node entered the view.
	ChangeJoin ChangeKind = iota
	// ChangeLeave means a node left deliberately, having announced it.
	// Its data was transferred if shutdown was graceful.
	ChangeLeave
	// ChangeFail means a node stopped responding and was declared dead by
	// the failure detector. It may still be alive on the other side of a
	// partition, which is the whole reason activation is epoch-guarded.
	ChangeFail
	// ChangeUpdate means an existing node's metadata changed.
	ChangeUpdate
)

// String implements [fmt.Stringer].
func (k ChangeKind) String() string {
	switch k {
	case ChangeJoin:
		return "join"
	case ChangeLeave:
		return "leave"
	case ChangeFail:
		return "fail"
	case ChangeUpdate:
		return "update"
	default:
		return fmt.Sprintf("ChangeKind(%d)", int(k))
	}
}

// MembershipChange is one transition in the local view of the cluster.
//
// Epoch is the view's epoch *after* applying the change, so a watcher can use
// it directly as the epoch to stamp subsequent ownership decisions with.
type MembershipChange struct {
	Kind  ChangeKind
	Node  Node
	Epoch Epoch
}

// MembershipAuthority is the source of truth for who is in the cluster.
//
// SPEC.md §3.5. The gossip implementation is AP and is the default; the
// interface exists so a CP implementation can be substituted without a rewrite.
type MembershipAuthority interface {
	Self() Node
	Members() ([]Node, Epoch)
	Watch() <-chan MembershipChange
	Join(seeds []string) error
	Leave(ctx context.Context) error
}
