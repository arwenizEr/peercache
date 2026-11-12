package core

import "fmt"

// EventKind classifies an [Event]. The set matches the library's lifecycle
// hooks.
type EventKind int

const (
	// EventCreate is emitted when a key appears in a group for the first
	// time.
	EventCreate EventKind = iota
	// EventUpdate is emitted when an existing key is overwritten.
	EventUpdate
	// EventDelete is emitted when a key is removed explicitly.
	EventDelete
	// EventEvict is reserved: nothing emits it. Group state has no TTL
	// and no capacity limit. See the root package's EventEvict.
	EventEvict
	// EventActivate is emitted when a group is activated on this node.
	EventActivate
	// EventDeactivate is emitted when a group is deactivated, whether
	// through idle timeout, capacity pressure or ownership transfer.
	EventDeactivate
	// EventCustom is emitted by a registered group function through
	// [GroupState.Emit].
	EventCustom
)

// String implements [fmt.Stringer].
func (k EventKind) String() string {
	switch k {
	case EventCreate:
		return "create"
	case EventUpdate:
		return "update"
	case EventDelete:
		return "delete"
	case EventEvict:
		return "evict"
	case EventActivate:
		return "activate"
	case EventDeactivate:
		return "deactivate"
	case EventCustom:
		return "custom"
	default:
		return fmt.Sprintf("EventKind(%d)", int(k))
	}
}

// Event is a lifecycle or state-change notification.
//
// Events are informational. Nothing in PeerCache may depend on an event being
// observed, for the same reason nothing may depend on an invalidation broadcast
// arriving.
type Event struct {
	Kind    EventKind
	GroupID string
	Key     string
	Value   []byte
	// Name distinguishes user-defined events when Kind is EventCustom.
	Name string
}
