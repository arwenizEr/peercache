// Package digest keeps a running, order-independent summary of the keys and
// versions a store holds, split into a fixed number of partitions by key, so
// that two stores can be compared without walking either. See
// docs/decisions/0039-cluster-view.md.
package digest
