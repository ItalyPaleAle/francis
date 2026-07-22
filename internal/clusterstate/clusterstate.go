// Package clusterstate holds the in-memory representation of the cluster-admission state stored in the provider's cluster_config table
// The state carries the cluster-wide host limit and the exclusive-access lease, and its row is the single serialization point for host registration and exclusive access
package clusterstate

// State is the cluster-admission state read from the cluster_config table
type State struct {
	// MaxHosts is the effective cluster host limit, or nil until the first host claims it
	// A claimed value of 0 means no limit
	MaxHosts *int
	// ExclusiveOwner is the owner of the current exclusive-access lease, or empty when no lease is held
	ExclusiveOwner string
	// ExclusiveExpiresAt is the lease expiry as a Unix timestamp in milliseconds, or 0 when no lease is held
	ExclusiveExpiresAt int64
}

// LeaseLive reports whether an exclusive-access lease is held and not expired at the given Unix-millisecond time
func (s State) LeaseLive(nowMs int64) bool {
	return s.ExclusiveOwner != "" && s.ExclusiveExpiresAt >= nowMs
}
