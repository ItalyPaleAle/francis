// Package clusterstate defines the JSON document stored in the singleton "cluster" row of the provider metadata table
// The row carries the cluster-wide host limit and the exclusive-access lease, and is the single serialization point for host registration and exclusive access
package clusterstate

import (
	"encoding/json"
	"fmt"
)

// MetadataKey is the key of the singleton cluster-admission row in the provider metadata table
const MetadataKey = "cluster"

// State is the JSON document stored in the cluster metadata row
type State struct {
	// MaxHosts is the effective cluster host limit, or nil until the first host claims it
	// A claimed value of 0 means no limit
	MaxHosts *int `json:"max_hosts"`
	// Exclusive is the current exclusive-access lease, or nil when no lease is held
	Exclusive *Lease `json:"exclusive"`
}

// Lease is an exclusive-access lease held by a single ClusterAdmin
type Lease struct {
	// Owner is the UUID of the ClusterAdmin that holds the lease
	Owner string `json:"owner"`
	// ExpiresAt is the lease expiry as a Unix timestamp in milliseconds
	ExpiresAt int64 `json:"expires_at"`
}

// Parse decodes the raw JSON value read from the cluster metadata row
func Parse(raw string) (State, error) {
	var s State
	if raw == "" {
		return s, nil
	}
	err := json.Unmarshal([]byte(raw), &s)
	if err != nil {
		return s, fmt.Errorf("failed to decode cluster state: %w", err)
	}
	return s, nil
}

// Marshal encodes the state back to the JSON stored in the cluster metadata row
func (s State) Marshal() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("failed to encode cluster state: %w", err)
	}
	return string(data), nil
}

// LeaseLive reports whether the lease is non-nil and not expired at the given Unix-millisecond time
func (s State) LeaseLive(nowMs int64) bool {
	return s.Exclusive != nil && s.Exclusive.ExpiresAt >= nowMs
}
