package protocol

// This file defines the message DTOs for management traffic
// The same payloads travel Runtime -> Host in the remote topology, Host -> Host over the peer server in the local topology, and Runtime -> Runtime between replicas

// Management message kinds
const (
	// Runtime -> Host requests and responses

	KindHostSnapshot         = "runtime.host.snapshot"
	KindHostSnapshotResponse = "runtime.host.snapshot.response"
	KindHostDrain            = "runtime.host.drain"
	KindHostDrainResponse    = "runtime.host.drain.response"

	// Host -> Host management requests and responses, served by the peer server of every local host

	KindPeerHostSnapshot            = "peer.mgmt.snapshot"
	KindPeerHostSnapshotResponse    = "peer.mgmt.snapshot.response"
	KindPeerHostDrain               = "peer.mgmt.drain"
	KindPeerHostDrainResponse       = "peer.mgmt.drain.response"
	KindPeerDeactivateActor         = "peer.mgmt.deactivate"
	KindPeerDeactivateActorResponse = "peer.mgmt.deactivate.response"

	// Runtime -> Runtime requests and responses, sent to the replica that owns a host session
	// Each request names the host and session it expects, and the receiving replica rejects it if its live session for the host differs

	KindRuntimeHostSnapshot            = "runtime.peer.snapshot"
	KindRuntimeHostSnapshotResponse    = "runtime.peer.snapshot.response"
	KindRuntimeHostDrain               = "runtime.peer.drain"
	KindRuntimeHostDrainResponse       = "runtime.peer.drain.response"
	KindRuntimeDeactivateActor         = "runtime.peer.deactivate"
	KindRuntimeDeactivateActorResponse = "runtime.peer.deactivate.response"
	KindRuntimeSessions                = "runtime.peer.sessions"
	KindRuntimeSessionsResponse        = "runtime.peer.sessions.response"
)

// PeerManagementKindPrefix is the prefix shared by every Host -> Host management kind
const PeerManagementKindPrefix = "peer.mgmt."

// Management-specific error codes
const (
	// ErrCodeHostReattached indicates the host's session changed before it received the request, so the request was not delivered
	ErrCodeHostReattached ErrorCode = "host_reattached"
	// ErrCodeHostUnavailable indicates no live session or peer connection to the host could be reached
	ErrCodeHostUnavailable ErrorCode = "host_unavailable"
)

// MaxSnapshotPageSize is the largest number of activations a host returns in one snapshot page
const MaxSnapshotPageSize = 5000

// HostSnapshotRequest asks a host for a page of its in-memory activations and its capacity groups
type HostSnapshotRequest struct {
	// ActorType, when set, restricts the activations to this actor type
	ActorType string `msgpack:"type,omitempty"`
	// After is the continuation cursor returned by the previous page, empty for the first page
	After string `msgpack:"after,omitempty"`
	// Limit is the maximum number of activations to return, capped at MaxSnapshotPageSize
	Limit int `msgpack:"limit,omitempty"`
	// SkipActivations returns no activations, only counts, capacity groups and workflow definitions
	SkipActivations bool `msgpack:"skipActivations,omitempty"`
}

// HostSnapshotResponse is a page of a host's in-memory activations
type HostSnapshotResponse struct {
	// HostID is the ID of the host that took the snapshot
	HostID string `msgpack:"hostId"`
	// ObservedAtUnixMs is when the host took the snapshot
	ObservedAtUnixMs int64 `msgpack:"observed"`
	// ActiveCount is the total number of activations matching the request on the host, across all pages
	ActiveCount int `msgpack:"count"`
	// Activations in this page, ordered by actor type and ID
	Activations []ActivationInfo `msgpack:"activations,omitempty"`
	// Next is the cursor for the following page, empty when this is the last page
	Next string `msgpack:"next,omitempty"`
	// CapacityGroups are the host-local capacity groups with their current usage
	CapacityGroups []CapacityGroupInfo `msgpack:"groups,omitempty"`
	// Draining is true once the host started draining
	Draining bool `msgpack:"draining,omitempty"`
	// Workflows are the workflow definitions the host serves, so they can be compared with the durable registry
	Workflows []WorkflowDefinitionInfo `msgpack:"workflows,omitempty"`
}

// WorkflowDefinitionInfo describes a workflow definition served by a host
type WorkflowDefinitionInfo struct {
	Name        string `msgpack:"name"`
	Version     int    `msgpack:"version"`
	Fingerprint string `msgpack:"fingerprint"`
}

// ActivationInfo describes one actor held in memory by a host
type ActivationInfo struct {
	ActorType string `msgpack:"type"`
	ActorID   string `msgpack:"id"`
	// ActivatedAtUnixMs is when the host created the in-memory actor
	ActivatedAtUnixMs int64 `msgpack:"activated"`
	// Deactivating is true once the actor started halting
	Deactivating bool `msgpack:"deactivating,omitempty"`
}

// CapacityGroupInfo describes a host-local capacity group, which bounds concurrent job and alarm executions for the actor types that joined it
type CapacityGroupInfo struct {
	Name       string   `msgpack:"name"`
	Limit      int      `msgpack:"limit"`
	InUse      int      `msgpack:"inUse"`
	ActorTypes []string `msgpack:"types,omitempty"`
}

// HostDrainRequest asks a host to drain: stop accepting new work, deactivate its actors, unregister, and stop
type HostDrainRequest struct {
	// TimeoutMs bounds how long the in-flight calls of the halting actors can run before they are canceled
	// The host still waits for the canceled calls to return before it unregisters, and zero means no bound
	TimeoutMs int64 `msgpack:"timeout,omitempty"`
	// Reason is an optional operator-supplied reason, logged by the host
	Reason string `msgpack:"reason,omitempty"`
}

// HostDrainResponse is the host's acknowledgement of a drain request
type HostDrainResponse struct {
	// AlreadyDraining is true when the host was already draining, so the request had no further effect
	AlreadyDraining bool `msgpack:"already,omitempty"`
}

// TerminateActorResponse is the host's acknowledgement of a TerminateActorRequest
// Older hosts reply with an empty body, which decodes as an actor that was active
type TerminateActorResponse struct {
	// NotActive is true when the actor was not active on the host
	NotActive bool `msgpack:"notActive,omitempty"`
}

// DeactivateActorRequest asks a local host, over the peer server, to halt one of its actors
type DeactivateActorRequest struct {
	// TargetHostID is the host the caller expects to own the actor
	TargetHostID string `msgpack:"hostId"`
	ActorType    string `msgpack:"type"`
	ActorID      string `msgpack:"id"`
}

// RuntimeHostRequest wraps a management request that a runtime replica forwards to the replica owning a host's session
type RuntimeHostRequest struct {
	// HostID is the host the request is for
	HostID string `msgpack:"hostId"`
	// SessionID is the session the caller expects the owning replica to hold for the host
	SessionID string `msgpack:"sessionId"`
	// Snapshot is set for KindRuntimeHostSnapshot
	Snapshot *HostSnapshotRequest `msgpack:"snapshot,omitempty"`
	// Drain is set for KindRuntimeHostDrain
	Drain *HostDrainRequest `msgpack:"drain,omitempty"`
	// Terminate is set for KindRuntimeDeactivateActor
	Terminate *TerminateActorRequest `msgpack:"terminate,omitempty"`
}

// RuntimeSessionsResponse lists the host sessions a runtime replica holds
type RuntimeSessionsResponse struct {
	// RuntimeID is the ID of the replica
	RuntimeID string `msgpack:"runtimeId"`
	// Sessions are the host sessions the replica holds
	Sessions []RuntimeSessionInfo `msgpack:"sessions,omitempty"`
}

// RuntimeSessionInfo describes a host session held by a runtime replica
type RuntimeSessionInfo struct {
	HostID    string `msgpack:"hostId"`
	SessionID string `msgpack:"sessionId"`
	Draining  bool   `msgpack:"draining,omitempty"`
}
