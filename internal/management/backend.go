package management

import (
	"context"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// Topology identifies the kind of deployment serving the API
type Topology string

const (
	// TopologyRemote is a cluster of hosts connected to standalone runtime replicas
	TopologyRemote Topology = "remote"
	// TopologyLocal is a cluster of hosts that embed the provider and talk to each other directly
	TopologyLocal Topology = "local"
)

// Backend is what the management server needs from the process that runs it
// The standalone runtime and the local host each implement it
type Backend interface {
	// Topology returns the topology of the cluster
	Topology() Topology

	// Provider returns the actor provider, the source of truth for every durable read
	Provider() components.ManagementProvider

	// HostReachability reports, for each host, whether a session to it is currently held
	// In the local topology every registered host is reachable through its peer server, so every host is reported as reachable
	HostReachability(ctx context.Context, hosts []components.HostDetails) map[string]HostReachability

	// HostSnapshot asks a host for a page of its in-memory activations and its execution capacity
	HostSnapshot(ctx context.Context, host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error)

	// DrainHost asks a host to drain, returning once the host accepted the request
	// The caller has already persisted the draining flag
	DrainHost(ctx context.Context, host components.HostDetails, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error)

	// DeactivateActor asks a host to halt an actor it hosts
	// notActive is true when the actor was not active on the host
	DeactivateActor(ctx context.Context, host components.HostDetails, actorType string, actorID string) (notActive bool, err error)

	// DispatchJob durably stores a job, as a host dispatching it would
	DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (created bool, err error)

	// Runtimes returns the runtime replicas with a live membership, with their locally connected hosts
	// It returns ErrNotApplicable in the local topology
	Runtimes(ctx context.Context) ([]RuntimeStatus, error)
}

// HostReachability describes whether a host can currently be reached
type HostReachability struct {
	// Reachable is true when a session to the host is held
	Reachable bool
	// OwnerRuntimeID is the runtime replica holding the host's session, empty in the local topology
	OwnerRuntimeID string
}

// RuntimeStatus describes a runtime replica
type RuntimeStatus struct {
	components.RuntimeInfo

	// Self is true for the replica serving the request
	Self bool
	// ConnectedHosts is the number of host sessions the replica holds, nil when it could not be queried
	ConnectedHosts *int
	// Error is set when the replica could not be queried
	Error string
}
