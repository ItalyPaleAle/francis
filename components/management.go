package components

import (
	"context"
	"time"
	"uuid"

	"github.com/italypaleale/francis/internal/ref"
)

const (
	// DefaultManagementListLimit is the page size of the management listing methods when the request does not specify a limit
	DefaultManagementListLimit = 100
	// MaxManagementListLimit is the largest page the management listing methods return
	MaxManagementListLimit = 1_000
)

// ManagementProvider is implemented by ActorProvider to include management API and the membership of runtime replicas
type ManagementProvider interface {
	// GetExclusiveLease returns the holder and expiry of the cluster exclusive-access lease
	// It returns a zero ExclusiveLeaseInfo (with an empty Owner) when no live lease is held
	GetExclusiveLease(ctx context.Context) (ExclusiveLeaseInfo, error)

	// ListHostDetails returns a page of the hosts with a live registration, ordered by host ID
	// Unlike ListHosts, each entry carries the host's session and owner runtime, its draining flag, and its per-type placement usage and settings
	ListHostDetails(ctx context.Context, req ListHostDetailsReq) (ListHostDetailsRes, error)

	// GetHostDetails returns the details of a single host with a live registration
	// If the host doesn't exist or its registration expired, returns ErrHostUnregistered
	GetHostDetails(ctx context.Context, hostID string) (HostDetails, error)

	// MarkHostDraining marks a live host draining, like UpdateActorHost with Draining set, unless that would leave an actor type without a live, non-draining server
	// The check and the update are atomic with respect to other MarkHostDraining calls, so two hosts that are the last servers of a type can't both be marked draining unless forced
	// If the host doesn't exist or its registration expired, returns ErrHostUnregistered
	// While an exclusive-access lease is held it changes nothing and returns ErrClusterLocked, checking the lease atomically with the update
	MarkHostDraining(ctx context.Context, req MarkHostDrainingReq) (MarkHostDrainingRes, error)

	// ClearHostDraining clears the draining flag of a live host, putting it back into placement
	// It undoes MarkHostDraining for a drain the host never accepted, and is a no-op for a host that is not draining
	// If the host doesn't exist or its registration expired, returns ErrHostUnregistered
	ClearHostDraining(ctx context.Context, hostID string) error

	// ListPlacements returns a page of the provider's actor placements, ordered by actor type and then actor ID
	// It never creates, moves or removes a placement
	ListPlacements(ctx context.Context, req ListPlacementsReq) (ListPlacementsRes, error)

	// QueryJobs returns a page of jobs across the whole cluster, spanning live and terminal jobs, ordered by job ID
	// Live and terminal job IDs never overlap, so the job ID alone is a stable pagination cursor
	QueryJobs(ctx context.Context, req QueryJobsReq) (QueryJobsRes, error)

	// CountJobs counts the jobs across the whole cluster that QueryJobs lists with the same status filter, stopping at the request's limit
	// It returns the limit when at least that many jobs match, so counting a large collection does a bounded amount of work
	CountJobs(ctx context.Context, req CountJobsReq) (int, error)

	// CountStates counts the live stored states of an actor type that ListStates lists with the same workflow label filter, stopping at the request's limit
	// It returns the limit when at least that many states match, so counting a large collection does a bounded amount of work
	CountStates(ctx context.Context, req CountStatesReq) (int, error)

	// ListAlarms returns a page of plain alarms (excluding jobs), ordered by actor type, actor ID and alarm name
	ListAlarms(ctx context.Context, req ListAlarmsReq) (ListAlarmsRes, error)

	// ListStateActorTypes returns the distinct actor types that have live stored state and whose name starts with prefix, in ascending order
	// An empty prefix matches every type
	ListStateActorTypes(ctx context.Context, prefix string) ([]string, error)

	// ListWorkflowEvents returns a page of the workflow events appended for an actor's state through SetStateOpts.AppendEvents, ordered by sequence number
	// Events are only returned while the actor's state is live, so expired or deleted state returns no events
	ListWorkflowEvents(ctx context.Context, req ListWorkflowEventsReq) (ListWorkflowEventsRes, error)

	// RegisterRuntime records or renews the membership of a runtime replica, extending its lease to now+ttl
	// It returns ErrRuntimeIDInUse if another address currently holds a live lease for the same runtime ID
	RegisterRuntime(ctx context.Context, req RegisterRuntimeReq) error

	// UnregisterRuntime removes the membership record of a runtime replica, if it is still held by the given address
	// It is idempotent: removing a record that does not exist is not an error
	UnregisterRuntime(ctx context.Context, runtimeID string, address string) error

	// ListRuntimes returns the runtime replicas with a live membership lease, ordered by runtime ID
	ListRuntimes(ctx context.Context) ([]RuntimeInfo, error)
}

// EffectiveListLimit applies the default and the cap of the management listing methods to a requested page size
func EffectiveListLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultManagementListLimit
	case limit > MaxManagementListLimit:
		return MaxManagementListLimit
	default:
		return limit
	}
}

// UUIDCursor is a pagination cursor over a collection keyed by UUIDs, holding the ID of the last item of the previous page
// Its zero value sorts before every ID, so it starts from the beginning
// IDs are stored in their canonical lowercase text form, whose ordering agrees with the ordering of the UUIDs
type UUIDCursor = uuid.UUID

// ExclusiveLeaseInfo describes the holder of the cluster exclusive-access lease
type ExclusiveLeaseInfo struct {
	// Owner is the lease holder, empty when no live lease is held
	Owner string
	// ExpiresAt is when the lease expires unless renewed
	ExpiresAt time.Time
}

// IsHeld reports whether a live lease is held
func (i ExclusiveLeaseInfo) IsHeld() bool {
	return i.Owner != ""
}

// ListHostDetailsReq is the request object for the ListHostDetails method
type ListHostDetailsReq struct {
	// After is the pagination cursor: only hosts whose ID sorts strictly after it are returned
	After string
	// Limit is the maximum number of hosts to return, see EffectiveListLimit
	Limit int
}

// ListHostDetailsRes is the response object for the ListHostDetails method
type ListHostDetailsRes struct {
	// Hosts in this page, ordered by host ID
	Hosts []HostDetails
	// HasMore is true when more hosts follow the last one in this page
	HasMore bool
}

// HostDetails describes a host with a live registration
type HostDetails struct {
	// Host ID
	HostID string
	// Host address (including port)
	Address string
	// Time of the host's last health check
	LastHealthCheck time.Time
	// SessionID is the session that owns the registration
	// Empty for hosts not connected through a runtime
	SessionID string
	// RuntimeID is the runtime replica that owns the session
	// Empty for hosts not connected through a runtime
	RuntimeID string
	// Draining is true when the host was marked draining
	Draining bool
	// ActorTypes are the actor types the host serves, ordered by actor type
	ActorTypes []HostActorTypeDetails
}

// HostActorTypeDetails describes one actor type served by a host, with its current placement usage
type HostActorTypeDetails struct {
	// Actor type
	ActorType string
	// Idle timeout for the actor type
	IdleTimeout time.Duration
	// ConcurrencyLimit is the maximum number of actors of this type placed on the host, where 0 means no limit
	ConcurrencyLimit int32
	// ActiveCount is the number of actors of this type currently placed on the host
	ActiveCount int
	// CompletedJobRetention is the retention the host registered for completed jobs, with the ActorHostType semantics
	CompletedJobRetention time.Duration
	// Retention the host registered for dead-lettered jobs, with the ActorHostType semantics
	DeadLetteredJobRetention time.Duration
}

// MarkHostDrainingReq is the request object for the MarkHostDraining method
type MarkHostDrainingReq struct {
	// HostID is the host to mark draining
	HostID string
	// Force marks the host draining even when it is the last live, non-draining server of one or more actor types
	Force bool
}

// MarkHostDrainingRes is the response object for the MarkHostDraining method
type MarkHostDrainingRes struct {
	// AlreadyDraining is true when the host was already draining, in which case nothing was checked or changed
	AlreadyDraining bool
	// LastServerOf lists, in ascending order, the actor types the host serves that no other live, non-draining host serves
	// When it is not empty and the request did not set Force, the host was left unchanged
	LastServerOf []string
}

// Refused reports whether the host was left unchanged because it is the last server of some actor types and the request was not forced
func (r MarkHostDrainingRes) Refused(req MarkHostDrainingReq) bool {
	return !r.AlreadyDraining && len(r.LastServerOf) > 0 && !req.Force
}

// ListPlacementsReq is the request object for the ListPlacements method
type ListPlacementsReq struct {
	// HostID, when set, restricts the listing to placements on this host
	HostID string
	// ActorType, when set, restricts the listing to placements of this actor type
	ActorType string
	// After is the pagination cursor: only placements sorting strictly after this actor type and ID are returned
	// A zero value starts from the beginning
	After ref.ActorRef
	// Limit is the maximum number of placements to return, see EffectiveListLimit
	Limit int
}

// ListPlacementsRes is the response object for the ListPlacements method
type ListPlacementsRes struct {
	// Placements in this page, ordered by actor type and then actor ID
	Placements []PlacementInfo
	// HasMore is true when more placements follow the last one in this page
	HasMore bool
}

// PlacementInfo describes an actor placement recorded by the provider
type PlacementInfo struct {
	ActorType string
	ActorID   string
	HostID    string
	// Idle timeout copied from the host's actor type when the actor was placed
	IdleTimeout time.Duration
}

// QueryJobsReq is the request object for the QueryJobs method
type QueryJobsReq struct {
	// ActorType, when set, restricts the listing to jobs of this actor type
	ActorType string
	// ActorID, when set together with ActorType, restricts the listing to jobs of this actor
	ActorID string
	// Status, when set, restricts the listing to jobs in this status
	Status JobStatus
	// After is the pagination cursor: only jobs whose ID sorts strictly after it are returned
	After UUIDCursor
	// Limit is the maximum number of jobs to return, see EffectiveListLimit
	Limit int
}

// IncludeLive reports whether the status filter can match live jobs, which are pending or active
func (r QueryJobsReq) IncludeLive() bool {
	return r.Status == "" || r.Status == JobStatusPending || r.Status == JobStatusActive
}

// IncludeTerminal reports whether the status filter can match terminal jobs, which are completed or dead-lettered
func (r QueryJobsReq) IncludeTerminal() bool {
	return r.Status == "" || r.Status.IsTerminal()
}

// QueryJobsRes is the response object for the QueryJobs method
type QueryJobsRes struct {
	// Jobs in this page, ordered by job ID
	Jobs []JobInfo
	// HasMore is true when more jobs follow the last one in this page
	HasMore bool
}

// CountJobsReq is the request object for the CountJobs method
type CountJobsReq struct {
	// Status, when set, restricts the count to jobs in this status
	Status JobStatus
	// Limit is the count at which counting stops
	// A limit that is not positive counts nothing
	Limit int
}

// IncludeLive reports whether the status filter can match live jobs, which are pending or active
func (r CountJobsReq) IncludeLive() bool {
	return r.Status == "" || r.Status == JobStatusPending || r.Status == JobStatusActive
}

// IncludeTerminal reports whether the status filter can match terminal jobs, which are completed or dead-lettered
func (r CountJobsReq) IncludeTerminal() bool {
	return r.Status == "" || r.Status.IsTerminal()
}

// CountStatesReq is the request object for the CountStates method
type CountStatesReq struct {
	// Actor type whose stored states are counted
	ActorType string
	// WorkflowLabels, when set, restricts the count to rows whose labels match every field it sets, as in ListStatesReq
	WorkflowLabels *WorkflowLabels
	// Limit is the count at which counting stops
	// A limit that is not positive counts nothing
	Limit int
}

// ListAlarmsReq is the request object for the ListAlarms method
type ListAlarmsReq struct {
	// ActorType, when set, restricts the listing to alarms of this actor type
	ActorType string
	// ActorID, when set together with ActorType, restricts the listing to alarms of this actor
	ActorID string
	// After is the pagination cursor: only alarms sorting strictly after this reference are returned
	// A zero value starts from the beginning
	After ref.AlarmRef
	// Limit is the maximum number of alarms to return, see EffectiveListLimit
	Limit int
}

// ListAlarmsRes is the response object for the ListAlarms method
type ListAlarmsRes struct {
	// Alarms in this page, ordered by actor type, actor ID and alarm name
	Alarms []AlarmInfo
	// HasMore is true when more alarms follow the last one in this page
	HasMore bool
}

// AlarmInfo describes a plain alarm
type AlarmInfo struct {
	AlarmID   string
	ActorType string
	ActorID   string
	Name      string
	DueTime   time.Time
	// Interval is the repetition interval as an ISO8601-formatted duration, empty for a one-shot alarm
	Interval string
	// TTL is the time repetitions stop, nil when there is none
	TTL *time.Time
	// LeaseExpiration is when the current lease expires, nil when the alarm is not leased or the lease expired
	LeaseExpiration *time.Time
	// LeaseHostID is the host the alarm's actor is placed on while the alarm is leased, since alarm leases record no owner
	// Empty when the alarm is not leased or its actor has no placement on a live host
	LeaseHostID string
}

// WorkflowEvent is an entry of a workflow instance's append-only event history
// The provider stores it opaquely: only the sequence number is interpreted
type WorkflowEvent struct {
	// Seq is the event's sequence number, starting at 1 and unique per actor
	Seq int64
	// Time is when the event happened
	Time time.Time
	// Kind is the event kind
	Kind string
	// Data holds the event details, encoded by the workflow engine
	Data []byte
}

// ListWorkflowEventsReq is the request object for the ListWorkflowEvents method
type ListWorkflowEventsReq struct {
	ActorType string
	ActorID   string
	// AfterSeq is the pagination cursor: only events with a greater sequence number are returned
	AfterSeq int64
	// Limit is the maximum number of events to return, see EffectiveListLimit
	Limit int
}

// ListWorkflowEventsRes is the response object for the ListWorkflowEvents method
type ListWorkflowEventsRes struct {
	// Events in this page, ordered by sequence number
	Events []WorkflowEvent
	// HasMore is true when more events follow the last one in this page
	HasMore bool
}

// RegisterRuntimeReq is the request object for the RegisterRuntime method
type RegisterRuntimeReq struct {
	// RuntimeID is the replica's unique runtime ID
	RuntimeID string
	// Address is the peer address other replicas dial to reach this one
	Address string
	// TTL is how long the membership lease lasts unless renewed
	TTL time.Duration
}

// RuntimeInfo describes a runtime replica with a live membership lease
type RuntimeInfo struct {
	RuntimeID string
	Address   string
	// LastHeartbeat is when the lease was last registered or renewed
	LastHeartbeat time.Time
	// ExpiresAt is when the lease expires unless renewed
	ExpiresAt time.Time
}
