package components

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
	"uuid"

	"github.com/italypaleale/francis/internal/ref"
)

const (
	DefaultHostHealthCheckDeadline   = 20 * time.Second
	DefaultAlarmsLeaseDuration       = 20 * time.Second
	DefaultAlarmsFetchAheadInterval  = 2500 * time.Millisecond
	DefaultAlarmsFetchAheadBatchSize = 25

	// DefaultListStatesLimit is the number of actor states returned by ListStates when the request does not specify a limit
	DefaultListStatesLimit = 100
	// MaxListStatesLimit is the largest page ListStates will return
	MaxListStatesLimit = 1000
)

// ActorProvider is the interface implemented by all actor providers
type ActorProvider interface {
	// Init the actor provider
	Init(ctx context.Context) error

	// Run the actor provider
	// This method blocks until the context is canceled
	// If the provider is already running, returns ErrAlreadyRunning
	Run(ctx context.Context) error

	// Close releases the resources owned by the provider, including the connection to the database if the provider established it.
	// A connection passed in by the caller through the provider's options is not closed, since it remains owned by the caller.
	// The provider cannot be used after being closed, and calling Close more than once is a no-op.
	Close() error

	// RegisterHost registers a new actor host, or reattaches to an existing registration when req.ExistingHostID is set.
	// If a different, healthy host is already registered at the same address, returns ErrHostAlreadyRegistered.
	RegisterHost(ctx context.Context, req RegisterHostReq) (RegisterHostRes, error)

	// UpdateActorHost updates the properties for an actor host
	// If the host doesn't exist, returns ErrHostUnregistered.
	UpdateActorHost(ctx context.Context, hostID string, req UpdateActorHostReq) error

	// UnregisterHost unregisters an actor host.
	// If the host doesn't exist, returns ErrHostUnregistered.
	UnregisterHost(ctx context.Context, hostID string) error

	// ListHosts returns all actor hosts that are currently registered and healthy
	ListHosts(ctx context.Context) ([]HostInfo, error)

	// LookupActor returns the address of the actor host for a given actor type and ID.
	// If the actor is not currently active on any host, a new actor is created and assigned to a random host
	// If it's not possible to find an instance capable of hosting the given actor, ErrNoHost is returned instead.
	LookupActor(ctx context.Context, ref ref.ActorRef, opts LookupActorOpts) (LookupActorRes, error)

	// RemoveActor removes an actor from the collection of active actors.
	// If the actor doesn't exist, returns ErrNoActor.
	RemoveActor(ctx context.Context, ref ref.ActorRef) error

	// GetAlarm returns an alarm.
	// It returns ErrNoAlarm if it doesn't exist.
	GetAlarm(ctx context.Context, ref ref.AlarmRef) (GetAlarmRes, error)

	// SetAlarm sets or replaces an alarm configured for an actor and optionally returns a lease acquired while storing it
	SetAlarm(ctx context.Context, ref ref.AlarmRef, req SetAlarmReq) (*ref.AlarmLease, error)

	// DeleteAlarm removes an alarm configured for an actor.
	// If the alarm doesn't exist, returns ErrNoAlarm.
	DeleteAlarm(ctx context.Context, ref ref.AlarmRef) error

	// DispatchJob creates a job as an alarm row with Kind = job, returning the job ID and any lease acquired while storing it
	// When req carries an alarm name (an idempotency key), a job with the same (actor_type, actor_id, name) is kept and its existing job ID is returned, so re-dispatching with the same key is idempotent (first-write-wins)
	DispatchJob(ctx context.Context, ref ref.AlarmRef, req SetAlarmReq) (jobID string, lease *ref.AlarmLease, err error)

	// DeadLetterAlarm moves a leased job from the alarms table to the terminal-job store, recording it as dead-lettered.
	// It accepts a lease the job's own actor released by deactivating, for the reason given on DeleteLeasedAlarm.
	// When req.Reschedule is set, the recurrence is re-created for its next occurrence in the same transaction, so a repeating job survives the dead-lettering of one occurrence.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req DeadLetterAlarmReq) error

	// CompleteJob moves a leased job from the alarms table to the terminal-job store, recording it as completed.
	// It accepts a lease the job's own actor released by deactivating, for the reason given on DeleteLeasedAlarm.
	// When req.Reschedule is set, the recurrence is re-created for its next occurrence atomically, so a repeating job keeps running while each occurrence leaves a record.
	// The recurrence keeps the job ID and the completed occurrence is recorded under one of its own, because a job ID identifies a schedule for as long as it exists.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	CompleteJob(ctx context.Context, lease *ref.AlarmLease, req CompleteJobReq) error

	// GetJob returns the information for a job by its ID, spanning both live jobs (in the alarms table) and terminal ones (completed or dead-lettered).
	// Returns ErrNoJob if the job cannot be found.
	GetJob(ctx context.Context, jobID string) (JobInfo, error)

	// ListJobs returns all of an actor's jobs: the live ones, and any terminal record still retained.
	ListJobs(ctx context.Context, actorType string, actorID string) ([]JobInfo, error)

	// DeleteJob removes one of an actor's jobs by its ID, whatever state it is in: live (scheduled or leased) or terminal (completed or dead-lettered).
	// Returns ErrNoJob if that actor has no job with that ID.
	DeleteJob(ctx context.Context, actorType string, actorID string, jobID string) error

	// GetTerminalJob returns a completed or dead-lettered job by its ID, including its raw input data.
	// Returns ErrNoJob if the terminal job cannot be found.
	GetTerminalJob(ctx context.Context, jobID string) (GetTerminalJobRes, error)

	// RetryDeadJob atomically re-dispatches a dead-lettered job as a fresh, immediate one-shot job and removes its terminal record, returning the new job ID.
	// The re-dispatch and the removal happen in a single transaction, so a crash can never leave both a replayed job and its record behind.
	// Returns ErrNoJob if the job cannot be found or did not end dead-lettered, since a job that completed has nothing to retry.
	RetryDeadJob(ctx context.Context, jobID string) (newJobID string, err error)

	// FetchAndLeaseUpcomingAlarms fetches the upcoming alarms, acquiring a lease on them.
	FetchAndLeaseUpcomingAlarms(ctx context.Context, req FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error)

	// RenewAlarmLeases renews the leases for the alarms in the request.
	// The method can renew specific alarm leases and/or those tied to specific hosts.
	RenewAlarmLeases(ctx context.Context, req RenewAlarmLeasesReq) (RenewAlarmLeasesRes, error)

	// ReleaseAlarmLease releases an active lease on an alarm.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error

	// GetLeasedAlarm retrieves an alarm using an alarm lease object.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (GetLeasedAlarmRes, error)

	// UpdateLeasedAlarm updates an alarm using an alarm lease object.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req UpdateLeasedAlarmReq) error

	// DeleteLeasedAlarm deletes an alarm using an alarm lease object, finalizing the occurrence the lease's holder just executed.
	// A lease whose actor released it by deactivating is accepted, because an actor that halts itself from its own handler drops the leases of its own alarms, and the occurrence being finalized must not be left behind to be delivered again.
	// Replacing an alarm by name mints a new alarm ID, so a lease can never name a row its holder did not execute.
	// Returns ErrNoAlarm if the alarm doesn't exist, or the lease expired or belongs to someone else.
	DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error

	// GetState retrieves the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	GetState(ctx context.Context, ref ref.ActorRef) ([]byte, error)

	// SetState sets the persistent state of an actor.
	SetState(ctx context.Context, ref ref.ActorRef, data []byte, opts SetStateOpts) error

	// DeleteState deletes the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	DeleteState(ctx context.Context, ref ref.ActorRef) error

	// ListStates returns the actors of a given type that have persistent state stored.
	// Results are ordered by actor ID in ascending order (expired state is omitted).
	ListStates(ctx context.Context, req ListStatesReq) (ListStatesRes, error)

	// Backup writes a portable, versioned snapshot of all persistent data (actor state, alarms, and dead-lettered jobs) to w.
	// It takes a consistent snapshot inside a transaction, so it can run while the cluster is online.
	Backup(ctx context.Context, w io.Writer) error

	// Restore wipes all existing persistent data (actor state, alarms, and dead-lettered jobs) and loads a snapshot produced by Backup from r.
	// It returns ErrHostsConnected if any host is currently connected, since restoring underneath live hosts would corrupt running actors.
	Restore(ctx context.Context, r io.Reader) error

	// HealthCheckPolicy returns a HealthCheckPolicy object for one sequence of host health checks
	HealthCheckPolicy() *HealthCheckPolicy

	// RenewLeaseInterval returns the recommended lease renewal interval for hosts.
	RenewLeaseInterval() time.Duration

	// AcquireExclusiveLease acquires or re-acquires the cluster exclusive-access lease for owner, extending it to now+ttl
	// The ClusterAdmin uses it to take exclusive access to a cluster for a maintenance operation such as a data restore
	// It returns ErrExclusiveHeld if a different owner currently holds a live (non-expired) lease
	AcquireExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error)

	// RenewExclusiveLease extends the exclusive-access lease for owner to now+ttl
	// It returns ErrExclusiveHeld if owner no longer holds a live lease, so the caller can treat the lease as lost
	RenewExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error)

	// ReleaseExclusiveLease clears the exclusive-access lease if it is held by owner
	// It is idempotent: releasing a lease this owner does not hold is not an error
	ReleaseExclusiveLease(ctx context.Context, owner string) error
}

// ProviderOptions is an empty interface implemented by all options structs for providers
type ProviderOptions any

// RegisterHostReq is the request object for the RegisterHost method.
type RegisterHostReq struct {
	// Host address, where
	Address string
	// List of supported actor types
	ActorTypes []ActorHostType
	// ExistingHostID, when non-empty, requests reattachment to an existing host registration with this ID
	// This is used when a host reconnects (for example after a runtime failover) and wants to reclaim its registration rather than starting over
	// If a host with this ID exists and its health record is still live, its address, supported actor types, and health check are refreshed in place, and the same host ID is returned with Reattached set to true
	// If no such host exists (or if its health record has expired), a brand-new registration is created with a freshly-generated host ID and Reattached is false
	// A host that sees Reattached false after having been registered before must drop every actor it's still holding
	ExistingHostID string
	// JoinToken is the jti from the JWT bootstrap token
	// When non-empty, the provider records it to prevent replay
	// Empty when the token carried no jti or no expiry, and for non-JWT auth paths
	JoinToken string
	// JoinTokenExpiresAt is the expiry time of the join token (zero when JoinToken is empty)
	JoinTokenExpiresAt time.Time
}

// RegisterHostRes is the response object for the RegisterHost method.
type RegisterHostRes struct {
	// Auto-generated ID of the actor host
	HostID string
	// Reattached is true if the registration reattached to an existing host (matching req.ExistingHostID)
	// If false, a new host registration was created
	Reattached bool
}

// UpdateActorHostReq is the request object for the UpdateActorHost method.
type UpdateActorHostReq struct {
	// Updates last health check time
	// If true, will update the value in the database with the current time
	UpdateLastHealthCheck bool
	// List of supported actor types
	// If non-nil, will replace all existing, registered actor types (an empty, non-nil slice indicates no supported actor types)
	ActorTypes []ActorHostType
	// Retry indicates the call repeats an earlier attempt that failed without a definitive answer
	Retry bool
}

// ActorHostType references a supported actor type.
type ActorHostType struct {
	// Actor type
	ActorType string
	// Idle timeout for the actor type
	// A negative value means no timeout
	IdleTimeout time.Duration
	// Maximum number of actors of the given type active on the current host
	// Set to 0 for no limit
	ConcurrencyLimit int32
	// Actor deactivation timeout
	DeactivationTimeout time.Duration
	// Maximum number of attempts when invoking the actor or executing alarms
	MaxAttempts int
	// CompletedJobRetention is how long a job of this actor type keeps a record after it completes successfully
	// Zero keeps no record at all, a positive duration keeps one for that long, and a negative duration keeps one that never expires
	CompletedJobRetention time.Duration
	// DeadLetteredJobRetention is how long a job of this actor type keeps its record after it is dead-lettered
	// A dead-lettered job is always recorded, since dropping a failure silently is never useful: a positive duration expires the record after that long, and zero or a negative duration keeps it until something removes it
	DeadLetteredJobRetention time.Duration
	// Initial retry delay after failed invocation attempts
	InitialRetryDelay time.Duration
}

// CompletedJobRecord reports whether a completed job of this type leaves a record, and the retention to store it with.
// The two are separate because a provider expresses "never expires" as a zero retention, which is also what "no record" would look like as a single number.
func (t ActorHostType) CompletedJobRecord() (record bool, retention time.Duration) {
	switch {
	case t.CompletedJobRetention == 0:
		return false, 0
	case t.CompletedJobRetention < 0:
		return true, 0
	default:
		return true, t.CompletedJobRetention
	}
}

// DeadLetteredJobRecordRetention returns the retention a dead-lettered record of this type is stored with, where zero means it never expires.
func (t ActorHostType) DeadLetteredJobRecordRetention() time.Duration {
	if t.DeadLetteredJobRetention < 0 {
		return 0
	}
	return t.DeadLetteredJobRetention
}

// HostInfo describes a registered, healthy actor host returned by ListHosts
type HostInfo struct {
	// Host ID
	HostID string
	// Host address (including port)
	Address string
	// Time of the host's last health check
	LastHealthCheck time.Time
}

// LookupActorOpts contains options for LookupActor.
type LookupActorOpts struct {
	// List of hosts on which the actor can be activated.
	// If the actor is active on a different host, ErrNoActorHost is returned.
	Hosts []string
	// If true, performs a lookup for an actor that's currently active only
	ActiveOnly bool
}

// LookupActorRes is the response object for the LookupActor method.
type LookupActorRes struct {
	// Host ID
	HostID string
	// Host address (including port)
	Address string
	// Actor idle timeout
	// Note: this is the absolute idle timeout, and not the remaining lifetime of the actor
	IdleTimeout time.Duration
}

// AlarmKind discriminates a row in the alarms table as a plain alarm or a dispatched job.
type AlarmKind string

const (
	// AlarmKindAlarm is a plain alarm, delivered to the actor's Alarm method.
	AlarmKindAlarm AlarmKind = "alarm"
	// AlarmKindJob is a dispatched job, delivered to the actor's Job method.
	AlarmKindJob AlarmKind = "job"
)

// GetAlarmRes is the response object for the GetAlarm method.
type GetAlarmRes struct {
	ref.AlarmProperties

	// Kind discriminates a plain alarm from a job
	Kind AlarmKind
	// JobMethod is the job handler method, set only for jobs
	JobMethod string
}

// SetAlarmReq is the request object for the SetAlarm method.
type SetAlarmReq struct {
	ref.AlarmProperties

	// Kind discriminates a plain alarm from a job
	// An empty value is treated as a plain alarm
	Kind AlarmKind
	// JobMethod is the job handler method, set only for jobs
	JobMethod string
	// LeaseImmediate lists the hosts eligible to own an immediate lease when the alarm is within the provider's fetch-ahead interval
	// An empty list or an alarm outside the interval does not attempt to acquire a lease
	LeaseImmediate []string
}

// FetchAndLeaseUpcomingAlarmsReq is the request object for the FetchAndLeaseUpcomingAlarms method.
type FetchAndLeaseUpcomingAlarmsReq struct {
	// Limits to alarms that can be fetched on these hosts.
	Hosts []string
}

// RenewAlarmLeasesReq is the request object for the RenewAlarmLeases method.
type RenewAlarmLeasesReq struct {
	// Limits to alarms owned by these hosts.
	Hosts []string
	// Optional list of leases to renew.
	// If this is empty, renews the lease for all alarms on the host.
	Leases []*ref.AlarmLease
}

// RenewAlarmLeasesRes is the response object for the RenewAlarmLeases method.
type RenewAlarmLeasesRes struct {
	// List of leases that were successfully renewed.
	Leases []*ref.AlarmLease
}

// GetLeasedAlarmRes is the response object for the GetLeasedAlarm method.
type GetLeasedAlarmRes struct {
	ref.AlarmRef
	ref.AlarmProperties

	// Kind discriminates a plain alarm from a job
	// This is the linchpin that lets the execution path call Job vs Alarm
	Kind AlarmKind
	// JobMethod is the job handler method, set only for jobs
	JobMethod string
}

// UpdateLeasedAlarmReq is the request object for the UpdateLeasedAlarm method.
type UpdateLeasedAlarmReq struct {
	// Due time.
	DueTime time.Time
	// When true, preserves and refreshes the lease on the alarm.
	// The default behavior is to release the lease.
	RefreshLease bool
}

// SetStateOpts contains options for SetState
type SetStateOpts struct {
	TTL time.Duration
	// WorkflowLabels, when set, is stored in the state row itself, so it carries the row's expiration and is replaced with it.
	// Nil removes whatever the row had.
	WorkflowLabels *WorkflowLabels
}

// ListStatesReq is the request object for the ListStates method.
type ListStatesReq struct {
	// Actor type whose stored states are listed
	ActorType string
	// When true, the stored state data is returned alongside each actor ID
	IncludeData bool
	// WorkflowLabels, when set, restricts the listing to rows whose labels match every field it sets, by equality.
	WorkflowLabels *WorkflowLabels
	// Pagination cursor: only actor IDs sorting strictly after this value are returned
	After string
	// Maximum number of states to return
	// Zero means DefaultListStatesLimit, and values above MaxListStatesLimit are capped
	Limit int
}

// EffectiveLimit returns the number of states the provider should return for this request, applying the default when the caller didn't set a limit and the cap when it asked for too many.
func (r ListStatesReq) EffectiveLimit() int {
	switch {
	case r.Limit <= 0:
		return DefaultListStatesLimit
	case r.Limit > MaxListStatesLimit:
		return MaxListStatesLimit
	default:
		return r.Limit
	}
}

// Names of the workflow label fields, as they are stored in the JSON object and as the indexes created by the providers' migrations extract them
const (
	WorkflowLabelStatus  = "status"
	WorkflowLabelVersion = "version"
	WorkflowLabelParent  = "parent"
)

// WorkflowLabels is the fixed set of fields the workflow engine stores alongside an instance's journal so that instances can be listed without reading every journal
// This list is fixed and not meant for general purpose labeling (editing fields requires updating migrations that include indexes)
// As a filter on ListStates, a field left at its zero value is not matched on, so the zero value matches every row that has labels at all
type WorkflowLabels struct {
	// Status is the instance's status
	Status string `json:"status,omitempty"`
	// Version is the version of the definition the instance is running, which is always positive for a stored instance
	// Note this is stored as string
	Version int `json:"version,string,omitempty"`
	// Parent is the instance ID of the parent instance, empty for a top-level instance
	Parent string `json:"parent,omitempty"`
}

// IsZero reports whether no field is set
func (l WorkflowLabels) IsZero() bool {
	return l.Status == "" && l.Version == 0 && l.Parent == ""
}

// JSON encodes the labels as the JSON object a provider stores in the row's label column
// It returns an empty string when no field is set
func (l WorkflowLabels) JSON() (string, error) {
	if l.IsZero() {
		return "", nil
	}

	res, err := json.Marshal(l)
	if err != nil {
		return "", fmt.Errorf("failed to encode the workflow labels: %w", err)
	}

	return string(res), nil
}

// Fields returns the label fields that are set, keyed by the names above, with every value rendered as the string the stored JSON holds
// A provider that filters per field iterates this rather than reaching for the struct fields one at a time
func (l WorkflowLabels) Fields() map[string]string {
	res := make(map[string]string, 3)
	if l.Status != "" {
		res[WorkflowLabelStatus] = l.Status
	}
	if l.Version != 0 {
		res[WorkflowLabelVersion] = strconv.Itoa(l.Version)
	}
	if l.Parent != "" {
		res[WorkflowLabelParent] = l.Parent
	}
	return res
}

// DecodeWorkflowLabels reads the labels back from the JSON object a provider stored them as, returning nil when the column held nothing.
func DecodeWorkflowLabels(data []byte) (*WorkflowLabels, error) {
	if len(data) == 0 {
		return nil, nil
	}

	res := &WorkflowLabels{}
	err := json.Unmarshal(data, res)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the workflow labels: %w", err)
	}

	return res, nil
}

// ListStatesRes is the response object for the ListStates method.
type ListStatesRes struct {
	// States in this page, ordered by actor ID in ascending order
	States []ActorStateInfo
	// HasMore is true when the collection contains more states after the last one in this page
	HasMore bool
}

// ActorStateInfo describes the stored state of a single actor returned by ListStates.
type ActorStateInfo struct {
	// ID of the actor the state belongs to
	ActorID string
	// Stored state data, populated only when the request set IncludeData
	Data []byte
}

// JobStatus is the provider-level lifecycle stage of a job.
type JobStatus string

const (
	// JobStatusPending indicates a live job that is scheduled and not currently leased
	JobStatusPending JobStatus = "pending"
	// JobStatusActive indicates a live job that currently holds a valid lease
	JobStatusActive JobStatus = "active"
	// JobStatusCompleted indicates a job that ran successfully and whose record was retained
	JobStatusCompleted JobStatus = "completed"
	// JobStatusDeadLettered indicates a job that exhausted its retries or failed permanently
	JobStatusDeadLettered JobStatus = "dead"
)

// IsTerminal reports whether the status is one a job ends in, and therefore one held in the terminal-job store rather than among the live jobs
func (s JobStatus) IsTerminal() bool {
	return s == JobStatusCompleted || s == JobStatusDeadLettered
}

// JobInfo describes a job, spanning both live and terminal jobs.
// Attempts is only populated for a terminal job, and LastError only for one that dead-lettered.
type JobInfo struct {
	JobID     string
	ActorType string
	ActorID   string
	Method    string
	Status    JobStatus
	DueTime   time.Time
	Interval  string
	Cron      string
	Attempts  int
	LastError string
	CreatedAt time.Time
	// EndedAt is when the job reached its terminal status, and is zero for a live job
	EndedAt time.Time
}

// GetTerminalJobRes is the response object for the GetTerminalJob method.
// It carries the raw input data so a dead job can be re-dispatched.
type GetTerminalJobRes struct {
	JobID       string
	ActorType   string
	ActorID     string
	Method      string
	Data        []byte
	Status      JobStatus
	Attempts    int
	LastError   string
	EndedAt     time.Time
	OriginalDue time.Time
	Interval    string
	Cron        string
	// Expiration is when the record is garbage collected, and is nil for one kept until something removes it
	Expiration *time.Time
}

// DeadLetterAlarmReq is the request object for the DeadLetterAlarm method.
type DeadLetterAlarmReq struct {
	// Reason is the last error message recorded with the dead job
	Reason string
	// Attempts is the number of attempts made before dead-lettering
	Attempts int
	// Retention is how long the record is kept before it is garbage collected
	// Zero keeps it until something removes it, which is what an actor type asks for with a negative DeadLetteredJobRetention
	Retention time.Duration
	// Reschedule, when true, re-creates the alarm for its next occurrence in the same transaction, under the same job ID
	// This is how a repeating job's recurrence survives the dead-lettering of one occurrence
	Reschedule bool
	// NextDueTime is the due time of the rescheduled occurrence, used only when Reschedule is true
	NextDueTime time.Time
}

// CompleteJobReq is the request object for the CompleteJob method.
type CompleteJobReq struct {
	// Attempts is the number of attempts the occurrence took to succeed
	Attempts int
	// Retention is how long the record is kept before it is garbage collected
	// It is always set, since a job whose actor type asked for no retention is deleted outright rather than recorded here
	Retention time.Duration
	// Reschedule, when true, re-creates the alarm for its next occurrence in the same transaction, under the same job ID
	// This is how each occurrence of a repeating job can leave a record without stopping the recurrence
	Reschedule bool
	// NextDueTime is the due time of the rescheduled occurrence, used only when Reschedule is true
	NextDueTime time.Time
}

// JobCreatedAt extracts the creation time embedded in a UUIDv7 job ID.
// Job IDs are UUIDv7 values whose first 48 bits are the Unix-millisecond creation timestamp, so a separate created-at column is not needed.
// It returns the zero time if the ID is not a parseable UUID.
func JobCreatedAt(jobID string) time.Time {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return time.Time{}
	}

	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 | int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	return time.UnixMilli(ms)
}
