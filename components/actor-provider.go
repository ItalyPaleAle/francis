package components

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/internal/ref"
)

const (
	DefaultHostHealthCheckDeadline   = 20 * time.Second
	DefaultAlarmsLeaseDuration       = 20 * time.Second
	DefaultAlarmsFetchAheadInterval  = 2500 * time.Millisecond
	DefaultAlarmsFetchAheadBatchSize = 25
)

// ActorProvider is the interface implemented by all actor providers
type ActorProvider interface {
	// Init the actor provider
	Init(ctx context.Context) error

	// Run the actor provider
	// This method blocks until the context is canceled
	// If the provider is already running, returns ErrAlreadyRunning
	Run(ctx context.Context) error

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

	// SetAlarm sets or replaces an alarm configured for an actor.
	SetAlarm(ctx context.Context, ref ref.AlarmRef, req SetAlarmReq) error

	// DeleteAlarm removes an alarm configured for an actor.
	// If the alarm doesn't exist, returns ErrNoAlarm.
	DeleteAlarm(ctx context.Context, ref ref.AlarmRef) error

	// DispatchJob creates a job as an alarm row with Kind = job, returning the job ID (the alarm_id).
	// When req carries an alarm name (an idempotency key), a job with the same (actor_type, actor_id, name) is kept and its existing job ID is returned, so re-dispatching with the same key is idempotent (first-write-wins).
	DispatchJob(ctx context.Context, ref ref.AlarmRef, req SetAlarmReq) (jobID string, err error)

	// DeadLetterAlarm atomically moves a leased job from the alarms table to the dead_jobs store.
	// When req.Reschedule is set, the recurrence is re-created for its next occurrence in the same transaction, so a repeating job survives the dead-lettering of one occurrence.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req DeadLetterAlarmReq) error

	// GetJob returns the information for a job by its ID, spanning both live jobs (in the alarms table) and dead-lettered jobs.
	// Returns ErrNoJob if the job cannot be found.
	GetJob(ctx context.Context, jobID string) (JobInfo, error)

	// ListJobs returns all live and dead-lettered jobs for an actor.
	ListJobs(ctx context.Context, actorType string, actorID string) ([]JobInfo, error)

	// CancelJob deletes a live job (in the alarms table) by its ID.
	// Returns ErrNoJob if the job cannot be found among live jobs.
	CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error

	// GetDeadJob returns a dead-lettered job by its ID, including its raw input data.
	// Returns ErrNoJob if the dead job cannot be found.
	GetDeadJob(ctx context.Context, jobID string) (GetDeadJobRes, error)

	// DeleteDeadJob removes a dead-lettered job by its ID.
	// Returns ErrNoJob if the dead job cannot be found.
	DeleteDeadJob(ctx context.Context, jobID string) error

	// RetryDeadJob atomically re-dispatches a dead-lettered job as a fresh, immediate one-shot job and removes its dead-letter record, returning the new job ID.
	// The re-dispatch and the removal happen in a single transaction, so a crash can never leave both a replayed job and its dead-letter record behind.
	// Returns ErrNoJob if the dead job cannot be found.
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

	// DeleteLeasedAlarm deletes an alarm using an alarm lease object.
	// Returns ErrNoAlarm if the alarm doesn't exist or the lease is not valid.
	DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error

	// GetState retrieves the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	GetState(ctx context.Context, ref ref.ActorRef) ([]byte, error)

	// SetState sets the persistent state of an actor.
	SetState(ctx context.Context, ref ref.ActorRef, data []byte, opts SetStateOpts) error

	// DeleteState deletes the persistent state of an actor.
	// If there's no state, returns ErrNoState.
	DeleteState(ctx context.Context, ref ref.ActorRef) error

	// Backup writes a portable, versioned snapshot of all persistent data (actor state, alarms, and dead-lettered jobs) to w.
	// It takes a consistent snapshot inside a transaction, so it can run while the cluster is online.
	Backup(ctx context.Context, w io.Writer) error

	// Restore wipes all existing persistent data (actor state, alarms, and dead-lettered jobs) and loads a snapshot produced by Backup from r.
	// It returns ErrHostsConnected if any host is currently connected, since restoring underneath live hosts would corrupt running actors.
	Restore(ctx context.Context, r io.Reader) error

	// HealthCheckInterval returns the recommended health check interval for hosts.
	HealthCheckInterval() time.Duration

	// HealthCheckInterval returns the recommended lease renewal interval for hosts.
	RenewLeaseInterval() time.Duration
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
	// This is used when a host reconnects (for example after a runtime failover) and wants to reclaim its registration rather than waiting for the previous health record to expire
	// If a host with this ID exists, its address, supported actor types, and health check are refreshed in place, regardless of the previous record's health, and the same host ID is returned with Reattached set to true
	// If no such host exists (for example it was already garbage-collected), a brand-new registration is created with a freshly-generated host ID and Reattached is false
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
	// Initial retry delay after failed invocation attempts
	InitialRetryDelay time.Duration
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
}

// JobStatus is the provider-level lifecycle stage of a job.
type JobStatus string

const (
	// JobStatusPending indicates a live job that is scheduled and not currently leased
	JobStatusPending JobStatus = "pending"
	// JobStatusActive indicates a live job that currently holds a valid lease
	JobStatusActive JobStatus = "active"
	// JobStatusDeadLettered indicates a job that was recorded in the dead-letter store
	JobStatusDeadLettered JobStatus = "dead"
)

// JobInfo describes a job, spanning both live and dead-lettered jobs.
// Attempts and LastError are only populated for dead-lettered jobs.
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
}

// GetDeadJobRes is the response object for the GetDeadJob method.
// It carries the raw input data so a dead job can be re-dispatched.
type GetDeadJobRes struct {
	JobID       string
	ActorType   string
	ActorID     string
	Method      string
	Data        []byte
	Attempts    int
	LastError   string
	FailedAt    time.Time
	OriginalDue time.Time
	Interval    string
	Cron        string
}

// DeadLetterAlarmReq is the request object for the DeadLetterAlarm method.
type DeadLetterAlarmReq struct {
	// Reason is the last error message recorded with the dead job
	Reason string
	// Attempts is the number of attempts made before dead-lettering
	Attempts int
	// Reschedule, when true, re-creates the alarm for its next occurrence in the same transaction
	// This is how a repeating job's recurrence survives the dead-lettering of one occurrence
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
