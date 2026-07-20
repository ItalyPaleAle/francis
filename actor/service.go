package actor

import (
	"context"
	"errors"
	"io"

	"github.com/italypaleale/francis/internal/ref"
)

var (
	// ErrStateNotFound is returned by GetState and DeleteState when the object cannot be found.
	ErrStateNotFound = errors.New("state not found for actor")
	// ErrAlarmNotFound is returned by DeleteAlarm when the alarm cannot be found.
	ErrAlarmNotFound = errors.New("alarm not found")
	// ErrActorNotHosted is returned by Halt when the actor is not active on the current host.
	ErrActorNotHosted = errors.New("actor is not active on the current host")
	// ErrActorNotActive is returned by Invoke when requesting to invoke only active actors and the actor isn't active.
	ErrActorNotActive = errors.New("actor is not currently active")
	// ErrActorHalted is returned by methods that perform invocation when the actor is halted on the host where it was previously active.
	// Callers should retry after a delay.
	ErrActorHalted = errors.New("actor is halted")
	// ErrActorTypeUnsupported is returned by methods that perform invocation when the actor type is not supported for this cluster.
	ErrActorTypeUnsupported = errors.New("actor type is not supported in the cluster")
	// ErrActorTypeReserved is returned by the Service methods that target an actor by type when that type is a built-in actor type, which clients cannot invoke or operate on directly.
	ErrActorTypeReserved = errors.New("cannot target a built-in actor type directly")
	// ErrMethodReserved is returned by the Service and Client invocation methods when the method name is reserved for a framework lifecycle (such as bootstrap), which clients cannot invoke directly.
	ErrMethodReserved = errors.New("cannot invoke a reserved framework method directly")
	// ErrNoHost is returned by methods that perform invocation when no host is currently available to place the actor
	ErrNoHost = errors.New("no host is available to place the actor")
	// ErrServiceNotInitialized is returned by Service methods when the Service was not created via NewService and therefore has no host to delegate to
	ErrServiceNotInitialized = errors.New("actor service is not initialized; use NewService")
	// ErrJobPermanentFailure is returned from an actor's Job method to skip the remaining retries and dead-letter the job immediately.
	ErrJobPermanentFailure = errors.New("job failed permanently")
	// ErrJobRejected is returned from an actor's Job method to decline the occurrence on this host without failing it, so the framework re-routes it to another host.
	// Unlike a returned error it does not count as a failed attempt and never dead-letters the job, and unlike ErrJobPermanentFailure the work is not abandoned but retried elsewhere.
	ErrJobRejected = errors.New("job rejected by host, will be re-routed")
	// ErrJobNotFound is returned by job methods when the job cannot be found.
	ErrJobNotFound = errors.New("job not found")
	// ErrReadOnly is returned by Client methods that mutate state, alarms, or jobs when called from within a Peek/PeekStream invocation.
	ErrReadOnly = errors.New("cannot modify state during a read-only (Peek) invocation")
)

// Service allows interacting with the actor host, to invoke actors and perform operations on state and alarms.
type Service struct {
	host Host
}

// NewService returns a new Service configured to interact with specific Host.
func NewService(host Host) *Service {
	return &Service{
		host: host,
	}
}

// ready reports whether the Service has a host to delegate to
// A zero-value or nil Service was never wired to a host, so calling through it would panic
func (s *Service) ready() bool {
	return s != nil && s.host != nil
}

// Invoke an actor
// Pass WithInvokeActiveOnly to invoke the actor only if it is already active, without activating it, in which case ErrActorNotActive is returned when it is not
func (s *Service) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error) {
	if ref.IsBuiltInActorType(actorType) {
		return nil, ErrActorTypeReserved
	}
	if ref.IsReservedMethod(method) {
		return nil, ErrMethodReserved
	}

	return s.invoke(ctx, actorType, actorID, method, data, opts...)
}

// invoke is the unguarded Invoke used by the in-actor client and by the built-in actor lifecycle, which are allowed to target built-in actors
func (s *Service) invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error) {
	if !s.ready() {
		return nil, ErrServiceNotInitialized
	}

	return s.host.Invoke(ctx, actorType, actorID, method, data, opts...)
}

// InvokeStream performs a streamed invocation of an actor.
// The request body is streamed from body, and the response body is returned as a reader that the caller must close.
// Pass WithInvokeActiveOnly to invoke the actor only if it is already active, without activating it, in which case ErrActorNotActive is returned when it is not
func (s *Service) InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...InvokeOption) (respContentType string, resp io.ReadCloser, err error) {
	if ref.IsBuiltInActorType(actorType) {
		return "", nil, ErrActorTypeReserved
	}
	if ref.IsReservedMethod(method) {
		return "", nil, ErrMethodReserved
	}

	if !s.ready() {
		return "", nil, ErrServiceNotInitialized
	}

	return s.host.InvokeStream(ctx, actorType, actorID, method, reqContentType, body, opts...)
}

// Peek performs a read-only invocation of an actor.
// Unlike Invoke, concurrent Peek calls against the same actor can run at the same time (however a Peek and an Invoke never overlap).
// The actor must implement ActorPeek to be called this way.
// Pass WithInvokeActiveOnly to peek the actor only if it is already active, without activating it, in which case ErrActorNotActive is returned when it is not
func (s *Service) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error) {
	if ref.IsBuiltInActorType(actorType) {
		return nil, ErrActorTypeReserved
	}
	if ref.IsReservedMethod(method) {
		return nil, ErrMethodReserved
	}

	return s.peek(ctx, actorType, actorID, method, data, opts...)
}

// peek is the unguarded Peek used by the in-actor client, which is allowed to target built-in actors
func (s *Service) peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error) {
	if !s.ready() {
		return nil, ErrServiceNotInitialized
	}

	return s.host.Peek(ctx, actorType, actorID, method, data, opts...)
}

// PeekStream performs a streamed, read-only invocation of an actor.
// The request body is streamed from body, and the response body is returned as a reader that the caller must close.
// The actor must implement ActorPeekStream to be called this way.
// Pass WithInvokeActiveOnly to peek the actor only if it is already active, without activating it, in which case ErrActorNotActive is returned when it is not
func (s *Service) PeekStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...InvokeOption) (respContentType string, resp io.ReadCloser, err error) {
	if ref.IsBuiltInActorType(actorType) {
		return "", nil, ErrActorTypeReserved
	}
	if ref.IsReservedMethod(method) {
		return "", nil, ErrMethodReserved
	}

	if !s.ready() {
		return "", nil, ErrServiceNotInitialized
	}

	return s.host.PeekStream(ctx, actorType, actorID, method, reqContentType, body, opts...)
}

// SetState saves the state for an actor.
func (s *Service) SetState(ctx context.Context, actorType string, actorID string, state any, opts *SetStateOpts) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.setState(ctx, actorType, actorID, state, opts)
}

// setState is the unguarded SetState used by the in-actor client, which is allowed to target built-in actors
func (s *Service) setState(ctx context.Context, actorType string, actorID string, state any, opts *SetStateOpts) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.SetState(ctx, actorType, actorID, state, opts)
}

// GetState retrieves the state for an actor.
// The state is JSON-decoded into dest.
// Returns ErrStateNotFound if the state cannot be found.
func (s *Service) GetState(ctx context.Context, actorType string, actorID string, dest any) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.getState(ctx, actorType, actorID, dest)
}

// getState is the unguarded GetState used by the in-actor client, which is allowed to target built-in actors
func (s *Service) getState(ctx context.Context, actorType string, actorID string, dest any) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.GetState(ctx, actorType, actorID, dest)
}

// DeleteState deletes the state for an actor.
// Returns ErrStateNotFound if the state cannot be found.
func (s *Service) DeleteState(ctx context.Context, actorType string, actorID string) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.deleteState(ctx, actorType, actorID)
}

// deleteState is the unguarded DeleteState used by the in-actor client, which is allowed to target built-in actors
func (s *Service) deleteState(ctx context.Context, actorType string, actorID string) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.DeleteState(ctx, actorType, actorID)
}

// SetAlarm creates or replaces an alarm for an actor.
func (s *Service) SetAlarm(ctx context.Context, actorType string, actorID string, alarmName string, properties AlarmProperties) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.setAlarm(ctx, actorType, actorID, alarmName, properties)
}

// setAlarm is the unguarded SetAlarm used by the in-actor client and by the built-in actor lifecycle, which are allowed to target built-in actors
func (s *Service) setAlarm(ctx context.Context, actorType string, actorID string, alarmName string, properties AlarmProperties) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.SetAlarm(ctx, actorType, actorID, alarmName, properties)
}

// DeleteAlarm deletes an alarm for an actor.
// Returns ErrAlarmNotFound if the alarm cannot be found.
func (s *Service) DeleteAlarm(ctx context.Context, actorType string, actorID string, alarmName string) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.deleteAlarm(ctx, actorType, actorID, alarmName)
}

// deleteAlarm is the unguarded DeleteAlarm used by the in-actor client, which is allowed to target built-in actors
func (s *Service) deleteAlarm(ctx context.Context, actorType string, actorID string, alarmName string) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.DeleteAlarm(ctx, actorType, actorID, alarmName)
}

// Dispatch sends a durable, fire-and-forget job to a specific actor.
// The job is delivered to the actor's Job method on whatever host serves the actor type, and is retried automatically and dead-lettered on permanent failure.
// It returns the server-issued job ID.
func (s *Service) Dispatch(ctx context.Context, actorType string, actorID string, method string, input any, opts ...JobOption) (jobID string, err error) {
	if ref.IsBuiltInActorType(actorType) {
		return "", ErrActorTypeReserved
	}

	return s.dispatch(ctx, actorType, actorID, method, input, opts...)
}

// dispatch is the unguarded Dispatch used by the in-actor client and by the built-in actor lifecycle, which are allowed to target built-in actors
func (s *Service) dispatch(ctx context.Context, actorType string, actorID string, method string, input any, opts ...JobOption) (jobID string, err error) {
	if !s.ready() {
		return "", ErrServiceNotInitialized
	}

	properties, err := newJobProperties(opts...)
	if err != nil {
		return "", err
	}

	return s.host.Dispatch(ctx, actorType, actorID, method, input, properties)
}

// GetJob returns the information for a job by its ID, spanning both live and dead-lettered jobs.
// Returns ErrJobNotFound if the job cannot be found.
func (s *Service) GetJob(ctx context.Context, jobID string) (JobInfo, error) {
	if !s.ready() {
		return JobInfo{}, ErrServiceNotInitialized
	}

	return s.host.GetJob(ctx, jobID)
}

// ListJobs returns all live and dead-lettered jobs for an actor.
func (s *Service) ListJobs(ctx context.Context, actorType string, actorID string) ([]JobInfo, error) {
	if ref.IsBuiltInActorType(actorType) {
		return nil, ErrActorTypeReserved
	}

	return s.listJobs(ctx, actorType, actorID)
}

// listJobs is the unguarded ListJobs used by the in-actor client, which is allowed to target built-in actors
func (s *Service) listJobs(ctx context.Context, actorType string, actorID string) ([]JobInfo, error) {
	if !s.ready() {
		return nil, ErrServiceNotInitialized
	}

	return s.host.ListJobs(ctx, actorType, actorID)
}

// CancelJob cancels a live (pending or active) job for an actor.
// Returns ErrJobNotFound if the job cannot be found among live jobs.
func (s *Service) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	return s.cancelJob(ctx, actorType, actorID, jobID)
}

// cancelJob is the unguarded CancelJob used by the in-actor client, which is allowed to target built-in actors
func (s *Service) cancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.CancelJob(ctx, actorType, actorID, jobID)
}

// RetryJob re-dispatches a dead-lettered job, scheduled to run as soon as possible.
// It returns the ID of the newly dispatched job and removes the dead-letter record.
// Returns ErrJobNotFound if the dead job cannot be found.
func (s *Service) RetryJob(ctx context.Context, jobID string) (newJobID string, err error) {
	if !s.ready() {
		return "", ErrServiceNotInitialized
	}

	return s.host.RetryJob(ctx, jobID)
}

// HaltAll halts all actors currently active on the host.
func (s *Service) HaltAll() error {
	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.HaltAll()
}

// Halt halts one actor currently active on the host.
func (s *Service) Halt(actorType string, actorID string) error {
	if ref.IsBuiltInActorType(actorType) {
		return ErrActorTypeReserved
	}

	if !s.ready() {
		return ErrServiceNotInitialized
	}

	return s.host.Halt(actorType, actorID)
}

// HaltDeferred halts an actor currently active on the host.
// This is a non-blocking variant of the Halt method, which runs in background
// It is a no-op on an uninitialized Service, since there is no host to halt against and the method has no way to report an error
func (s *Service) HaltDeferred(actorType string, actorID string) {
	// HaltDeferred cannot report an error, so a built-in target is silently ignored rather than returning ErrActorTypeReserved
	if ref.IsBuiltInActorType(actorType) {
		return
	}

	s.haltDeferred(actorType, actorID)
}

// haltDeferred is the unguarded HaltDeferred used by the in-actor client, which is allowed to target built-in actors
func (s *Service) haltDeferred(actorType string, actorID string) {
	if !s.ready() {
		return
	}

	s.host.HaltDeferred(actorType, actorID)
}
