package actor

import (
	"context"
	"errors"

	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
)

// Client allows interacting with the actor service, and it's pre-configured for the active actor
// A Client is bound to a single activation and a single invocation turn: it must not be shared across goroutines and must not be reused across activations or turns.
// GetState caches the fetched value in-memory so repeated calls within the same turn avoid redundant store reads.
type Client[T any] interface {
	// SetState saves the actor's state.
	SetState(ctx context.Context, state T, opts *SetStateOpts) error
	// GetState retrieves the actor's state.
	GetState(ctx context.Context) (state T, err error)
	// DeleteState deletes the actor's state.
	DeleteState(ctx context.Context) error
	// SetAlarm creates or replaces an alarm.
	SetAlarm(ctx context.Context, alarmName string, properties AlarmProperties) error
	// DeleteAlarm deletes an alarm.
	DeleteAlarm(ctx context.Context, alarmName string) error
	// Invoke synchronously invokes another actor and returns its response envelope.
	// Note: invoking your own actor from within its own turn deadlocks on the turn lock.
	Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error)
	// Dispatch sends a durable, fire-and-forget job to the current actor.
	Dispatch(ctx context.Context, method string, input any, opts ...JobOption) (jobID string, err error)
	// GetJob returns the information for a job by its ID, spanning both live and dead-lettered jobs.
	GetJob(ctx context.Context, jobID string) (JobInfo, error)
	// ListJobs returns all live and dead-lettered jobs for the current actor.
	ListJobs(ctx context.Context) ([]JobInfo, error)
	// CancelJob cancels a live job for the current actor.
	CancelJob(ctx context.Context, jobID string) error
	// RetryJob re-dispatches a dead-lettered job and returns the new job ID.
	RetryJob(ctx context.Context, jobID string) (newJobID string, err error)
	// Halt the current actor upon returning.
	Halt()
}

// client is the concrete implementation of Client
type client[T any] struct {
	actorID   string
	actorType string

	service *Service

	// privileged marks the framework's built-in actor client, which is allowed to operate on built-in actor types
	// A normal client rejects built-in targets with ErrActorTypeReserved, so application actors cannot reach built-in actors through it
	privileged bool

	// hasState is set once the state has been fetched or written so subsequent GetState calls within the same turn are served from the in-memory copy
	// This is safe because the turn-based lock guarantees no other caller modifies the actor's state while this turn is executing
	hasState bool
	state    T
}

// NewActorClient returns a new ActorClient object.
func NewActorClient[T any](actorType string, actorID string, service *Service) Client[T] {
	return &client[T]{
		actorType: actorType,
		actorID:   actorID,
		service:   service,
	}
}

// NewBuiltInActorClient returns a privileged client that is allowed to operate on built-in actor types, which the public client rejects.
// It is reserved for the framework's built-in actor machinery: the builtinkey.Key argument can only be supplied by francis-internal packages, since that package cannot be imported externally.
func NewBuiltInActorClient[T any](_ builtinkey.Key, actorType string, actorID string, service *Service) Client[T] {
	return &client[T]{
		actorType:  actorType,
		actorID:    actorID,
		service:    service,
		privileged: true,
	}
}

// canTarget reports whether this client may operate on the given actor type
// A normal client cannot target a built-in actor, while the privileged built-in client can
func (c *client[T]) canTarget(actorType string) bool {
	return c.privileged || !ref.IsBuiltInActorType(actorType)
}

// SetState saves the actor's state.
func (c *client[T]) SetState(ctx context.Context, state T, opts *SetStateOpts) error {
	if !c.canTarget(c.actorType) {
		return ErrActorTypeReserved
	}

	err := c.service.setState(ctx, c.actorType, c.actorID, state, opts)
	if err != nil {
		return err
	}

	// Store the state in the local object
	c.hasState = true
	c.state = state
	return nil
}

// GetState retrieves the actor's state.
func (c *client[T]) GetState(ctx context.Context) (state T, err error) {
	if !c.canTarget(c.actorType) {
		return state, ErrActorTypeReserved
	}

	if c.hasState {
		// Return from the local object
		return c.state, nil
	}

	err = c.service.getState(ctx, c.actorType, c.actorID, &state)
	// Ignore the error indicating the state can't be found, and return a zero state
	if err != nil && !errors.Is(err, ErrStateNotFound) {
		return state, err
	}

	// Store the state in the local object
	c.hasState = true
	c.state = state

	return state, nil
}

// DeleteState deletes the actor's state.
func (c *client[T]) DeleteState(ctx context.Context) error {
	if !c.canTarget(c.actorType) {
		return ErrActorTypeReserved
	}

	// We set "hasState" to indicate we have the cached state
	var zero T
	c.hasState = true
	c.state = zero

	return c.service.deleteState(ctx, c.actorType, c.actorID)
}

// SetAlarm creates or replaces an alarm.
func (c *client[T]) SetAlarm(ctx context.Context, alarmName string, properties AlarmProperties) error {
	if !c.canTarget(c.actorType) {
		return ErrActorTypeReserved
	}

	return c.service.setAlarm(ctx, c.actorType, c.actorID, alarmName, properties)
}

// DeleteAlarm deletes an alarm.
func (c *client[T]) DeleteAlarm(ctx context.Context, alarmName string) error {
	if !c.canTarget(c.actorType) {
		return ErrActorTypeReserved
	}

	return c.service.deleteAlarm(ctx, c.actorType, c.actorID, alarmName)
}

// Invoke synchronously invokes another actor and returns its response envelope.
func (c *client[T]) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error) {
	if !c.canTarget(actorType) {
		return nil, ErrActorTypeReserved
	}

	return c.service.invoke(ctx, actorType, actorID, method, data, opts...)
}

// Dispatch sends a durable, fire-and-forget job to the current actor.
func (c *client[T]) Dispatch(ctx context.Context, method string, input any, opts ...JobOption) (jobID string, err error) {
	if !c.canTarget(c.actorType) {
		return "", ErrActorTypeReserved
	}

	return c.service.dispatch(ctx, c.actorType, c.actorID, method, input, opts...)
}

// GetJob returns the information for a job by its ID, spanning both live and dead-lettered jobs.
func (c *client[T]) GetJob(ctx context.Context, jobID string) (JobInfo, error) {
	return c.service.GetJob(ctx, jobID)
}

// ListJobs returns all live and dead-lettered jobs for the current actor.
func (c *client[T]) ListJobs(ctx context.Context) ([]JobInfo, error) {
	if !c.canTarget(c.actorType) {
		return nil, ErrActorTypeReserved
	}

	return c.service.listJobs(ctx, c.actorType, c.actorID)
}

// CancelJob cancels a live job for the current actor.
func (c *client[T]) CancelJob(ctx context.Context, jobID string) error {
	if !c.canTarget(c.actorType) {
		return ErrActorTypeReserved
	}

	return c.service.cancelJob(ctx, c.actorType, c.actorID, jobID)
}

// RetryJob re-dispatches a dead-lettered job and returns the new job ID.
func (c *client[T]) RetryJob(ctx context.Context, jobID string) (newJobID string, err error) {
	return c.service.RetryJob(ctx, jobID)
}

// Halt the current actor upon returning.
func (c *client[T]) Halt() {
	if !c.canTarget(c.actorType) {
		return
	}

	// We use the deferred halt because otherwise it would block the current goroutine, causing a deadlock.
	c.service.haltDeferred(c.actorType, c.actorID)
}
