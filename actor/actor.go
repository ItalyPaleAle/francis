package actor

import (
	"context"
)

// SingletonActorID is the well-known actor ID of a singleton actor's cluster-wide instance.
// A singleton actor is reached at this fixed ID from every host, so all callers target the same instance, and it is the instance the host bootstraps at startup (see ActorBootstrapper).
const SingletonActorID = "singleton"

// Actor is an alias for "any".
// It's included for convenience.
type Actor = any

// ActorInvoke can be implemented by actors that offer the Invoke method.
type ActorInvoke interface {
	// Invoke is called when an actor is invoked.
	Invoke(ctx context.Context, method string, data Envelope) (any, error)
}

// ActorPeek can be implemented by actors that offer the read-only Peek method.
// Peek is the read-side analogue of Invoke: the framework runs it under the actor's shared (read) lock, so multiple Peek calls can run concurrently with each other, while still excluding any in-flight Invoke.
// The framework rejects state-mutating client calls made from within Peek, but it cannot stop a handler from mutating its own in-memory fields, so Peek implementations must treat the actor as read-only themselves.
type ActorPeek interface {
	// Peek is called when an actor is peeked.
	Peek(ctx context.Context, method string, data Envelope) (any, error)
}

// ActorAlarm can be implemented by actors that offer the Alarm method.
type ActorAlarm interface {
	// Alarm is invoked upon execution of an alarm.
	// The parameter "data" is an envelope that allows decoding the associated data into a custom object
	// It could be nil if there's no data
	Alarm(ctx context.Context, name string, data Envelope) error
}

// ActorBootstrapper can be implemented by an actor that needs one-time setup once the host is ready.
// It is primarily useful for singleton actors: the host invokes Bootstrap on the actor's SingletonActorID instance at startup, routed through placement so it runs on the single owning host at a time and is serialized by that instance's turn lock, exactly like a normal invocation.
// Every host triggers it, so Bootstrap must be idempotent (for example, registering a durable recurring job only if one is not already set up).
// Register such an actor with the host's RegisterSingletonActor so the host knows to bootstrap it.
// The data argument carries the optional bootstrap payload supplied when the actor was registered via RegisterSingletonActorOptions.BootstrapData (may be nil).
type ActorBootstrapper interface {
	// Bootstrap is called once the host is ready, to set up the actor's durable work.
	Bootstrap(ctx context.Context, data Envelope) error
}

// ActorJob can be implemented by actors that receive dispatched jobs.
type ActorJob interface {
	// Job is invoked upon execution of a dispatched job.
	// The parameter "method" identifies the job handler, and "data" is an envelope that allows decoding the associated input into a custom object.
	// Returning ErrJobPermanentFailure skips the remaining retries and dead-letters the job immediately.
	// Returning ErrJobRejected declines the occurrence on this host without failing it, so the job is re-routed to another host without counting an attempt.
	Job(ctx context.Context, method string, data Envelope) error
}

// ActorJobFailed can be implemented by actors that want a reaction after one of their jobs is dead-lettered.
type ActorJobFailed interface {
	// JobFailed is called best-effort after a job is recorded in the dead-letter store.
	// It is never itself dead-lettered: the dead-letter record is the source of truth, and an error here is only logged.
	JobFailed(ctx context.Context, jobID string, method string, data Envelope, jobErr error) error
}

// ActorDeactivate can be implemented by actors that offer the Deactivate method.
type ActorDeactivate interface {
	// Deactivate is invoked upon actor deactivation
	Deactivate(ctx context.Context) error
}

// Factory is a function that initializes a new actor.
type Factory func(actorID string, service *Service) Actor

// Envelope allows retrieving data and decoding it into a custom object
type Envelope interface {
	Decode(into any) error
}
