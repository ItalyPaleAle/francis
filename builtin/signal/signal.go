// Package signal provides a built-in actor that broadcasts a one-shot signal to any number of waiting callers.
//
// Build one with New and register the result on a host with the host's RegisterBuiltInActor method, then obtain a SignalService with Service to call Wait and Complete.
// Each signal is independent: there is one actor instance per signal ID, which could be a free-form string such as a deployment ID or a job ID.
//
// A signal fires once and callers block in Wait until it does.
// One caller fires it with Complete, optionally attaching a payload, and every waiter is released at that moment with that payload.
// A caller that arrives after the completion does not block at all: it is answered from the durable completion record, for as long as the retention window set with WithRetention.
//
// That makes a signal level-triggered rather than edge-triggered: a caller that loses its connection and calls Wait again is answered correctly, whether the signal fired while it was away or has yet to fire.
//
// A signal is not a message queue: it carries one payload, it cannot be re-armed, and it delivers nothing to a caller that is neither waiting nor asking.
// Use jobs when what you need is durable, at-least-once delivery to a known recipient.
package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// signalActorTypePrefix namespaces signal actor types within the signal's own bare type space
	signalActorTypePrefix = "signal."

	// methodWait blocks until the signal completes and returns its payload
	methodWait = "wait"
	// methodComplete completes the signal, releasing every waiter
	methodComplete = "complete"
	// methodCheck reports whether the signal has already completed, without blocking
	methodCheck = "check"
)

var (
	// ErrAlreadyCompleted is returned by Complete when the signal had already completed
	// It is safe to ignore: a signal fires once and the first completion stands, so a caller that only needs the signal to have fired can treat it as success
	ErrAlreadyCompleted = errors.New("signal has already completed")

	// ErrPayloadTooLarge is returned by Complete when the encoded payload exceeds the size set with WithMaxPayloadSize
	ErrPayloadTooLarge = errors.New("signal payload is too large")
)

// New builds a signal built-in actor identified by name
// It serves one signal per signal ID, each with its own actor instance, so signals never contend with each other
// A completed signal's payload is kept for the window set by WithRetention, which is how long a caller can arrive late and still be answered immediately
//
// Register the returned value on a host with the host's RegisterBuiltInActor method, then call Wait, Complete, and Check on the SignalService obtained from Service
// Register the same signal set (same name and options) on every host that should be able to wait on or complete its signals
// Names must be unique within a cluster
func New(name string, opts ...Option) (*Signal, error) {
	if name == "" {
		return nil, errors.New("signal name is required")
	}

	err := ref.ValidateComponents(name)
	if err != nil {
		return nil, fmt.Errorf("invalid signal name: %w", err)
	}

	var o signalOptions
	for _, opt := range opts {
		opt(&o)
	}

	// The retention window follows the framework's convention for durations: zero takes the default, and a negative value means no expiration
	retention := o.retention
	switch {
	case retention == 0:
		retention = defaultRetention
	case retention < 0:
		// A state TTL of zero means never expires
		retention = 0
	}

	// A signal with no idle timeout would keep one activation per signal ID in memory forever, so the default stands in for both zero and a negative value
	idleTimeout := o.idleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultIdleTimeout
	}

	maxPayloadSize := o.maxPayloadSize
	if maxPayloadSize <= 0 {
		maxPayloadSize = defaultMaxPayloadSize
	}

	bareType := signalActorTypePrefix + name
	return &Signal{
		actorType:      bareType,
		retention:      retention,
		maxPayloadSize: maxPayloadSize,
		factory: func(actorID string, svc *actor.Service) actor.Actor {
			// Each signal ID gets its own activation, and therefore its own broadcast channel, built from the shared configuration
			return &signalActor{
				client:    builtinactor.NewClient[signalState](bareType, actorID, svc),
				retention: retention,
				done:      make(chan struct{}),
			}
		},
		regOpts: actorcore.RegisterActorOptions{
			IdleTimeout: idleTimeout,
			// Every invocation runs under the shared lock, so a Wait parked on the actor never blocks the Complete that has to release it
			// The actor synchronizes itself with its own mutex instead
			LockMode: actorcore.LockModeShared,
		},
	}, nil
}

// Signal is a built-in signal actor, returned by New and registered on a host with RegisterBuiltInActor
// It satisfies the framework's built-in actor contract and exposes a Service method that returns a SignalService for the Wait, Complete, and Check operations
// The actor behavior itself lives in the unexported signalActor instances that Factory builds, one per signal ID
type Signal struct {
	actorType      string
	retention      time.Duration
	maxPayloadSize int
	factory        actor.Factory
	regOpts        actorcore.RegisterActorOptions

	// servicesMu guards services
	servicesMu sync.Mutex
	// services memoizes the SignalService bound to each actor.Service, so repeated calls to Service hand back the same instance
	// Each one owns the registry that collapses a signal's local waiters onto a single invocation, which a fresh instance per call would defeat
	services map[*actor.Service]*SignalService
}

// ActorType returns the reserved actor type registered for this signal set
func (s *Signal) ActorType() string {
	return s.actorType
}

// Factory returns the actor factory the host registers
func (s *Signal) Factory() actor.Factory {
	return s.factory
}

// RegisterOptions returns the registration options the host uses to register the actor
func (s *Signal) RegisterOptions() actorcore.RegisterActorOptions {
	return s.regOpts
}

// Singleton reports that a signal set is not a singleton and needs no bootstrapping
func (s *Signal) Singleton() bool {
	return false
}

// signalState is a completed signal's durable record, written once when the signal completes and kept for the configured retention window
// A signal that has not fired has no record at all, which is what a fresh activation reads to tell the two apart
type signalState struct {
	// Completed is always true in a stored record, and distinguishes one from the zero value GetState returns when there is no state to read
	Completed bool `msgpack:"completed"`
	// Data is the completion payload, already MessagePack-encoded, so it stays opaque between the caller that sends it and the callers that receive it
	Data []byte `msgpack:"data,omitempty"`
	// CompletedAt is when the completion was persisted
	CompletedAt time.Time `msgpack:"completedAt"`
}

// completeRequest carries a completion's payload from the service to the actor
type completeRequest struct {
	// Data is the caller's payload, already MessagePack-encoded by the service so the actor never has to interpret it
	Data []byte `msgpack:"data,omitempty"`
}

// completeResult is the reply a complete call carries back from the actor to the service
type completeResult struct {
	// AlreadyCompleted reports that the signal had already completed and this call changed nothing
	// It travels as a result rather than an error so the outcome reads the same whether the actor ran on this host or on a peer, where an error would arrive as an opaque protocol failure
	AlreadyCompleted bool `msgpack:"alreadyCompleted,omitempty"`
}

// signalResult is the reply a wait or check call carries back from the actor to the service
type signalResult struct {
	// Completed reports whether the signal has fired
	// It is always true for a wait call, which only returns once the signal has completed
	Completed bool `msgpack:"completed"`
	// Data is the completion payload, still MessagePack-encoded, and empty when the signal carried none
	Data []byte `msgpack:"data,omitempty"`
}

// signalActor is one signal: a single actor instance, keyed by the signal ID, holding the broadcast channel that releases its waiters
// It is registered with actorcore.LockModeShared, so the framework runs its invocations concurrently and the actor synchronizes itself with mu
type signalActor struct {
	client    actor.Client[signalState]
	retention time.Duration

	// mu guards every field below
	mu sync.Mutex
	// loaded reports whether the durable state has been consulted during this activation
	loaded bool
	// completed and data mirror the durable record once the signal has fired
	completed bool
	data      []byte
	// done is closed exactly once, when the signal completes, which is the whole fan-out: every parked waiter is released at the same moment
	done chan struct{}
}

// Invoke handles the wait, complete, and check methods
func (a *signalActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case methodWait:
		return a.wait(ctx)
	case methodComplete:
		return a.complete(ctx, data)
	case methodCheck:
		return a.check(ctx)
	default:
		// Only these three methods are invoked on a signal actor, so anything else is a programming error
		return nil, fmt.Errorf("unknown signal method %q", method)
	}
}

// load reads the signal's durable record into memory, the first time this activation needs it
// Placement guarantees a single active instance per signal across the cluster, so once loaded this instance is authoritative for the rest of its life: any completion has to pass through it
// That is what keeps a signal with thousands of waiters down to a single read
// The caller must hold mu
func (a *signalActor) load(ctx context.Context) error {
	if a.loaded {
		return nil
	}

	state, err := a.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to load signal state: %w", err)
	}

	// GetState reports a missing record as the zero value with no error, so the Completed flag is the only thing that tells a stored completion apart from a signal that has not fired
	a.loaded = true
	if state.Completed {
		a.completed = true
		a.data = state.Data
	}

	return nil
}

// wait blocks until the signal completes and returns its payload, or returns straight away when it has already completed
func (a *signalActor) wait(ctx context.Context) (any, error) {
	// Take a snapshot under the lock, then release it, to avoid locking the actor
	a.mu.Lock()
	err := a.load(ctx)
	if err != nil {
		a.mu.Unlock()
		return nil, err
	}
	completed := a.completed
	data := a.data
	done := a.done
	a.mu.Unlock()

	if completed {
		return signalResult{Completed: true, Data: data}, nil
	}

	// Park until the signal fires, the caller gives up, or the actor starts halting
	select {
	case <-done:
		// Re-read the payload rather than trusting the snapshot, which was taken before the completion wrote it
		a.mu.Lock()
		data = a.data
		a.mu.Unlock()
		return signalResult{Completed: true, Data: data}, nil
	case <-actor.HaltingFromContext(ctx):
		// Report the halt so the caller re-resolves the placement and resumes waiting on the signal's next activation, instead of this call sitting here until the host's shutdown grace period expires
		return nil, actor.ErrActorHalted
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// complete fires the signal, persisting the payload and then releasing every waiter
func (a *signalActor) complete(ctx context.Context, data actor.Envelope) (any, error) {
	// Decode before taking the lock, so a malformed request does not hold up a concurrent completion
	var req completeRequest
	if data != nil {
		err := data.Decode(&req)
		if err != nil {
			return nil, fmt.Errorf("failed to decode signal completion request: %w", err)
		}
	}

	// mu is held across the durable write, which serializes concurrent completions and makes the first one win
	a.mu.Lock()
	defer a.mu.Unlock()

	err := a.load(ctx)
	if err != nil {
		return nil, err
	}

	if a.completed {
		return completeResult{AlreadyCompleted: true}, nil
	}

	// Persist before making the completion observable: a crash after this point costs only the in-memory wake-up, and the waiters that re-resolve are answered from the stored record
	// A failure here leaves nothing changed, so the caller's Complete reports the failure and the signal has genuinely not fired
	err = a.client.SetState(ctx,
		signalState{
			Completed:   true,
			Data:        req.Data,
			CompletedAt: time.Now(),
		},
		&actor.SetStateOpts{
			TTL: a.retention,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to persist signal completion: %w", err)
	}

	// Closing the channel releases every parked waiter at once, with no per-waiter bookkeeping to walk
	a.completed = true
	a.data = req.Data
	close(a.done)

	return nil, nil
}

// check reports whether the signal has already completed, without blocking
func (a *signalActor) check(ctx context.Context) (any, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	err := a.load(ctx)
	if err != nil {
		return nil, err
	}

	return signalResult{Completed: a.completed, Data: a.data}, nil
}
