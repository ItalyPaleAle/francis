package activeactor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/eventqueue"
	"github.com/italypaleale/francis/internal/locker"
	"github.com/italypaleale/francis/internal/ref"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

type idleActorProcessor = *eventqueue.Processor[string, *Instance]

var ErrActiveActorAlreadyHalted = errors.New("actor is already halted")

// Instance references an actor that is currently active on this host
type Instance struct {
	// Actor reference
	ref ref.ActorRef

	// Actor object
	instance actor.Actor

	// Configured max idle time for actors of this type
	idleTimeout time.Duration

	// Time after which this actor is considered to be idle
	// When the actor is locked, idleAt is updated by adding the idleTimeout to the current time
	idleAt atomic.Pointer[time.Time]

	// Halted is set to true when the actor is halted and should not begin more work
	halted atomic.Bool

	// Channel that is closed when the actor is halted
	// This is used by callers who currently have a lock to understand if they need to cancel in-flight requests
	haltCh chan struct{}

	locker        locker.TurnBasedLocker
	idleProcessor idleActorProcessor
	clock         clock.Clock
}

func NewInstance(ref ref.ActorRef, instance actor.Actor, idleTimeout time.Duration, idleProcessor idleActorProcessor, cl clock.Clock) *Instance {
	if cl == nil {
		cl = &clock.RealClock{}
	}

	a := &Instance{
		ref:           ref,
		instance:      instance,
		idleTimeout:   idleTimeout,
		haltCh:        make(chan struct{}),
		locker:        locker.TurnBasedLocker{},
		idleProcessor: idleProcessor,
		clock:         cl,
	}
	a.UpdateIdleAt(0)

	return a
}

// UpdateIdleAt updates the idle timeout property (i.e. time the actor becomes idle at)
// d allows overriding the idle interval; if zero, uses the default for the actor type
func (a *Instance) UpdateIdleAt(d time.Duration) {
	if a.idleTimeout <= 0 {
		// Actor doesn't have an idle timeout
		return
	}

	if d == 0 {
		d = a.idleTimeout
	}

	// Update the idleAt time
	idleAt := a.clock.Now().Add(d)
	a.idleAt.Store(&idleAt)

	// (Re-)enqueue in the idle processor
	_ = a.idleProcessor.Enqueue(a)
}

// TryLock tries to lock the actor for turn-based concurrency, if the actor isn't already locked.
func (a *Instance) TryLock() (bool, chan struct{}, error) {
	if a.halted.Load() {
		return false, nil, actor.ErrActorHalted
	}

	ok, err := a.locker.TryLock()
	switch {
	case errors.Is(err, locker.ErrStopped):
		// If the locker is stopped, it means that the actor has been halted
		return false, nil, actor.ErrActorHalted
	case err != nil:
		return false, nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// If we did not acquire the lock, return
	if !ok {
		return false, nil, nil
	}

	// Update the time the actor became idle at
	a.UpdateIdleAt(0)

	return true, a.haltCh, nil
}

// Lock the actor for turn-based concurrency.
// This function blocks until the lock is acquired
func (a *Instance) Lock(ctx context.Context) (chan struct{}, error) {
	if a.halted.Load() {
		return nil, actor.ErrActorHalted
	}

	err := a.locker.Lock(ctx)
	switch {
	case errors.Is(err, locker.ErrStopped):
		// If the locker is stopped, it means that the actor has been halted
		return nil, actor.ErrActorHalted
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		return nil, ctx.Err()
	case err != nil:
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Update the time the actor became idle at
	a.UpdateIdleAt(0)

	return a.haltCh, nil
}

// Unlock releases the lock for turn-based concurrency
func (a *Instance) Unlock() {
	a.locker.Unlock()
}

// Halt the active actor
func (a *Instance) Halt(drain bool) error {
	if !a.halted.CompareAndSwap(false, true) {
		return ErrActiveActorAlreadyHalted
	}

	// Stop the turn-based locker
	// This will signal to all callers currently waiting to acquire the lock that the actor has stopped
	a.locker.Stop()

	// Close haltCh, which signals whoever may be owning the lock right now that the actor is shutting down
	close(a.haltCh)

	// Also remove from the idle actor processor
	if a.idleTimeout > 0 {
		_ = a.idleProcessor.Dequeue(a.Key())
	}

	// If we need to drain the actor, call StopAndWait on the locker, which now makes sure no one is holding the lock
	// Otherwise, we've already called "Stop" before
	if drain {
		a.locker.StopAndWait()
	}

	return nil
}

// Instance returns the instance of the actor
func (a *Instance) Instance() actor.Actor {
	return a.instance
}

// ActorType returns the type of the actor.
func (a *Instance) ActorType() string {
	return a.ref.ActorType
}

// Key returns the key for the actor.
// This is implemented to comply with the queueable interface.
func (a *Instance) Key() string {
	return a.ref.String()
}

// DueTime returns the time the actor becomes idle at.
// This is implemented to comply with the queueable interface.
func (a *Instance) DueTime() time.Time {
	return *a.idleAt.Load()
}

// Halted returns true if the actor is halted.
func (a *Instance) Halted() bool {
	return a.halted.Load()
}

// ActorRef returns the actor ref
func (a *Instance) ActorRef() ref.ActorRef {
	return a.ref
}
