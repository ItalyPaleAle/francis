package host

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/internal/eventqueue"
	"github.com/italypaleale/actors/internal/locker"
	"github.com/italypaleale/actors/internal/ref"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

type idleActorProcessor = *eventqueue.Processor[string, *activeActor]

// activeActor references an actor that is currently active on this host
type activeActor struct {
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

func newActiveActor(ref ref.ActorRef, instance actor.Actor, idleTimeout time.Duration, idleProcessor idleActorProcessor, cl clock.Clock) *activeActor {
	if cl == nil {
		cl = &clock.RealClock{}
	}

	a := &activeActor{
		ref:           ref,
		instance:      instance,
		idleTimeout:   idleTimeout,
		haltCh:        make(chan struct{}),
		locker:        locker.TurnBasedLocker{},
		idleProcessor: idleProcessor,
		clock:         cl,
	}
	a.updateIdleAt(0)

	return a
}

// Updates the idle timeout property (i.e. time the actor becomes idle at)
// d allows overriding the idle interval; if zero, uses the default for the actor type
func (a *activeActor) updateIdleAt(d time.Duration) {
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
func (a *activeActor) TryLock() (bool, chan struct{}, error) {
	if a.halted.Load() {
		return false, nil, errActorHalted
	}

	ok, err := a.locker.TryLock()
	switch {
	case errors.Is(err, locker.ErrStopped):
		// If the locker is stopped, it means that the actor has been halted
		return false, nil, errActorHalted
	case err != nil:
		return false, nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// If we did not acquire the lock, return
	if !ok {
		return false, nil, nil
	}

	// Update the time the actor became idle at
	a.updateIdleAt(0)

	return true, a.haltCh, nil
}

// Lock the actor for turn-based concurrency.
// This function blocks until the lock is acquired
func (a *activeActor) Lock(ctx context.Context) (chan struct{}, error) {
	if a.halted.Load() {
		return nil, errActorHalted
	}

	err := a.locker.Lock(ctx)
	switch {
	case errors.Is(err, locker.ErrStopped):
		// If the locker is stopped, it means that the actor has been halted
		return nil, errActorHalted
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		return nil, ctx.Err()
	case err != nil:
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Update the time the actor became idle at
	a.updateIdleAt(0)

	return a.haltCh, nil
}

// Unlock releases the lock for turn-based concurrency
func (a *activeActor) Unlock() {
	a.locker.Unlock()
}

func (a *activeActor) Halt() error {
	if !a.halted.CompareAndSwap(false, true) {
		return errors.New("actor is already halted")
	}

	// Stop the turn-based locker
	// This will signal to all callers currently waiting to acquire the lock that the actor has stopped
	a.locker.Stop()

	// Close haltCh, which signals whoever may be owning the lock right now that the actor is shutting down
	close(a.haltCh)

	// Also remove from the idle actor processor
	_ = a.idleProcessor.Dequeue(a.Key())

	// Call StopAndWait on the locker, which now makes sure no one is holding the lock
	a.locker.StopAndWait()

	return nil
}

// ActorType returns the type of the actor.
func (a *activeActor) ActorType() string {
	return a.ref.ActorType
}

// Key returns the key for the actor.
// This is implemented to comply with the queueable interface.
func (a *activeActor) Key() string {
	return a.ref.String()
}

// DueTime returns the time the actor becomes idle at.
// This is implemented to comply with the queueable interface.
func (a *activeActor) DueTime() time.Time {
	return *a.idleAt.Load()
}
