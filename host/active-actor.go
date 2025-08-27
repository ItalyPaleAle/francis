package host

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

var errActorHalted = errors.New("actor is halted")

// activeActor references an actor that is currently active on this host
type activeActor struct {
	// Actor reference
	ref components.ActorRef

	// Actor object
	instance actor.Actor

	// Used as semaphore the actor when a call is in progress
	sempahore chan struct{}

	// Number of the current pending actor calls
	pendingCalls atomic.Int32

	// Configured max idle time for actors of this type
	idleTimeout time.Duration

	// Time after which this actor is considered to be idle
	// When the actor is locked, idleAt is updated by adding the idleTimeout to the current time
	idleAt atomic.Pointer[time.Time]

	// Halted is set to true when the actor is halted and should not begin more work
	halted atomic.Bool

	// Channel used to signal the actor has been halted
	haltCh chan struct{}

	clock clock.Clock
}

func newActiveActor(ref components.ActorRef, instance actor.Actor, idleTimeout time.Duration, cl clock.Clock) *activeActor {
	if cl == nil {
		cl = &clock.RealClock{}
	}

	a := &activeActor{
		ref:         ref,
		instance:    instance,
		clock:       cl,
		idleTimeout: idleTimeout,
		sempahore:   make(chan struct{}, 1),
		haltCh:      make(chan struct{}),
	}
	a.updateIdleAt(0)

	return a
}

func (a *activeActor) updateIdleAt(d time.Duration) {
	if d == 0 {
		d = a.idleTimeout
	}
	idleAt := a.clock.Now().Add(d)
	a.idleAt.Store(&idleAt)
}

// IsBusy returns true when the actor is busy, i.e. when there are pending calls
func (a *activeActor) IsBusy() bool {
	return a.pendingCalls.Load() > 0
}

func (a *activeActor) Halt() error {
	if !a.halted.CompareAndSwap(false, true) {
		return errors.New("actor is already halted")
	}

	// First, try to grab a lock right away, which will succeed only if the actor is not active
	select {
	case a.sempahore <- struct{}{}:
		// We got the lock
	default:
		// The actor is currently locked
		// Signal the halt, which should make everyone waiting to get a lock return right away
		close(a.haltCh)

		// Now, grab the semaphore so no one else can lock
		// TODO: We need to handle timeouts here
		a.sempahore <- struct{}{}
	}

	return nil
}

// Lock the actor for turn-based concurrency
// This function blocks until the lock is acquired
func (a *activeActor) Lock(ctx context.Context) error {
	if a.halted.Load() {
		return errActorHalted
	}

	pending := a.pendingCalls.Add(1)
	if pending < 0 {
		// Overflow
		a.pendingCalls.Add(-1)
		return errors.New("pending actor calls overflow")
	}

	// Blocks until the lock is acquired or context is canceled
	select {
	case a.sempahore <- struct{}{}:
		// Check again to make sure actor isn't _also_ halted
		// This could happen because of a race condition between grabbing the lock and halting
		select {
		case <-a.haltCh:
			// We should not have acquired the lock
			<-a.sempahore
			return errActorHalted
		default:
			// All good, fall through
		}
	case <-a.haltCh:
		// Actor is halted
		return errActorHalted
	case <-ctx.Done():
		return ctx.Err()
	}

	// Update the time the actor became idle at
	a.updateIdleAt(0)

	return nil
}

// Unlock releases the lock for turn-based concurrency
func (a *activeActor) Unlock() {
	_ = a.pendingCalls.Add(-1)

	select {
	case <-a.sempahore:
		// Released lock
	default:
		// Indicates a development-time error: we unlocked the actor before locking it
		panic("active actor was not locked")
	}
}

// ActorType returns the type of the actor.
func (a *activeActor) ActorType() string {
	return a.ref.ActorType
}

// Key returns the key for this unique actor.
// This is implemented to comply with the queueable interface.
func (a *activeActor) Key() string {
	return a.ref.String()
}

// ScheduledTime returns the time the actor becomes idle at.
// This is implemented to comply with the queueable interface.
func (a *activeActor) ScheduledTime() time.Time {
	return *a.idleAt.Load()
}
