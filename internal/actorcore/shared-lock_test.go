package actorcore

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	actor_mocks "github.com/italypaleale/francis/internal/mocks/actor"
	"github.com/italypaleale/francis/internal/ref"
)

func TestSharedLockMode(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	// newHost returns a minimal manager with one shared-mode actor type and one ordinary turn-based one, so tests can compare the two
	newHost := func() *Manager {
		host := &Manager{
			Actors:              haxmap.New[string, *ActiveActor](8),
			log:                 log,
			clock:               clock,
			shutdownGracePeriod: 5 * time.Second,
			ActorsConfig: map[string]components.ActorHostType{
				"sharedactor":    {IdleTimeout: 5 * time.Minute},
				"exclusiveactor": {IdleTimeout: 5 * time.Minute},
			},
			ActorFactories: map[string]actor.Factory{
				"sharedactor": func(actorID string, service *actor.Service) actor.Actor {
					return &actor_mocks.MockActorDeactivate{}
				},
				"exclusiveactor": func(actorID string, service *actor.Service) actor.Actor {
					return &actor_mocks.MockActorDeactivate{}
				},
			},
			actorTypeLockMode: map[string]LockMode{
				"sharedactor":    LockModeShared,
				"exclusiveactor": LockModeExclusive,
			},
		}
		host.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
			ExecuteFn: host.HandleIdleActor,
			Clock:     clock,
		})
		return host
	}

	t.Run("invocations run concurrently", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("sharedactor", "actor1")

		// Every invocation waits on the barrier, so they can only all return if they are all running at the same time
		const count = 20
		var wg sync.WaitGroup
		barrier := make(chan struct{})
		var running atomic.Int32

		wg.Add(count)
		for range count {
			go func() {
				defer wg.Done()
				_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
					if running.Add(1) == count {
						close(barrier)
					}
					<-barrier
					return nil, nil
				})
				assert.NoError(t, err)
			}()
		}

		waitOrFail(t, wgDone(&wg), 5*time.Second, "invocations did not all run concurrently")
	})

	t.Run("a parked invocation does not block the invocation that releases it", func(t *testing.T) {
		// This is the case the shared lock exists for: under the exclusive lock the releasing call would queue behind every parked one, and every parked one is waiting for it to run
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("sharedactor", "actor2")

		const waiters = 50
		release := make(chan struct{})
		var wg sync.WaitGroup

		// Park the waiters, and make sure they are all holding the lock before the release runs
		var parked atomic.Int32
		wg.Add(waiters)
		for range waiters {
			go func() {
				defer wg.Done()
				_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
					parked.Add(1)
					<-release
					return nil, nil
				})
				assert.NoError(t, err)
			}()
		}

		require.Eventually(t, func() bool {
			return parked.Load() == waiters
		}, 5*time.Second, 10*time.Millisecond, "waiters did not park")

		// The releasing invocation must get in while all of them are still parked
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
				close(release)
				return nil, nil
			})
			assert.NoError(t, err)
		}()

		waitOrFail(t, done, 5*time.Second, "the releasing invocation was blocked by the parked ones")
		waitOrFail(t, wgDone(&wg), 5*time.Second, "the parked invocations were not released")
	})

	t.Run("Peek is rejected", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("sharedactor", "actor3")

		_, err := host.LockAndPeek(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
			return nil, nil
		})
		require.ErrorIs(t, err, ErrActorMethodUnsupported)
	})

	t.Run("an actor with an invocation in flight is not deactivated", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("sharedactor", "actor4")

		release := make(chan struct{})
		invoked := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
				close(invoked)
				<-release
				return nil, nil
			})
			assert.NoError(t, err)
		}()

		waitOrFail(t, invoked, 5*time.Second, "the invocation did not start")

		// The idle processor must see the actor as busy while the invocation holds the shared lock, exactly as it does for the exclusive one
		act, ok := host.Actors.Get(actorRef.String())
		require.True(t, ok)
		host.HandleIdleActor(act)

		_, stillActive := host.Actors.Get(actorRef.String())
		assert.True(t, stillActive, "the actor was deactivated while an invocation was in flight")

		close(release)
		waitOrFail(t, done, 5*time.Second, "the invocation did not return")
	})

	t.Run("halting is signaled to the invocation through the context", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("sharedactor", "actor5")

		invoked := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
				close(invoked)

				// A handler that parks selects on the halt signal so it can return without waiting out the shutdown grace period
				select {
				case <-actor.HaltingFromContext(ctx):
					return nil, actor.ErrActorHalted
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			assert.ErrorIs(t, err, actor.ErrActorHalted)
		}()

		waitOrFail(t, invoked, 5*time.Second, "the invocation did not start")

		act, ok := host.Actors.Get(actorRef.String())
		require.True(t, ok)
		require.NoError(t, act.Halt(false))

		// The shutdown grace period is 5s, so returning quickly is what proves the halt signal was observed rather than the context cancellation that follows it
		waitOrFail(t, done, 2*time.Second, "the invocation did not observe the halt signal")
	})

	t.Run("an exclusive-mode actor still serializes its invocations", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("exclusiveactor", "actor1")

		var concurrent atomic.Int32
		var maxConcurrent atomic.Int32
		var wg sync.WaitGroup

		wg.Add(10)
		for range 10 {
			go func() {
				defer wg.Done()
				_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
					cur := concurrent.Add(1)
					for {
						observed := maxConcurrent.Load()
						if cur <= observed || maxConcurrent.CompareAndSwap(observed, cur) {
							break
						}
					}
					time.Sleep(time.Millisecond)
					concurrent.Add(-1)
					return nil, nil
				})
				assert.NoError(t, err)
			}()
		}

		waitOrFail(t, wgDone(&wg), 5*time.Second, "invocations did not complete")
		assert.Equal(t, int32(1), maxConcurrent.Load(), "turn-based concurrency was not preserved for the default lock mode")
	})
}

func TestLockModeIsValid(t *testing.T) {
	assert.True(t, LockModeExclusive.IsValid())
	assert.True(t, LockModeShared.IsValid())
	assert.False(t, LockMode(2).IsValid())
	assert.False(t, LockMode(255).IsValid())
}

func TestRegisterActorOptionsRejectsInvalidLockMode(t *testing.T) {
	opts := RegisterActorOptions{LockMode: LockMode(7)}
	require.ErrorContains(t, opts.Validate(), "LockMode")
}

// wgDone returns a channel that is closed once the wait group's counter reaches zero
func wgDone(wg *sync.WaitGroup) chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// waitOrFail fails the test with msg when ch is not closed within the timeout
func waitOrFail(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}
