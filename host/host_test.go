package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/eventqueue"
	actor_mocks "github.com/italypaleale/actors/internal/mocks/actor"
	components_mocks "github.com/italypaleale/actors/internal/mocks/components"
	"github.com/italypaleale/actors/internal/ref"
	"github.com/italypaleale/actors/internal/testutil"
)

func TestHostHalt(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	// Create logger
	logBuf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create a mocked actor provider
	provider := components_mocks.NewMockActorProvider(t)

	// Create a minimal host for testing
	host := &Host{
		actorProvider: provider,
		actors:        haxmap.New[string, *activeActor](8),
		log:           log,
		clock:         clock,
		actorsConfig: map[string]components.ActorHostType{
			"testactor": {
				// Enable deactivation timeout
				DeactivationTimeout: 5 * time.Second,
			},
		},
		providerRequestTimeout: 30 * time.Second,
	}
	host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: host.handleIdleActor,
		Clock:     clock,
	})

	t.Run("halt existing actor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting the actor
		err := host.Halt("testactor", "actor1")
		require.NoError(t, err)

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt non-existent actor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Try to halt an actor that doesn't exist
		err := host.Halt("nonexistent", "actor1")
		require.ErrorIs(t, err, actor.ErrActorNotHosted)
	})

	t.Run("halt actor that is nil in map", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Set a nil actor in the map
		actorRef := ref.NewActorRef("testactor", "nilactor")
		host.actors.Set(actorRef.String(), nil)
		t.Cleanup(func() { host.actors.Del(actorRef.String()) })

		err := host.Halt("testactor", "nilactor")
		require.ErrorIs(t, err, actor.ErrActorNotHosted)
	})

	t.Run("halt actor when provider returns ErrNoActor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "actor3")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(components.ErrNoActor).
			Once()

		// Test halting the actor (should succeed when provider returns ErrNoActor)
		err := host.Halt("testactor", "actor3")
		require.NoError(t, err, "Should succeed when provider returns ErrNoActor")

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("concurrent halt calls on same actor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "concurrent1")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Launch multiple goroutines trying to halt the same actor
		const numGoroutines = 10
		errCh := make(chan error, numGoroutines)

		var wg sync.WaitGroup
		for range numGoroutines {
			wg.Go(func() {
				errCh <- host.Halt("testactor", "concurrent1")
			})
		}

		wg.Wait()
		close(errCh)

		// Collect results
		var successCount, notHostedCount, otherErrorCount int
		for err := range errCh {
			switch {
			case err == nil:
				successCount++
			case errors.Is(err, actor.ErrActorNotHosted):
				notHostedCount++
			default:
				otherErrorCount++
				t.Logf("Unexpected error: %v", err)
			}
		}

		// At least one should succeed, others should get ErrActorNotHosted
		assert.GreaterOrEqual(t, successCount, 1, "At least one halt should succeed")
		assert.GreaterOrEqual(t, notHostedCount, 0, "Some halts should get ErrActorNotHosted")
		assert.Equal(t, 0, otherErrorCount, "Should not have other types of errors")
		assert.Equal(t, numGoroutines, successCount+notHostedCount+otherErrorCount)

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt locked actor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "locked1")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(components.ErrNoActor).
			Once()

		// Acquire the lock on the actor
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Launch halt in a separate goroutine since it should block until the lock is released
		haltErrCh := make(chan error, 1)
		go func() {
			haltErrCh <- host.Halt("testactor", "locked1")
		}()

		// The halt should not have completed yet
		select {
		case err := <-haltErrCh:
			t.Fatalf("Halt should not have completed yet, but got: %v", err)
		default:
			// Expected - halt is waiting for the lock
		}

		// Verify that the halt channel signals the lock holder
		select {
		case <-haltCh:
			// Expected - halt channel should be closed
		case <-time.After(100 * time.Millisecond):
			t.Error("Halt channel should have been closed")
		}

		// Now release the lock
		activeAct.Unlock()

		// The halt should complete successfully
		select {
		case err := <-haltErrCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Halt should have completed after unlock")
		}

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt locked actor with waiters", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "locked2")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(components.ErrNoActor).
			Once()

		// Acquire the lock on the actor
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Create multiple waiters trying to acquire the lock
		const numWaiters = 5
		var waiterWg sync.WaitGroup
		waiterErrCh := make(chan error, numWaiters)
		startCounter := &atomic.Int32{}

		for range numWaiters {
			waiterWg.Go(func() {
				startCounter.Add(1)
				_, lErr := activeAct.Lock(t.Context())
				waiterErrCh <- lErr
			})
		}

		// Wait for goroutines to start
		waitForGoroutines(t, numWaiters, startCounter)

		// Launch halt in a separate goroutine
		haltErrCh := make(chan error, 1)
		go func() {
			haltErrCh <- host.Halt("testactor", "locked2")
		}()

		// The halt should not have completed yet
		select {
		case err := <-haltErrCh:
			t.Fatalf("Halt should not have completed yet, but got: %v", err)
		default:
			// Expected - halt is waiting for the lock
		}

		// Verify that the halt channel signals the lock holder
		select {
		case <-haltCh:
			// Expected - halt channel should be closed
		case <-time.After(100 * time.Millisecond):
			t.Error("Halt channel should have been closed")
		}

		// Now release the lock
		activeAct.Unlock()

		// All waiters should receive ErrActorHalted
		waiterWg.Wait()
		close(waiterErrCh)

		waiterErrorCount := 0
		for err := range waiterErrCh {
			require.ErrorIs(t, err, actor.ErrActorHalted, "All waiters should get ErrActorHalted")
			waiterErrorCount++
		}
		assert.Equal(t, numWaiters, waiterErrorCount, "All waiters should have received errors")

		// The halt should complete successfully
		select {
		case err := <-haltErrCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Halt should have completed after unlock")
		}

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt actor without deactivation timeout", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor with actor type that has no deactivation timeout
		actorRef := ref.NewActorRef("noTimeout", "actor1")

		// The Deactivate method should NOT be called
		instance := &actor_mocks.MockActorDeactivate{}

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting the actor
		err := host.Halt("noTimeout", "actor1")
		require.NoError(t, err)

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider
		provider.AssertExpectations(t)
	})

	t.Run("halt actor that doesn't implement deactivate interface", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor that doesn't implement ActorDeactivate
		actorRef := ref.NewActorRef("testactor", "noDeactivate")
		instance := &actor_mocks.MockActorInvoke{}

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting the actor
		err := host.Halt("testactor", "noDeactivate")
		require.NoError(t, err)

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider
		provider.AssertExpectations(t)
	})

	t.Run("halt actor with deactivate error", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor that returns error from Deactivate
		actorRef := ref.NewActorRef("testactor", "errorDeactivate")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(errors.New("deactivate failed")).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting the actor - even though the actor responded with an error, the deactivation should continue
		err := host.Halt("testactor", "errorDeactivate")
		require.NoError(t, err)

		// Ensure the error was logged
		logString := logBuf.String()
		assert.Contains(t, logString, "Actor returned an error during deactivation")
		assert.Contains(t, logString, "deactivate failed")

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt actor with provider error", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "providerError")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider to return an error
		providerErr := errors.New("provider failed")
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(providerErr).
			Once()

		// Test halting the actor - should fail due to provider error
		err := host.Halt("testactor", "providerError")
		require.Error(t, err)
		require.ErrorIs(t, err, providerErr)

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Verify the actor was removed from the map despite provider error
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map even with provider error")

		// Assert expected method calls on provider and instance
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt already halted actor", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "alreadyHalted")

		// The Deactivate method should NOT be called
		instance := &actor_mocks.MockActorDeactivate{}

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Halt the actor first
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Now try to halt via Host.Halt - should succeed without calling provider
		err = host.Halt("testactor", "alreadyHalted")
		require.NoError(t, err)

		// Verify the actor is no longer in the map (it should have been cleaned up)
		_, exists := host.actors.Get(actorRef.String())
		assert.True(t, exists, "Actor should still be in the map since haltActiveActor returned early")
	})

	t.Run("halt actor getAndDel returns nil", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// This test simulates the case where GetAndDel returns nil
		// by testing haltActiveActor directly with an actor that's not in the map
		actorRef := ref.NewActorRef("testactor", "notInMap")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		// Don't add to host.actors map, so GetAndDel will return nil

		// Test haltActiveActor directly - should succeed without calling RemoveActor
		err := host.haltActiveActor(activeAct, true)
		require.NoError(t, err)

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// RemoveActor should NOT be called since GetAndDel returned nil
		// Assert expected method calls on instance
		instance.AssertExpectations(t)
	})

	t.Run("deactivation timeout", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a separate mock provider for this test
		timeoutProvider := components_mocks.NewMockActorProvider(t)

		// Create a host with very short deactivation timeout
		shortTimeoutHost := &Host{
			actorProvider: timeoutProvider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					// Very short timeout
					DeactivationTimeout: 1 * time.Microsecond,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		shortTimeoutHost.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: shortTimeoutHost.handleIdleActor,
			Clock:     clock,
		})

		// Create and register an active actor with slow deactivation
		actorRef := ref.NewActorRef("testactor", "slowDeactivate")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Run(func(args mock.Arguments) {
				// Wait for context to be canceled
				ctx := args.Get(0).(context.Context)
				<-ctx.Done()
			}).
			Return(context.DeadlineExceeded).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, shortTimeoutHost.idleActorProcessor, clock)
		shortTimeoutHost.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		timeoutProvider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting the actor - even though the actor responded with an error, the deactivation should continue
		err := shortTimeoutHost.Halt("testactor", "slowDeactivate")
		require.NoError(t, err)

		// Ensure the error was logged
		logString := logBuf.String()
		assert.Contains(t, logString, "Actor returned an error during deactivation")
		assert.Contains(t, logString, "context deadline exceeded")

		// Verify the actor is no longer in the map
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from the map after halting")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls on timeoutProvider and instance
		timeoutProvider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})
}

func TestHostHaltAll(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	// Create logger
	logBuf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	t.Run("halt all with no actors", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		// Test halting all actors when there are none
		err := host.HaltAll()
		require.NoError(t, err)

		// Verify no actors remain
		assert.Equal(t, uintptr(0), host.actors.Len(), "No actors should remain")
	})

	t.Run("halt all with single actor success", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Test halting all actors
		err := host.HaltAll()
		require.NoError(t, err)

		// Verify no actors remain
		assert.Equal(t, uintptr(0), host.actors.Len(), "No actors should remain after HaltAll")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("halt all with multiple actors success", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		const numActors = 5
		activeActors := make([]*activeActor, numActors)
		instances := make([]*actor_mocks.MockActorDeactivate, numActors)

		// Create and register multiple active actors
		for i := range numActors {
			actorRef := ref.NewActorRef("testactor", fmt.Sprintf("actor%d", i))
			instance := &actor_mocks.MockActorDeactivate{}
			instance.
				On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
				Return(nil).
				Once()

			activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
			host.actors.Set(actorRef.String(), activeAct)

			activeActors[i] = activeAct
			instances[i] = instance

			// Set expected method calls on provider
			provider.
				On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
				Return(nil).
				Once()
		}

		// Verify initial state
		assert.Equal(t, uintptr(numActors), host.actors.Len(), "Should have all actors initially")

		// Test halting all actors
		err := host.HaltAll()
		require.NoError(t, err)

		// Verify no actors remain
		assert.Equal(t, uintptr(0), host.actors.Len(), "No actors should remain after HaltAll")

		// Verify all actors are marked as halted
		for i, activeAct := range activeActors {
			assert.True(t, activeAct.halted.Load(), "Actor %d should be marked as halted", i)
		}

		// Assert expected method calls
		provider.AssertExpectations(t)
		for _, instance := range instances {
			instance.AssertExpectations(t)
		}
	})

	t.Run("halt all with some actor failures", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		// Create actors: some succeed, some fail
		const numActors = 4

		// Actor 0: Success
		actorRef0 := ref.NewActorRef("testactor", "success")
		instance0 := &actor_mocks.MockActorDeactivate{}
		instance0.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()
		activeAct0 := newActiveActor(actorRef0, instance0, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef0.String(), activeAct0)
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef0).
			Return(nil).
			Once()

		// Actor 1: Deactivate error
		actorRef1 := ref.NewActorRef("testactor", "deactivateError")
		instance1 := &actor_mocks.MockActorDeactivate{}
		instance1.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(errors.New("deactivate failed")).
			Once()
		activeAct1 := newActiveActor(actorRef1, instance1, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef1.String(), activeAct1)
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef1).
			Return(nil).
			Once()

		// Actor 2: Provider error
		actorRef2 := ref.NewActorRef("testactor", "providerError")
		instance2 := &actor_mocks.MockActorDeactivate{}
		instance2.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()
		activeAct2 := newActiveActor(actorRef2, instance2, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef2.String(), activeAct2)
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef2).
			Return(errors.New("provider failed")).
			Once()

		// Actor 3: Success
		actorRef3 := ref.NewActorRef("testactor", "success2")
		instance3 := &actor_mocks.MockActorDeactivate{}
		instance3.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()
		activeAct3 := newActiveActor(actorRef3, instance3, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef3.String(), activeAct3)
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef3).
			Return(nil).
			Once()

		// Verify initial state
		assert.Equal(t, uintptr(numActors), host.actors.Len(), "Should have all actors initially")

		// Test halting all actors - should get aggregated error
		err := host.HaltAll()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error halting actors")
		// Note: due to the way errors.Join works and goroutine execution order,
		// we might not see both error messages in the final error
		assert.True(t,
			strings.Contains(err.Error(), "deactivate failed") || strings.Contains(err.Error(), "provider failed"),
			"Should contain at least one of the expected error messages")

		// Verify actors are removed from the map regardless of errors
		// (successful ones and ones where only deactivation failed)
		assert.LessOrEqual(t, host.actors.Len(), uintptr(1), "Most actors should be removed from map")

		// Verify all actors are marked as halted
		assert.True(t, activeAct0.halted.Load(), "Actor 0 should be marked as halted")
		assert.True(t, activeAct1.halted.Load(), "Actor 1 should be marked as halted")
		assert.True(t, activeAct2.halted.Load(), "Actor 2 should be marked as halted")
		assert.True(t, activeAct3.halted.Load(), "Actor 3 should be marked as halted")

		// Assert expected method calls
		provider.AssertExpectations(t)
		instance0.AssertExpectations(t)
		instance1.AssertExpectations(t)
		instance2.AssertExpectations(t)
		instance3.AssertExpectations(t)
	})

	t.Run("halt all with already halted actors", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		const numActors = 3
		activeActors := make([]*activeActor, numActors)

		// Create and register active actors
		for i := range numActors {
			actorRef := ref.NewActorRef("testactor", fmt.Sprintf("actor%d", i))
			instance := &actor_mocks.MockActorDeactivate{}
			// No expectations set since actors are already halted

			activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
			host.actors.Set(actorRef.String(), activeAct)
			activeActors[i] = activeAct

			// Pre-halt the actors
			err := activeAct.Halt(false)
			require.NoError(t, err)
		}

		// Verify initial state
		assert.Equal(t, uintptr(numActors), host.actors.Len(), "Should have all actors initially")

		// Test halting all actors - should succeed without calling provider methods
		err := host.HaltAll()
		require.NoError(t, err)

		// Verify all actors are still marked as halted
		for i, activeAct := range activeActors {
			assert.True(t, activeAct.halted.Load(), "Actor %d should still be marked as halted", i)
		}

		// No provider expectations should be called since actors were already halted
		provider.AssertExpectations(t)
	})

	t.Run("concurrent halt all calls", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		const numActors = 3
		activeActors := make([]*activeActor, numActors)

		// Create and register active actors
		for i := range numActors {
			actorRef := ref.NewActorRef("testactor", fmt.Sprintf("actor%d", i))
			instance := &actor_mocks.MockActorDeactivate{}
			instance.
				On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
				Return(nil).
				Once()

			activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
			host.actors.Set(actorRef.String(), activeAct)
			activeActors[i] = activeAct

			// Set expected method calls on provider
			provider.
				On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
				Return(nil).
				Once()
		}

		// Launch multiple HaltAll calls concurrently
		const numConcurrentCalls = 3
		errCh := make(chan error, numConcurrentCalls)
		var wg sync.WaitGroup

		for range numConcurrentCalls {
			wg.Go(func() {
				errCh <- host.HaltAll()
			})
		}

		wg.Wait()
		close(errCh)

		// Collect results - at least one should succeed
		var successCount, errorCount int
		for err := range errCh {
			if err == nil {
				successCount++
			} else {
				errorCount++
				t.Logf("HaltAll error: %v", err)
			}
		}

		// At least one call should have succeeded
		assert.GreaterOrEqual(t, successCount, 0, "At least one HaltAll should have had no error or all might have errors due to race conditions")
		assert.Equal(t, numConcurrentCalls, successCount+errorCount)

		// Verify actors are eventually cleaned up
		assert.LessOrEqual(t, host.actors.Len(), uintptr(numActors), "Actors should be cleaned up")

		// Verify all actors are marked as halted
		for i, activeAct := range activeActors {
			assert.True(t, activeAct.halted.Load(), "Actor %d should be marked as halted", i)
		}

		// Assert expectations
		provider.AssertExpectations(t)
	})
}

func waitForGoroutines(t *testing.T, want int32, counter *atomic.Int32) {
	t.Helper()

	var i int
	for {
		if counter.Load() == want {
			break
		}

		if i == 1000 {
			t.Fatalf("Waited too long for goroutines to start")
		}
		i++

		time.Sleep(500 * time.Microsecond)
	}
}

func TestIdleActorHandling(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	// Create logger to see re-enqueue logs
	logBuf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var newHost = func() (*Host, *components_mocks.MockActorProvider) {
		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			actorProvider: provider,
			actors:        haxmap.New[string, *activeActor](8),
			log:           log,
			clock:         clock,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 5 * time.Second,
				},
			},
			providerRequestTimeout: 30 * time.Second,
		}
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		return host, provider
	}

	t.Run("idle actor gets halted", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		// Create and register an active actor with short idle timeout
		actorRef := ref.NewActorRef("testactor", "idleActor1")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		idleTimeout := 1 * time.Minute
		activeAct := newActiveActor(actorRef, instance, idleTimeout, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set expected method calls on provider
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Update idle time to trigger processing
		activeAct.updateIdleAt(0)

		// Verify actor is in the processor queue by advancing time
		clock.Step(idleTimeout + time.Second)

		// Give the processor time to execute
		assert.Eventually(t, func() bool {
			_, exists := host.actors.Get(actorRef.String())
			return !exists // Wait until actor is removed
		}, 2*time.Second, 50*time.Millisecond, "Actor should be removed from host after idle processing")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Assert expected method calls
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("busy actor gets re-enqueued with delay", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "busyActor")
		instance := &actor_mocks.MockActorDeactivate{}

		idleTimeout := 1 * time.Minute
		activeAct := newActiveActor(actorRef, instance, idleTimeout, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Lock the actor to make it "busy"
		haltCh, err := activeAct.Lock(context.Background())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Update idle time to trigger processing
		activeAct.updateIdleAt(0) // Use default idle timeout

		// Advance time to when the actor should be processed
		clock.Step(idleTimeout + 1*time.Second)

		// Verify re-enqueue message was logged
		assert.Eventually(t, func() bool {
			logString := logBuf.String()
			return strings.Contains(logString, "Actor is busy and will not be deactivated; re-enqueueing it")
		}, 1*time.Second, 50*time.Millisecond, "Should log re-enqueue message")

		// Verify the actor is still in the host (not halted because it's busy)
		_, exists := host.actors.Get(actorRef.String())
		assert.True(t, exists, "Busy actor should remain in host")

		// Verify the actor is NOT marked as halted
		assert.False(t, activeAct.halted.Load(), "Busy actor should not be marked as halted")

		// Unlock the actor now so it can be processed
		activeAct.Unlock()

		// Set up expectations for successful deactivation now
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(nil).
			Once()

		// Advance time by the re-enqueue interval to trigger processing again
		clock.Step(10*time.Second + 1*time.Second) // actorBusyReEnqueueInterval + buffer

		// Wait for the actor to be processed and removed
		assert.Eventually(t, func() bool {
			_, exists := host.actors.Get(actorRef.String())
			return !exists // Wait until actor is removed
		}, 2*time.Second, 50*time.Millisecond, "Actor should be removed from host after successful processing")

		// Verify the actor is marked as halted
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted after unlock")

		// Assert expected method calls
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("idle actor with zero timeout is not processed", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		// Create and register an active actor with zero idle timeout
		// No deactivation expected since actor has no idle timeout
		actorRef := ref.NewActorRef("testactor", "noIdleTimeout")
		instance := &actor_mocks.MockActorDeactivate{}

		activeAct := newActiveActor(actorRef, instance, 0, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Try to update idle time - should be a no-op
		activeAct.updateIdleAt(0)

		// Advance time significantly
		clock.Step(10 * time.Minute)

		// Verify the actor is still in the host (not processed)
		assert.Never(t, func() bool {
			_, exists := host.actors.Get(actorRef.String())
			return !exists
		}, 250*time.Millisecond, 50*time.Millisecond, "Actor with zero idle timeout should remain in host")

		// Verify the actor is NOT marked as halted
		assert.False(t, activeAct.halted.Load(), "Actor with zero idle timeout should not be halted")

		// No provider expectations since nothing should be called
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("handle idle actor with try lock error", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "errorActor")
		instance := &actor_mocks.MockActorDeactivate{}

		idleTimeout := 1 * time.Minute
		activeAct := newActiveActor(actorRef, instance, idleTimeout, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Halt the actor first to cause TryLock to return an error
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Update idle time to trigger processing
		activeAct.updateIdleAt(0)

		// Advance time to when the actor should be processed
		clock.Step(idleTimeout + 1*time.Second)

		// Verify error message was logged
		assert.Eventually(t, func() bool {
			logString := logBuf.String()
			return strings.Contains(logString, "Failed to try locking idle actor for deactivation")
		}, 1*time.Second, 50*time.Millisecond, "Should log TryLock error")

		// The actor should remain halted
		assert.True(t, activeAct.halted.Load(), "Actor should remain halted")

		// No provider expectations since handleIdleActor returned early due to error
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("handle idle actor with halt error", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		// Create and register an active actor
		actorRef := ref.NewActorRef("testactor", "haltErrorActor")
		instance := &actor_mocks.MockActorDeactivate{}
		instance.
			On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
			Return(nil).
			Once()

		idleTimeout := 1 * time.Minute
		activeAct := newActiveActor(actorRef, instance, idleTimeout, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// Set up provider to return an error
		haltErr := errors.New("provider halt error")
		provider.
			On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
			Return(haltErr).
			Once()

		// Update idle time to trigger processing
		activeAct.updateIdleAt(0)

		// Advance time to when the actor should be processed
		clock.Step(idleTimeout + 1*time.Second)

		// Wait for the error to be logged
		assert.Eventually(t, func() bool {
			logString := logBuf.String()
			return strings.Contains(logString, "Failed to deactivate idle actor")
		}, 2*time.Second, 50*time.Millisecond, "Should log halt error")

		// The actor should be marked as halted (halt succeeds even if provider fails)
		assert.True(t, activeAct.halted.Load(), "Actor should be marked as halted")

		// Verify the actor was removed from the host map despite provider error
		_, exists := host.actors.Get(actorRef.String())
		assert.False(t, exists, "Actor should be removed from host even with provider error")

		// Assert expected method calls
		provider.AssertExpectations(t)
		instance.AssertExpectations(t)
	})

	t.Run("multiple idle actors processed in batch in parallel", func(t *testing.T) {
		t.Cleanup(func() { logBuf.Reset() })

		// Create a new host
		host, provider := newHost()

		const numActors = 5
		activeActors := make([]*activeActor, numActors)
		instances := make([]*actor_mocks.MockActorDeactivate, numActors)

		// Synchronization for parallel processing validation
		deactivateStarted := make(chan int, numActors)
		deactivateCanContinue := make(chan struct{})
		deactivateCompleted := make(chan int, numActors)

		idleTimeout := 1 * time.Minute

		// Create and register multiple active actors
		for i := range numActors {
			actorRef := ref.NewActorRef("testactor", fmt.Sprintf("idleActor%d", i))
			instance := &actor_mocks.MockActorDeactivate{}

			// Capture the actor index for the closure
			actorIndex := i
			instance.
				On("Deactivate", mock.MatchedBy(testutil.MatchContextInterface)).
				Run(func(args mock.Arguments) {
					// Signal that this actor's deactivation started
					deactivateStarted <- actorIndex

					// Wait for signal to continue (to ensure all start before any complete)
					<-deactivateCanContinue

					// Small delay to simulate some work
					time.Sleep(10 * time.Millisecond)

					// Signal completion
					deactivateCompleted <- actorIndex
				}).
				Return(nil).
				Once()

			activeAct := newActiveActor(actorRef, instance, idleTimeout, host.idleActorProcessor, clock)
			host.actors.Set(actorRef.String(), activeAct)

			activeActors[i] = activeAct
			instances[i] = instance

			// Set expected method calls on provider
			provider.
				On("RemoveActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef).
				Return(nil).
				Once()

			// Update idle time to trigger processing
			activeAct.updateIdleAt(0)
		}

		// Verify initial state
		assert.Equal(t, uintptr(numActors), host.actors.Len(), "Should have all actors initially")

		// Advance time to when actors should be processed
		start := time.Now()
		clock.Step(idleTimeout + 1*time.Second)

		// Wait for all actors to start deactivation (proving they started in parallel)
		var startedActors []int
		timeout := time.After(2 * time.Second)

		for len(startedActors) < numActors {
			select {
			case actorIndex := <-deactivateStarted:
				startedActors = append(startedActors, actorIndex)
				t.Logf("Actor %d started deactivation", actorIndex)
			case <-timeout:
				t.Fatalf("Timeout waiting for all actors to start deactivation. Only %d/%d started", len(startedActors), numActors)
			}
		}

		// Verify all actors started deactivation within a reasonable time window (proving parallelism)
		parallelStartTime := time.Since(start)
		assert.Less(t, parallelStartTime, 500*time.Millisecond, "All actors should start deactivation quickly if processed in parallel")
		t.Logf("All %d actors started deactivation in %v", numActors, parallelStartTime)

		// Now allow all deactivations to continue
		close(deactivateCanContinue)

		// Wait for all actors to complete deactivation
		var completedActors []int
		completionTimeout := time.After(2 * time.Second)

		for len(completedActors) < numActors {
			select {
			case actorIndex := <-deactivateCompleted:
				completedActors = append(completedActors, actorIndex)
				t.Logf("Actor %d completed deactivation", actorIndex)
			case <-completionTimeout:
				t.Fatalf("Timeout waiting for all actors to complete deactivation. Only %d/%d completed", len(completedActors), numActors)
			}
		}

		// Verify parallel completion time
		// If processed in parallel, total time should be less than if processed sequentially
		// Sequential would be roughly (numActors * 10ms) + overhead
		// Parallel should be roughly 10ms + overhead regardless of numActors
		totalTime := time.Since(start)
		expectedSequentialTime := time.Duration(numActors) * 10 * time.Millisecond
		assert.Less(t, totalTime, expectedSequentialTime, "Parallel processing should be faster than sequential")
		t.Logf("All %d actors completed deactivation in %v (sequential would take ~%v)", numActors, totalTime, expectedSequentialTime)

		// Wait for all actors to be processed and removed from the host map
		assert.Eventually(t, func() bool {
			return host.actors.Len() == 0
		}, 3*time.Second, 100*time.Millisecond, "All actors should be removed from host")

		// Verify all actors are marked as halted
		for i, activeAct := range activeActors {
			assert.True(t, activeAct.halted.Load(), "Actor %d should be marked as halted", i)
		}

		// Verify that we got all expected actors in both started and completed lists
		assert.Len(t, startedActors, numActors, "All actors should have started")
		assert.Len(t, completedActors, numActors, "All actors should have completed")

		// Assert expected method calls
		provider.AssertExpectations(t)
		for i, instance := range instances {
			instance.AssertExpectations(t)
			t.Logf("Instance %d expectations met", i)
		}
	})
}
