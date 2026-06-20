package actorcore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
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

func TestLockAndInvokeFn(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	newHost := func() *Manager {
		// Create a minimal host for testing
		host := &Manager{
			Actors:              haxmap.New[string, *ActiveActor](8),
			log:                 log,
			clock:               clock,
			shutdownGracePeriod: 5 * time.Second,
			ActorsConfig: map[string]components.ActorHostType{
				"testactor": {
					IdleTimeout: 5 * time.Minute,
				},
			},
			ActorFactories: map[string]actor.Factory{
				"testactor": func(actorID string, service *actor.Service) actor.Actor {
					return &actor_mocks.MockActorDeactivate{}
				},
			},
		}
		host.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
			ExecuteFn: host.HandleIdleActor,
			Clock:     clock,
		})
		return host
	}

	t.Run("successful invocation", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor1")

		// Create a mock function that returns a test value
		testValue := "test-result"
		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return testValue, nil
		}

		// Test successful invocation
		result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
		require.NoError(t, err)
		assert.Equal(t, testValue, result)

		// Verify the actor was created and is in the map
		act, exists := host.Actors.Get(actorRef.String())
		assert.True(t, exists)
		assert.NotNil(t, act)
	})

	t.Run("invocation returns error", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor2")

		// Create a mock function that returns an error
		testError := errors.New("test error")
		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return nil, testError
		}

		// Test invocation with error
		result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
		require.ErrorIs(t, err, testError)
		assert.Nil(t, result)
	})

	t.Run("unsupported actor type", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("unsupported", "actor1")

		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return "result", nil
		}

		// Test with unsupported actor type
		result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
		require.Error(t, err)
		require.ErrorContains(t, err, "unsupported actor type")
		assert.Nil(t, result)
	})

	t.Run("context cancellation before acquiring lock", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor3")

		// Create and register an active actor, then lock it
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 5*time.Minute, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Acquire the lock first
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Create a context that will be canceled quickly
		start := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return "result", nil
		}

		// Test with context that will timeout while waiting for lock
		result, err := host.LockAndInvoke(ctx, actorRef, mockFn)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to acquire lock for actor")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, result)

		// Should take at least 50ms
		assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	})

	t.Run("invoking halted actor returns ErrActorHalted", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor4")

		// Create and register an active actor
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 5*time.Minute, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Halt the actor first
		err := activeAct.Halt(false)
		require.NoError(t, err)

		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return "result", nil
		}

		// Test invocation on halted actor
		result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
		require.ErrorIs(t, err, actor.ErrActorHalted)
		assert.Nil(t, result)
	})

	t.Run("waiting for lock when actor gets halted returns ErrActorHalted", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor5")

		// Create and register an active actor
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 5*time.Minute, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Acquire the lock first
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			return "result", nil
		}

		// Launch LockAndInvoke in a separate goroutine (it will wait for the lock)
		type resultData struct {
			result any
			err    error
		}
		resultCh := make(chan resultData, 1)
		startedCh := make(chan struct{})
		go func() {
			close(startedCh)
			result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
			resultCh <- resultData{result: result, err: err}
		}()

		// Wait for the function to start executing
		select {
		case <-startedCh:
			// All good
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Function did not start executing in 500ms")
		}

		// Sleep for 50ms so we can test the goroutine did wait
		start := time.Now()
		time.Sleep(50 * time.Millisecond)

		// There shouldn't be anything in resultCh
		select {
		case r := <-resultCh:
			t.Fatalf("Unexpected message in resultCh: %v", r)
		default:
			// All good, fallthrough
		}

		// Now halt the actor while the goroutine is waiting for the lock
		err = activeAct.Halt(false)
		require.NoError(t, err)

		// The LockAndInvoke should return ErrActorHalted
		select {
		case r := <-resultCh:
			require.ErrorIs(t, r.err, actor.ErrActorHalted)
			assert.Nil(t, r.result)
			assert.Greater(t, time.Since(start), 50*time.Millisecond)
		case <-time.After(1 * time.Second):
			t.Fatal("LockAndInvoke did not return within timeout")
		}
	})

	t.Run("halt signal triggers graceful cancellation", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor6")

		// Create and register an active actor
		// The idle timeout is disabled (0) so the graceful-shutdown timer is the only waiter the halt registers on the shared fake clock
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 0, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Launch LockAndInvoke in a separate goroutine
		var ctxCanceled atomic.Bool
		errCh := make(chan error, 1)
		startedCh := make(chan struct{})
		go func() {
			_, err := host.LockAndInvoke(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
				close(startedCh)
				// Wait for context cancellation
				select {
				case <-ctx.Done():
					ctxCanceled.Store(true)
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
					return nil, errors.New("should not reach here")
				}
			})
			errCh <- err
		}()

		// Wait for the function to start executing
		select {
		case <-startedCh:
			// All good
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Function did not start executing in 500ms")
		}

		waiters := clock.Waiters()

		// Now halt the actor, which should signal the halt channel
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Wait for the clock to have a new waiter before we advance the time
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, clock.Waiters(), waiters)
		}, 200*time.Millisecond, 10*time.Millisecond)

		// Advance the clock to trigger the graceful timeout
		clock.Step(host.shutdownGracePeriod + time.Second)

		// The function should return with context cancellation error
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
			assert.True(t, ctxCanceled.Load(), "Context should have been canceled")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("LockAndInvoke did not return within timeout")
		}
	})

	t.Run("halt signal and context cancellation", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor6")

		// Create and register an active actor
		// The idle timeout is disabled (0) so the graceful-shutdown timer is the only waiter
		// the halt registers on the shared fake clock. With an idle timeout set, halting also
		// removes the actor's idle-deactivation timer, leaving the waiter count unchanged, so
		// the wait below could never observe the new waiter.
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 0, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Launch LockAndInvoke in a separate goroutine
		errCh := make(chan error, 1)
		startedCh := make(chan struct{})
		go func() {
			_, err := host.LockAndInvoke(ctx, actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
				close(startedCh)
				// Wait for context cancellation
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
					return nil, errors.New("should not reach here")
				}
			})
			errCh <- err
		}()

		// Wait for the function to start executing
		select {
		case <-startedCh:
			// All good
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Function did not start executing in 500ms")
		}

		waiters := clock.Waiters()

		// Now halt the actor, which should signal the halt channel
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Wait for the clock to have a new waiter before we advance the time
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, clock.Waiters(), waiters)
		}, 200*time.Millisecond, 10*time.Millisecond)

		// Cancel the context while waiting for the timeout
		cancel()

		// The function should return with context cancellation error
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("LockAndInvoke did not return within timeout")
		}
	})

	t.Run("function completes before graceful timeout", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor7")

		// Create and register an active actor
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 5*time.Minute, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		// Create a function that completes quickly
		const testValue = "quick-result"
		startedCh := make(chan struct{})
		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			close(startedCh)
			return testValue, nil
		}

		// Launch LockAndInvoke in a separate goroutine
		type resultData struct {
			result any
			err    error
		}
		resultCh := make(chan resultData, 1)
		go func() {
			result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
			resultCh <- resultData{result: result, err: err}
		}()

		// Wait for the function to start executing
		select {
		case <-startedCh:
			// All good
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Function did not start executing in 500ms")
		}
		runtime.Gosched()

		// Now halt the actor, which should signal the halt channel
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// The function should complete successfully before the graceful timeout
		select {
		case r := <-resultCh:
			require.NoError(t, r.err)
			assert.Equal(t, testValue, r.result)
		case <-time.After(1 * time.Second):
			t.Fatal("LockAndInvoke did not return within timeout")
		}
	})

	t.Run("concurrent invocations with different actors", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		const numActors = 5
		var wg sync.WaitGroup
		results := make([]any, numActors)
		errors := make([]error, numActors)

		for i := range numActors {
			wg.Add(1)
			go func(actorID int) {
				defer wg.Done()
				actorRef := ref.NewActorRef("testactor", fmt.Sprintf("actor%d", actorID))
				expectedResult := fmt.Sprintf("result%d", actorID)

				mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
					// Add small delay to simulate work
					time.Sleep(10 * time.Millisecond)
					return expectedResult, nil
				}

				result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
				results[actorID] = result
				errors[actorID] = err
			}(i)
		}

		wg.Wait()

		// Verify all invocations succeeded
		for i := range numActors {
			require.NoError(t, errors[i], "Actor %d should succeed", i)
			assert.Equal(t, fmt.Sprintf("result%d", i), results[i])
		}

		// Verify all actors were created
		assert.Equal(t, uintptr(numActors), host.Actors.Len())
	})

	t.Run("concurrent invocations with same actor are serialized", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "shared-actor")

		const numInvocations = 5
		var wg sync.WaitGroup
		results := make([]any, numInvocations)
		errors := make([]error, numInvocations)
		var executionOrder []int
		var orderMutex sync.Mutex

		for i := range numInvocations {
			wg.Add(1)
			go func(invocationID int) {
				defer wg.Done()

				mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
					orderMutex.Lock()
					executionOrder = append(executionOrder, invocationID)
					orderMutex.Unlock()

					// Add delay to ensure serialization is tested
					time.Sleep(20 * time.Millisecond)
					return fmt.Sprintf("result%d", invocationID), nil
				}

				result, err := host.LockAndInvoke(t.Context(), actorRef, mockFn)
				results[invocationID] = result
				errors[invocationID] = err
			}(i)
		}

		wg.Wait()

		// Verify all invocations succeeded
		for i := range numInvocations {
			require.NoError(t, errors[i], "Invocation %d should succeed", i)
			assert.Equal(t, fmt.Sprintf("result%d", i), results[i])
		}

		// Verify execution was serialized (exactly numInvocations entries)
		assert.Len(t, executionOrder, numInvocations)

		// Only one actor should exist
		assert.Equal(t, uintptr(1), host.Actors.Len())
	})

	t.Run("parent context cancellation", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor8")

		// Create a context with timeout
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		mockFn := func(ctx context.Context, act *ActiveActor) (any, error) {
			// Wait longer than the context timeout
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return "should not reach here", nil
			}
		}

		// Test invocation with context timeout
		result, err := host.LockAndInvoke(ctx, actorRef, mockFn)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, result)
	})
}

func TestLockAndInvokeActive(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	newHost := func() *Manager {
		host := &Manager{
			Actors:              haxmap.New[string, *ActiveActor](8),
			log:                 log,
			clock:               clock,
			shutdownGracePeriod: 5 * time.Second,
			ActorsConfig: map[string]components.ActorHostType{
				"testactor": {IdleTimeout: 5 * time.Minute},
			},
			ActorFactories: map[string]actor.Factory{
				"testactor": func(actorID string, service *actor.Service) actor.Actor {
					return &actor_mocks.MockActorDeactivate{}
				},
			},
		}
		host.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
			ExecuteFn: host.HandleIdleActor,
			Clock:     clock,
		})
		return host
	}

	t.Run("invokes an already-active actor", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "actor1")

		// Pre-activate the actor
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := NewActiveActor(actorRef, instance, 5*time.Minute, host.IdleProcessor, clock)
		host.Actors.Set(actorRef.String(), activeAct)

		called := false
		result, err := host.LockAndInvokeActive(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
			called = true
			return "ok", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "ok", result)
		assert.True(t, called)
	})

	t.Run("returns ErrActorNotActive and does not create the actor", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host := newHost()
		defer host.IdleProcessor.Close()

		actorRef := ref.NewActorRef("testactor", "inactive")

		called := false
		result, err := host.LockAndInvokeActive(t.Context(), actorRef, func(ctx context.Context, act *ActiveActor) (any, error) {
			called = true
			return "ok", nil
		})
		require.ErrorIs(t, err, actor.ErrActorNotActive)
		assert.Nil(t, result)
		assert.False(t, called, "the invocation function must not run for an inactive actor")

		// The actor must not have been created
		_, exists := host.Actors.Get(actorRef.String())
		assert.False(t, exists, "an active-only invocation must never activate the actor")
	})
}
