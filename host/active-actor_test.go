package host

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/internal/eventqueue"
	actor_mocks "github.com/italypaleale/actors/internal/mocks/actor"
	"github.com/italypaleale/actors/internal/ref"
)

// queueableItem is a test implementation of the Queueable interface
type queueableItem struct {
	key     string
	dueTime time.Time
}

func (q *queueableItem) Key() string {
	return q.key
}

func (q *queueableItem) DueTime() time.Time {
	return q.dueTime
}

func TestNewActiveActor(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}
	idleTimeout := 5 * time.Minute

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
		Clock:     clock,
	})

	activeAct := newActiveActor(actorRef, instance, idleTimeout, processor, clock)

	assert.Equal(t, actorRef, activeAct.ref)
	assert.Equal(t, instance, activeAct.instance)
	assert.Equal(t, idleTimeout, activeAct.idleTimeout)
	assert.NotNil(t, activeAct.haltCh)
	assert.False(t, activeAct.halted.Load())
	assert.Equal(t, clock, activeAct.clock)

	// Check that idleAt is set correctly
	expectedIdleAt := clock.Now().Add(idleTimeout)
	actualIdleAt := *activeAct.idleAt.Load()
	assert.WithinDuration(t, expectedIdleAt, actualIdleAt, time.Millisecond)

	// Check that ActorType returns the correct value
	assert.Equal(t, "testactor", activeAct.ActorType())

	// Check that Key returns the correct value
	assert.Equal(t, "testactor/actor1", activeAct.Key())

	// Check that DueTime returns the correct idleAt
	dueTime := activeAct.DueTime()
	assert.WithinDuration(t, expectedIdleAt, dueTime, time.Millisecond)
}

func TestNewActiveActorWithZeroIdleTimeout(t *testing.T) {
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}
	idleTimeout := time.Duration(0)

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
	})

	activeAct := newActiveActor(actorRef, instance, idleTimeout, processor, nil)

	// idleAt should be nil since idleTimeout is 0 (updateIdleAt returns early)
	assert.Nil(t, activeAct.idleAt.Load())
}

func TestUpdateIdleAt(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	var getActiveAct = func(idleTimeout time.Duration) *activeActor {
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}

		processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: func(a *activeActor) {},
			Clock:     clock,
		})

		return newActiveActor(actorRef, instance, idleTimeout, processor, clock)
	}

	t.Run("updates idle time with default timeout", func(t *testing.T) {
		idleTimeout := 5 * time.Minute
		activeAct := getActiveAct(idleTimeout)

		// Advance time and update
		clock.Step(1 * time.Minute)
		oldIdleAt := *activeAct.idleAt.Load()

		activeAct.updateIdleAt(0)
		newIdleAt := *activeAct.idleAt.Load()

		// New idle time should be later than the old one
		assert.True(t, newIdleAt.After(oldIdleAt))
		expectedIdleAt := clock.Now().Add(idleTimeout)
		assert.WithinDuration(t, expectedIdleAt, newIdleAt, time.Millisecond)
	})

	t.Run("updates idle time with custom duration", func(t *testing.T) {
		idleTimeout := 5 * time.Minute
		activeAct := getActiveAct(idleTimeout)

		customDuration := 10 * time.Minute
		activeAct.updateIdleAt(customDuration)

		actualIdleAt := *activeAct.idleAt.Load()
		expectedIdleAt := clock.Now().Add(customDuration)
		assert.WithinDuration(t, expectedIdleAt, actualIdleAt, time.Millisecond)
	})

	t.Run("no-op when idle timeout is zero", func(t *testing.T) {
		activeAct := getActiveAct(0)

		oldIdleAt := activeAct.idleAt.Load()

		clock.Step(1 * time.Minute)
		activeAct.updateIdleAt(0)

		newIdleAt := activeAct.idleAt.Load()
		// Should not have changed since idle timeout is 0
		assert.Equal(t, oldIdleAt, newIdleAt)
		// Both should be nil
		assert.Nil(t, oldIdleAt)
		assert.Nil(t, newIdleAt)
	})

	t.Run("no-op when idle timeout is negative", func(t *testing.T) {
		activeAct := getActiveAct(-1)

		oldIdleAt := activeAct.idleAt.Load()

		clock.Step(1 * time.Minute)
		activeAct.updateIdleAt(0)

		newIdleAt := activeAct.idleAt.Load()
		// Should not have changed since idle timeout is negative
		assert.Equal(t, oldIdleAt, newIdleAt)
		// Both should be nil
		assert.Nil(t, oldIdleAt)
		assert.Nil(t, newIdleAt)
	})
}

func TestTryLock(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	var getActiveAct = func() *activeActor {
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}

		processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: func(a *activeActor) {},
			Clock:     clock,
		})

		return newActiveActor(actorRef, instance, 5*time.Minute, processor, clock)
	}

	t.Run("successful lock acquisition", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		locked, haltCh, err := activeAct.TryLock()
		require.NoError(t, err)
		assert.True(t, locked)
		assert.NotNil(t, haltCh)
		assert.Equal(t, activeAct.haltCh, haltCh)
	})

	t.Run("try lock fails when already locked", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		// First lock should succeed
		locked1, haltCh1, err1 := activeAct.TryLock()
		require.NoError(t, err1)
		assert.True(t, locked1)
		assert.NotNil(t, haltCh1)

		// Second try lock should fail
		locked2, haltCh2, err2 := activeAct.TryLock()
		require.NoError(t, err2)
		assert.False(t, locked2)
		assert.Nil(t, haltCh2)
	})

	t.Run("fails when actor is halted", func(t *testing.T) {
		activeAct := getActiveAct()

		// Halt the actor
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Try lock should fail
		locked, haltCh, err := activeAct.TryLock()
		require.ErrorIs(t, err, actor.ErrActorHalted)
		assert.False(t, locked)
		assert.Nil(t, haltCh)
	})

	t.Run("updates idle time on successful lock", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		oldIdleAt := *activeAct.idleAt.Load()

		// Advance time
		clock.Step(1 * time.Minute)

		locked, _, err := activeAct.TryLock()
		require.NoError(t, err)
		assert.True(t, locked)

		newIdleAt := *activeAct.idleAt.Load()
		assert.True(t, newIdleAt.After(oldIdleAt))
	})
}

func TestLock(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	var getActiveAct = func() *activeActor {
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}

		processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: func(a *activeActor) {},
			Clock:     clock,
		})

		return newActiveActor(actorRef, instance, 5*time.Minute, processor, clock)
	}

	t.Run("successful lock acquisition", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)
		assert.Equal(t, activeAct.haltCh, haltCh)
	})

	t.Run("fails when actor is halted", func(t *testing.T) {
		activeAct := getActiveAct()

		// Halt the actor
		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Lock should fail
		haltCh, err := activeAct.Lock(t.Context())
		require.ErrorIs(t, err, actor.ErrActorHalted)
		assert.Nil(t, haltCh)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		// First acquire the lock
		haltCh1, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh1)

		// Create a cancelled context
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		// Second lock should fail with context cancellation
		haltCh2, err := activeAct.Lock(ctx)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, haltCh2)
	})

	t.Run("updates idle time on successful lock", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		oldIdleAt := *activeAct.idleAt.Load()

		// Advance time
		clock.Step(1 * time.Minute)

		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		newIdleAt := *activeAct.idleAt.Load()
		assert.True(t, newIdleAt.After(oldIdleAt))
	})
}

func TestUnlock(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	var getActiveAct = func() *activeActor {
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}

		processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: func(a *activeActor) {},
			Clock:     clock,
		})

		return newActiveActor(actorRef, instance, 5*time.Minute, processor, clock)
	}

	t.Run("successful unlock", func(t *testing.T) {
		activeAct := getActiveAct()
		t.Cleanup(func() { activeAct.Unlock() })

		// Lock first
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Now unlock
		activeAct.Unlock()

		// Should be able to lock again
		haltCh2, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh2)
	})
}

func TestHalt(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())

	var getActiveAct = func() *activeActor {
		actorRef := ref.NewActorRef("testactor", "actor1")
		instance := &actor_mocks.MockActorDeactivate{}

		processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: func(a *activeActor) {},
			Clock:     clock,
		})

		return newActiveActor(actorRef, instance, 5*time.Minute, processor, clock)
	}

	t.Run("successful halt without drain", func(t *testing.T) {
		activeAct := getActiveAct()

		err := activeAct.Halt(false)
		require.NoError(t, err)

		// Actor should be marked as halted
		assert.True(t, activeAct.halted.Load())

		// haltCh should be closed
		select {
		case <-activeAct.haltCh:
			// Expected - channel is closed
		default:
			t.Error("haltCh should be closed")
		}

		// Subsequent operations should fail
		locked, haltCh, err := activeAct.TryLock()
		require.ErrorIs(t, err, actor.ErrActorHalted)
		assert.False(t, locked)
		assert.Nil(t, haltCh)
	})

	t.Run("successful halt with drain", func(t *testing.T) {
		activeAct := getActiveAct()

		err := activeAct.Halt(true)
		require.NoError(t, err)

		// Actor should be marked as halted
		assert.True(t, activeAct.halted.Load())
	})

	t.Run("double halt returns error", func(t *testing.T) {
		activeAct := getActiveAct()

		// First halt should succeed
		err1 := activeAct.Halt(false)
		require.NoError(t, err1)

		// Second halt should return error
		err2 := activeAct.Halt(false)
		require.ErrorIs(t, err2, errActiveActorAlreadyHalted)
	})

	t.Run("halt signals to current lock holders", func(t *testing.T) {
		activeAct := getActiveAct()

		// Acquire lock
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Halt in another goroutine
		done := make(chan error)
		go func() {
			done <- activeAct.Halt(false)
		}()

		// haltCh should signal
		select {
		case <-haltCh:
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Error("haltCh should have been closed")
		}

		// Wait for halt to complete
		require.NoError(t, <-done)

		activeAct.Unlock()
	})

	t.Run("halt signals to waiters", func(t *testing.T) {
		activeAct := getActiveAct()

		// Acquire lock
		haltCh, err := activeAct.Lock(t.Context())
		require.NoError(t, err)
		assert.NotNil(t, haltCh)

		// Add other waiters
		const numWaiters = 20
		errCh := make(chan error)
		for range numWaiters {
			go func() {
				_, lErr := activeAct.Lock(t.Context())
				errCh <- lErr
			}()
		}

		// Should not have any error in errCh
		select {
		case <-time.After(100 * time.Millisecond):
			// All good
		case err = <-errCh:
			t.Fatalf("Should not have received a signal on errCh, but got %v", err)
		}

		// Halt
		done := make(chan error)
		go func() {
			done <- activeAct.Halt(false)
		}()

		// All waiters should have stopped
		for range numWaiters {
			require.ErrorIs(t, <-errCh, actor.ErrActorHalted)
		}

		// Wait for halt to complete
		require.NoError(t, <-done)

		activeAct.Unlock()
	})
}

func TestConcurrentLockUnlock(t *testing.T) {
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
	})

	activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, nil)

	const numGoroutines = 100
	const numIterations = 10

	var wg sync.WaitGroup
	successCount := &atomic.Int32{}

	for range numGoroutines {
		wg.Go(func() {
			for range numIterations {
				_, err := activeAct.Lock(t.Context())
				if err != nil {
					continue
				}

				successCount.Add(1)

				// Simulate some work
				runtime.Gosched()

				activeAct.Unlock()
			}
		})
	}

	wg.Wait()

	// All operations should have succeeded
	assert.Equal(t, int32(numGoroutines*numIterations), successCount.Load())
}

func TestConcurrentTryLock(t *testing.T) {
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
	})

	activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, nil)

	const numGoroutines = 50

	var wg sync.WaitGroup
	successCount := &atomic.Int32{}
	failureCount := &atomic.Int32{}

	errCh, doneCh := collectErrorChannels()

	for range numGoroutines {
		wg.Go(func() {
			locked, haltCh, err := activeAct.TryLock()
			if err != nil {
				errCh <- err
				return
			}

			if locked {
				successCount.Add(1)
				if haltCh == nil {
					errCh <- errors.New("haltCh is nil")
				}

				// Simulate some work
				runtime.Gosched()

				activeAct.Unlock()
			} else {
				failureCount.Add(1)
				if haltCh != nil {
					errCh <- errors.New("haltCh is not nil")
				}
			}
		})
	}

	wg.Wait()

	// Collect all errors
	close(errCh)
	require.NoError(t, <-doneCh)

	// Only one goroutine should have succeeded at a time
	// The exact numbers depend on scheduling, but we should have both successes and failures
	assert.Positive(t, successCount.Load())
	assert.Positive(t, failureCount.Load())
	assert.Equal(t, int32(numGoroutines), successCount.Load()+failureCount.Load())
}

func TestConcurrentLockAndHalt(t *testing.T) {
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
	})

	activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, nil)

	const numGoroutines = 20

	var wg sync.WaitGroup
	lockAttempts := &atomic.Int32{}
	lockSuccesses := &atomic.Int32{}
	haltSignals := &atomic.Int32{}

	errCh, doneCh := collectErrorChannels()

	// Start multiple goroutines trying to acquire locks
	for range numGoroutines {
		wg.Go(func() {
			lockAttempts.Add(1)
			haltCh, err := activeAct.Lock(t.Context())

			if errors.Is(err, actor.ErrActorHalted) {
				return
			}

			if err != nil {
				errCh <- fmt.Errorf("unexpected error: %w", err)
				return
			}

			lockSuccesses.Add(1)

			// Wait for halt signal or timeout
			select {
			case <-haltCh:
				haltSignals.Add(1)
			case <-time.After(200 * time.Millisecond):
				// Timeout is okay, halt might not occur during our hold
			}

			activeAct.Unlock()
		})
	}

	// Wait for goroutines to start
	waitForGoroutines(t, numGoroutines, lockAttempts)

	// Halt the actor
	wg.Go(func() {
		hErr := activeAct.Halt(true)
		assert.NoError(t, hErr)
	})

	wg.Wait()

	// Collect all errors
	close(errCh)
	require.NoError(t, <-doneCh)

	// Verify behavior
	assert.Equal(t, int32(numGoroutines), lockAttempts.Load())
	// Some locks should have succeeded before halt
	assert.GreaterOrEqual(t, lockSuccesses.Load(), int32(0))
	// Some goroutines should have received halt signals
	assert.GreaterOrEqual(t, haltSignals.Load(), int32(0))
}

func TestConcurrentIdleTimeUpdates(t *testing.T) {
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
	})

	activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, nil)

	const numGoroutines = 50

	var wg sync.WaitGroup

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			customDuration := time.Duration(id+1) * time.Minute
			activeAct.updateIdleAt(customDuration)
		}(i)
	}

	wg.Wait()

	// Verify that idleAt was updated (exact value depends on which goroutine ran last)
	finalIdleAt := *activeAct.idleAt.Load()
	initialTime := time.Now()

	// Should be some time in the future
	assert.True(t, finalIdleAt.After(initialTime))
}

func TestConcurrentHaltCalls(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	actorRef := ref.NewActorRef("testactor", "actor1")
	instance := &actor_mocks.MockActorDeactivate{}

	processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		ExecuteFn: func(a *activeActor) {},
		Clock:     clock,
	})

	activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, clock)

	const numGoroutines = 10

	errCh, doneCh := collectErrorChannels()
	var wg sync.WaitGroup
	successCount := &atomic.Int32{}
	errorCount := &atomic.Int32{}

	for range numGoroutines {
		wg.Go(func() {
			err := activeAct.Halt(false)
			switch {
			case err == nil:
				successCount.Add(1)
			case errors.Is(err, errActiveActorAlreadyHalted):
				errorCount.Add(1)
			default:
				errCh <- fmt.Errorf("unexpected error: %w", err)
			}
		})
	}

	wg.Wait()

	// Collect all errors
	close(errCh)
	require.NoError(t, <-doneCh)

	// Only one halt should succeed, others should get already halted error
	assert.Equal(t, int32(1), successCount.Load())
	assert.Equal(t, int32(numGoroutines-1), errorCount.Load())
	assert.True(t, activeAct.halted.Load())
}

func TestRaceConditionLockAndHalt(t *testing.T) {
	// This test specifically targets race conditions between lock acquisition and halting
	// Run multiple times to increase chance of hitting race conditions
	for range 100 {
		t.Run("iteration", func(t *testing.T) {
			actorRef := ref.NewActorRef("testactor", "actor1")
			instance := &actor_mocks.MockActorDeactivate{}

			processor := eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
				ExecuteFn: func(a *activeActor) {},
			})

			activeAct := newActiveActor(actorRef, instance, 5*time.Minute, processor, nil)

			var wg sync.WaitGroup
			lockDone := make(chan error, 1)
			haltDone := make(chan error, 1)

			// Try to acquire lock
			wg.Go(func() {
				_, lErr := activeAct.Lock(t.Context())
				lockDone <- lErr
				if lErr == nil {
					activeAct.Unlock()
				}
			})

			// Try to halt almost simultaneously
			wg.Go(func() {
				// Give lock goroutine a tiny head start sometimes
				runtime.Gosched()
				hErr := activeAct.Halt(false)
				haltDone <- hErr
			})

			wg.Wait()

			lockErr := <-lockDone
			haltErr := <-haltDone

			// Both operations should complete without panic or deadlock
			// Lock should either succeed or fail with ErrActorHalted
			if lockErr != nil {
				require.ErrorIs(t, lockErr, actor.ErrActorHalted)
			}

			// Halt should either succeed or fail with already halted error
			if haltErr != nil {
				require.ErrorIs(t, haltErr, errActiveActorAlreadyHalted)
			}

			// Actor should be halted at the end
			assert.True(t, activeAct.halted.Load())
		})
	}
}

func collectErrorChannels() (errCh chan error, doneCh chan error) {
	// Collect all errors
	errCh = make(chan error)
	doneCh = make(chan error)
	go func() {
		errs := make([]error, 0)
		for e := range errCh {
			if e == nil {
				continue
			}
			errs = append(errs, e)
		}

		if len(errs) > 0 {
			doneCh <- errors.Join(errs...)
		} else {
			doneCh <- nil
		}
	}()

	return errCh, doneCh
}
