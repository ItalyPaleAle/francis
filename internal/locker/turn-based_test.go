package locker

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTurnBasedLocker_Lock(t *testing.T) {
	t.Run("first acquire succeeds", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.Lock(t.Context())
		require.NoError(t, err)
		assert.True(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("already stopped returns error", func(t *testing.T) {
		locker := &TurnBasedLocker{stopped: true}

		err := locker.Lock(t.Context())
		require.ErrorIs(t, err, ErrStopped)
	})

	t.Run("context cancellation removes from queue", func(t *testing.T) {
		locker := &TurnBasedLocker{}
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Second and third goroutines wait for the lock
		done := make(chan error, 2)
		go func() {
			done <- locker.Lock(ctx)
		}()
		go func() {
			done <- locker.Lock(ctx)
		}()

		// Wait for the 2 goroutines to be queued
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond, "Queue's length was not 1 before the deadline")

		// Cancel the context
		cancel()

		for range 2 {
			select {
			case err = <-done:
				// Got the error
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(3 * time.Second):
				t.Fatal("Did not receive an error in 3s")
			}
		}

		// Queue should be empty, still locked
		assert.True(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("FIFO ordering", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		const numWaiters = 5
		results := make([]chan int, numWaiters)

		// Start multiple goroutines waiting for the lock
		var wg sync.WaitGroup
		for i := range numWaiters {
			results[i] = make(chan int, 1)
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rErr := locker.Lock(t.Context())
				if rErr == nil {
					results[id] <- id
					// Hold the lock briefly
					time.Sleep(10 * time.Millisecond)
					locker.Unlock()
				}
			}(i)
		}

		// Wait for all goroutines to queue up
		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numWaiters, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numWaiters)

		// Release the initial lock
		locker.Unlock()

		// Wait for all goroutines to complete
		wg.Wait()

		// Verify FIFO ordering
		for i := range numWaiters {
			select {
			case id := <-results[i]:
				assert.Equal(t, i, id)
			default:
				t.Errorf("Goroutine %d did not complete", i)
			}
		}
	})
}

func TestTurnBasedLocker_TryLock(t *testing.T) {
	t.Run("succeeds when unlocked", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		acquired, err := locker.TryLock()
		require.NoError(t, err)
		assert.True(t, acquired)

		assert.True(t, locker.IsLocked())
	})

	t.Run("fails when already locked", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First acquire succeeds
		acquired, err := locker.TryLock()
		require.NoError(t, err)
		assert.True(t, acquired)

		// Second acquire fails
		acquired, err = locker.TryLock()
		require.NoError(t, err)
		assert.False(t, acquired)

		assert.True(t, locker.IsLocked())
	})

	t.Run("returns error when stopped", func(t *testing.T) {
		locker := &TurnBasedLocker{}
		locker.Stop()

		_, err := locker.TryLock()
		require.ErrorIs(t, err, ErrStopped)

		assert.False(t, locker.IsLocked())
	})

	t.Run("does not affect queue", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires with Lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Second goroutine waits in queue
		go func() {
			_ = locker.Lock(t.Context())
		}()

		// Wait for the goroutine to be queued
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		// TryLock should fail and not affect the queue
		acquired, err := locker.TryLock()
		require.NoError(t, err)
		assert.False(t, acquired)

		assert.Equal(t, 1, locker.QueueLength())
	})

	t.Run("concurrent try locks", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		const numGoroutines = 100
		results := make(chan bool, numGoroutines)
		errors := make(chan error, numGoroutines)

		var wg sync.WaitGroup
		for range numGoroutines {
			wg.Go(func() {
				acquired, err := locker.TryLock()
				results <- acquired
				errors <- err
			})
		}

		wg.Wait()
		close(results)
		close(errors)

		// Exactly one should succeed
		successCount := 0
		for acquired := range results {
			if acquired {
				successCount++
			}
		}
		assert.Equal(t, 1, successCount)

		// All should have no error
		for err := range errors {
			require.NoError(t, err)
		}
	})

	t.Run("unlock after try lock", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Acquire with TryLock
		acquired, err := locker.TryLock()
		require.NoError(t, err)
		assert.True(t, acquired)

		assert.True(t, locker.IsLocked())

		// Unlock should work normally
		locker.Unlock()
		assert.False(t, locker.IsLocked())

		// Another TryLock should succeed
		acquired, err = locker.TryLock()
		require.NoError(t, err)
		assert.True(t, acquired)
	})
}

func TestTurnBasedLocker_Unlock(t *testing.T) {
	t.Run("do nothing if unlocked", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Should not panic or cause issues
		locker.Unlock()
		assert.False(t, locker.IsLocked())
	})

	t.Run("no waiters unlocks", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.Lock(t.Context())
		require.NoError(t, err)
		assert.True(t, locker.IsLocked())

		locker.Unlock()
		assert.False(t, locker.IsLocked())
	})

	t.Run("passes lock to next waiter", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Second goroutine waits
		lockAcquired := make(chan struct{})
		go func() {
			err := locker.Lock(t.Context())
			if err == nil {
				close(lockAcquired)
			}
		}()

		// Wait for the second goroutine to be queued
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond, "Queue's length was not 1 before the deadline")

		// Release the lock
		locker.Unlock()

		// Second goroutine should acquire the lock
		select {
		case <-lockAcquired:
			// Success
		case <-time.After(3 * time.Second):
			t.Error("Second goroutine did not acquire the lock in 3s")
		}

		assert.True(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})
}

func TestTurnBasedLocker_Stop(t *testing.T) {
	t.Run("sets stopped", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		assert.False(t, locker.IsStopped())
		locker.Stop()
		assert.True(t, locker.IsStopped())
	})

	t.Run("cancels all waiters", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		const numWaiters = 3
		results := make([]chan error, numWaiters)

		// Start multiple waiting goroutines
		for i := range numWaiters {
			results[i] = make(chan error, 1)
			go func(resultChan chan error) {
				resultChan <- locker.Lock(t.Context())
			}(results[i])
		}

		// Wait for goroutines to queue up
		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numWaiters, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numWaiters)

		// Stop the locker
		locker.Stop()

		// All waiters should receive ErrStopped
		for i := range numWaiters {
			select {
			case err := <-results[i]:
				require.ErrorIs(t, err, ErrStopped)
			case <-time.After(3 * time.Second):
				t.Errorf("Waiter %d did not receive error in 3s", i)
			}
		}

		assert.True(t, locker.stopped)
		assert.Equal(t, 0, locker.QueueLength())
	})
}

func TestTurnBasedLocker_StopAndWait(t *testing.T) {
	t.Run("sets stopped", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		assert.False(t, locker.IsStopped())
		locker.StopAndWait()
		assert.True(t, locker.IsStopped())
	})

	t.Run("cancels all waiters", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		const numWaiters = 3
		results := make([]chan error, numWaiters)

		// Start multiple waiting goroutines
		for i := range numWaiters {
			results[i] = make(chan error, 1)
			go func(resultChan chan error) {
				resultChan <- locker.Lock(t.Context())
			}(results[i])
		}

		// Wait for goroutines to queue up
		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numWaiters, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numWaiters)

		// Call StopAndWait in a background goroutine
		go locker.StopAndWait()

		// All waiters should receive ErrStopped
		for i := range numWaiters {
			select {
			case err := <-results[i]:
				require.ErrorIs(t, err, ErrStopped)
			case <-time.After(3 * time.Second):
				t.Errorf("Waiter %d did not receive error in 3s", i)
			}
		}

		assert.True(t, locker.stopped)
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("waits for current lock holder to unlock", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock and holds it
		lockHeld := make(chan error)
		unlockSignal := make(chan struct{})
		lockReleased := make(chan struct{})

		go func() {
			lockHeld <- locker.Lock(t.Context())

			// Wait for signal to unlock
			<-unlockSignal
			locker.Unlock()
			close(lockReleased)
		}()

		// Wait for the lock to be acquired
		require.NoError(t, <-lockHeld)

		// Start StopAndWait in another goroutine
		stopCompleted := make(chan struct{})
		go func() {
			locker.StopAndWait()
			close(stopCompleted)
		}()

		// Give StopAndWait some time to start, but it shouldn't complete yet
		select {
		case <-stopCompleted:
			t.Error("StopAndWait completed before lock was released")
		case <-time.After(100 * time.Millisecond):
			// Expected - StopAndWait should be waiting
		}

		// Signal the lock holder to unlock
		close(unlockSignal)

		// Wait for the lock to be released
		<-lockReleased

		// Now StopAndWait should complete
		select {
		case <-stopCompleted:
			// Expected
		case <-time.After(3 * time.Second):
			t.Error("StopAndWait did not complete after lock was released")
		}

		assert.True(t, locker.IsStopped())
		assert.False(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("waits for current lock holder with waiters in queue", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		unlockSignal := make(chan struct{})
		lockReleased := make(chan struct{})

		err := locker.Lock(t.Context())
		require.NoError(t, err)

		go func() {
			// Wait for signal to unlock
			<-unlockSignal
			locker.Unlock()
			close(lockReleased)
		}()

		const numWaiters = 3
		results := make([]chan error, numWaiters)

		// Start multiple waiting goroutines
		for i := range numWaiters {
			results[i] = make(chan error, 1)
			go func(resultChan chan error) {
				resultChan <- locker.Lock(t.Context())
			}(results[i])
		}

		// Wait for goroutines to queue up
		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numWaiters, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numWaiters)

		// Start StopAndWait in another goroutine
		stopCompleted := make(chan struct{})
		go func() {
			locker.StopAndWait()
			close(stopCompleted)
		}()

		// Give StopAndWait some time to start, but it shouldn't complete yet
		select {
		case <-stopCompleted:
			t.Error("StopAndWait completed before lock was released")
		case <-time.After(100 * time.Millisecond):
			// Expected - StopAndWait should be waiting
		}

		// All waiters should receive ErrStopped immediately (they don't need to wait for unlock)
		for i := range numWaiters {
			select {
			case err := <-results[i]:
				require.ErrorIs(t, err, ErrStopped)
			case <-time.After(3 * time.Second):
				t.Errorf("Waiter %d did not receive error in 3s", i)
			}
		}

		// StopAndWait should still be waiting for the lock holder
		select {
		case <-stopCompleted:
			t.Error("StopAndWait completed before lock was released")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		// Signal the lock holder to unlock
		close(unlockSignal)

		// Wait for the lock to be released
		<-lockReleased

		// Now StopAndWait should complete
		select {
		case <-stopCompleted:
			// Expected
		case <-time.After(3 * time.Second):
			t.Error("StopAndWait did not complete after lock was released")
		}

		assert.True(t, locker.IsStopped())
		assert.False(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("returns immediately when not locked", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Add some waiters to the queue
		const numWaiters = 2
		results := make([]chan error, numWaiters)

		for i := range numWaiters {
			results[i] = make(chan error, 1)
			go func(resultChan chan error) {
				resultChan <- locker.Lock(t.Context())
			}(results[i])
		}

		// Wait for goroutines to queue up
		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numWaiters-1, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numWaiters-1)

		// Now unlock both so the locker is free
		locker.Unlock()
		locker.Unlock()

		// StopAndWait should complete immediately and should not set any closing waiter
		locker.StopAndWait()

		assert.Nil(t, locker.closingWaiter)
		assert.True(t, locker.IsStopped())
		assert.False(t, locker.IsLocked())
	})

	t.Run("multiple StopAndWait calls", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First StopAndWait
		locker.StopAndWait()
		assert.True(t, locker.IsStopped())

		// Second StopAndWait should not cause issues
		locker.StopAndWait()
		assert.True(t, locker.IsStopped())
	})

	t.Run("concurrent StopAndWait calls", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		lockHeld := make(chan error)
		unlockSignal := make(chan struct{})

		go func() {
			lockHeld <- locker.Lock(t.Context())

			// Wait for signal to unlock
			<-unlockSignal
			locker.Unlock()
		}()

		// Wait for the lock to be acquired
		require.NoError(t, <-lockHeld)

		// Start multiple StopAndWait calls concurrently
		const numStoppers = 3
		stopCompleted := make([]chan struct{}, numStoppers)

		for i := range numStoppers {
			stopCompleted[i] = make(chan struct{})
			go func(completeChan chan struct{}) {
				locker.StopAndWait()
				close(completeChan)
			}(stopCompleted[i])
		}

		// Give all StopAndWait calls time to start
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			locker.mu.Lock()
			defer locker.mu.Unlock()
			assert.NotNil(c, locker.closingWaiter)
		}, 3*time.Second, 50*time.Millisecond, "Closing waiter channel was not set within the deadline")

		// Signal the lock holder to unlock
		close(unlockSignal)

		// All StopAndWait calls should complete
		for i := range numStoppers {
			select {
			case <-stopCompleted[i]:
				// Expected
			case <-time.After(3 * time.Second):
				t.Errorf("StopAndWait %d did not complete in 3s", i)
			}
		}

		assert.True(t, locker.IsStopped())
		assert.False(t, locker.IsLocked())
	})

	t.Run("call StopAndWait after Stop", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Call Stop
		locker.Stop()

		start := time.Now()
		locker.StopAndWait()

		// Should return immediately
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("call StopAndWait after Stop and Unlock", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Acquire a lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Wait for signal to unlock
		unlockSignal := make(chan struct{})
		lockReleased := make(chan struct{})
		go func() {
			<-unlockSignal
			locker.Unlock()
			close(lockReleased)
		}()

		// Call Stop
		locker.Stop()

		// Start StopAndWait in another goroutine
		stopCompleted := make(chan struct{})
		go func() {
			locker.StopAndWait()
			close(stopCompleted)
		}()

		// StopAndWait should still be waiting for the lock holder
		select {
		case <-stopCompleted:
			t.Error("StopAndWait completed before lock was released")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		// Signal the lock holder to unlock
		close(unlockSignal)

		// Wait for the lock to be released
		<-lockReleased

		// Now StopAndWait should complete
		select {
		case <-stopCompleted:
			// Expected
		case <-time.After(3 * time.Second):
			t.Error("StopAndWait did not complete after lock was released")
		}
	})
}

func TestTurnBasedLocker_IsStopped(t *testing.T) {
	t.Run("is thread safe", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// Test concurrent access to IsStopped
		var wg sync.WaitGroup
		const numGoroutines = 100

		for range numGoroutines {
			wg.Go(func() {
				locker.IsStopped()
			})
		}

		// Stop in the middle of concurrent reads
		go func() {
			time.Sleep(1 * time.Millisecond)
			locker.Stop()
		}()

		wg.Wait()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, locker.IsStopped())
		}, 3*time.Second, 10*time.Millisecond)
	})
}

func TestTurnBasedLocker(t *testing.T) {
	t.Run("concurrent operations", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		var wg sync.WaitGroup
		const numOperations = 50
		results := make([]bool, numOperations)

		// Start multiple goroutines doing lock/unlock cycles
		for i := range numOperations {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				err := locker.Lock(t.Context())
				if err == nil {
					results[id] = true
					// Simulate some work
					time.Sleep(5 * time.Millisecond)
					locker.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// All operations should have succeeded
		for i, success := range results {
			assert.True(t, success, "Operation %d", i)
		}

		// Locker should be unlocked at the end
		assert.False(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("context timeout", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Second goroutine tries to acquire with timeout
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- locker.Lock(ctx)
		}()

		// Wait for goroutine to be queued
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond, "Queue's length was not 1 before the deadline")

		// Wait for context timeout in real time since locker doesn't use fake clock
		select {
		case err = <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(3 * time.Second):
			t.Error("Did not receive context timeout in 3s")
		}

		// Queue should be empty after timeout
		assert.Equal(t, 0, locker.QueueLength())
	})

	t.Run("stop while waiting", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First goroutine acquires the lock
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Second goroutine waits
		done := make(chan error, 1)
		go func() {
			done <- locker.Lock(t.Context())
		}()

		// Wait for second goroutine to queue up
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond, "Queue's length was not 1 before the deadline")

		// Stop the locker
		locker.Stop()

		// Second goroutine should receive ErrStopped
		select {
		case err := <-done:
			require.ErrorIs(t, err, ErrStopped)
		case <-time.After(3 * time.Second):
			t.Error("Did not receive ErrStopped in 3s")
		}
	})

	t.Run("multiple unlocks", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// First unlock should work
		locker.Unlock()
		assert.False(t, locker.IsLocked())

		// Additional unlocks should not cause issues
		locker.Unlock()
		locker.Unlock()
		assert.False(t, locker.IsLocked())
	})

	t.Run("multiple stops", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// First stop
		locker.Stop()
		assert.True(t, locker.IsStopped())

		// Second stop should not cause issues
		locker.Stop()
		assert.True(t, locker.IsStopped())
	})
}

func TestTurnBasedLocker_GrantCancelRace(t *testing.T) {
	// If the granted-but-canceled waiter returned without releasing, the lock would be leaked and the locker would wedge forever
	t.Run("grant racing context cancellation does not leak the lock", func(t *testing.T) {
		// Each iteration is its own func so the deferred cancel runs per iteration
		iteration := func() {
			l := &TurnBasedLocker{}

			// Hold the lock so the next caller has to queue
			err := l.Lock(t.Context())
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			// The waiter queues, then releases the lock if it ends up being granted it
			var werr error
			waiterDone := make(chan struct{})
			go func() {
				werr = l.Lock(ctx)
				if werr == nil {
					l.Unlock()
				}
				close(waiterDone)
			}()

			// Wait until the waiter is parked in the queue
			for l.QueueLength() == 0 {
				runtime.Gosched()
			}

			// Fire the cancellation and the unlock as concurrently as possible to maximize the chance they interleave
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				cancel()
			}()
			go func() {
				defer wg.Done()
				<-start
				l.Unlock()
			}()
			close(start)
			wg.Wait()

			<-waiterDone

			// Whatever the interleaving, the lock must end up free and the queue empty
			require.False(t, l.IsLocked(), "lock leaked after grant/cancel race")
			require.Zero(t, l.QueueLength())

			// A fresh acquisition must succeed, proving the locker is not wedged
			acquired, tryErr := l.TryLock()
			require.NoError(t, tryErr)
			require.True(t, acquired, "locker wedged after grant/cancel race")
		}

		for range 2000 {
			iteration()
		}
	})
}

func TestTurnBasedLocker_RLock(t *testing.T) {
	t.Run("first acquire succeeds", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.RLock(t.Context())
		require.NoError(t, err)
		assert.True(t, locker.IsLocked())
		assert.Equal(t, 0, locker.QueueLength())
		assert.Equal(t, 1, locker.readers)

		locker.RUnlock()
	})

	t.Run("already stopped returns error", func(t *testing.T) {
		locker := &TurnBasedLocker{stopped: true}

		err := locker.RLock(t.Context())
		require.ErrorIs(t, err, ErrStopped)
	})

	t.Run("multiple readers proceed concurrently", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		const numReaders = 10
		var (
			active    atomic.Int32
			maxActive atomic.Int32
			wg        sync.WaitGroup
		)
		errs := make(chan error, numReaders)

		for range numReaders {
			wg.Go(func() {
				err := locker.RLock(t.Context())
				if err != nil {
					errs <- err
					return
				}

				n := active.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}

				time.Sleep(20 * time.Millisecond)

				active.Add(-1)
				locker.RUnlock()
			})
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}

		// More than one reader must have been active at the same time, proving they ran concurrently rather than serialized
		assert.Greater(t, maxActive.Load(), int32(1))
		assert.False(t, locker.IsLocked())
	})

	t.Run("queues behind a waiting writer to avoid writer starvation", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// An active reader forces the writer below to queue
		err := locker.RLock(t.Context())
		require.NoError(t, err)

		writerAcquired := make(chan struct{})
		go func() {
			wErr := locker.Lock(t.Context())
			if wErr == nil {
				close(writerAcquired)
			}
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		// A new reader arriving while a writer is queued must queue too, rather than joining the active reader and starving the writer
		readerAcquired := make(chan struct{})
		go func() {
			rErr := locker.RLock(t.Context())
			if rErr == nil {
				close(readerAcquired)
			}
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		select {
		case <-writerAcquired:
			t.Fatal("writer acquired the lock while the original reader still held it")
		default:
		}

		// Releasing the original reader must grant the queued writer next, not the queued reader
		locker.RUnlock()

		select {
		case <-writerAcquired:
		case <-time.After(3 * time.Second):
			t.Fatal("writer did not acquire the lock after the reader released it")
		}

		select {
		case <-readerAcquired:
			t.Fatal("queued reader acquired the lock before the writer")
		default:
		}

		locker.Unlock()

		select {
		case <-readerAcquired:
		case <-time.After(3 * time.Second):
			t.Fatal("queued reader did not acquire the lock after the writer released it")
		}

		locker.RUnlock()
	})

	t.Run("context cancellation removes from queue", func(t *testing.T) {
		locker := &TurnBasedLocker{}
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		// A writer holds the lock so the readers below have to queue
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		done := make(chan error, 2)
		go func() {
			done <- locker.RLock(ctx)
		}()
		go func() {
			done <- locker.RLock(ctx)
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		cancel()

		for range 2 {
			select {
			case err = <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(3 * time.Second):
				t.Fatal("Did not receive an error in 3s")
			}
		}

		assert.Equal(t, 0, locker.QueueLength())
		locker.Unlock()
	})
}

func TestTurnBasedLocker_WriterReaderExclusion(t *testing.T) {
	t.Run("writer excludes readers", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.Lock(t.Context())
		require.NoError(t, err)

		readerDone := make(chan error, 1)
		go func() {
			readerDone <- locker.RLock(t.Context())
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		select {
		case <-readerDone:
			t.Fatal("reader acquired the lock while the writer held it")
		case <-time.After(50 * time.Millisecond):
			// Expected
		}

		locker.Unlock()

		select {
		case err = <-readerDone:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("reader did not acquire the lock after the writer released it")
		}

		locker.RUnlock()
	})

	t.Run("readers exclude a writer until every one of them releases", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.RLock(t.Context())
		require.NoError(t, err)
		err = locker.RLock(t.Context())
		require.NoError(t, err)

		writerDone := make(chan error, 1)
		go func() {
			writerDone <- locker.Lock(t.Context())
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		select {
		case <-writerDone:
			t.Fatal("writer acquired the lock while readers held it")
		case <-time.After(50 * time.Millisecond):
			// Expected
		}

		// Releasing only one of the two readers must not be enough to grant the writer
		locker.RUnlock()

		select {
		case <-writerDone:
			t.Fatal("writer acquired the lock while a reader still held it")
		case <-time.After(50 * time.Millisecond):
			// Expected
		}

		locker.RUnlock()

		select {
		case err = <-writerDone:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("writer did not acquire the lock after the last reader released it")
		}

		locker.Unlock()
	})
}

func TestTurnBasedLocker_ReaderBatching(t *testing.T) {
	t.Run("grants every queued reader together in one batch", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		// A writer holds the lock so the readers below all queue up behind it
		err := locker.Lock(t.Context())
		require.NoError(t, err)

		const numReaders = 5
		acquired := make(chan int, numReaders)
		for i := range numReaders {
			go func(id int) {
				rErr := locker.RLock(t.Context())
				if rErr == nil {
					acquired <- id
				}
			}(i)
		}

		assert.EventuallyWithTf(t, func(c *assert.CollectT) {
			assert.Equal(c, numReaders, locker.QueueLength())
		}, 3*time.Second, 50*time.Millisecond, "Queue's length was not %d before the deadline", numReaders)

		// Releasing the writer should hand the lock to every queued reader at once
		locker.Unlock()

		for range numReaders {
			select {
			case <-acquired:
			case <-time.After(3 * time.Second):
				t.Fatal("not all readers were granted the lock")
			}
		}

		assert.Equal(t, numReaders, locker.readers)
		assert.Equal(t, 0, locker.QueueLength())

		for range numReaders {
			locker.RUnlock()
		}
		assert.False(t, locker.IsLocked())
	})

	t.Run("batch stops at the next queued writer", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.Lock(t.Context())
		require.NoError(t, err)

		// Two readers, then a writer, then one more reader all queue up behind the held writer
		readerAcquired := make(chan int, 3)
		writerAcquired := make(chan struct{})
		go func() {
			if locker.RLock(t.Context()) == nil {
				readerAcquired <- 1
			}
		}()
		go func() {
			if locker.RLock(t.Context()) == nil {
				readerAcquired <- 2
			}
		}()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		go func() {
			if locker.Lock(t.Context()) == nil {
				close(writerAcquired)
			}
		}()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 3, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		go func() {
			if locker.RLock(t.Context()) == nil {
				readerAcquired <- 3
			}
		}()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 4, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		// Releasing the held writer must grant only the first two readers, leaving the writer and the trailing reader queued
		locker.Unlock()

		for range 2 {
			select {
			case <-readerAcquired:
			case <-time.After(3 * time.Second):
				t.Fatal("first batch of readers was not granted the lock")
			}
		}

		select {
		case <-writerAcquired:
			t.Fatal("writer acquired the lock ahead of its turn")
		case <-readerAcquired:
			t.Fatal("trailing reader acquired the lock ahead of the queued writer")
		case <-time.After(50 * time.Millisecond):
			// Expected: the writer and the trailing reader are still queued
		}

		assert.Equal(t, 2, locker.readers)
		assert.Equal(t, 2, locker.QueueLength())

		locker.RUnlock()
		locker.RUnlock()

		select {
		case <-writerAcquired:
		case <-time.After(3 * time.Second):
			t.Fatal("writer did not acquire the lock after both readers released")
		}

		locker.Unlock()

		select {
		case <-readerAcquired:
		case <-time.After(3 * time.Second):
			t.Fatal("trailing reader did not acquire the lock after the writer released")
		}

		locker.RUnlock()
	})
}

func TestTurnBasedLocker_TryLockWithReaders(t *testing.T) {
	t.Run("fails while any reader is active", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.RLock(t.Context())
		require.NoError(t, err)

		acquired, err := locker.TryLock()
		require.NoError(t, err)
		assert.False(t, acquired)

		locker.RUnlock()

		acquired, err = locker.TryLock()
		require.NoError(t, err)
		assert.True(t, acquired)
		locker.Unlock()
	})
}

func TestTurnBasedLocker_StopWithReaders(t *testing.T) {
	t.Run("cancels the queue without draining active readers", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.RLock(t.Context())
		require.NoError(t, err)

		errCh := make(chan error, 1)
		go func() {
			errCh <- locker.Lock(t.Context())
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, locker.QueueLength())
		}, 3*time.Second, 10*time.Millisecond)

		locker.Stop()

		select {
		case err = <-errCh:
			require.ErrorIs(t, err, ErrStopped)
		case <-time.After(3 * time.Second):
			t.Fatal("queued writer did not receive ErrStopped")
		}

		// Stop cancels only queued waiters: the already-active reader is untouched
		assert.True(t, locker.IsLocked())
		locker.RUnlock()
		assert.False(t, locker.IsLocked())
	})
}

func TestTurnBasedLocker_StopAndWaitWithReaders(t *testing.T) {
	t.Run("waits for every active reader to release before returning", func(t *testing.T) {
		locker := &TurnBasedLocker{}

		err := locker.RLock(t.Context())
		require.NoError(t, err)
		err = locker.RLock(t.Context())
		require.NoError(t, err)

		stopCh := make(chan struct{})
		go func() {
			locker.StopAndWait()
			close(stopCh)
		}()

		select {
		case <-stopCh:
			t.Fatal("StopAndWait completed while readers were still active")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		// Releasing only one of the two readers must not be enough
		locker.RUnlock()

		select {
		case <-stopCh:
			t.Fatal("StopAndWait completed while a reader was still active")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		locker.RUnlock()

		select {
		case <-stopCh:
			// Expected
		case <-time.After(3 * time.Second):
			t.Fatal("StopAndWait did not complete after the last reader released")
		}

		assert.True(t, locker.IsStopped())
		assert.False(t, locker.IsLocked())
	})
}

func TestTurnBasedLocker_RLockGrantCancelRace(t *testing.T) {
	// Mirrors TestTurnBasedLocker_GrantCancelRace for the shared side: if a canceled reader failed to release its just-granted slot, the reader count would leak and the locker would wedge forever
	t.Run("grant racing context cancellation does not leak a reader slot", func(t *testing.T) {
		iteration := func() {
			l := &TurnBasedLocker{}

			// Hold the lock exclusively so the next RLock has to queue
			err := l.Lock(t.Context())
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			waiterDone := make(chan struct{})
			go func() {
				werr := l.RLock(ctx)
				if werr == nil {
					l.RUnlock()
				}
				close(waiterDone)
			}()

			for l.QueueLength() == 0 {
				runtime.Gosched()
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				cancel()
			})
			wg.Go(func() {
				<-start
				l.Unlock()
			})
			close(start)
			wg.Wait()

			<-waiterDone

			require.False(t, l.IsLocked(), "lock leaked after grant/cancel race")
			require.Zero(t, l.QueueLength())

			acquired, tryErr := l.TryLock()
			require.NoError(t, tryErr)
			require.True(t, acquired, "locker wedged after grant/cancel race")
			l.Unlock()
		}

		for range 2000 {
			iteration()
		}
	})
}

// TestTurnBasedLocker_QueueOrderingAroundLongRunningHead simulates a long-running holder as a goroutine parked on a channel, rather than a sleep, so the test controls exactly when it exits and can assert on the exact instant the queue is unblocked
// It walks through both directions required by the RW contract: while a Peek is the active head, a later Peek must join immediately but an Invoke must wait
// While an Invoke is the active head, every Peek must wait, and releasing the Invoke grants them all together
func TestTurnBasedLocker_QueueOrderingAroundLongRunningHead(t *testing.T) {
	l := &TurnBasedLocker{}

	// Peek A is the head: it acquires immediately and then simulates a long-running task by blocking on a channel until the test releases it
	// errA is written only by the goroutine below and read only after acquiredA is closed, so the channel close/receive establishes the happens-before relationship that makes this safe without asserting inside the goroutine
	var errA error
	unblockA := make(chan struct{})
	acquiredA := make(chan struct{})
	doneA := make(chan struct{})
	go func() {
		errA = l.RLock(t.Context())
		close(acquiredA)
		<-unblockA
		l.RUnlock()
		close(doneA)
	}()

	select {
	case <-acquiredA:
	case <-time.After(3 * time.Second):
		t.Fatal("peek A did not acquire in time")
	}
	require.NoError(t, errA)

	// Peek B arrives while peek A (the head) is still running: it must join immediately, not wait, since no writer is queued yet
	var errB error
	acquiredB := make(chan struct{})
	unblockB := make(chan struct{})
	doneB := make(chan struct{})
	go func() {
		errB = l.RLock(t.Context())
		close(acquiredB)
		<-unblockB
		l.RUnlock()
		close(doneB)
	}()

	select {
	case <-acquiredB:
	case <-time.After(3 * time.Second):
		t.Fatal("peek B should join the active peek A immediately rather than waiting")
	}
	require.NoError(t, errB)

	// An invoke arriving now must wait, since peeks A and B are both still active
	invokeErr := make(chan error)
	go func() {
		invokeErr <- l.Lock(t.Context())
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, l.QueueLength())
	}, 3*time.Second, 10*time.Millisecond, "the invoke should be parked in the queue behind the two active peeks")

	select {
	case <-invokeErr:
		t.Fatal("invoke must not acquire while peeks A and B are still active")
	case <-time.After(100 * time.Millisecond):
		// Expected: still waiting
	}

	// Peek A and B finish their long-running work (the channel unblocks, simulating the task exiting)
	close(unblockA)
	close(unblockB)

	select {
	case <-doneA:
	case <-time.After(3 * time.Second):
		t.Fatal("peek A did not release")
	}
	select {
	case <-doneB:
	case <-time.After(3 * time.Second):
		t.Fatal("peek B did not release")
	}

	// The queue continues processing: with both peeks gone, the waiting invoke is granted next
	select {
	case err := <-invokeErr:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("invoke did not acquire after both peeks released")
	}

	// The invoke is now the head: it simulates its own long-running task, blocking on a channel
	unblockInvoke := make(chan struct{})
	invokeDone := make(chan struct{})
	go func() {
		<-unblockInvoke
		l.Unlock()
		close(invokeDone)
	}()

	// Two more peeks arrive while the invoke is active: both must wait, since a writer excludes every reader
	var errC, errD error
	peekCAcquired := make(chan struct{})
	peekDAcquired := make(chan struct{})
	go func() {
		errC = l.RLock(t.Context())
		close(peekCAcquired)
	}()
	go func() {
		errD = l.RLock(t.Context())
		close(peekDAcquired)
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, l.QueueLength())
	}, 3*time.Second, 10*time.Millisecond, "peeks C and D should be parked in the queue behind the active invoke")

	select {
	case <-peekCAcquired:
		t.Fatal("peek C must not acquire while the invoke is active")
	case <-peekDAcquired:
		t.Fatal("peek D must not acquire while the invoke is active")
	case <-time.After(100 * time.Millisecond):
		// Expected: both still waiting
	}

	// The invoke finishes its long-running work (the channel unblocks): the queue continues processing by granting both waiting peeks together
	close(unblockInvoke)

	select {
	case <-invokeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("invoke did not release")
	}

	select {
	case <-peekCAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("peek C did not acquire after the invoke released")
	}
	select {
	case <-peekDAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("peek D did not acquire after the invoke released")
	}
	require.NoError(t, errC)
	require.NoError(t, errD)

	l.RUnlock()
	l.RUnlock()
}

func BenchmarkTurnBasedLocker_LockUnlock(b *testing.B) {
	locker := &TurnBasedLocker{}
	ctx := b.Context()

	for b.Loop() {
		err := locker.Lock(ctx)
		if err != nil {
			b.Fatal(err)
		}
		locker.Unlock()
	}
}

func BenchmarkTurnBasedLocker_ConcurrentLockUnlock(b *testing.B) {
	locker := &TurnBasedLocker{}
	ctx := b.Context()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := locker.Lock(ctx)
			if err != nil {
				b.Error(err)
				return
			}
			locker.Unlock()
		}
	})
}

func BenchmarkTurnBasedLocker_ConcurrentRLockRUnlock(b *testing.B) {
	locker := &TurnBasedLocker{}
	ctx := b.Context()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := locker.RLock(ctx)
			if err != nil {
				b.Error(err)
				return
			}
			locker.RUnlock()
		}
	})
}
