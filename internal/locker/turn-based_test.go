package locker

import (
	"context"
	"sync"
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
		err := locker.Lock(context.Background())
		require.NoError(t, err)

		// Second goroutine waits in queue
		go func() {
			_ = locker.Lock(context.Background())
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
		lockHeld := make(chan struct{})
		unlockSignal := make(chan struct{})
		lockReleased := make(chan struct{})

		go func() {
			err := locker.Lock(t.Context())
			require.NoError(t, err)
			close(lockHeld)

			// Wait for signal to unlock
			<-unlockSignal
			locker.Unlock()
			close(lockReleased)
		}()

		// Wait for the lock to be acquired
		<-lockHeld

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
		lockHeld := make(chan struct{})
		unlockSignal := make(chan struct{})

		go func() {
			err := locker.Lock(t.Context())
			require.NoError(t, err)
			close(lockHeld)

			// Wait for signal to unlock
			<-unlockSignal
			locker.Unlock()
		}()

		// Wait for the lock to be acquired
		<-lockHeld

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

func BenchmarkTurnBasedLocker_LockUnlock(b *testing.B) {
	locker := &TurnBasedLocker{}
	ctx := b.Context()

	b.ResetTimer()
	for range b.N {
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
