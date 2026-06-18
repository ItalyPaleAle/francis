package locker

import (
	"context"
	"errors"
	"sync"
)

var ErrStopped = errors.New("queue is stopped")

// waiter represents a caller parked in the queue waiting to acquire the lock
type waiter struct {
	// ready is closed to wake the waiter, both when the lock is granted and when the waiter is canceled by Stop
	ready chan struct{}
	// granted is set under the locker mutex by Unlock when it hands the lock to this waiter
	// It lets the woken waiter tell a grant apart from a Stop cancellation, which both close ready
	granted bool
}

// TurnBasedLocker manages turn-based concurrency with FIFO ordering.
type TurnBasedLocker struct {
	mu sync.Mutex
	// Queue of waiting callers
	queue []*waiter
	// Whether the lock is currently held
	isLocked bool
	// Whether the queue has been stopped
	stopped bool
	// Channel used for waiting for the last unlock after calling StopAndWait
	closingWaiter chan struct{}
}

// Lock attempts to acquire the lock in FIFO order.
// Blocks until the lock is acquired or the context is canceled.
func (l *TurnBasedLocker) Lock(ctx context.Context) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return ErrStopped
	}

	if !l.isLocked {
		l.isLocked = true
		l.mu.Unlock()
		return nil
	}

	w := &waiter{ready: make(chan struct{})}
	l.queue = append(l.queue, w)
	l.mu.Unlock()

	select {
	case <-w.ready:
		l.mu.Lock()
		defer l.mu.Unlock()

		// A closed ready channel means either Unlock handed us the lock or Stop canceled us
		// granted disambiguates the two, since Stop closes the channel without granting
		if !w.granted {
			return ErrStopped
		}

		// We own the lock now (Unlock already set isLocked), but if the locker was stopped after the grant we release it and report stopped so it is not leaked
		if l.stopped {
			l.unlockLocked()
			return ErrStopped
		}

		return nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()

		// Unlock may have raced our cancellation and already handed us the lock by closing ready and setting granted
		// We are abandoning the call, so we must release the lock or it would stay held forever with no owner
		if w.granted {
			l.unlockLocked()
			return ctx.Err()
		}

		// Otherwise we still hold a slot in the queue, so remove it before a later Unlock tries to hand us the lock
		l.removeWaiter(w)

		return ctx.Err()
	}
}

// removeWaiter drops w from the queue if it is still present
// The caller must hold l.mu
func (l *TurnBasedLocker) removeWaiter(w *waiter) {
	var j int
	for i := range l.queue {
		if l.queue[i] != w {
			l.queue[j] = l.queue[i]
			j++
		}
	}
	l.queue = l.queue[:j]
}

// TryLock attempts to acquire the lock immediately if it's available and the queue isn't stopped.
// It returns true if the lock was acquired, false if it's already locked, and an error if the queue is stopped.
func (l *TurnBasedLocker) TryLock() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return false, ErrStopped
	}

	if !l.isLocked {
		// Lock acquired successfully
		l.isLocked = true
		return true, nil
	}

	// Lock is already held
	return false, nil
}

// Unlock releases the lock, allowing the next waiter to acquire it.
func (l *TurnBasedLocker) Unlock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.unlockLocked()
}

// unlockLocked releases the lock and hands it to the next waiter, if any.
// The caller must hold l.mu.
func (l *TurnBasedLocker) unlockLocked() {
	if !l.isLocked {
		// Not locked, nothing to do
		return
	}
	l.isLocked = false

	// If there's closing waiter, close it and return
	if l.closingWaiter != nil {
		close(l.closingWaiter)
		return
	}

	if len(l.queue) == 0 {
		return
	}

	next := l.queue[0]
	l.queue = l.queue[1:]

	// Next waiter now holds the lock
	l.isLocked = true

	// Mark the grant so the woken waiter does not mistake it for a Stop cancellation, then signal it
	next.granted = true
	close(next.ready)
}

// Stop cancels the queue: all waiting callers are canceled, and the stopped state is set.
// The current holder can check IsStopped() to be notified.
func (l *TurnBasedLocker) Stop() {
	l.doStop(false)
}

// StopAndWait cancels the queue like Stop.
// If anyone is holding a lock on the locker, it blocks until the lock is released
func (l *TurnBasedLocker) StopAndWait() {
	l.doStop(true)
}

func (l *TurnBasedLocker) doStop(wait bool) {
	l.mu.Lock()

	l.stopped = true

	// Close all waiters and clear the queue
	// We close ready without setting granted, so each woken waiter reports ErrStopped rather than taking the lock
	for _, w := range l.queue {
		// Cancel waiting callers
		close(w.ready)
	}
	l.queue = nil

	// If we're not waiting, or if the locker is currently unlocked, we're done
	if !wait || !l.isLocked {
		// Unlock and return
		l.mu.Unlock()
		return
	}

	// Otherwise, set the closingWaiter channel that can be used to allow others to wait for the last owner of the lock to unlock
	// Note that closingWaiter could be non-nil if someone else is waiting
	if l.closingWaiter == nil {
		l.closingWaiter = make(chan struct{})
	}

	// Unlock the lock so the other call to the Unlock method of the locker can continue
	l.mu.Unlock()

	// Wait for closingWaiter to be closed, which happens on the unlock
	<-l.closingWaiter
}

// IsStopped returns whether the queue has been stopped.
// The current lock holder should check this after acquiring the lock.
func (l *TurnBasedLocker) IsStopped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.stopped
}

// IsLocked returns true if the lock is currently being held.
func (l *TurnBasedLocker) IsLocked() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.isLocked
}

// QueueLength returns the length of the queue.
func (l *TurnBasedLocker) QueueLength() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.queue)
}
