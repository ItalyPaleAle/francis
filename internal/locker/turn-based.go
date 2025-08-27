package locker

import (
	"context"
	"errors"
	"sync"
)

var ErrStopped = errors.New("queue is stopped")

// TurnBasedLocker manages turn-based concurrency with FIFO ordering.
type TurnBasedLocker struct {
	mu sync.Mutex
	// Queue of waiting channels
	queue []chan struct{}
	// Whether the lock is currently held
	isLocked bool
	// Whether the queue has been stopped
	stopped bool
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

	ready := make(chan struct{})
	l.queue = append(l.queue, ready)
	l.mu.Unlock()

	select {
	case <-ready:
		l.mu.Lock()
		defer l.mu.Unlock()

		if l.stopped {
			return ErrStopped
		}
		l.isLocked = true
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()

		var j int
		for i, ch := range l.queue {
			if ch != ready {
				l.queue[j] = l.queue[i]
				j++
			}
		}
		l.queue = l.queue[:j]

		return ctx.Err()
	}
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

	if !l.isLocked {
		// Not locked, nothing to do
		return
	}
	l.isLocked = false

	if len(l.queue) == 0 {
		return
	}

	next := l.queue[0]
	l.queue = l.queue[1:]

	// Next waiter now holds the lock
	l.isLocked = true

	// Signal the next waiter
	close(next)
}

// Stop cancels the queue: all waiting callers are canceled, and the stopped state is set.
// The current holder can check IsStopped() to be notified.
func (l *TurnBasedLocker) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopped = true
	for _, ch := range l.queue {
		// Cancel waiting callers
		close(ch)
	}

	// Clear the queue
	l.queue = nil
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
