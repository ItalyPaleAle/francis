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
	// granted is set under the locker mutex by unlockLocked/runlockLocked when it hands the lock to this waiter
	// It lets the woken waiter tell a grant apart from a Stop cancellation, which both close ready
	granted bool
	// shared marks a waiter parked on RLock rather than Lock
	// It tells grantNextLocked whether to hand off the exclusive lock or batch this waiter with the other shared waiters at the front of the queue, and tells the woken waiter which counter to release if it abandons a grant it raced with cancellation
	shared bool
}

// newWaiter returns a new waiter object
func newWaiter(shared bool) *waiter {
	return &waiter{
		ready:  make(chan struct{}),
		shared: shared,
	}
}

// TurnBasedLocker manages turn-based concurrency with FIFO ordering
// It behaves like Go's sync.RWMutex: Lock/Unlock is the exclusive (write) side, RLock/RUnlock is the shared (read) side
// A queued writer blocks later readers from joining the active readers, which prevents writer starvation
// Note: re-entrancy is not supported, on either side: a caller that already holds the lock and calls Lock/RLock again deadlocks
type TurnBasedLocker struct {
	mu sync.Mutex
	// Queue of waiting callers
	queue []*waiter
	// Whether a writer currently holds the lock
	writerActive bool
	// Number of readers currently holding the lock
	readers int
	// Whether the queue has been stopped
	stopped bool
	// Channel used for waiting for the last unlock after calling StopAndWait
	closingWaiter chan struct{}
}

// Lock attempts to acquire the exclusive (write) lock in FIFO order.
// Blocks until the lock is acquired or the context is canceled.
func (l *TurnBasedLocker) Lock(ctx context.Context) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return ErrStopped
	}

	// An exclusive lock can be granted immediately only when there are no active holders and no one is already queued
	if !l.writerActive && l.readers == 0 && len(l.queue) == 0 {
		l.writerActive = true
		l.mu.Unlock()
		return nil
	}

	w := newWaiter(false)
	l.queue = append(l.queue, w)
	l.mu.Unlock()

	return l.waitForGrant(ctx, w)
}

// RLock attempts to acquire a shared (read) lock in FIFO order.
// Multiple callers can hold the shared lock at once, but RLock still queues behind a waiting writer so writers are never starved.
// Blocks until the lock is acquired or the context is canceled.
func (l *TurnBasedLocker) RLock(ctx context.Context) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return ErrStopped
	}

	// A shared lock can be granted immediately when no writer holds or is waiting for the lock
	// Other active readers never block a new reader, but a queued writer does: joining the active readers here would let readers starve it indefinitely
	if !l.writerActive && len(l.queue) == 0 {
		l.readers++
		l.mu.Unlock()
		return nil
	}

	w := newWaiter(true)
	l.queue = append(l.queue, w)
	l.mu.Unlock()

	return l.waitForGrant(ctx, w)
}

// waitForGrant parks the caller on w.ready until the lock is granted, the queue is stopped, or ctx is canceled
// The caller must have already appended w to the queue and released l.mu before calling this
func (l *TurnBasedLocker) waitForGrant(ctx context.Context, w *waiter) error {
	select {
	case <-w.ready:
		l.mu.Lock()
		defer l.mu.Unlock()

		// A closed ready channel means either a grant handed us the lock or Stop canceled us
		// granted disambiguates the two, since Stop closes the channel without granting
		if !w.granted {
			return ErrStopped
		}

		// We own our slice of the lock now, but if the locker was stopped after the grant we release it and report stopped so it is not leaked
		if l.stopped {
			l.releaseGrantedLocked(w)
			return ErrStopped
		}

		return nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()

		// A grant may have raced our cancellation and already handed us the lock
		// We are abandoning the call, so we must release our slice of the lock or it would stay held forever with no owner
		if w.granted {
			l.releaseGrantedLocked(w)
			return ctx.Err()
		}

		// Otherwise we still hold a slot in the queue, so remove it before a later release tries to hand us the lock
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

// releaseGrantedLocked releases a hold that was just granted to w but is being abandoned by the caller, because a Stop or context cancellation raced the grant
// The caller must hold l.mu
func (l *TurnBasedLocker) releaseGrantedLocked(w *waiter) {
	if w.shared {
		l.runlockLocked()
		return
	}

	l.unlockLocked()
}

// TryLock attempts to acquire the exclusive lock immediately if there are no active holders and the queue isn't stopped.
// An actor with active RLock holders is correctly reported as busy, since it has no way to safely interrupt an in-flight Peek.
// It returns true if the lock was acquired, false if it's already held (exclusively or by any reader), and an error if the queue is stopped.
func (l *TurnBasedLocker) TryLock() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return false, ErrStopped
	}

	if !l.writerActive && l.readers == 0 {
		// Lock acquired successfully
		l.writerActive = true
		return true, nil
	}

	// Lock is already held
	return false, nil
}

// Unlock releases the exclusive (write) lock, allowing the next waiter(s) to acquire it.
func (l *TurnBasedLocker) Unlock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.unlockLocked()
}

// unlockLocked releases the exclusive lock and hands it off, if there is anyone waiting.
// The caller must hold l.mu.
func (l *TurnBasedLocker) unlockLocked() {
	if !l.writerActive {
		// Not locked, nothing to do
		return
	}
	l.writerActive = false

	l.afterReleaseLocked()
}

// RUnlock releases one shared (read) hold, handing the lock off once the last reader leaves.
func (l *TurnBasedLocker) RUnlock() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.runlockLocked()
}

// runlockLocked releases one shared hold and, once the last reader leaves, hands the lock off, if there is anyone waiting.
// The caller must hold l.mu.
func (l *TurnBasedLocker) runlockLocked() {
	if l.readers == 0 {
		// Not locked, nothing to do
		return
	}
	l.readers--

	if l.readers > 0 {
		// Other readers still hold the lock, so there is nothing to hand off yet
		return
	}

	l.afterReleaseLocked()
}

// afterReleaseLocked runs once the last active holder (the writer, or the last reader) leaves
// It wakes a pending StopAndWait first, since a stopped queue has no waiters left to grant the lock to
// The caller must hold l.mu
func (l *TurnBasedLocker) afterReleaseLocked() {
	if l.closingWaiter != nil {
		close(l.closingWaiter)
		return
	}

	l.grantNextLocked()
}

// grantNextLocked hands the lock to the next waiter(s) at the front of the queue, if any
// A writer at the front is granted alone
// One or more readers at the front are granted together in a single batch, stopping at the next writer so it still gets its turn once those readers release
// The caller must hold l.mu
func (l *TurnBasedLocker) grantNextLocked() {
	if len(l.queue) == 0 {
		return
	}

	// A writer at the front must run alone, so grant it and stop
	next := l.queue[0]
	if !next.shared {
		l.writerActive = true
		next.granted = true
		close(next.ready)

		l.queue = l.queue[1:]
		return
	}

	// Grant every consecutive reader at the front of the queue in one batch, so they all run concurrently, stopping at the next writer (if any)
	var i int
	for i = 0; i < len(l.queue) && l.queue[i].shared; i++ {
		l.readers++
		l.queue[i].granted = true
		close(l.queue[i].ready)
	}

	l.queue = l.queue[i:]
}

// Stop cancels the queue: all waiting callers are canceled, and the stopped state is set.
// The current holder(s) can check IsStopped() to be notified.
func (l *TurnBasedLocker) Stop() {
	l.doStop(false)
}

// StopAndWait cancels the queue like Stop.
// If anyone is holding the lock on the locker, exclusively or as a reader, it blocks until every holder has released it.
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

	// If we're not waiting, or if no one currently holds the lock (exclusively or as a reader), we're done
	if !wait || (!l.writerActive && l.readers == 0) {
		// Unlock and return
		l.mu.Unlock()
		return
	}

	// Otherwise, set the closingWaiter channel that can be used to allow others to wait for the last holder to release the lock
	// Note that closingWaiter could be non-nil if someone else is waiting
	if l.closingWaiter == nil {
		l.closingWaiter = make(chan struct{})
	}

	// Unlock so the holder(s) can still call Unlock/RUnlock while we wait
	l.mu.Unlock()

	// Wait for closingWaiter to be closed, which happens once the last holder releases
	<-l.closingWaiter
}

// IsStopped returns whether the queue has been stopped.
// The current lock holder should check this after acquiring the lock.
func (l *TurnBasedLocker) IsStopped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.stopped
}

// IsLocked returns true if the lock is currently held, exclusively or by one or more readers.
func (l *TurnBasedLocker) IsLocked() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.writerActive || l.readers > 0
}

// QueueLength returns the length of the queue.
func (l *TurnBasedLocker) QueueLength() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.queue)
}
