package actorcore

// capacitySemaphore is a non-blocking counting semaphore that enforces a strict concurrency limit for one capacity group on this host
// A host is a single process, so an in-memory semaphore is an exact bound on how many of the group's jobs run at once, unlike the best-effort, cluster-wide placement gate
type capacitySemaphore struct {
	// slots is a buffered channel whose capacity is the group limit: a buffered send takes a slot, a receive returns one
	slots chan struct{}
	// limit is retained so a second actor type joining the group can be validated against the declared limit
	limit int
}

// newCapacitySemaphore builds a semaphore that admits at most limit concurrent holders
func newCapacitySemaphore(limit int) *capacitySemaphore {
	return &capacitySemaphore{
		slots: make(chan struct{}, limit),
		limit: limit,
	}
}

// tryAcquire takes a slot without blocking, reporting whether one was available
func (s *capacitySemaphore) tryAcquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a previously-acquired slot
// The non-blocking receive guards against an accidental extra release, which would otherwise be a no-op rather than a panic
func (s *capacitySemaphore) release() {
	select {
	case <-s.slots:
	default:
	}
}
