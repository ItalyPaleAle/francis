package actorcore

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrNotRunning is returned by AdminDrain.Accept when the host is not running, so there is nothing to drain
var ErrNotRunning = errors.New("host is not running")

// AdminDrain tracks an administrative drain of a host, and is shared by the local and remote hosts
// A drain is accepted at most once and never cleared, since a drained host cannot run again
type AdminDrain struct {
	mu sync.Mutex
	// running is set while the host's Run is in progress
	running bool
	// stopping is set once the host began stopping in the current run, for a drain or a shutdown
	stopping bool
	// accepted is set once a drain is accepted, and never cleared
	accepted bool
	// triggered is set once the accepted drain started stopping the host
	triggered bool
	// timeout bounds how long the accepted drain waits for actors to halt, zero meaning no bound
	timeout time.Duration
	// stop stops the host, or the part of it that leads into the graceful teardown
	stop context.CancelFunc
}

// Start records that the host started running
// It returns false, recording nothing, when a drain was accepted, since a drained host cannot run again
func (d *AdminDrain) Start() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.accepted {
		return false
	}

	d.running = true
	d.stopping = false
	d.stop = nil
	return true
}

// Stopped records that the host's Run returned
func (d *AdminDrain) Stopped() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.running = false
	d.stop = nil
}

// SetStop sets the function that stops the host when a drain is triggered
// A drain that was already triggered calls it right away, so a stop function registered late still takes effect
func (d *AdminDrain) SetStop(stop context.CancelFunc) {
	d.mu.Lock()
	d.stop = stop
	call := d.triggered && stop != nil
	d.mu.Unlock()

	if call {
		stop()
	}
}

// Accept records a drain bounded by timeout, where zero means no bound
// It reports already when the host was draining or stopping, in which case the request has no further effect
// It returns ErrNotRunning when the host is not running
func (d *AdminDrain) Accept(timeout time.Duration) (already bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return false, ErrNotRunning
	}
	if d.accepted || d.stopping {
		return true, nil
	}

	d.accepted = true
	d.timeout = max(timeout, 0)
	return false, nil
}

// Trigger stops the host after an accepted drain, and is a no-op when no drain was accepted
// Callers trigger the drain once they have done what must happen before the host stops, such as acknowledging the request
func (d *AdminDrain) Trigger() {
	d.mu.Lock()
	if !d.accepted || d.triggered {
		d.mu.Unlock()
		return
	}

	d.triggered = true
	stop := d.stop
	d.mu.Unlock()

	if stop != nil {
		stop()
	}
}

// BeginStopping records that the host began stopping, so a later drain request is reported as already draining
// It returns whether a drain was accepted, and the timeout that bounds halting the host's actors, which is zero for a shutdown
func (d *AdminDrain) BeginStopping() (accepted bool, timeout time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stopping = true
	if !d.accepted {
		return false, 0
	}

	return true, d.timeout
}

// Accepted reports whether a drain was accepted
func (d *AdminDrain) Accepted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.accepted
}
