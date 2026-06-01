package runtime

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/protocol"
)

// Session error codes used when the runtime closes a host's WebTransport session
const (
	// sessionErrorSuperseded is sent when a session is superseded by a newer one for the same host
	sessionErrorSuperseded webtransport.SessionErrorCode = 1
	// sessionErrorShutdown is sent when the runtime is shutting down
	sessionErrorShutdown webtransport.SessionErrorCode = 2
)

// hostConn represents a single connected host and its active WebTransport session
// One hostConn corresponds to one QUIC connection
// In case of reconnection, we create a new hostConn superseding the previous one for the same ID
type hostConn struct {
	session   *webtransport.Session
	hostID    string
	sessionID string
	address   string

	protocolVersion uint16
	actorTypes      []protocol.ActorHostType

	// draining is set when a graceful UnregisterHost is in progress
	// While draining the host must not be selected for new placement or alarm work
	draining atomic.Bool

	log *slog.Logger
}

// ID returns the stable host ID
func (c *hostConn) ID() string {
	return c.hostID
}

// SessionID returns the runtime-generated session ID used to detect superseded sessions
func (c *hostConn) SessionID() string {
	return c.sessionID
}

// IsDraining reports whether the host is gracefully draining
func (c *hostConn) IsDraining() bool {
	return c.draining.Load()
}

// setDraining marks the host as draining
func (c *hostConn) setDraining() {
	c.draining.Store(true)
}

// close terminates the host's WebTransport session with the given error code
func (c *hostConn) close(code webtransport.SessionErrorCode, msg string) {
	if c.session == nil {
		return
	}
	_ = c.session.CloseWithError(code, msg)
}

// HostManager tracks the hosts currently connected to this runtime replica, keyed by host ID
// It is the in-memory source of truth for which hosts this runtime owns sessions for
type HostManager struct {
	mu    sync.RWMutex
	hosts map[string]*hostConn
}

// NewHostManager returns an empty HostManager
func NewHostManager() *HostManager {
	return &HostManager{
		hosts: make(map[string]*hostConn),
	}
}

// Register adds a connected host, superseding any existing session for the same host ID
// If a different session already existed for the host, it is returned so the caller can close the superseded one
// The newest valid session always wins
func (m *HostManager) Register(c *hostConn) (superseded *hostConn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, ok := m.hosts[c.hostID]
	m.hosts[c.hostID] = c
	if ok && prev != nil && prev.sessionID != c.sessionID {
		return prev
	}

	return nil
}

// Remove removes a host, but only if its currently-tracked session matches sessionID
// This prevents a superseded or stale session's teardown from evicting the host's newer session
// It returns true if the host was removed
func (m *HostManager) Remove(hostID string, sessionID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.hosts[hostID]
	if !ok || cur == nil || cur.sessionID != sessionID {
		return false
	}

	delete(m.hosts, hostID)

	return true
}

// Get returns the connected host for the given host ID
func (m *HostManager) Get(hostID string) (*hostConn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.hosts[hostID]
	return c, ok
}

// ConnectedHostIDs returns the IDs of all hosts currently connected to this runtime
// Draining hosts are excluded so they are not selected for new placement or alarm work
func (m *HostManager) ConnectedHostIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	i := 0
	ids := make([]string, len(m.hosts))
	for id, c := range m.hosts {
		if c.IsDraining() {
			continue
		}
		ids[i] = id
		i++
	}
	return ids[:i]
}

// Count returns the number of connected hosts, including draining ones
func (m *HostManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.hosts)
}
