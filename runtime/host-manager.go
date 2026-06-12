package runtime

import (
	"sync"
)

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
