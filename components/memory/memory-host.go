package memory

import (
	"context"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (m *MemoryProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up unhealthy hosts first
	m.cleanupUnhealthyHosts()

	// Check if there's already a healthy host at this address
	existingHostID, ok := m.hostsByAddress[req.Address]
	if ok {
		existingHost, ok := m.hosts[existingHostID]
		if ok && m.isHostHealthy(existingHost) {
			return components.RegisterHostRes{}, components.ErrHostAlreadyRegistered
		}
	}

	// Generate a new host ID
	hostIDObj, err := uuid.NewV7()
	if err != nil {
		return components.RegisterHostRes{}, err
	}
	hostID := hostIDObj.String()

	// Create the host
	now := m.clock.Now()
	h := &host{
		id:              hostID,
		address:         req.Address,
		lastHealthCheck: now,
	}

	m.hosts[hostID] = h
	m.hostsByAddress[req.Address] = hostID

	// Add actor types
	if len(req.ActorTypes) > 0 {
		m.hostActorTypes[hostID] = make([]*hostActorType, len(req.ActorTypes))
		for i, at := range req.ActorTypes {
			m.hostActorTypes[hostID][i] = &hostActorType{
				hostID:           hostID,
				actorType:        at.ActorType,
				idleTimeout:      at.IdleTimeout,
				concurrencyLimit: at.ConcurrencyLimit,
			}
		}
	}

	return components.RegisterHostRes{HostID: hostID}, nil
}

func (m *MemoryProvider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// Nothing to update
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	h, ok := m.hosts[hostID]
	if !ok || !m.isHostHealthy(h) {
		return components.ErrHostUnregistered
	}

	// Update last health check if requested
	if req.UpdateLastHealthCheck {
		h.lastHealthCheck = m.clock.Now()
	}

	// Update actor types if provided (non-nil)
	if req.ActorTypes != nil {
		// Clear existing actor types
		// Try to preserve the allocated memory if the slice has capacity
		if cap(m.hostActorTypes[hostID]) >= len(req.ActorTypes) {
			clear(m.hostActorTypes[hostID])
			m.hostActorTypes[hostID] = m.hostActorTypes[hostID][:len(req.ActorTypes)]
		} else {
			// Allocate a new slice
			m.hostActorTypes[hostID] = make([]*hostActorType, len(req.ActorTypes))
		}

		// Add new actor types
		for i, at := range req.ActorTypes {
			m.hostActorTypes[hostID][i] = &hostActorType{
				hostID:           hostID,
				actorType:        at.ActorType,
				idleTimeout:      at.IdleTimeout,
				concurrencyLimit: at.ConcurrencyLimit,
			}
		}
	}

	return nil
}

func (m *MemoryProvider) UnregisterHost(ctx context.Context, hostID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	h, ok := m.hosts[hostID]
	if !ok {
		return components.ErrHostUnregistered
	}

	// Check if host was healthy
	wasHealthy := m.isHostHealthy(h)

	// Remove the host and all associated data
	delete(m.hostsByAddress, h.address)
	delete(m.hosts, hostID)
	delete(m.hostActorTypes, hostID)

	// Remove active actors on this host
	for key, actor := range m.activeActors {
		if actor.hostID != hostID {
			continue
		}

		delete(m.activeActors, key)
	}

	// If host was unhealthy, return error (we still cleaned it up)
	if !wasHealthy {
		return components.ErrHostUnregistered
	}

	return nil
}

func (m *MemoryProvider) LookupActor(ctx context.Context, r ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	// If we only want actors that are already active, use a simpler path
	if opts.ActiveOnly {
		return m.lookupActiveActor(r, opts.Hosts)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if actor is already active on a healthy host
	actor, ok := m.activeActors[key]
	if ok {
		h, ok := m.hosts[actor.hostID]
		if ok && m.isHostHealthy(h) {
			// If host restrictions are set, check if the current host is allowed
			if len(opts.Hosts) > 0 && !slices.Contains(opts.Hosts, actor.hostID) {
				return components.LookupActorRes{}, components.ErrNoHost
			}

			return components.LookupActorRes{
				HostID:      actor.hostID,
				Address:     h.address,
				IdleTimeout: actor.idleTimeout,
			}, nil
		}
	}

	// Actor is not active on a healthy host; find a host with capacity
	host, idleTimeout := m.findHostWithCapacity(r.ActorType, opts.Hosts)
	if host == nil {
		return components.LookupActorRes{}, components.ErrNoHost
	}

	// Activate the actor on the selected host
	m.activeActors[key] = &activeActor{
		actorType:   r.ActorType,
		actorID:     r.ActorID,
		hostID:      host.id,
		idleTimeout: idleTimeout,
		activation:  m.clock.Now(),
	}

	return components.LookupActorRes{
		HostID:      host.id,
		Address:     host.address,
		IdleTimeout: idleTimeout,
	}, nil
}

func (m *MemoryProvider) lookupActiveActor(r ref.ActorRef, hosts []string) (components.LookupActorRes, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	m.mu.RLock()
	defer m.mu.RUnlock()

	actor, ok := m.activeActors[key]
	if !ok {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	h, ok := m.hosts[actor.hostID]
	if !ok || !m.isHostHealthy(h) {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	// Check host restrictions
	if len(hosts) > 0 && !slices.Contains(hosts, actor.hostID) {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	return components.LookupActorRes{
		HostID:      actor.hostID,
		Address:     h.address,
		IdleTimeout: actor.idleTimeout,
	}, nil
}

// findHostWithCapacity finds a healthy host that can host an actor of the given type.
// Must be called while holding the write lock.
func (m *MemoryProvider) findHostWithCapacity(actorType string, allowedHosts []string) (*host, time.Duration) {
	type candidate struct {
		host        *host
		idleTimeout time.Duration
	}

	candidates := make([]candidate, 0, len(m.hostActorTypes))

	for hostID, actorTypes := range m.hostActorTypes {
		// Check if this host supports the actor type
		var hat *hostActorType
		for _, at := range actorTypes {
			if at.actorType == actorType {
				hat = at
				break
			}
		}

		if hat == nil {
			continue
		}

		h, ok := m.hosts[hostID]
		if !ok || !m.isHostHealthy(h) {
			continue
		}

		// Check if this host is in the allowed list (if specified)
		if len(allowedHosts) > 0 && !slices.Contains(allowedHosts, hostID) {
			continue
		}

		// Check capacity
		if hat.concurrencyLimit > 0 {
			currentCount := m.countActiveActorsOnHost(hostID, actorType)
			if currentCount >= int(hat.concurrencyLimit) {
				continue
			}
		}

		candidates = append(candidates, candidate{host: h, idleTimeout: hat.idleTimeout})
	}

	if len(candidates) == 0 {
		return nil, 0
	}

	// Pick a random host
	// #nosec G404
	idx := rand.IntN(len(candidates))
	return candidates[idx].host, candidates[idx].idleTimeout
}

func (m *MemoryProvider) RemoveActor(ctx context.Context, r ref.ActorRef) error {
	key := newActorKey(r.ActorType, r.ActorID)

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.activeActors[key]
	if !ok {
		return components.ErrNoActor
	}

	delete(m.activeActors, key)

	// Release any alarm leases for this actor
	for _, a := range m.alarms {
		if a.actorType != r.ActorType || a.actorID != r.ActorID {
			continue
		}

		a.leaseID = nil
		a.leaseExpiration = nil
	}

	return nil
}
