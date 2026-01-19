package standalone

import (
	"context"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *provider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	changes := newChanges()

	// Clean up unhealthy hosts first
	p.cleanupUnhealthyHostsWithChanges(changes)

	// Check if there's already a healthy host at this address
	existingHostID, ok := p.hostsByAddress[req.Address]
	if ok {
		existingHost, ok := p.hosts[existingHostID]
		if ok && p.isHostHealthy(existingHost) {
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
	now := p.clock.Now()
	h := &host{
		id:              hostID,
		address:         req.Address,
		lastHealthCheck: now,
	}

	p.hosts[hostID] = h
	p.hostsByAddress[req.Address] = hostID
	changes.Hosts.Create = append(changes.Hosts.Create, h)

	// Add actor types
	if len(req.ActorTypes) > 0 {
		p.hostActorTypes[hostID] = make([]*hostActorType, len(req.ActorTypes))
		for i, at := range req.ActorTypes {
			hat := &hostActorType{
				hostID:           hostID,
				actorType:        at.ActorType,
				idleTimeout:      at.IdleTimeout,
				concurrencyLimit: at.ConcurrencyLimit,
			}
			p.hostActorTypes[hostID][i] = hat
			changes.HostActorTypes.Create = append(changes.HostActorTypes.Create, hat)
		}
	}

	// Persist changes
	err = p.persistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback in-memory changes
		delete(p.hosts, hostID)
		delete(p.hostsByAddress, req.Address)
		delete(p.hostActorTypes, hostID)
		return components.RegisterHostRes{}, err
	}

	return components.RegisterHostRes{HostID: hostID}, nil
}

func (p *provider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// Nothing to update
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	h, ok := p.hosts[hostID]
	if !ok || !p.isHostHealthy(h) {
		return components.ErrHostUnregistered
	}

	changes := newChanges()

	// Update last health check if requested
	if req.UpdateLastHealthCheck {
		h.lastHealthCheck = p.clock.Now()
		changes.Hosts.Update[hostID] = h
	}

	// Update actor types if provided (non-nil)
	if req.ActorTypes != nil {
		// Record deletions for existing actor types
		for _, hat := range p.hostActorTypes[hostID] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, hostActorTypeKey{
				hostID:    hat.hostID,
				actorType: hat.actorType,
			})
		}

		// Clear existing actor types and create new ones
		if cap(p.hostActorTypes[hostID]) >= len(req.ActorTypes) {
			clear(p.hostActorTypes[hostID])
			p.hostActorTypes[hostID] = p.hostActorTypes[hostID][:len(req.ActorTypes)]
		} else {
			p.hostActorTypes[hostID] = make([]*hostActorType, len(req.ActorTypes))
		}

		// Add new actor types
		for i, at := range req.ActorTypes {
			hat := &hostActorType{
				hostID:           hostID,
				actorType:        at.ActorType,
				idleTimeout:      at.IdleTimeout,
				concurrencyLimit: at.ConcurrencyLimit,
			}
			p.hostActorTypes[hostID][i] = hat
			changes.HostActorTypes.Create = append(changes.HostActorTypes.Create, hat)
		}
	}

	// Persist changes
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}

func (p *provider) UnregisterHost(ctx context.Context, hostID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	h, ok := p.hosts[hostID]
	if !ok {
		return components.ErrHostUnregistered
	}

	// Check if host was healthy
	wasHealthy := p.isHostHealthy(h)

	changes := newChanges()

	// Remove the host and all associated data
	delete(p.hostsByAddress, h.address)
	delete(p.hosts, hostID)
	changes.Hosts.Delete = append(changes.Hosts.Delete, hostID)

	// Remove host actor types
	for _, hat := range p.hostActorTypes[hostID] {
		changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, hostActorTypeKey{
			hostID:    hat.hostID,
			actorType: hat.actorType,
		})
	}
	delete(p.hostActorTypes, hostID)

	// Remove active actors on this host
	for key, actor := range p.activeActors {
		if actor.hostID != hostID {
			continue
		}

		delete(p.activeActors, key)
		changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
	}

	// Persist changes
	err := p.persistHook.PersistChanges(ctx, changes)
	if err != nil {
		return err
	}

	// If host was unhealthy, return error (we still cleaned it up)
	if !wasHealthy {
		return components.ErrHostUnregistered
	}

	return nil
}

func (p *provider) LookupActor(ctx context.Context, r ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	// If we only want actors that are already active, use a simpler path
	if opts.ActiveOnly {
		return p.lookupActiveActor(r, opts.Hosts)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if actor is already active on a healthy host
	actor, ok := p.activeActors[key]
	if ok {
		h, ok := p.hosts[actor.hostID]
		if ok && p.isHostHealthy(h) {
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
	host, idleTimeout := p.findHostWithCapacity(r.ActorType, opts.Hosts)
	if host == nil {
		return components.LookupActorRes{}, components.ErrNoHost
	}

	// Activate the actor on the selected host
	newActor := &activeActor{
		actorType:   r.ActorType,
		actorID:     r.ActorID,
		hostID:      host.id,
		idleTimeout: idleTimeout,
		activation:  p.clock.Now(),
	}
	p.activeActors[key] = newActor

	// Persist changes
	changes := newChanges()
	changes.ActiveActors.Create = append(changes.ActiveActors.Create, newActor)
	err := p.persistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		delete(p.activeActors, key)
		return components.LookupActorRes{}, err
	}

	return components.LookupActorRes{
		HostID:      host.id,
		Address:     host.address,
		IdleTimeout: idleTimeout,
	}, nil
}

func (p *provider) lookupActiveActor(r ref.ActorRef, hosts []string) (components.LookupActorRes, error) {
	key := newActorKey(r.ActorType, r.ActorID)

	p.mu.RLock()
	defer p.mu.RUnlock()

	actor, ok := p.activeActors[key]
	if !ok {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	h, ok := p.hosts[actor.hostID]
	if !ok || !p.isHostHealthy(h) {
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
func (p *provider) findHostWithCapacity(actorType string, allowedHosts []string) (*host, time.Duration) {
	type candidate struct {
		host        *host
		idleTimeout time.Duration
	}

	candidates := make([]candidate, 0, len(p.hostActorTypes))

	for hostID, actorTypes := range p.hostActorTypes {
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

		h, ok := p.hosts[hostID]
		if !ok || !p.isHostHealthy(h) {
			continue
		}

		// Check if this host is in the allowed list (if specified)
		if len(allowedHosts) > 0 && !slices.Contains(allowedHosts, hostID) {
			continue
		}

		// Check capacity
		if hat.concurrencyLimit > 0 {
			currentCount := p.countActiveActorsOnHost(hostID, actorType)
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

func (p *provider) RemoveActor(ctx context.Context, r ref.ActorRef) error {
	key := newActorKey(r.ActorType, r.ActorID)

	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.activeActors[key]
	if !ok {
		return components.ErrNoActor
	}

	changes := newChanges()

	delete(p.activeActors, key)
	changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)

	// Release any alarm leases for this actor
	for _, a := range p.alarms {
		if a.actorType != r.ActorType || a.actorID != r.ActorID {
			continue
		}

		if a.leaseID != nil {
			a.leaseID = nil
			a.leaseExpiration = nil
			changes.Alarms.Update[a.id] = a
		}
	}

	// Persist changes
	err := p.persistHook.PersistChanges(ctx, changes)
	if err != nil {
		return err
	}

	return nil
}
