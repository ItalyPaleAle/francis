package internal

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// Clean up unhealthy hosts first
	cleanupRollback := p.CleanupUnhealthyHosts(changes)

	// Check if there's already a healthy host at this address
	existingHostID, ok := p.HostsByAddress[req.Address]
	if ok {
		existingHost, ok := p.Hosts[existingHostID]
		if ok && p.IsHostHealthy(existingHost) {
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
	now := p.Clock.Now()
	h := &Host{
		ID:              hostID,
		Address:         req.Address,
		LastHealthCheck: now,
	}

	p.Hosts[hostID] = h
	p.HostsByAddress[req.Address] = hostID
	changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: h})

	// Add actor types
	if len(req.ActorTypes) > 0 {
		p.HostActorTypes[hostID] = make([]*HostActorType, len(req.ActorTypes))
		for i, at := range req.ActorTypes {
			hat := &HostActorType{
				HostID:           hostID,
				ActorType:        at.ActorType,
				IdleTimeout:      at.IdleTimeout,
				ConcurrencyLimit: at.ConcurrencyLimit,
			}
			p.HostActorTypes[hostID][i] = hat
			changes.HostActorTypes.Set = append(changes.HostActorTypes.Set, hat)
		}
	}

	// Persist changes
	err = p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback in-memory changes for the new host
		delete(p.Hosts, hostID)
		delete(p.HostsByAddress, req.Address)
		delete(p.HostActorTypes, hostID)

		// Rollback cleanup changes
		cleanupRollback()

		return components.RegisterHostRes{}, fmt.Errorf("error persisting changes: %w", err)
	}

	return components.RegisterHostRes{HostID: hostID}, nil
}

func (p *Provider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// Nothing to update
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		return nil
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	h, ok := p.Hosts[hostID]
	if !ok || !p.IsHostHealthy(h) {
		return components.ErrHostUnregistered
	}

	changes := NewChanges()
	defer changes.Release()

	// Update last health check if requested
	var rollbackHealthCheck time.Time
	if req.UpdateLastHealthCheck {
		rollbackHealthCheck = h.LastHealthCheck
		h.LastHealthCheck = p.Clock.Now()
		changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: h})
	}

	// Update actor types if provided (non-nil)
	var rollbackHat []*HostActorType
	if req.ActorTypes != nil {
		// Record deletions for existing actor types
		for _, hat := range p.HostActorTypes[hostID] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}

		// Add new actor types
		rollbackHat = p.HostActorTypes[hostID]
		p.HostActorTypes[hostID] = make([]*HostActorType, len(req.ActorTypes))
		for i, at := range req.ActorTypes {
			hat := &HostActorType{
				HostID:           hostID,
				ActorType:        at.ActorType,
				IdleTimeout:      at.IdleTimeout,
				ConcurrencyLimit: at.ConcurrencyLimit,
			}
			p.HostActorTypes[hostID][i] = hat
			changes.HostActorTypes.Set = append(changes.HostActorTypes.Set, hat)
		}
	}

	// Persist changes
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		h.LastHealthCheck = rollbackHealthCheck
		p.HostActorTypes[hostID] = rollbackHat

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) UnregisterHost(ctx context.Context, hostID string) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	h, ok := p.Hosts[hostID]
	if !ok {
		return components.ErrHostUnregistered
	}

	// Check if host was healthy
	wasHealthy := p.IsHostHealthy(h)

	changes := NewChanges()
	defer changes.Release()

	// Track deleted data for rollback
	deletedHostActorTypes := p.HostActorTypes[hostID]
	deletedActiveActors := make(map[ActorKey]*ActiveActor)

	// Remove the host and all associated data
	delete(p.HostsByAddress, h.Address)
	delete(p.Hosts, hostID)
	changes.Hosts.Delete = append(changes.Hosts.Delete, hostID)

	// Remove host actor types
	for _, hat := range p.HostActorTypes[hostID] {
		changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
			HostID:    hat.HostID,
			ActorType: hat.ActorType,
		})
	}
	delete(p.HostActorTypes, hostID)

	// Remove active actors on this host
	for key, actor := range p.ActiveActors {
		if actor.HostID != hostID {
			continue
		}

		deletedActiveActors[key] = actor
		delete(p.ActiveActors, key)
		changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
	}

	// Persist changes
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		p.Hosts[hostID] = h
		p.HostsByAddress[h.Address] = hostID
		p.HostActorTypes[hostID] = deletedHostActorTypes
		for key, actor := range deletedActiveActors {
			p.ActiveActors[key] = actor
		}

		return fmt.Errorf("error persisting changes: %w", err)
	}

	// If host was unhealthy, return error (we still cleaned it up)
	if !wasHealthy {
		return components.ErrHostUnregistered
	}

	return nil
}

func (p *Provider) LookupActor(ctx context.Context, r ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	key := NewActorKey(r.ActorType, r.ActorID)

	// If we only want actors that are already active, use a simpler path
	if opts.ActiveOnly {
		return p.lookupActiveActor(r, opts.Hosts)
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	// Check if actor is already active on a healthy host
	actor, ok := p.ActiveActors[key]
	if ok {
		h, ok := p.Hosts[actor.HostID]
		if ok && p.IsHostHealthy(h) {
			// If host restrictions are set, check if the current host is allowed
			if len(opts.Hosts) > 0 && !slices.Contains(opts.Hosts, actor.HostID) {
				return components.LookupActorRes{}, components.ErrNoHost
			}

			return components.LookupActorRes{
				HostID:      actor.HostID,
				Address:     h.Address,
				IdleTimeout: actor.IdleTimeout,
			}, nil
		}
	}

	// Actor is not active on a healthy host; find a host with capacity
	host, idleTimeout := p.findHostWithCapacity(r.ActorType, opts.Hosts)
	if host == nil {
		return components.LookupActorRes{}, components.ErrNoHost
	}

	// Activate the actor on the selected host
	newActor := &ActiveActor{
		ActorType:   r.ActorType,
		ActorID:     r.ActorID,
		HostID:      host.ID,
		IdleTimeout: idleTimeout,
		Activation:  p.Clock.Now(),
	}
	p.ActiveActors[key] = newActor

	// Persist changes
	changes := NewChanges()
	defer changes.Release()
	changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: key, Value: newActor})
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		delete(p.ActiveActors, key)

		return components.LookupActorRes{}, fmt.Errorf("error persisting changes: %w", err)
	}

	return components.LookupActorRes{
		HostID:      host.ID,
		Address:     host.Address,
		IdleTimeout: idleTimeout,
	}, nil
}

func (p *Provider) lookupActiveActor(r ref.ActorRef, hosts []string) (components.LookupActorRes, error) {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	actor, ok := p.ActiveActors[key]
	if !ok {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	h, ok := p.Hosts[actor.HostID]
	if !ok || !p.IsHostHealthy(h) {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	// Check host restrictions
	if len(hosts) > 0 && !slices.Contains(hosts, actor.HostID) {
		return components.LookupActorRes{}, components.ErrNoActor
	}

	return components.LookupActorRes{
		HostID:      actor.HostID,
		Address:     h.Address,
		IdleTimeout: actor.IdleTimeout,
	}, nil
}

// findHostWithCapacity finds a healthy host that can host an actor of the given type.
// Must be called while holding the write lock.
func (p *Provider) findHostWithCapacity(actorType string, allowedHosts []string) (*Host, time.Duration) {
	type candidate struct {
		host        *Host
		idleTimeout time.Duration
	}

	candidates := make([]candidate, 0, len(p.HostActorTypes))

	for hostID, actorTypes := range p.HostActorTypes {
		// Check if this host supports the actor type
		var hat *HostActorType
		for _, at := range actorTypes {
			if at.ActorType == actorType {
				hat = at
				break
			}
		}

		if hat == nil {
			continue
		}

		h, ok := p.Hosts[hostID]
		if !ok || !p.IsHostHealthy(h) {
			continue
		}

		// Check if this host is in the allowed list (if specified)
		if len(allowedHosts) > 0 && !slices.Contains(allowedHosts, hostID) {
			continue
		}

		// Check capacity
		if hat.ConcurrencyLimit > 0 {
			currentCount := p.CountActiveActorsOnHost(hostID, actorType)
			if currentCount >= int(hat.ConcurrencyLimit) {
				continue
			}
		}

		candidates = append(candidates, candidate{host: h, idleTimeout: hat.IdleTimeout})
	}

	if len(candidates) == 0 {
		return nil, 0
	}

	// Pick a random host
	// #nosec G404
	idx := rand.IntN(len(candidates))
	return candidates[idx].host, candidates[idx].idleTimeout
}

func (p *Provider) RemoveActor(ctx context.Context, r ref.ActorRef) error {
	key := NewActorKey(r.ActorType, r.ActorID)

	p.Mu.Lock()
	defer p.Mu.Unlock()

	activeActor, ok := p.ActiveActors[key]
	if !ok {
		return components.ErrNoActor
	}

	changes := NewChanges()
	defer changes.Release()

	delete(p.ActiveActors, key)
	changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)

	// Prepare for rollbacks
	type rollbackLease struct {
		LeaseID         *string
		LeaseExpiration *time.Time
	}
	rollbackLeases := make(map[AlarmKey]rollbackLease, 0)

	// Release any alarm leases for this actor
	for k, a := range p.Alarms {
		if a.ActorType != r.ActorType || a.ActorID != r.ActorID {
			continue
		}

		if a.LeaseID != nil {
			rollbackLeases[k] = rollbackLease{LeaseID: a.LeaseID, LeaseExpiration: a.LeaseExpiration}
			a.LeaseID = nil
			a.LeaseExpiration = nil
			changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: a.ID, Value: a})
		}
	}

	// Persist changes
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		p.ActiveActors[key] = activeActor
		for k, a := range rollbackLeases {
			p.Alarms[k].LeaseID = a.LeaseID
			p.Alarms[k].LeaseExpiration = a.LeaseExpiration
		}

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}
