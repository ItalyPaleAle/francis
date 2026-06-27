package internal

import (
	"context"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	// If the host wants to reattach to an existing registration, take the dedicated path
	if req.ExistingHostID != "" {
		return p.reattachHost(ctx, req)
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	p.Mu.RLock()
	// Clean up unhealthy hosts first
	// The deletions are applied together with the new host
	cleanupApply := p.CleanupUnhealthyHosts(changes)

	// Check if there's already a healthy host at this address
	// Unhealthy hosts at the same address are cleaned up above, so they don't block registration
	alreadyRegistered := false
	if existingHostID, ok := p.HostsByAddress[req.Address]; ok {
		existingHost, ok := p.Hosts[existingHostID]
		if ok && p.IsHostHealthy(existingHost) {
			alreadyRegistered = true
		}
	}
	p.Mu.RUnlock()

	if alreadyRegistered {
		return components.RegisterHostRes{}, components.ErrHostAlreadyRegistered
	}

	// Generate a new host ID
	hostIDObj, err := uuid.NewV7()
	if err != nil {
		return components.RegisterHostRes{}, err
	}
	hostID := hostIDObj.String()

	// Create the host
	h := &Host{
		ID:              hostID,
		Address:         req.Address,
		LastHealthCheck: p.Clock.Now(),
	}
	changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: h})

	// Build actor types
	var hats []*HostActorType
	if len(req.ActorTypes) > 0 {
		hats = make([]*HostActorType, len(req.ActorTypes))
		for i, at := range req.ActorTypes {
			hat := &HostActorType{
				HostID:           hostID,
				ActorType:        at.ActorType,
				IdleTimeout:      at.IdleTimeout,
				ConcurrencyLimit: at.ConcurrencyLimit,
			}
			hats[i] = hat
			changes.HostActorTypes.Set = append(changes.HostActorTypes.Set, hat)
		}
	}

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		cleanupApply()
		p.Hosts[hostID] = h
		p.HostsByAddress[req.Address] = hostID
		if hats != nil {
			p.HostActorTypes[hostID] = hats
		}
	})
	if err != nil {
		return components.RegisterHostRes{}, err
	}

	return components.RegisterHostRes{HostID: hostID}, nil
}

// reattachHost reattaches a reconnecting host to its existing registration, identified by req.ExistingHostID
// It refreshes the registration in place even if its health record is stale, so a host can reclaim it after a runtime failover without waiting for the previous health record to expire
// If the registration no longer exists, a brand-new one is created instead
func (p *Provider) reattachHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	p.Mu.RLock()
	existing, exists := p.Hosts[req.ExistingHostID]

	// Detect an address conflict with a different, healthy host
	// Unhealthy hosts at the same address are cleaned up below, so they do not block reattachment
	conflict := false
	otherID, ok := p.HostsByAddress[req.Address]
	if ok && otherID != req.ExistingHostID {
		other, ok2 := p.Hosts[otherID]
		if ok2 && p.IsHostHealthy(other) {
			conflict = true
		}
	}

	// Plan cleanup of unhealthy hosts, but never the registration we are reattaching to
	var (
		cleanupHostIDs   []string
		cleanupAddresses []string
		cleanupActors    []ActorKey
	)
	for id, h := range p.Hosts {
		if id == req.ExistingHostID || p.IsHostHealthy(h) {
			continue
		}

		cleanupHostIDs = append(cleanupHostIDs, id)
		cleanupAddresses = append(cleanupAddresses, h.Address)
		changes.Hosts.Delete = append(changes.Hosts.Delete, id)

		for _, hat := range p.HostActorTypes[id] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}

		for key, actor := range p.ActiveActors {
			if actor.HostID != id {
				continue
			}
			cleanupActors = append(cleanupActors, key)
			changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
		}
	}

	// Build the reattach changes while still holding the read lock
	var (
		updatedHost *Host
		oldAddress  string
		newHats     []*HostActorType
	)
	if exists {
		oldAddress = existing.Address
		updatedHost = existing.Clone()
		updatedHost.Address = req.Address
		updatedHost.LastHealthCheck = p.Clock.Now()
		changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: req.ExistingHostID, Value: updatedHost})

		// Replace the supported actor types
		for _, hat := range p.HostActorTypes[req.ExistingHostID] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}
		newHats = buildHostActorTypes(req.ExistingHostID, req.ActorTypes, changes)
	}
	p.Mu.RUnlock()

	if conflict {
		return components.RegisterHostRes{}, components.ErrHostAlreadyRegistered
	}

	applyCleanup := func() {
		for i, id := range cleanupHostIDs {
			delete(p.Hosts, id)
			delete(p.HostsByAddress, cleanupAddresses[i])
			delete(p.HostActorTypes, id)
		}

		for _, key := range cleanupActors {
			delete(p.ActiveActors, key)
		}
	}

	// Reattach to the existing registration
	if exists {
		err := p.persistThenApply(ctx, &p.Mu, changes, func() {
			applyCleanup()

			if oldAddress != req.Address {
				delete(p.HostsByAddress, oldAddress)
			}

			p.Hosts[req.ExistingHostID] = updatedHost
			p.HostsByAddress[req.Address] = req.ExistingHostID

			if len(newHats) > 0 {
				p.HostActorTypes[req.ExistingHostID] = newHats
			} else {
				delete(p.HostActorTypes, req.ExistingHostID)
			}
		})
		if err != nil {
			return components.RegisterHostRes{}, err
		}
		return components.RegisterHostRes{HostID: req.ExistingHostID, Reattached: true}, nil
	}

	// The existing registration was not found: create a new one with a fresh host ID
	hostIDObj, err := uuid.NewV7()
	if err != nil {
		return components.RegisterHostRes{}, err
	}
	hostID := hostIDObj.String()

	h := &Host{
		ID:              hostID,
		Address:         req.Address,
		LastHealthCheck: p.Clock.Now(),
	}
	changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: h})
	freshHats := buildHostActorTypes(hostID, req.ActorTypes, changes)

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		applyCleanup()

		p.Hosts[hostID] = h
		p.HostsByAddress[req.Address] = hostID

		if len(freshHats) > 0 {
			p.HostActorTypes[hostID] = freshHats
		}
	})
	if err != nil {
		return components.RegisterHostRes{}, err
	}

	return components.RegisterHostRes{HostID: hostID, Reattached: false}, nil
}

// buildHostActorTypes constructs HostActorType entries for the given host and records them as upserts in changes
func buildHostActorTypes(hostID string, actorTypes []components.ActorHostType, changes *Changes) []*HostActorType {
	if len(actorTypes) == 0 {
		return nil
	}

	hats := make([]*HostActorType, len(actorTypes))
	for i, at := range actorTypes {
		hat := &HostActorType{
			HostID:           hostID,
			ActorType:        at.ActorType,
			IdleTimeout:      at.IdleTimeout,
			ConcurrencyLimit: at.ConcurrencyLimit,
		}
		hats[i] = hat
		changes.HostActorTypes.Set = append(changes.HostActorTypes.Set, hat)
	}
	return hats
}

func (p *Provider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// Nothing to update
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		return nil
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	p.Mu.RLock()
	h, ok := p.Hosts[hostID]
	healthy := ok && p.IsHostHealthy(h)

	var (
		updatedHost *Host
		newHats     []*HostActorType
	)
	if healthy {
		// Update last health check if requested
		// We clone the host instead of mutating it in place, so nothing changes in memory until the change has been persisted
		if req.UpdateLastHealthCheck {
			updatedHost = h.Clone()
			updatedHost.LastHealthCheck = p.Clock.Now()
			changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: updatedHost})
		}

		// Update actor types if provided (non-nil)
		// A nil list means "do not update", while an empty, non-nil list removes all actor types
		if req.ActorTypes != nil {
			for _, hat := range p.HostActorTypes[hostID] {
				changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
					HostID:    hat.HostID,
					ActorType: hat.ActorType,
				})
			}

			newHats = make([]*HostActorType, len(req.ActorTypes))
			for i, at := range req.ActorTypes {
				hat := &HostActorType{
					HostID:           hostID,
					ActorType:        at.ActorType,
					IdleTimeout:      at.IdleTimeout,
					ConcurrencyLimit: at.ConcurrencyLimit,
				}
				newHats[i] = hat
				changes.HostActorTypes.Set = append(changes.HostActorTypes.Set, hat)
			}
		}
	}
	p.Mu.RUnlock()

	if !healthy {
		// Host doesn't exist, or exists but is un-healthy
		return components.ErrHostUnregistered
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		if updatedHost != nil {
			p.Hosts[hostID] = updatedHost
		}
		if req.ActorTypes != nil {
			p.HostActorTypes[hostID] = newHats
		}
	})
}

func (p *Provider) UnregisterHost(ctx context.Context, hostID string) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	p.Mu.RLock()
	h, ok := p.Hosts[hostID]
	var (
		address      string
		wasHealthy   bool
		deleteActors []ActorKey
	)
	if ok {
		address = h.Address
		wasHealthy = p.IsHostHealthy(h)

		changes.Hosts.Delete = append(changes.Hosts.Delete, hostID)

		for _, hat := range p.HostActorTypes[hostID] {
			changes.HostActorTypes.Delete = append(changes.HostActorTypes.Delete, HostActorTypeKey{
				HostID:    hat.HostID,
				ActorType: hat.ActorType,
			})
		}

		for key, actor := range p.ActiveActors {
			if actor.HostID != hostID {
				continue
			}
			deleteActors = append(deleteActors, key)
			changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)
		}
	}
	p.Mu.RUnlock()

	if !ok {
		return components.ErrHostUnregistered
	}

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Hosts, hostID)
		delete(p.HostsByAddress, address)
		delete(p.HostActorTypes, hostID)
		for _, key := range deleteActors {
			delete(p.ActiveActors, key)
		}
	})
	if err != nil {
		return err
	}

	// If the host was unhealthy, we still cleaned it up, but we report it as unregistered
	if !wasHealthy {
		return components.ErrHostUnregistered
	}

	return nil
}

// ListHosts returns all hosts that are currently registered and healthy
func (p *Provider) ListHosts(ctx context.Context) ([]components.HostInfo, error) {
	// A read lock is enough because we only take a snapshot of the in-memory host map
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Collect every healthy host, skipping those whose health check has expired but that have not been garbage-collected yet
	hosts := make([]components.HostInfo, 0, len(p.Hosts))
	for _, h := range p.Hosts {
		if !p.IsHostHealthy(h) {
			continue
		}
		hosts = append(hosts, components.HostInfo{
			HostID:          h.ID,
			Address:         h.Address,
			LastHealthCheck: h.LastHealthCheck,
		})
	}

	return hosts, nil
}

func (p *Provider) LookupActor(ctx context.Context, r ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	key := NewActorKey(r.ActorType, r.ActorID)

	// If we only want actors that are already active, use a simpler path
	if opts.ActiveOnly {
		return p.lookupActiveActor(r, opts.Hosts)
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	var (
		existingRes  components.LookupActorRes
		haveExisting bool
		restricted   bool // actor is active on a healthy host that's not allowed
		host         *Host
		idleTimeout  time.Duration
	)

	// Check if the actor is already active on a healthy host
	actor, ok := p.ActiveActors[key]
	if ok {
		h, ok := p.Hosts[actor.HostID]
		if ok && p.IsHostHealthy(h) {
			if len(opts.Hosts) > 0 && !slices.Contains(opts.Hosts, actor.HostID) {
				restricted = true
			} else {
				existingRes = components.LookupActorRes{
					HostID:      actor.HostID,
					Address:     h.Address,
					IdleTimeout: actor.IdleTimeout,
				}
				haveExisting = true
			}
		}
	}

	// If not active on a healthy host, find a host with capacity to activate it on
	if !haveExisting && !restricted {
		host, idleTimeout = p.findHostWithCapacity(r.ActorType, opts.Hosts)
	}
	p.Mu.RUnlock()

	switch {
	case restricted:
		return components.LookupActorRes{}, components.ErrNoHost
	case haveExisting:
		return existingRes, nil
	case host == nil:
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

	changes := NewChanges()
	defer changes.Release()
	changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: key, Value: newActor})

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.ActiveActors[key] = newActor
	})
	if err != nil {
		return components.LookupActorRes{}, err
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

// findHostWithCapacity finds a healthy host that can host an actor of the given type
// Must be called while holding at least a read lock on Mu
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

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// alarmUpdate carries the cloned alarm (with its lease cleared) that replaces the live one
	type alarmUpdate struct {
		key     AlarmKey
		updated *Alarm
	}

	p.Mu.RLock()
	_, ok := p.ActiveActors[key]
	var alarmUpdates []alarmUpdate
	if ok {
		changes.ActiveActors.Delete = append(changes.ActiveActors.Delete, key)

		// Release any alarm leases for this actor
		// We clone the affected alarms (rather than mutate them in place) so nothing changes in memory until the change has been persisted
		for k, a := range p.Alarms {
			if a.ActorType != r.ActorType || a.ActorID != r.ActorID {
				continue
			}
			if a.LeaseID != nil {
				updated := a.Clone()
				updated.LeaseID = nil
				updated.LeaseExpiration = nil
				alarmUpdates = append(alarmUpdates, alarmUpdate{key: k, updated: updated})
				changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: updated.ID, Value: updated})
			}
		}
	}
	p.Mu.RUnlock()

	if !ok {
		return components.ErrNoActor
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.ActiveActors, key)
		for _, au := range alarmUpdates {
			p.Alarms[au.key] = au.updated
			p.AlarmsByID[au.updated.ID] = au.updated
		}
	})
}
