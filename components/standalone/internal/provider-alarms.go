package internal

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ptr"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) GetAlarm(ctx context.Context, aRef ref.AlarmRef) (components.GetAlarmRes, error) {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	a, ok := p.Alarms[key]
	if !ok {
		return components.GetAlarmRes{}, components.ErrNoAlarm
	}

	res := components.GetAlarmRes{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  a.DueTime,
			Interval: a.Interval,
			TTL:      a.TTL,
			Data:     a.Data,
		},
	}

	return res, nil
}

func (p *Provider) SetAlarm(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) error {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// Check if the alarm already exists with the same properties (to avoid resetting leases unnecessarily)
	p.Mu.RLock()
	existing, exists := p.Alarms[key]
	sameProps := exists && existing.EqualProperties(AlarmProperties{
		DueTime:  req.DueTime,
		Interval: req.Interval,
		TTL:      req.TTL,
		Data:     req.Data,
	})
	var existingID string
	if exists {
		existingID = existing.ID
	}
	p.Mu.RUnlock()

	if sameProps {
		// Properties are the same: keep the existing alarm with its lease
		return nil
	}

	// Normalize empty data to nil
	data := req.Data
	if data != nil && len(data) == 0 {
		data = nil
	}

	// Generate a new alarm ID
	alarmIDObj, err := uuid.NewV7()
	if err != nil {
		return err
	}
	alarmID := alarmIDObj.String()

	a := &Alarm{
		ID:        alarmID,
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		Name:      aRef.Name,
		DueTime:   req.DueTime,
		Interval:  req.Interval,
		TTL:       req.TTL,
		Data:      data,
		// Reset lease on any update
		LeaseID:         nil,
		LeaseExpiration: nil,
	}

	if exists {
		// Remove the old alarm (it gets a fresh ID)
		changes.Alarms.Delete = append(changes.Alarms.Delete, existingID)
	}
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: alarmID, Value: a})

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = a
		p.AlarmsByID[alarmID] = a
		// When replacing an existing alarm, the new alarm gets a fresh ID, so we drop the previous ID's mapping to avoid leaking it (and to invalidate any stale lease still referencing the old alarm ID)
		if exists && existingID != alarmID {
			delete(p.AlarmsByID, existingID)
		}
	})
}

func (p *Provider) DeleteAlarm(ctx context.Context, aRef ref.AlarmRef) error {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	a, ok := p.Alarms[key]
	var id string
	if ok {
		id = a.ID
	}
	p.Mu.RUnlock()

	if !ok {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, id)

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Alarms, key)
		delete(p.AlarmsByID, id)
	})
}

func (p *Provider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error) {
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()
	horizon := now.Add(p.Cfg.AlarmsFetchAheadInterval)
	leaseExpiration := now.Add(p.Cfg.AlarmsLeaseDuration)

	changes := NewChanges()
	defer changes.Release()

	// hostCapacity tracks the remaining capacity for a (host, actor type) pair.
	type hostCapacity struct {
		hostID      string
		idleTimeout time.Duration
		capacity    int // remaining capacity (MaxInt32 for unlimited)
	}

	// actorPlacement is a staged actor activation (applied only after persistence).
	type actorPlacement struct {
		key   ActorKey
		actor *ActiveActor
	}
	// leaseUpdate is a staged alarm lease assignment (applied only after persistence).
	type leaseUpdate struct {
		key     AlarmKey
		updated *Alarm
	}

	var (
		result       []*ref.AlarmLease
		placements   []actorPlacement
		leaseUpdates []leaseUpdate
		stagedActors = make(map[ActorKey]struct{})
	)

	// Everything below only reads the in-memory maps; mutations are staged and applied after the
	// change set has been persisted. writeMu ensures no other writer alters the maps meanwhile.
	p.Mu.RLock()

	// Build a map of actor type -> list of hosts with capacity, and the set of healthy hosts
	capacitiesByType := make(map[string][]*hostCapacity)
	healthyHostsSet := make(map[string]struct{})
	for _, hostID := range req.Hosts {
		h, ok := p.Hosts[hostID]
		if !ok || !p.IsHostHealthy(h) {
			continue
		}
		healthyHostsSet[hostID] = struct{}{}

		for _, hat := range p.HostActorTypes[hostID] {
			capacity := math.MaxInt32
			if hat.ConcurrencyLimit > 0 {
				currentCount := p.CountActiveActorsOnHost(hostID, hat.ActorType)
				capacity = max(int(hat.ConcurrencyLimit)-currentCount, 0)
			}

			hc := &hostCapacity{
				hostID:      hostID,
				idleTimeout: hat.IdleTimeout,
				capacity:    capacity,
			}
			capacitiesByType[hat.ActorType] = append(capacitiesByType[hat.ActorType], hc)
		}
	}

	if len(healthyHostsSet) == 0 {
		p.Mu.RUnlock()
		return nil, nil
	}

	// Collect all potentially upcoming alarms (within the horizon and not validly leased)
	upcoming := make([]*Alarm, 0)
	for _, a := range p.Alarms {
		if a.DueTime.After(horizon) {
			continue
		}
		// Skip alarms with valid leases
		if a.LeaseID != nil && a.LeaseExpiration != nil && a.LeaseExpiration.After(now) {
			continue
		}
		upcoming = append(upcoming, a)
	}

	// Sort by due time, then by alarm ID for consistent ordering
	sort.Slice(upcoming, func(i, j int) bool {
		if upcoming[i].DueTime.Equal(upcoming[j].DueTime) {
			return upcoming[i].ID < upcoming[j].ID
		}
		return upcoming[i].DueTime.Before(upcoming[j].DueTime)
	})

	// alarmCandidate pairs an alarm with the host its actor will run on (nil host = skip).
	type alarmCandidate struct {
		alarm  *Alarm
		hostID *string
	}
	candidates := make([]alarmCandidate, 0, p.Cfg.AlarmsFetchAheadBatchSize)

	for _, a := range upcoming {
		if len(candidates) >= p.Cfg.AlarmsFetchAheadBatchSize {
			break
		}

		actKey := a.GetActorKey()
		var (
			hostID         *string
			needsPlacement bool
		)

		actor, ok := p.ActiveActors[actKey]
		switch {
		case ok:
			if _, isInRequestList := healthyHostsSet[actor.HostID]; isInRequestList {
				// Actor is active on a host in the request list - include the alarm
				hostID = &actor.HostID
			} else if h, ok := p.Hosts[actor.HostID]; ok && p.IsHostHealthy(h) {
				// Actor is active on a healthy host not in our list - SKIP this alarm
				continue
			} else {
				// Actor is on an unhealthy host - it can be re-placed
				needsPlacement = true
			}
		default:
			// Actor is not active - needs placement
			needsPlacement = true
		}

		if needsPlacement {
			caps := capacitiesByType[a.ActorType]
			if len(caps) == 0 {
				// No host can handle this actor type
				continue
			}

			// Find hosts with remaining capacity
			hostsWithCap := make([]*hostCapacity, 0, len(caps))
			for _, hc := range caps {
				if hc.capacity > 0 {
					hostsWithCap = append(hostsWithCap, hc)
				}
			}
			if len(hostsWithCap) == 0 {
				// No capacity available
				continue
			}

			// Pick a random host and consume one unit of its capacity
			// #nosec G404
			selected := hostsWithCap[rand.IntN(len(hostsWithCap))]
			selected.capacity--
			hostID = &selected.hostID
		}

		candidates = append(candidates, alarmCandidate{alarm: a, hostID: hostID})
	}

	if len(candidates) == 0 {
		p.Mu.RUnlock()
		return nil, nil
	}

	// Generate a single lease ID prefix shared by this batch
	leaseIDPrefixObj, err := uuid.NewV7()
	if err != nil {
		p.Mu.RUnlock()
		return nil, err
	}
	leaseIDPrefix := leaseIDPrefixObj.String()

	result = make([]*ref.AlarmLease, 0, len(candidates))
	for _, c := range candidates {
		a := c.alarm

		// Stage actor activation if needed (only once per actor in this batch)
		if c.hostID != nil {
			actKey := a.GetActorKey()
			if _, staged := stagedActors[actKey]; !staged {
				existingActor, activeExists := p.ActiveActors[actKey]
				needsUpdate := !activeExists
				if activeExists {
					h, ok := p.Hosts[existingActor.HostID]
					if !ok || !p.IsHostHealthy(h) {
						needsUpdate = true
					}
				}

				if needsUpdate {
					// Find the idle timeout for this host/actor type
					var idleTimeout time.Duration
					for _, hat := range p.HostActorTypes[*c.hostID] {
						if hat.ActorType == a.ActorType {
							idleTimeout = hat.IdleTimeout
							break
						}
					}

					activationTime := a.DueTime
					if now.After(activationTime) {
						activationTime = now
					}

					newActor := &ActiveActor{
						ActorType:   a.ActorType,
						ActorID:     a.ActorID,
						HostID:      *c.hostID,
						IdleTimeout: idleTimeout,
						Activation:  activationTime,
					}
					stagedActors[actKey] = struct{}{}
					placements = append(placements, actorPlacement{key: actKey, actor: newActor})
					changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: actKey, Value: newActor})
				}
			}
		}

		// Stage the lease assignment on a clone of the alarm
		leaseID := leaseIDPrefix + "_" + a.ID
		updated := a.Clone()
		updated.LeaseID = &leaseID
		updated.LeaseExpiration = &leaseExpiration
		leaseUpdates = append(leaseUpdates, leaseUpdate{key: a.GetAlarmKey(), updated: updated})
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: updated.ID, Value: updated})

		result = append(result, ref.NewAlarmLease(
			ref.AlarmRef{
				ActorType: a.ActorType,
				ActorID:   a.ActorID,
				Name:      a.Name,
			},
			a.ID,
			a.DueTime,
			leaseID,
		))
	}
	p.Mu.RUnlock()

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		for _, pl := range placements {
			p.ActiveActors[pl.key] = pl.actor
		}
		for _, lu := range leaseUpdates {
			p.Alarms[lu.key] = lu.updated
			p.AlarmsByID[lu.updated.ID] = lu.updated
		}
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *Provider) GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (components.GetLeasedAlarmRes, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	a, ok := p.AlarmsByID[lease.Key()]
	if !ok {
		return components.GetLeasedAlarmRes{}, components.ErrNoAlarm
	}

	// Check lease validity
	if !a.HasValidLease(lease.LeaseID(), p.Clock.Now()) {
		return components.GetLeasedAlarmRes{}, components.ErrNoAlarm
	}

	return components.GetLeasedAlarmRes{
		AlarmRef: ref.AlarmRef{
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
		},
		AlarmProperties: ref.AlarmProperties{
			DueTime:  a.DueTime,
			Interval: a.Interval,
			TTL:      a.TTL,
			Data:     a.Data,
		},
	}, nil
}

func (p *Provider) RenewAlarmLeases(ctx context.Context, req components.RenewAlarmLeasesReq) (components.RenewAlarmLeasesRes, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()
	expTime := now.Add(p.Cfg.AlarmsLeaseDuration)

	changes := NewChanges()
	defer changes.Release()

	// Build set of lease IDs to renew if specified
	var leaseIDSet map[string]struct{}
	if len(req.Leases) > 0 {
		leaseIDSet = make(map[string]struct{}, len(req.Leases))
		for _, l := range req.Leases {
			leaseID, ok := l.LeaseID().(string)
			if !ok {
				// Most certainly a development-time error
				return components.RenewAlarmLeasesRes{}, fmt.Errorf("invalid lease ID %v: expected a string but got %T", l.LeaseID(), l.LeaseID())
			}
			leaseIDSet[leaseID] = struct{}{}
		}
	}

	// Build set of host IDs
	hostIDSet := make(map[string]struct{}, len(req.Hosts))
	for _, h := range req.Hosts {
		hostIDSet[h] = struct{}{}
	}

	// leaseRenewal is a staged lease renewal (applied only after persistence).
	type leaseRenewal struct {
		key     AlarmKey
		updated *Alarm
	}

	var (
		renewals      []leaseRenewal
		renewedLeases = make([]*ref.AlarmLease, 0, len(req.Leases))
	)

	p.Mu.RLock()
	for _, a := range p.Alarms {
		// Check if alarm has a valid lease
		if a.LeaseID == nil || a.LeaseExpiration == nil || a.LeaseExpiration.Before(now) {
			continue
		}

		// Check lease ID filter
		if leaseIDSet != nil {
			if _, ok := leaseIDSet[*a.LeaseID]; !ok {
				continue
			}
		}

		// Check that the actor is active on one of the specified hosts
		actor, ok := p.ActiveActors[a.GetActorKey()]
		if !ok {
			continue
		}
		if _, ok := hostIDSet[actor.HostID]; !ok {
			continue
		}

		// Renew the lease on a clone of the alarm
		updated := a.Clone()
		updated.LeaseExpiration = &expTime
		renewals = append(renewals, leaseRenewal{key: a.GetAlarmKey(), updated: updated})
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: updated.ID, Value: updated})

		renewedLeases = append(renewedLeases, ref.NewAlarmLease(
			ref.AlarmRef{
				ActorType: a.ActorType,
				ActorID:   a.ActorID,
				Name:      a.Name,
			},
			updated.ID,
			updated.DueTime,
			*updated.LeaseID,
		))
	}
	p.Mu.RUnlock()

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		for _, r := range renewals {
			p.Alarms[r.key] = r.updated
			p.AlarmsByID[r.updated.ID] = r.updated
		}
	})
	if err != nil {
		return components.RenewAlarmLeasesRes{}, err
	}

	return components.RenewAlarmLeasesRes{Leases: renewedLeases}, nil
}

func (p *Provider) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[lease.Key()]
	valid := ok && a.HasValidLease(lease.LeaseID(), p.Clock.Now())
	var (
		key     AlarmKey
		updated *Alarm
	)
	if valid {
		key = a.GetAlarmKey()
		updated = a.Clone()
		updated.LeaseID = nil
		updated.LeaseExpiration = nil
	}
	p.Mu.RUnlock()

	if !valid {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: updated.ID, Value: updated})

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = updated
		p.AlarmsByID[updated.ID] = updated
	})
}

func (p *Provider) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[lease.Key()]
	valid := ok && a.HasValidLease(lease.LeaseID(), now)
	var (
		key     AlarmKey
		updated *Alarm
	)
	if valid {
		key = a.GetAlarmKey()
		updated = a.Clone()
		updated.DueTime = req.DueTime
		if req.RefreshLease {
			// Refresh the lease
			updated.LeaseExpiration = ptr.Of(now.Add(p.Cfg.AlarmsLeaseDuration))
		} else {
			// Release the lease
			updated.LeaseID = nil
			updated.LeaseExpiration = nil
		}
	}
	p.Mu.RUnlock()

	if !valid {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: updated.ID, Value: updated})

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = updated
		p.AlarmsByID[updated.ID] = updated
	})
}

func (p *Provider) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[lease.Key()]
	valid := ok && a.HasValidLease(lease.LeaseID(), p.Clock.Now())
	var (
		key AlarmKey
		id  string
	)
	if valid {
		key = a.GetAlarmKey()
		id = a.ID
	}
	p.Mu.RUnlock()

	if !valid {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, id)

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Alarms, key)
		delete(p.AlarmsByID, id)
	})
}
