package standalone

import (
	"context"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ptr"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *provider) GetAlarm(ctx context.Context, aRef ref.AlarmRef) (components.GetAlarmRes, error) {
	key := newAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.mu.RLock()
	defer p.mu.RUnlock()

	a, ok := p.alarms[key]
	if !ok {
		return components.GetAlarmRes{}, components.ErrNoAlarm
	}

	res := components.GetAlarmRes{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  a.dueTime,
			Interval: a.interval,
			TTL:      a.ttl,
			Data:     a.data,
		},
	}

	return res, nil
}

func (p *provider) SetAlarm(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) error {
	key := newAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.mu.Lock()
	defer p.mu.Unlock()

	changes := newChanges()

	// Check if alarm already exists with same properties (to avoid resetting leases unnecessarily)
	existing, ok := p.alarms[key]
	if ok {
		// Check if properties are the same
		ok = existing.EqualProperties(alarmProperties{
			dueTime:  req.DueTime,
			interval: req.Interval,
			ttl:      req.TTL,
			data:     req.Data,
		})
		if ok {
			// Properties are the same, keep the existing alarm with its lease
			return nil
		}

		// Remove old alarm from alarmsByID
		delete(p.alarmsByID, existing.id)
		changes.Alarms.Delete = append(changes.Alarms.Delete, existing.id)
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

	a := &alarm{
		id:        alarmID,
		actorType: aRef.ActorType,
		actorID:   aRef.ActorID,
		name:      aRef.Name,
		dueTime:   req.DueTime,
		interval:  req.Interval,
		ttl:       req.TTL,
		data:      data,
		// Reset lease on any update
		leaseID:         nil,
		leaseExpiration: nil,
	}

	p.alarms[key] = a
	p.alarmsByID[alarmID] = a
	changes.Alarms.Create = append(changes.Alarms.Create, a)

	// Persist changes
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		// Rollback
		delete(p.alarms, key)
		delete(p.alarmsByID, alarmID)
		return err
	}

	return nil
}

func (p *provider) DeleteAlarm(ctx context.Context, aRef ref.AlarmRef) error {
	key := newAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.mu.Lock()
	defer p.mu.Unlock()

	a, ok := p.alarms[key]
	if !ok {
		return components.ErrNoAlarm
	}

	changes := newChanges()
	changes.Alarms.Delete = append(changes.Alarms.Delete, a.id)

	delete(p.alarmsByID, a.id)
	delete(p.alarms, key)

	// Persist changes
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}

func (p *provider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error) {
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.clock.Now()
	horizon := now.Add(p.cfg.AlarmsFetchAheadInterval)

	changes := newChanges()

	// Get healthy hosts from the request list and build capacity info
	type hostCapacity struct {
		hostID      string
		actorType   string
		idleTimeout time.Duration
		capacity    int // remaining capacity (MaxInt32 for unlimited)
	}

	// Build a map of actor type -> list of hosts with capacity
	capacitiesByType := make(map[string][]*hostCapacity)
	healthyHostsSet := make(map[string]struct{})

	for _, hostID := range req.Hosts {
		h, ok := p.hosts[hostID]
		if !ok || !p.isHostHealthy(h) {
			continue
		}
		healthyHostsSet[hostID] = struct{}{}

		for _, hat := range p.hostActorTypes[hostID] {
			capacity := math.MaxInt32
			if hat.concurrencyLimit > 0 {
				currentCount := p.countActiveActorsOnHost(hostID, hat.actorType)
				capacity = max(int(hat.concurrencyLimit)-currentCount, 0)
			}

			hc := &hostCapacity{
				hostID:      hostID,
				actorType:   hat.actorType,
				idleTimeout: hat.idleTimeout,
				capacity:    capacity,
			}
			capacitiesByType[hat.actorType] = append(capacitiesByType[hat.actorType], hc)
		}
	}

	if len(healthyHostsSet) == 0 {
		return nil, nil
	}

	// Collect upcoming alarms sorted by due time
	type alarmCandidate struct {
		alarm  *alarm
		hostID *string // nil if actor needs to be created
	}

	candidates := make([]alarmCandidate, 0, p.cfg.AlarmsFetchAheadBatchSize)

	// First pass: collect all potentially upcoming alarms
	type alarmWithTime struct {
		alarm *alarm
	}
	allAlarms := make([]alarmWithTime, 0)
	for _, a := range p.alarms {
		if a.dueTime.After(horizon) {
			continue
		}

		// Skip alarms with valid leases
		if a.leaseID != nil && a.leaseExpiration != nil && a.leaseExpiration.After(now) {
			continue
		}

		allAlarms = append(allAlarms, alarmWithTime{alarm: a})
	}

	// Sort by due time, then by alarm ID for consistent ordering
	sort.Slice(allAlarms, func(i, j int) bool {
		if allAlarms[i].alarm.dueTime.Equal(allAlarms[j].alarm.dueTime) {
			return allAlarms[i].alarm.id < allAlarms[j].alarm.id
		}
		return allAlarms[i].alarm.dueTime.Before(allAlarms[j].alarm.dueTime)
	})

	// Process alarms and check placement
	for _, awt := range allAlarms {
		if len(candidates) >= p.cfg.AlarmsFetchAheadBatchSize {
			break
		}

		a := awt.alarm

		// Check if actor is already active
		actKey := a.actorKey()
		var hostID *string
		var needsPlacement bool

		actor, ok := p.activeActors[actKey]
		if ok {
			// Actor is active - check where
			_, isInRequestList := healthyHostsSet[actor.hostID]
			if isInRequestList {
				// Actor is active on a host in the request list - include the alarm
				hostID = &actor.hostID
			} else {
				// Actor is active on a host NOT in the request list
				// Check if that host is healthy
				h, ok := p.hosts[actor.hostID]
				if ok && p.isHostHealthy(h) {
					// Actor is on a healthy host not in our list - SKIP this alarm
					continue
				}
				// Actor is on an unhealthy host - can be re-placed
				needsPlacement = true
			}
		} else {
			// Actor is not active - needs placement
			needsPlacement = true
		}

		if needsPlacement {
			// Need to find a host with capacity
			caps := capacitiesByType[a.actorType]
			if len(caps) == 0 {
				// No host can handle this actor type
				continue
			}

			// Find hosts with capacity
			hostsWithCap := make([]*hostCapacity, 0)
			for _, hc := range caps {
				if hc.capacity > 0 {
					hostsWithCap = append(hostsWithCap, hc)
				}
			}

			if len(hostsWithCap) == 0 {
				// No capacity available
				continue
			}

			// Pick random host
			// #nosec G404
			idx := rand.IntN(len(hostsWithCap))
			selected := hostsWithCap[idx]

			// Decrement capacity
			selected.capacity--

			hostID = &selected.hostID
		}

		candidates = append(candidates, alarmCandidate{alarm: a, hostID: hostID})
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// Generate a single lease ID prefix
	leaseIDPrefixObj, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	leaseIDPrefix := leaseIDPrefixObj.String()

	leaseExpiration := now.Add(p.cfg.AlarmsLeaseDuration)
	result := make([]*ref.AlarmLease, 0, len(candidates))

	for _, c := range candidates {
		a := c.alarm

		// Activate or update actor if needed
		if c.hostID != nil {
			actKey := a.actorKey()
			existingActor, exists := p.activeActors[actKey]

			// Check if actor needs to be created or updated
			needsUpdate := !exists
			if exists {
				h, ok := p.hosts[existingActor.hostID]
				if !ok || !p.isHostHealthy(h) {
					needsUpdate = true
				}
			}

			if needsUpdate {
				// Find idle timeout for this host/actor type
				var idleTimeout time.Duration
				for _, hat := range p.hostActorTypes[*c.hostID] {
					if hat.actorType == a.actorType {
						idleTimeout = hat.idleTimeout
						break
					}
				}

				activationTime := a.dueTime
				if now.After(activationTime) {
					activationTime = now
				}

				newActor := &activeActor{
					actorType:   a.actorType,
					actorID:     a.actorID,
					hostID:      *c.hostID,
					idleTimeout: idleTimeout,
					activation:  activationTime,
				}
				p.activeActors[actKey] = newActor

				if exists {
					changes.ActiveActors.Update[actKey] = newActor
				} else {
					changes.ActiveActors.Create = append(changes.ActiveActors.Create, newActor)
				}
			}
		}

		// Acquire lease
		leaseID := leaseIDPrefix + "_" + a.id
		a.leaseID = &leaseID
		a.leaseExpiration = &leaseExpiration
		changes.Alarms.Update[a.id] = a

		result = append(result, ref.NewAlarmLease(
			ref.AlarmRef{
				ActorType: a.actorType,
				ActorID:   a.actorID,
				Name:      a.name,
			},
			a.id,
			a.dueTime,
			leaseID,
		))
	}

	// Persist changes
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return nil, err
	}

	return result, nil
}

func (p *provider) GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (components.GetLeasedAlarmRes, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	a, ok := p.alarmsByID[lease.Key()]
	if !ok {
		return components.GetLeasedAlarmRes{}, components.ErrNoAlarm
	}

	// Check lease validity
	if !a.hasValidLease(lease.LeaseID(), p.clock.Now()) {
		return components.GetLeasedAlarmRes{}, components.ErrNoAlarm
	}

	return components.GetLeasedAlarmRes{
		AlarmRef: ref.AlarmRef{
			ActorType: a.actorType,
			ActorID:   a.actorID,
			Name:      a.name,
		},
		AlarmProperties: ref.AlarmProperties{
			DueTime:  a.dueTime,
			Interval: a.interval,
			TTL:      a.ttl,
			Data:     a.data,
		},
	}, nil
}

func (p *provider) RenewAlarmLeases(ctx context.Context, req components.RenewAlarmLeasesReq) (components.RenewAlarmLeasesRes, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.clock.Now()
	expTime := now.Add(p.cfg.AlarmsLeaseDuration)

	changes := newChanges()
	renewedLeases := make([]*ref.AlarmLease, 0, len(p.alarms))

	// Build set of lease IDs to renew if specified
	var leaseIDSet map[string]struct{}
	if len(req.Leases) > 0 {
		leaseIDSet = make(map[string]struct{}, len(req.Leases))
		for _, l := range req.Leases {
			leaseID, ok := l.LeaseID().(string)
			if !ok {
				// Indicates a development-time error
				panic("leaseID was expected to be a string")
			}
			leaseIDSet[leaseID] = struct{}{}
		}
	}

	// Build set of host IDs
	hostIDSet := make(map[string]struct{}, len(req.Hosts))
	for _, h := range req.Hosts {
		hostIDSet[h] = struct{}{}
	}

	for _, a := range p.alarms {
		// Check if alarm has a valid lease
		if a.leaseID == nil || a.leaseExpiration == nil || a.leaseExpiration.Before(now) {
			continue
		}

		// Check lease ID filter
		if leaseIDSet != nil {
			_, ok := leaseIDSet[*a.leaseID]
			if !ok {
				continue
			}
		}

		// Check if the actor is on one of the specified hosts
		actKey := a.actorKey()
		actor, ok := p.activeActors[actKey]
		if !ok {
			continue
		}

		_, ok = hostIDSet[actor.hostID]
		if !ok {
			continue
		}

		// Renew the lease
		a.leaseExpiration = &expTime
		changes.Alarms.Update[a.id] = a

		renewedLeases = append(renewedLeases, ref.NewAlarmLease(
			ref.AlarmRef{
				ActorType: a.actorType,
				ActorID:   a.actorID,
				Name:      a.name,
			},
			a.id,
			a.dueTime,
			*a.leaseID,
		))
	}

	// Persist changes
	if !changes.isEmpty() {
		if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
			return components.RenewAlarmLeasesRes{}, err
		}
	}

	return components.RenewAlarmLeasesRes{Leases: renewedLeases}, nil
}

func (p *provider) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	a, ok := p.alarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	if !a.hasValidLease(lease.LeaseID(), p.clock.Now()) {
		return components.ErrNoAlarm
	}

	// Release the lease
	a.leaseID = nil
	a.leaseExpiration = nil

	// Persist changes
	changes := newChanges()
	changes.Alarms.Update[a.id] = a
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}

func (p *provider) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	a, ok := p.alarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	now := p.clock.Now()
	if !a.hasValidLease(lease.LeaseID(), now) {
		return components.ErrNoAlarm
	}

	// Update due time
	a.dueTime = req.DueTime

	// Handle lease
	if req.RefreshLease {
		// Refresh the lease
		a.leaseExpiration = ptr.Of(now.Add(p.cfg.AlarmsLeaseDuration))
	} else {
		// Release the lease
		a.leaseID = nil
		a.leaseExpiration = nil
	}

	// Persist changes
	changes := newChanges()
	changes.Alarms.Update[a.id] = a
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}

func (p *provider) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	a, ok := p.alarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	if !a.hasValidLease(lease.LeaseID(), p.clock.Now()) {
		return components.ErrNoAlarm
	}

	// Delete the alarm
	delete(p.alarms, a.alarmKey())
	delete(p.alarmsByID, a.id)

	// Persist changes
	changes := newChanges()
	changes.Alarms.Delete = append(changes.Alarms.Delete, a.id)
	if err := p.persistHook.PersistChanges(ctx, changes); err != nil {
		return err
	}

	return nil
}
