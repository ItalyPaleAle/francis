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

	p.Mu.Lock()
	defer p.Mu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// Check if alarm already exists with same properties (to avoid resetting leases unnecessarily)
	existing, ok := p.Alarms[key]
	if ok {
		// Check if properties are the same
		ok = existing.EqualProperties(AlarmProperties{
			DueTime:  req.DueTime,
			Interval: req.Interval,
			TTL:      req.TTL,
			Data:     req.Data,
		})
		if ok {
			// Properties are the same, keep the existing alarm with its lease
			return nil
		}

		// Remove old alarm
		changes.Alarms.Delete = append(changes.Alarms.Delete, existing.ID)
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

	p.Alarms[key] = a
	p.AlarmsByID[alarmID] = a
	// When replacing an existing alarm, the new alarm gets a fresh ID, so we must
	// drop the previous ID's mapping to avoid leaking it (and to invalidate any
	// stale lease still referencing the old alarm ID)
	if existing != nil && existing.ID != alarmID {
		delete(p.AlarmsByID, existing.ID)
	}
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: alarmID, Value: a})

	// Persist changes
	err = p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		if existing != nil {
			p.Alarms[key] = existing
			p.AlarmsByID[existing.ID] = existing
		} else {
			delete(p.Alarms, key)
		}
		delete(p.AlarmsByID, alarmID)

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) DeleteAlarm(ctx context.Context, aRef ref.AlarmRef) error {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.Mu.Lock()
	defer p.Mu.Unlock()

	a, ok := p.Alarms[key]
	if !ok {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, a.ID)

	delete(p.AlarmsByID, a.ID)
	delete(p.Alarms, key)

	// Persist changes
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		p.Alarms[key] = a
		p.AlarmsByID[a.ID] = a

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error) {
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	now := p.Clock.Now()
	horizon := now.Add(p.Cfg.AlarmsFetchAheadInterval)

	changes := NewChanges()
	defer changes.Release()

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
				actorType:   hat.ActorType,
				idleTimeout: hat.IdleTimeout,
				capacity:    capacity,
			}
			capacitiesByType[hat.ActorType] = append(capacitiesByType[hat.ActorType], hc)
		}
	}

	if len(healthyHostsSet) == 0 {
		return nil, nil
	}

	// Collect upcoming alarms sorted by due time
	type alarmCandidate struct {
		alarm  *Alarm
		hostID *string // nil if actor needs to be created
	}

	candidates := make([]alarmCandidate, 0, p.Cfg.AlarmsFetchAheadBatchSize)

	// First pass: collect all potentially upcoming alarms
	type alarmWithTime struct {
		alarm *Alarm
	}
	allAlarms := make([]alarmWithTime, 0)
	for _, a := range p.Alarms {
		if a.DueTime.After(horizon) {
			continue
		}

		// Skip alarms with valid leases
		if a.LeaseID != nil && a.LeaseExpiration != nil && a.LeaseExpiration.After(now) {
			continue
		}

		allAlarms = append(allAlarms, alarmWithTime{alarm: a})
	}

	// Sort by due time, then by alarm ID for consistent ordering
	sort.Slice(allAlarms, func(i, j int) bool {
		if allAlarms[i].alarm.DueTime.Equal(allAlarms[j].alarm.DueTime) {
			return allAlarms[i].alarm.ID < allAlarms[j].alarm.ID
		}
		return allAlarms[i].alarm.DueTime.Before(allAlarms[j].alarm.DueTime)
	})

	// Process alarms and check placement
	for _, awt := range allAlarms {
		if len(candidates) >= p.Cfg.AlarmsFetchAheadBatchSize {
			break
		}

		a := awt.alarm

		// Check if actor is already active
		actKey := a.GetActorKey()
		var hostID *string
		var needsPlacement bool

		actor, ok := p.ActiveActors[actKey]
		if ok {
			// Actor is active - check where
			_, isInRequestList := healthyHostsSet[actor.HostID]
			if isInRequestList {
				// Actor is active on a host in the request list - include the alarm
				hostID = &actor.HostID
			} else {
				// Actor is active on a host NOT in the request list
				// Check if that host is healthy
				h, ok := p.Hosts[actor.HostID]
				if ok && p.IsHostHealthy(h) {
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
			caps := capacitiesByType[a.ActorType]
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

	leaseExpiration := now.Add(p.Cfg.AlarmsLeaseDuration)
	result := make([]*ref.AlarmLease, 0, len(candidates))

	// Track changes for rollback
	type actorRollback struct {
		key           ActorKey
		previousActor *ActiveActor // nil if actor was created
	}
	type alarmRollback struct {
		alarm                   *Alarm
		previousLeaseID         *string
		previousLeaseExpiration *time.Time
	}
	actorRollbacks := make([]actorRollback, 0)
	alarmRollbacks := make([]alarmRollback, 0)

	for _, c := range candidates {
		a := c.alarm

		// Activate or update actor if needed
		if c.hostID != nil {
			actKey := a.GetActorKey()
			existingActor, exists := p.ActiveActors[actKey]

			// Check if actor needs to be created or updated
			needsUpdate := !exists
			if exists {
				h, ok := p.Hosts[existingActor.HostID]
				if !ok || !p.IsHostHealthy(h) {
					needsUpdate = true
				}
			}

			if needsUpdate {
				// Find idle timeout for this host/actor type
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

				// Track for rollback
				if exists {
					actorRollbacks = append(actorRollbacks, actorRollback{key: actKey, previousActor: existingActor})
				} else {
					actorRollbacks = append(actorRollbacks, actorRollback{key: actKey, previousActor: nil})
				}

				p.ActiveActors[actKey] = newActor
				changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: actKey, Value: newActor})
			}
		}

		// Acquire lease
		alarmRollbacks = append(alarmRollbacks, alarmRollback{
			alarm:                   a,
			previousLeaseID:         a.LeaseID,
			previousLeaseExpiration: a.LeaseExpiration,
		})

		leaseID := leaseIDPrefix + "_" + a.ID
		a.LeaseID = &leaseID
		a.LeaseExpiration = &leaseExpiration
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: a.ID, Value: a})

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

	// Persist changes
	err = p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback actors
		for _, rb := range actorRollbacks {
			if rb.previousActor != nil {
				p.ActiveActors[rb.key] = rb.previousActor
			} else {
				delete(p.ActiveActors, rb.key)
			}
		}
		// Rollback alarm leases
		for _, rb := range alarmRollbacks {
			rb.alarm.LeaseID = rb.previousLeaseID
			rb.alarm.LeaseExpiration = rb.previousLeaseExpiration
		}

		return nil, fmt.Errorf("error persisting changes: %w", err)
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
	p.Mu.Lock()
	defer p.Mu.Unlock()

	now := p.Clock.Now()
	expTime := now.Add(p.Cfg.AlarmsLeaseDuration)

	changes := NewChanges()
	defer changes.Release()
	renewedLeases := make([]*ref.AlarmLease, 0, len(p.Alarms))

	// Build set of lease IDs to renew if specified
	var leaseIDSet map[string]struct{}
	if len(req.Leases) > 0 {
		leaseIDSet = make(map[string]struct{}, len(req.Leases))
		for _, l := range req.Leases {
			leaseID, ok := l.LeaseID().(string)
			if !ok {
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

	// Track changes for rollback
	type leaseRollback struct {
		alarm              *Alarm
		previousExpiration *time.Time
	}
	rollbacks := make([]leaseRollback, 0)

	for _, a := range p.Alarms {
		// Check if alarm has a valid lease
		if a.LeaseID == nil || a.LeaseExpiration == nil || a.LeaseExpiration.Before(now) {
			continue
		}

		// Check lease ID filter
		if leaseIDSet != nil {
			_, ok := leaseIDSet[*a.LeaseID]
			if !ok {
				continue
			}
		}

		// Check if the actor is on one of the specified hosts
		actKey := a.GetActorKey()
		actor, ok := p.ActiveActors[actKey]
		if !ok {
			continue
		}

		_, ok = hostIDSet[actor.HostID]
		if !ok {
			continue
		}

		// Track for rollback
		rollbacks = append(rollbacks, leaseRollback{alarm: a, previousExpiration: a.LeaseExpiration})

		// Renew the lease
		a.LeaseExpiration = &expTime
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: a.ID, Value: a})

		renewedLeases = append(renewedLeases, ref.NewAlarmLease(
			ref.AlarmRef{
				ActorType: a.ActorType,
				ActorID:   a.ActorID,
				Name:      a.Name,
			},
			a.ID,
			a.DueTime,
			*a.LeaseID,
		))
	}

	// Persist changes
	if !changes.IsEmpty() {
		err := p.PersistHook.PersistChanges(ctx, changes)
		if err != nil {
			// Rollback
			for _, rb := range rollbacks {
				rb.alarm.LeaseExpiration = rb.previousExpiration
			}

			return components.RenewAlarmLeasesRes{}, fmt.Errorf("error persisting changes: %w", err)
		}
	}

	return components.RenewAlarmLeasesRes{Leases: renewedLeases}, nil
}

func (p *Provider) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	a, ok := p.AlarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	if !a.HasValidLease(lease.LeaseID(), p.Clock.Now()) {
		return components.ErrNoAlarm
	}

	// Release the lease
	rollbackLeaseID := a.LeaseID
	rollbackLeaseExpiration := a.LeaseExpiration
	a.LeaseID = nil
	a.LeaseExpiration = nil

	// Persist changes
	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: a.ID, Value: a})
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		a.LeaseID = rollbackLeaseID
		a.LeaseExpiration = rollbackLeaseExpiration

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	a, ok := p.AlarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	now := p.Clock.Now()
	if !a.HasValidLease(lease.LeaseID(), now) {
		return components.ErrNoAlarm
	}

	// Update due time
	rollbackDueTime := a.DueTime
	a.DueTime = req.DueTime

	// Handle lease
	rollBackLeaseID := a.LeaseID
	rollbackLeaseExpiration := a.LeaseExpiration
	if req.RefreshLease {
		// Refresh the lease
		a.LeaseExpiration = ptr.Of(now.Add(p.Cfg.AlarmsLeaseDuration))
	} else {
		// Release the lease
		a.LeaseID = nil
		a.LeaseExpiration = nil
	}

	// Persist changes
	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: a.ID, Value: a})
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		a.DueTime = rollbackDueTime
		a.LeaseID = rollBackLeaseID
		a.LeaseExpiration = rollbackLeaseExpiration

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}

func (p *Provider) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error {
	p.Mu.Lock()
	defer p.Mu.Unlock()

	a, ok := p.AlarmsByID[lease.Key()]
	if !ok {
		return components.ErrNoAlarm
	}

	// Check lease validity
	if !a.HasValidLease(lease.LeaseID(), p.Clock.Now()) {
		return components.ErrNoAlarm
	}

	// Delete the alarm
	delete(p.Alarms, a.GetAlarmKey())
	delete(p.AlarmsByID, a.ID)

	// Persist changes
	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, a.ID)
	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		// Rollback
		p.Alarms[a.GetAlarmKey()] = a
		p.AlarmsByID[a.ID] = a

		return fmt.Errorf("error persisting changes: %w", err)
	}

	return nil
}
