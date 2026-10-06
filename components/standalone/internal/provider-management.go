package internal

import (
	"cmp"
	"context"
	"slices"
	"uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) ListHostDetails(_ context.Context, req components.ListHostDetailsReq) (components.ListHostDetailsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Collect the visible hosts after the cursor, including draining ones, which ListHosts would include too
	hosts := make([]*Host, 0, len(p.Hosts))
	for id, h := range p.Hosts {
		if id <= req.After || !p.IsHostHealthy(h) {
			continue
		}
		hosts = append(hosts, h)
	}

	// The map has no order of its own, so the host ID order the API promises has to be established here
	slices.SortFunc(hosts, func(a, b *Host) int {
		return cmp.Compare(a.ID, b.ID)
	})

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	hasMore := len(hosts) > limit
	if hasMore {
		hosts = hosts[:limit]
	}

	// Count the placements of each host and type in a single pass rather than once per actor type
	counts := p.countPlacementsByHostType()

	res := components.ListHostDetailsRes{
		Hosts:   make([]components.HostDetails, len(hosts)),
		HasMore: hasMore,
	}
	for i, h := range hosts {
		res.Hosts[i] = p.hostDetails(h, counts)
	}

	return res, nil
}

func (p *Provider) GetHostDetails(_ context.Context, hostID string) (components.HostDetails, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// A host whose registration expired is reported as missing, even before the garbage collector removes it
	h, ok := p.Hosts[hostID]
	if !ok || !p.IsHostHealthy(h) {
		return components.HostDetails{}, components.ErrHostUnregistered
	}

	return p.hostDetails(h, p.countPlacementsByHostType()), nil
}

func (p *Provider) ClearHostDraining(ctx context.Context, hostID string, rollbackToken string) (bool, error) {
	// writeMu orders the change with every other writer, including MarkHostDraining
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// Read the host, which must be live
	p.Mu.RLock()
	h, ok := p.Hosts[hostID]
	switch {
	case !ok || !p.IsHostHealthy(h):
		p.Mu.RUnlock()
		return false, components.ErrHostUnregistered
	case !h.Draining:
		p.Mu.RUnlock()
		return true, nil
	case rollbackToken == "" || h.DrainToken != rollbackToken:
		p.Mu.RUnlock()
		return false, nil
	}

	updatedHost := h.Clone()
	p.Mu.RUnlock()

	// The flag is persisted before it is applied, like every other change
	updatedHost.Draining = false
	updatedHost.DrainToken = ""

	changes := NewChanges()
	defer changes.Release()

	changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: hostID, Value: updatedHost})

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Hosts[hostID] = updatedHost
	})

	return err == nil, err
}

func (p *Provider) MarkHostDraining(ctx context.Context, req components.MarkHostDrainingReq) (components.MarkHostDrainingRes, error) {
	// writeMu keeps every other writer out until the flag is applied, so neither another drain nor an exclusive-access lease can change things between the checks and the update
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	var res components.MarkHostDrainingRes

	// Nothing is changed while an exclusive-access lease is held
	err := p.checkClusterNotLocked()
	if err != nil {
		return res, err
	}

	// Read the host, and the other live, non-draining servers of its types
	p.Mu.RLock()
	h, ok := p.Hosts[req.HostID]
	if !ok || !p.IsHostHealthy(h) {
		p.Mu.RUnlock()
		return res, components.ErrHostUnregistered
	}
	res.AlreadyDraining = h.Draining
	served := make(map[string]struct{})
	for id, other := range p.Hosts {
		if id == req.HostID || other.Draining || !p.IsHostHealthy(other) {
			continue
		}

		for _, hat := range p.HostActorTypes[id] {
			served[hat.ActorType] = struct{}{}
		}
	}

	for _, hat := range p.HostActorTypes[req.HostID] {
		_, ok = served[hat.ActorType]
		if !ok {
			res.LastServerOf = append(res.LastServerOf, hat.ActorType)
		}
	}

	updatedHost := h.Clone()
	p.Mu.RUnlock()
	slices.Sort(res.LastServerOf)

	if res.AlreadyDraining {
		res.LastServerOf = nil
	}

	// Leave the host alone when it is the last server of some type and the drain is not forced
	if res.Refused(req) {
		return res, nil
	}

	// The flag is persisted before it is applied, like every other change
	updatedHost.Draining = true
	updatedHost.DrainToken = ""
	if !res.AlreadyDraining {
		res.RollbackToken = uuid.NewV7().String()
		updatedHost.DrainToken = res.RollbackToken
	}

	changes := NewChanges()
	defer changes.Release()

	changes.Hosts.Set = append(changes.Hosts.Set, HostChange{Key: req.HostID, Value: updatedHost})

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Hosts[req.HostID] = updatedHost
	})
	if err != nil {
		return components.MarkHostDrainingRes{}, err
	}

	return res, nil
}

// countPlacementsByHostType counts the active actors of each host and actor type
// Must be called while holding at least a read lock on Mu
func (p *Provider) countPlacementsByHostType() map[HostActorTypeKey]int {
	counts := make(map[HostActorTypeKey]int)
	for _, actor := range p.ActiveActors {
		counts[HostActorTypeKey{HostID: actor.HostID, ActorType: actor.ActorType}]++
	}
	return counts
}

// hostDetails builds the details of a host, with its actor types ordered by type
// Must be called while holding at least a read lock on Mu
func (p *Provider) hostDetails(h *Host, counts map[HostActorTypeKey]int) components.HostDetails {
	hats := p.HostActorTypes[h.ID]
	res := components.HostDetails{
		HostID:          h.ID,
		Address:         h.Address,
		LastHealthCheck: h.LastHealthCheck,
		SessionID:       h.SessionID,
		RuntimeID:       h.RuntimeID,
		Draining:        h.Draining,
		ActorTypes:      make([]components.HostActorTypeDetails, len(hats)),
	}
	for i, hat := range hats {
		res.ActorTypes[i] = components.HostActorTypeDetails{
			ActorType:                hat.ActorType,
			IdleTimeout:              hat.IdleTimeout,
			ConcurrencyLimit:         hat.ConcurrencyLimit,
			ActiveCount:              counts[HostActorTypeKey{HostID: h.ID, ActorType: hat.ActorType}],
			CompletedJobRetention:    hat.CompletedJobRetention,
			DeadLetteredJobRetention: hat.DeadLetteredJobRetention,
		}
	}

	// The types are stored in registration order, so the order the API promises has to be established here
	slices.SortFunc(res.ActorTypes, func(a, b components.HostActorTypeDetails) int {
		return cmp.Compare(a.ActorType, b.ActorType)
	})

	return res
}

func (p *Provider) ListPlacements(_ context.Context, req components.ListPlacementsReq) (components.ListPlacementsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Keep the first placements that match the filters and sort after the cursor, plus one that tells whether more follow
	// Placements on a host whose registration expired are skipped, since they are about to be garbage collected
	sel := newPageSelector(limit+1, func(a, b components.PlacementInfo) int {
		return cmp.Or(cmp.Compare(a.ActorType, b.ActorType), cmp.Compare(a.ActorID, b.ActorID))
	})

	for _, actor := range p.ActiveActors {
		switch {
		case req.HostID != "" && actor.HostID != req.HostID,
			req.ActorType != "" && actor.ActorType != req.ActorType,
			compareActorRef(actor.ActorType, actor.ActorID, req.After) <= 0:
			continue
		}

		h, ok := p.Hosts[actor.HostID]
		if !ok || !p.IsHostHealthy(h) {
			continue
		}

		sel.Add(components.PlacementInfo{
			ActorType:   actor.ActorType,
			ActorID:     actor.ActorID,
			HostID:      actor.HostID,
			IdleTimeout: actor.IdleTimeout,
		})
	}

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	placements := sel.Sorted()
	hasMore := len(placements) > limit
	if hasMore {
		placements = placements[:limit]
	}

	return components.ListPlacementsRes{
		Placements: placements,
		HasMore:    hasMore,
	}, nil
}

// compareActorRef compares an actor type and ID with a cursor, ordering by type and then ID
func compareActorRef(actorType string, actorID string, cursor ref.ActorRef) int {
	return cmp.Or(
		cmp.Compare(actorType, cursor.ActorType),
		cmp.Compare(actorID, cursor.ActorID),
	)
}

func (p *Provider) QueryJobs(_ context.Context, req components.QueryJobsReq) (components.QueryJobsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	now := p.Clock.Now()

	// The actor ID is only a filter together with the actor type
	matchesActor := func(actorType string, actorID string) bool {
		if req.ActorType == "" {
			return true
		}
		return actorType == req.ActorType && (req.ActorID == "" || actorID == req.ActorID)
	}

	after := req.After.String()

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Keep the first jobs that match, plus one that tells whether more follow
	// Live and terminal job IDs never overlap, so ordering by job ID alone gives a stable union
	sel := newPageSelector(limit+1, func(a, b components.JobInfo) int {
		return cmp.Compare(a.JobID, b.JobID)
	})

	// Live jobs, skipped when the status filter can't match one
	if req.IncludeLive() {
		for _, a := range p.AlarmsByID {
			if a.Kind != string(components.AlarmKindJob) || a.ID <= after || !matchesActor(a.ActorType, a.ActorID) {
				continue
			}

			info := liveJobToInfo(a, now)
			if req.Status != "" && info.Status != req.Status {
				continue
			}
			sel.Add(info)
		}
	}

	// Terminal jobs, skipped when the status filter can't match one
	// An expired record is treated as gone before the collector gets to it, as in ListJobs
	if req.IncludeTerminal() {
		for _, d := range p.TerminalJobs {
			if d.JobID <= after || d.HasExpired(now) || !matchesActor(d.ActorType, d.ActorID) {
				continue
			}
			if req.Status != "" && components.JobStatus(d.Status) != req.Status {
				continue
			}
			sel.Add(terminalJobToInfo(d))
		}
	}

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	jobs := sel.Sorted()
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}

	return components.QueryJobsRes{
		Jobs:    jobs,
		HasMore: hasMore,
	}, nil
}

func (p *Provider) CountJobs(_ context.Context, req components.CountJobsReq) (int, error) {
	if req.Limit <= 0 {
		return 0, nil
	}
	now := p.Clock.Now()

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Count without collecting or sorting the jobs, stopping as soon as the limit is reached
	var count int

	// Live jobs, skipped when the status filter can't match one
	if req.IncludeLive() {
		for _, a := range p.AlarmsByID {
			if a.Kind != string(components.AlarmKindJob) {
				continue
			}

			status := components.JobStatusPending
			if a.LeaseValid(now) {
				status = components.JobStatusActive
			}
			if req.Status != "" && status != req.Status {
				continue
			}

			count++
			if count >= req.Limit {
				return req.Limit, nil
			}
		}
	}

	// Terminal jobs, skipped when the status filter can't match one
	// An expired record is treated as gone before the collector gets to it, as in QueryJobs
	if req.IncludeTerminal() {
		for _, d := range p.TerminalJobs {
			if d.HasExpired(now) || (req.Status != "" && components.JobStatus(d.Status) != req.Status) {
				continue
			}

			count++
			if count >= req.Limit {
				return req.Limit, nil
			}
		}
	}

	return count, nil
}

func (p *Provider) ListAlarms(_ context.Context, req components.ListAlarmsReq) (components.ListAlarmsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	now := p.Clock.Now()

	p.Mu.RLock()
	defer p.Mu.RUnlock()

	// Keep the first plain alarms that match the filters and sort after the cursor, leaving jobs out, plus one that tells whether more follow
	sel := newPageSelector(limit+1, func(a, b *Alarm) int {
		return compareAlarmKey(a.GetAlarmKey(), ref.AlarmRef{ActorType: b.ActorType, ActorID: b.ActorID, Name: b.Name})
	})

	for key, a := range p.Alarms {
		switch {
		case a.Kind != "" && a.Kind != string(components.AlarmKindAlarm),
			req.ActorType != "" && a.ActorType != req.ActorType,
			req.ActorType != "" && req.ActorID != "" && a.ActorID != req.ActorID,
			compareAlarmKey(key, req.After) <= 0:
			continue
		}
		sel.Add(a)
	}

	// Anything past the limit is dropped from the page, but its existence is reported through HasMore
	alarms := sel.Sorted()
	hasMore := len(alarms) > limit
	if hasMore {
		alarms = alarms[:limit]
	}

	res := components.ListAlarmsRes{
		Alarms:  make([]components.AlarmInfo, len(alarms)),
		HasMore: hasMore,
	}
	for i, a := range alarms {
		info := components.AlarmInfo{
			AlarmID:   a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueTime:   a.DueTime,
			Interval:  a.Interval,
		}
		if a.TTL != nil {
			info.TTL = new(*a.TTL)
		}

		// Alarm leases record no owner, so the lease holder is the host the actor is placed on
		// A placement on an expired host is about to be garbage collected, so that host is not reported
		if a.LeaseValid(now) {
			info.LeaseExpiration = new(*a.LeaseExpiration)
			info.LeaseHostID = p.liveHostOf(a.GetActorKey())
		}

		res.Alarms[i] = info
	}

	return res, nil
}

// liveHostOf returns the host the actor is placed on, or an empty string when the actor has no placement or its host expired
// Must be called while holding at least a read lock on Mu
func (p *Provider) liveHostOf(key ActorKey) string {
	actor, ok := p.ActiveActors[key]
	if !ok {
		return ""
	}
	h, ok := p.Hosts[actor.HostID]
	if !ok || !p.IsHostHealthy(h) {
		return ""
	}
	return actor.HostID
}

// compareAlarmKey compares an alarm key with a cursor, ordering by actor type, actor ID and alarm name
func compareAlarmKey(key AlarmKey, cursor ref.AlarmRef) int {
	return cmp.Or(
		cmp.Compare(key.ActorType, cursor.ActorType),
		cmp.Compare(key.ActorID, cursor.ActorID),
		cmp.Compare(key.Name, cursor.Name),
	)
}

// RegisterRuntime records or renews the membership of a runtime replica, extending its lease to now+ttl
// It returns components.ErrRuntimeIDInUse if another address holds a live lease for the same runtime ID
func (p *Provider) RegisterRuntime(_ context.Context, req components.RegisterRuntimeReq) error {
	now := p.Clock.Now()

	p.runtimesMu.Lock()
	defer p.runtimesMu.Unlock()

	// A live record at a different address belongs to another replica using the same ID
	existing, ok := p.runtimes[req.RuntimeID]
	if ok && existing.Address != req.Address && existing.ExpiresAt.After(now) {
		return components.ErrRuntimeIDInUse
	}

	p.runtimes[req.RuntimeID] = components.RuntimeInfo{
		RuntimeID:     req.RuntimeID,
		Address:       req.Address,
		LastHeartbeat: now,
		ExpiresAt:     now.Add(req.TTL),
	}
	return nil
}

// UnregisterRuntime removes the membership record of a runtime replica, only while it is still held by the given address
func (p *Provider) UnregisterRuntime(_ context.Context, runtimeID string, address string) error {
	p.runtimesMu.Lock()
	defer p.runtimesMu.Unlock()

	// A record taken over by another address is left alone
	existing, ok := p.runtimes[runtimeID]
	if ok && existing.Address == address {
		delete(p.runtimes, runtimeID)
	}
	return nil
}

// ListRuntimes returns the runtime replicas with a live membership lease, ordered by runtime ID
func (p *Provider) ListRuntimes(_ context.Context) ([]components.RuntimeInfo, error) {
	now := p.Clock.Now()

	p.runtimesMu.Lock()
	defer p.runtimesMu.Unlock()

	// Expired records are skipped even before the cleanup loop removes them
	res := make([]components.RuntimeInfo, 0, len(p.runtimes))
	for _, rt := range p.runtimes {
		if !rt.ExpiresAt.After(now) {
			continue
		}
		res = append(res, rt)
	}

	slices.SortFunc(res, func(a, b components.RuntimeInfo) int {
		return cmp.Compare(a.RuntimeID, b.RuntimeID)
	})
	return res, nil
}

// cleanupExpiredRuntimes removes the runtime memberships whose lease expired
func (p *Provider) cleanupExpiredRuntimes() {
	now := p.Clock.Now()

	p.runtimesMu.Lock()
	defer p.runtimesMu.Unlock()

	for id, rt := range p.runtimes {
		if !rt.ExpiresAt.After(now) {
			delete(p.runtimes, id)
		}
	}
}
