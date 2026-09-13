package internal

import (
	"context"
	"slices"
	"time"
	"uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, *ref.AlarmLease, error) {
	// Trying to acquire a lease requires using the slower path
	// Requests outside fetch-ahead stay on the storage-only path even when an idempotency conflict retains an earlier due time
	if len(req.LeaseImmediate) > 0 && !req.DueTime.After(p.Clock.Now().Add(p.Cfg.AlarmsFetchAheadInterval)) {
		return p.dispatchAndLeaseJob(ctx, aRef, req)
	}

	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// Idempotency: keep an existing job (or alarm) with the same key and return its ID
	p.Mu.RLock()
	existing, exists := p.Alarms[key]
	var existingID string
	if exists {
		existingID = existing.ID
	}
	p.Mu.RUnlock()

	if exists {
		return existingID, nil, nil
	}

	// Normalize empty data to nil
	data := req.Data
	if data != nil && len(data) == 0 {
		data = nil
	}

	alarmID := uuid.NewV7().String()

	a := &Alarm{
		ID:        alarmID,
		ActorType: aRef.ActorType,
		ActorID:   aRef.ActorID,
		Name:      aRef.Name,
		DueTime:   req.DueTime,
		Interval:  req.Interval,
		Cron:      req.Cron,
		Kind:      string(components.AlarmKindJob),
		JobMethod: req.JobMethod,
		TTL:       req.TTL,
		Data:      data,
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: alarmID, Value: a})

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = a
		p.AlarmsByID[alarmID] = a
	})
	if err != nil {
		return "", nil, err
	}

	return alarmID, nil, nil
}

// dispatchAndLeaseJob atomically persists a new idempotent job with any required actor placement and lease
func (p *Provider) dispatchAndLeaseJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, *ref.AlarmLease, error) {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	changes := NewChanges()
	defer changes.Release()

	// Preserve the first job stored for an idempotency key while allowing an unleased occurrence to become immediately schedulable
	now := p.Clock.Now()
	p.Mu.RLock()
	existing, exists := p.Alarms[key]
	var job *Alarm
	if exists {
		job = existing
		hasLiveLease := job.LeaseID != nil && job.LeaseExpiration != nil && !job.LeaseExpiration.Before(now)
		if job.DueTime.After(now.Add(p.Cfg.AlarmsFetchAheadInterval)) || hasLiveLease {
			jobID := job.ID
			p.Mu.RUnlock()
			return jobID, nil, nil
		}
	} else {
		data := req.Data
		if data != nil && len(data) == 0 {
			data = nil
		}
		job = &Alarm{
			ID:        uuid.NewV7().String(),
			ActorType: aRef.ActorType,
			ActorID:   aRef.ActorID,
			Name:      aRef.Name,
			DueTime:   req.DueTime,
			Interval:  req.Interval,
			Cron:      req.Cron,
			TTL:       req.TTL,
			Data:      data,
			Kind:      string(components.AlarmKindJob),
			JobMethod: req.JobMethod,
		}
	}

	// Resolve placement only when the stored occurrence is inside fetch-ahead
	actorKey := NewActorKey(aRef.ActorType, aRef.ActorID)
	var newActor *ActiveActor
	canLease := false
	if !job.DueTime.After(now.Add(p.Cfg.AlarmsFetchAheadInterval)) {
		existingActor, actorExists := p.ActiveActors[actorKey]
		if actorExists {
			host, hostExists := p.Hosts[existingActor.HostID]
			if hostExists && p.IsHostHealthy(host) {
				canLease = slices.Contains(req.LeaseImmediate, existingActor.HostID)
			} else {
				actorExists = false
			}
		}
		if !actorExists {
			host, idleTimeout := p.findHostWithCapacity(aRef.ActorType, req.LeaseImmediate)
			if host != nil {
				activation := now
				if job.DueTime.After(activation) {
					activation = job.DueTime
				}
				newActor = &ActiveActor{
					ActorType:   aRef.ActorType,
					ActorID:     aRef.ActorID,
					HostID:      host.ID,
					IdleTimeout: idleTimeout,
					Activation:  activation,
				}
				canLease = true
			}
		}
	}

	// Acquire the lease in the same change set as the job and any new placement
	var lease *ref.AlarmLease
	if canLease {
		job = job.Clone()
		leaseID := uuid.NewV7().String() + "_" + job.ID
		leaseExpiration := now.Add(p.Cfg.AlarmsLeaseDuration)
		job.LeaseID = &leaseID
		job.LeaseExpiration = &leaseExpiration
		lease = ref.NewAlarmLease(aRef, job.ID, job.DueTime, leaseID)
	}
	p.Mu.RUnlock()
	if exists && !canLease {
		return job.ID, nil, nil
	}

	// Expose the job and lease only after their complete durable change set succeeds
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: job.ID, Value: job})
	if newActor != nil {
		changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: actorKey, Value: newActor})
	}
	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = job
		p.AlarmsByID[job.ID] = job
		if newActor != nil {
			p.ActiveActors[actorKey] = newActor
		}
	})
	if err != nil {
		return "", nil, err
	}
	return job.ID, lease, nil
}

func (p *Provider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	return p.endJob(ctx, lease, endJobReq{
		status:      components.JobStatusDeadLettered,
		reason:      req.Reason,
		attempts:    req.Attempts,
		retention:   req.Retention,
		reschedule:  req.Reschedule,
		nextDueTime: req.NextDueTime,
	})
}

func (p *Provider) CompleteJob(ctx context.Context, lease *ref.AlarmLease, req components.CompleteJobReq) error {
	return p.endJob(ctx, lease, endJobReq{
		status:      components.JobStatusCompleted,
		attempts:    req.Attempts,
		retention:   req.Retention,
		reschedule:  req.Reschedule,
		nextDueTime: req.NextDueTime,
	})
}

// endJobReq is the shared shape of the two ways a job ends, since completing and dead-lettering differ only in what they record
type endJobReq struct {
	status      components.JobStatus
	reason      string
	attempts    int
	retention   time.Duration
	reschedule  bool
	nextDueTime time.Time
}

// endJob moves a leased job out of the alarms map and into the terminal-job store, optionally re-creating its recurrence in the same change set
func (p *Provider) endJob(ctx context.Context, lease *ref.AlarmLease, req endJobReq) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[lease.Key()]
	valid := ok && a.HasValidLease(lease.LeaseID(), now)
	var (
		alarmKey    AlarmKey
		oldID       string
		terminalJob *TerminalJob
		newAlarm    *Alarm
	)
	if valid {
		alarmKey = a.GetAlarmKey()
		oldID = a.ID
		// Only a dead job keeps its input, since that is what a replay needs and a completed one is never replayed
		// This is what keeps a wide fan-out's retained records cheap, where the payload is much larger than the metadata around it
		var data []byte
		if req.status == components.JobStatusDeadLettered {
			data = a.Data
		}

		terminalJob = &TerminalJob{
			JobID:       oldID,
			ActorType:   a.ActorType,
			ActorID:     a.ActorID,
			Method:      a.JobMethod,
			Data:        data,
			Status:      string(req.status),
			Attempts:    req.attempts,
			LastError:   req.reason,
			EndedAt:     now,
			OriginalDue: a.DueTime,
			Interval:    a.Interval,
			Cron:        a.Cron,
		}
		if req.retention > 0 {
			terminalJob.Expiration = new(now.Add(req.retention))
		}

		// Re-create the recurrence (same name, fresh ID) so a repeating job survives one occurrence ending
		if req.reschedule {
			newAlarm = a.Clone()
			newAlarm.ID = uuid.NewV7().String()
			newAlarm.DueTime = req.nextDueTime
			newAlarm.LeaseID = nil
			newAlarm.LeaseExpiration = nil
		}
	}
	p.Mu.RUnlock()

	if !valid {
		return components.ErrNoAlarm
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, oldID)
	changes.TerminalJobs.Set = append(changes.TerminalJobs.Set, TerminalJobChange{Key: oldID, Value: terminalJob})
	if newAlarm != nil {
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: newAlarm.ID, Value: newAlarm})
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Alarms, alarmKey)
		delete(p.AlarmsByID, oldID)

		p.TerminalJobs[oldID] = terminalJob

		if newAlarm != nil {
			p.Alarms[alarmKey] = newAlarm
			p.AlarmsByID[newAlarm.ID] = newAlarm
		}
	})
}

func (p *Provider) GetJob(ctx context.Context, jobID string) (components.JobInfo, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()

	// First look for a live job
	a, ok := p.AlarmsByID[jobID]
	if ok && a.Kind == string(components.AlarmKindJob) {
		status := components.JobStatusPending
		if a.LeaseID != nil && a.LeaseExpiration != nil && !a.LeaseExpiration.Before(now) {
			status = components.JobStatusActive
		}
		return components.JobInfo{
			JobID:     a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Method:    a.JobMethod,
			Status:    status,
			DueTime:   a.DueTime,
			Interval:  a.Interval,
			Cron:      a.Cron,
			CreatedAt: components.JobCreatedAt(jobID),
		}, nil
	}

	// Then look for a job that ended, whether it completed or dead-lettered
	// An expired record is treated as gone before the collector gets to it, exactly as expired state is
	d, ok := p.TerminalJobs[jobID]
	if !ok || d.HasExpired(now) {
		return components.JobInfo{}, components.ErrNoJob
	}

	return terminalJobToInfo(d), nil
}

func (p *Provider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()

	// Allocate with enough capacity for at least all the terminal jobs
	res := make([]components.JobInfo, 0, len(p.TerminalJobs)+1)

	// Live jobs
	for _, a := range p.Alarms {
		if a.Kind != string(components.AlarmKindJob) || a.ActorType != actorType || a.ActorID != actorID {
			continue
		}
		status := components.JobStatusPending
		if a.LeaseID != nil && a.LeaseExpiration != nil && !a.LeaseExpiration.Before(now) {
			status = components.JobStatusActive
		}

		res = append(res, components.JobInfo{
			JobID:     a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Method:    a.JobMethod,
			Status:    status,
			DueTime:   a.DueTime,
			Interval:  a.Interval,
			Cron:      a.Cron,
			CreatedAt: components.JobCreatedAt(a.ID),
		})
	}

	// Jobs that ended, whether they completed or dead-lettered
	for _, d := range p.TerminalJobs {
		if d.ActorType != actorType || d.ActorID != actorID || d.HasExpired(now) {
			continue
		}

		res = append(res, terminalJobToInfo(d))
	}

	return res, nil
}

func (p *Provider) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// A job lives in one of two maps depending on whether it has ended, and the caller does not have to know which
	// The actor scope is optional: with both parts empty the job is removed by ID alone, which is what an operator holding a job ID does
	inScope := func(at string, ai string) bool {
		return actorType == "" || actorID == "" || (at == actorType && ai == actorID)
	}

	p.Mu.RLock()
	a, live := p.AlarmsByID[jobID]
	live = live && a.Kind == string(components.AlarmKindJob) && inScope(a.ActorType, a.ActorID)
	var alarmKey AlarmKey
	if live {
		alarmKey = a.GetAlarmKey()
	}
	d, terminal := p.TerminalJobs[jobID]
	terminal = terminal && inScope(d.ActorType, d.ActorID)
	p.Mu.RUnlock()

	if !live && !terminal {
		return components.ErrNoJob
	}

	changes := NewChanges()
	defer changes.Release()
	if live {
		changes.Alarms.Delete = append(changes.Alarms.Delete, jobID)
	}
	if terminal {
		changes.TerminalJobs.Delete = append(changes.TerminalJobs.Delete, jobID)
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		if live {
			delete(p.Alarms, alarmKey)
			delete(p.AlarmsByID, jobID)
		}
		if terminal {
			delete(p.TerminalJobs, jobID)
		}
	})
}

func (p *Provider) GetTerminalJob(ctx context.Context, jobID string) (components.GetTerminalJobRes, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	d, ok := p.TerminalJobs[jobID]
	if !ok || d.HasExpired(p.Clock.Now()) {
		return components.GetTerminalJobRes{}, components.ErrNoJob
	}

	res := components.GetTerminalJobRes{
		JobID:       d.JobID,
		ActorType:   d.ActorType,
		ActorID:     d.ActorID,
		Method:      d.Method,
		Status:      components.JobStatus(d.Status),
		Attempts:    d.Attempts,
		LastError:   d.LastError,
		EndedAt:     d.EndedAt,
		OriginalDue: d.OriginalDue,
		Interval:    d.Interval,
		Cron:        d.Cron,
		Expiration:  d.Expiration,
	}
	if len(d.Data) > 0 {
		res.Data = make([]byte, len(d.Data))
		copy(res.Data, d.Data)
	}
	return res, nil
}

func (p *Provider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// Read the dead job's fields needed to re-dispatch it
	// A job that ended by completing has nothing to retry, so only a dead-lettered one is taken
	p.Mu.RLock()
	d, ok := p.TerminalJobs[jobID]
	ok = ok && d.Status == string(components.JobStatusDeadLettered) && !d.HasExpired(p.Clock.Now())
	var (
		actorType, actorID, method string
		data                       []byte
	)
	if ok {
		actorType = d.ActorType
		actorID = d.ActorID
		method = d.Method
		if len(d.Data) > 0 {
			data = make([]byte, len(d.Data))
			copy(data, d.Data)
		}
	}
	p.Mu.RUnlock()

	if !ok {
		return "", components.ErrNoJob
	}

	newID := uuid.NewV7().String()

	// Re-dispatch as a fresh, immediate one-shot job with the same method and data, under a new random name
	newAlarm := &Alarm{
		ID:        newID,
		ActorType: actorType,
		ActorID:   actorID,
		Name:      uuid.NewV4().String(),
		DueTime:   p.Clock.Now(),
		Kind:      string(components.AlarmKindJob),
		JobMethod: method,
		Data:      data,
	}
	key := newAlarm.GetAlarmKey()

	// Remove the dead-letter record and add the new job in one change set, so the two are persisted atomically
	changes := NewChanges()
	defer changes.Release()
	changes.TerminalJobs.Delete = append(changes.TerminalJobs.Delete, jobID)
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: newID, Value: newAlarm})

	err := p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.TerminalJobs, jobID)
		p.Alarms[key] = newAlarm
		p.AlarmsByID[newID] = newAlarm
	})
	if err != nil {
		return "", err
	}

	return newID, nil
}

// terminalJobToInfo maps a stored terminal job to the public JobInfo, deriving the creation time from the job ID.
func terminalJobToInfo(d *TerminalJob) components.JobInfo {
	return components.JobInfo{
		JobID:     d.JobID,
		ActorType: d.ActorType,
		ActorID:   d.ActorID,
		Method:    d.Method,
		Status:    components.JobStatus(d.Status),
		DueTime:   d.OriginalDue,
		Interval:  d.Interval,
		Cron:      d.Cron,
		Attempts:  d.Attempts,
		LastError: d.LastError,
		CreatedAt: jobCreatedAtOrEnded(d),
		EndedAt:   d.EndedAt,
	}
}

// jobCreatedAtOrEnded derives the creation time from the job ID, falling back to the time the job ended if the ID is not a parseable UUIDv7.
func jobCreatedAtOrEnded(d *TerminalJob) time.Time {
	t := components.JobCreatedAt(d.JobID)
	if t.IsZero() {
		return d.EndedAt
	}
	return t
}
