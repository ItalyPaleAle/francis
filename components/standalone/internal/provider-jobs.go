package internal

import (
	"context"
	"fmt"
	"slices"
	"time"
	"uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, bool, *ref.AlarmLease, error) {
	// Trying to acquire a lease requires using the slower path
	// Requests outside fetch-ahead stay on the storage-only path even when an idempotency conflict retains an earlier due time
	if len(req.LeaseImmediate) > 0 && !req.DueTime.After(p.Clock.Now().Add(p.Cfg.AlarmsFetchAheadInterval)) {
		return p.dispatchAndLeaseJob(ctx, aRef, req)
	}

	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// The lease can't be taken while writeMu is held, so this check is atomic with the insert
	if req.RejectIfClusterLocked {
		err := p.checkClusterNotLocked()
		if err != nil {
			return "", false, nil, err
		}
	}

	// The state domain is locked only when the job carries an initial state, in the same order Restore uses
	if req.InitialState != nil {
		p.stateWriteMu.Lock()
		defer p.stateWriteMu.Unlock()
	}
	stateChange := p.initialStateChange(aRef.ActorRef(), req.InitialState)

	changes := NewChanges()
	defer changes.Release()
	if stateChange != nil {
		changes.ActorState.Set = append(changes.ActorState.Set, *stateChange)
	}

	// Idempotency: keep an existing job (or alarm) with the same key and return its ID
	p.Mu.RLock()
	existing, exists := p.Alarms[key]
	var existingID string
	if exists {
		existingID = existing.ID
	}
	p.Mu.RUnlock()

	if exists {
		// The job that already holds this idempotency key is the one the caller gets back, and this call did not create it
		// Its initial state is still stored, since the actor may have none even though its job is live
		if stateChange != nil {
			err := p.persistThenApplyWithState(ctx, changes, stateChange, func() {})
			if err != nil {
				return "", false, nil, err
			}
		}
		return existingID, false, nil, nil
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

	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: alarmID, Value: a})

	err := p.persistThenApplyWithState(ctx, changes, stateChange, func() {
		p.Alarms[key] = a
		p.AlarmsByID[alarmID] = a
	})
	if err != nil {
		return "", false, nil, err
	}

	return alarmID, true, nil, nil
}

// dispatchAndLeaseJob atomically persists a new idempotent job with any required actor placement and lease
func (p *Provider) dispatchAndLeaseJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, bool, *ref.AlarmLease, error) {
	key := NewAlarmKey(aRef.ActorType, aRef.ActorID, aRef.Name)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// The lease can't be taken while writeMu is held, so this check is atomic with the insert
	if req.RejectIfClusterLocked {
		err := p.checkClusterNotLocked()
		if err != nil {
			return "", false, nil, err
		}
	}

	// The state domain is locked only when the job carries an initial state, in the same order Restore uses
	if req.InitialState != nil {
		p.stateWriteMu.Lock()
		defer p.stateWriteMu.Unlock()
	}
	stateChange := p.initialStateChange(aRef.ActorRef(), req.InitialState)

	changes := NewChanges()
	defer changes.Release()
	if stateChange != nil {
		changes.ActorState.Set = append(changes.ActorState.Set, *stateChange)
	}

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
			if stateChange != nil {
				err := p.persistThenApplyWithState(ctx, changes, stateChange, func() {})
				if err != nil {
					return "", false, nil, err
				}
			}
			return jobID, false, nil, nil
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
		if stateChange != nil {
			err := p.persistThenApplyWithState(ctx, changes, stateChange, func() {})
			if err != nil {
				return "", false, nil, err
			}
		}
		return job.ID, false, nil, nil
	}

	// Expose the job and lease only after their complete durable change set succeeds
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: job.ID, Value: job})
	if newActor != nil {
		changes.ActiveActors.Set = append(changes.ActiveActors.Set, ActiveActorChange{Key: actorKey, Value: newActor})
	}
	err := p.persistThenApplyWithState(ctx, changes, stateChange, func() {
		p.Alarms[key] = job
		p.AlarmsByID[job.ID] = job
		if newActor != nil {
			p.ActiveActors[actorKey] = newActor
		}
	})
	if err != nil {
		return "", false, nil, err
	}
	return job.ID, !exists, lease, nil
}

// initialStateChange returns the change that stores a job's initial state, or nil when there is none or the actor already has live state
// The caller must hold stateWriteMu
func (p *Provider) initialStateChange(r ref.ActorRef, initial *components.InitialState) *ActorStateChange {
	if initial == nil {
		return nil
	}

	key := NewActorKey(r.ActorType, r.ActorID)
	p.StateMu.RLock()
	current, ok := p.ActorState[key]
	live := ok && !current.IsExpired(p.Clock.Now())
	p.StateMu.RUnlock()
	if live {
		return nil
	}

	entry := &StateEntry{Data: initial.Data}
	if initial.WorkflowLabels != nil && !initial.WorkflowLabels.IsZero() {
		entry.WorkflowLabels = new(*initial.WorkflowLabels)
	}
	return &ActorStateChange{Key: key, Value: entry}
}

// persistThenApplyWithState persists a job's change set, then applies it under Mu and, when it stores an initial state, under StateMu too
// The caller must hold writeMu, and stateWriteMu when stateChange is set
func (p *Provider) persistThenApplyWithState(ctx context.Context, changes *Changes, stateChange *ActorStateChange, apply func()) error {
	if stateChange == nil {
		return p.persistThenApply(ctx, &p.Mu, changes, apply)
	}

	err := p.PersistHook.PersistChanges(ctx, changes)
	if err != nil {
		return fmt.Errorf("error persisting changes: %w", err)
	}

	p.Mu.Lock()
	apply()
	p.Mu.Unlock()

	p.StateMu.Lock()
	p.ActorState[stateChange.Key] = stateChange.Value
	p.StateMu.Unlock()

	return nil
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
	valid := ok && a.CanFinalize(lease.LeaseID(), lease.DueTime(), now)
	var (
		alarmKey    AlarmKey
		jobID       string
		terminalJob *TerminalJob
		newAlarm    *Alarm
	)
	if valid {
		alarmKey = a.GetAlarmKey()
		jobID = a.ID

		// A repeating job keeps one identity for the life of its schedule, so the occurrence that just ended is recorded under an ID of its own and the job ID goes back to the recurrence
		// Callers hold that ID for as long as the schedule exists: a cron actor deletes and reconciles its recurrence by it, and re-minting it on every occurrence would orphan the schedule
		// A one-shot job has no recurrence to carry it, so its record keeps the ID the caller already knows
		occurrenceID := jobID
		if req.reschedule {
			occurrenceID = uuid.NewV7().String()
		}

		// Only a dead job keeps its input, since that is what a replay needs and a completed one is never replayed
		// A wide fan-out's retained records stay small this way, since the payload dwarfs the metadata around it
		var data []byte
		if req.status == components.JobStatusDeadLettered {
			data = a.Data
		}

		terminalJob = &TerminalJob{
			JobID:       occurrenceID,
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

		// Carry the recurrence forward to its next occurrence, keeping the row it already had, so a repeating job survives one occurrence ending
		if req.reschedule {
			newAlarm = a.Clone()
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
	changes.TerminalJobs.Set = append(changes.TerminalJobs.Set, TerminalJobChange{Key: terminalJob.JobID, Value: terminalJob})
	if newAlarm != nil {
		// The recurrence keeps its row, so there is nothing to delete: the write replaces it in place
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: newAlarm.ID, Value: newAlarm})
	} else {
		changes.Alarms.Delete = append(changes.Alarms.Delete, jobID)
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.TerminalJobs[terminalJob.JobID] = terminalJob

		if newAlarm != nil {
			p.Alarms[alarmKey] = newAlarm
			p.AlarmsByID[newAlarm.ID] = newAlarm
			return
		}

		delete(p.Alarms, alarmKey)
		delete(p.AlarmsByID, jobID)
	})
}

func (p *Provider) GetJob(ctx context.Context, jobID string) (components.JobInfo, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()

	// First look for a live job
	a, ok := p.AlarmsByID[jobID]
	if ok && a.Kind == string(components.AlarmKindJob) {
		return liveJobToInfo(a, now), nil
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
		res = append(res, liveJobToInfo(a, now))
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

func (p *Provider) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string, req components.DeleteJobReq) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// A job lives in one of two maps depending on whether it has ended, and the caller does not have to know which
	inScope := func(at string, ai string) bool {
		return at == actorType && ai == actorID
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

	// A cancellation only removes the live row, and the write lock this call holds is what makes that atomic against a concurrent finalization
	if req.LiveOnly {
		terminal = false
	}

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

// liveJobToInfo maps a live job to the public JobInfo, deriving its status from the lease as ListJobs does
func liveJobToInfo(a *Alarm, now time.Time) components.JobInfo {
	status := components.JobStatusPending
	if a.LeaseValid(now) {
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
		CreatedAt: components.JobCreatedAt(a.ID),
	}
}
