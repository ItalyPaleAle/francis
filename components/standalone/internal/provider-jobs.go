package internal

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *Provider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, error) {
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
		return existingID, nil
	}

	// Normalize empty data to nil
	data := req.Data
	if data != nil && len(data) == 0 {
		data = nil
	}

	alarmIDObj, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	alarmID := alarmIDObj.String()

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

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		p.Alarms[key] = a
		p.AlarmsByID[alarmID] = a
	})
	if err != nil {
		return "", err
	}

	return alarmID, nil
}

func (p *Provider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	now := p.Clock.Now()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[lease.Key()]
	valid := ok && a.HasValidLease(lease.LeaseID(), now)
	var (
		alarmKey AlarmKey
		oldID    string
		deadJob  *DeadJob
		newAlarm *Alarm
	)
	if valid {
		alarmKey = a.GetAlarmKey()
		oldID = a.ID
		deadJob = &DeadJob{
			JobID:       oldID,
			ActorType:   a.ActorType,
			ActorID:     a.ActorID,
			Method:      a.JobMethod,
			Data:        a.Data,
			Attempts:    req.Attempts,
			LastError:   req.Reason,
			FailedAt:    now,
			OriginalDue: a.DueTime,
			Interval:    a.Interval,
			Cron:        a.Cron,
		}

		// Re-create the recurrence (same name, fresh ID) so a repeating job survives the dead-lettering of one occurrence
		if req.Reschedule {
			newIDObj, genErr := uuid.NewV7()
			if genErr != nil {
				p.Mu.RUnlock()
				return genErr
			}
			newAlarm = a.Clone()
			newAlarm.ID = newIDObj.String()
			newAlarm.DueTime = req.NextDueTime
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
	changes.DeadJobs.Set = append(changes.DeadJobs.Set, DeadJobChange{Key: oldID, Value: deadJob})
	if newAlarm != nil {
		changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: newAlarm.ID, Value: newAlarm})
	}

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Alarms, alarmKey)
		delete(p.AlarmsByID, oldID)

		p.DeadJobs[oldID] = deadJob

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

	// Then look for a dead-lettered job
	d, ok := p.DeadJobs[jobID]
	if !ok {
		return components.JobInfo{}, components.ErrNoJob
	}

	return deadJobToInfo(d), nil
}

func (p *Provider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	now := p.Clock.Now()

	// Allocate with enough capacity for at least all the dead jobs
	res := make([]components.JobInfo, 0, len(p.DeadJobs)+1)

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

	// Dead-lettered jobs
	for _, d := range p.DeadJobs {
		if d.ActorType != actorType || d.ActorID != actorID {
			continue
		}

		res = append(res, deadJobToInfo(d))
	}

	return res, nil
}

func (p *Provider) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	a, ok := p.AlarmsByID[jobID]
	canCancel := ok && a.Kind == string(components.AlarmKindJob) && a.ActorType == actorType && a.ActorID == actorID
	var alarmKey AlarmKey
	if canCancel {
		alarmKey = a.GetAlarmKey()
	}
	p.Mu.RUnlock()

	if !canCancel {
		return components.ErrNoJob
	}

	changes := NewChanges()
	defer changes.Release()
	changes.Alarms.Delete = append(changes.Alarms.Delete, jobID)

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.Alarms, alarmKey)
		delete(p.AlarmsByID, jobID)
	})
}

func (p *Provider) GetDeadJob(ctx context.Context, jobID string) (components.GetDeadJobRes, error) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	d, ok := p.DeadJobs[jobID]
	if !ok {
		return components.GetDeadJobRes{}, components.ErrNoJob
	}

	res := components.GetDeadJobRes{
		JobID:       d.JobID,
		ActorType:   d.ActorType,
		ActorID:     d.ActorID,
		Method:      d.Method,
		Attempts:    d.Attempts,
		LastError:   d.LastError,
		FailedAt:    d.FailedAt,
		OriginalDue: d.OriginalDue,
		Interval:    d.Interval,
		Cron:        d.Cron,
	}
	if len(d.Data) > 0 {
		res.Data = make([]byte, len(d.Data))
		copy(res.Data, d.Data)
	}
	return res, nil
}

func (p *Provider) DeleteDeadJob(ctx context.Context, jobID string) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.Mu.RLock()
	_, ok := p.DeadJobs[jobID]
	p.Mu.RUnlock()

	if !ok {
		return components.ErrNoJob
	}

	changes := NewChanges()
	defer changes.Release()
	changes.DeadJobs.Delete = append(changes.DeadJobs.Delete, jobID)

	return p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.DeadJobs, jobID)
	})
}

func (p *Provider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	// Read the dead job's fields needed to re-dispatch it
	p.Mu.RLock()
	d, ok := p.DeadJobs[jobID]
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

	newIDObj, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	newID := newIDObj.String()

	// Re-dispatch as a fresh, immediate one-shot job with the same method and data, under a new random name
	newAlarm := &Alarm{
		ID:        newID,
		ActorType: actorType,
		ActorID:   actorID,
		Name:      uuid.NewString(),
		DueTime:   p.Clock.Now(),
		Kind:      string(components.AlarmKindJob),
		JobMethod: method,
		Data:      data,
	}
	key := newAlarm.GetAlarmKey()

	// Remove the dead-letter record and add the new job in one change set, so the two are persisted atomically
	changes := NewChanges()
	defer changes.Release()
	changes.DeadJobs.Delete = append(changes.DeadJobs.Delete, jobID)
	changes.Alarms.Set = append(changes.Alarms.Set, AlarmChange{Key: newID, Value: newAlarm})

	err = p.persistThenApply(ctx, &p.Mu, changes, func() {
		delete(p.DeadJobs, jobID)
		p.Alarms[key] = newAlarm
		p.AlarmsByID[newID] = newAlarm
	})
	if err != nil {
		return "", err
	}

	return newID, nil
}

// deadJobToInfo maps a stored dead job to the public JobInfo, deriving the creation time from the job ID.
func deadJobToInfo(d *DeadJob) components.JobInfo {
	return components.JobInfo{
		JobID:     d.JobID,
		ActorType: d.ActorType,
		ActorID:   d.ActorID,
		Method:    d.Method,
		Status:    components.JobStatusDeadLettered,
		DueTime:   d.OriginalDue,
		Interval:  d.Interval,
		Cron:      d.Cron,
		Attempts:  d.Attempts,
		LastError: d.LastError,
		CreatedAt: jobCreatedAtOrFailed(d),
	}
}

// jobCreatedAtOrFailed derives the creation time from the job ID, falling back to the failure time if the ID is not a parseable UUIDv7.
func jobCreatedAtOrFailed(d *DeadJob) time.Time {
	t := components.JobCreatedAt(d.JobID)
	if t.IsZero() {
		return d.FailedAt
	}
	return t
}
