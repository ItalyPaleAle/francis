package internal

import (
	"context"
	"fmt"
	"io"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
)

// restoreBatchSize is the number of records applied per persist-then-apply flush during a restore
const restoreBatchSize = 1000

// Backup writes a snapshot of all persistent data to w
// It can run while the cluster is online: each domain is snapshotted under its read lock, then streamed once the lock is released, so writers are only blocked briefly and never during the stream I/O
func (p *Provider) Backup(ctx context.Context, w io.Writer) error {
	// Snapshot each domain under its read lock
	// Records placed in the maps are treated as immutable (mutations replace the entry rather than editing it in place), so the collected records stay valid after the lock is released
	stateRecs := p.snapshotStateRecords()
	alarmRecs, deadJobRecs := p.snapshotAlarmAndDeadJobRecords()

	// Write the header, which records the format version
	bw, err := backup.NewWriter(w, p.Clock.Now())
	if err != nil {
		return err
	}

	// Stream state, then alarms, then dead jobs, matching the format's section ordering
	for _, rec := range stateRecs {
		err = bw.WriteState(rec)
		if err != nil {
			return err
		}
	}
	for _, rec := range alarmRecs {
		err = bw.WriteAlarm(rec)
		if err != nil {
			return err
		}
	}
	for _, rec := range deadJobRecs {
		err = bw.WriteDeadJob(rec)
		if err != nil {
			return err
		}
	}

	return nil
}

// snapshotStateRecords collects the non-expired actor state under the state read lock
func (p *Provider) snapshotStateRecords() []*backup.StateRecord {
	p.StateMu.RLock()
	defer p.StateMu.RUnlock()

	now := p.Clock.Now()
	recs := make([]*backup.StateRecord, 0, len(p.ActorState))
	for key, entry := range p.ActorState {
		if entry.IsExpired(now) {
			continue
		}

		recs = append(recs, &backup.StateRecord{
			ActorType:  key.ActorType,
			ActorID:    key.ActorID,
			Data:       entry.Data,
			Expiration: entry.Expiration,
		})
	}
	return recs
}

// snapshotAlarmAndDeadJobRecords collects all alarms (plain alarms and jobs, without the ephemeral lease fields) and dead jobs under the alarms read lock
func (p *Provider) snapshotAlarmAndDeadJobRecords() ([]*backup.AlarmRecord, []*backup.DeadJobRecord) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	alarms := make([]*backup.AlarmRecord, len(p.AlarmsByID))
	var i int
	for _, a := range p.AlarmsByID {
		alarms[i] = &backup.AlarmRecord{
			ID:        a.ID,
			ActorType: a.ActorType,
			ActorID:   a.ActorID,
			Name:      a.Name,
			DueTime:   a.DueTime,
			Interval:  a.Interval,
			Cron:      a.Cron,
			TTL:       a.TTL,
			Data:      a.Data,
			Kind:      a.Kind,
			JobMethod: a.JobMethod,
		}
		i++
	}

	deadJobs := make([]*backup.DeadJobRecord, len(p.DeadJobs))
	i = 0
	for _, d := range p.DeadJobs {
		deadJobs[i] = &backup.DeadJobRecord{
			JobID:       d.JobID,
			ActorType:   d.ActorType,
			ActorID:     d.ActorID,
			Method:      d.Method,
			Data:        d.Data,
			Attempts:    d.Attempts,
			LastError:   d.LastError,
			FailedAt:    d.FailedAt,
			OriginalDue: d.OriginalDue,
			Interval:    d.Interval,
			Cron:        d.Cron,
		}
		i++
	}

	return alarms, deadJobs
}

// Restore wipes all persistent data and loads a snapshot from r
// It holds both domain write mutexes for the whole operation and refuses to run while any host is connected
func (p *Provider) Restore(ctx context.Context, r io.Reader) error {
	// Serialize against every writer in both domains for the whole wipe-and-load sequence
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.stateWriteMu.Lock()
	defer p.stateWriteMu.Unlock()

	// Restoring underneath live hosts would corrupt running actors
	err := p.ensureNoHostsConnected()
	if err != nil {
		return err
	}

	// Validate the header before touching any data
	br, _, err := backup.NewReader(r)
	if err != nil {
		return err
	}

	// Wipe existing persistent data so the restore produces an exact mirror
	err = p.wipePersistentData(ctx)
	if err != nil {
		return fmt.Errorf("failed to wipe existing data: %w", err)
	}

	// Load records in batches, keyed to the two lock domains
	var (
		alarmSets   []AlarmChange
		deadJobSets []DeadJobChange
		stateSets   []ActorStateChange
	)

	// flushMuDomain persists and applies the buffered alarm and dead-job records
	flushMuDomain := func() error {
		if len(alarmSets) == 0 && len(deadJobSets) == 0 {
			return nil
		}

		changes := NewChanges()
		defer changes.Release()
		changes.Alarms.Set = append(changes.Alarms.Set, alarmSets...)
		changes.DeadJobs.Set = append(changes.DeadJobs.Set, deadJobSets...)

		flushErr := p.persistThenApply(ctx, &p.Mu, changes, func() {
			for _, ac := range alarmSets {
				a := ac.Value
				p.Alarms[a.GetAlarmKey()] = a
				p.AlarmsByID[a.ID] = a
			}
			for _, dc := range deadJobSets {
				p.DeadJobs[dc.Value.JobID] = dc.Value
			}
		})
		if flushErr != nil {
			return flushErr
		}

		alarmSets = alarmSets[:0]
		deadJobSets = deadJobSets[:0]
		return nil
	}

	// flushStateDomain persists and applies the buffered state records
	flushStateDomain := func() error {
		if len(stateSets) == 0 {
			return nil
		}

		changes := NewChanges()
		defer changes.Release()
		changes.ActorState.Set = append(changes.ActorState.Set, stateSets...)

		flushErr := p.persistThenApply(ctx, &p.StateMu, changes, func() {
			for _, sc := range stateSets {
				p.ActorState[sc.Key] = sc.Value
			}
		})
		if flushErr != nil {
			return flushErr
		}

		stateSets = stateSets[:0]
		return nil
	}

	// Consume the stream, buffering into the matching domain and flushing when a batch fills
	for rec, recErr := range br.All() {
		if recErr != nil {
			return recErr
		}

		switch rec.Type {
		case backup.RecordTypeState:
			stateSets = append(stateSets, ActorStateChange{
				Key:   NewActorKey(rec.State.ActorType, rec.State.ActorID),
				Value: &StateEntry{Data: rec.State.Data, Expiration: rec.State.Expiration},
			})
			if len(stateSets) >= restoreBatchSize {
				err = flushStateDomain()
				if err != nil {
					return err
				}
			}
		case backup.RecordTypeAlarm:
			a := alarmFromRecord(rec.Alarm)
			alarmSets = append(alarmSets, AlarmChange{Key: a.ID, Value: a})
			if len(alarmSets)+len(deadJobSets) >= restoreBatchSize {
				err = flushMuDomain()
				if err != nil {
					return err
				}
			}
		case backup.RecordTypeDeadJob:
			d := deadJobFromRecord(rec.DeadJob)
			deadJobSets = append(deadJobSets, DeadJobChange{Key: d.JobID, Value: d})
			if len(alarmSets)+len(deadJobSets) >= restoreBatchSize {
				err = flushMuDomain()
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown backup record type %q", rec.Type)
		}
	}

	// Flush whatever is left in each domain
	err = flushMuDomain()
	if err != nil {
		return err
	}

	err = flushStateDomain()
	if err != nil {
		return err
	}

	return nil
}

// ensureNoHostsConnected returns ErrHostsConnected if any registered host is still healthy
// The caller must hold writeMu so the host set cannot change between this check and the operation that follows
func (p *Provider) ensureNoHostsConnected() error {
	p.Mu.RLock()
	defer p.Mu.RUnlock()

	for _, h := range p.Hosts {
		if p.IsHostHealthy(h) {
			return components.ErrHostsConnected
		}
	}

	return nil
}

// wipePersistentData deletes all actor state, alarms, and dead jobs from both the backing store and memory
// The caller must hold writeMu and stateWriteMu
func (p *Provider) wipePersistentData(ctx context.Context) error {
	// Alarms and dead jobs live in the Mu domain
	alarmChanges := NewChanges()
	defer alarmChanges.Release()
	for id := range p.AlarmsByID {
		alarmChanges.Alarms.Delete = append(alarmChanges.Alarms.Delete, id)
	}
	for id := range p.DeadJobs {
		alarmChanges.DeadJobs.Delete = append(alarmChanges.DeadJobs.Delete, id)
	}
	err := p.persistThenApply(ctx, &p.Mu, alarmChanges, func() {
		clear(p.Alarms)
		clear(p.AlarmsByID)
		clear(p.DeadJobs)
	})
	if err != nil {
		return err
	}

	// Actor state lives in the StateMu domain
	stateChanges := NewChanges()
	defer stateChanges.Release()
	for key := range p.ActorState {
		stateChanges.ActorState.Delete = append(stateChanges.ActorState.Delete, key)
	}
	err = p.persistThenApply(ctx, &p.StateMu, stateChanges, func() {
		clear(p.ActorState)
	})
	if err != nil {
		return err
	}

	return nil
}

// alarmFromRecord builds an in-memory Alarm from a backup record, leaving the lease fields unset
func alarmFromRecord(r *backup.AlarmRecord) *Alarm {
	return &Alarm{
		ID:        r.ID,
		ActorType: r.ActorType,
		ActorID:   r.ActorID,
		Name:      r.Name,
		DueTime:   r.DueTime,
		Interval:  r.Interval,
		Cron:      r.Cron,
		TTL:       r.TTL,
		Data:      r.Data,
		Kind:      r.Kind,
		JobMethod: r.JobMethod,
	}
}

// deadJobFromRecord builds an in-memory DeadJob from a backup record
func deadJobFromRecord(r *backup.DeadJobRecord) *DeadJob {
	return &DeadJob{
		JobID:       r.JobID,
		ActorType:   r.ActorType,
		ActorID:     r.ActorID,
		Method:      r.Method,
		Data:        r.Data,
		Attempts:    r.Attempts,
		LastError:   r.LastError,
		FailedAt:    r.FailedAt,
		OriginalDue: r.OriginalDue,
		Interval:    r.Interval,
		Cron:        r.Cron,
	}
}
