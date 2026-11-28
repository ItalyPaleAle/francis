package internal

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
)

// restoreBatchSize is the number of records written per SQL batch inside a restore transaction
const restoreBatchSize = 1000

// Backup writes a consistent snapshot of all persistent data to w
// Holding Mu.RLock and StateMu.RLock together while snapshotting prevents concurrent turns from producing torn state-and-alarm snapshots, while keeping stream I/O outside both locks
func (p *Provider) Backup(_ context.Context, w io.Writer) error {
	p.Mu.RLock()
	p.StateMu.RLock()
	now := p.Clock.Now()
	stateRecs, eventRecs := p.snapshotStateRecordsLocked(now)
	alarmRecs, terminalJobRecs := p.snapshotAlarmAndTerminalJobRecordsLocked(now)
	p.StateMu.RUnlock()
	p.Mu.RUnlock()

	// Write the header, which records the format version
	bw, err := backup.NewWriter(w, now)
	if err != nil {
		return err
	}

	// Stream state, then alarms, then terminal jobs, then workflow events, matching the format's section ordering
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
	for _, rec := range terminalJobRecs {
		err = bw.WriteTerminalJob(rec)
		if err != nil {
			return err
		}
	}
	for _, rec := range eventRecs {
		err = bw.WriteWorkflowEvent(rec)
		if err != nil {
			return err
		}
	}

	return nil
}

// snapshotStateRecordsLocked collects non-expired actor state and matching workflow events while StateMu is read-locked
func (p *Provider) snapshotStateRecordsLocked(now time.Time) ([]*backup.StateRecord, []*backup.WorkflowEventRecord) {
	recs := make([]*backup.StateRecord, 0, len(p.ActorState))
	var events []*backup.WorkflowEventRecord
	for key, entry := range p.ActorState {
		if entry.IsExpired(now) {
			continue
		}

		rec := &backup.StateRecord{
			ActorType:  key.ActorType,
			ActorID:    key.ActorID,
			Data:       entry.Data,
			Expiration: entry.Expiration,
		}

		if entry.WorkflowLabels != nil {
			rec.WorkflowLabels = new(*entry.WorkflowLabels)
		}

		recs = append(recs, rec)

		// Events are only included for state that is part of the backup
		for _, ev := range p.WorkflowEvents[key] {
			events = append(events, &backup.WorkflowEventRecord{
				ActorType: key.ActorType,
				ActorID:   key.ActorID,
				Seq:       ev.Seq,
				Time:      ev.Time,
				Kind:      ev.Kind,
				Data:      ev.Data,
			})
		}
	}

	return recs, events
}

// snapshotAlarmAndTerminalJobRecordsLocked collects all alarms and non-expired terminal jobs while Mu is read-locked
func (p *Provider) snapshotAlarmAndTerminalJobRecordsLocked(now time.Time) ([]*backup.AlarmRecord, []*backup.TerminalJobRecord) {
	alarms := make([]*backup.AlarmRecord, 0, len(p.AlarmsByID))
	for _, a := range p.AlarmsByID {
		alarms = append(alarms, &backup.AlarmRecord{
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
		})
	}

	terminalJobs := make([]*backup.TerminalJobRecord, 0, len(p.TerminalJobs))
	for _, d := range p.TerminalJobs {
		if d.HasExpired(now) {
			continue
		}

		terminalJobs = append(terminalJobs, &backup.TerminalJobRecord{
			JobID:       d.JobID,
			ActorType:   d.ActorType,
			ActorID:     d.ActorID,
			Method:      d.Method,
			Data:        d.Data,
			Status:      d.Status,
			Attempts:    d.Attempts,
			LastError:   d.LastError,
			EndedAt:     d.EndedAt,
			OriginalDue: d.OriginalDue,
			Interval:    d.Interval,
			Cron:        d.Cron,
			Expiration:  d.Expiration,
		})
	}

	return alarms, terminalJobs
}

// Restore wipes all persistent data and loads a snapshot from r atomically
// Both SQL persistence (via RestorePersistHook) and in-memory maps are rolled back or left untouched if any error occurs mid-stream
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

	// Validate the header before opening a transaction or touching any data
	br, _, err := backup.NewReader(r)
	if err != nil {
		return err
	}

	// Stage restored records into fresh maps so existing in-memory data survives any mid-stream error
	newAlarms := make(map[AlarmKey]*Alarm)
	newAlarmsByID := make(map[string]*Alarm)
	newTerminalJobs := make(map[string]*TerminalJob)
	newActorState := make(map[ActorKey]*StateEntry)
	newWorkflowEvents := make(map[ActorKey][]components.WorkflowEvent)

	var (
		rtx         RestoreTx
		hasTx       bool
		nonTxHook   PersistHook
		alarmSets   []AlarmChange
		termJobSets []TerminalJobChange
		stateSets   []ActorStateChange
		eventSets   []WorkflowEventChange
		eventCount  int
		stateLoaded bool
	)

	restoreHook, isRestoreHook := p.PersistHook.(RestorePersistHook)
	if isRestoreHook {
		rtx, err = restoreHook.BeginRestore(ctx)
		if err != nil {
			return err
		}
		hasTx = true
		defer func() {
			if hasTx {
				rtx.Rollback(ctx)
			}
		}()
	} else if p.PersistHook != nil {
		nonTxHook = p.PersistHook
	}

	// flushTxBatch writes buffered records into the open RestoreTx when SQL-backed
	flushTxBatch := func(changes *Changes) error {
		if !hasTx || changes.IsEmpty() {
			return nil
		}
		return rtx.PersistBatch(ctx, changes)
	}

	flushMuBatch := func() error {
		if len(alarmSets) == 0 && len(termJobSets) == 0 {
			return nil
		}
		if hasTx {
			changes := NewChanges()
			changes.Alarms.Set = append(changes.Alarms.Set, alarmSets...)
			changes.TerminalJobs.Set = append(changes.TerminalJobs.Set, termJobSets...)
			batchErr := flushTxBatch(changes)
			changes.Release()
			if batchErr != nil {
				return batchErr
			}
			alarmSets = alarmSets[:0]
			termJobSets = termJobSets[:0]
		}
		return nil
	}

	flushStateBatch := func() error {
		if len(stateSets) == 0 && len(eventSets) == 0 {
			return nil
		}
		if hasTx {
			changes := NewChanges()
			changes.ActorState.Set = append(changes.ActorState.Set, stateSets...)
			changes.WorkflowEvents.Insert = append(changes.WorkflowEvents.Insert, eventSets...)
			batchErr := flushTxBatch(changes)
			changes.Release()
			if batchErr != nil {
				return batchErr
			}
			stateSets = stateSets[:0]
			eventSets = eventSets[:0]
			eventCount = 0
		}
		return nil
	}

	for rec, recErr := range br.All() {
		if recErr != nil {
			return recErr
		}

		switch rec.Type {
		case backup.RecordTypeState:
			key := NewActorKey(rec.State.ActorType, rec.State.ActorID)
			entry := &StateEntry{
				Data:           rec.State.Data,
				Expiration:     rec.State.Expiration,
				WorkflowLabels: rec.State.WorkflowLabels,
			}
			newActorState[key] = entry
			stateSets = append(stateSets, ActorStateChange{Key: key, Value: entry})
			if hasTx && len(stateSets) >= restoreBatchSize {
				err = flushStateBatch()
				if err != nil {
					return err
				}
			}

		case backup.RecordTypeAlarm:
			a := alarmFromRecord(rec.Alarm)
			newAlarms[a.GetAlarmKey()] = a
			newAlarmsByID[a.ID] = a
			alarmSets = append(alarmSets, AlarmChange{Key: a.ID, Value: a})
			if hasTx && len(alarmSets)+len(termJobSets) >= restoreBatchSize {
				err = flushMuBatch()
				if err != nil {
					return err
				}
			}

		case backup.RecordTypeTerminalJob:
			d := terminalJobFromRecord(rec.TerminalJob)
			newTerminalJobs[d.JobID] = d
			termJobSets = append(termJobSets, TerminalJobChange{Key: d.JobID, Value: d})
			if hasTx && len(alarmSets)+len(termJobSets) >= restoreBatchSize {
				err = flushMuBatch()
				if err != nil {
					return err
				}
			}

		case backup.RecordTypeWorkflowEvent:
			if !stateLoaded {
				err = flushStateBatch()
				if err != nil {
					return err
				}
				stateLoaded = true
			}

			ev := rec.WorkflowEvent
			key := NewActorKey(ev.ActorType, ev.ActorID)
			_, hasState := newActorState[key]
			if !hasState {
				continue
			}
			event := components.WorkflowEvent{Seq: ev.Seq, Time: ev.Time, Kind: ev.Kind, Data: ev.Data}
			existing := newWorkflowEvents[key]
			newWorkflowEvents[key] = mergeSortedEvents(existing, newWorkflowEventsFromSlice(existing, []components.WorkflowEvent{event}))

			if len(eventSets) > 0 && eventSets[len(eventSets)-1].Key == key {
				last := &eventSets[len(eventSets)-1]
				last.Events = append(last.Events, event)
			} else {
				eventSets = append(eventSets, WorkflowEventChange{Key: key, Events: []components.WorkflowEvent{event}})
			}
			eventCount++
			if hasTx && len(stateSets)+eventCount >= restoreBatchSize {
				err = flushStateBatch()
				if err != nil {
					return err
				}
			}

		default:
			return fmt.Errorf("unknown backup record type %q", rec.Type)
		}
	}

	if hasTx {
		err = flushMuBatch()
		if err != nil {
			return err
		}

		err = flushStateBatch()
		if err != nil {
			return err
		}

		err = rtx.Commit(ctx)
		if err != nil {
			return err
		}
		hasTx = false
	} else if nonTxHook != nil {
		// Fallback for custom test PersistHooks that do not implement RestorePersistHook, invoked only after the entire stream is validated
		changes := NewChanges()
		defer changes.Release()
		for id := range p.AlarmsByID {
			changes.Alarms.Delete = append(changes.Alarms.Delete, id)
		}
		for id := range p.TerminalJobs {
			changes.TerminalJobs.Delete = append(changes.TerminalJobs.Delete, id)
		}
		for key := range p.ActorState {
			changes.ActorState.Delete = append(changes.ActorState.Delete, key)
		}
		changes.Alarms.Set = append(changes.Alarms.Set, alarmSets...)
		changes.TerminalJobs.Set = append(changes.TerminalJobs.Set, termJobSets...)
		changes.ActorState.Set = append(changes.ActorState.Set, stateSets...)
		changes.WorkflowEvents.Insert = append(changes.WorkflowEvents.Insert, eventSets...)

		err = nonTxHook.PersistChanges(ctx, changes)
		if err != nil {
			return err
		}
	}

	// Atomically swap the validated, committed state into memory
	p.Mu.Lock()
	p.StateMu.Lock()
	p.Alarms = newAlarms
	p.AlarmsByID = newAlarmsByID
	p.TerminalJobs = newTerminalJobs
	p.ActorState = newActorState
	p.WorkflowEvents = newWorkflowEvents
	p.StateMu.Unlock()
	p.Mu.Unlock()

	return nil
}

func newWorkflowEventsFromSlice(existing []components.WorkflowEvent, incoming []components.WorkflowEvent) []components.WorkflowEvent {
	return newWorkflowEvents(existing, incoming)
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

// terminalJobFromRecord builds an in-memory TerminalJob from a backup record
func terminalJobFromRecord(r *backup.TerminalJobRecord) *TerminalJob {
	return &TerminalJob{
		JobID:       r.JobID,
		ActorType:   r.ActorType,
		ActorID:     r.ActorID,
		Method:      r.Method,
		Data:        r.Data,
		Status:      r.Status,
		Attempts:    r.Attempts,
		LastError:   r.LastError,
		EndedAt:     r.EndedAt,
		OriginalDue: r.OriginalDue,
		Interval:    r.Interval,
		Cron:        r.Cron,
		Expiration:  r.Expiration,
	}
}
