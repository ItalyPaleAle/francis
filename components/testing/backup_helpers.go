package comptesting

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
	"github.com/italypaleale/francis/internal/ref"
)

// BackupContents is a decoded backup stream, keyed for order-independent comparison
type BackupContents struct {
	States   map[string]backup.StateRecord
	Alarms   map[string]backup.AlarmRecord
	DeadJobs map[string]backup.DeadJobRecord
}

// DecodeBackup reads a whole backup stream into a BackupContents
func DecodeBackup(t testing.TB, data []byte) BackupContents {
	t.Helper()

	r, _, err := backup.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out := BackupContents{
		States:   map[string]backup.StateRecord{},
		Alarms:   map[string]backup.AlarmRecord{},
		DeadJobs: map[string]backup.DeadJobRecord{},
	}
	for rec, err := range r.All() {
		require.NoError(t, err)

		switch rec.Type {
		case backup.RecordTypeState:
			out.States[rec.State.ActorType+"/"+rec.State.ActorID] = *rec.State
		case backup.RecordTypeAlarm:
			out.Alarms[rec.Alarm.ID] = *rec.Alarm
		case backup.RecordTypeDeadJob:
			out.DeadJobs[rec.DeadJob.JobID] = *rec.DeadJob
		default:
			t.Fatalf("unexpected record type %q", rec.Type)
		}
	}

	return out
}

// AssertBackupContentsEqual asserts that two decoded backups carry the same records
// Times are compared at millisecond granularity, since some providers store timestamps as unix milliseconds while others keep nanoseconds
func AssertBackupContentsEqual(t testing.TB, want, got BackupContents) {
	t.Helper()

	require.Len(t, got.States, len(want.States), "state count mismatch")
	for k, w := range want.States {
		g, ok := got.States[k]
		require.Truef(t, ok, "missing state %q", k)
		assert.Truef(t, bytes.Equal(w.Data, g.Data), "state %q data", k)
		assertTimePtrEqual(t, w.Expiration, g.Expiration, "state "+k+" expiration")
	}

	require.Len(t, got.Alarms, len(want.Alarms), "alarm count mismatch")
	for k, w := range want.Alarms {
		g, ok := got.Alarms[k]
		require.Truef(t, ok, "missing alarm %q", k)
		assert.Equalf(t, w.ActorType, g.ActorType, "alarm %q actorType", k)
		assert.Equalf(t, w.ActorID, g.ActorID, "alarm %q actorID", k)
		assert.Equalf(t, w.Name, g.Name, "alarm %q name", k)
		assert.Equalf(t, w.Interval, g.Interval, "alarm %q interval", k)
		assert.Equalf(t, w.Cron, g.Cron, "alarm %q cron", k)
		assert.Equalf(t, w.Kind, g.Kind, "alarm %q kind", k)
		assert.Equalf(t, w.JobMethod, g.JobMethod, "alarm %q jobMethod", k)
		assert.Truef(t, bytes.Equal(w.Data, g.Data), "alarm %q data", k)
		assertTimeEqual(t, w.DueTime, g.DueTime, "alarm "+k+" dueTime")
		assertTimePtrEqual(t, w.TTL, g.TTL, "alarm "+k+" ttl")
	}

	require.Len(t, got.DeadJobs, len(want.DeadJobs), "dead job count mismatch")
	for k, w := range want.DeadJobs {
		g, ok := got.DeadJobs[k]
		require.Truef(t, ok, "missing dead job %q", k)
		assert.Equalf(t, w.ActorType, g.ActorType, "dead job %q actorType", k)
		assert.Equalf(t, w.ActorID, g.ActorID, "dead job %q actorID", k)
		assert.Equalf(t, w.Method, g.Method, "dead job %q method", k)
		assert.Equalf(t, w.Attempts, g.Attempts, "dead job %q attempts", k)
		assert.Equalf(t, w.LastError, g.LastError, "dead job %q lastError", k)
		assert.Equalf(t, w.Interval, g.Interval, "dead job %q interval", k)
		assert.Equalf(t, w.Cron, g.Cron, "dead job %q cron", k)
		assert.Truef(t, bytes.Equal(w.Data, g.Data), "dead job %q data", k)
		assertTimeEqual(t, w.FailedAt, g.FailedAt, "dead job "+k+" failedAt")
		assertTimeEqual(t, w.OriginalDue, g.OriginalDue, "dead job "+k+" originalDue")
	}
}

func assertTimeEqual(t testing.TB, want, got time.Time, msg string) {
	t.Helper()
	assert.Truef(t, want.Truncate(time.Millisecond).Equal(got.Truncate(time.Millisecond)), "%s: want %v got %v", msg, want, got)
}

func assertTimePtrEqual(t testing.TB, want, got *time.Time, msg string) {
	t.Helper()
	if want == nil {
		assert.Nilf(t, got, "%s: expected nil", msg)
		return
	}
	require.NotNilf(t, got, "%s: expected non-nil", msg)
	assertTimeEqual(t, *want, *got, msg)
}

// SeedBackupSample writes one of each kind of persistent record (actor state, a plain alarm, a live job, and a dead job) through the provider's public API, then removes the host so the cluster is quiescent and a backup or restore can run
// now should be the provider's current time, so callers with a mock clock should pass the provider's Now
func SeedBackupSample(t testing.TB, ctx context.Context, p components.ActorProvider, now time.Time) {
	t.Helper()

	const actorType = "BK"
	now = now.Truncate(time.Millisecond)

	// A host is required to lease and dead-letter a job
	hostRes, err := p.RegisterHost(ctx, components.RegisterHostReq{
		Address: "127.0.0.1:9990",
		ActorTypes: []components.ActorHostType{
			{ActorType: actorType, IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 0},
		},
	})
	require.NoError(t, err)

	// Actor state, without expiration so the values do not depend on the provider clock
	err = p.SetState(ctx, ref.NewActorRef(actorType, "state-1"), []byte("state-data-1"), components.SetStateOpts{})
	require.NoError(t, err)
	err = p.SetState(ctx, ref.NewActorRef(actorType, "state-2"), []byte("state-data-2"), components.SetStateOpts{})
	require.NoError(t, err)

	// A plain alarm with the optional fields populated
	ttl := now.Add(24 * time.Hour)
	err = p.SetAlarm(ctx, ref.NewAlarmRef(actorType, "alarm-actor", "alarm-1"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  now.Add(time.Hour),
			Interval: "PT5M",
			TTL:      &ttl,
			Data:     []byte("alarm-data"),
		},
		Kind: components.AlarmKindAlarm,
	})
	require.NoError(t, err)

	// A live job, due far in the future so the fetcher leaves it alone
	_, err = p.DispatchJob(ctx, ref.NewAlarmRef(actorType, "job-actor", "live-job"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime: now.Add(time.Hour),
			Data:    []byte("live-job-data"),
		},
		Kind:      components.AlarmKindJob,
		JobMethod: "Process",
	})
	require.NoError(t, err)

	// A dead job: dispatch a job due now, lease it through the fetcher, then dead-letter it
	deadJobID, err := p.DispatchJob(ctx, ref.NewAlarmRef(actorType, "dead-actor", "dead-job"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime: now,
			Data:    []byte("dead-job-data"),
		},
		Kind:      components.AlarmKindJob,
		JobMethod: "Process",
	})
	require.NoError(t, err)

	leases, err := p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{Hosts: []string{hostRes.HostID}})
	require.NoError(t, err)

	var deadLease *ref.AlarmLease
	for _, l := range leases {
		if l.Key() == deadJobID {
			deadLease = l
			break
		}
	}
	require.NotNil(t, deadLease, "expected to lease the dispatched dead job")

	err = p.DeadLetterAlarm(ctx, deadLease, components.DeadLetterAlarmReq{Reason: "boom", Attempts: 3})
	require.NoError(t, err)

	// Remove the host so the cluster is quiescent
	err = p.UnregisterHost(ctx, hostRes.HostID)
	require.NoError(t, err)
}

// AddExtraBackupData writes records under distinct keys that are absent from a prior snapshot, so a wipe-then-restore can be observed to remove them
func AddExtraBackupData(t testing.TB, ctx context.Context, p components.ActorProvider, now time.Time) {
	t.Helper()

	err := p.SetState(ctx, ref.NewActorRef("EXTRA", "extra-1"), []byte("extra-state"), components.SetStateOpts{})
	require.NoError(t, err)

	err = p.SetAlarm(ctx, ref.NewAlarmRef("EXTRA", "extra-actor", "extra-alarm"), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{DueTime: now.Add(2 * time.Hour), Data: []byte("extra-alarm")},
		Kind:            components.AlarmKindAlarm,
	})
	require.NoError(t, err)
}
