package backup

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/components"
	comptesting "github.com/italypaleale/francis/components/testing"
	"github.com/italypaleale/francis/internal/backup"
	"github.com/italypaleale/francis/internal/ref"
)

// A backup is the one artifact that outlives the build that wrote it, so a stream an older Francis produced has to keep restoring into the current one
// The format version is not bumped for a change a reader can absorb, so nothing stops such a stream from being accepted and quietly losing whatever the reader no longer recognizes: only a test catches that
//
// legacyCase is one such stream paired with what restoring it must produce
// Adding a case is how the next format change gets its regression test, rather than writing another test from scratch
type legacyCase struct {
	// name says which change the stream predates, and names the subtest
	name string
	// stream writes the backup exactly as that version of Francis wrote it, records and all
	// Each case spells its record shapes out in this file rather than reaching for the current types, so a later change to those types cannot quietly rewrite what history looked like
	stream func(t *testing.T, enc *msgpack.Encoder)
	// assert checks what the provider holds once the stream has been restored, through the provider's own API and, where it matters, through a fresh backup
	assert func(t *testing.T, ctx context.Context, p components.ActorProvider)
}

// legacyHeader is the stream header, which has not changed across any of the cases below
type legacyHeader struct {
	Format    string    `msgpack:"format"`
	Version   int       `msgpack:"version"`
	CreatedAt time.Time `msgpack:"createdAt"`
}

// legacyStateRecord is an actor-state record from before the workflow-label column existed
// It is shared by the cases because a stream needs a little unrelated content to prove the records under test are not being read out of an otherwise empty database
type legacyStateRecord struct {
	ActorType  string     `msgpack:"actorType"`
	ActorID    string     `msgpack:"actorId"`
	Data       []byte     `msgpack:"data,omitempty"`
	Expiration *time.Time `msgpack:"expiration,omitempty"`
}

// legacyDeadJobRecord is what a terminal job looked like when the store held only the jobs that had failed
type legacyDeadJobRecord struct {
	JobID       string    `msgpack:"jobId"`
	ActorType   string    `msgpack:"actorType"`
	ActorID     string    `msgpack:"actorId"`
	Method      string    `msgpack:"method"`
	Data        []byte    `msgpack:"data,omitempty"`
	Attempts    int       `msgpack:"attempts"`
	LastError   string    `msgpack:"lastError,omitempty"`
	FailedAt    time.Time `msgpack:"failedAt"`
	OriginalDue time.Time `msgpack:"originalDue"`
	Interval    string    `msgpack:"interval,omitempty"`
	Cron        string    `msgpack:"cron,omitempty"`
}

// legacyDeadJobEnvelope is the record envelope of that era: a type discriminator and the payload under the name it had then
type legacyDeadJobEnvelope struct {
	Type    string               `msgpack:"type"`
	State   *legacyStateRecord   `msgpack:"state,omitempty"`
	DeadJob *legacyDeadJobRecord `msgpack:"deadJob,omitempty"`
}

// deadJobIDFailed and deadJobIDRecurring are the two jobs the dead-letter case restores, one a one-shot and one a recurrence
const (
	deadJobIDFailed    = "01a09d00-0000-7000-8000-00000000dead"
	deadJobIDRecurring = "01a09d00-0000-7000-8000-00000000beef"
)

// legacyCases are the streams under test, oldest change first
var legacyCases = []legacyCase{
	{
		// The dead-letter store became the terminal-job store: what held only the jobs that failed now holds the ones that completed as well
		// The record gained a status and an expiration, lost its failure-specific name, and its type discriminator changed with it, so a reader that does not map the old shape drops every dead job in the stream
		name: "dead-letter store before it became the terminal-job store",
		stream: func(t *testing.T, enc *msgpack.Encoder) {
			t.Helper()

			failedAt, originalDue := legacyDeadJobTimes()

			err := enc.Encode(&legacyDeadJobEnvelope{
				Type:  "state",
				State: &legacyStateRecord{ActorType: "OLD", ActorID: "actor-1", Data: []byte("state-data")},
			})
			require.NoError(t, err)

			err = enc.Encode(&legacyDeadJobEnvelope{
				Type: "deadjob",
				DeadJob: &legacyDeadJobRecord{
					JobID: deadJobIDFailed, ActorType: "OLD", ActorID: "actor-1",
					Method: "process", Data: []byte("job-payload"), Attempts: 5, LastError: "boom",
					FailedAt: failedAt, OriginalDue: originalDue,
				},
			})
			require.NoError(t, err)

			err = enc.Encode(&legacyDeadJobEnvelope{
				Type: "deadjob",
				DeadJob: &legacyDeadJobRecord{
					JobID: deadJobIDRecurring, ActorType: "OLD", ActorID: "actor-2",
					Method: "sweep", Attempts: 3, LastError: "still boom",
					FailedAt: failedAt, OriginalDue: originalDue, Interval: "PT1H",
				},
			})
			require.NoError(t, err)
		},
		assert: func(t *testing.T, ctx context.Context, p components.ActorProvider) {
			t.Helper()

			failedAt, originalDue := legacyDeadJobTimes()

			// The provider serves them as terminal jobs, which is what an operator reads after a restore
			dead, err := p.GetTerminalJob(ctx, deadJobIDFailed)
			require.NoError(t, err)
			assert.Equal(t, components.JobStatusDeadLettered, dead.Status, "a job from the old store ended by failing, which is the status the old record had no need to carry")
			assert.Equal(t, "process", dead.Method)
			assert.Equal(t, []byte("job-payload"), dead.Data, "a dead job keeps its input, which is what a replay needs")
			assert.Equal(t, 5, dead.Attempts)
			assert.Nil(t, dead.Expiration, "the old store kept a dead job until something removed it, which is what a nil expiration still means")

			info, err := p.GetJob(ctx, deadJobIDFailed)
			require.NoError(t, err)
			assert.Equal(t, components.JobStatusDeadLettered, info.Status)
			assert.WithinDuration(t, failedAt, info.EndedAt, time.Millisecond, "the old record's failure time is when the job ended")

			// It is listed for its actor alongside anything live, so the restored record is reachable without knowing its ID
			jobs, err := p.ListJobs(ctx, "OLD", "actor-1")
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			assert.Equal(t, deadJobIDFailed, jobs[0].JobID)

			// A dead job can still be replayed after the restore, which is the whole reason its payload is kept
			newID, err := p.RetryDeadJob(ctx, deadJobIDFailed)
			require.NoError(t, err)
			assert.NotEqual(t, deadJobIDFailed, newID)

			// A backup taken now writes them in the current shape, so the old naming does not survive the round trip
			var buf bytes.Buffer
			err = p.Backup(ctx, &buf)
			require.NoError(t, err)

			got := comptesting.DecodeBackup(t, buf.Bytes())
			recurring, ok := got.TerminalJobs[deadJobIDRecurring]
			require.True(t, ok, "the second dead job should be in the new backup")
			assert.Equal(t, string(components.JobStatusDeadLettered), recurring.Status)
			assert.Equal(t, "sweep", recurring.Method)
			assert.Equal(t, "PT1H", recurring.Interval)
			assert.Equal(t, 3, recurring.Attempts)
			assert.Equal(t, "still boom", recurring.LastError)
			assert.WithinDuration(t, failedAt, recurring.EndedAt, time.Millisecond)
			assert.WithinDuration(t, originalDue, recurring.OriginalDue, time.Millisecond)
		},
	},
}

// legacyDeadJobTimes returns the timestamps the dead-letter case writes and then asserts on
// They are fixed rather than taken from the clock, because a legacy backup is a historical artifact: the same bytes every time, and the same bytes the assertions expect
func legacyDeadJobTimes() (failedAt time.Time, originalDue time.Time) {
	failedAt = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	return failedAt, failedAt.Add(-time.Minute)
}

// TestRestoreOfLegacyBackups restores a backup written by an older Francis into each provider and checks the current build makes the right thing of it
func TestRestoreOfLegacyBackups(t *testing.T) {
	// The providers every case is checked against, since a stream is only portable if each of them reads it the same way
	// open returns nil for a provider this environment cannot reach, which skips that leg rather than failing it
	providers := []struct {
		name string
		open func(t *testing.T) components.ActorProvider
	}{
		{"memory", func(t *testing.T) components.ActorProvider { return newMemory(t) }},
		{"sqlite", func(t *testing.T) components.ActorProvider { return newSQLite(t) }},
		{"postgres", func(t *testing.T) components.ActorProvider {
			p := newPostgres(t)
			if p == nil {
				// Already skipped, and a typed nil would not compare equal to nil once it is in the interface
				return nil
			}
			return p
		}},
	}

	for _, tc := range legacyCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, prov := range providers {
				t.Run(prov.name, func(t *testing.T) {
					p := prov.open(t)
					if p == nil {
						// Test already skipped
						return
					}

					err := p.Restore(t.Context(), bytes.NewReader(encodeLegacyStream(t, tc)))
					require.NoError(t, err)

					tc.assert(t, t.Context(), p)

					// Every case seeds this row, so a case whose own records went missing cannot pass on an empty database
					stateData, err := p.GetState(t.Context(), ref.NewActorRef("OLD", "actor-1"))
					require.NoError(t, err)
					assert.Equal(t, []byte("state-data"), stateData, "the rest of the stream should have restored too")
				})
			}
		})
	}
}

// encodeLegacyStream writes a case's stream, header first, exactly as the Francis of that era laid it out
func encodeLegacyStream(t *testing.T, tc legacyCase) []byte {
	t.Helper()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	// The format name and version are what that build wrote, and the reader still accepts them
	created, _ := legacyDeadJobTimes()
	err := enc.Encode(&legacyHeader{Format: backup.Format, Version: 1, CreatedAt: created})
	require.NoError(t, err)

	tc.stream(t, enc)

	return buf.Bytes()
}
