package backup

import (
	"bytes"
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

// The record shapes below are what Francis wrote when the store held only dead-lettered jobs, before it was widened to record completed ones too.
type (
	legacyHeader struct {
		Format    string    `msgpack:"format"`
		Version   int       `msgpack:"version"`
		CreatedAt time.Time `msgpack:"createdAt"`
	}

	legacyStateRecord struct {
		ActorType  string     `msgpack:"actorType"`
		ActorID    string     `msgpack:"actorId"`
		Data       []byte     `msgpack:"data,omitempty"`
		Expiration *time.Time `msgpack:"expiration,omitempty"`
	}

	legacyDeadJobRecord struct {
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

	legacyRecord struct {
		Type    string               `msgpack:"type"`
		State   *legacyStateRecord   `msgpack:"state,omitempty"`
		DeadJob *legacyDeadJobRecord `msgpack:"deadJob,omitempty"`
	}
)

// TestRestoreOfAPreRenameBackup verifies a backup whose jobs are in the old dead-letter shape restores into the terminal-job store
//
// The store was renamed and widened in the same change: what used to hold only the jobs that failed now holds the ones that completed as well, so the record gained a status and an expiration and lost its failure-specific name.
// The format version did not change with it, so a reader accepts such a stream, and every dead job in it would be dropped without a word unless the reader maps the old shape onto the new one.
// A backup is the one artifact that outlives the build that wrote it, so this is the case that has to keep working.
func TestRestoreOfAPreRenameBackup(t *testing.T) {
	failedAt := time.Now().Add(-time.Hour).Truncate(time.Millisecond).UTC()
	originalDue := failedAt.Add(-time.Minute)

	// A stream in the old shape: a header, one state row, and two dead jobs, one of them a recurrence
	legacy := func(t *testing.T) []byte {
		t.Helper()

		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		err := enc.Encode(&legacyHeader{Format: backup.Format, Version: 1, CreatedAt: failedAt})
		require.NoError(t, err)

		err = enc.Encode(&legacyRecord{
			Type:  "state",
			State: &legacyStateRecord{ActorType: "OLD", ActorID: "actor-1", Data: []byte("state-data")},
		})
		require.NoError(t, err)

		err = enc.Encode(&legacyRecord{
			Type: "deadjob",
			DeadJob: &legacyDeadJobRecord{
				JobID: "01a09d00-0000-7000-8000-00000000dead", ActorType: "OLD", ActorID: "actor-1",
				Method: "process", Data: []byte("job-payload"), Attempts: 5, LastError: "boom",
				FailedAt: failedAt, OriginalDue: originalDue,
			},
		})
		require.NoError(t, err)

		err = enc.Encode(&legacyRecord{
			Type: "deadjob",
			DeadJob: &legacyDeadJobRecord{
				JobID: "01a09d00-0000-7000-8000-00000000beef", ActorType: "OLD", ActorID: "actor-2",
				Method: "sweep", Attempts: 3, LastError: "still boom",
				FailedAt: failedAt, OriginalDue: originalDue, Interval: "PT1H",
			},
		})
		require.NoError(t, err)

		return buf.Bytes()
	}

	// restoreLegacy restores the old stream into p and asserts the jobs arrive as dead-lettered terminal jobs, through the provider's own API and through a fresh backup
	restoreLegacy := func(t *testing.T, p components.ActorProvider) {
		t.Helper()
		ctx := t.Context()

		err := p.Restore(ctx, bytes.NewReader(legacy(t)))
		require.NoError(t, err)

		// The provider serves them as terminal jobs, which is what an operator reads after a restore
		dead, err := p.GetTerminalJob(ctx, "01a09d00-0000-7000-8000-00000000dead")
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusDeadLettered, dead.Status, "a job from the old store ended by failing, which is the status the old record had no need to carry")
		assert.Equal(t, "process", dead.Method)
		assert.Equal(t, []byte("job-payload"), dead.Data, "a dead job keeps its input, which is what a replay needs")
		assert.Equal(t, 5, dead.Attempts)
		assert.Nil(t, dead.Expiration, "the old store kept a dead job until something removed it, which is what a nil expiration still means")

		info, err := p.GetJob(ctx, "01a09d00-0000-7000-8000-00000000dead")
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusDeadLettered, info.Status)
		assert.WithinDuration(t, failedAt, info.EndedAt, time.Millisecond, "the old record's failure time is when the job ended")

		// It is listed for its actor alongside anything live, so the restored record is reachable without knowing its ID
		jobs, err := p.ListJobs(ctx, "OLD", "actor-1")
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, "01a09d00-0000-7000-8000-00000000dead", jobs[0].JobID)

		// A dead job can still be replayed after the restore, which is the whole reason its payload is kept
		newID, err := p.RetryDeadJob(ctx, "01a09d00-0000-7000-8000-00000000dead")
		require.NoError(t, err)
		assert.NotEqual(t, "01a09d00-0000-7000-8000-00000000dead", newID)

		// The rest of the stream restored too, so the jobs are not being read out of an otherwise empty database
		stateData, err := p.GetState(ctx, ref.NewActorRef("OLD", "actor-1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("state-data"), stateData)

		// And a backup taken now writes them in the current shape, so the old naming does not survive the round trip
		var buf bytes.Buffer
		err = p.Backup(ctx, &buf)
		require.NoError(t, err)

		got := comptesting.DecodeBackup(t, buf.Bytes())
		recurring, ok := got.TerminalJobs["01a09d00-0000-7000-8000-00000000beef"]
		require.True(t, ok, "the second dead job should be in the new backup")
		assert.Equal(t, string(components.JobStatusDeadLettered), recurring.Status)
		assert.Equal(t, "sweep", recurring.Method)
		assert.Equal(t, "PT1H", recurring.Interval)
		assert.Equal(t, 3, recurring.Attempts)
		assert.Equal(t, "still boom", recurring.LastError)
		assert.WithinDuration(t, failedAt, recurring.EndedAt, time.Millisecond)
		assert.WithinDuration(t, originalDue, recurring.OriginalDue, time.Millisecond)
	}

	t.Run("memory", func(t *testing.T) {
		restoreLegacy(t, newMemory(t))
	})

	t.Run("sqlite", func(t *testing.T) {
		restoreLegacy(t, newSQLite(t))
	})

	t.Run("postgres", func(t *testing.T) {
		pg := newPostgres(t)
		if pg == nil {
			// Test already skipped
			return
		}

		restoreLegacy(t, pg)
	})
}
