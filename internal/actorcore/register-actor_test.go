//go:build unit

package actorcore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
)

func TestJobRetentionOptions(t *testing.T) {
	t.Run("the two windows default independently", func(t *testing.T) {
		opts := &RegisterActorOptions{}
		require.NoError(t, opts.Validate())

		assert.Zero(t, opts.CompletedJobRetention, "a completed job leaves no record unless asked for")
		assert.Equal(t, 30*24*time.Hour, opts.DeadLetteredJobRetention, "a dead-lettered job is kept for the default window")
	})

	t.Run("either window can be set without the other", func(t *testing.T) {
		completedOnly := &RegisterActorOptions{}
		WithCompletedJobRetention(time.Hour)(completedOnly)
		err := completedOnly.Validate()
		require.NoError(t, err)
		assert.Equal(t, time.Hour, completedOnly.CompletedJobRetention)
		assert.Equal(t, 30*24*time.Hour, completedOnly.DeadLetteredJobRetention, "setting one window leaves the other on its default")

		deadOnly := &RegisterActorOptions{}
		WithDeadLetteredJobRetention(2 * time.Hour)(deadOnly)
		err = deadOnly.Validate()
		require.NoError(t, err)
		assert.Zero(t, deadOnly.CompletedJobRetention, "setting one window leaves the other on its default")
		assert.Equal(t, 2*time.Hour, deadOnly.DeadLetteredJobRetention)
	})

	t.Run("a negative window is normalized", func(t *testing.T) {
		// Every negative spelling has to behave the same, since the value reaches a provider as "no expiry" rather than as a duration
		opts := &RegisterActorOptions{}
		WithCompletedJobRetention(-5 * time.Hour)(opts)
		WithDeadLetteredJobRetention(-time.Second)(opts)
		err := opts.Validate()
		require.NoError(t, err)

		assert.Equal(t, time.Duration(-1), opts.CompletedJobRetention)
		assert.Equal(t, time.Duration(-1), opts.DeadLetteredJobRetention)
	})
}

func TestActorHostTypeJobRetention(t *testing.T) {
	t.Run("a completed job", func(t *testing.T) {
		tests := []struct {
			name          string
			configured    time.Duration
			wantRecord    bool
			wantRetention time.Duration
		}{
			{name: "unset keeps no record", configured: 0, wantRecord: false, wantRetention: 0},
			{name: "a window keeps a record that expires", configured: time.Hour, wantRecord: true, wantRetention: time.Hour},
			{name: "negative keeps a record that never expires", configured: -1, wantRecord: true, wantRetention: 0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				record, retention := components.ActorHostType{CompletedJobRetention: tc.configured}.CompletedJobRecord()
				assert.Equal(t, tc.wantRecord, record)
				assert.Equal(t, tc.wantRetention, retention)
			})
		}
	})

	t.Run("a dead-lettered job", func(t *testing.T) {
		// There is no "no record" case here: a failure is always recorded, and only the expiry varies
		assert.Equal(t, time.Hour, components.ActorHostType{DeadLetteredJobRetention: time.Hour}.DeadLetteredJobRecordRetention())
		assert.Zero(t, components.ActorHostType{DeadLetteredJobRetention: -1}.DeadLetteredJobRecordRetention(), "negative never expires")
		assert.Zero(t, components.ActorHostType{}.DeadLetteredJobRecordRetention(), "an unset window reaching a provider never expires either, rather than dropping the failure")
	})
}
