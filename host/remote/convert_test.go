package remote

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/italypaleale/francis/components"
)

func TestRetentionSurvivesTheWire(t *testing.T) {
	// The wire carries milliseconds, and zero is the one value that means "keep no record", so the truncation must never land a retention on it
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{name: "unset keeps no record", in: 0, want: 0},
		{name: "a window is carried as milliseconds", in: 90 * time.Second, want: 90_000},
		{name: "a window shorter than a millisecond stays a window", in: 500 * time.Microsecond, want: 1},
		{name: "the never-expires sentinel stays negative", in: -1, want: -1},
		{name: "any negative window is the same sentinel", in: -5 * time.Hour, want: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retentionToWireMs(tt.in))
		})
	}
}

func TestActorTypesCarryBothRetentionWindows(t *testing.T) {
	// The runtime reads a zero completed-job window as "keep no record", so a host asking to keep one forever must not arrive saying the opposite
	got := componentsActorTypesToProtocol([]components.ActorHostType{
		{
			ActorType:                "T",
			CompletedJobRetention:    -1,
			DeadLetteredJobRetention: 30 * 24 * time.Hour,
		},
	})

	assert.Len(t, got, 1)
	assert.EqualValues(t, -1, got[0].CompletedJobRetentionMs)
	assert.EqualValues(t, 30*24*time.Hour/time.Millisecond, got[0].DeadLetteredJobRetentionMs)
}
