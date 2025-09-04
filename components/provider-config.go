package components

import (
	"time"
)

// ProviderConfig contains the configuration for the actor provider
type ProviderConfig struct {
	// Maximum interval between pings received from an actor host.
	HostHealthCheckDeadline time.Duration

	// Alarms lease duration
	AlarmsLeaseDuration time.Duration

	// Pre-fetch interval for alarms
	AlarmsFetchAheadInterval time.Duration

	// Batch size for pre-fetching alarms
	AlarmsFetchAheadBatchSize int
}

func (o *ProviderConfig) SetDefaults() {
	if o.HostHealthCheckDeadline < time.Second {
		o.HostHealthCheckDeadline = DefaultHostHealthCheckDeadline
	}
	if o.AlarmsLeaseDuration < time.Second {
		o.AlarmsLeaseDuration = DefaultAlarmsLeaseDuration
	}
	if o.AlarmsFetchAheadInterval < 100*time.Millisecond {
		o.AlarmsFetchAheadInterval = DefaultAlarmsFetchAheadInterval
	}
	if o.AlarmsFetchAheadBatchSize <= 0 {
		o.AlarmsFetchAheadBatchSize = DefaultAlarmsFetchAheadBatch
	}
}
