package components

import (
	"errors"
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

func (o *ProviderConfig) Validate() error {
	if o.HostHealthCheckDeadline < time.Second {
		return errors.New("property HostHealthCheckDeadline is not valid: must be at least 1s")
	}
	if o.AlarmsLeaseDuration < time.Second {
		return errors.New("property AlarmsLeaseDuration is not valid: must be at least 1s")
	}
	if o.AlarmsFetchAheadInterval < 100*time.Millisecond {
		return errors.New("property AlarmsFetchAheadInterval is not valid: must be at least 100ms")
	}
	if o.AlarmsFetchAheadBatchSize <= 0 {
		return errors.New("property AlarmsFetchAheadBatchSize is not valid: must be greater than 0")
	}
	return nil
}
