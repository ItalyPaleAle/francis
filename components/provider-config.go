package components

import (
	"errors"
	"fmt"
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

	// Maximum number of hosts allowed to join the cluster at the same time
	// A value of 0 (the default) means there is no limit
	// The first host to join a cluster establishes the effective value, and a host configured with a different value is rejected (to change the limit, shut down the whole cluster first)
	MaxHosts int

	// HealthCheck is how hosts retry their health checks, which decides both the gap left between checks and the shortest usable HostHealthCheckDeadline
	// A nil policy, or any field left unset, means the defaults
	HealthCheck *HealthCheckPolicy
}

// HealthCheckInterval returns the gap hosts should leave between health checks under this configuration
func (o *ProviderConfig) HealthCheckInterval() time.Duration {
	return o.HealthCheck.Interval(o.HostHealthCheckDeadline)
}

// NewProviderConfig returns a ProviderConfig with all default values
func NewProviderConfig() ProviderConfig {
	return ProviderConfig{
		HostHealthCheckDeadline:   DefaultHostHealthCheckDeadline,
		AlarmsLeaseDuration:       DefaultAlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: DefaultAlarmsFetchAheadBatchSize,
		HealthCheck:               NewHealthCheckPolicy(),
	}
}

func (o *ProviderConfig) Validate() error {
	minDeadline := o.HealthCheck.MinDeadline()
	if o.HostHealthCheckDeadline < minDeadline {
		return fmt.Errorf("property HostHealthCheckDeadline is not valid: must be at least %v with the configured health check policy (%v)", minDeadline, o.HealthCheck)
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
	if o.MaxHosts < 0 {
		return errors.New("property MaxHosts is not valid: must be greater than or equal to 0")
	}

	return nil
}
