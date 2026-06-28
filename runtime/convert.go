package runtime

import (
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// protocolActorTypesToComponents converts protocol actor host type DTOs to their components equivalents
// The protocol expresses durations in milliseconds, which are converted back to time.Duration here
func protocolActorTypesToComponents(in []protocol.ActorHostType) []components.ActorHostType {
	if len(in) == 0 {
		return nil
	}

	out := make([]components.ActorHostType, len(in))
	for i, t := range in {
		out[i] = components.ActorHostType{
			ActorType:           t.ActorType,
			IdleTimeout:         time.Duration(t.IdleTimeoutMs) * time.Millisecond,
			ConcurrencyLimit:    t.ConcurrencyLimit,
			DeactivationTimeout: time.Duration(t.DeactivationTimeoutMs) * time.Millisecond,
			MaxAttempts:         t.MaxAttempts,
			InitialRetryDelay:   time.Duration(t.InitialRetryDelayMs) * time.Millisecond,
		}
	}
	return out
}

// protocolAlarmPropsToRef converts protocol alarm properties to the ref equivalent
// Timestamps arrive as Unix milliseconds and are converted back to time.Time, with a zero TTL meaning none
func protocolAlarmPropsToRef(p protocol.AlarmProperties) ref.AlarmProperties {
	props := ref.AlarmProperties{
		DueTime:  time.UnixMilli(p.DueTimeUnixMs),
		Interval: p.Interval,
		Data:     p.Data,
	}
	if p.TTLUnixMs > 0 {
		ttl := time.UnixMilli(p.TTLUnixMs)
		props.TTL = &ttl
	}
	return props
}

// refAlarmPropsToProtocol converts ref alarm properties to the protocol DTO
func refAlarmPropsToProtocol(p ref.AlarmProperties) protocol.AlarmProperties {
	out := protocol.AlarmProperties{
		DueTimeUnixMs: p.DueTime.UnixMilli(),
		Interval:      p.Interval,
		Data:          p.Data,
	}
	if p.TTL != nil {
		out.TTLUnixMs = p.TTL.UnixMilli()
	}
	return out
}

// componentsJobStatusToProtocol maps the provider job status to the wire integer status
func componentsJobStatusToProtocol(s components.JobStatus) int {
	switch s {
	case components.JobStatusActive:
		return 1
	case components.JobStatusDeadLettered:
		return 2
	default:
		return 0
	}
}

// componentsJobInfoToProtocol converts a provider JobInfo to the wire DTO, expressing timestamps as Unix milliseconds
func componentsJobInfoToProtocol(j components.JobInfo) protocol.JobInfo {
	out := protocol.JobInfo{
		JobID:     j.JobID,
		ActorType: j.ActorType,
		ActorID:   j.ActorID,
		Method:    j.Method,
		Status:    componentsJobStatusToProtocol(j.Status),
		Interval:  j.Interval,
		Cron:      j.Cron,
		Attempts:  j.Attempts,
		LastError: j.LastError,
	}
	if !j.DueTime.IsZero() {
		out.DueTimeUnixMs = j.DueTime.UnixMilli()
	}
	if !j.CreatedAt.IsZero() {
		out.CreatedAtUnixMs = j.CreatedAt.UnixMilli()
	}
	return out
}
