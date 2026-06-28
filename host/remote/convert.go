package remote

import (
	"bytes"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// componentsActorTypesToProtocol converts the actor type configuration owned by the manager into the protocol DTOs sent at registration
// Durations are expressed in milliseconds on the wire
func componentsActorTypesToProtocol(in []components.ActorHostType) []protocol.ActorHostType {
	if len(in) == 0 {
		return nil
	}

	out := make([]protocol.ActorHostType, len(in))
	for i, t := range in {
		out[i] = protocol.ActorHostType{
			ActorType:             t.ActorType,
			IdleTimeoutMs:         t.IdleTimeout.Milliseconds(),
			ConcurrencyLimit:      t.ConcurrencyLimit,
			DeactivationTimeoutMs: t.DeactivationTimeout.Milliseconds(),
			MaxAttempts:           t.MaxAttempts,
			InitialRetryDelayMs:   t.InitialRetryDelay.Milliseconds(),
		}
	}
	return out
}

// protocolAlarmPropsToActor converts a runtime alarm response into the public actor properties
// The wire format carries data as MessagePack bytes and timestamps as Unix milliseconds
func protocolAlarmPropsToActor(p protocol.AlarmProperties) (actor.AlarmProperties, error) {
	o := actor.AlarmProperties{
		DueTime:  time.UnixMilli(p.DueTimeUnixMs),
		Interval: p.Interval,
	}

	// Decode the opaque alarm data from MessagePack into the public any-typed field
	if len(p.Data) > 0 {
		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(p.Data))
		defer msgpack.PutDecoder(dec)
		err := dec.Decode(&o.Data)
		if err != nil {
			return actor.AlarmProperties{}, fmt.Errorf("failed to deserialize data using msgpack: %w", err)
		}
	}

	// A zero TTL on the wire means no deadline
	if p.TTLUnixMs > 0 {
		o.TTL = time.UnixMilli(p.TTLUnixMs)
	}

	return o, nil
}

// actorJobPropsToProtocol converts the public job properties into the wire DTO, resolving the due time against the given clock and encoding the input as MessagePack
func actorJobPropsToProtocol(p actor.JobProperties, input any, now time.Time) (protocol.JobProperties, error) {
	out := protocol.JobProperties{
		DueTimeUnixMs: p.EffectiveDueTime(now).UnixMilli(),
		Interval:      p.Interval,
		Cron:          p.Cron,
	}

	if input != nil {
		data, err := msgpack.Marshal(input)
		if err != nil {
			return protocol.JobProperties{}, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}
		out.Data = data
	}

	if !p.TTL.IsZero() {
		out.TTLUnixMs = p.TTL.UnixMilli()
	}

	return out, nil
}

// protocolJobStatusToActor maps the wire integer status to the public actor job status
func protocolJobStatusToActor(s int) actor.JobStatus {
	switch s {
	case 1:
		return actor.JobStatusActive
	case 2:
		return actor.JobStatusDeadLettered
	default:
		return actor.JobStatusPending
	}
}

// protocolJobInfoToActor converts a wire JobInfo DTO to the public actor JobInfo
func protocolJobInfoToActor(j protocol.JobInfo) actor.JobInfo {
	out := actor.JobInfo{
		JobID:     j.JobID,
		ActorType: j.ActorType,
		ActorID:   j.ActorID,
		Method:    j.Method,
		Status:    protocolJobStatusToActor(j.Status),
		Interval:  j.Interval,
		Cron:      j.Cron,
		Attempts:  j.Attempts,
		LastError: j.LastError,
	}
	if j.DueTimeUnixMs > 0 {
		out.DueTime = time.UnixMilli(j.DueTimeUnixMs)
	}
	if j.CreatedAtUnixMs > 0 {
		out.CreatedAt = time.UnixMilli(j.CreatedAtUnixMs)
	}
	return out
}

// actorAlarmPropsToProtocol converts the public actor properties into the wire DTO sent to the runtime
func actorAlarmPropsToProtocol(o actor.AlarmProperties) (protocol.AlarmProperties, error) {
	p := protocol.AlarmProperties{
		DueTimeUnixMs: o.DueTime.UnixMilli(),
		Interval:      o.Interval,
	}

	// Encode the opaque alarm data to MessagePack
	if o.Data != nil {
		data, err := msgpack.Marshal(o.Data)
		if err != nil {
			return protocol.AlarmProperties{}, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}
		p.Data = data
	}

	// Only set the TTL when one was provided
	if !o.TTL.IsZero() {
		p.TTLUnixMs = o.TTL.UnixMilli()
	}

	return p, nil
}
