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
