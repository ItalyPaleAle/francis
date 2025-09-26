package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (h *Host) GetAlarm(ctx context.Context, actorType string, actorID string, name string) (actor.AlarmProperties, error) {
	res, err := h.actorProvider.GetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
	if errors.Is(err, components.ErrNoAlarm) {
		return actor.AlarmProperties{}, actor.ErrAlarmNotFound
	} else if err != nil {
		return actor.AlarmProperties{}, fmt.Errorf("failed to get alarm: %w", err)
	}

	return alarmPropertiesFromAlarmRes(res)
}

func (h *Host) SetAlarm(ctx context.Context, actorType string, actorID string, name string, properties actor.AlarmProperties) error {
	req, err := alarmPropertiesToAlarmReq(properties)
	if err != nil {
		return err
	}

	err = h.actorProvider.SetAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name), req)
	if err != nil {
		return fmt.Errorf("failed to set alarm: %w", err)
	}

	return nil
}

func (h *Host) DeleteAlarm(ctx context.Context, actorType string, actorID string, name string) error {
	err := h.actorProvider.DeleteAlarm(ctx, ref.NewAlarmRef(actorType, actorID, name))
	if errors.Is(err, components.ErrNoAlarm) {
		return actor.ErrAlarmNotFound
	} else if err != nil {
		return fmt.Errorf("failed to delete alarm: %w", err)
	}

	return nil
}

func alarmPropertiesFromAlarmRes(res components.GetAlarmRes) (actor.AlarmProperties, error) {
	o := actor.AlarmProperties{
		DueTime:  res.DueTime,
		Interval: res.Interval,
	}

	if len(res.Data) > 0 {
		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(res.Data))
		defer msgpack.PutDecoder(dec)
		err := dec.Decode(&o.Data)
		if err != nil {
			return actor.AlarmProperties{}, fmt.Errorf("failed to deserialize data using msgpack: %w", err)
		}
	}

	if res.TTL != nil {
		o.TTL = *res.TTL
	}

	return o, nil
}

func alarmPropertiesToAlarmReq(o actor.AlarmProperties) (components.SetAlarmReq, error) {
	req := components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  o.DueTime,
			Interval: o.Interval,
		},
	}

	if o.Data != nil {
		var data bytes.Buffer
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(&data)

		err := enc.Encode(o.Data)
		if err != nil {
			return components.SetAlarmReq{}, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}

		req.Data = data.Bytes()
	}

	if !o.TTL.IsZero() {
		req.TTL = &o.TTL
	}

	return req, nil
}
