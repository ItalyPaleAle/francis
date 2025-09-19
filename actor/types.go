package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	timeutils "github.com/italypaleale/actors/internal/time"
)

type Duration = timeutils.Duration

type Host interface {
	Invoke(ctx context.Context, actorType string, actorID string, method string, data any) (Envelope, error)

	HaltAll() error
	Halt(actorType string, actorID string) error

	GetAlarm(ctx context.Context, actorType string, actorID string, name string) (AlarmProperties, error)
	SetAlarm(ctx context.Context, actorType string, actorID string, name string, properties AlarmProperties) error
	DeleteAlarm(ctx context.Context, actorType string, actorID string, name string) error

	SetState(ctx context.Context, actorType string, actorID string, state any, opts *SetStateOpts) error
	GetState(ctx context.Context, actorType string, actorID string, dest any) error
	DeleteState(ctx context.Context, actorType string, actorID string) error
}

// SetStateOpts is the options for the SetState method
type SetStateOpts struct {
	// Optional TTL for the state
	TTL time.Duration
}

// AlarmProperties contains the options for a new alarm.
type AlarmProperties struct {
	// Due time, as an absolute timestamp.
	// When parsed from JSON, it could be a RFC3339/ISO8601-formatted string, or a number indicating a UNIX timestamp in milliseconds
	DueTime time.Time `json:"dueTime"`
	// Alarm repetition interval, as an ISO8601-formatted string.
	// When parsed from JSON, it can be an ISO-formatted duration, a Go duration string, or a number in milliseconds.
	Interval string `json:"interval"`
	// Deadline for repeating alarms.
	// When parsed from JSON, it could be a RFC3339/ISO8601-formatted string, or a number indicating a UNIX timestamp in milliseconds
	TTL time.Time `json:"ttl"`
	// Optional data associated with the alarm.
	Data any `json:"data"`
}

// UnmarshalJSON implements custom unmarshaling for AlarmProperties.
func (a *AlarmProperties) UnmarshalJSON(data []byte) error {
	type Alias AlarmProperties
	aux := &struct {
		*Alias

		DueTime  any `json:"dueTime"`
		Interval any `json:"interval"`
		TTL      any `json:"ttl"`
		Data     any `json:"data"`
	}{
		Alias: (*Alias)(a),
	}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	// Parse DueTime
	a.DueTime, err = timeutils.ParseTime(aux.DueTime)
	if err != nil {
		return fmt.Errorf("invalid dueTime: %w", err)
	}

	// Parse Interval
	a.Interval, err = timeutils.ParseDuration(aux.Interval)
	if err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}

	// Parse TTL
	a.TTL, err = timeutils.ParseTime(aux.TTL)
	if err != nil {
		return fmt.Errorf("invalid ttl: %w", err)
	}

	a.Data = aux.Data
	return nil
}
