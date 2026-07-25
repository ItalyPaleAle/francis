package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	timeutils "github.com/italypaleale/francis/internal/time"
	"github.com/italypaleale/francis/internal/types"
)

type Host interface {
	Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error)
	InvokeStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...InvokeOption) (respContentType string, resp io.ReadCloser, err error)

	Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...InvokeOption) (Envelope, error)
	PeekStream(ctx context.Context, actorType string, actorID string, method string, reqContentType string, body io.Reader, opts ...InvokeOption) (respContentType string, resp io.ReadCloser, err error)

	HaltAll() error
	Halt(actorType string, actorID string) error
	HaltDeferred(actorType string, actorID string)

	GetAlarm(ctx context.Context, actorType string, actorID string, name string) (AlarmProperties, error)
	SetAlarm(ctx context.Context, actorType string, actorID string, name string, properties AlarmProperties) error
	DeleteAlarm(ctx context.Context, actorType string, actorID string, name string) error

	Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, properties JobProperties) (jobID string, err error)
	GetJob(ctx context.Context, jobID string) (JobInfo, error)
	ListJobs(ctx context.Context, actorType string, actorID string) ([]JobInfo, error)
	CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error
	RetryJob(ctx context.Context, jobID string) (newJobID string, err error)

	SetState(ctx context.Context, actorType string, actorID string, state any, opts *SetStateOpts) error
	GetState(ctx context.Context, actorType string, actorID string, dest any) error
	DeleteState(ctx context.Context, actorType string, actorID string) error
	ListStates(ctx context.Context, actorType string, opts *ListStatesOpts) (StateList, error)
}

// SetStateOpts is the options for the SetState method
type SetStateOpts struct {
	// Optional TTL for the state
	TTL time.Duration
}

// ListStatesOpts is the options for the ListStates method
type ListStatesOpts struct {
	// When true, the stored state is returned alongside each actor ID
	IncludeData bool
	// Pagination cursor: only actor IDs sorting strictly after this value are returned
	After string
	// Maximum number of states to return
	// If empty, requests the default page size
	Limit int
}

// StateList is a page of actor states returned by ListStates.
type StateList struct {
	// States in this page, ordered by actor ID in ascending order
	States []StateInfo
	// HasMore is true when more states exist after the last one in this page
	HasMore bool
}

// AfterID returns the cursor to set as ListStatesOpts.After to retrieve the page following this one.
// It is empty when this page is the last one, so a loop that pages until it gets an empty cursor visits every state exactly once.
func (l StateList) AfterID() string {
	if !l.HasMore || len(l.States) == 0 {
		return ""
	}

	return l.States[len(l.States)-1].ActorID
}

// StateInfo describes the stored state of a single actor.
type StateInfo struct {
	// ID of the actor the state belongs to
	ActorID string
	// Data decodes the stored state
	// It is nil when the actor's stored state is empty or when the listing didn't request the data
	Data Envelope
}

// TypedStateList is a page of actor states whose data has been decoded into T, returned by the ListStates method of Client.
type TypedStateList[T any] struct {
	// States in this page, ordered by actor ID in ascending order
	States []TypedStateInfo[T]
	// HasMore is true when more states exist after the last one in this page
	HasMore bool
}

// AfterID returns the cursor to set as ListStatesOpts.After to retrieve the page following this one.
// It is empty when this page is the last one, so a loop that pages until it gets an empty cursor visits every state exactly once.
func (l TypedStateList[T]) AfterID() string {
	if !l.HasMore || len(l.States) == 0 {
		return ""
	}

	return l.States[len(l.States)-1].ActorID
}

// TypedStateInfo describes the stored state of a single actor, decoded into T.
type TypedStateInfo[T any] struct {
	// ID of the actor the state belongs to
	ActorID string
	// Decoded state of the actor
	// It is the zero value when when the actor's stored state is empty or when the listing didn't request the data
	Data T
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

// Validate checks that AlarmProperties fields are well-formed.
// In particular it rejects a non-empty Interval that cannot be parsed or that would produce a zero repeat period.
func (a *AlarmProperties) Validate() error {
	if a.Interval == "" {
		return nil
	}

	d, err := timeutils.ParseISO8601Duration(a.Interval)
	if err != nil {
		return fmt.Errorf("invalid alarm interval: %w", err)
	}

	if d.IsZero() {
		return errors.New("alarm interval must be greater than zero")
	}

	return nil
}

type InvokeOption func(*types.InvokeOpts)

// WithInvokeActiveOnly causes the invocation to not allocate an actor if it isn't already active
func WithInvokeActiveOnly() InvokeOption {
	return func(o *types.InvokeOpts) {
		o.ActiveOnly = true
	}
}
