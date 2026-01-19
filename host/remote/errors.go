package remote

import "errors"

var (
	// ErrNotConnected is returned when the client is not connected to Francis
	ErrNotConnected = errors.New("not connected to Francis")
	// ErrNotRegistered is returned when the client is not registered with Francis
	ErrNotRegistered = errors.New("not registered with Francis")
	// ErrActorNotFound is returned when an actor is not found
	ErrActorNotFound = errors.New("actor not found")
	// ErrActorNotHosted is returned when an actor is not hosted locally
	ErrActorNotHosted = errors.New("actor is not hosted locally")
	// ErrActorHalted is returned when an actor is being halted
	ErrActorHalted = errors.New("actor is halted")
	// ErrActorTypeUnsupported is returned when an actor type is not supported
	ErrActorTypeUnsupported = errors.New("actor type is not supported")
	// ErrAlarmNotFound is returned when an alarm is not found
	ErrAlarmNotFound = errors.New("alarm not found")
	// ErrStateNotFound is returned when state is not found
	ErrStateNotFound = errors.New("state not found")
	// ErrAlreadyRunning is returned when the host is already running
	ErrAlreadyRunning = errors.New("host is already running")
)
