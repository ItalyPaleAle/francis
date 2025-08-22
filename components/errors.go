package components

import (
	"errors"
)

var (
	ErrAlreadyRunning        = errors.New("already running")
	ErrHostAlreadyRegistered = errors.New("another host is already registered at the same address")
	ErrHostUnregistered      = errors.New("host is not registered")
	ErrNoHost                = errors.New("could not find a suitable host for this actor")
	ErrNoActor               = errors.New("actor does not exist")
	ErrNoAlarm               = errors.New("alarm does not exist")
	ErrNoState               = errors.New("no state found for the actor")
)
