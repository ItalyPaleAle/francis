package components

import (
	"errors"
)

var (
	ErrHostAlreadyRegistered error = errors.New("another host is already registered at the same address")
	ErrHostUnregistered      error = errors.New("host is not registered")
	ErrNoHost                error = errors.New("could not find a suitable host for this actor")
	ErrNoActor               error = errors.New("actor does not exist")
	ErrNoAlarm               error = errors.New("alarm does not exist")
	ErrNoState               error = errors.New("no state found for the actor")
)
