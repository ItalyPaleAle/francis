package host

import (
	"errors"
)

var (
	errActorHalted    = errors.New("actor is halted")
	errActorNotHosted = errors.New("actor is not active on the current host")
)
