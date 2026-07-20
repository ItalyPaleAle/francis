package components

import (
	"errors"
)

var (
	ErrAlreadyRunning           = errors.New("already running")
	ErrHostAlreadyRegistered    = errors.New("another host is already registered at the same address")
	ErrHostUnregistered         = errors.New("host is not registered")
	ErrJoinTokenAlreadyConsumed = errors.New("join token has already been used")
	ErrNoHost                   = errors.New("could not find a suitable host for this actor")
	ErrNoActor                  = errors.New("actor does not exist")
	ErrNoAlarm                  = errors.New("alarm does not exist")
	ErrNoJob                    = errors.New("job does not exist")
	ErrNoState                  = errors.New("no state found for the actor")
	ErrHostsConnected           = errors.New("one or more hosts are currently connected")
	ErrClusterFull              = errors.New("the cluster has reached its maximum number of hosts")
	ErrClusterLocked            = errors.New("the cluster is locked for exclusive access")
	ErrMaxHostsMismatch         = errors.New("host was configured with a different max hosts value than the rest of the cluster")
	ErrExclusiveHeld            = errors.New("another exclusive-access lease is currently held")
	ErrExclusiveNotSupported    = errors.New("the provider does not support exclusive-access leases")
)
