// Package host defines the interface that the Francis actor hosts implement
//
// Both the local host (package host/local) and the remote host (package host/remote) satisfy Host, so an application can pick its topology at startup and keep the rest of its code unchanged
package host

import (
	"context"
	"errors"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// RegisterActorOption is a functional option for RegisterActor/RegisterSingletonActor
// It is an alias of the option type both hosts accept, so options built with local.With… or remote.With… can be passed to any Host
type RegisterActorOption = actorcore.RegisterActorOption

// ErrAdministrativeDrain is returned by Host.Run after the host was drained through the management API
// The host has stopped serving, deactivated its actors, and unregistered, so the application should exit its process; a replacement process registers as a new host
var ErrAdministrativeDrain = errors.New("host was drained by an administrator")

// Host is an actor host, in either the local or the remote topology
// Values of this interface are created with local.NewHost or remote.NewHost, and the concrete host packages expose the topology-specific construction options
type Host interface {
	// A host is also the transport the actor Service is built on, which is where invocations, state, alarms, and jobs live
	actor.Host

	// Service returns a Service object configured to interact with this host
	Service() *actor.Service

	// Run the host service
	// Note this function is blocking, and returns when the service is shut down via context cancellation, when it fails, or after an administrative drain
	// After an administrative drain it returns an error that matches ErrAdministrativeDrain with errors.Is, so the application can tell a requested drain apart and exit its process
	// A host that was drained cannot be run again: a later call returns ErrAdministrativeDrain immediately
	Run(ctx context.Context) error

	// Ready returns a channel that is closed once the host has joined the cluster for the first time and can serve invocations
	Ready() <-chan struct{}

	// HostID returns the current ID of the host, or empty if the host has not joined the cluster yet
	HostID() string

	// RegisterActor registers a new actor in the host
	// Must be called before Run
	RegisterActor(actorType string, factory actor.Factory, opts ...RegisterActorOption) error

	// RegisterSingletonActor registers a singleton actor in the host
	// Must be called before Run, and can be called multiple times to register more than one singleton actor
	RegisterSingletonActor(actorType string, factory actor.Factory, opts ...RegisterActorOption) error

	// RegisterBuiltInActor registers a framework-managed built-in actor on the host, such as one created with cronjob.New
	// Must be called before Run, and can be called multiple times to register more than one built-in actor
	RegisterBuiltInActor(b builtinactor.BuiltInActor) error
}
