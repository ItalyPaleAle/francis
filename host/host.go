// Package host defines the interface that the Francis actor hosts implement
//
// Both the local host (package host/local) and the remote host (package host/remote) satisfy Host, so an application can pick its topology at startup and keep the rest of its code unchanged
package host

import (
	"context"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// RegisterActorOption is a functional option for RegisterActor/RegisterSingletonActor
// It is an alias of the option type both hosts accept, so options built with local.With… or remote.With… can be passed to any Host
type RegisterActorOption = actorcore.RegisterActorOption

// Host is an actor host, in either the local or the remote topology
// Values of this interface are created with local.NewHost or remote.NewHost, and the concrete host packages expose the topology-specific construction options
type Host interface {
	// A host is also the transport the actor Service is built on, which is where invocations, state, alarms, and jobs live
	actor.Host

	// Service returns a Service object configured to interact with this host
	Service() *actor.Service

	// Run the host service
	// Note this function is blocking, and will return only when the service is shut down via context cancellation
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
