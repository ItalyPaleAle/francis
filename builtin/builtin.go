// Package builtin provides framework-managed actors that a host registers automatically
//
// Pass a built-in actor to a host with the WithBuiltInActor option (repeatable), available on both the local and remote hosts
// The host registers the actor before it starts and, once it is ready, bootstraps it by invoking a one-time "register" method that sets up the actor's durable work
//
// Built-in actors use a reserved actor-type prefix (see ref.BuiltInActorTypePrefix) that clients cannot target through the public Service, which rejects every built-in target with actor.ErrActorTypeReserved
// The host imposes no such restriction, so the framework reaches them through the in-actor client: registration and removal are invocations of the actor's Invoke method, while each scheduled occurrence rides on a durable job delivered to its Job method
package builtin

import (
	"context"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinkey"
)

const (
	// singletonActorID is the fixed actor ID used for built-in singleton actors, so every host targets the same cluster-wide instance
	singletonActorID = "singleton"

	// MethodRegister is the lifecycle method a host invokes once to set up the actor's durable work, and is a no-op afterwards
	MethodRegister = "register"
	// MethodUnregister is the lifecycle method that tears down the actor's durable work
	MethodUnregister = "unregister"
)

// BuiltInActor is a framework-managed actor that a host registers automatically and bootstraps at startup
// Construct one with a builder such as cronjob.New, then pass it to a host via WithBuiltInActor
type BuiltInActor struct {
	// actorType is the reserved actor type (carries ref.BuiltInActorTypePrefix)
	actorType string
	// factory builds the actor instance the host registers
	factory actor.Factory
	// regOpts are the registration options the host uses to register the actor
	regOpts actorcore.RegisterActorOptions
}

// NewBuiltInActor assembles a BuiltInActor from its reserved actor type, factory, and registration options
// Built-in actor builders such as cronjob.New use it to construct the value passed to a host via WithBuiltInActor
func NewBuiltInActor(actorType string, factory actor.Factory, regOpts actorcore.RegisterActorOptions) *BuiltInActor {
	return &BuiltInActor{
		actorType: actorType,
		factory:   factory,
		regOpts:   regOpts,
	}
}

// ActorType returns the reserved actor type registered for this built-in actor
func (b *BuiltInActor) ActorType() string {
	return b.actorType
}

// Factory returns the actor factory the host registers
func (b *BuiltInActor) Factory() actor.Factory {
	return b.factory
}

// RegisterOptions returns the registration options the host uses to register the actor
func (b *BuiltInActor) RegisterOptions() actorcore.RegisterActorOptions {
	return b.regOpts
}

// Bootstrap triggers the built-in actor's one-time registration by invoking its "register" method
// The host calls this once it is ready
// It is safe to call from every host: invocations of the cluster-wide singleton are serialized by its turn lock, and the register handler is idempotent
func (b *BuiltInActor) Bootstrap(ctx context.Context, svc *actor.Service) error {
	return b.invokeLifecycle(ctx, svc, MethodRegister)
}

// Unregister removes the built-in actor's scheduled work by invoking its "unregister" method
// After it runs the recurring job is cancelled and the actor's state is cleared, so a later Bootstrap re-registers it
func (b *BuiltInActor) Unregister(ctx context.Context, svc *actor.Service) error {
	return b.invokeLifecycle(ctx, svc, MethodUnregister)
}

// invokeLifecycle invokes one of the built-in actor's lifecycle methods on the cluster-wide singleton
// It goes through the privileged built-in client because the public Service and client both reject built-in targets
func (b *BuiltInActor) invokeLifecycle(ctx context.Context, svc *actor.Service, method string) error {
	client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, b.actorType, singletonActorID, svc)
	_, err := client.Invoke(ctx, b.actorType, singletonActorID, method, nil)
	return err
}
