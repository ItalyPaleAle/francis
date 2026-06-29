// Package builtin provides framework-managed actors that a host registers automatically.
//
// Pass a built-in actor to a host with the WithBuiltInActor option (repeatable), available on both
// the local and remote hosts. The host registers the actor before it starts and, once it is ready,
// bootstraps it by dispatching a one-time "register" job that sets up the actor's durable work.
//
// Built-in actors use a reserved actor-type prefix (see ref.BuiltInActorTypePrefix) and cannot be
// invoked directly by clients: Invoke and InvokeStream reject them with actor.ErrActorTypeReserved.
// Their register/run/unregister flow rides on durable jobs, which are delivered to the actor's Job
// method on a path separate from invocation.
package builtin

import (
	"context"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
)

const (
	// singletonActorID is the fixed actor ID used for built-in singleton actors, so every host targets the same cluster-wide instance.
	singletonActorID = "singleton"

	// methodRegister sets up the actor's durable work the first time it runs, and is a no-op afterwards.
	methodRegister = "register"
	// methodRun delivers each scheduled occurrence to the user-supplied job.
	methodRun = "run"
	// methodUnregister tears down the actor's durable work.
	methodUnregister = "unregister"
)

// BuiltInActor is a framework-managed actor that a host registers automatically and bootstraps at startup.
// Construct one with a builder such as NewCronJobActor, then pass it to a host via WithBuiltInActor.
type BuiltInActor struct {
	// actorType is the reserved actor type (carries ref.BuiltInActorTypePrefix)
	actorType string
	// factory builds the actor instance the host registers
	factory actor.Factory
	// regOpts are the registration options the host uses to register the actor
	regOpts actorcore.RegisterActorOptions
}

// ActorType returns the reserved actor type registered for this built-in actor.
func (b *BuiltInActor) ActorType() string {
	return b.actorType
}

// Factory returns the actor factory the host registers.
func (b *BuiltInActor) Factory() actor.Factory {
	return b.factory
}

// RegisterOptions returns the registration options the host uses to register the actor.
func (b *BuiltInActor) RegisterOptions() actorcore.RegisterActorOptions {
	return b.regOpts
}

// Bootstrap triggers the built-in actor's one-time registration by dispatching its "register" job.
// The host calls this once it is ready.
// It is safe to call from every host: the register handler is idempotent, and the idempotency key collapses concurrent dispatches into a single job.
func (b *BuiltInActor) Bootstrap(ctx context.Context, svc *actor.Service) error {
	_, err := svc.Dispatch(ctx, b.actorType, singletonActorID, methodRegister, nil, actor.WithIdempotencyKey(methodRegister))
	return err
}

// Unregister removes the built-in actor's scheduled work by dispatching its "unregister" job.
// After it runs the recurring job is cancelled and the actor's state is cleared, so a later Bootstrap re-registers it.
func (b *BuiltInActor) Unregister(ctx context.Context, svc *actor.Service) error {
	_, err := svc.Dispatch(ctx, b.actorType, singletonActorID, methodUnregister, nil, actor.WithIdempotencyKey(methodUnregister))
	return err
}
