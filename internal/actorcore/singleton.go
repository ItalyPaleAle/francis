package actorcore

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ref"
)

// singletonBootstrapMaxAttempts is how many times BootstrapSingletons tries to bootstrap each singleton actor
const singletonBootstrapMaxAttempts = 5

// singletonRegistration is a singleton actor type whose singleton instance the host bootstraps once ready
type singletonRegistration struct {
	actorType     string
	bootstrapData any
}

// RegisterSingletonActor registers an actor type like RegisterActor, and records it so BootstrapSingletons bootstraps its singleton instance
// The bootstrap data comes from the options' BootstrapData
func (m *Manager) RegisterSingletonActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if m.started.Load() {
		return errors.New("cannot call RegisterSingletonActor after the host has started")
	}

	err := m.RegisterActor(actorType, factory, opts)
	if err != nil {
		return err
	}

	m.singletons = append(m.singletons, singletonRegistration{
		actorType:     actorType,
		bootstrapData: opts.BootstrapData,
	})
	return nil
}

// SingletonActorTypes returns the singleton actor types registered with RegisterSingletonActor, in registration order
func (m *Manager) SingletonActorTypes() []string {
	out := make([]string, len(m.singletons))
	for i, reg := range m.singletons {
		out[i] = reg.actorType
	}
	return out
}

// BootstrapSingletons drives the Bootstrap hook of each registered singleton actor, and is called once the host is ready
// It invokes the reserved bootstrap lifecycle on the singleton instance through the privileged client, which routes to the owning host and serializes on that instance's turn lock
// It retries with a short backoff because an invocation can briefly fail right after startup (Bootstrap is idempotent, so retrying is safe)
// Bootstrap runs on the cluster-wide singleton instance, so it is harmless for every host to do this
func (m *Manager) BootstrapSingletons(ctx context.Context) {
	for _, reg := range m.singletons {
		at := reg.actorType

		// The privileged client is allowed to target reserved built-in types and to send the reserved bootstrap method, both of which the public client rejects
		client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, at, actor.SingletonActorID, m.service)
		for i := 1; ; i++ {
			invokeCtx, cancel := context.WithTimeout(ctx, m.providerRequestTimeout)
			_, err := client.Invoke(invokeCtx, at, actor.SingletonActorID, ref.MethodBootstrap, reg.bootstrapData)
			cancel()
			if err == nil {
				m.log.DebugContext(ctx, "Bootstrapped singleton actor", slog.String("actorType", at))
				break
			}

			if i >= singletonBootstrapMaxAttempts || ctx.Err() != nil {
				m.log.WarnContext(ctx, "Failed to bootstrap singleton actor", slog.String("actorType", at), slog.Any("error", err))
				break
			}

			// Back off before the next attempt, but stop promptly if the host is shutting down
			t := m.clock.NewTimer(time.Duration(i) * 500 * time.Millisecond)
			select {
			case <-t.C():
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}
}
