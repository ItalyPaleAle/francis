package local

import (
	"errors"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
)

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions = actorcore.RegisterActorOptions

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}

	return h.core.RegisterActor(actorType, factory, opts)
}
