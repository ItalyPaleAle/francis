//go:build unit

package builtinactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
)

// fakeBuiltIn is a single-type built-in whose registration options the test controls
type fakeBuiltIn struct {
	opts actorcore.RegisterActorOptions
}

func (f *fakeBuiltIn) ActorType() string { return "fake" }

func (f *fakeBuiltIn) Factory() actor.Factory {
	return func(actorID string, svc *actor.Service) actor.Actor { return nil }
}

func (f *fakeBuiltIn) RegisterOptions() actorcore.RegisterActorOptions { return f.opts }

func (f *fakeBuiltIn) Singleton() bool { return false }

// fakeMultiBuiltIn registers more than one type, which takes the other branch of RegistrationsFor
type fakeMultiBuiltIn struct {
	fakeBuiltIn

	regs []BuiltInActorRegistration
}

func (f *fakeMultiBuiltIn) Registrations() []BuiltInActorRegistration { return f.regs }

// TestRegistrationsForAppliesJobRetentionDefault covers the one funnel every host registers a built-in through
//
// A built-in runs work on a caller's behalf without the caller holding a handle to it, so the record of a run that succeeded is the only way to see it happened
func TestRegistrationsForAppliesJobRetentionDefault(t *testing.T) {
	t.Run("a single-type built-in that names none gets the default", func(t *testing.T) {
		regs := RegistrationsFor(&fakeBuiltIn{})
		assert.Len(t, regs, 1)
		assert.Equal(t, DefaultJobRetention, regs[0].RegisterOptions.JobRetention)
	})

	t.Run("a built-in that names its own window keeps it", func(t *testing.T) {
		regs := RegistrationsFor(&fakeBuiltIn{opts: actorcore.RegisterActorOptions{JobRetention: 7 * 24 * time.Hour}})
		assert.Len(t, regs, 1)
		assert.Equal(t, 7*24*time.Hour, regs[0].RegisterOptions.JobRetention)
	})

	t.Run("every type of a multi-type built-in is covered", func(t *testing.T) {
		b := &fakeMultiBuiltIn{regs: []BuiltInActorRegistration{
			{ActorType: "a"},
			{ActorType: "b", RegisterOptions: actorcore.RegisterActorOptions{JobRetention: time.Hour}},
		}}

		regs := RegistrationsFor(b)
		assert.Len(t, regs, 2)
		assert.Equal(t, DefaultJobRetention, regs[0].RegisterOptions.JobRetention, "the type that named none gets the default")
		assert.Equal(t, time.Hour, regs[1].RegisterOptions.JobRetention, "the type that named its own keeps it")
	})
}
