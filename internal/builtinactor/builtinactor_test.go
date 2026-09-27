package builtinactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
)

// fakeBuiltIn is a single-type built-in whose registration options the test controls
type fakeBuiltIn struct {
	opts actorcore.RegisterActorOptions
}

func (f *fakeBuiltIn) ActorType() string {
	return "fake"
}

func (f *fakeBuiltIn) Factory() actor.Factory {
	return func(actorID string, svc *actor.Service) actor.Actor {
		return nil
	}
}

func (f *fakeBuiltIn) RegisterOptions() actorcore.RegisterActorOptions {
	return f.opts
}

func (f *fakeBuiltIn) Singleton() bool {
	return false
}

// fakeMultiBuiltIn registers more than one type, which takes the other branch of RegistrationsFor
type fakeMultiBuiltIn struct {
	fakeBuiltIn

	regs []BuiltInActorRegistration
}

func (f *fakeMultiBuiltIn) Registrations() []BuiltInActorRegistration { return f.regs }

func TestRegistrationsForAppliesCompletedJobRetentionDefault(t *testing.T) {
	t.Run("a single-type built-in that names none gets the default", func(t *testing.T) {
		regs := RegistrationsFor(&fakeBuiltIn{})
		assert.Len(t, regs, 1)
		assert.Equal(t, DefaultCompletedJobRetention, regs[0].RegisterOptions.CompletedJobRetention)

		// Only the completed window is filled in here: the dead-lettered one is the framework's to default, which happens at registration
		assert.Zero(t, regs[0].RegisterOptions.DeadLetteredJobRetention)
	})

	t.Run("a built-in that names its own window keeps it", func(t *testing.T) {
		regs := RegistrationsFor(&fakeBuiltIn{opts: actorcore.RegisterActorOptions{CompletedJobRetention: 7 * 24 * time.Hour}})
		assert.Len(t, regs, 1)
		assert.Equal(t, 7*24*time.Hour, regs[0].RegisterOptions.CompletedJobRetention)
	})

	t.Run("a built-in that asked for no record of a success keeps that too", func(t *testing.T) {
		// A negative window is how a built-in says it wants no expiry, and the default must not overwrite a deliberate choice
		regs := RegistrationsFor(&fakeBuiltIn{opts: actorcore.RegisterActorOptions{CompletedJobRetention: -1}})
		assert.Len(t, regs, 1)
		assert.Equal(t, time.Duration(-1), regs[0].RegisterOptions.CompletedJobRetention)
	})

	t.Run("a built-in that set only its dead-lettered window still gets the completed default", func(t *testing.T) {
		regs := RegistrationsFor(&fakeBuiltIn{opts: actorcore.RegisterActorOptions{DeadLetteredJobRetention: time.Hour}})
		assert.Len(t, regs, 1)
		assert.Equal(t, DefaultCompletedJobRetention, regs[0].RegisterOptions.CompletedJobRetention, "the two windows are set independently")
		assert.Equal(t, time.Hour, regs[0].RegisterOptions.DeadLetteredJobRetention)
	})

	t.Run("every type of a multi-type built-in is covered", func(t *testing.T) {
		b := &fakeMultiBuiltIn{regs: []BuiltInActorRegistration{
			{ActorType: "a"},
			{ActorType: "b", RegisterOptions: actorcore.RegisterActorOptions{CompletedJobRetention: time.Hour}},
		}}

		regs := RegistrationsFor(b)
		assert.Len(t, regs, 2)
		assert.Equal(t, DefaultCompletedJobRetention, regs[0].RegisterOptions.CompletedJobRetention, "the type that named none gets the default")
		assert.Equal(t, time.Hour, regs[1].RegisterOptions.CompletedJobRetention, "the type that named its own keeps it")
	})
}

// fakeSingletonBuiltIn is a single-type built-in the host must bootstrap as a singleton
type fakeSingletonBuiltIn struct {
	fakeBuiltIn
}

func (f *fakeSingletonBuiltIn) Singleton() bool {
	return true
}

func TestRegister(t *testing.T) {
	t.Run("registers every type and returns the singleton ones", func(t *testing.T) {
		core := actorcore.NewManager(actorcore.Options{})
		b := &fakeMultiBuiltIn{regs: []BuiltInActorRegistration{
			{ActorType: "a", Factory: (&fakeBuiltIn{}).Factory()},
			{ActorType: "b", Factory: (&fakeBuiltIn{}).Factory(), Singleton: true},
		}}

		singletonTypes, err := Register(core, b)
		require.NoError(t, err)
		assert.Equal(t, []string{FullActorType("b")}, singletonTypes)
		assert.Contains(t, core.ActorFactories, FullActorType("a"))
		assert.Contains(t, core.ActorFactories, FullActorType("b"))
	})

	t.Run("a single-type singleton is returned", func(t *testing.T) {
		core := actorcore.NewManager(actorcore.Options{})

		singletonTypes, err := Register(core, &fakeSingletonBuiltIn{})
		require.NoError(t, err)
		assert.Equal(t, []string{FullActorType("fake")}, singletonTypes)
	})

	t.Run("a type repeated within the built-in registers none of it", func(t *testing.T) {
		core := actorcore.NewManager(actorcore.Options{})
		b := &fakeMultiBuiltIn{regs: []BuiltInActorRegistration{
			{ActorType: "a", Factory: (&fakeBuiltIn{}).Factory()},
			{ActorType: "a", Factory: (&fakeBuiltIn{}).Factory()},
		}}

		_, err := Register(core, b)
		require.ErrorIs(t, err, actorcore.ErrActorTypeAlreadyRegistered)
		assert.Empty(t, core.ActorFactories)
	})

	t.Run("a type already registered on the host registers none of the built-in", func(t *testing.T) {
		core := actorcore.NewManager(actorcore.Options{})
		_, err := Register(core, &fakeBuiltIn{})
		require.NoError(t, err)

		// The first type is new, so only an up-front check keeps it from being registered before the second one fails
		b := &fakeMultiBuiltIn{regs: []BuiltInActorRegistration{
			{ActorType: "new", Factory: (&fakeBuiltIn{}).Factory()},
			{ActorType: "fake", Factory: (&fakeBuiltIn{}).Factory()},
		}}
		_, err = Register(core, b)
		require.ErrorIs(t, err, actorcore.ErrActorTypeAlreadyRegistered)
		assert.NotContains(t, core.ActorFactories, FullActorType("new"))
	})

	t.Run("a nil built-in is rejected", func(t *testing.T) {
		core := actorcore.NewManager(actorcore.Options{})

		_, err := Register(core, nil)
		require.Error(t, err)
	})
}
