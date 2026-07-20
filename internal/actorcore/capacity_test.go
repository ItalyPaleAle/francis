package actorcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

func TestCapacitySemaphore(t *testing.T) {
	t.Run("admits up to the limit then rejects", func(t *testing.T) {
		s := newCapacitySemaphore(2)

		assert.True(t, s.tryAcquire())
		assert.True(t, s.tryAcquire())
		// The third acquire exceeds the limit and is rejected
		assert.False(t, s.tryAcquire())

		// Releasing a slot lets exactly one more acquire through
		s.release()
		assert.True(t, s.tryAcquire())
		assert.False(t, s.tryAcquire())
	})

	t.Run("release without a held slot is a no-op", func(t *testing.T) {
		s := newCapacitySemaphore(1)

		// An extra release must not make room beyond the limit
		s.release()
		assert.True(t, s.tryAcquire())
		assert.False(t, s.tryAcquire())
	})
}

func TestTryAcquireCapacity(t *testing.T) {
	noopFactory := func(string, *actor.Service) actor.Actor { return struct{}{} }

	t.Run("ungated type is always admitted", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterActor("plain", noopFactory, RegisterActorOptions{})
		require.NoError(t, err)

		release, admitted := m.TryAcquireCapacity("plain")
		require.True(t, admitted)
		require.NotNil(t, release)
		// Releasing an ungated slot is safe
		release()
	})

	t.Run("gated type enforces the strict limit", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterActor("gated", noopFactory, RegisterActorOptions{
			CapacityGroup:      "g",
			CapacityGroupLimit: 1,
		})
		require.NoError(t, err)

		release1, admitted1 := m.TryAcquireCapacity("gated")
		require.True(t, admitted1)

		// The group is full, so the next acquire is refused
		_, admitted2 := m.TryAcquireCapacity("gated")
		assert.False(t, admitted2)

		// Releasing frees the slot for another acquire
		release1()
		_, admitted3 := m.TryAcquireCapacity("gated")
		assert.True(t, admitted3)
	})

	t.Run("types in the same group share one budget", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterActor("a", noopFactory, RegisterActorOptions{CapacityGroup: "shared", CapacityGroupLimit: 1})
		require.NoError(t, err)
		err = m.RegisterActor("b", noopFactory, RegisterActorOptions{CapacityGroup: "shared", CapacityGroupLimit: 1})
		require.NoError(t, err)

		// A slot taken by type "a" leaves none for type "b" in the same group
		_, admittedA := m.TryAcquireCapacity("a")
		require.True(t, admittedA)
		_, admittedB := m.TryAcquireCapacity("b")
		assert.False(t, admittedB)
	})
}

func TestRegisterCapacityGroupValidation(t *testing.T) {
	noopFactory := func(string, *actor.Service) actor.Actor { return struct{}{} }

	t.Run("mismatched limits within a group are rejected", func(t *testing.T) {
		m := NewManager(Options{})
		err := m.RegisterActor("a", noopFactory, RegisterActorOptions{CapacityGroup: "g", CapacityGroupLimit: 2})
		require.NoError(t, err)

		err = m.RegisterActor("b", noopFactory, RegisterActorOptions{CapacityGroup: "g", CapacityGroupLimit: 3})
		require.Error(t, err)
	})

	t.Run("capacity group without a positive limit is rejected", func(t *testing.T) {
		var o RegisterActorOptions
		o.CapacityGroup = "g"
		err := o.Validate()
		require.Error(t, err)
	})
}
