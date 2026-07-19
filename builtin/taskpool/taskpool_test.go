//go:build unit

package taskpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/builtinactor"
)

func TestNew(t *testing.T) {
	noop := func(context.Context, Task) error { return nil }

	t.Run("valid pool with defaults", func(t *testing.T) {
		p, err := New("media", WithHandler(noop))
		require.NoError(t, err)
		assert.Equal(t, taskPoolActorTypePrefix+"media", p.ActorType())
		assert.NotNil(t, p.Factory())
		assert.False(t, p.Singleton())

		// With no capabilities there is exactly one queue: the base queue
		regs := p.Registrations()
		require.Len(t, regs, 1)
		assert.Equal(t, taskPoolActorTypePrefix+"media", regs[0].ActorType)

		// The default strict limit is one, mirrored into both the capacity group and the coarse placement hint
		opts := p.RegisterOptions()
		assert.Equal(t, defaultConcurrency, opts.CapacityGroupLimit)
		assert.Equal(t, defaultConcurrency, opts.ConcurrencyLimit)
		assert.Equal(t, builtinactor.FullActorType(taskPoolActorTypePrefix+"media"), opts.CapacityGroup)
	})

	t.Run("registers one queue per capability plus the base", func(t *testing.T) {
		p, err := New("media",
			WithHandler(noop),
			WithConcurrency(4),
			WithCapability("gpu"),
			WithCapability("avx512"),
		)
		require.NoError(t, err)

		regs := p.Registrations()
		require.Len(t, regs, 3)

		// The base queue is always first
		assert.Equal(t, taskPoolActorTypePrefix+"media", regs[0].ActorType)
		assert.Equal(t, taskPoolActorTypePrefix+"media.gpu", regs[1].ActorType)
		assert.Equal(t, taskPoolActorTypePrefix+"media.avx512", regs[2].ActorType)

		// Every queue shares the same capacity group and strict limit
		group := builtinactor.FullActorType(taskPoolActorTypePrefix + "media")
		for _, reg := range regs {
			assert.Equal(t, group, reg.RegisterOptions.CapacityGroup)
			assert.Equal(t, 4, reg.RegisterOptions.CapacityGroupLimit)
			assert.False(t, reg.Singleton)
		}
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := New("", WithHandler(noop))
		require.Error(t, err)
	})

	t.Run("rejects name with slash", func(t *testing.T) {
		_, err := New("a/b", WithHandler(noop))
		require.Error(t, err)
	})

	t.Run("rejects missing handler", func(t *testing.T) {
		_, err := New("media")
		require.Error(t, err)
	})

	t.Run("rejects empty capability", func(t *testing.T) {
		_, err := New("media", WithHandler(noop), WithCapability(""))
		require.Error(t, err)
	})

	t.Run("rejects capability with slash", func(t *testing.T) {
		_, err := New("media", WithHandler(noop), WithCapability("a/b"))
		require.Error(t, err)
	})

	t.Run("rejects duplicate capability", func(t *testing.T) {
		_, err := New("media", WithHandler(noop), WithCapability("gpu"), WithCapability("gpu"))
		require.Error(t, err)
	})
}

func TestCapabilityFromType(t *testing.T) {
	noop := func(context.Context, Task) error { return nil }
	p, err := New("media", WithHandler(noop), WithCapability("gpu"))
	require.NoError(t, err)

	// The base queue maps back to an empty capability
	assert.Equal(t, "", p.capabilityFromType(builtinactor.FullActorType(taskPoolActorTypePrefix+"media")))
	// A capability queue maps back to its capability
	assert.Equal(t, "gpu", p.capabilityFromType(builtinactor.FullActorType(taskPoolActorTypePrefix+"media.gpu")))
}
