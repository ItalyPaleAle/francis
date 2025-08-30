package comptesting

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/actors/components"
)

// Suite implements a test suite for actor provider components.
type Suite struct {
	p ActorProviderTesting
}

func NewSuite(p ActorProviderTesting) *Suite {
	return &Suite{p: p}
}

func (s Suite) Run(t *testing.T) {
	t.Run("register host", s.TestRegisterHost)
	t.Run("actor state", s.TestState)
}

func (s Suite) TestRegisterHost(t *testing.T) {
	expectHosts := func(t *testing.T, expectedHosts HostSpecCollection, expectedActorTypes HostActorTypeSpecCollection) {
		t.Helper()
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		actualHosts := HostSpecCollection(spec.Hosts)
		actualActorTypes := HostActorTypeSpecCollection(spec.HostActorTypes)

		assert.True(t, expectedHosts.Equal(actualHosts), "unexpected host collection: got=%v expected=%v", actualHosts, expectedHosts)
		assert.True(t, expectedActorTypes.Equal(actualActorTypes), "unexpected host actor type collection: got=%v expected=%v", actualActorTypes, expectedActorTypes)
	}

	t.Run("register new host with actor types", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{
					ActorType:           "TestActor",
					IdleTimeout:         5 * time.Minute,
					ConcurrencyLimit:    10,
					DeactivationTimeout: 30 * time.Second,
				},
				{
					ActorType:           "AnotherActor",
					IdleTimeout:         2 * time.Minute,
					ConcurrencyLimit:    0, // unlimited
					DeactivationTimeout: 15 * time.Second,
				},
			},
		}

		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)
		assert.NotEmpty(t, res.HostID)

		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.100:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res.HostID, ActorType: "TestActor", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 10},
			{HostID: res.HostID, ActorType: "AnotherActor", ActorIdleTimeout: 2 * time.Minute, ActorConcurrencyLimit: 0},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("cannot register host with same address if healthy", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register first host
		req1 := components.RegisterHostReq{
			Address: "192.168.1.101:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}

		res1, err := s.p.RegisterHost(ctx, req1)
		require.NoError(t, err)

		// Try to register second host with same address immediately (should fail since first host is healthy)
		req2 := components.RegisterHostReq{
			Address: "192.168.1.101:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "DifferentActor", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 8},
			},
		}

		_, err = s.p.RegisterHost(ctx, req2)
		require.ErrorIs(t, err, components.ErrHostAlreadyRegistered)

		// Verify only first host still exists with original actor types
		expectedHosts := HostSpecCollection{
			{HostID: res1.HostID, Address: "192.168.1.101:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res1.HostID, ActorType: "TestActor", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 5},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("can override unhealthy host with same address", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register first host
		req1 := components.RegisterHostReq{
			Address: "192.168.1.102:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "OldActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}

		res1, err := s.p.RegisterHost(ctx, req1)
		require.NoError(t, err)

		// Make the host unhealthy by advancing clock beyond health check deadline
		s.p.AdvanceClock(2 * time.Minute) // Assuming health check deadline is 1 minute

		// Register second host with same address but different actor types
		req2 := components.RegisterHostReq{
			Address: "192.168.1.102:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "NewActor", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 8},
			},
		}

		res2, err := s.p.RegisterHost(ctx, req2)
		require.NoError(t, err)
		assert.NotEmpty(t, res2.HostID)
		assert.NotEqual(t, res1.HostID, res2.HostID, "should get new host ID")

		// Verify only new host exists with new actor types
		expectedHosts := HostSpecCollection{
			{HostID: res2.HostID, Address: "192.168.1.102:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res2.HostID, ActorType: "NewActor", ActorIdleTimeout: 3 * time.Minute, ActorConcurrencyLimit: 8},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("register host with no actor types", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		req := components.RegisterHostReq{
			Address:    "192.168.1.103:8080",
			ActorTypes: []components.ActorHostType{}, // empty slice
		}

		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)
		assert.NotEmpty(t, res.HostID)

		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.103:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{} // empty
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("unhealthy hosts and their actor types are cleaned up", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register multiple hosts with different actor types
		req1 := components.RegisterHostReq{
			Address: "192.168.1.104:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TypeA", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 10},
				{ActorType: "TypeB", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		_, err := s.p.RegisterHost(ctx, req1)
		require.NoError(t, err)

		req2 := components.RegisterHostReq{
			Address: "192.168.1.105:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TypeC", IdleTimeout: 2 * time.Minute, ConcurrencyLimit: 0},
			},
		}
		_, err = s.p.RegisterHost(ctx, req2)
		require.NoError(t, err)

		// Verify both hosts and all actor types exist
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		assert.Len(t, spec.Hosts, 2, "should have two hosts")
		assert.Len(t, spec.HostActorTypes, 3, "should have three actor types total")

		// Advance time to make hosts unhealthy (beyond 1 minute health check deadline)
		s.p.AdvanceClock(2 * time.Minute)

		// Register a new host - this should clean up all unhealthy hosts
		req3 := components.RegisterHostReq{
			Address: "192.168.1.106:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TypeD", IdleTimeout: 4 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		res3, err := s.p.RegisterHost(ctx, req3)
		require.NoError(t, err)

		// Verify old unhealthy hosts and their actor types are gone, only new host remains
		expectedHosts := HostSpecCollection{
			{HostID: res3.HostID, Address: "192.168.1.106:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res3.HostID, ActorType: "TypeD", ActorIdleTimeout: 4 * time.Minute, ActorConcurrencyLimit: 2},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})
}

func (s Suite) TestState(t *testing.T) {
	expectCollection := func(t *testing.T, expected ActorStateSpecCollection) {
		t.Helper()
		rows, err := s.p.GetAllActorState(t.Context())
		require.NoError(t, err)
		assert.True(t, expected.Equal(rows), "unexpected actor state collection: got=%v expected=%v", rows, expected)
	}

	// Seed with empty database
	require.NoError(t, s.p.Seed(t.Context(), Spec{}))

	t.Run("get returns ErrNoState if no state", func(t *testing.T) {
		_, err := s.p.GetState(t.Context(), components.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("delete returns ErrNoState if no state", func(t *testing.T) {
		err := s.p.DeleteState(t.Context(), components.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("set get overwrite delete", func(t *testing.T) {
		ctx := t.Context()
		ref := components.ActorRef{ActorType: "TestType", ActorID: "actor-1"}

		data1 := []byte("hello world")
		err := s.p.SetState(ctx, ref, data1, components.SetStateOpts{})
		require.NoError(t, err)

		got, err := s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data1, got))
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: data1}})

		data2 := []byte("goodbye")
		err = s.p.SetState(ctx, ref, data2, components.SetStateOpts{})
		require.NoError(t, err)

		got, err = s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(data2, got))
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: data2}})

		err = s.p.SetState(ctx, ref, []byte{}, components.SetStateOpts{})
		require.NoError(t, err)

		got, err = s.p.GetState(ctx, ref)
		require.NoError(t, err)
		assert.Len(t, got, 0)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref.ActorType, ActorID: ref.ActorID, Data: []byte{}}})

		err = s.p.DeleteState(ctx, ref)
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref)
		require.ErrorIs(t, err, components.ErrNoState)
		expectCollection(t, ActorStateSpecCollection{})

		err = s.p.DeleteState(ctx, ref)
		require.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("ttl expiration", func(t *testing.T) {
		ctx := t.Context()
		ref2 := components.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-1"}
		data := []byte("with-ttl")

		err := s.p.SetState(ctx, ref2, data, components.SetStateOpts{TTL: time.Second})
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref2)
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref2.ActorType, ActorID: ref2.ActorID, Data: data}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		err = s.p.CleanupExpired()
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref2)
		require.ErrorIs(t, err, components.ErrNoState)
		expectCollection(t, ActorStateSpecCollection{})
	})

	t.Run("ttl extension on overwrite", func(t *testing.T) {
		ctx := t.Context()
		ref3 := components.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-extend"}
		data1 := []byte("first")
		data2 := []byte("second")

		err := s.p.SetState(ctx, ref3, data1, components.SetStateOpts{TTL: 2 * time.Second})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data1}})

		s.p.AdvanceClock(time.Second)
		err = s.p.SetState(ctx, ref3, data2, components.SetStateOpts{TTL: 2 * time.Second})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		_, err = s.p.GetState(ctx, ref3)
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		s.p.AdvanceClock(1200 * time.Millisecond)
		_, err = s.p.GetState(ctx, ref3)
		require.ErrorIs(t, err, components.ErrNoState)

		// GC hasn't run yet
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		err = s.p.CleanupExpired()
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{})
	})
}
