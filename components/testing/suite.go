package comptesting

import (
	"bytes"
	"fmt"
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
	t.Run("update actor host", s.TestUpdateActorHost)
	t.Run("unregister host", s.TestUnregisterHost)

	t.Run("lookup actor", s.TestLookupActor)
	t.Run("remove actor", s.TestRemoveActor)

	t.Run("actor state", s.TestState)

	t.Run("fetch alarms", s.TestFetchAlarms)
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

func (s Suite) TestUpdateActorHost(t *testing.T) {
	expectHosts := func(t *testing.T, expectedHosts HostSpecCollection, expectedActorTypes HostActorTypeSpecCollection) {
		t.Helper()
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)
		assert.True(t, expectedHosts.Equal(spec.Hosts), "unexpected hosts: got=%v expected=%v", spec.Hosts, expectedHosts)
		assert.True(t, expectedActorTypes.Equal(spec.HostActorTypes), "unexpected actor types: got=%v expected=%v", spec.HostActorTypes, expectedActorTypes)
	}

	t.Run("update last health check only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Advance time to make host appear older
		s.p.AdvanceClock(30 * time.Second)

		// Update just the health check
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: true,
			ActorTypes:            nil, // Don't update actor types
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.NoError(t, err)

		// Verify host still exists with same actor types (health check updated internally)
		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.100:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res.HostID, ActorType: "TestActor", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 5},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("update actor types only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Update just the actor types
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: false, // Don't update health check
			ActorTypes: []components.ActorHostType{
				{ActorType: "UpdatedActor", IdleTimeout: 10 * time.Minute, ConcurrencyLimit: 10},
				{ActorType: "AnotherActor", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.NoError(t, err)

		// Verify host exists with updated actor types
		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.100:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res.HostID, ActorType: "UpdatedActor", ActorIdleTimeout: 10 * time.Minute, ActorConcurrencyLimit: 10},
			{HostID: res.HostID, ActorType: "AnotherActor", ActorIdleTimeout: 3 * time.Minute, ActorConcurrencyLimit: 2},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("update both health check and actor types", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Advance time
		s.p.AdvanceClock(30 * time.Second)

		// Update both health check and actor types
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: true,
			ActorTypes: []components.ActorHostType{
				{ActorType: "BothUpdatedActor", IdleTimeout: 15 * time.Minute, ConcurrencyLimit: 20},
			},
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.NoError(t, err)

		// Verify host exists with updated actor types and refreshed health check
		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.100:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res.HostID, ActorType: "BothUpdatedActor", ActorIdleTimeout: 15 * time.Minute, ActorConcurrencyLimit: 20},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("clear all actor types with empty slice", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host with actor types
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
				{ActorType: "AnotherActor", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Update with empty, non-nil actor types slice (should clear all)
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: false,
			ActorTypes:            []components.ActorHostType{},
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.NoError(t, err)

		// Verify host exists but has no actor types
		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.100:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{} // Empty
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("returns ErrHostUnregistered if host not registered while updating last health check only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Try to update a non-existent host - only last health check
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: true,
		}
		err := s.p.UpdateActorHost(ctx, "non-existent-host-id", updateReq)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("returns ErrHostUnregistered if host not registered while updating actor types only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Try to update a non-existent host - only actor types
		updateReq := components.UpdateActorHostReq{
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		err := s.p.UpdateActorHost(ctx, "non-existent-host-id", updateReq)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("returns ErrHostUnregistered if host is unhealthy while updating last health check only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Advance time to make host unhealthy (beyond 1 minute health check deadline)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to update the now-unhealthy host - only last health check
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: true,
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("returns ErrHostUnregistered if host is unhealthy while updating actor types only", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Advance time to make host unhealthy (beyond 1 minute health check deadline)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to update the now-unhealthy host - only actor types
		updateReq := components.UpdateActorHostReq{
			ActorTypes: []components.ActorHostType{
				{ActorType: "UpdatedActor", IdleTimeout: 10 * time.Minute, ConcurrencyLimit: 10},
			},
		}
		err = s.p.UpdateActorHost(ctx, res.HostID, updateReq)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})
}

func (s Suite) TestUnregisterHost(t *testing.T) {
	expectHosts := func(t *testing.T, expectedHosts HostSpecCollection, expectedActorTypes HostActorTypeSpecCollection) {
		t.Helper()
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)
		assert.True(t, expectedHosts.Equal(spec.Hosts), "unexpected hosts: got=%v expected=%v", spec.Hosts, expectedHosts)
		assert.True(t, expectedActorTypes.Equal(spec.HostActorTypes), "unexpected actor types: got=%v expected=%v", spec.HostActorTypes, expectedActorTypes)
	}

	t.Run("unregister healthy host", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
				{ActorType: "AnotherActor", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Verify host was registered
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 1, "should have one host registered")
		require.Len(t, spec.HostActorTypes, 2, "should have two actor types registered")

		// Unregister the host
		err = s.p.UnregisterHost(ctx, res.HostID)
		require.NoError(t, err)

		// Verify host and its actor types are gone
		expectHosts(t, HostSpecCollection{}, HostActorTypeSpecCollection{})
	})

	t.Run("returns ErrHostUnregistered if host not registered", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Try to unregister a non-existent host
		err := s.p.UnregisterHost(ctx, "non-existent-host-id")
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrHostUnregistered)
	})

	t.Run("returns ErrHostUnregistered but deletes unhealthy host", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register a host
		req := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res, err := s.p.RegisterHost(ctx, req)
		require.NoError(t, err)

		// Advance time to make host unhealthy (beyond 1 minute health check deadline)
		s.p.AdvanceClock(2 * time.Minute)

		// Unregister the now-unhealthy host - should return ErrHostUnregistered but still delete it
		err = s.p.UnregisterHost(ctx, res.HostID)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrHostUnregistered)

		// Verify host and its actor types are still deleted despite the error
		expectHosts(t, HostSpecCollection{}, HostActorTypeSpecCollection{})
	})

	t.Run("unregister one of multiple hosts", func(t *testing.T) {
		// Seed with empty database
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		ctx := t.Context()

		// Register two hosts
		req1 := components.RegisterHostReq{
			Address: "192.168.1.100:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TypeA", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 5},
			},
		}
		res1, err := s.p.RegisterHost(ctx, req1)
		require.NoError(t, err)

		req2 := components.RegisterHostReq{
			Address: "192.168.1.101:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TypeB", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 2},
			},
		}
		res2, err := s.p.RegisterHost(ctx, req2)
		require.NoError(t, err)

		// Verify both hosts exist
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		require.Len(t, spec.Hosts, 2, "should have two hosts registered")
		require.Len(t, spec.HostActorTypes, 2, "should have two actor types registered")

		// Unregister the first host
		err = s.p.UnregisterHost(ctx, res1.HostID)
		require.NoError(t, err)

		// Verify only second host remains
		expectedHosts := HostSpecCollection{
			{HostID: res2.HostID, Address: "192.168.1.101:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res2.HostID, ActorType: "TypeB", ActorIdleTimeout: 3 * time.Minute, ActorConcurrencyLimit: 2},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})
}

func (s Suite) TestLookupActor(t *testing.T) {
	t.Run("returns existing actor on healthy host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up an existing actor that's already active on a healthy host
		// From GetSpec: B-1 is active on H1 (healthy)
		ref := components.ActorRef{ActorType: "B", ActorID: "B-1"}
		res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.NoError(t, err)

		// Should return the existing host H1
		assert.Equal(t, "H1", res.HostID)
		assert.Equal(t, "127.0.0.1:4001", res.Address)
		assert.Equal(t, 5*time.Minute, res.IdleTimeout)
	})

	t.Run("creates new actor when not active", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create multiple actors to validate they're distributed across different hosts
		seenHosts := make(map[string]bool)
		for i := range 10 { // Try up to 10 times to see distribution
			ref := components.ActorRef{ActorType: "B", ActorID: fmt.Sprintf("B-new-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should place it on one of the healthy hosts that support B (H1, H2, or H3)
			assert.Contains(t, []string{"H1", "H2", "H3"}, res.HostID)
			assert.Contains(t, []string{"127.0.0.1:4001", "127.0.0.1:4002", "127.0.0.1:4003"}, res.Address)
			assert.Equal(t, 5*time.Minute, res.IdleTimeout)

			seenHosts[res.HostID] = true

			// If we've seen more than one host, we've validated distribution
			if len(seenHosts) > 1 {
				break
			}
		}

		// Should have distributed across multiple hosts
		assert.Greater(t, len(seenHosts), 1, "actors should be distributed across multiple hosts, but only saw: %v", seenHosts)
	})

	t.Run("replaces actor on unhealthy host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up an actor that exists only on unhealthy host H6
		// From GetSpec: D-1 is active on H6 (unhealthy), but D is only supported on H6
		// This should fail with ErrNoHost because D is not supported on any healthy host
		ref := components.ActorRef{ActorType: "D", ActorID: "D-1"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("respects host restrictions on active actor - allowed host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up actor B-1 which is active on H1, but restrict to only H1
		ref := components.ActorRef{ActorType: "B", ActorID: "B-1"}
		opts := components.LookupActorOpts{Hosts: []string{"H1"}}
		res, err := s.p.LookupActor(ctx, ref, opts)
		require.NoError(t, err)

		// Should return the existing actor on H1
		assert.Equal(t, "H1", res.HostID)
		assert.Equal(t, "127.0.0.1:4001", res.Address)
	})

	t.Run("respects host restrictions on active actor - disallowed host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up actor B-1 which is active on H1, but restrict to only H2
		// This should return ErrNoHost because the actor is on a disallowed host
		ref := components.ActorRef{ActorType: "B", ActorID: "B-1"}
		opts := components.LookupActorOpts{Hosts: []string{"H2"}}
		_, err := s.p.LookupActor(ctx, ref, opts)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("creates new actor with host restrictions", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create 3 actors of type C, but restrict to only H2
		// Type C has unlimited capacity so this should work
		for i := range 3 {
			ref := components.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-restricted-%d", i)}
			opts := components.LookupActorOpts{Hosts: []string{"H2"}}
			res, err := s.p.LookupActor(ctx, ref, opts)
			require.NoError(t, err)

			// Should always place it on H2 only
			assert.Equal(t, "H2", res.HostID)
			assert.Equal(t, "127.0.0.1:4002", res.Address)
			assert.Equal(t, 5*time.Minute, res.IdleTimeout)
		}
	})

	t.Run("returns ErrNoHost when no capacity available", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to create a new actor of type A
		// From GetSpec: A is at capacity on both H1 (3/3) and H2 (2/2)
		ref := components.ActorRef{ActorType: "A", ActorID: "A-new"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("creates unlimited actors on healthy hosts", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create 40 actors of type C (unlimited on H1 and H2) to validate distribution
		hostCounts := make(map[string]int)
		for i := range 40 {
			ref := components.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-unlimited-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should place it on one of the healthy hosts that support C (H1 or H2)
			assert.Contains(t, []string{"H1", "H2"}, res.HostID)
			assert.Contains(t, []string{"127.0.0.1:4001", "127.0.0.1:4002"}, res.Address)
			assert.Equal(t, 5*time.Minute, res.IdleTimeout)

			hostCounts[res.HostID]++
		}

		// Should have distributed across both hosts
		assert.Len(t, hostCounts, 2, "should distribute across both H1 and H2")

		// Validate approximately even distribution (at least 12 on each host out of 40 total)
		// This allows for some randomness while ensuring reasonable distribution
		h1Count := hostCounts["H1"]
		h2Count := hostCounts["H2"]

		assert.GreaterOrEqual(t, h1Count, 12, "H1 should have at least 12 actors for reasonable distribution, got %d", h1Count)
		assert.GreaterOrEqual(t, h2Count, 12, "H2 should have at least 12 actors for reasonable distribution, got %d", h2Count)
		assert.Equal(t, 40, h1Count+h2Count, "total should be 40 actors")

		t.Logf("Distribution: H1=%d, H2=%d", h1Count, h2Count)
	})

	t.Run("ignores unhealthy hosts for new actors", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create multiple actors of type C (unlimited capacity) to validate they never go to unhealthy hosts
		// Type C is supported on H1 and H2 (both healthy) but not on H5/H6 (unhealthy)
		seenHosts := make(map[string]bool)
		for i := range 10 { // Try multiple times to ensure consistent behavior
			ref := components.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-ignore-unhealthy-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should ONLY be placed on healthy hosts H1, H2 (where C is supported)
			assert.Contains(t, []string{"H1", "H2"}, res.HostID)
			assert.NotEqual(t, "H5", res.HostID) // H5 is unhealthy
			assert.NotEqual(t, "H6", res.HostID) // H6 is unhealthy
			assert.NotEqual(t, "H3", res.HostID) // H3 doesn't support C

			seenHosts[res.HostID] = true
		}

		// Should have used both healthy hosts (validation that distribution works)
		assert.Len(t, seenHosts, 2, "should distribute across both healthy hosts that support C: %v", seenHosts)
	})

	t.Run("returns ErrNoHost for unsupported actor type", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to create an actor of type "UNSUPPORTED"
		ref := components.ActorRef{ActorType: "UNSUPPORTED", ActorID: "unsupported-1"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("host restrictions with non-existent host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to create actor with restriction to non-existent host
		ref := components.ActorRef{ActorType: "B", ActorID: "B-nonexistent-host"}
		opts := components.LookupActorOpts{Hosts: []string{"NON-EXISTENT"}}
		_, err := s.p.LookupActor(ctx, ref, opts)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("validates capacity tracking and exhaustion", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// From GetSpec: Type A has capacity limits:
		// H1: supports A with capacity 3, currently has 3 active (at capacity)
		// H2: supports A with capacity 2, currently has 2 active (at capacity)
		// Total capacity for A is full (5/5)

		// Verify initial state - should already be at capacity
		_, err := s.p.LookupActor(ctx, components.ActorRef{ActorType: "A", ActorID: "A-should-fail"}, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost, "should fail when capacity is already exhausted")

		// Get initial host state to verify capacity tracking
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Find hosts that support type A and verify their active counts
		var h1ActiveCount, h2ActiveCount int
		for _, activeActor := range spec.ActiveActors {
			if activeActor.ActorType == "A" {
				switch activeActor.HostID {
				case "H1":
					h1ActiveCount++
				case "H2":
					h2ActiveCount++
				}
			}
		}

		// Verify initial capacity usage matches expected from GetSpec
		assert.Equal(t, 3, h1ActiveCount, "H1 should have 3 active A actors")
		assert.Equal(t, 2, h2ActiveCount, "H2 should have 2 active A actors")

		// Now let's create space by using a different actor type (B) to verify capacity tracking works
		// Create several B actors to fill up some capacity on hosts that also support A
		createdActors := 0
		for i := range 10 {
			ref := components.ActorRef{ActorType: "B", ActorID: fmt.Sprintf("B-capacity-test-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			if err != nil {
				break // Stop if we can't create more
			}
			createdActors++

			// Verify the actor was created on a valid host
			assert.Contains(t, []string{"H1", "H2", "H3"}, res.HostID)
		}

		// Verify we could create at least some B actors (B has unlimited capacity on some hosts)
		assert.Greater(t, createdActors, 0, "should be able to create B actors since they have unlimited capacity")

		// Verify that A is still at capacity after creating B actors
		_, err = s.p.LookupActor(ctx, components.ActorRef{ActorType: "A", ActorID: "A-still-should-fail"}, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost, "A should still be at capacity")

		// Get final state to verify capacity tracking
		finalSpec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Verify A actors are still at capacity (unchanged)
		var finalH1ACount, finalH2ACount int
		for _, activeActor := range finalSpec.ActiveActors {
			if activeActor.ActorType == "A" {
				switch activeActor.HostID {
				case "H1":
					finalH1ACount++
				case "H2":
					finalH2ACount++
				}
			}
		}

		assert.Equal(t, 3, finalH1ACount, "H1 should still have 3 active A actors")
		assert.Equal(t, 2, finalH2ACount, "H2 should still have 2 active A actors")

		// But we should have more total active actors due to the B actors we created
		assert.Greater(t, len(finalSpec.ActiveActors), len(spec.ActiveActors), "should have more total active actors after creating B actors")
	})
}

func (s Suite) TestRemoveActor(t *testing.T) {
	t.Run("removes existing active actor", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Verify initial state - B-1 should be active on H1
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Find B-1 in active actors
		var foundActor *ActiveActorSpec
		for _, aa := range spec.ActiveActors {
			if aa.ActorType == "B" && aa.ActorID == "B-1" {
				foundActor = &aa
				break
			}
		}
		require.NotNil(t, foundActor, "B-1 should exist in initial test data")
		assert.Equal(t, "H1", foundActor.HostID)

		// Remove the actor
		ref := components.ActorRef{ActorType: "B", ActorID: "B-1"}
		err = s.p.RemoveActor(ctx, ref)
		require.NoError(t, err)

		// Verify actor is no longer active
		spec, err = s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// B-1 should no longer be in active actors
		for _, aa := range spec.ActiveActors {
			if aa.ActorType == "B" && aa.ActorID == "B-1" {
				t.Fatalf("B-1 should have been removed but is still active on host %s", aa.HostID)
			}
		}
	})

	t.Run("returns ErrNoActor for non-existent actor", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to remove a non-existent actor
		ref := components.ActorRef{ActorType: "B", ActorID: "NonExistent"}
		err := s.p.RemoveActor(ctx, ref)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("returns ErrNoActor for non-existent actor type", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to remove an actor with non-existent type
		ref := components.ActorRef{ActorType: "NonExistentType", ActorID: "SomeID"}
		err := s.p.RemoveActor(ctx, ref)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("removes actor and frees up capacity", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// From GetSpec: Type A is at capacity (H1: 3/3, H2: 2/2)
		// First verify we can't create a new A actor
		_, err := s.p.LookupActor(ctx, components.ActorRef{ActorType: "A", ActorID: "A-should-fail"}, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost, "should fail when capacity is exhausted")

		// Remove one of the existing A actors (A-1 is on H1)
		ref := components.ActorRef{ActorType: "A", ActorID: "A-1"}
		err = s.p.RemoveActor(ctx, ref)
		require.NoError(t, err)

		// Now we should be able to create a new A actor
		res, err := s.p.LookupActor(ctx, components.ActorRef{ActorType: "A", ActorID: "A-new-after-removal"}, components.LookupActorOpts{})
		require.NoError(t, err)
		assert.NotEmpty(t, res.HostID)
		assert.Contains(t, []string{"H1", "H2"}, res.HostID, "should be placed on one of the hosts that support A")

		// Verify the capacity was freed up correctly by checking final state
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Count A actors on each host
		var h1Count, h2Count int
		for _, aa := range spec.ActiveActors {
			if aa.ActorType == "A" {
				switch aa.HostID {
				case "H1":
					h1Count++
				case "H2":
					h2Count++
				}
			}
		}

		// Should have same total capacity (5) but with the new actor instead of A-1
		assert.Equal(t, 5, h1Count+h2Count, "should still have 5 A actors total")

		// Verify A-1 is gone and A-new-after-removal exists
		hasA1, hasNewA := false, false
		for _, aa := range spec.ActiveActors {
			if aa.ActorType != "A" {
				continue
			}

			if aa.ActorID == "A-1" {
				hasA1 = true
			}
			if aa.ActorID == "A-new-after-removal" {
				hasNewA = true
			}
		}
		assert.False(t, hasA1, "A-1 should be removed")
		assert.True(t, hasNewA, "A-new-after-removal should exist")
	})

	t.Run("removes multiple actors", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Get initial count
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		initialCount := len(spec.ActiveActors)

		// Remove multiple actors
		actors := []components.ActorRef{
			{ActorType: "B", ActorID: "B-1"},
			{ActorType: "B", ActorID: "B-2"},
			{ActorType: "A", ActorID: "A-2"},
		}

		for _, ref := range actors {
			err = s.p.RemoveActor(ctx, ref)
			require.NoError(t, err, "should successfully remove actor %s", ref.String())
		}

		// Verify all actors were removed
		spec, err = s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		assert.Equal(t, initialCount-3, len(spec.ActiveActors), "should have 3 fewer active actors")

		// Verify none of the removed actors are still present
		for _, aa := range spec.ActiveActors {
			for _, ref := range actors {
				if aa.ActorType == ref.ActorType && aa.ActorID == ref.ActorID {
					t.Fatalf("Actor %s should have been removed but is still active", ref.String())
				}
			}
		}
	})

	t.Run("idempotent removal - removing same actor twice", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		ref := components.ActorRef{ActorType: "B", ActorID: "B-1"}

		// Remove the actor first time - should succeed
		err := s.p.RemoveActor(ctx, ref)
		require.NoError(t, err)

		// Remove the same actor second time - should return ErrNoActor
		err = s.p.RemoveActor(ctx, ref)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
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

func (s Suite) TestFetchAlarms(t *testing.T) {
	t.Run("fetches upcoming alarms without capacity constraints", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{"H7", "H8"},
		})
		require.NoError(t, err)
		fmt.Println("RES IS", res)
	})

	t.Run("fetches upcoming alarms with capacity constraints", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{"H1", "H2"},
		})
		require.NoError(t, err)
		fmt.Println("RES IS", res)
	})
}
