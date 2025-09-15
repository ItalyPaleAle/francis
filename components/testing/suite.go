package comptesting

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
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
	t.Run("get leased alarm", s.TestGetLeasedAlarm)
	t.Run("renew alarm leases", s.TestRenewAlarmLeases)
	t.Run("release alarm lease", s.TestReleaseAlarmLease)
	t.Run("update leased alarm", s.TestUpdateLeasedAlarm)
	t.Run("delete leased alarm", s.TestDeleteLeasedAlarm)
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
					ActorType:        "TestActor",
					IdleTimeout:      5 * time.Minute,
					ConcurrencyLimit: 10,
				},
				{
					ActorType:        "AnotherActor",
					IdleTimeout:      2 * time.Minute,
					ConcurrencyLimit: 0, // unlimited
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
		err := s.p.UpdateActorHost(ctx, SpecHostNonExistent, updateReq)
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
		err := s.p.UpdateActorHost(ctx, SpecHostNonExistent, updateReq)
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
		err := s.p.UnregisterHost(ctx, SpecHostNonExistent)
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
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
		res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.NoError(t, err)

		// Should return the existing host H1
		assert.Equal(t, SpecHostH1, res.HostID)
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
			ref := ref.ActorRef{ActorType: "B", ActorID: fmt.Sprintf("B-new-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should place it on one of the healthy hosts that support B (H1, H2, or H3)
			assert.Contains(t, []string{SpecHostH1, SpecHostH2, SpecHostH3}, res.HostID)
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
		ref := ref.ActorRef{ActorType: "D", ActorID: "D-1"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("respects host restrictions on active actor - allowed host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up actor B-1 which is active on H1, but restrict to only H1
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
		opts := components.LookupActorOpts{Hosts: []string{SpecHostH1}}
		res, err := s.p.LookupActor(ctx, ref, opts)
		require.NoError(t, err)

		// Should return the existing actor on H1
		assert.Equal(t, SpecHostH1, res.HostID)
		assert.Equal(t, "127.0.0.1:4001", res.Address)
	})

	t.Run("respects host restrictions on active actor - disallowed host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up actor B-1 which is active on H1, but restrict to only H2
		// This should return ErrNoHost because the actor is on a disallowed host
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
		opts := components.LookupActorOpts{Hosts: []string{SpecHostH2}}
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
			ref := ref.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-restricted-%d", i)}
			opts := components.LookupActorOpts{Hosts: []string{SpecHostH2}}
			res, err := s.p.LookupActor(ctx, ref, opts)
			require.NoError(t, err)

			// Should always place it on H2 only
			assert.Equal(t, SpecHostH2, res.HostID)
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
		ref := ref.ActorRef{ActorType: "A", ActorID: "A-new"}
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
			ref := ref.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-unlimited-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should place it on one of the healthy hosts that support C (H1 or H2)
			assert.Contains(t, []string{SpecHostH1, SpecHostH2}, res.HostID)
			assert.Contains(t, []string{"127.0.0.1:4001", "127.0.0.1:4002"}, res.Address)
			assert.Equal(t, 5*time.Minute, res.IdleTimeout)

			hostCounts[res.HostID]++
		}

		// Should have distributed across both hosts
		assert.Len(t, hostCounts, 2, "should distribute across both H1 and H2")

		// Validate approximately even distribution (at least 12 on each host out of 40 total)
		// This allows for some randomness while ensuring reasonable distribution
		h1Count := hostCounts[SpecHostH1]
		h2Count := hostCounts[SpecHostH2]

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
			ref := ref.ActorRef{ActorType: "C", ActorID: fmt.Sprintf("C-ignore-unhealthy-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			require.NoError(t, err)

			// Should ONLY be placed on healthy hosts H1, H2 (where C is supported)
			assert.Contains(t, []string{SpecHostH1, SpecHostH2}, res.HostID)
			assert.NotEqual(t, SpecHostH5, res.HostID) // H5 is unhealthy
			assert.NotEqual(t, SpecHostH6, res.HostID) // H6 is unhealthy
			assert.NotEqual(t, SpecHostH3, res.HostID) // H3 doesn't support C

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
		ref := ref.ActorRef{ActorType: "UNSUPPORTED", ActorID: "unsupported-1"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost)
	})

	t.Run("host restrictions with non-existent host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to create actor with restriction to non-existent host
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-nonexistent-host"}
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
		_, err := s.p.LookupActor(ctx, ref.ActorRef{ActorType: "A", ActorID: "A-should-fail"}, components.LookupActorOpts{})
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
				case SpecHostH1:
					h1ActiveCount++
				case SpecHostH2:
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
			ref := ref.ActorRef{ActorType: "B", ActorID: fmt.Sprintf("B-capacity-test-%d", i)}
			res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
			if err != nil {
				break // Stop if we can't create more
			}
			createdActors++

			// Verify the actor was created on a valid host
			assert.Contains(t, []string{SpecHostH1, SpecHostH2, SpecHostH3}, res.HostID)
		}

		// Verify we could create at least some B actors (B has unlimited capacity on some hosts)
		assert.Greater(t, createdActors, 0, "should be able to create B actors since they have unlimited capacity")

		// Verify that A is still at capacity after creating B actors
		_, err = s.p.LookupActor(ctx, ref.ActorRef{ActorType: "A", ActorID: "A-still-should-fail"}, components.LookupActorOpts{})
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
				case SpecHostH1:
					finalH1ACount++
				case SpecHostH2:
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
		assert.Equal(t, SpecHostH1, foundActor.HostID)

		// Remove the actor
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
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
		ref := ref.ActorRef{ActorType: "B", ActorID: "NonExistent"}
		err := s.p.RemoveActor(ctx, ref)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("returns ErrNoActor for non-existent actor type", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to remove an actor with non-existent type
		ref := ref.ActorRef{ActorType: "NonExistentType", ActorID: "SomeID"}
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
		_, err := s.p.LookupActor(ctx, ref.ActorRef{ActorType: "A", ActorID: "A-should-fail"}, components.LookupActorOpts{})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoHost, "should fail when capacity is exhausted")

		// Remove one of the existing A actors (A-1 is on H1)
		aRef := ref.ActorRef{ActorType: "A", ActorID: "A-1"}
		err = s.p.RemoveActor(ctx, aRef)
		require.NoError(t, err)

		// Now we should be able to create a new A actor
		res, err := s.p.LookupActor(ctx, ref.ActorRef{ActorType: "A", ActorID: "A-new-after-removal"}, components.LookupActorOpts{})
		require.NoError(t, err)
		assert.NotEmpty(t, res.HostID)
		assert.Contains(t, []string{SpecHostH1, SpecHostH2}, res.HostID, "should be placed on one of the hosts that support A")

		// Verify the capacity was freed up correctly by checking final state
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Count A actors on each host
		var h1Count, h2Count int
		for _, aa := range spec.ActiveActors {
			if aa.ActorType == "A" {
				switch aa.HostID {
				case SpecHostH1:
					h1Count++
				case SpecHostH2:
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
		actors := []ref.ActorRef{
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

		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}

		// Remove the actor first time - should succeed
		err := s.p.RemoveActor(ctx, ref)
		require.NoError(t, err)

		// Remove the same actor second time - should return ErrNoActor
		err = s.p.RemoveActor(ctx, ref)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("automatically cancels alarm leases when actor is removed", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create a new actor and set an alarm for it
		aRef := ref.ActorRef{ActorType: "X", ActorID: "X-lease-test"}

		// First create the actor by looking it up (this activates it)
		lookupRes, err := s.p.LookupActor(ctx, aRef, components.LookupActorOpts{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		assert.Contains(t, []string{SpecHostH7, SpecHostH8}, lookupRes.HostID)

		// Set an alarm for this actor
		alarmRef := ref.AlarmRef{
			ActorType: aRef.ActorType,
			ActorID:   aRef.ActorID,
			Name:      "test-alarm",
		}
		alarmReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				// Overdue so it's fetched right away
				DueTime: s.p.Now().Add(-time.Second),
			},
		}
		err = s.p.SetAlarm(ctx, alarmRef, alarmReq)
		require.NoError(t, err)

		// Fetch and lease the alarm
		fetchRes, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{lookupRes.HostID},
		})
		require.NoError(t, err)

		// Find our specific alarm lease
		var targetLease *ref.AlarmLease
		for _, lease := range fetchRes {
			alarmDetails, err := s.p.GetLeasedAlarm(ctx, lease)
			if err == nil && alarmDetails.ActorType == aRef.ActorType && alarmDetails.ActorID == aRef.ActorID {
				targetLease = lease
				break
			}
		}
		require.NotNil(t, targetLease, "should have found and leased the alarm for our test actor")

		// Verify the alarm is properly leased before removal
		_, err = s.p.GetLeasedAlarm(ctx, targetLease)
		require.NoError(t, err, "alarm should be properly leased before actor removal")

		// Remove the actor: this should automatically cancel any alarm leases via the database trigger
		err = s.p.RemoveActor(ctx, aRef)
		require.NoError(t, err)

		// Verify the alarm lease has been automatically canceled
		_, err = s.p.GetLeasedAlarm(ctx, targetLease)
		require.ErrorIs(t, err, components.ErrNoAlarm, "alarm lease should be automatically canceled after actor removal")

		// Verify the alarm still exists but is no longer leased by checking the database state
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		// Find the alarm in the database
		var foundAlarm *AlarmSpec
		for _, alarm := range spec.Alarms {
			if alarm.ActorType == aRef.ActorType && alarm.ActorID == aRef.ActorID && alarm.Name == "test-alarm" {
				foundAlarm = &alarm
				break
			}
		}
		require.NotNil(t, foundAlarm, "alarm should still exist in database")

		// But it should not have lease information anymore
		assert.Nil(t, foundAlarm.LeaseID, "alarm should not have lease ID after actor removal")
		assert.Nil(t, foundAlarm.LeaseExp, "alarm should not have lease expiration after actor removal")
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
		_, err := s.p.GetState(t.Context(), ref.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("delete returns ErrNoState if no state", func(t *testing.T) {
		err := s.p.DeleteState(t.Context(), ref.ActorRef{ActorType: "TestType", ActorID: "actor-1"})
		require.ErrorIs(t, err, components.ErrNoState)
	})

	t.Run("set get overwrite delete", func(t *testing.T) {
		ctx := t.Context()
		ref := ref.ActorRef{ActorType: "TestType", ActorID: "actor-1"}

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
		ref2 := ref.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-1"}
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
		ref3 := ref.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-extend"}
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
	// In the seed data, ALM-C-001...ALM-C-005 are already leased with a valid lease
	// ALM-C-006 has an expired lease
	expectPreLeasedAlarms := func(alarmID string) bool {
		switch alarmID {
		case "ALM-C-001", "ALM-C-002", "ALM-C-003", "ALM-C-004", "ALM-C-005", "ALM-C-006":
			return true
		default:
			return false
		}
	}

	t.Run("fetches upcoming alarms without capacity constraints", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Retrieve the alarms
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7},
		})
		require.NoError(t, err)

		// This should return a total of 24 alarms, all of types X and Y
		// Alarms ALM-X-002 and ALM-Y-001 (for actors X-2 and Y-1) should not be returned because the actors are active on H8
		// (and that's why we iterate till 13)
		// Alarm ALM-Y-002 should be included even though it's active on actor Y-2, because it's on H9 which is unhealthy
		expectAlarmIDs := make([]string, 0, 24)
		expectAlarmIDsMap := make(map[string]bool, 24)
		expectActorIDs := make([]string, 0, 24)
		for _, typ := range []string{"X", "Y"} {
			for i := 1; i <= 13; i++ {
				if (typ == "X" && i == 2) || (typ == "Y" && i == 1) {
					continue
				}

				alarmID := fmt.Sprintf("ALM-%s-%03d", typ, i)
				actorID := fmt.Sprintf("%s-%d", typ, i)

				expectAlarmIDs = append(expectAlarmIDs, alarmID)
				expectAlarmIDsMap[alarmID] = true
				expectActorIDs = append(expectActorIDs, actorID)
			}
		}

		// Collect all alarm IDs
		gotIDs := make([]string, 0, 24)
		for _, a := range res {
			gotIDs = append(gotIDs, a.Key())
			assert.NotEmpty(t, a.LeaseID())
		}

		// Order doesn't matter
		slices.Sort(expectAlarmIDs)
		slices.Sort(gotIDs)
		assert.Equal(t, expectAlarmIDs, gotIDs)

		// Ensure that the alarms' leases were acquired in the database, and only for the alarms we retrieved
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		for _, a := range spec.Alarms {
			if expectPreLeasedAlarms(a.AlarmID) {
				continue
			}

			if !expectAlarmIDsMap[a.AlarmID] {
				assert.Emptyf(t, a.LeaseID, "expected alarm %q not to have a lease ID", a.AlarmID)
				assert.Emptyf(t, a.LeaseExp, "expected alarm %q not to have a lease expiration", a.AlarmID)
				continue
			}

			_ = assert.NotNil(t, a.LeaseID, "expected alarm %q to have a lease ID", a.AlarmID) &&
				assert.NotEmpty(t, *a.LeaseID, "expected alarm %q to have a lease ID", a.AlarmID)
			_ = assert.NotNil(t, a.LeaseExp, "expected alarm %q to have a lease expiration", a.AlarmID) &&
				assert.Greater(t, *a.LeaseExp, s.p.Now(), "expected alarm's %q lease expiration to be in the future", a.AlarmID)
		}

		// Also ensure that all actors were activated on H7
		// Note that seed data contains active actors already
		gotActiveActorIDs := make(map[string]string, len(spec.ActiveActors))
		for _, a := range spec.ActiveActors {
			gotActiveActorIDs[a.ActorID] = a.HostID
		}

		for _, id := range expectActorIDs {
			_ = assert.NotEmptyf(t, gotActiveActorIDs[id], "expected actor %q to be active on host H7, but it was not active", id) &&
				assert.Equalf(t, SpecHostH7, gotActiveActorIDs[id], "expected actor %q to be active on host H7, but it was active on host %q", id, gotActiveActorIDs[id])
		}
	})

	t.Run("fetches upcoming alarms with capacity constraints", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Retrieve the alarms
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1, SpecHostH2},
		})
		require.NoError(t, err)

		// This should return a total of 24 alarms, all of types A, B, and C
		// Type A doesn't have any capacity left, but actors A-1, A-2, A-4 are active on H1 and H2, so alarms ALM-A-1, ALM-A-2, ALM-A-4 should be included
		// For type B, the combined capacity between H1 and H2 is 10, with 2 actors already active, so we should only get the earliest 8 plus ALM-B-1 and ALM-B-2 which are for the actors active on H1 and H2 (meanwhile, ALM-B-3 is active on H3 so should not be returned)
		// There's no capacity limit on type C, so we should get 12 of them. However, ALM-C-001...ALM-C-005 are already leased with a valid lease
		expectAlarmIDs := []string{
			"ALM-A-1", "ALM-A-2", "ALM-A-4",
			"ALM-B-1", "ALM-B-2",
			"ALM-B-001", "ALM-B-007", "ALM-B-014", "ALM-B-021", "ALM-B-028", "ALM-B-035", "ALM-B-042", "ALM-B-049",
			"ALM-C-006", "ALM-C-010", "ALM-C-011", "ALM-C-015", "ALM-C-020", "ALM-C-025", "ALM-C-030", "ALM-C-035", "ALM-C-040", "ALM-C-045", "ALM-C-050",
		}
		expectAlarmIDsMap := make(map[string]bool, len(expectAlarmIDs))
		expectActorIDs := make([]string, len(expectAlarmIDs))
		for i, id := range expectAlarmIDs {
			expectAlarmIDsMap[id] = true
			expectActorIDs[i] = strings.TrimPrefix(id, "ALM-")
		}

		// Collect all alarm IDs
		gotIDs := make([]string, 0, 24)
		for _, a := range res {
			gotIDs = append(gotIDs, a.Key())
			assert.NotEmpty(t, a.LeaseID())
		}

		// Order doesn't matter
		slices.Sort(expectAlarmIDs)
		slices.Sort(gotIDs)
		assert.Equal(t, expectAlarmIDs, gotIDs)

		// Ensure that the alarms' leases were acquired in the database, and only for the alarms we retrieved
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		for _, a := range spec.Alarms {
			// ALM-C-006's leases was expired and we should have taken it over
			if a.AlarmID != "ALM-C-006" && expectPreLeasedAlarms(a.AlarmID) {
				continue
			}

			if !expectAlarmIDsMap[a.AlarmID] {
				// Seed data doesn't contain any leased alarm, so we can confidently exclude others
				assert.Emptyf(t, a.LeaseID, "expected alarm %q not to have a lease ID", a.AlarmID)
				assert.Emptyf(t, a.LeaseExp, "expected alarm %q not to have a lease expiration", a.AlarmID)
				continue
			}

			_ = assert.NotNil(t, a.LeaseID, "expected alarm %q to have a lease ID", a.AlarmID) &&
				assert.NotEmpty(t, *a.LeaseID, "expected alarm %q to have a lease ID", a.AlarmID)
			_ = assert.NotNil(t, a.LeaseExp, "expected alarm %q to have a lease expiration", a.AlarmID) &&
				assert.Greater(t, *a.LeaseExp, s.p.Now(), "expected alarm's %q lease expiration to be in the future", a.AlarmID)
		}

		// Also ensure that all actors were activated on H1 or H2
		// Note that seed data contains active actors already
		gotActiveActorIDs := make(map[string]string, len(spec.ActiveActors))
		for _, a := range spec.ActiveActors {
			gotActiveActorIDs[a.ActorID] = a.HostID
		}

		hostCounts := make(map[string]int, 2)
		for _, id := range expectActorIDs {
			if !assert.NotEmptyf(t, gotActiveActorIDs[id], "expected actor %q to be active on a host, but it was not active", id) {
				continue
			}

			switch id {
			// These actors were already active in the seed data
			case "A-1", "A-2", "B-1":
				assert.Equalf(t, SpecHostH1, gotActiveActorIDs[id], "expected actor %q to be active on host H1, but it was active on host %q", id, gotActiveActorIDs[id])
			case "A-4", "B-2":
				assert.Equalf(t, SpecHostH2, gotActiveActorIDs[id], "expected actor %q to be active on host H2, but it was active on host %q", id, gotActiveActorIDs[id])
			default:
				assert.Contains(t, []string{SpecHostH1, SpecHostH2}, gotActiveActorIDs[id], "expected actor %q to be active on host H1 or H2, but it was active on host %q", id, gotActiveActorIDs[id])
				hostCounts[gotActiveActorIDs[id]]++
			}
		}

		// There should be some level of distribution for actors that were just activated
		// It doesn't have to be 50/50 since there's randomness involved
		assert.Len(t, hostCounts, 2)
		assert.GreaterOrEqual(t, hostCounts[SpecHostH1], 4)
		assert.GreaterOrEqual(t, hostCounts[SpecHostH2], 4)
	})

	t.Run("returns empty slice when no hosts provided", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch alarms with empty hosts list
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{},
		})
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("returns empty slice when all hosts are unhealthy", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch alarms only from unhealthy hosts
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			// Both unhealthy in seed data
			Hosts: []string{SpecHostH5, SpecHostH6},
		})
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("returns empty slice when hosts don't exist", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch alarms from non-existent hosts
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{"NON-EXISTENT-1", "NON-EXISTENT-2"},
		})
		require.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("returns empty slice when no upcoming alarms", func(t *testing.T) {
		ctx := t.Context()

		// Seed with hosts but no alarms
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{},
		}
		require.NoError(t, s.p.Seed(ctx, customSpec))

		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1},
		})
		require.NoError(t, err)
		assert.Empty(t, res, "should return empty slice when no upcoming alarms")
	})

	t.Run("doesn't return already leased alarms with valid leases", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// First fetch should get some alarms and lease them
		res1, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res1)

		// Second fetch immediately should not return the same alarms (they're already leased)
		res2, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7},
		})
		require.NoError(t, err)

		// Verify no overlap between the two batches
		leased1 := make(map[string]bool)
		for _, lease := range res1 {
			leased1[lease.Key()] = true
		}

		for _, lease := range res2 {
			assert.False(t, leased1[lease.Key()], "alarm %s should not appear in both batches", lease.Key())
		}
	})

	t.Run("takes over expired leases", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data (contains ALM-C-006 with expired lease)
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch alarms from H1 and H2 where C type is supported
		// This should include ALM-C-006 which has an expired lease
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1, SpecHostH2},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res)

		// Check if we got the expired lease ALM-C-006 and gave it a new lease
		foundExpiredAlarm := false
		for _, lease := range res {
			if lease.Key() == "ALM-C-006" {
				// Should have a lease ID (new lease was created)
				assert.NotEmpty(t, lease.LeaseID(), "ALM-C-006 should have been given a new lease")
				foundExpiredAlarm = true
				break
			}
		}
		assert.True(t, foundExpiredAlarm, "should have found and taken over the expired lease ALM-C-006")
	})

	t.Run("fetches overdue alarms", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom test spec with overdue alarms
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second}, // healthy
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestOverdue", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{
				{
					AlarmID:   "ALM-OVERDUE-1",
					ActorType: "TestOverdue",
					ActorID:   "overdue-actor-1",
					Name:      "overdue-alarm-1",
					DueIn:     -5 * time.Minute, // Due 5 minutes ago (overdue)
					Data:      []byte("overdue-data-1"),
				},
				{
					AlarmID:   "ALM-OVERDUE-2",
					ActorType: "TestOverdue",
					ActorID:   "overdue-actor-2",
					Name:      "overdue-alarm-2",
					DueIn:     -30 * time.Second, // Due 30 seconds ago (overdue)
					Data:      []byte("overdue-data-2"),
				},
			},
		}

		// Seed with overdue alarms
		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Fetch alarms - should include overdue ones
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should fetch overdue alarms")

		// Verify both overdue alarms were fetched
		foundOverdue1 := false
		foundOverdue2 := false
		for _, lease := range res {
			if lease.Key() == "ALM-OVERDUE-1" {
				foundOverdue1 = true
				// Verify the alarm is in the past
				assert.True(t, lease.DueTime().Before(s.p.Now()), "ALM-OVERDUE-1 should be overdue")
				assert.Equal(t, "TestOverdue/overdue-actor-1", lease.ActorRef().String())
			}
			if lease.Key() == "ALM-OVERDUE-2" {
				foundOverdue2 = true
				// Verify the alarm is in the past
				assert.True(t, lease.DueTime().Before(s.p.Now()), "ALM-OVERDUE-2 should be overdue")
				assert.Equal(t, "TestOverdue/overdue-actor-2", lease.ActorRef().String())
			}
		}
		assert.True(t, foundOverdue1, "should have found overdue alarm ALM-OVERDUE-1")
		assert.True(t, foundOverdue2, "should have found overdue alarm ALM-OVERDUE-2")

		// Verify the leased overdue alarms can be retrieved
		for _, lease := range res {
			key := lease.Key()
			if key == "ALM-OVERDUE-1" || key == "ALM-OVERDUE-2" {
				alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
				require.NoError(t, err, "overdue alarm %s should be properly leased", key)
				assert.Equal(t, "TestOverdue", alarmRes.ActorType)
				assert.Equal(t, "TestOverdue/overdue-actor-"+key[len(key)-1:], lease.ActorRef().String())
			}
		}
	})

	t.Run("mixed healthy and unhealthy hosts filters correctly", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Request from mix of healthy and unhealthy hosts
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH5, SpecHostH8, SpecHostH6}, // H7,H8 healthy, H5,H6 unhealthy
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should return alarms from healthy hosts")

		// Verify all returned alarms can be retrieved (meaning they were properly leased)
		for _, lease := range res {
			alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "alarm %s should be properly leased", lease.Key())
			assert.Contains(t, []string{"X", "Y"}, alarmRes.ActorType, "should only have X/Y type alarms from H7/H8")
		}

		// Verify actors were only placed on healthy hosts
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)

		gotActiveActorIDs := make(map[string]string)
		for _, a := range spec.ActiveActors {
			gotActiveActorIDs[a.ActorID] = a.HostID
		}

		for _, lease := range res {
			alarmRes, _ := s.p.GetLeasedAlarm(ctx, lease)
			if hostID, exists := gotActiveActorIDs[alarmRes.ActorID]; exists {
				assert.Contains(t, []string{SpecHostH7, SpecHostH8}, hostID, "actor %s should only be placed on healthy hosts", alarmRes.ActorID)
			}
		}
	})
}

func (s Suite) TestGetLeasedAlarm(t *testing.T) {
	t.Run("returns alarm with valid lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm to test with
		lease := res[0]

		// Get the leased alarm details
		alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Verify the alarm details
		assert.NotEmpty(t, alarmRes.ActorType)
		assert.NotEmpty(t, alarmRes.ActorID)
		assert.NotEmpty(t, alarmRes.Name)
		assert.Equal(t, lease.DueTime(), alarmRes.DueTime)

		// The alarm should be of type X or Y based on our test data
		assert.Contains(t, []string{"X", "Y"}, alarmRes.ActorType)
	})

	t.Run("returns ErrNoAlarm if alarm doesn't exist", func(t *testing.T) {
		ctx := t.Context()

		// Seed with empty database
		require.NoError(t, s.p.Seed(ctx, Spec{}))

		// Try to get a non-existent alarm
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "not-exists", time.Now(), "fake-lease-id")
		_, err := s.p.GetLeasedAlarm(ctx, nonExistentLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm if alarm isn't leased", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data (has un-leased alarms)
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to get an alarm that exists but isn't leased
		// From spec, ALM-B-007 and later B alarms should not be pre-leased
		unleaedAlarmLease := ref.NewAlarmLease(ref.NewAlarmRef("B", "B-007", "Alarm-B-007"), "ALM-B-007", time.Now(), "fake-lease-id")
		_, err := s.p.GetLeasedAlarm(ctx, unleaedAlarmLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm if alarm's lease belongs to others", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Create a fake lease with the same alarm ID but different lease ID
		fakeLease := ref.NewAlarmLease(lease.AlarmRef(), lease.Key(), lease.DueTime(), "bad-lease-id")

		// Try to get the alarm with the wrong lease ID
		_, err = s.p.GetLeasedAlarm(ctx, fakeLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm if alarm's lease has expired", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Advance time beyond lease expiration (lease duration is 1 minute from GetProviderConfig)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to get the alarm with the now-expired lease
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns alarm data correctly", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1, SpecHostH2},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Find an alarm with known data - look for one of the active actor alarms
		targetLease := &ref.AlarmLease{}
		var found bool
		for _, lease := range res {
			// ALM-A-1, ALM-A-2, ALM-A-4, ALM-B-1, ALM-B-2 should have specific data
			if lease.Key() == "ALM-A-1" || lease.Key() == "ALM-B-1" {
				targetLease = lease
				found = true
				break
			}
		}
		require.True(t, found, "should have found a known alarm with data")

		// Get the leased alarm details
		alarmRes, err := s.p.GetLeasedAlarm(ctx, targetLease)
		require.NoError(t, err)

		// Verify the alarm data matches expected values from GetSpec
		switch targetLease.Key() {
		case "ALM-A-1":
			assert.Equal(t, "A", alarmRes.ActorType)
			assert.Equal(t, "A-1", alarmRes.ActorID)
			assert.Equal(t, "Alarm-A-1", alarmRes.Name)
			assert.Equal(t, []byte("active-A-1"), alarmRes.Data)
		case "ALM-B-1":
			assert.Equal(t, "B", alarmRes.ActorType)
			assert.Equal(t, "B-1", alarmRes.ActorID)
			assert.Equal(t, "Alarm-B-1", alarmRes.Name)
			assert.Equal(t, []byte("active-B-1"), alarmRes.Data)
		}
	})

	t.Run("returns alarm with interval and TTL correctly", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom test spec with an alarm that has interval and TTL
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{
				{
					AlarmID:   "test-alarm-with-extras",
					ActorType: "TestType",
					ActorID:   "test-actor",
					Name:      "test-alarm",
					DueIn:     time.Second,
					Interval:  "PT1H",
					TTL:       24 * time.Hour,
					Data:      []byte("test-data-with-extras"),
				},
			},
		}

		// Seed with custom data
		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Fetch the alarm to create a lease
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1},
		})
		require.NoError(t, err)
		require.Len(t, res, 1, "should have fetched exactly one alarm")

		lease := res[0]

		// Get the leased alarm details
		alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Verify all fields including interval and TTL
		assert.Equal(t, "TestType", alarmRes.ActorType)
		assert.Equal(t, "test-actor", alarmRes.ActorID)
		assert.Equal(t, "test-alarm", alarmRes.Name)
		assert.Equal(t, []byte("test-data-with-extras"), alarmRes.Data)
		assert.Equal(t, "PT1H", alarmRes.Interval)
		assert.NotNil(t, alarmRes.TTL)

		// TTL should be approximately 24 hours from now (allowing some tolerance for execution time)
		expectedTTL := s.p.Now().Add(24 * time.Hour)
		assert.WithinDuration(t, expectedTTL, *alarmRes.TTL, 10*time.Second, "TTL should be approximately 24 hours from now")
	})

	t.Run("handles nil data correctly", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom test spec with an alarm that has no data
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{
				{
					AlarmID:   "test-alarm-no-data",
					ActorType: "TestType",
					ActorID:   "test-actor",
					Name:      "test-alarm",
					DueIn:     time.Second,
					Data:      nil, // No data
				},
			},
		}

		// Seed with custom data
		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Fetch the alarm to create a lease
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH1},
		})
		require.NoError(t, err)
		require.Len(t, res, 1, "should have fetched exactly one alarm")

		lease := res[0]

		// Get the leased alarm details
		alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Verify data is nil
		assert.Nil(t, alarmRes.Data, "data should be nil when not set")
		assert.Empty(t, alarmRes.Interval, "interval should be empty when not set")
		assert.Nil(t, alarmRes.TTL, "TTL should be nil when not set")
	})
}

func (s Suite) TestRenewAlarmLeases(t *testing.T) {
	t.Run("renews leases for specific hosts", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Advance time partway through lease duration to simulate renewal scenario
		s.p.AdvanceClock(30 * time.Second)

		// Renew leases for H7 only
		renewReq := components.RenewAlarmLeasesReq{
			Hosts: []string{SpecHostH7},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		require.NotEmpty(t, renewRes.Leases, "should have renewed some leases")

		// Verify all returned leases are still valid
		for _, lease := range renewRes.Leases {
			_, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "renewed lease should be valid")
		}

		// Advance time beyond original lease expiration
		// Total: 75 seconds (beyond original 60s lease)
		s.p.AdvanceClock(45 * time.Second)

		// Renewed leases should still be valid (they were extended)
		for _, lease := range renewRes.Leases {
			_, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "renewed lease should still be valid after original expiration")
		}
	})

	t.Run("renews specific leases only", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 3, "need at least 3 leases for this test")

		// Select first 2 leases for renewal
		leasesToRenew := []*ref.AlarmLease{res[0], res[1]}
		leaseNotRenewed := res[2]

		// Advance time partway through lease duration
		s.p.AdvanceClock(30 * time.Second)

		// Renew only specific leases
		renewReq := components.RenewAlarmLeasesReq{
			Hosts:  []string{SpecHostH7, SpecHostH8},
			Leases: leasesToRenew,
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		require.Len(t, renewRes.Leases, 2, "should have renewed exactly 2 leases")

		// Verify the specific leases were renewed
		renewedLeaseKeys := make(map[string]bool)
		for _, lease := range renewRes.Leases {
			renewedLeaseKeys[lease.Key()] = true
		}
		assert.True(t, renewedLeaseKeys[leasesToRenew[0].Key()], "first lease should be renewed")
		assert.True(t, renewedLeaseKeys[leasesToRenew[1].Key()], "second lease should be renewed")

		// Advance time beyond original lease expiration
		// Total: 75 seconds
		s.p.AdvanceClock(45 * time.Second)

		// Renewed leases should still be valid
		for _, lease := range renewRes.Leases {
			_, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "renewed lease should still be valid")
		}

		// Non-renewed lease should have expired
		_, err = s.p.GetLeasedAlarm(ctx, leaseNotRenewed)
		require.ErrorIs(t, err, components.ErrNoAlarm, "non-renewed lease should have expired")
	})

	t.Run("returns empty result when no matching leases", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to renew leases for hosts with no active leases
		renewReq := components.RenewAlarmLeasesReq{
			// These hosts don't have any leases in the initial seed
			Hosts: []string{SpecHostH1, SpecHostH2},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		assert.Empty(t, renewRes.Leases, "should return empty result when no matching leases")
	})

	t.Run("returns empty result for non-existent hosts", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to renew leases for non-existent hosts
		renewReq := components.RenewAlarmLeasesReq{
			Hosts: []string{"NON-EXISTENT-1", "NON-EXISTENT-2"},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		assert.Empty(t, renewRes.Leases, "should return empty result for non-existent hosts")
	})

	t.Run("ignores expired leases", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Advance time beyond lease expiration (1 minute)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to renew expired leases
		renewReq := components.RenewAlarmLeasesReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		assert.Empty(t, renewRes.Leases, "should not renew expired leases")
	})

	t.Run("handles mixed valid and invalid lease IDs", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 2, "need at least 2 leases for this test")

		// Create a mix of valid and invalid lease IDs
		validLease := res[0]
		invalidLease := ref.NewAlarmLease(validLease.AlarmRef(), "non-existent-alarm", time.Now(), "fake-lease-id")

		// Advance time partway through lease duration
		s.p.AdvanceClock(30 * time.Second)

		// Try to renew mix of valid and invalid leases
		renewReq := components.RenewAlarmLeasesReq{
			Hosts:  []string{SpecHostH7, SpecHostH8},
			Leases: []*ref.AlarmLease{validLease, invalidLease},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)

		// Should only renew the valid lease
		require.Len(t, renewRes.Leases, 1, "should renew only the valid lease")
		assert.Equal(t, validLease.Key(), renewRes.Leases[0].Key(), "should renew the correct lease")
	})

	t.Run("renews all leases for multiple hosts", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch alarms from H7
		res1, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res1)

		// Fetch alarms from H8
		res2, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res2)

		totalExpectedLeases := len(res1) + len(res2)

		// Advance time partway through lease duration
		s.p.AdvanceClock(30 * time.Second)

		// Renew all leases for both hosts
		renewReq := components.RenewAlarmLeasesReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		assert.Equal(t, totalExpectedLeases, len(renewRes.Leases), "should renew all leases from both hosts")

		// Verify all renewed leases are valid
		for _, lease := range renewRes.Leases {
			_, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "all renewed leases should be valid")
		}
	})
}

func (s Suite) TestReleaseAlarmLease(t *testing.T) {
	t.Run("releases valid lease successfully", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm to test with
		lease := res[0]

		// Verify the alarm is leased
		alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
		assert.NotEmpty(t, alarmRes.ActorType)

		// Release the lease
		err = s.p.ReleaseAlarmLease(ctx, lease)
		require.NoError(t, err)

		// Verify the lease is no longer valid
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for non-existent alarm", func(t *testing.T) {
		ctx := t.Context()

		// Seed with empty database
		require.NoError(t, s.p.Seed(ctx, Spec{}))

		// Try to release a non-existent alarm lease
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "non-existent-alarm", time.Now(), "fake-lease-id")
		err := s.p.ReleaseAlarmLease(ctx, nonExistentLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for alarm with no lease", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom test spec with an unleased alarm
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{
				{
					AlarmID:   "test-alarm-no-lease",
					ActorType: "TestType",
					ActorID:   "test-actor",
					Name:      "test-alarm",
					DueIn:     time.Second,
					Data:      []byte("test-data"),
				},
			},
		}

		// Seed with custom data
		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Try to release a lease for an alarm that was never leased
		fakeLease := ref.NewAlarmLease(ref.NewAlarmRef("TestType", "test-actor", "test-alarm"), "test-alarm-no-lease", time.Now(), "fake-lease-id")
		err := s.p.ReleaseAlarmLease(ctx, fakeLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for wrong lease ID", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Create a fake lease with wrong lease ID
		fakeLease := ref.NewAlarmLease(lease.AlarmRef(), lease.Key(), lease.DueTime(), "wrong-lease-id")

		// Try to release with wrong lease ID
		err = s.p.ReleaseAlarmLease(ctx, fakeLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify original lease is still valid
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err, "original lease should still be valid")
	})

	t.Run("returns ErrNoAlarm for expired lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Advance time beyond lease expiration (lease duration is 1 minute from GetProviderConfig)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to release the now-expired lease
		err = s.p.ReleaseAlarmLease(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("idempotent release - releasing same lease twice", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Release the lease first time - should succeed
		err = s.p.ReleaseAlarmLease(ctx, lease)
		require.NoError(t, err)

		// Release the same lease second time - should return ErrNoAlarm
		err = s.p.ReleaseAlarmLease(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("multiple releases work independently", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch multiple alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 3, "should have fetched at least 3 alarms for this test")

		// Take the first 3 leases
		lease1 := res[0]
		lease2 := res[1]
		lease3 := res[2]

		// Release lease1 and lease3, but leave lease2
		err = s.p.ReleaseAlarmLease(ctx, lease1)
		require.NoError(t, err)

		err = s.p.ReleaseAlarmLease(ctx, lease3)
		require.NoError(t, err)

		// Verify lease1 and lease3 are no longer valid
		_, err = s.p.GetLeasedAlarm(ctx, lease1)
		require.ErrorIs(t, err, components.ErrNoAlarm, "lease1 should be released")

		_, err = s.p.GetLeasedAlarm(ctx, lease3)
		require.ErrorIs(t, err, components.ErrNoAlarm, "lease3 should be released")

		// Verify lease2 is still valid
		_, err = s.p.GetLeasedAlarm(ctx, lease2)
		require.NoError(t, err, "lease2 should still be valid")
	})
}

func (s Suite) TestUpdateLeasedAlarm(t *testing.T) {
	t.Run("updates alarm with refresh lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Get original alarm details
		originalAlarm, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Update the alarm with new details and refresh lease
		newDueTime := time.Now().Add(2 * time.Hour)
		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      newDueTime,
			RefreshLease: true,
		}

		err = s.p.UpdateLeasedAlarm(ctx, lease, updateReq)
		require.NoError(t, err)

		// Verify the alarm was updated and lease is still valid
		updatedAlarm, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Check updated fields
		assert.Equal(t, newDueTime.UnixMilli(), updatedAlarm.DueTime.UnixMilli())

		// Verify other fields remain unchanged
		assert.Equal(t, originalAlarm.ActorType, updatedAlarm.ActorType)
		assert.Equal(t, originalAlarm.ActorID, updatedAlarm.ActorID)
		assert.Equal(t, originalAlarm.Name, updatedAlarm.Name)
		assert.Equal(t, originalAlarm.Data, updatedAlarm.Data)
		assert.Equal(t, originalAlarm.Interval, updatedAlarm.Interval)
		if originalAlarm.TTL == nil {
			require.Nil(t, updatedAlarm.TTL)
		} else {
			require.NotNil(t, updatedAlarm.TTL)
			assert.Equal(t, originalAlarm.TTL.UnixMilli(), updatedAlarm.TTL.UnixMilli())
		}
	})

	t.Run("updates alarm without refresh lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Update the alarm without refreshing lease
		// This will release the lease
		newDueTime := time.Now().Add(3 * time.Hour)
		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      newDueTime,
			RefreshLease: false,
		}

		err = s.p.UpdateLeasedAlarm(ctx, lease, updateReq)
		require.NoError(t, err)

		// Verify the lease is no longer valid (was released)
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm, "lease should be released")
	})

	t.Run("returns ErrNoAlarm for non-existent alarm", func(t *testing.T) {
		ctx := t.Context()

		// Seed with empty database
		require.NoError(t, s.p.Seed(ctx, Spec{}))

		// Try to update a non-existent alarm
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "non-existent", time.Now(), "fake-lease-id")
		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      time.Now().Add(1 * time.Hour),
			RefreshLease: true,
		}

		err := s.p.UpdateLeasedAlarm(ctx, nonExistentLease, updateReq)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for wrong lease ID", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm and create fake lease with wrong ID
		validLease := res[0]
		fakeLease := ref.NewAlarmLease(validLease.AlarmRef(), validLease.Key(), validLease.DueTime(), "wrong-lease-id")

		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      time.Now().Add(1 * time.Hour),
			RefreshLease: true,
		}

		err = s.p.UpdateLeasedAlarm(ctx, fakeLease, updateReq)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify original lease is still valid
		_, err = s.p.GetLeasedAlarm(ctx, validLease)
		require.NoError(t, err, "original lease should still be valid")
	})

	t.Run("returns ErrNoAlarm for expired lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Advance time beyond lease expiration (lease duration is 1 minute from GetProviderConfig)
		s.p.AdvanceClock(2 * time.Minute)

		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      time.Now().Add(1 * time.Hour),
			RefreshLease: true,
		}

		// Try to update the now-expired lease
		err = s.p.UpdateLeasedAlarm(ctx, lease, updateReq)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})
}

func (s Suite) TestDeleteLeasedAlarm(t *testing.T) {
	t.Run("deletes leased alarm successfully", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Verify the alarm exists and is leased
		alarmRes, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
		assert.NotEmpty(t, alarmRes.ActorType)

		// Delete the leased alarm
		err = s.p.DeleteLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Verify the alarm no longer exists
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Also verify using standard GetAlarm that it's completely gone
		alarmRef := ref.AlarmRef{
			ActorType: alarmRes.ActorType,
			ActorID:   alarmRes.ActorID,
			Name:      alarmRes.Name,
		}
		_, err = s.p.GetAlarm(ctx, alarmRef)
		require.ErrorIs(t, err, components.ErrNoAlarm, "alarm should be completely deleted")
	})

	t.Run("returns ErrNoAlarm for non-existent alarm", func(t *testing.T) {
		ctx := t.Context()

		// Seed with empty database
		require.NoError(t, s.p.Seed(ctx, Spec{}))

		// Try to delete a non-existent alarm
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "non-existent", time.Now(), "fake-lease-id")
		err := s.p.DeleteLeasedAlarm(ctx, nonExistentLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for alarm with no lease", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom test spec with an unleased alarm
		customSpec := Spec{
			Hosts: []HostSpec{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: []HostActorTypeSpec{
				{HostID: SpecHostH1, ActorType: "TestType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
			Alarms: []AlarmSpec{
				{
					AlarmID:   "test-alarm-no-lease",
					ActorType: "TestType",
					ActorID:   "test-actor",
					Name:      "test-alarm",
					DueIn:     time.Second,
					Data:      []byte("test-data"),
				},
			},
		}

		// Seed with custom data
		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Try to delete an alarm that was never leased
		fakeLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "test-alarm-no-lease", time.Now(), "fake-lease-id")
		err := s.p.DeleteLeasedAlarm(ctx, fakeLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify the unleased alarm still exists via GetAlarm
		alarmRef := ref.AlarmRef{ActorType: "TestType", ActorID: "test-actor", Name: "test-alarm"}
		_, err = s.p.GetAlarm(ctx, alarmRef)
		require.NoError(t, err, "unleased alarm should still exist")
	})

	t.Run("returns ErrNoAlarm for wrong lease ID", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		validLease := res[0]

		// Create a fake lease with wrong lease ID
		fakeLease := ref.NewAlarmLease(validLease.AlarmRef(), validLease.Key(), validLease.DueTime(), "wrong-lease-id")

		// Try to delete with wrong lease ID
		err = s.p.DeleteLeasedAlarm(ctx, fakeLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify original lease is still valid
		_, err = s.p.GetLeasedAlarm(ctx, validLease)
		require.NoError(t, err, "original lease should still be valid")
	})

	t.Run("returns ErrNoAlarm for expired lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Advance time beyond lease expiration (lease duration is 1 minute from GetProviderConfig)
		s.p.AdvanceClock(2 * time.Minute)

		// Try to delete the now-expired lease
		err = s.p.DeleteLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("idempotent deletion - deleting same alarm twice", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch some alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Pick the first leased alarm
		lease := res[0]

		// Delete the alarm first time - should succeed
		err = s.p.DeleteLeasedAlarm(ctx, lease)
		require.NoError(t, err)

		// Delete the same alarm second time - should return ErrNoAlarm
		err = s.p.DeleteLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("multiple deletions work independently", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Fetch multiple alarms to create valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 3, "should have fetched at least 3 alarms for this test")

		// Take the first 3 leases
		lease1 := res[0]
		lease2 := res[1]
		lease3 := res[2]

		// Delete lease1 and lease3, but leave lease2
		err = s.p.DeleteLeasedAlarm(ctx, lease1)
		require.NoError(t, err)

		err = s.p.DeleteLeasedAlarm(ctx, lease3)
		require.NoError(t, err)

		// Verify lease1 and lease3 are completely gone
		_, err = s.p.GetLeasedAlarm(ctx, lease1)
		require.ErrorIs(t, err, components.ErrNoAlarm, "lease1 should be deleted")

		_, err = s.p.GetLeasedAlarm(ctx, lease3)
		require.ErrorIs(t, err, components.ErrNoAlarm, "lease3 should be deleted")

		// Verify lease2 is still valid
		_, err = s.p.GetLeasedAlarm(ctx, lease2)
		require.NoError(t, err, "lease2 should still be valid")
	})
}
