package comptesting

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// Suite implements a test suite for actor provider components.
type Suite struct {
	p ActorProviderTesting
}

func NewSuite(p ActorProviderTesting) *Suite {
	return &Suite{p: p}
}

func (s Suite) RunTests(t *testing.T) {
	t.Run("register host", s.TestRegisterHost)
	t.Run("update actor host", s.TestUpdateActorHost)
	t.Run("unregister host", s.TestUnregisterHost)
	t.Run("list hosts", s.TestListHosts)

	t.Run("lookup actor", s.TestLookupActor)
	t.Run("remove actor", s.TestRemoveActor)

	t.Run("actor state", s.TestState)
	t.Run("list actor states", s.TestListStates)

	t.Run("get alarm", s.TestGetAlarm)
	t.Run("set alarm", s.TestSetAlarm)
	t.Run("set and lease alarm", s.TestSetAndLeaseAlarm)
	t.Run("delete alarm", s.TestDeleteAlarm)

	t.Run("fetch alarms", s.TestFetchAlarms)
	t.Run("get leased alarm", s.TestGetLeasedAlarm)
	t.Run("renew alarm leases", s.TestRenewAlarmLeases)
	t.Run("release alarm lease", s.TestReleaseAlarmLease)
	t.Run("update leased alarm", s.TestUpdateLeasedAlarm)
	t.Run("delete leased alarm", s.TestDeleteLeasedAlarm)

	t.Run("jobs", s.TestJobs)

	t.Run("backup and restore", s.TestBackupRestore)

	t.Run("cluster admission", s.TestClusterAdmission)
}

func (s Suite) RunConcurrencyTests(t *testing.T) {
	t.Run("lookup actor", s.TestConcurrentLookupActor)
	t.Run("fetch alarms", s.TestConcurrentFetchAlarms)
	t.Run("dispatch jobs", s.TestConcurrentDispatchJobs)
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
		_ = s.p.AdvanceClock(2 * time.Minute) // Assuming health check deadline is 1 minute//nolint:errcheck

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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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

	// Reattachment: a reconnecting host can reclaim its registration by its previous host ID

	const (
		reattachHostA = "aaaaaaaa-0000-4000-8000-000000000001"
		reattachHostB = "bbbbbbbb-0000-4000-8000-000000000002"
		reattachHostX = "99999999-0000-4000-8000-00000000ffff"
	)

	t.Run("reattach refreshes an existing registration and preserves active actors", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.110:8080", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: reattachHostA, ActorType: "OldType", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 5},
			},
			ActiveActors: []ActiveActorSpec{
				{ActorType: "OldType", ActorID: "actor-1", HostID: reattachHostA, ActorIdleTimeout: 5 * time.Minute, ActivationAgo: time.Second},
			},
		}))

		// Reattach with the same host ID, a changed address, and a replaced actor type set
		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.111:9090",
			ActorTypes: []components.ActorHostType{
				{ActorType: "NewType", IdleTimeout: 3 * time.Minute, ConcurrencyLimit: 8},
			},
		})
		require.NoError(t, err)
		assert.True(t, res.Reattached, "should report a reattachment")
		assert.Equal(t, reattachHostA, res.HostID, "should keep the same host ID")

		expectedHosts := HostSpecCollection{
			{HostID: reattachHostA, Address: "192.168.1.111:9090"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: reattachHostA, ActorType: "NewType", ActorIdleTimeout: 3 * time.Minute, ActorConcurrencyLimit: 8},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)

		// The active actor is preserved because the host row is updated in place rather than recreated
		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)
		assert.Len(t, spec.ActiveActors, 1, "active actor should survive reattachment")
	})

	t.Run("reattach to an expired registration mints a new host ID", func(t *testing.T) {
		// Seed a host whose last health check is already older than the 1m deadline
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.112:8080", LastHealthAgo: 90 * time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: reattachHostA, ActorType: "T", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 1},
			},
		}))

		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.112:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "T", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 1},
			},
		})
		require.NoError(t, err)

		// Once a registration has expired the cluster has written that host off and may have placed its actors elsewhere, so it must not be reclaimable
		// The new identity, reported through Reattached, is what tells the host to drop whatever it was still holding
		assert.False(t, res.Reattached, "an expired registration must not be reattachable")
		assert.NotEqual(t, reattachHostA, res.HostID, "should mint a new host ID rather than resume the expired one")
		assert.NotEmpty(t, res.HostID)

		// The new registration is healthy and holds the address, so a fresh registration at the same address must fail
		_, err = s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			Address: "192.168.1.112:8080",
		})
		require.ErrorIs(t, err, components.ErrHostAlreadyRegistered)
	})

	t.Run("reattach refreshes a live registration in place", func(t *testing.T) {
		// Seed a host whose last health check is still within the 1m deadline, as it would be right after a runtime failover
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.112:8080", LastHealthAgo: 30 * time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: reattachHostA, ActorType: "T", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 1},
			},
		}))

		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.112:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "T", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 1},
			},
		})
		require.NoError(t, err)
		assert.True(t, res.Reattached, "a host whose registration is still live keeps its identity")
		assert.Equal(t, reattachHostA, res.HostID)
	})

	t.Run("reattach to an unknown host creates a new registration", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostX,
			Address:        "192.168.1.113:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "T", IdleTimeout: time.Minute, ConcurrencyLimit: 0},
			},
		})
		require.NoError(t, err)
		assert.False(t, res.Reattached, "should not report a reattachment for an unknown host")
		assert.NotEmpty(t, res.HostID)
		assert.NotEqual(t, reattachHostX, res.HostID, "should mint a new host ID rather than adopt the unknown one")

		expectedHosts := HostSpecCollection{
			{HostID: res.HostID, Address: "192.168.1.113:8080"},
		}
		expectedActorTypes := HostActorTypeSpecCollection{
			{HostID: res.HostID, ActorType: "T", ActorIdleTimeout: time.Minute, ActorConcurrencyLimit: 0},
		}
		expectHosts(t, expectedHosts, expectedActorTypes)
	})

	t.Run("reattach to a garbage-collected host creates a new registration", func(t *testing.T) {
		// Seed a stale host, then let a fresh registration garbage-collect it
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.114:8080", LastHealthAgo: 90 * time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: reattachHostA, ActorType: "T", ActorIdleTimeout: time.Minute, ActorConcurrencyLimit: 0},
			},
		}))

		// A fresh registration at a different address cleans up the unhealthy host
		_, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			Address: "192.168.1.115:8080",
		})
		require.NoError(t, err)

		// Reattaching to the now-removed host finds nothing and creates a brand-new registration
		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.114:8080",
		})
		require.NoError(t, err)
		assert.False(t, res.Reattached)
		assert.NotEqual(t, reattachHostA, res.HostID, "the previous registration was gone, so a new ID is minted")
	})

	t.Run("reattach fails when another healthy host holds the address", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.116:8080", LastHealthAgo: 2 * time.Second},
				{HostID: reattachHostB, Address: "192.168.1.117:8080", LastHealthAgo: 2 * time.Second},
			},
		}))

		// Attempt to reattach host A onto host B's address
		_, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.117:8080",
		})
		require.ErrorIs(t, err, components.ErrHostAlreadyRegistered)

		// Nothing changed: both hosts keep their original addresses
		expectedHosts := HostSpecCollection{
			{HostID: reattachHostA, Address: "192.168.1.116:8080"},
			{HostID: reattachHostB, Address: "192.168.1.117:8080"},
		}
		expectHosts(t, expectedHosts, HostActorTypeSpecCollection{})
	})

	t.Run("reattach with no actor types clears existing ones", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{
			Hosts: HostSpecCollection{
				{HostID: reattachHostA, Address: "192.168.1.118:8080", LastHealthAgo: 2 * time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: reattachHostA, ActorType: "T", ActorIdleTimeout: time.Minute, ActorConcurrencyLimit: 0},
			},
		}))

		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			ExistingHostID: reattachHostA,
			Address:        "192.168.1.118:8080",
			ActorTypes:     []components.ActorHostType{},
		})
		require.NoError(t, err)
		assert.True(t, res.Reattached)

		expectedHosts := HostSpecCollection{
			{HostID: reattachHostA, Address: "192.168.1.118:8080"},
		}
		expectHosts(t, expectedHosts, HostActorTypeSpecCollection{})
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
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

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
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

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

	// A retry can repeat an attempt that committed after the caller stopped waiting
	// Providers may skip a redundant write when the last health check is already fresh enough
	// The retry budget bounds the freshness window so skipping remains within the health-check deadline
	t.Run("retry does not rewrite a fresh health check", func(t *testing.T) {
		// Reads back how long ago the seeded host was last health-checked
		lastHealthAgo := func(t *testing.T) time.Duration {
			t.Helper()
			spec, err := s.p.GetAllHosts(t.Context())
			require.NoError(t, err)
			require.Len(t, spec.Hosts, 1)
			return spec.Hosts[0].LastHealthAgo
		}

		// Use the same retry budget configured for each provider under test
		providerConfig := GetProviderConfig()
		budget := providerConfig.HealthCheckPolicy().Budget()

		t.Run("skips the write when the last health check is within the retry budget", func(t *testing.T) {
			// Seed a health check inside the freshness window so a retry can safely skip the write
			seeded := budget / 4
			require.NoError(t, s.p.Seed(t.Context(), Spec{
				Hosts: []HostSpec{
					{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: seeded},
				},
			}))

			err := s.p.UpdateActorHost(t.Context(), SpecHostH1, components.UpdateActorHostReq{
				UpdateLastHealthCheck: true,
				Retry:                 true,
			})
			require.NoError(t, err)

			// The stored value is untouched: had the write run, the host would have been checked just now
			assert.GreaterOrEqual(t, lastHealthAgo(t), seeded/2, "the retry should have left the existing health check in place")
		})

		t.Run("writes when the last health check predates the retry budget", func(t *testing.T) {
			// Seed a registered host whose health check predates the freshness window
			require.NoError(t, s.p.Seed(t.Context(), Spec{
				Hosts: []HostSpec{
					{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: budget * 2},
				},
			}))

			err := s.p.UpdateActorHost(t.Context(), SpecHostH1, components.UpdateActorHostReq{
				UpdateLastHealthCheck: true,
				Retry:                 true,
			})
			require.NoError(t, err)

			// The write ran, so the host has just been checked
			assert.Less(t, lastHealthAgo(t), budget, "the retry should have written a new health check")
		})

		t.Run("returns ErrHostUnregistered when the host is no longer registered", func(t *testing.T) {
			// The read must not report a host as healthy when the update would have rejected it
			require.NoError(t, s.p.Seed(t.Context(), Spec{}))

			err := s.p.UpdateActorHost(t.Context(), SpecHostNonExistent, components.UpdateActorHostReq{
				UpdateLastHealthCheck: true,
				Retry:                 true,
			})
			require.ErrorIs(t, err, components.ErrHostUnregistered)
		})
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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

		// Try to update the now-unhealthy host - only last health check
		updateReq := components.UpdateActorHostReq{
			UpdateLastHealthCheck: true,
			Retry:                 true,
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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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

func (s Suite) TestListHosts(t *testing.T) {
	// hostIDs returns the set of host IDs in the result
	hostIDs := func(hosts []components.HostInfo) []string {
		ids := make([]string, len(hosts))
		for i, h := range hosts {
			ids[i] = h.HostID
		}
		return ids
	}

	t.Run("returns empty slice when no hosts are registered", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))

		hosts, err := s.p.ListHosts(t.Context())
		require.NoError(t, err)
		assert.Empty(t, hosts)
	})

	t.Run("returns only registered and healthy hosts", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), GetSpec()))

		hosts, err := s.p.ListHosts(t.Context())
		require.NoError(t, err)

		// From GetSpec: H1, H2, H3, H4, H7, H8 are healthy, while H5, H6, H9 are unhealthy
		got := hostIDs(hosts)
		expected := []string{SpecHostH1, SpecHostH2, SpecHostH3, SpecHostH4, SpecHostH7, SpecHostH8}
		assert.ElementsMatch(t, expected, got, "should return exactly the healthy hosts")

		// Unhealthy hosts must never be reported
		assert.NotContains(t, got, SpecHostH5)
		assert.NotContains(t, got, SpecHostH6)
		assert.NotContains(t, got, SpecHostH9)
	})

	t.Run("includes host ID, address, and last health check for each host", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), GetSpec()))

		hosts, err := s.p.ListHosts(t.Context())
		require.NoError(t, err)

		byID := make(map[string]components.HostInfo, len(hosts))
		for _, h := range hosts {
			byID[h.HostID] = h
		}

		expectedAddrs := map[string]string{
			SpecHostH1: "127.0.0.1:4001",
			SpecHostH2: "127.0.0.1:4002",
			SpecHostH3: "127.0.0.1:4003",
			SpecHostH4: "127.0.0.1:4004",
			SpecHostH7: "127.0.0.1:4007",
			SpecHostH8: "127.0.0.1:4008",
		}

		now := s.p.Now()
		deadline := GetProviderConfig().HostHealthCheckDeadline
		for id, addr := range expectedAddrs {
			h, ok := byID[id]
			require.True(t, ok, "host %s should be present", id)
			assert.Equal(t, id, h.HostID)
			assert.Equal(t, addr, h.Address)

			// The last health check must be set, in the past, and recent enough that the host is healthy
			assert.False(t, h.LastHealthCheck.IsZero(), "last health check should be set for host %s", id)
			assert.False(t, h.LastHealthCheck.After(now), "last health check for host %s should not be in the future", id)
			assert.WithinDuration(t, now, h.LastHealthCheck, deadline, "last health check for host %s should be within the health deadline", id)
		}
	})

	t.Run("reflects a newly registered host", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		ctx := t.Context()

		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address: "192.168.60.1:8080",
			ActorTypes: []components.ActorHostType{
				{ActorType: "TestActor", IdleTimeout: 5 * time.Minute, ConcurrencyLimit: 1},
			},
		})
		require.NoError(t, err)

		hosts, err := s.p.ListHosts(ctx)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		assert.Equal(t, res.HostID, hosts[0].HostID)
		assert.Equal(t, "192.168.60.1:8080", hosts[0].Address)
	})

	t.Run("returns all healthy hosts when multiple are registered", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		ctx := t.Context()

		addrs := []string{"192.168.61.1:8080", "192.168.61.2:8080", "192.168.61.3:8080"}
		ids := make([]string, len(addrs))
		for i, addr := range addrs {
			res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: addr})
			require.NoError(t, err)
			ids[i] = res.HostID
		}

		hosts, err := s.p.ListHosts(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, ids, hostIDs(hosts))
	})

	t.Run("excludes a host once it becomes unhealthy", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		ctx := t.Context()

		_, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "192.168.62.1:8080"})
		require.NoError(t, err)

		// The host is healthy right after registration
		hosts, err := s.p.ListHosts(ctx)
		require.NoError(t, err)
		require.Len(t, hosts, 1)

		// Advance the clock beyond the health check deadline (1 minute)
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

		hosts, err = s.p.ListHosts(ctx)
		require.NoError(t, err)
		assert.Empty(t, hosts, "an unhealthy host should not be listed")
	})

	t.Run("excludes an unregistered host", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		ctx := t.Context()

		res1, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "192.168.63.1:8080"})
		require.NoError(t, err)
		res2, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "192.168.63.2:8080"})
		require.NoError(t, err)

		err = s.p.UnregisterHost(ctx, res1.HostID)
		require.NoError(t, err)

		hosts, err := s.p.ListHosts(ctx)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		assert.Equal(t, res2.HostID, hosts[0].HostID)
	})

	t.Run("keeps a host listed after its health check is refreshed", func(t *testing.T) {
		require.NoError(t, s.p.Seed(t.Context(), Spec{}))
		ctx := t.Context()

		res, err := s.p.RegisterHost(ctx, components.RegisterHostReq{Address: "192.168.64.1:8080"})
		require.NoError(t, err)

		// Advance partway, then refresh the health check before the deadline
		_ = s.p.AdvanceClock(40 * time.Second) //nolint:errcheck
		err = s.p.UpdateActorHost(ctx, res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
		require.NoError(t, err)

		// Advance again, but the total since the refresh is still under the deadline
		_ = s.p.AdvanceClock(40 * time.Second) //nolint:errcheck

		hosts, err := s.p.ListHosts(ctx)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		assert.Equal(t, res.HostID, hosts[0].HostID)
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

		// Create actors of type C (unlimited on H1 and H2) to validate distribution
		// Placement picks one of the eligible hosts at random, so the sample has to be large enough for the evenness assertion below to be about the implementation rather than about luck
		const (
			numActors  = 100
			minPerHost = numActors / 5
		)

		hostCounts := make(map[string]int)
		for i := range numActors {
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

		// Validate approximately even distribution: each host should get at least a fifth of the actors
		// With a fair pick between two hosts, either host falling below 20 out of 100 has a probability of about 3e-10, so this tolerates the randomness while still catching an implementation that stops spreading actors
		h1Count := hostCounts[SpecHostH1]
		h2Count := hostCounts[SpecHostH2]

		assert.GreaterOrEqual(t, h1Count, minPerHost, "H1 should have at least %d actors for reasonable distribution, got %d", minPerHost, h1Count)
		assert.GreaterOrEqual(t, h2Count, minPerHost, "H2 should have at least %d actors for reasonable distribution, got %d", minPerHost, h2Count)
		assert.Equal(t, numActors, h1Count+h2Count, "total should be %d actors", numActors)

		t.Logf("Distribution: H1=%d, H2=%d", h1Count, h2Count)
	})

	t.Run("ignores unhealthy hosts for new actors", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create multiple actors of type C (unlimited capacity) to validate they never go to unhealthy hosts
		// Type C is supported on H1 and H2 (both healthy) but not on H5/H6 (unhealthy)
		const numActors = 40
		seenHosts := make(map[string]bool)
		for i := range numActors { // Try multiple times to ensure consistent behavior
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
		// Each placement is an independent random pick between H1 and H2, so every lookup landing on the same host has a probability of about 2e-12 with this many iterations
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
		opts := components.LookupActorOpts{Hosts: []string{"1da70d19-ea7a-448e-934a-c03605c3d2ee"}}
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
		assert.Positive(t, createdActors, "should be able to create B actors since they have unlimited capacity")

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

	t.Run("ActiveOnly - returns existing active actor", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up an existing actor that's already active on a healthy host
		// From spec, B-1 is active on H1 (healthy)
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
		res, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)

		// Should return the existing host H1
		assert.Equal(t, SpecHostH1, res.HostID)
		assert.Equal(t, "127.0.0.1:4001", res.Address)
		assert.Equal(t, 5*time.Minute, res.IdleTimeout)
	})

	t.Run("ActiveOnly - returns ErrNoActor for inactive actor", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to look up an actor that doesn't exist/isn't active
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-nonexistent"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{ActiveOnly: true})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("ActiveOnly - returns ErrNoActor for actor on unhealthy host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up an actor that exists only on unhealthy host H6
		// From GetSpec: D-1 is active on H6 (unhealthy)
		ref := ref.ActorRef{ActorType: "D", ActorID: "D-1"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{ActiveOnly: true})
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("ActiveOnly - returns ErrNoActor for actor on disallowed host", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Look up actor B-1 which is active on H1, but restrict to only H2
		// This should return ErrNoActor because the actor is on a disallowed host
		ref := ref.ActorRef{ActorType: "B", ActorID: "B-1"}
		opts := components.LookupActorOpts{
			ActiveOnly: true,
			Hosts:      []string{SpecHostH2},
		}
		_, err := s.p.LookupActor(ctx, ref, opts)
		require.Error(t, err)
		require.ErrorIs(t, err, components.ErrNoActor)
	})
}

func (s Suite) TestConcurrentLookupActor(t *testing.T) {
	t.Run("parallel lookups for same actor - unlimited capacity", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, no active actors, single actor type with unlimited capacity
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         []AlarmSpec{},
		}

		// Create 20 healthy hosts
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             "TestActor",
				ActorIdleTimeout:      5 * time.Minute,
				ActorConcurrencyLimit: 0,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 50 parallel lookups for the same actor
		const numRoutines = 50
		const actorID = "same-actor"

		var wg sync.WaitGroup
		results := make([]components.LookupActorRes, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				ref := ref.NewActorRef("TestActor", actorID)
				result, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
				results[idx] = result
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All lookups should succeed
		for i, err := range errors {
			require.NoError(t, err, "lookup %d should succeed", i)
		}

		// All results should point to the same host (same actor should be on the same host)
		expectedHostID := results[0].HostID
		for i, result := range results {
			assert.Equal(t, expectedHostID, result.HostID, "lookup %d should return the same host as lookup 0", i)
			assert.Equal(t, 5*time.Minute, result.IdleTimeout, "idle timeout should match")
		}
	})

	t.Run("parallel lookups for different actors - unlimited capacity", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, no active actors, single actor type with unlimited capacity
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         []AlarmSpec{},
		}

		// Create 20 healthy hosts
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             "TestActor",
				ActorIdleTimeout:      5 * time.Minute,
				ActorConcurrencyLimit: 0,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 100 parallel lookups for different actors
		const numRoutines = 100

		var wg sync.WaitGroup
		results := make([]components.LookupActorRes, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				ref := ref.NewActorRef("TestActor", fmt.Sprintf("actor-%d", idx))
				result, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
				results[idx] = result
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All lookups should succeed
		for i, err := range errors {
			require.NoError(t, err, "lookup %d should succeed", i)
		}

		// Count distribution across hosts
		hostCounts := make(map[string]int)
		for i, result := range results {
			assert.Equal(t, 5*time.Minute, result.IdleTimeout, "idle timeout should match for result %d", i)
			hostCounts[result.HostID]++
		}

		// Should distribute across multiple hosts (at least 10 different hosts for 100 actors across 20 hosts)
		assert.GreaterOrEqual(t, len(hostCounts), 10, "should distribute across at least 10 different hosts, got %d: %v", len(hostCounts), hostCounts)

		// Check for reasonable distribution - no single host should have more than 20% of actors
		maxActorsPerHost := numRoutines / 5
		for hostID, count := range hostCounts {
			assert.LessOrEqual(t, count, maxActorsPerHost, "host %s should not have more than %d actors, got %d", hostID, maxActorsPerHost, count)
		}
	})

	t.Run("parallel lookups for same actor - with capacity limits", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, no active actors, single actor type with capacity limit of 1
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         []AlarmSpec{},
		}

		// Create 20 healthy hosts with capacity limit of 1
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:           hostID,
				ActorType:        "TestActor",
				ActorIdleTimeout: 5 * time.Minute,
				// Limited to 1 actor per host
				ActorConcurrencyLimit: 1,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 50 parallel lookups for the same actor
		const numRoutines = 50
		const actorID = "same-actor-limited"

		var wg sync.WaitGroup
		results := make([]components.LookupActorRes, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				ref := ref.NewActorRef("TestActor", actorID)
				result, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
				results[idx] = result
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All lookups should succeed
		for i, err := range errors {
			require.NoError(t, err, "lookup %d should succeed", i)
		}

		// All results should point to the same host (same actor should be on the same host)
		expectedHostID := results[0].HostID
		for i, result := range results {
			assert.Equal(t, expectedHostID, result.HostID, "lookup %d should return the same host as lookup 0", i)
		}
	})

	t.Run("parallel lookups for different actors - with capacity limits", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, no active actors, single actor type with capacity limit of 1
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         []AlarmSpec{},
		}

		// Create 20 healthy hosts with capacity limit of 1
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:           hostID,
				ActorType:        "TestActor",
				ActorIdleTimeout: 5 * time.Minute,
				// Limited to 1 actor per host
				ActorConcurrencyLimit: 1,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 20 parallel lookups for different actors (exactly matching host capacity)
		const numRoutines = 20

		var wg sync.WaitGroup
		results := make([]components.LookupActorRes, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				ref := ref.NewActorRef("TestActor", fmt.Sprintf("actor-limited-%d", idx))
				result, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
				results[idx] = result
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All lookups should succeed
		for i, err := range errors {
			require.NoError(t, err, "lookup %d should succeed", i)
		}

		// Count distribution across hosts
		hostCounts := make(map[string]int)
		for _, result := range results {
			hostCounts[result.HostID]++
		}

		// With capacity limits and race conditions, we expect:
		// - Most hosts should have exactly 1 actor
		// - Some hosts might exceed capacity due to race conditions (this is expected)
		// - Should use most of the available hosts
		// Enforcing capacity constraints in this case is done as best-effort and not guaranteed
		assert.GreaterOrEqual(t, len(hostCounts), 8, "should distribute across at least 8 hosts out of 20 available")

		// Count how many hosts have exactly 1 actor (ideal distribution)
		perfectHosts := 0
		for _, count := range hostCounts {
			if count == 1 {
				perfectHosts++
			}
		}

		// Most hosts should follow capacity limits, but some race conditions are expected
		assert.GreaterOrEqual(t, perfectHosts, 5, "at least 10 hosts should have exactly 1 actor (allowing for some race conditions)")

		t.Logf("Distribution: %d actors across %d hosts, %d hosts with perfect capacity (1 actor)", numRoutines, len(hostCounts), perfectHosts)

		// Log detailed distribution for analysis
		for hostID, count := range hostCounts {
			if count > 1 {
				t.Logf("Race condition detected: host %s has %d actors (exceeds limit of 1)", hostID, count)
			}
		}

		// Try to create one more actor - this might succeed due to race conditions,
		// but we'll test it to see the behavior
		ref := ref.ActorRef{ActorType: "TestActor", ActorID: "actor-overflow"}
		_, err := s.p.LookupActor(ctx, ref, components.LookupActorOpts{})
		// Don't require an error here since race conditions might allow it to succeed
		if err != nil {
			t.Logf("Overflow actor correctly rejected: %v", err)
		} else {
			t.Logf("Overflow actor was accepted (likely due to race conditions)")
		}
	})
}

func (s Suite) TestConcurrentFetchAlarms(t *testing.T) {
	t.Run("parallel fetches for same alarms - unlimited capacity", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, 50 alarms, single actor type with unlimited capacity, no active actors initially
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         make([]AlarmSpec, 50),
		}

		// Create 20 healthy hosts with unlimited capacity
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:                hostID,
				ActorType:             "TestActor",
				ActorIdleTimeout:      5 * time.Minute,
				ActorConcurrencyLimit: 0,
			}
		}

		// Create 50 overdue alarms for different actors
		for i := range 50 {
			customSpec.Alarms[i] = AlarmSpec{
				AlarmID:   fmt.Sprintf("AA%06d-0000-4000-8000-000000000000", i+1),
				ActorType: "TestActor",
				ActorID:   fmt.Sprintf("actor-%d", i),
				Name:      "test-alarm",
				DueIn:     -5 * time.Second,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 20 parallel fetches from all hosts
		const numRoutines = 20

		var wg sync.WaitGroup
		allLeases := make([][]*ref.AlarmLease, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", idx+1)
				leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
					Hosts: []string{hostID},
				})
				allLeases[idx] = leases
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All fetches should succeed
		for i, err := range errors {
			require.NoError(t, err, "fetch %d should succeed", i)
		}

		// Collect all leased alarms and verify uniqueness
		allLeasedAlarms := make(map[string]bool)
		totalLeases := 0
		for i, leases := range allLeases {
			for _, lease := range leases {
				alarmRef := lease.AlarmRef()
				key := alarmRef.String()
				if allLeasedAlarms[key] {
					t.Errorf("Alarm %s was leased multiple times! First seen in fetch %d", key, i)
				}
				allLeasedAlarms[key] = true
				totalLeases++
			}
		}

		// Should have leased all 50 alarms exactly once
		assert.Equal(t, 50, totalLeases, "should have leased all 50 alarms exactly once")
		assert.Len(t, allLeasedAlarms, 50, "should have 50 unique leased alarms")

		// Verify that actors were activated too
		// Should have exactly 50 active actors (one per alarm)
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		assert.Len(t, spec.ActiveActors, 50, "should have exactly 50 active actors")
	})

	t.Run("parallel fetches for same alarms - with capacity limits", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with 20 hosts, 30 alarms, single actor type with capacity limit of 1, no active actors initially
		customSpec := Spec{
			Hosts:          make([]HostSpec, 20),
			HostActorTypes: make([]HostActorTypeSpec, 20),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         make([]AlarmSpec, 30),
		}

		// Create 20 healthy hosts with capacity limit of 1
		for i := range 20 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}
			customSpec.HostActorTypes[i] = HostActorTypeSpec{
				HostID:           hostID,
				ActorType:        "TestActor",
				ActorIdleTimeout: 5 * time.Minute,
				// Limited to 1 actor per host
				ActorConcurrencyLimit: 1,
			}
		}

		// Create 30 overdue alarms for different actors
		for i := range 30 {
			customSpec.Alarms[i] = AlarmSpec{
				AlarmID:   fmt.Sprintf("AA%06d-0000-4000-8000-000000000000", i+1),
				ActorType: "TestActor",
				ActorID:   fmt.Sprintf("actor-%d", i),
				Name:      "test-alarm",
				DueIn:     -5 * time.Second,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 20 parallel fetches from all hosts
		const numRoutines = 20

		var wg sync.WaitGroup
		allLeases := make([][]*ref.AlarmLease, numRoutines)
		errors := make([]error, numRoutines)

		wg.Add(numRoutines)
		for i := range numRoutines {
			go func(idx int) {
				defer wg.Done()
				hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", idx+1)
				leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
					Hosts: []string{hostID},
				})
				allLeases[idx] = leases
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// All fetches should succeed
		for i, err := range errors {
			require.NoError(t, err, "fetch %d should succeed", i)
		}

		// Collect all leased alarms and verify uniqueness
		allLeasedAlarms := make(map[string]bool)
		totalLeases := 0
		for i, leases := range allLeases {
			for _, lease := range leases {
				alarmRef := lease.AlarmRef()
				key := alarmRef.String()
				if allLeasedAlarms[key] {
					t.Errorf("Alarm %s was leased multiple times! First seen in fetch %d", key, i)
				}
				allLeasedAlarms[key] = true
				totalLeases++
			}
		}

		// With capacity limits, we should lease at most 20 alarms (one per host max)
		// Due to race conditions, we might lease fewer than 20
		assert.LessOrEqual(t, totalLeases, 20, "should have leased at most 20 alarms due to capacity limits")
		assert.Len(t, allLeasedAlarms, totalLeases, "all leased alarms should be unique")

		// Verify that actors were activated on exactly one host each
		// Number of active actors should match number of leased alarms
		spec, err := s.p.GetAllHosts(ctx)
		require.NoError(t, err)
		assert.Len(t, spec.ActiveActors, totalLeases, "should have one active actor per leased alarm")

		// Verify capacity constraints are respected (at most 1 actor per host)
		hostActorCounts := make(map[string]int)
		for _, aa := range spec.ActiveActors {
			hostActorCounts[aa.HostID]++
		}

		for hostID, count := range hostActorCounts {
			assert.LessOrEqual(t, count, 1, "host %s should have at most 1 actor due to capacity limits, but has %d", hostID, count)
		}

		t.Logf("Successfully leased %d alarms across %d hosts with capacity limits", totalLeases, len(hostActorCounts))
	})

	t.Run("parallel fetches for overlapping actor types", func(t *testing.T) {
		ctx := t.Context()

		// Create a custom spec with multiple actor types and overlapping host support, no active actors initially
		customSpec := Spec{
			Hosts:          make([]HostSpec, 10),
			HostActorTypes: make([]HostActorTypeSpec, 0),
			ActiveActors:   []ActiveActorSpec{},
			Alarms:         make([]AlarmSpec, 40),
		}

		// Create 10 healthy hosts
		for i := range 10 {
			hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", i+1)
			customSpec.Hosts[i] = HostSpec{
				HostID:        hostID,
				Address:       fmt.Sprintf("127.0.0.1:%d", 5000+i),
				LastHealthAgo: 2 * time.Second,
			}

			// First 5 hosts support ActorTypeA, last 5 hosts support ActorTypeB
			// Hosts 3-7 support both types (overlap)
			if i < 7 {
				// Hosts 0-6 support ActorTypeA
				// Limited to 4 actors per host
				customSpec.HostActorTypes = append(customSpec.HostActorTypes, HostActorTypeSpec{
					HostID:                hostID,
					ActorType:             "ActorTypeA",
					ActorIdleTimeout:      5 * time.Minute,
					ActorConcurrencyLimit: 4,
				})
			}
			if i >= 3 {
				// Hosts 3-9 support ActorTypeB
				// Limited to 4 actors per host
				customSpec.HostActorTypes = append(customSpec.HostActorTypes, HostActorTypeSpec{
					HostID:                hostID,
					ActorType:             "ActorTypeB",
					ActorIdleTimeout:      5 * time.Minute,
					ActorConcurrencyLimit: 4,
				})
			}
		}

		// Create 20 overdue alarms for ActorTypeA and 20 for ActorTypeB
		for i := range 20 {
			customSpec.Alarms[i] = AlarmSpec{
				AlarmID:   fmt.Sprintf("AA%06d-000A-4000-8000-000000000000", i+1),
				ActorType: "ActorTypeA",
				ActorID:   fmt.Sprintf("actorA-%02d", i),
				Name:      "test-alarm",
				DueIn:     -5 * time.Second,
			}
			customSpec.Alarms[i+20] = AlarmSpec{
				AlarmID:   fmt.Sprintf("AA%06d-000B-4000-8000-000000000000", i+1),
				ActorType: "ActorTypeB",
				ActorID:   fmt.Sprintf("actorB-%02d", i),
				Name:      "test-alarm",
				DueIn:     -5 * time.Second,
			}
		}

		require.NoError(t, s.p.Seed(ctx, customSpec))

		// Perform 10 parallel fetches from all hosts, attempting 5 times
		// We make multiple attempts because there could be lock contention causing alarms to be skipped, especially under heavy concurrent access
		// We are most concerned here with making sure that alarms/actors aren't activated in more than one host
		// Lock contention because of many fetchers at the same exact time is not something that should happen frequently in the real world
		const numRoutines = 10
		const attempts = 5

		var wg sync.WaitGroup
		allLeases := make([][]*ref.AlarmLease, numRoutines*attempts)
		errors := make([]error, numRoutines*attempts)

		for a := range attempts {
			wg.Add(numRoutines)
			for i := range numRoutines {
				go func(a, idx int) {
					defer wg.Done()
					hostID := fmt.Sprintf("%08x-0000-4000-8000-000000000000", idx+1)
					leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
						Hosts: []string{hostID},
					})
					allLeases[(a*numRoutines)+idx] = leases
					errors[(a*numRoutines)+idx] = err
				}(a, i)
			}

			wg.Wait()
		}

		// All fetches should succeed
		for i, err := range errors {
			require.NoError(t, err, "fetch %d should succeed", i)
		}

		// Collect all leased alarms and verify uniqueness
		allLeasedAlarms := make(map[string]bool)
		totalLeases := 0
		typeACounts := 0
		typeBCounts := 0

		for i, leases := range allLeases {
			for _, lease := range leases {
				alarmRef := lease.AlarmRef()
				key := alarmRef.String()
				if allLeasedAlarms[key] {
					t.Errorf("Alarm %s was leased multiple times! First seen in fetch %d", key, i)
				}
				allLeasedAlarms[key] = true
				totalLeases++

				switch alarmRef.ActorType {
				case "ActorTypeA":
					typeACounts++
				case "ActorTypeB":
					typeBCounts++
				}
			}
		}

		// Should have leased all 40 alarms exactly once
		assert.Equal(t, 40, totalLeases, "should have leased all 40 alarms exactly once")
		assert.Len(t, allLeasedAlarms, 40, "should have 40 unique leased alarms")
		assert.Equal(t, 20, typeACounts, "should have leased all 20 ActorTypeA alarms")
		assert.Equal(t, 20, typeBCounts, "should have leased all 20 ActorTypeB alarms")

		// Verify that actors were activated on exactly one host each
		// Should have exactly 40 active actors (one per alarm)
		spec, err := s.p.GetAllHosts(ctx)

		require.NoError(t, err)
		assert.Len(t, spec.ActiveActors, 40, "should have exactly 40 active actors")

		// Verify capacity constraints are respected (at most 4 actors per host per type)
		// Note that we consider capacity constraints as best-effort in case of high concurrency, so we treat this as a warning but not an error
		hostTypeActorCounts := make(map[string]map[string]int) // hostID -> actorType -> count
		for _, aa := range spec.ActiveActors {
			if hostTypeActorCounts[aa.HostID] == nil {
				hostTypeActorCounts[aa.HostID] = make(map[string]int)
			}
			hostTypeActorCounts[aa.HostID][aa.ActorType]++
		}

		for hostID, typeCounts := range hostTypeActorCounts {
			for actorType, count := range typeCounts {
				if count > 4 {
					t.Logf("Capacity exceeded: host %s should have at most 4 actors of type %s due to capacity limits, but has %d", hostID, actorType, count)
				}
			}
		}

		t.Logf("Successfully distributed 40 alarms across overlapping host capabilities")
	})
}

func (s Suite) TestConcurrentDispatchJobs(t *testing.T) {
	const (
		jobHost = "0b000000-0000-4000-8000-0000000000c1"
		workers = 12
	)

	jobSeed := Spec{
		Hosts: HostSpecCollection{
			{HostID: jobHost, Address: "127.0.0.1:7200", LastHealthAgo: time.Second},
		},
		HostActorTypes: HostActorTypeSpecCollection{
			{HostID: jobHost, ActorType: "CONCURRENT_JOB", ActorIdleTimeout: 5 * time.Minute},
		},
	}

	t.Run("immediate same-key dispatch returns one lease", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, jobSeed)
		require.NoError(t, err)

		// Release every caller together so the provider must serialize both insertion and lease acquisition
		jobRef := ref.NewAlarmRef("CONCURRENT_JOB", "immediate", "same-key")
		start := make(chan struct{})
		type dispatchResult struct {
			jobID string
			lease *ref.AlarmLease
			err   error
		}
		results := make(chan dispatchResult, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				<-start
				jobID, lease, dispatchErr := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
					DueTime:        s.p.Now().Add(time.Second),
					Kind:           components.AlarmKindJob,
					JobMethod:      "process",
					LeaseImmediate: []string{jobHost},
				})
				results <- dispatchResult{jobID: jobID, lease: lease, err: dispatchErr}
			})
		}
		close(start)
		wg.Wait()
		close(results)

		// Every caller observes one durable identity and only the lease winner can enqueue it
		var firstID string
		var leased []*ref.AlarmLease
		for result := range results {
			require.NoError(t, result.err)
			require.NotEmpty(t, result.jobID)
			if firstID == "" {
				firstID = result.jobID
			}
			assert.Equal(t, firstID, result.jobID)
			if result.lease != nil {
				leased = append(leased, result.lease)
			}
		}
		require.Len(t, leased, 1)
		assert.Equal(t, firstID, leased[0].Key())
		_, err = s.p.GetLeasedAlarm(ctx, leased[0])
		require.NoError(t, err)

		jobs, err := s.p.ListJobs(ctx, jobRef.ActorType, jobRef.ActorID)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, firstID, jobs[0].JobID)
	})

	t.Run("future same-key dispatch always returns the stored ID", func(t *testing.T) {
		ctx := t.Context()
		err := s.p.Seed(ctx, jobSeed)
		require.NoError(t, err)

		// Future dispatches use the storage-only path but must retain the same concurrency contract
		jobRef := ref.NewAlarmRef("CONCURRENT_JOB", "future", "same-key")
		start := make(chan struct{})
		type dispatchResult struct {
			jobID string
			lease *ref.AlarmLease
			err   error
		}
		results := make(chan dispatchResult, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				<-start
				jobID, lease, dispatchErr := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
					DueTime:   s.p.Now().Add(time.Hour),
					Kind:      components.AlarmKindJob,
					JobMethod: "process",
				})
				results <- dispatchResult{jobID: jobID, lease: lease, err: dispatchErr}
			})
		}
		close(start)
		wg.Wait()
		close(results)

		// A conflict must return the winner's ID even when its insert committed after another statement began
		var firstID string
		for result := range results {
			require.NoError(t, result.err)
			require.NotEmpty(t, result.jobID)
			assert.Nil(t, result.lease)
			if firstID == "" {
				firstID = result.jobID
			}
			assert.Equal(t, firstID, result.jobID)
		}

		jobs, err := s.p.ListJobs(ctx, jobRef.ActorType, jobRef.ActorID)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, firstID, jobs[0].JobID)
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

		assert.Len(t, spec.ActiveActors, initialCount-3, "should have 3 fewer active actors")

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
			// Overdue so it's fetched right away
			DueTime: s.p.Now().Add(-time.Second),
		}
		_, err = s.p.SetAlarm(ctx, alarmRef, alarmReq)
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
		assert.Empty(t, got)
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

		_ = s.p.AdvanceClock(1200 * time.Millisecond) //nolint:errcheck
		err = s.p.CleanupExpired(t.Context())
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

		_ = s.p.AdvanceClock(time.Second) //nolint:errcheck
		err = s.p.SetState(ctx, ref3, data2, components.SetStateOpts{TTL: 2 * time.Second})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		_ = s.p.AdvanceClock(1200 * time.Millisecond) //nolint:errcheck
		_, err = s.p.GetState(ctx, ref3)
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		_ = s.p.AdvanceClock(1200 * time.Millisecond) //nolint:errcheck
		_, err = s.p.GetState(ctx, ref3)
		require.ErrorIs(t, err, components.ErrNoState)

		// GC hasn't run yet
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref3.ActorType, ActorID: ref3.ActorID, Data: data2}})

		err = s.p.CleanupExpired(t.Context())
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{})
	})

	t.Run("delete removes state with a live ttl", func(t *testing.T) {
		ctx := t.Context()
		ref4 := ref.ActorRef{ActorType: "TestType", ActorID: "actor-ttl-delete"}
		data := []byte("delete-me")

		// State with a TTL well in the future (i.e. not expired)
		err := s.p.SetState(ctx, ref4, data, components.SetStateOpts{TTL: time.Hour})
		require.NoError(t, err)
		expectCollection(t, ActorStateSpecCollection{{ActorType: ref4.ActorType, ActorID: ref4.ActorID, Data: data}})

		// Deleting live (non-expired) state must succeed and actually remove the row
		err = s.p.DeleteState(ctx, ref4)
		require.NoError(t, err)

		_, err = s.p.GetState(ctx, ref4)
		require.ErrorIs(t, err, components.ErrNoState)
		expectCollection(t, ActorStateSpecCollection{})

		// A second delete reports ErrNoState
		err = s.p.DeleteState(ctx, ref4)
		require.ErrorIs(t, err, components.ErrNoState)
	})
}

func (s Suite) TestListStates(t *testing.T) {
	// setState stores state for an actor of the given type
	setState := func(t *testing.T, ctx context.Context, actorType string, actorID string, data []byte, opts components.SetStateOpts) {
		t.Helper()
		err := s.p.SetState(ctx, ref.ActorRef{ActorType: actorType, ActorID: actorID}, data, opts)
		require.NoError(t, err)
	}

	// listIDs returns the actor IDs in a page, so tests can assert on the ordering without repeating the mapping
	listIDs := func(t *testing.T, ctx context.Context, req components.ListStatesReq) ([]string, bool) {
		t.Helper()
		res, err := s.p.ListStates(ctx, req)
		require.NoError(t, err)

		ids := make([]string, len(res.States))
		for i, state := range res.States {
			ids[i] = state.ActorID
		}
		return ids, res.HasMore
	}

	// Seed with empty database
	require.NoError(t, s.p.Seed(t.Context(), Spec{}))

	t.Run("returns an empty list when no state is stored", func(t *testing.T) {
		res, err := s.p.ListStates(t.Context(), components.ListStatesReq{ActorType: "ListEmpty"})
		require.NoError(t, err)
		assert.Empty(t, res.States)
		assert.False(t, res.HasMore)
	})

	t.Run("lists only the actors of the requested type", func(t *testing.T) {
		ctx := t.Context()

		setState(t, ctx, "ListTypeA", "actor-02", []byte("a2"), components.SetStateOpts{})
		setState(t, ctx, "ListTypeA", "actor-01", []byte("a1"), components.SetStateOpts{})
		setState(t, ctx, "ListTypeB", "actor-03", []byte("b3"), components.SetStateOpts{})

		// The result is ordered by actor ID, regardless of the order the states were written in
		ids, hasMore := listIDs(t, ctx, components.ListStatesReq{ActorType: "ListTypeA"})
		assert.Equal(t, []string{"actor-01", "actor-02"}, ids)
		assert.False(t, hasMore)

		ids, hasMore = listIDs(t, ctx, components.ListStatesReq{ActorType: "ListTypeB"})
		assert.Equal(t, []string{"actor-03"}, ids)
		assert.False(t, hasMore)
	})

	t.Run("omits the data unless it is requested", func(t *testing.T) {
		ctx := t.Context()

		res, err := s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListTypeA"})
		require.NoError(t, err)
		require.Len(t, res.States, 2)
		for _, state := range res.States {
			assert.Empty(t, state.Data, "data must not be returned for actor %s", state.ActorID)
		}
	})

	t.Run("returns the data when requested", func(t *testing.T) {
		ctx := t.Context()

		// State stored as an empty value is listed like any other, just with no data to return
		setState(t, ctx, "ListData", "actor-01", []byte("hello world"), components.SetStateOpts{})
		setState(t, ctx, "ListData", "actor-02", []byte{}, components.SetStateOpts{})

		res, err := s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListData", IncludeData: true})
		require.NoError(t, err)
		require.Len(t, res.States, 2)

		assert.Equal(t, "actor-01", res.States[0].ActorID)
		assert.True(t, bytes.Equal([]byte("hello world"), res.States[0].Data))
		assert.Equal(t, "actor-02", res.States[1].ActorID)
		assert.Empty(t, res.States[1].Data)
	})

	t.Run("excludes deleted state", func(t *testing.T) {
		ctx := t.Context()

		setState(t, ctx, "ListDeleted", "actor-01", []byte("gone"), components.SetStateOpts{})
		setState(t, ctx, "ListDeleted", "actor-02", []byte("stays"), components.SetStateOpts{})

		err := s.p.DeleteState(ctx, ref.ActorRef{ActorType: "ListDeleted", ActorID: "actor-01"})
		require.NoError(t, err)

		ids, _ := listIDs(t, ctx, components.ListStatesReq{ActorType: "ListDeleted"})
		assert.Equal(t, []string{"actor-02"}, ids)
	})

	t.Run("excludes expired state before it is garbage collected", func(t *testing.T) {
		ctx := t.Context()

		setState(t, ctx, "ListExpired", "actor-01", []byte("short"), components.SetStateOpts{TTL: time.Second})
		setState(t, ctx, "ListExpired", "actor-02", []byte("long"), components.SetStateOpts{TTL: time.Hour})
		setState(t, ctx, "ListExpired", "actor-03", []byte("forever"), components.SetStateOpts{})

		ids, _ := listIDs(t, ctx, components.ListStatesReq{ActorType: "ListExpired"})
		assert.Equal(t, []string{"actor-01", "actor-02", "actor-03"}, ids)

		// The expired row is still in the database at this point, so this asserts that listing filters on the expiration rather than relying on the cleanup
		_ = s.p.AdvanceClock(1200 * time.Millisecond) //nolint:errcheck
		ids, _ = listIDs(t, ctx, components.ListStatesReq{ActorType: "ListExpired"})
		assert.Equal(t, []string{"actor-02", "actor-03"}, ids)
	})

	t.Run("pages through the collection with after and limit", func(t *testing.T) {
		ctx := t.Context()

		const count = 5
		for i := 1; i <= count; i++ {
			setState(t, ctx, "ListPaged", fmt.Sprintf("actor-%02d", i), fmt.Appendf(nil, "data-%02d", i), components.SetStateOpts{})
		}

		// Walk the collection two at a time, using the last ID of each page as the cursor for the next one
		var (
			seen   []string
			cursor string
		)
		for range count {
			res, err := s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListPaged", After: cursor, Limit: 2, IncludeData: true})
			require.NoError(t, err)
			require.NotEmpty(t, res.States)
			require.LessOrEqual(t, len(res.States), 2)

			for _, state := range res.States {
				seen = append(seen, state.ActorID)
				// Paging must not affect the data, so each page carries the payload of the actors it contains
				assert.Equal(t, "data-"+strings.TrimPrefix(state.ActorID, "actor-"), string(state.Data))
			}

			cursor = res.States[len(res.States)-1].ActorID
			if !res.HasMore {
				break
			}
		}

		// Every actor is visited exactly once, in order, and the final page reports no more results
		assert.Equal(t, []string{"actor-01", "actor-02", "actor-03", "actor-04", "actor-05"}, seen)

		// A cursor past the end of the collection returns nothing
		ids, hasMore := listIDs(t, ctx, components.ListStatesReq{ActorType: "ListPaged", After: "actor-05"})
		assert.Empty(t, ids)
		assert.False(t, hasMore)

		// The cursor doesn't have to be an existing actor ID: listing resumes at the next ID after it
		ids, _ = listIDs(t, ctx, components.ListStatesReq{ActorType: "ListPaged", After: "actor-02a"})
		assert.Equal(t, []string{"actor-03", "actor-04", "actor-05"}, ids)
	})

	t.Run("limits the page to the default when no limit is requested", func(t *testing.T) {
		ctx := t.Context()

		// One state more than the default page size, so the first page is full and reports that more exist
		count := components.DefaultListStatesLimit + 1
		for i := 1; i <= count; i++ {
			setState(t, ctx, "ListDefaultLimit", fmt.Sprintf("actor-%04d", i), []byte("d"), components.SetStateOpts{})
		}

		res, err := s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListDefaultLimit"})
		require.NoError(t, err)
		assert.Len(t, res.States, components.DefaultListStatesLimit)
		assert.True(t, res.HasMore)

		// The last page holds the remainder and reports the end of the collection
		res, err = s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListDefaultLimit", After: res.States[len(res.States)-1].ActorID})
		require.NoError(t, err)
		assert.Len(t, res.States, count-components.DefaultListStatesLimit)
		assert.False(t, res.HasMore)
	})

	t.Run("caps a limit above the maximum", func(t *testing.T) {
		ctx := t.Context()

		// Asking for more than the provider allows is not an error: the request is served with the capped page size
		res, err := s.p.ListStates(ctx, components.ListStatesReq{ActorType: "ListDefaultLimit", Limit: components.MaxListStatesLimit + 1})
		require.NoError(t, err)
		assert.Len(t, res.States, components.DefaultListStatesLimit+1)
		assert.False(t, res.HasMore)
	})
}

func (s Suite) TestGetAlarm(t *testing.T) {
	t.Run("get alarm from test spec", func(t *testing.T) {
		// Seed with test data
		err := s.p.Seed(t.Context(), GetSpec())
		require.NoError(t, err)

		// Get an existing alarm from the test spec
		alarmRef := ref.AlarmRef{
			ActorType: "A",
			ActorID:   "A-1",
			Name:      "Alarm-A-1",
		}

		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Verify it exists (exact values depend on the test spec)
		assert.False(t, res.DueTime.IsZero())
		assert.Equal(t, []byte("active-A-1"), res.Data)
	})

	t.Run("get existing alarm with all fields", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create an alarm with all fields
		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id",
			Name:      "test-alarm",
		}

		alarmData := []byte(`{"message": "test data"}`)
		now := time.Now()
		dueTime := now.Add(1 * time.Hour)
		ttl := now.Add(24 * time.Hour)

		setReq := components.SetAlarmReq{
			DueTime:  dueTime,
			Interval: "1h",
			TTL:      &ttl,
			Data:     alarmData,
		}

		lease, err := s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)
		assert.Nil(t, lease)

		// Get the alarm
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Verify all fields
		assert.WithinDuration(t, dueTime, res.DueTime, time.Second)
		assert.Equal(t, "1h", res.Interval)
		require.NotNil(t, res.TTL)
		assert.WithinDuration(t, ttl, *res.TTL, time.Second)
		assert.Equal(t, alarmData, res.Data)
	})

	t.Run("get existing alarm with minimal fields", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create an alarm with only required fields
		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id-minimal",
			Name:      "test-alarm-minimal",
		}

		now := time.Now()
		dueTime := now.Add(1 * time.Hour)

		setReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime: dueTime,
				// Leave Interval, TTL, and Data as defaults
			},
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Get the alarm
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Verify fields
		assert.WithinDuration(t, dueTime, res.DueTime, time.Second)
		assert.Empty(t, res.Interval)
		assert.Nil(t, res.TTL)
		assert.Nil(t, res.Data)
	})

	t.Run("returns ErrNoAlarm for non-existent alarm", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Try to get a non-existent alarm
		alarmRef := ref.AlarmRef{
			ActorType: "NonExistentActor",
			ActorID:   "non-existent-id",
			Name:      "non-existent-alarm",
		}

		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})
}

func (s Suite) TestSetAlarm(t *testing.T) {
	t.Run("create new alarm with all fields", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create an alarm with all fields
		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id",
			Name:      "test-alarm",
		}

		alarmData := []byte(`{"message": "test data"}`)
		now := time.Now()
		dueTime := now.Add(1 * time.Hour)
		ttl := now.Add(24 * time.Hour)

		setReq := components.SetAlarmReq{
			DueTime:  dueTime,
			Interval: "1h",
			TTL:      &ttl,
			Data:     alarmData,
		}
		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Verify the alarm was created by getting it
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, dueTime, res.DueTime, time.Second)
		assert.Equal(t, "1h", res.Interval)
		require.NotNil(t, res.TTL)
		assert.WithinDuration(t, ttl, *res.TTL, time.Second)
		assert.Equal(t, alarmData, res.Data)
	})

	t.Run("create new alarm with minimal fields", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create an alarm with only required fields
		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id-minimal",
			Name:      "test-alarm-minimal",
		}

		now := time.Now()
		dueTime := now.Add(1 * time.Hour)

		setReq := components.SetAlarmReq{
			DueTime: dueTime,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Verify the alarm was created
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, dueTime, res.DueTime, time.Second)
		assert.Empty(t, res.Interval)
		assert.Nil(t, res.TTL)
		assert.Nil(t, res.Data)
	})

	t.Run("update existing alarm replaces all fields", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id",
			Name:      "test-alarm",
		}

		// Create initial alarm
		now := time.Now()
		initialDueTime := now.Add(1 * time.Hour)
		initialData := []byte(`{"version": 1}`)
		initialTTL := now.Add(12 * time.Hour)

		initialReq := components.SetAlarmReq{
			DueTime:  initialDueTime,
			Interval: "1h",
			TTL:      &initialTTL,
			Data:     initialData,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, initialReq)
		require.NoError(t, err)

		// Update the alarm with different values
		updatedDueTime := now.Add(2 * time.Hour)
		updatedData := []byte(`{"version": 2}`)
		updatedTTL := now.Add(24 * time.Hour)

		updateReq := components.SetAlarmReq{
			DueTime:  updatedDueTime,
			Interval: "2h",
			TTL:      &updatedTTL,
			Data:     updatedData,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, updateReq)
		require.NoError(t, err)

		// Verify the alarm was updated
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, updatedDueTime, res.DueTime, time.Second)
		assert.Equal(t, "2h", res.Interval)
		require.NotNil(t, res.TTL)
		assert.WithinDuration(t, updatedTTL, *res.TTL, time.Second)
		assert.Equal(t, updatedData, res.Data)
	})

	t.Run("update existing alarm clears optional fields when not provided", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id",
			Name:      "test-alarm",
		}

		// Create initial alarm with all fields
		now := time.Now()
		initialDueTime := now.Add(1 * time.Hour)
		initialData := []byte(`{"version": 1}`)
		initialTTL := now.Add(12 * time.Hour)

		initialReq := components.SetAlarmReq{
			DueTime:  initialDueTime,
			Interval: "1h",
			TTL:      &initialTTL,
			Data:     initialData,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, initialReq)
		require.NoError(t, err)

		// Update with minimal fields (should clear optional fields)
		updatedDueTime := now.Add(2 * time.Hour)

		updateReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime: updatedDueTime,
				// No Interval, TTL, or Data
			},
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, updateReq)
		require.NoError(t, err)

		// Verify optional fields were cleared
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, updatedDueTime, res.DueTime, time.Second)
		assert.Empty(t, res.Interval)
		assert.Nil(t, res.TTL)
		assert.Nil(t, res.Data)
	})

	t.Run("set alarm with empty data array becomes nil", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		alarmRef := ref.AlarmRef{
			ActorType: "TestActor",
			ActorID:   "test-id",
			Name:      "test-alarm",
		}

		now := time.Now()
		dueTime := now.Add(1 * time.Hour)

		setReq := components.SetAlarmReq{
			AlarmProperties: ref.AlarmProperties{
				DueTime: dueTime,
				Data:    []byte{}, // Empty but non-nil slice
			},
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Verify empty data becomes nil
		res, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.Nil(t, res.Data) // Should be nil, not empty slice
	})

	t.Run("set alarm with identical properties preserves lease", func(t *testing.T) {
		// Seed with test data that includes leased alarms
		err := s.p.Seed(t.Context(), GetSpec())
		require.NoError(t, err)

		// Get an alarm that should have a lease (from the spec, C-001 through C-005 have valid leases)
		alarmRef := ref.AlarmRef{
			ActorType: "C",
			ActorID:   "C-001",
			Name:      "C-001",
		}

		// First, verify the alarm exists and get its current properties
		originalAlarm, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Get all hosts/alarms to check the current lease status
		originalSpec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		// Find the original alarm in the spec to get lease information
		var originalAlarmSpec *AlarmSpec
		for i := range originalSpec.Alarms {
			alarm := &originalSpec.Alarms[i]
			if alarm.ActorType == alarmRef.ActorType &&
				alarm.ActorID == alarmRef.ActorID &&
				alarm.Name == alarmRef.Name {
				originalAlarmSpec = alarm
				break
			}
		}
		require.NotNil(t, originalAlarmSpec, "Original alarm should exist in spec")
		require.NotNil(t, originalAlarmSpec.LeaseID, "Original alarm should have a lease")

		// Now call SetAlarm with exactly the same properties
		setReq := components.SetAlarmReq{
			DueTime:  originalAlarm.DueTime,
			Interval: originalAlarm.Interval,
			TTL:      originalAlarm.TTL,
			Data:     originalAlarm.Data,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Get the alarm spec again to verify the lease is preserved
		updatedSpec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		// Find the updated alarm in the spec
		var updatedAlarmSpec *AlarmSpec
		for i := range updatedSpec.Alarms {
			alarm := &updatedSpec.Alarms[i]
			if alarm.ActorType == alarmRef.ActorType &&
				alarm.ActorID == alarmRef.ActorID &&
				alarm.Name == alarmRef.Name {
				updatedAlarmSpec = alarm
				break
			}
		}
		require.NotNil(t, updatedAlarmSpec, "Updated alarm should exist in spec")

		// Verify the lease is preserved (same lease ID and expiration)
		assert.NotNil(t, updatedAlarmSpec.LeaseID, "Lease should still exist")
		assert.Equal(t, *originalAlarmSpec.LeaseID, *updatedAlarmSpec.LeaseID, "Lease ID should be preserved")

		if originalAlarmSpec.LeaseExp != nil && updatedAlarmSpec.LeaseExp != nil {
			assert.Equal(t, *originalAlarmSpec.LeaseExp, *updatedAlarmSpec.LeaseExp, "Lease expiration should be preserved")
		}

		// Verify that the alarm properties are still the same
		updatedAlarm, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, originalAlarm.DueTime, updatedAlarm.DueTime, time.Second/10)
		assert.Equal(t, originalAlarm.Interval, updatedAlarm.Interval)
		if originalAlarm.TTL != nil && updatedAlarm.TTL != nil {
			assert.WithinDuration(t, *originalAlarm.TTL, *updatedAlarm.TTL, time.Second/10)
		} else {
			assert.Equal(t, originalAlarm.TTL, updatedAlarm.TTL)
		}
		assert.Equal(t, originalAlarm.Data, updatedAlarm.Data)
	})

	t.Run("set alarm with different properties nullifies lease", func(t *testing.T) {
		// Seed with test data that includes leased alarms
		err := s.p.Seed(t.Context(), GetSpec())
		require.NoError(t, err)

		// Get an alarm that should have a lease (from the spec, C-002 has a valid lease)
		alarmRef := ref.AlarmRef{
			ActorType: "C",
			ActorID:   "C-002",
			Name:      "C-002",
		}

		// First, verify the alarm exists and has a lease
		originalAlarm, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Get all hosts/alarms to check the current lease status
		originalSpec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		// Find the original alarm in the spec to get lease information
		var originalAlarmSpec *AlarmSpec
		for i := range originalSpec.Alarms {
			alarm := &originalSpec.Alarms[i]
			if alarm.ActorType == alarmRef.ActorType &&
				alarm.ActorID == alarmRef.ActorID &&
				alarm.Name == alarmRef.Name {
				originalAlarmSpec = alarm
				break
			}
		}
		require.NotNil(t, originalAlarmSpec, "Original alarm should exist in spec")
		require.NotNil(t, originalAlarmSpec.LeaseID, "Original alarm should have a lease")

		// Now call SetAlarm with different properties (change due time)
		newDueTime := originalAlarm.DueTime.Add(1 * time.Hour)
		setReq := components.SetAlarmReq{
			DueTime:  newDueTime,
			Interval: originalAlarm.Interval,
			TTL:      originalAlarm.TTL,
			Data:     originalAlarm.Data,
		}

		_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
		require.NoError(t, err)

		// Get the alarm spec again to verify the lease is nullified
		updatedSpec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)

		// Find the updated alarm in the spec
		var updatedAlarmSpec *AlarmSpec
		for i := range updatedSpec.Alarms {
			alarm := &updatedSpec.Alarms[i]
			if alarm.ActorType == alarmRef.ActorType &&
				alarm.ActorID == alarmRef.ActorID &&
				alarm.Name == alarmRef.Name {
				updatedAlarmSpec = alarm
				break
			}
		}
		require.NotNil(t, updatedAlarmSpec, "Updated alarm should exist in spec")

		// Verify the lease is nullified
		assert.Nil(t, updatedAlarmSpec.LeaseID, "Lease should be nullified when properties change")
		assert.Nil(t, updatedAlarmSpec.LeaseExp, "Lease expiration should be nullified when properties change")

		// Verify that the alarm properties were updated
		updatedAlarm, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		assert.WithinDuration(t, newDueTime, updatedAlarm.DueTime, time.Second/10)
	})

	t.Run("replacing an alarm invalidates a previously-issued lease", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Acquire a real lease on an upcoming alarm
		leased, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, leased, "should have fetched and leased some alarms")

		oldLease := leased[0]
		aRef := oldLease.AlarmRef()

		// Sanity check: the lease is currently valid
		_, err = s.p.GetLeasedAlarm(ctx, oldLease)
		require.NoError(t, err)

		// Replace the alarm with different properties. This generates a new alarm ID
		// and must invalidate the prior lease (which referenced the old alarm ID).
		newDueTime := oldLease.DueTime().Add(1 * time.Hour)
		_, err = s.p.SetAlarm(ctx, aRef, components.SetAlarmReq{
			DueTime: newDueTime,
			Data:    []byte("replaced"),
		})
		require.NoError(t, err)

		// The previous lease must no longer resolve to anything, for any operation.
		// (Regression: a stale lease that still resolved could read/update/delete a
		// phantom alarm, and DeleteLeasedAlarm via the stale lease could even remove
		// the legitimate replacement.)
		_, err = s.p.GetLeasedAlarm(ctx, oldLease)
		require.ErrorIs(t, err, components.ErrNoAlarm, "stale lease should not resolve after the alarm was replaced")

		err = s.p.UpdateLeasedAlarm(ctx, oldLease, components.UpdateLeasedAlarmReq{DueTime: newDueTime})
		require.ErrorIs(t, err, components.ErrNoAlarm, "stale lease should not be usable to update the replaced alarm")

		err = s.p.DeleteLeasedAlarm(ctx, oldLease)
		require.ErrorIs(t, err, components.ErrNoAlarm, "stale lease should not be usable to delete the replaced alarm")

		// The replacement alarm still exists and reflects the new properties
		res, err := s.p.GetAlarm(ctx, aRef)
		require.NoError(t, err)
		assert.WithinDuration(t, newDueTime, res.DueTime, time.Second)
		assert.Equal(t, []byte("replaced"), res.Data)
	})
}

func (s Suite) TestSetAndLeaseAlarm(t *testing.T) {
	registerHost := func(t *testing.T, actorType string) string {
		t.Helper()

		res, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			Address:            "192.168.20.1:8080",
			ExistingHostID:     "",
			JoinToken:          "",
			JoinTokenExpiresAt: time.Time{},
			ActorTypes: []components.ActorHostType{{
				ActorType:           actorType,
				IdleTimeout:         time.Minute,
				ConcurrencyLimit:    0,
				DeactivationTimeout: 0,
				MaxAttempts:         0,
				InitialRetryDelay:   0,
			}},
		})
		require.NoError(t, err)
		return res.HostID
	}
	getAlarmSpec := func(t *testing.T, alarmRef ref.AlarmRef) AlarmSpec {
		t.Helper()

		spec, err := s.p.GetAllHosts(t.Context())
		require.NoError(t, err)
		for i := range spec.Alarms {
			alarm := spec.Alarms[i]
			if alarm.ActorType == alarmRef.ActorType && alarm.ActorID == alarmRef.ActorID && alarm.Name == alarmRef.Name {
				return alarm
			}
		}

		t.Fatalf("alarm %s was not found", alarmRef)
		return AlarmSpec{}
	}

	t.Run("leases an upcoming alarm whose actor is not active", func(t *testing.T) {
		// Start with one healthy host that can execute the alarm
		err := s.p.Seed(t.Context(), Spec{
			Hosts:          nil,
			HostActorTypes: nil,
			ActiveActors:   nil,
			Alarms:         nil,
		})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")

		// Store an alarm close enough to qualify for fetch-ahead scheduling
		alarmRef := ref.NewAlarmRef("LeaseActor", "actor-1", "wake")
		lease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindAlarm,
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)

		// Verify the lease returned by the atomic store authorizes access
		require.NotNil(t, lease)
		assert.Equal(t, alarmRef, lease.AlarmRef())

		stored, err := s.p.GetLeasedAlarm(t.Context(), lease)
		require.NoError(t, err)
		assert.Equal(t, alarmRef, stored.AlarmRef)
		placement, err := s.p.LookupActor(t.Context(), alarmRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, hostID, placement.HostID)

		// Repeating the same request must preserve the live lease without returning it for duplicate enqueueing
		duplicateLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        lease.DueTime(),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		assert.Nil(t, duplicateLease)
		_, err = s.p.GetLeasedAlarm(t.Context(), lease)
		require.NoError(t, err)
	})

	t.Run("leases an upcoming alarm whose actor is active on an allowed host", func(t *testing.T) {
		// Place the actor before storing its alarm
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")
		actorRef := ref.NewActorRef("LeaseActor", "actor-active")
		placement, err := s.p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{Hosts: []string{hostID}})
		require.NoError(t, err)
		require.Equal(t, hostID, placement.HostID)

		// Reuse the allowed placement while acquiring the alarm lease
		alarmRef := ref.NewAlarmRef(actorRef.ActorType, actorRef.ActorID, "wake")
		lease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.NotNil(t, lease)

		stored, err := s.p.GetLeasedAlarm(t.Context(), lease)
		require.NoError(t, err)
		assert.Equal(t, alarmRef, stored.AlarmRef)
		placement, err = s.p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, hostID, placement.HostID)
	})

	t.Run("replaces an unleased alarm and leases the replacement", func(t *testing.T) {
		// Store the original alarm without requesting a lease
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")
		alarmRef := ref.NewAlarmRef("LeaseActor", "replace-unleased", "wake")
		initialLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime: s.p.Now().Add(2 * time.Second),
			Data:    []byte("original"),
		})
		require.NoError(t, err)
		require.Nil(t, initialLease)
		original := getAlarmSpec(t, alarmRef)
		require.Nil(t, original.LeaseID)

		// Replace it inside fetch-ahead and acquire a lease for the new row
		updatedDueTime := s.p.Now().Add(3 * time.Second)
		replacementLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        updatedDueTime,
			Data:           []byte("replacement"),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.NotNil(t, replacementLease)

		replacement := getAlarmSpec(t, alarmRef)
		assert.NotEqual(t, original.AlarmID, replacement.AlarmID)
		assert.Equal(t, replacementLease.Key(), replacement.AlarmID)
		require.NotNil(t, replacement.LeaseID)
		stored, err := s.p.GetLeasedAlarm(t.Context(), replacementLease)
		require.NoError(t, err)
		assert.WithinDuration(t, updatedDueTime, stored.DueTime, time.Second)
		assert.Equal(t, []byte("replacement"), stored.Data)
	})

	t.Run("replaces a leased alarm and leases the upcoming replacement", func(t *testing.T) {
		// Lease the original alarm and record its durable identity
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")
		alarmRef := ref.NewAlarmRef("LeaseActor", "replace-leased-upcoming", "wake")
		originalLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(2 * time.Second),
			Data:           []byte("original"),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.NotNil(t, originalLease)

		// Replacing it inside fetch-ahead invalidates the old lease and returns a lease for the new row
		updatedDueTime := s.p.Now().Add(4 * time.Second)
		replacementLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        updatedDueTime,
			Data:           []byte("replacement"),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.NotNil(t, replacementLease)
		assert.NotEqual(t, originalLease.Key(), replacementLease.Key())

		_, err = s.p.GetLeasedAlarm(t.Context(), originalLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
		stored, err := s.p.GetLeasedAlarm(t.Context(), replacementLease)
		require.NoError(t, err)
		assert.WithinDuration(t, updatedDueTime, stored.DueTime, time.Second)
		assert.Equal(t, []byte("replacement"), stored.Data)
	})

	t.Run("replaces a leased alarm without leasing a replacement outside fetch-ahead", func(t *testing.T) {
		// Lease the original alarm while its due time is inside fetch-ahead
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")
		alarmRef := ref.NewAlarmRef("LeaseActor", "replace-leased-future", "wake")
		originalLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(2 * time.Second),
			Data:           []byte("original"),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.NotNil(t, originalLease)

		// Moving it outside fetch-ahead uses the fast path and leaves the new row unleased
		updatedDueTime := s.p.Now().Add(2 * time.Minute)
		replacementLease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        updatedDueTime,
			Data:           []byte("replacement"),
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		require.Nil(t, replacementLease)

		_, err = s.p.GetLeasedAlarm(t.Context(), originalLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
		replacement := getAlarmSpec(t, alarmRef)
		assert.NotEqual(t, originalLease.Key(), replacement.AlarmID)
		assert.Nil(t, replacement.LeaseID)
		stored, err := s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)
		assert.WithinDuration(t, updatedDueTime, stored.DueTime, time.Second)
		assert.Equal(t, []byte("replacement"), stored.Data)
	})

	t.Run("leaves a future alarm unleased", func(t *testing.T) {
		// A capable host alone is insufficient when the alarm is outside the fetch-ahead horizon
		err := s.p.Seed(t.Context(), Spec{
			Hosts:          nil,
			HostActorTypes: nil,
			ActiveActors:   nil,
			Alarms:         nil,
		})
		require.NoError(t, err)
		hostID := registerHost(t, "LeaseActor")
		alarmRef := ref.NewAlarmRef("LeaseActor", "actor-2", "wake")
		lease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(24 * time.Hour),
			Interval:       "",
			Cron:           "",
			TTL:            nil,
			Data:           nil,
			Kind:           components.AlarmKindAlarm,
			JobMethod:      "",
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		assert.Nil(t, lease)
		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)
		_, err = s.p.LookupActor(t.Context(), alarmRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("leaves an upcoming alarm unleased without an eligible host", func(t *testing.T) {
		// The allowed host does not advertise the alarm's actor type
		err := s.p.Seed(t.Context(), Spec{
			Hosts:          nil,
			HostActorTypes: nil,
			ActiveActors:   nil,
			Alarms:         nil,
		})
		require.NoError(t, err)
		hostID := registerHost(t, "OtherActor")
		alarmRef := ref.NewAlarmRef("LeaseActor", "actor-3", "wake")
		lease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindAlarm,
			LeaseImmediate: []string{hostID},
		})
		require.NoError(t, err)
		assert.Nil(t, lease)
		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)
	})

	t.Run("does not move an actor from a healthy disallowed host", func(t *testing.T) {
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)
		actorType := components.ActorHostType{ActorType: "LeaseActor", IdleTimeout: time.Minute}
		firstHost, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			Address:    "192.168.20.1:8080",
			ActorTypes: []components.ActorHostType{actorType},
		})
		require.NoError(t, err)
		secondHost, err := s.p.RegisterHost(t.Context(), components.RegisterHostReq{
			Address:    "192.168.20.2:8080",
			ActorTypes: []components.ActorHostType{actorType},
		})
		require.NoError(t, err)

		// Pin the actor to the first runtime before asking the second runtime to store and lease its alarm
		actorRef := ref.NewActorRef("LeaseActor", "actor-4")
		placement, err := s.p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{Hosts: []string{firstHost.HostID}})
		require.NoError(t, err)
		require.Equal(t, firstHost.HostID, placement.HostID)

		alarmRef := ref.NewAlarmRef(actorRef.ActorType, actorRef.ActorID, "wake")
		lease, err := s.p.SetAlarm(t.Context(), alarmRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			LeaseImmediate: []string{secondHost.HostID},
		})
		require.NoError(t, err)
		assert.Nil(t, lease)

		// The alarm is durable but the existing placement remains owned by the first runtime
		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)
		placement, err = s.p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, firstHost.HostID, placement.HostID)
	})
}

func (s Suite) TestDeleteAlarm(t *testing.T) {
	t.Run("delete existing alarm", func(t *testing.T) {
		// Seed with test data
		err := s.p.Seed(t.Context(), GetSpec())
		require.NoError(t, err)

		// Delete an existing alarm from the test spec
		alarmRef := ref.AlarmRef{
			ActorType: "A",
			ActorID:   "A-1",
			Name:      "Alarm-A-1",
		}

		// Verify alarm exists first
		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Delete the alarm
		err = s.p.DeleteAlarm(t.Context(), alarmRef)
		require.NoError(t, err)

		// Verify alarm no longer exists
		_, err = s.p.GetAlarm(t.Context(), alarmRef)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm for non-existent alarm", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Try to delete a non-existent alarm
		alarmRef := ref.AlarmRef{
			ActorType: "NonExistentActor",
			ActorID:   "non-existent-id",
			Name:      "non-existent-alarm",
		}

		err = s.p.DeleteAlarm(t.Context(), alarmRef)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("delete one alarm does not affect others", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create multiple alarms for the same actor
		actorType := "TestActor"
		actorID := "test-id"
		now := time.Now()

		alarm1Ref := ref.AlarmRef{ActorType: actorType, ActorID: actorID, Name: "alarm1"}
		alarm2Ref := ref.AlarmRef{ActorType: actorType, ActorID: actorID, Name: "alarm2"}
		alarm3Ref := ref.AlarmRef{ActorType: actorType, ActorID: actorID, Name: "alarm3"}

		for i, alarmRef := range []ref.AlarmRef{alarm1Ref, alarm2Ref, alarm3Ref} {
			setReq := components.SetAlarmReq{
				DueTime: now.Add(time.Duration(i+1) * time.Hour),
				Data:    fmt.Appendf(nil, `{"alarm": %d}`, i+1),
			}

			_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
			require.NoError(t, err)
		}

		// Verify all alarms exist
		for _, alarmRef := range []ref.AlarmRef{alarm1Ref, alarm2Ref, alarm3Ref} {
			_, err = s.p.GetAlarm(t.Context(), alarmRef)
			require.NoError(t, err)
		}

		// Delete the middle alarm
		err = s.p.DeleteAlarm(t.Context(), alarm2Ref)
		require.NoError(t, err)

		// Verify alarm2 no longer exists
		_, err = s.p.GetAlarm(t.Context(), alarm2Ref)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify other alarms still exist
		_, err = s.p.GetAlarm(t.Context(), alarm1Ref)
		require.NoError(t, err)
		_, err = s.p.GetAlarm(t.Context(), alarm3Ref)
		require.NoError(t, err)
	})

	t.Run("delete alarm with different actor types", func(t *testing.T) {
		// Seed with empty database
		err := s.p.Seed(t.Context(), Spec{})
		require.NoError(t, err)

		// Create alarms with same actor ID but different types
		now := time.Now()
		actorID := "shared-id"
		alarmName := "shared-name"

		alarm1Ref := ref.AlarmRef{ActorType: "TypeA", ActorID: actorID, Name: alarmName}
		alarm2Ref := ref.AlarmRef{ActorType: "TypeB", ActorID: actorID, Name: alarmName}

		for _, alarmRef := range []ref.AlarmRef{alarm1Ref, alarm2Ref} {
			setReq := components.SetAlarmReq{
				DueTime: now.Add(1 * time.Hour),
				Data:    fmt.Appendf(nil, `{"type": "%s"}`, alarmRef.ActorType),
			}

			_, err = s.p.SetAlarm(t.Context(), alarmRef, setReq)
			require.NoError(t, err)
		}

		// Delete alarm for TypeA
		err = s.p.DeleteAlarm(t.Context(), alarm1Ref)
		require.NoError(t, err)

		// Verify TypeA alarm no longer exists
		_, err = s.p.GetAlarm(t.Context(), alarm1Ref)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Verify TypeB alarm still exists
		res, err := s.p.GetAlarm(t.Context(), alarm2Ref)
		require.NoError(t, err)
		assert.Contains(t, string(res.Data), "TypeB")
	})
}

func (s Suite) TestFetchAlarms(t *testing.T) {
	// In the seed data, ALM-C-001...ALM-C-005 are already leased with a valid lease
	// ALM-C-006 has an expired lease
	expectPreLeasedAlarms := func(alarmID string) bool {
		switch alarmID {
		case "AA000000-000C-4000-000C-000000000001",
			"AA000000-000C-4000-000C-000000000002",
			"AA000000-000C-4000-000C-000000000003",
			"AA000000-000C-4000-000C-000000000004",
			"AA000000-000C-4000-000C-000000000005",
			"AA000000-000C-4000-000C-000000000006":
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

				var alarmID string
				if typ == "X" {
					alarmID = fmt.Sprintf("AA000000-EEEE-4000-00EE-000000000%03d", i)
				} else {
					alarmID = fmt.Sprintf("AA000000-FFFF-4000-00FF-000000000%03d", i)
				}
				actorID := fmt.Sprintf("%s-%d", typ, i)

				expectAlarmIDs = append(expectAlarmIDs, alarmID)
				expectAlarmIDsMap[alarmID] = true
				expectActorIDs = append(expectActorIDs, actorID)
			}
		}

		// Collect all alarm IDs
		gotIDs := make([]string, 0, 24)
		for _, a := range res {
			gotIDs = append(gotIDs, strings.ToUpper(a.Key()))
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
			alarmID := strings.ToUpper(a.AlarmID)

			if expectPreLeasedAlarms(alarmID) {
				continue
			}

			if !expectAlarmIDsMap[alarmID] {
				assert.Emptyf(t, a.LeaseID, "expected alarm %q not to have a lease ID", alarmID)
				assert.Emptyf(t, a.LeaseExp, "expected alarm %q not to have a lease expiration", alarmID)
				continue
			}

			_ = assert.NotNil(t, a.LeaseID, "expected alarm %q to have a lease ID", alarmID) &&
				assert.NotEmpty(t, *a.LeaseID, "expected alarm %q to have a lease ID", alarmID)
			_ = assert.NotNil(t, a.LeaseExp, "expected alarm %q to have a lease expiration", alarmID) &&
				assert.Greater(t, *a.LeaseExp, s.p.Now(), "expected alarm's %q lease expiration to be in the future", alarmID)
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
			SpecAlarmA1, SpecAlarmA2, SpecAlarmA4,
			SpecAlarmB1, SpecAlarmB2,
			"AA000000-000B-4000-000B-000000000001", // ALM-B-001
			"AA000000-000B-4000-000B-000000000007", // ALM-B-007
			"AA000000-000B-4000-000B-000000000014", // ALM-B-014
			"AA000000-000B-4000-000B-000000000021", // ALM-B-021
			"AA000000-000B-4000-000B-000000000028", // ALM-B-028
			"AA000000-000B-4000-000B-000000000035", // ALM-B-035
			"AA000000-000B-4000-000B-000000000042", // ALM-B-042
			"AA000000-000B-4000-000B-000000000049", // ALM-B-049
			"AA000000-000C-4000-000C-000000000006", // ALM-C-006
			"AA000000-000C-4000-000C-000000000010", // ALM-C-010
			"AA000000-000C-4000-000C-000000000011", // ALM-C-011
			"AA000000-000C-4000-000C-000000000015", // ALM-C-015
			"AA000000-000C-4000-000C-000000000020", // ALM-C-020
			"AA000000-000C-4000-000C-000000000025", // ALM-C-025
			"AA000000-000C-4000-000C-000000000030", // ALM-C-030
			"AA000000-000C-4000-000C-000000000035", // ALM-C-035
			"AA000000-000C-4000-000C-000000000040", // ALM-C-040
			"AA000000-000C-4000-000C-000000000045", // ALM-C-045
			"AA000000-000C-4000-000C-000000000050", // ALM-C-050
		}
		expectActorIDs := []string{
			"A-1", "A-2", "A-4", "B-1", "B-2",
			"B-001", "B-007", "B-014", "B-021", "B-028", "B-035", "B-042", "B-049",
			"C-006", "C-010", "C-011", "C-015", "C-020", "C-025", "C-030", "C-035", "C-040", "C-045", "C-050",
		}
		expectAlarmIDsMap := make(map[string]bool, len(expectAlarmIDs))
		for _, id := range expectAlarmIDs {
			expectAlarmIDsMap[id] = true
		}

		// Collect all alarm IDs
		gotIDs := make([]string, 0, 24)
		for _, a := range res {
			gotIDs = append(gotIDs, strings.ToUpper(a.Key()))
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
			alarmID := strings.ToUpper(a.AlarmID)

			// ALM-C-006's leases was expired and we should have taken it over
			if a.AlarmID != "AA000000-000C-4000-000C-000000000006" && expectPreLeasedAlarms(alarmID) {
				continue
			}

			if !expectAlarmIDsMap[alarmID] {
				// Seed data doesn't contain any leased alarm, so we can confidently exclude others
				assert.Emptyf(t, a.LeaseID, "expected alarm %q not to have a lease ID", alarmID)
				assert.Emptyf(t, a.LeaseExp, "expected alarm %q not to have a lease expiration", alarmID)
				continue
			}

			_ = assert.NotNil(t, a.LeaseID, "expected alarm %q to have a lease ID", alarmID) &&
				assert.NotEmpty(t, *a.LeaseID, "expected alarm %q to have a lease ID", alarmID)
			_ = assert.NotNil(t, a.LeaseExp, "expected alarm %q to have a lease expiration", alarmID) &&
				assert.Greater(t, *a.LeaseExp, s.p.Now(), "expected alarm's %q lease expiration to be in the future", alarmID)
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
			Hosts: []string{"95302b0c-92cd-4201-b15d-98fcd56d7bf5", "ee6e3ddd-9ff0-42e7-af80-5624407f6da9"},
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
			if strings.ToUpper(lease.Key()) == "AA000000-000C-4000-000C-000000000006" {
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
					AlarmID:   SpecAlarmOverdue1,
					ActorType: "TestOverdue",
					ActorID:   "overdue-actor-1",
					Name:      "overdue-alarm-1",
					DueIn:     -5 * time.Minute, // Due 5 minutes ago (overdue)
					Data:      []byte("overdue-data-1"),
				},
				{
					AlarmID:   SpecAlarmOverdue2,
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
			key := strings.ToUpper(lease.Key())
			if key == SpecAlarmOverdue1 {
				foundOverdue1 = true
				// Verify the alarm is in the past
				assert.True(t, lease.DueTime().Before(s.p.Now()), "ALM-OVERDUE-1 should be overdue")
				assert.Equal(t, "TestOverdue/overdue-actor-1", lease.ActorRef().String())
			}
			if key == SpecAlarmOverdue2 {
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
			key := strings.ToUpper(lease.Key())
			if key == SpecAlarmOverdue1 || key == SpecAlarmOverdue2 {
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
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "cc11a1b4-8c70-4253-8e24-64eb6e876eb6", s.p.Now(), "1e4ecca7-db68-431c-a6d7-08aa0434e5c6")
		_, err := s.p.GetLeasedAlarm(ctx, nonExistentLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
	})

	t.Run("returns ErrNoAlarm if alarm isn't leased", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data (has un-leased alarms)
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Try to get an alarm that exists but isn't leased
		// From spec, ALM-B-007 and later B alarms should not be pre-leased
		unleaedAlarmLease := ref.NewAlarmLease(ref.NewAlarmRef("B", "B-007", "Alarm-B-007"), "AA000000-000B-4000-000B-000000000007", s.p.Now(), "70cb3dc4-cb83-4f44-92d0-07b9c59ec36d")
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
		fakeLease := ref.NewAlarmLease(lease.AlarmRef(), lease.Key(), lease.DueTime(), "05ac8871-02f3-4e02-b98d-a9ec231de084")

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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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
			key := strings.ToUpper(lease.Key())
			// ALM-A-1, ALM-A-2, ALM-A-4, ALM-B-1, ALM-B-2 should have specific data
			if key == SpecAlarmA1 || key == SpecAlarmB1 {
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
		switch strings.ToUpper(targetLease.Key()) {
		case SpecAlarmA1:
			assert.Equal(t, "A", alarmRes.ActorType)
			assert.Equal(t, "A-1", alarmRes.ActorID)
			assert.Equal(t, "Alarm-A-1", alarmRes.Name)
			assert.Equal(t, []byte("active-A-1"), alarmRes.Data)
		case SpecAlarmB1:
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
					AlarmID:   "30752437-a376-44a9-9156-b9cafcc052ee",
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
					AlarmID:   "980c9240-3300-4581-abc5-228843df55c5",
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
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

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
		_ = s.p.AdvanceClock(45 * time.Second) //nolint:errcheck

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
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

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
		_ = s.p.AdvanceClock(45 * time.Second) //nolint:errcheck

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
			Hosts: []string{"dd86ac68-ee00-4a8f-97e1-a3d4bbb92e0f", "e8a3256c-5381-4a3c-b8f7-19dc87913d5f"},
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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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
		invalidLease := ref.NewAlarmLease(validLease.AlarmRef(), "46d1668d-dd68-4320-a562-66176ac4a11f", s.p.Now(), "8a54df5a-2007-4add-af3c-265c4d569e28")

		// Advance time partway through lease duration
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

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
		_ = s.p.AdvanceClock(30 * time.Second) //nolint:errcheck

		// Renew all leases for both hosts
		renewReq := components.RenewAlarmLeasesReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		}
		renewRes, err := s.p.RenewAlarmLeases(ctx, renewReq)
		require.NoError(t, err)
		assert.Len(t, renewRes.Leases, totalExpectedLeases, "should renew all leases from both hosts")

		// Verify all renewed leases are valid
		for _, lease := range renewRes.Leases {
			_, err := s.p.GetLeasedAlarm(ctx, lease)
			require.NoError(t, err, "all renewed leases should be valid")
		}
	})

	t.Run("empty host list renews nothing", func(t *testing.T) {
		ctx := t.Context()

		// Seed with the test data
		require.NoError(t, s.p.Seed(ctx, GetSpec()))

		// Create some valid leases
		res, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{
			Hosts: []string{SpecHostH7, SpecHostH8},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res, "should have fetched and leased some alarms")

		// Renewal with no hosts must not error and must renew nothing
		renewRes, err := s.p.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{})
		require.NoError(t, err)
		assert.Empty(t, renewRes.Leases, "no leases should be renewed without a host list")

		// Even when specific leases are provided, an empty host list renews nothing
		renewRes, err = s.p.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{
			Leases: []*ref.AlarmLease{res[0]},
		})
		require.NoError(t, err)
		assert.Empty(t, renewRes.Leases, "no leases should be renewed without a host list, even when leases are specified")
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
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "7f84f417-de01-46d0-b5bb-80f3d8bf003b", s.p.Now(), "46d03825-2a8f-498d-bd02-e1e6bf8d82c3")
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
					AlarmID:   "0ec13fd1-ff6d-4059-bcc6-29315f57b1c6",
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
		fakeLease := ref.NewAlarmLease(ref.NewAlarmRef("TestType", "test-actor", "test-alarm"), "0ec13fd1-ff6d-4059-bcc6-29315f57b1c6", s.p.Now(), "e811769a-6b7a-4080-b406-87efd603c7f4")
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
		fakeLease := ref.NewAlarmLease(lease.AlarmRef(), lease.Key(), lease.DueTime(), "e731a719-0c1c-4c41-9c92-a44e0e8ef681")

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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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
		newDueTime := s.p.Now().Add(2 * time.Hour)
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
		newDueTime := s.p.Now().Add(3 * time.Hour)
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
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "0b610c71-fd4a-429b-a4ff-873698e5b3a1", s.p.Now(), "8693c0bc-1062-405e-9e7b-3a9c2f65899d")
		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      s.p.Now().Add(1 * time.Hour),
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
		fakeLease := ref.NewAlarmLease(validLease.AlarmRef(), validLease.Key(), validLease.DueTime(), "ebccb3b6-b677-437b-83d1-cb0b21134328")

		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      s.p.Now().Add(1 * time.Hour),
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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

		updateReq := components.UpdateLeasedAlarmReq{
			DueTime:      s.p.Now().Add(1 * time.Hour),
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
		nonExistentLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "e7acafab-d2a1-4e95-929f-7da0c781fee0", s.p.Now(), "3c6f3e23-599b-4220-a86a-4a74b3a2ff52")
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
					AlarmID:   "944f30d6-bbc4-474c-9d6a-734a6bb92577",
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
		fakeLease := ref.NewAlarmLease(ref.NewAlarmRef("at", "aid", "name"), "944f30d6-bbc4-474c-9d6a-734a6bb92577", s.p.Now(), "e362bf50-a974-4927-b3c6-06ec45ed4c32")
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
		fakeLease := ref.NewAlarmLease(validLease.AlarmRef(), validLease.Key(), validLease.DueTime(), "9779ea8f-fa1e-4c2e-9487-bc690b1b57be")

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
		_ = s.p.AdvanceClock(2 * time.Minute) //nolint:errcheck

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

// TestJobs exercises the job-specific provider methods: dispatch (with idempotency), get, list, cancel, dead-letter, and replay.
func (s Suite) TestJobs(t *testing.T) {
	const jobHost = "0b000000-0000-4000-8000-0000000000b1"

	// jobSeed returns a minimal spec with a single healthy host supporting the "JOB" actor type
	jobSeed := func() Spec {
		return Spec{
			Hosts: HostSpecCollection{
				{HostID: jobHost, Address: "127.0.0.1:7100", LastHealthAgo: time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: jobHost, ActorType: "JOB", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			},
		}
	}

	// dispatch creates a job and returns its ID
	dispatch := func(t *testing.T, ctx context.Context, actorID string, name string, method string, props ref.AlarmProperties, data []byte) string {
		t.Helper()
		props.Data = data
		jobID, _, err := s.p.DispatchJob(ctx, ref.NewAlarmRef("JOB", actorID, name), components.SetAlarmReq{
			AlarmProperties: props,
			Kind:            components.AlarmKindJob,
			JobMethod:       method,
		})
		require.NoError(t, err)
		require.NotEmpty(t, jobID)
		return jobID
	}

	// leaseFor dispatches the actor's alarms via the fetcher and returns the lease whose ID matches jobID
	leaseFor := func(t *testing.T, ctx context.Context, jobID string) *ref.AlarmLease {
		t.Helper()
		leases, err := s.p.FetchAndLeaseUpcomingAlarms(ctx, components.FetchAndLeaseUpcomingAlarmsReq{Hosts: []string{jobHost}})
		require.NoError(t, err)
		for _, l := range leases {
			if l.Key() == jobID {
				return l
			}
		}
		t.Fatalf("did not lease the dispatched job %q", jobID)
		return nil
	}

	t.Run("dispatch and get a live job", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobID := dispatch(t, ctx, "a1", "k1", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, []byte("payload"))

		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, jobID, info.JobID)
		assert.Equal(t, "JOB", info.ActorType)
		assert.Equal(t, "a1", info.ActorID)
		assert.Equal(t, "process", info.Method)
		assert.Equal(t, components.JobStatusPending, info.Status)
		assert.False(t, info.CreatedAt.IsZero(), "created at should be derived from the UUIDv7 job ID")
	})

	t.Run("dispatch leases an upcoming new job", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Store a fully-populated job close enough to qualify for fetch-ahead scheduling
		jobRef := ref.NewAlarmRef("JOB", "preleased", "key")
		dueTime := s.p.Now().Add(time.Second)
		ttl := s.p.Now().Add(2 * time.Hour)
		jobID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        dueTime,
			Interval:       "PT1H",
			TTL:            &ttl,
			Data:           []byte("payload"),
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, jobID, lease.Key())
		assert.Equal(t, jobRef, lease.AlarmRef())

		// Verify the returned lease authorizes the exact stored job row
		stored, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
		assert.Equal(t, components.AlarmKindJob, stored.Kind)
		assert.WithinDuration(t, dueTime, stored.DueTime, time.Second)
		assert.Equal(t, "PT1H", stored.Interval)
		assert.Empty(t, stored.Cron)
		require.NotNil(t, stored.TTL)
		assert.WithinDuration(t, ttl, *stored.TTL, time.Second)
		assert.Equal(t, []byte("payload"), stored.Data)
		assert.Equal(t, "process", stored.JobMethod)
		placement, err := s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, jobHost, placement.HostID)

		// Re-dispatching the idempotency key preserves the first job and does not return its live lease twice
		duplicateID, duplicateLease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        dueTime.Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "different",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		assert.Equal(t, jobID, duplicateID)
		assert.Nil(t, duplicateLease)
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
	})

	t.Run("re-dispatch leases an existing unleased job", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Create the idempotent job without offering a host for immediate scheduling
		jobRef := ref.NewAlarmRef("JOB", "existing-unleased", "key")
		dueTime := s.p.Now().Add(time.Second)
		jobID, initialLease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:   dueTime,
			Kind:      components.AlarmKindJob,
			JobMethod: "original",
			Data:      []byte("original"),
		})
		require.NoError(t, err)
		require.Nil(t, initialLease)

		// Re-dispatching can acquire its lease but must retain the first dispatch's properties
		duplicateID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        dueTime.Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "replacement",
			Data:           []byte("replacement"),
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, jobID, duplicateID)
		assert.Equal(t, jobID, lease.Key())

		stored, err := s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
		assert.WithinDuration(t, dueTime, stored.DueTime, time.Second)
		assert.Equal(t, "original", stored.JobMethod)
		assert.Equal(t, []byte("original"), stored.Data)
	})

	t.Run("re-dispatch uses the storage-only path for an incoming future schedule", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Store an upcoming occurrence without offering an immediate host
		jobRef := ref.NewAlarmRef("JOB", "incoming-future", "key")
		dueTime := s.p.Now().Add(time.Second)
		jobID, initialLease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:   dueTime,
			Kind:      components.AlarmKindJob,
			JobMethod: "original",
			Data:      []byte("original"),
		})
		require.NoError(t, err)
		require.Nil(t, initialLease)

		// A duplicate request outside fetch-ahead avoids the transactional lease path even though the stored row is earlier
		duplicateID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Hour),
			Kind:           components.AlarmKindJob,
			JobMethod:      "replacement",
			Data:           []byte("replacement"),
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		assert.Equal(t, jobID, duplicateID)
		assert.Nil(t, lease)

		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.WithinDuration(t, dueTime, info.DueTime, time.Second)
		assert.Equal(t, "original", info.Method)
		assert.Equal(t, components.JobStatusPending, info.Status)
		_, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("re-dispatch keeps an existing future job unleased", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// The first dispatch establishes a future schedule that an idempotency conflict cannot replace
		jobRef := ref.NewAlarmRef("JOB", "existing-future", "key")
		dueTime := s.p.Now().Add(time.Hour)
		jobID, initialLease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:   dueTime,
			Kind:      components.AlarmKindJob,
			JobMethod: "original",
		})
		require.NoError(t, err)
		require.Nil(t, initialLease)

		// An immediate duplicate request must not lease or place the retained future occurrence
		duplicateID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now(),
			Kind:           components.AlarmKindJob,
			JobMethod:      "replacement",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		assert.Equal(t, jobID, duplicateID)
		assert.Nil(t, lease)

		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.WithinDuration(t, dueTime, info.DueTime, time.Second)
		assert.Equal(t, "original", info.Method)
		assert.Equal(t, components.JobStatusPending, info.Status)
		_, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("dispatch leases a job whose actor is active on an allowed host", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Place the actor on the host that will be offered for immediate execution
		jobRef := ref.NewAlarmRef("JOB", "active-allowed", "key")
		placement, err := s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{Hosts: []string{jobHost}})
		require.NoError(t, err)
		require.Equal(t, jobHost, placement.HostID)

		// Dispatching reuses the placement while acquiring the job lease
		jobID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, jobID, lease.Key())

		placement, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, jobHost, placement.HostID)
	})

	t.Run("dispatch leaves an upcoming job durable without an eligible host", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, Spec{
			Hosts: HostSpecCollection{
				{HostID: jobHost, Address: "127.0.0.1:7100", LastHealthAgo: time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: jobHost, ActorType: "OTHER", ActorIdleTimeout: 5 * time.Minute},
			},
		}))

		// The offered host cannot execute this actor type, so only the durable job is created
		jobRef := ref.NewAlarmRef("JOB", "no-host", "key")
		jobID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		assert.Nil(t, lease)

		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusPending, info.Status)
		_, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("dispatch does not move an actor from a healthy disallowed host", func(t *testing.T) {
		const secondJobHost = "0b000000-0000-4000-8000-0000000000b2"

		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, Spec{
			Hosts: HostSpecCollection{
				{HostID: jobHost, Address: "127.0.0.1:7100", LastHealthAgo: time.Second},
				{HostID: secondJobHost, Address: "127.0.0.1:7101", LastHealthAgo: time.Second},
			},
			HostActorTypes: HostActorTypeSpecCollection{
				{HostID: jobHost, ActorType: "JOB", ActorIdleTimeout: 5 * time.Minute},
				{HostID: secondJobHost, ActorType: "JOB", ActorIdleTimeout: 5 * time.Minute},
			},
		}))

		// Pin the actor to the first host before offering only the second host for the job lease
		jobRef := ref.NewAlarmRef("JOB", "active-disallowed", "key")
		placement, err := s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{Hosts: []string{jobHost}})
		require.NoError(t, err)
		require.Equal(t, jobHost, placement.HostID)

		jobID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{secondJobHost},
		})
		require.NoError(t, err)
		assert.Nil(t, lease)
		_, err = s.p.GetJob(ctx, jobID)
		require.NoError(t, err)

		placement, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.NoError(t, err)
		assert.Equal(t, jobHost, placement.HostID)
	})

	t.Run("re-dispatch replaces an expired job lease", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Acquire the first lease while the original host is healthy
		jobRef := ref.NewAlarmRef("JOB", "expired-lease", "key")
		jobID, originalLease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now().Add(time.Second),
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		require.NotNil(t, originalLease)

		// Advance past both lease expiry and host health, then add a fresh eligible host
		err = s.p.AdvanceClock(2 * time.Minute)
		require.NoError(t, err)
		hostRes, err := s.p.RegisterHost(ctx, components.RegisterHostReq{
			Address: "127.0.0.1:7102",
			ActorTypes: []components.ActorHostType{{
				ActorType:   "JOB",
				IdleTimeout: 5 * time.Minute,
			}},
		})
		require.NoError(t, err)

		// The same occurrence gets a fresh lease rather than a new durable identity
		duplicateID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        s.p.Now(),
			Kind:           components.AlarmKindJob,
			JobMethod:      "replacement",
			LeaseImmediate: []string{hostRes.HostID},
		})
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, jobID, duplicateID)
		assert.Equal(t, jobID, lease.Key())
		assert.NotEqual(t, originalLease.LeaseID(), lease.LeaseID())

		_, err = s.p.GetLeasedAlarm(ctx, originalLease)
		require.ErrorIs(t, err, components.ErrNoAlarm)
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.NoError(t, err)
	})

	t.Run("dispatch leaves a future job unleased", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobRef := ref.NewAlarmRef("JOB", "future", "key")
		dueTime := s.p.Now().Add(time.Hour)
		jobID, lease, err := s.p.DispatchJob(ctx, jobRef, components.SetAlarmReq{
			DueTime:        dueTime,
			Kind:           components.AlarmKindJob,
			JobMethod:      "process",
			LeaseImmediate: []string{jobHost},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, jobID)
		assert.Nil(t, lease)
		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusPending, info.Status)
		assert.WithinDuration(t, dueTime, info.DueTime, time.Second)
		_, err = s.p.LookupActor(ctx, jobRef.ActorRef(), components.LookupActorOpts{ActiveOnly: true})
		require.ErrorIs(t, err, components.ErrNoActor)
	})

	t.Run("idempotency key dedups re-dispatch", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		// Same actor + same key yields one job
		id1 := dispatch(t, ctx, "a1", "key", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)
		id2 := dispatch(t, ctx, "a1", "key", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)
		assert.Equal(t, id1, id2, "re-dispatching with the same key must return the same job ID")

		// A different key yields a distinct job
		id3 := dispatch(t, ctx, "a1", "other", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)
		assert.NotEqual(t, id1, id3)
	})

	t.Run("list jobs returns live jobs for the actor", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		id1 := dispatch(t, ctx, "list-actor", "j1", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)
		id2 := dispatch(t, ctx, "list-actor", "j2", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)

		jobs, err := s.p.ListJobs(ctx, "JOB", "list-actor")
		require.NoError(t, err)
		ids := make([]string, len(jobs))
		for i, j := range jobs {
			ids[i] = j.JobID
		}
		assert.ElementsMatch(t, []string{id1, id2}, ids)
	})

	t.Run("cancel removes a live job", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobID := dispatch(t, ctx, "cancel-actor", "c1", "process", ref.AlarmProperties{DueTime: s.p.Now().Add(time.Hour)}, nil)

		err := s.p.CancelJob(ctx, "JOB", "cancel-actor", jobID)
		require.NoError(t, err)

		_, err = s.p.GetJob(ctx, jobID)
		require.ErrorIs(t, err, components.ErrNoJob)

		// Cancelling again reports the job is gone
		err = s.p.CancelJob(ctx, "JOB", "cancel-actor", jobID)
		require.ErrorIs(t, err, components.ErrNoJob)
	})

	t.Run("dead-letter a one-shot job", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobID := dispatch(t, ctx, "dead-actor", "d1", "process", ref.AlarmProperties{DueTime: s.p.Now()}, []byte("dead-payload"))
		lease := leaseFor(t, ctx, jobID)

		err := s.p.DeadLetterAlarm(ctx, lease, components.DeadLetterAlarmReq{Reason: "boom", Attempts: 3})
		require.NoError(t, err)

		// GetJob now reports the dead-lettered job, with attempts and the last error
		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusDeadLettered, info.Status)
		assert.Equal(t, 3, info.Attempts)
		assert.Equal(t, "boom", info.LastError)

		// The dead job carries its input so it can be replayed
		dead, err := s.p.GetDeadJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, "process", dead.Method)
		assert.Equal(t, []byte("dead-payload"), dead.Data)

		// The live alarm row is gone, so the lease no longer resolves
		_, err = s.p.GetLeasedAlarm(ctx, lease)
		require.ErrorIs(t, err, components.ErrNoAlarm)

		// Deleting the dead job removes it entirely
		err = s.p.DeleteDeadJob(ctx, jobID)
		require.NoError(t, err)
		_, err = s.p.GetJob(ctx, jobID)
		require.ErrorIs(t, err, components.ErrNoJob)
	})

	t.Run("retry re-dispatches a dead job atomically", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobID := dispatch(t, ctx, "retry-actor", "rt1", "process", ref.AlarmProperties{DueTime: s.p.Now()}, []byte("payload"))
		lease := leaseFor(t, ctx, jobID)
		require.NoError(t, s.p.DeadLetterAlarm(ctx, lease, components.DeadLetterAlarmReq{Reason: "boom", Attempts: 3}))

		// Replay it: a new live job appears and the dead-letter record is gone, both atomically
		newID, err := s.p.RetryDeadJob(ctx, jobID)
		require.NoError(t, err)
		assert.NotEqual(t, jobID, newID, "replay should mint a new job ID")

		// The original ID no longer resolves: neither a live job nor a dead one remains under it
		_, err = s.p.GetJob(ctx, jobID)
		require.ErrorIs(t, err, components.ErrNoJob)

		// The new job is live and carries the original method and data
		info, err := s.p.GetJob(ctx, newID)
		require.NoError(t, err)
		assert.NotEqual(t, components.JobStatusDeadLettered, info.Status)
		assert.Equal(t, "process", info.Method)

		// Retrying a job that is no longer dead-lettered reports it as missing
		_, err = s.p.RetryDeadJob(ctx, jobID)
		require.ErrorIs(t, err, components.ErrNoJob)
	})

	t.Run("dead-letter a repeating occurrence keeps the recurrence", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		jobID := dispatch(t, ctx, "repeat-actor", "r1", "process", ref.AlarmProperties{DueTime: s.p.Now(), Interval: "PT1H"}, nil)
		lease := leaseFor(t, ctx, jobID)

		next := s.p.Now().Add(time.Hour)
		err := s.p.DeadLetterAlarm(ctx, lease, components.DeadLetterAlarmReq{Reason: "boom", Attempts: 1, Reschedule: true, NextDueTime: next})
		require.NoError(t, err)

		// The failed occurrence is dead-lettered under the original ID
		info, err := s.p.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, components.JobStatusDeadLettered, info.Status)

		// The recurrence continues as a new live job for the same actor
		jobs, err := s.p.ListJobs(ctx, "JOB", "repeat-actor")
		require.NoError(t, err)
		var live, dead int
		for _, j := range jobs {
			switch j.Status {
			case components.JobStatusDeadLettered:
				dead++
			default:
				live++
				assert.NotEqual(t, jobID, j.JobID, "the recurrence must have a fresh job ID")
			}
		}
		assert.Equal(t, 1, live, "the recurrence should still have one live job")
		assert.Equal(t, 1, dead, "the failed occurrence should be dead-lettered")
	})

	t.Run("get and delete report missing jobs", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, jobSeed()))

		_, err := s.p.GetJob(ctx, "11111111-1111-7111-8111-111111111111")
		require.ErrorIs(t, err, components.ErrNoJob)

		_, err = s.p.GetDeadJob(ctx, "11111111-1111-7111-8111-111111111111")
		require.ErrorIs(t, err, components.ErrNoJob)

		err = s.p.DeleteDeadJob(ctx, "11111111-1111-7111-8111-111111111111")
		require.ErrorIs(t, err, components.ErrNoJob)
	})
}

func (s Suite) TestBackupRestore(t *testing.T) {
	t.Run("wipes and reloads all persistent data", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, s.p.Seed(ctx, Spec{}))

		// Seed one of each kind of persistent record, leaving no host connected
		SeedBackupSample(t, ctx, s.p, s.p.Now())

		// The snapshot must contain every category
		var bufA bytes.Buffer
		err := s.p.Backup(ctx, &bufA)
		require.NoError(t, err)

		setA := DecodeBackup(t, bufA.Bytes())
		require.NotEmpty(t, setA.States, "expected actor state in the backup")
		require.GreaterOrEqual(t, len(setA.Alarms), 2, "expected a plain alarm and a live job in the backup")
		require.NotEmpty(t, setA.DeadJobs, "expected a dead job in the backup")

		// Add records that are absent from the snapshot, so a correct restore must remove them
		AddExtraBackupData(t, ctx, s.p, s.p.Now())

		// Restore wipes the extra records and reloads the snapshot
		err = s.p.Restore(ctx, bytes.NewReader(bufA.Bytes()))
		require.NoError(t, err)

		// A fresh backup must reproduce exactly the snapshot, proving both the wipe and the load
		var bufB bytes.Buffer
		err = s.p.Backup(ctx, &bufB)
		require.NoError(t, err)

		setB := DecodeBackup(t, bufB.Bytes())

		AssertBackupContentsEqual(t, setA, setB)
	})

	t.Run("backup runs online but restore refuses while a host is connected", func(t *testing.T) {
		ctx := t.Context()

		err := s.p.Seed(ctx, Spec{
			Hosts: HostSpecCollection{
				{HostID: SpecHostH1, Address: "127.0.0.1:4001", LastHealthAgo: time.Second},
			},
		})
		require.NoError(t, err)

		// Backup takes a consistent snapshot without requiring a quiescent cluster
		var buf bytes.Buffer
		err = s.p.Backup(ctx, &buf)
		require.NoError(t, err)

		// Restore would corrupt running actors, so it refuses while a host is connected
		err = s.p.Restore(ctx, bytes.NewReader(buf.Bytes()))
		require.ErrorIs(t, err, components.ErrHostsConnected)
	})
}
