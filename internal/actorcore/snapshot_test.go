package actorcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// fakeDefinition is a built-in actor stand-in that reports a management definition
type fakeDefinition struct {
	name        string
	version     int
	fingerprint string
}

func (f fakeDefinition) ManagementDefinition() (string, int, string) {
	return f.name, f.version, f.fingerprint
}

func newSnapshotManager(t *testing.T, clk *clocktesting.FakeClock) *Manager {
	t.Helper()

	noopFactory := func(string, *actor.Service) actor.Actor { return struct{}{} }
	m := NewManager(Options{
		Clock:               clk,
		RemoveActor:         func(context.Context, ref.ActorRef) error { return nil },
		ShutdownGracePeriod: time.Hour,
	})
	require.NoError(t, m.RegisterActor("a", noopFactory, RegisterActorOptions{CapacityGroup: "g1", CapacityGroupLimit: 3}))
	require.NoError(t, m.RegisterActor("a.b", noopFactory, RegisterActorOptions{CapacityGroup: "g1", CapacityGroupLimit: 3}))
	require.NoError(t, m.RegisterActor("c", noopFactory, RegisterActorOptions{CapacityGroup: "g0", CapacityGroupLimit: 1}))
	require.NoError(t, m.RegisterActor("plain", noopFactory, RegisterActorOptions{}))
	m.Start()
	t.Cleanup(m.Close)

	return m
}

func activate(t *testing.T, m *Manager, actorType string, actorID string) *ActiveActor {
	t.Helper()
	act, err := m.getOrCreateActor(t.Context(), ref.NewActorRef(actorType, actorID))
	require.NoError(t, err)
	return act
}

func snapshotKeys(res protocol.HostSnapshotResponse) []string {
	keys := make([]string, len(res.Activations))
	for i, a := range res.Activations {
		keys[i] = a.ActorType + "/" + a.ActorID
	}
	return keys
}

func TestSnapshot(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clk := clocktesting.NewFakeClock(start)
	m := newSnapshotManager(t, clk)

	// Activate actors at different times, so activation times are distinguishable
	activate(t, m, "a", "2")
	activate(t, m, "a", "1")
	clk.Step(time.Second)
	activate(t, m, "a.b", "1")
	activate(t, m, "plain", "x")
	halted := activate(t, m, "plain", "y")
	require.NoError(t, halted.Halt(false))

	t.Run("full page ordered by type then ID", func(t *testing.T) {
		res := m.Snapshot(protocol.HostSnapshotRequest{})
		assert.Equal(t, 5, res.ActiveCount)
		// "a" sorts before "a.b" even though the joined key "a.b/1" sorts before "a/1"
		assert.Equal(t, []string{"a/1", "a/2", "a.b/1", "plain/x", "plain/y"}, snapshotKeys(res))
		assert.Empty(t, res.Next)

		assert.Equal(t, start.UnixMilli(), res.Activations[0].ActivatedAtUnixMs)
		assert.Equal(t, start.Add(time.Second).UnixMilli(), res.Activations[2].ActivatedAtUnixMs)
		assert.False(t, res.Activations[3].Deactivating)
		assert.True(t, res.Activations[4].Deactivating)

		// The caller fills in these fields
		assert.Empty(t, res.HostID)
		assert.Zero(t, res.ObservedAtUnixMs)
		assert.Empty(t, res.Workflows)
	})

	t.Run("pagination with cursor", func(t *testing.T) {
		var (
			all   []string
			after string
			pages int
		)
		for {
			res := m.Snapshot(protocol.HostSnapshotRequest{Limit: 2, After: after})
			assert.Equal(t, 5, res.ActiveCount, "the count covers every page")
			all = append(all, snapshotKeys(res)...)
			pages++
			if res.Next == "" {
				break
			}
			after = res.Next
		}
		assert.Equal(t, 3, pages)
		assert.Equal(t, []string{"a/1", "a/2", "a.b/1", "plain/x", "plain/y"}, all)
	})

	t.Run("exact page has no cursor", func(t *testing.T) {
		res := m.Snapshot(protocol.HostSnapshotRequest{Limit: 5})
		assert.Len(t, res.Activations, 5)
		assert.Empty(t, res.Next)
	})

	t.Run("actor type filter", func(t *testing.T) {
		res := m.Snapshot(protocol.HostSnapshotRequest{ActorType: "a"})
		assert.Equal(t, 2, res.ActiveCount)
		assert.Equal(t, []string{"a/1", "a/2"}, snapshotKeys(res))

		res = m.Snapshot(protocol.HostSnapshotRequest{ActorType: "missing"})
		assert.Zero(t, res.ActiveCount)
		assert.Empty(t, res.Activations)
	})

	t.Run("skip activations", func(t *testing.T) {
		res := m.Snapshot(protocol.HostSnapshotRequest{SkipActivations: true})
		assert.Equal(t, 5, res.ActiveCount)
		assert.Empty(t, res.Activations)
		assert.Empty(t, res.Next)
		assert.Len(t, res.CapacityGroups, 2)
	})

	t.Run("capacity groups", func(t *testing.T) {
		release, admitted := m.TryAcquireCapacity("a.b")
		require.True(t, admitted)
		defer release()

		res := m.Snapshot(protocol.HostSnapshotRequest{SkipActivations: true})
		assert.Equal(t, []protocol.CapacityGroupInfo{
			{Name: "g0", Limit: 1, InUse: 0, ActorTypes: []string{"c"}},
			{Name: "g1", Limit: 3, InUse: 1, ActorTypes: []string{"a", "a.b"}},
		}, res.CapacityGroups)
	})
}

func TestSnapshotNoCapacityGroups(t *testing.T) {
	m := NewManager(Options{})
	assert.Nil(t, m.CapacityGroups())
	res := m.Snapshot(protocol.HostSnapshotRequest{})
	assert.Zero(t, res.ActiveCount)
	assert.Nil(t, res.CapacityGroups)
}

func TestWorkflowDefinitions(t *testing.T) {
	m := NewManager(Options{})
	assert.Nil(t, m.WorkflowDefinitions())

	// Values that do not implement the interface are ignored
	m.RecordManagementDefinition(struct{}{})
	m.RecordManagementDefinition(fakeDefinition{name: "b", version: 1, fingerprint: "fb"})
	m.RecordManagementDefinition(fakeDefinition{name: "a", version: 2, fingerprint: "fa2"})
	m.RecordManagementDefinition(fakeDefinition{name: "a", version: 1, fingerprint: "fa1"})

	assert.Equal(t, []protocol.WorkflowDefinitionInfo{
		{Name: "a", Version: 1, Fingerprint: "fa1"},
		{Name: "a", Version: 2, Fingerprint: "fa2"},
		{Name: "b", Version: 1, Fingerprint: "fb"},
	}, m.WorkflowDefinitions())
}

func TestActiveActorAccessors(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clocktesting.NewFakeClock(now)
	m := newSnapshotManager(t, clk)

	act := activate(t, m, "plain", "1")
	assert.Equal(t, now, act.ActivatedAt())
	assert.False(t, act.Deactivating())

	require.NoError(t, act.Halt(false))
	assert.True(t, act.Deactivating())
}

func TestHaltAllWithin(t *testing.T) {
	noopFactory := func(string, *actor.Service) actor.Actor { return struct{}{} }
	newManager := func(t *testing.T) *Manager {
		m := NewManager(Options{
			RemoveActor:         func(context.Context, ref.ActorRef) error { return nil },
			ShutdownGracePeriod: time.Hour,
		})
		require.NoError(t, m.RegisterActor("T", noopFactory, RegisterActorOptions{}))
		m.Start()
		t.Cleanup(m.Close)
		return m
	}

	t.Run("completes within the timeout", func(t *testing.T) {
		m := newManager(t)
		activate(t, m, "T", "1")
		activate(t, m, "T", "2")

		forced, err := m.HaltAllWithin(10 * time.Second)
		require.NoError(t, err)
		assert.Empty(t, forced)
		assert.Zero(t, m.Actors.Len())
	})

	t.Run("reports and cancels actors still busy at the timeout", func(t *testing.T) {
		m := newManager(t)
		activate(t, m, "T", "idle")

		// Start a call that only returns once its context is canceled, which the one-hour grace period would otherwise delay
		started := make(chan struct{})
		callErr := make(chan error, 1)
		go func() {
			_, err := m.LockAndInvoke(t.Context(), ref.NewActorRef("T", "busy"), func(ctx context.Context, _ *ActiveActor) (any, error) {
				close(started)
				<-ctx.Done()
				return nil, context.Cause(ctx)
			})
			callErr <- err
		}()
		<-started

		forced, err := m.HaltAllWithin(100 * time.Millisecond)
		require.NoError(t, err)
		assert.Equal(t, []string{"T/busy"}, forced)

		// The busy call is cut short instead of running out the grace period
		select {
		case err = <-callErr:
			require.ErrorIs(t, err, actor.ErrActorHalted)
		case <-time.After(5 * time.Second):
			t.Fatal("in-flight call was not canceled")
		}

		// The actor finishes halting in the background
		assert.Eventually(t, func() bool {
			return m.Actors.Len() == 0
		}, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("a call cut short is reported as canceled even when the method returns no error", func(t *testing.T) {
		m := newManager(t)

		// The method honors cancellation by returning nothing, which must not be reported to the caller as a success
		started := make(chan struct{})
		callErr := make(chan error, 1)
		go func() {
			res, err := m.LockAndInvoke(t.Context(), ref.NewActorRef("T", "quiet"), func(ctx context.Context, _ *ActiveActor) (any, error) {
				close(started)
				<-ctx.Done()
				return "partial", nil
			})
			assert.Nil(t, res, "a call cut short must not return a result")
			callErr <- err
		}()
		<-started

		_, err := m.HaltAllWithin(100 * time.Millisecond)
		require.NoError(t, err)

		// It is not reported as ErrActorHalted, which would make a caller retry work that already ran
		select {
		case err = <-callErr:
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, actor.ErrActorHalted)
		case <-time.After(5 * time.Second):
			t.Fatal("in-flight call was not canceled")
		}
	})

	t.Run("no timeout waits for completion", func(t *testing.T) {
		m := newManager(t)
		activate(t, m, "T", "1")

		forced, err := m.HaltAllWithin(0)
		require.NoError(t, err)
		assert.Empty(t, forced)
		assert.Zero(t, m.Actors.Len())
	})
}
