package local

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/testutil"
	"github.com/italypaleale/francis/protocol"
)

func TestLocalDrainStateMachine(t *testing.T) {
	provider := components_mocks.NewMockActorProvider(t)
	h := &Host{
		actorProvider:          provider,
		log:                    slog.New(slog.DiscardHandler),
		hostID:                 "host-1",
		providerRequestTimeout: 5 * time.Second,
	}

	// A host that is not running cannot be drained
	_, perr := h.localDrain(t.Context(), protocol.HostDrainRequest{})
	require.NotNil(t, perr)
	assert.Equal(t, protocol.ErrCodeHostUnavailable, perr.Code)

	// The draining flag is persisted once, before the drain is acknowledged
	provider.
		On("UpdateActorHost", mock.MatchedBy(testutil.MatchContextInterface), "host-1", components.UpdateActorHostReq{Draining: true}).
		Return(nil).
		Once()

	var canceled bool
	require.True(t, h.adminDrain.Start())
	h.adminDrain.SetStop(func() { canceled = true })

	out, perr := h.localDrain(t.Context(), protocol.HostDrainRequest{TimeoutMs: 1500, Reason: "test"})
	require.Nil(t, perr)
	assert.False(t, out.AlreadyDraining)
	assert.False(t, canceled, "accepting a drain preserves the connection until the reply is written")

	req, err := protocol.NewRequest(protocol.KindPeerHostDrain, protocol.HostDrainRequest{})
	require.NoError(t, err)

	duplicate := replyWith(req, protocol.KindPeerHostDrainResponse, protocol.HostDrainResponse{AlreadyDraining: true})
	h.managementResponseWritten(req, duplicate)
	assert.False(t, canceled, "a duplicate acknowledgement cannot start the first drain")
	h.managementResponseWritten(req, replyWith(req, protocol.KindPeerHostDrainResponse, out))
	assert.True(t, canceled, "writing the acknowledgement starts teardown")
	assert.True(t, h.draining.Load())
	assert.True(t, h.adminDrain.Accepted())

	// A second drain has no further effect, and the shutdown path does not persist the flag again
	out, perr = h.localDrain(t.Context(), protocol.HostDrainRequest{})
	require.Nil(t, perr)
	assert.True(t, out.AlreadyDraining)
	h.persistDraining()

	// The teardown is bounded by the drain's timeout
	_, timeout := h.adminDrain.BeginStopping()
	assert.Equal(t, 1500*time.Millisecond, timeout)

	// A drained host cannot run again
	h.adminDrain.Stopped()
	assert.False(t, h.adminDrain.Start())
}

func TestLocalDrainWhileShuttingDown(t *testing.T) {
	h := &Host{log: slog.New(slog.DiscardHandler)}
	require.True(t, h.adminDrain.Start())
	h.adminDrain.BeginStopping()
	h.draining.Store(true)

	// A host that is already shutting down reports the drain as already in progress, and the shutdown stays unbounded
	out, perr := h.localDrain(t.Context(), protocol.HostDrainRequest{TimeoutMs: 100})
	require.Nil(t, perr)
	assert.True(t, out.AlreadyDraining)
	assert.False(t, h.adminDrain.Accepted())
	_, timeout := h.adminDrain.BeginStopping()
	assert.Zero(t, timeout)
}

// fakeWorkflow is a built-in actor stand-in that reports a management definition
type fakeWorkflow struct {
	smokeActor
}

func (fakeWorkflow) ManagementDefinition() (string, int, string) {
	return "wf", 2, "fp"
}

// TestHostLocalPeerManagement exercises the management requests between two local hosts through the peer server
func TestHostLocalPeerManagement(t *testing.T) {
	// Both hosts share one SQLite database so they see the same placement table
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	newSharedHost := func(register bool) *Host {
		h, err := NewHost(
			WithAddress(localFreeUDPAddr(t)),
			WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
			WithRuntimePSKs(localTestRuntimePSK),
			WithLogger(slog.New(slog.DiscardHandler)))
		require.NoError(t, err)
		if register {
			err = h.RegisterActor("S", func(actorID string, service *actor.Service) actor.Actor {
				return smokeActor{}
			})
			require.NoError(t, err)
		}
		return h
	}

	// Host B owns actor type "S", while host A registers nothing and reaches B's actors through the peer transport
	hostB := newSharedHost(true)
	hostB.core.RecordManagementDefinition(fakeWorkflow{})
	hostA := newSharedHost(false)

	errB := make(chan error, 1)
	go func() {
		errB <- hostB.Run(t.Context())
	}()
	select {
	case <-hostB.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host B did not register")
	}
	runLocalHost(t, hostA)

	// Activate two actors on host B
	for _, id := range []string{"x", "y"} {
		_, err := hostA.Service().Invoke(t.Context(), "S", id, "echo", "hi")
		require.NoError(t, err)
	}
	bID := hostB.HostID()
	bAddr := hostB.address

	t.Run("snapshot of a peer", func(t *testing.T) {
		res, err := hostA.managementSnapshot(t.Context(), bID, bAddr, protocol.HostSnapshotRequest{})
		require.NoError(t, err)
		assert.Equal(t, bID, res.HostID)
		assert.Equal(t, 2, res.ActiveCount)
		require.Len(t, res.Activations, 2)
		assert.Equal(t, "x", res.Activations[0].ActorID)
		assert.Equal(t, "y", res.Activations[1].ActorID)
		assert.NotZero(t, res.ObservedAtUnixMs)
		assert.False(t, res.Draining)
		assert.Equal(t, []protocol.WorkflowDefinitionInfo{{Name: "wf", Version: 2, Fingerprint: "fp"}}, res.Workflows)

		// Skipping activations still reports the count and the workflows
		res, err = hostA.managementSnapshot(t.Context(), bID, bAddr, protocol.HostSnapshotRequest{SkipActivations: true})
		require.NoError(t, err)
		assert.Equal(t, 2, res.ActiveCount)
		assert.Empty(t, res.Activations)
		assert.Len(t, res.Workflows, 1)
	})

	t.Run("snapshot of self is served in-process", func(t *testing.T) {
		res, err := hostA.managementSnapshot(t.Context(), hostA.HostID(), "unreachable.invalid:1", protocol.HostSnapshotRequest{})
		require.NoError(t, err)
		assert.Equal(t, hostA.HostID(), res.HostID)
		assert.Zero(t, res.ActiveCount)
	})

	t.Run("wrong host at the address is rejected", func(t *testing.T) {
		_, err := hostA.managementSnapshot(t.Context(), "not-"+bID, bAddr, protocol.HostSnapshotRequest{})
		require.Error(t, err)
		var perr *protocol.Error
		require.ErrorAs(t, err, &perr)
		assert.Equal(t, protocol.ErrCodeHostMismatch, perr.Code)
	})

	t.Run("deactivate on a peer", func(t *testing.T) {
		notActive, err := hostA.managementDeactivate(t.Context(), bID, bAddr, "S", "x")
		require.NoError(t, err)
		assert.False(t, notActive)

		notActive, err = hostA.managementDeactivate(t.Context(), bID, bAddr, "S", "x")
		require.NoError(t, err)
		assert.True(t, notActive)

		res, err := hostA.managementSnapshot(t.Context(), bID, bAddr, protocol.HostSnapshotRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, res.ActiveCount)
	})

	t.Run("deactivate on self", func(t *testing.T) {
		notActive, err := hostA.managementDeactivate(t.Context(), hostA.HostID(), "unreachable.invalid:1", "S", "missing")
		require.NoError(t, err)
		assert.True(t, notActive)
	})

	t.Run("drain a peer", func(t *testing.T) {
		out, err := hostA.managementDrain(t.Context(), bID, bAddr, protocol.HostDrainRequest{TimeoutMs: 5000, Reason: "test"})
		require.NoError(t, err)
		assert.False(t, out.AlreadyDraining)

		// Host B shuts down and reports the drain
		select {
		case err = <-errB:
			require.ErrorIs(t, err, host.ErrAdministrativeDrain)
		case <-time.After(15 * time.Second):
			t.Fatal("host B did not shut down after the drain")
		}
		assert.Zero(t, hostB.core.Actors.Len())

		// A drained host cannot run again
		err = hostB.Run(t.Context())
		require.ErrorIs(t, err, host.ErrAdministrativeDrain)

		// Host B is gone, so it no longer answers management requests
		_, err = hostA.managementSnapshot(t.Context(), bID, bAddr, protocol.HostSnapshotRequest{})
		require.Error(t, err)
	})
}

func TestHandleManagementUnknownKind(t *testing.T) {
	h := &Host{}
	req := protocol.NewEnvelope("peer.mgmt.unknown", nil)
	resp := h.handleManagement(context.Background(), req)
	perr, isErr := resp.AsError()
	require.True(t, isErr)
	assert.Equal(t, protocol.ErrCodeBadRequest, perr.Code)
}

type drainRegressionJob struct {
	run func(context.Context) error
}

func (j drainRegressionJob) Job(ctx context.Context, _ string, _ actor.Envelope) error {
	return j.run(ctx)
}

func TestLocalDrainReleasesRunningJobs(t *testing.T) {
	for _, signal := range []string{"cancellation", "halting"} {
		t.Run(signal, func(t *testing.T) {
			started := make(chan struct{})

			h, err := NewHost(
				WithAddress(localFreeUDPAddr(t)),
				WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: filepath.Join(t.TempDir(), "drain.db")}),
				WithRuntimePSKs(localTestRuntimePSK),
				WithShutdownGracePeriod(time.Hour),
				WithLogger(slog.New(slog.DiscardHandler)),
			)
			require.NoError(t, err)

			err = h.RegisterActor("drain-job", func(string, *actor.Service) actor.Actor {
				return drainRegressionJob{run: func(ctx context.Context) error {
					close(started)
					stop := ctx.Done()
					if signal == "halting" {
						stop = actor.HaltingFromContext(ctx)
					}
					select {
					case <-stop:
						return ctx.Err()
					case <-t.Context().Done():
						return context.Canceled
					}
				}}
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			runErr := make(chan error, 1)
			go func() {
				runErr <- h.Run(ctx)
			}()

			select {
			case <-h.Ready():
			case <-time.After(15 * time.Second):
				t.Fatal("host did not start")
			}

			_, _, err = h.Dispatch(t.Context(), "drain-job", "one", "wait", nil, actor.JobProperties{})
			require.NoError(t, err)

			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("job did not start")
			}

			// The host must begin halting before waiting for this job, and its timeout overrides the hour-long ordinary grace period
			_, err = h.managementDrain(t.Context(), h.HostID(), h.address, protocol.HostDrainRequest{TimeoutMs: 1000})
			require.NoError(t, err)

			select {
			case err = <-runErr:
				require.ErrorIs(t, err, host.ErrAdministrativeDrain)
			case <-time.After(5 * time.Second):
				t.Fatal("bounded drain did not release the job")
			}

			assert.Empty(t, h.HostID())
			assert.Zero(t, h.core.Actors.Len())
		})
	}
}
