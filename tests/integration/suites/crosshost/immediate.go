//go:build integration

package crosshost

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	crossHostImmediatePollInterval = time.Minute
	crossHostImmediateDelay        = 500 * time.Millisecond
	crossHostImmediateTimeout      = 5 * time.Second
	crossHostImmediateTick         = 100 * time.Millisecond
	crossHostStartupSettle         = 750 * time.Millisecond
)

// Register plural-host immediate leasing for the remote runtime and each distinct provider implementation
func init() {
	for _, variant := range []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory} {
		suite.Register(&immediatePlacement{variant: variant})
	}
}

// immediatePlacement proves the runtime offers every eligible connection and schedules the lease on the actor's owner
type immediatePlacement struct {
	variant provider.Variant
	cluster *cluster.Cluster
}

func (s *immediatePlacement) Name() string {
	return "crosshost-immediate/remote/" + string(s.variant)
}

func (s *immediatePlacement) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    cluster.Remote,
		Variant: s.variant,
		Hosts:   2,
		Actors: []frameworkhost.ActorReg{
			shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute)),
		},
		AlarmsPollInterval: crossHostImmediatePollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *immediatePlacement) Run(t *testing.T) {
	ctx := t.Context()
	for i := range s.cluster.Len() {
		shared.SetHostLabel(s.cluster.Service(i), "h"+strconv.Itoa(i))
	}

	// Let the one-off startup fetch finish so only the immediate lease can satisfy the assertions
	time.Sleep(crossHostStartupSettle)

	dispatcherForOtherHost := func(t *testing.T, placed string) int {
		t.Helper()
		if placed == "h0" {
			return 1
		}
		require.Equal(t, "h1", placed)
		return 0
	}

	t.Run("alarm executes on its existing placement", func(t *testing.T) {
		actorID := "remote-immediate-alarm-" + string(s.variant)
		_, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodPing, nil)
		require.NoError(t, err)
		placed := shared.ProbeObserver.LastInvokeHost(actorID)
		require.NotEmpty(t, placed)
		dispatcher := dispatcherForOtherHost(t, placed)

		// Scheduling from the other host must still pre-lease the alarm for its current owner
		before := shared.ProbeObserver.AlarmCount(actorID)
		err = s.cluster.Service(dispatcher).SetAlarm(ctx, shared.ProbeActorType, actorID, "wake", actor.AlarmProperties{
			DueTime: time.Now().Add(crossHostImmediateDelay),
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) > before
		}, crossHostImmediateTimeout, crossHostImmediateTick, "the pre-leased alarm should execute before the periodic poll")
		assert.Equal(t, placed, shared.ProbeObserver.LastAlarmHost(actorID))
	})

	t.Run("job executes on its existing placement", func(t *testing.T) {
		actorID := "remote-immediate-job-" + string(s.variant)
		_, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodPing, nil)
		require.NoError(t, err)
		placed := shared.ProbeObserver.LastInvokeHost(actorID)
		require.NotEmpty(t, placed)
		dispatcher := dispatcherForOtherHost(t, placed)

		// Scheduling from the other host must still pre-lease the job for its current owner
		before := shared.ProbeObserver.JobCount(actorID)
		_, err = s.cluster.Service(dispatcher).Dispatch(ctx, shared.ProbeActorType, actorID, "process", nil, actor.WithJobDelay(crossHostImmediateDelay))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return shared.ProbeObserver.JobCount(actorID) > before
		}, crossHostImmediateTimeout, crossHostImmediateTick, "the pre-leased job should execute before the periodic poll")
		assert.Equal(t, placed, shared.ProbeObserver.LastJobHost(actorID))
	})
}
