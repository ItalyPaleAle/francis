//go:build integration

package alarms

import (
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
	immediateAlarmPollInterval = time.Minute
	startupFetchSettle         = 750 * time.Millisecond
	immediateAlarmDelay        = 500 * time.Millisecond
	immediateAlarmTimeout      = 5 * time.Second
	immediateAlarmTick         = 100 * time.Millisecond
	futureAlarmDelay           = 4 * time.Second
	futureAlarmObservation     = futureAlarmDelay + time.Second
)

var immediateAlarmVariants = []provider.Variant{
	provider.SQLite,
	provider.Postgres,
	provider.StandaloneMemory,
}

// Register immediate alarm leasing once for each distinct provider implementation
func init() {
	for _, variant := range immediateAlarmVariants {
		suite.Register(&immediateAlarms{variant: variant})
	}
}

// immediateAlarms separates immediate leasing from the periodic fetcher with a poll interval much longer than every assertion
type immediateAlarms struct {
	variant provider.Variant
	cluster *cluster.Cluster
}

func (s *immediateAlarms) Name() string {
	return "alarms/immediate/remote/" + string(s.variant)
}

func (s *immediateAlarms) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    cluster.Remote,
		Variant: s.variant,
		Hosts:   1,
		Actors: []frameworkhost.ActorReg{
			shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute)),
		},
		AlarmsPollInterval: immediateAlarmPollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *immediateAlarms) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// Let the runtime's one-off startup fetch finish so it cannot satisfy either assertion
	time.Sleep(startupFetchSettle)

	// An alarm inside fetch-ahead must execute without waiting for the one-minute periodic poll
	t.Run("inside fetch-ahead executes immediately", func(t *testing.T) {
		actorID := "immediate-" + string(s.variant)
		alarmName := "immediate"
		alarmData := "pre-leased"
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, alarmName, actor.AlarmProperties{
			DueTime: time.Now().Add(immediateAlarmDelay),
			Data:    alarmData,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) > 0
		}, immediateAlarmTimeout, immediateAlarmTick, "an alarm inside fetch-ahead should execute before the periodic poll")

		fires := shared.ProbeObserver.AlarmFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, alarmName, fires[0].Name)
		assert.Equal(t, alarmData, fires[0].Data)
	})

	// An alarm outside fetch-ahead must stay on the storage-only fast path until a later poll fetches it
	t.Run("outside fetch-ahead is not enqueued", func(t *testing.T) {
		actorID := "future-" + string(s.variant)
		err := svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "future", actor.AlarmProperties{
			DueTime: time.Now().Add(futureAlarmDelay),
		})
		require.NoError(t, err)

		neverFired := assert.Never(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) > 0
		}, futureAlarmObservation, immediateAlarmTick, "an alarm outside fetch-ahead should not execute before the periodic poll")
		if !neverFired {
			return
		}

		err = svc.DeleteAlarm(ctx, shared.ProbeActorType, actorID, "future")
		require.NoError(t, err)
	})
}
