//go:build integration

// Package alarminvoke exercises the interaction between alarm execution and actor invocation: that the two share the actor's turn-based lock and never overlap, and that an actor can schedule an alarm on itself from inside an invocation
package alarminvoke

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
	// pollInterval keeps alarm polling fast so the scenarios fire promptly
	pollInterval = 250 * time.Millisecond

	eventuallyTimeout = 20 * time.Second
	eventuallyTick    = 100 * time.Millisecond
)

// variants is the representative set: SQLite and Postgres each have their own alarm SQL, and the standalone providers share one in-memory path that StandaloneMemory stands in for
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the alarm-invocation scenario across the representative variants on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&alarmInvoke{kind: k, variant: v})
		}
	}
}

// alarmInvoke runs one host with fast alarm polling and exercises how alarms and invocations interleave on a single actor
type alarmInvoke struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *alarmInvoke) Name() string {
	return "alarminvoke/" + string(s.kind) + "/" + string(s.variant)
}

func (s *alarmInvoke) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors: []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{
			IdleTimeout: time.Minute,
		})},
		AlarmsPollInterval: pollInterval,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *alarmInvoke) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// An alarm and an invocation on the same actor share its turn, so they must never run at the same time
	t.Run("alarm and invocation do not overlap", func(t *testing.T) {
		const actorID = "excl-1"

		// Make this actor's alarm execution occupy the hold gauge too, so any overlap with a holding invocation would be visible
		shared.ProbeObserver.SetAlarmHold(actorID)

		// A repeating alarm keeps firing throughout the invocation storm, maximizing the chance of an overlap if one were possible
		require.NoError(t, svc.SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
			DueTime:  time.Now(),
			Interval: shared.ISOInterval(200 * time.Millisecond),
		}))

		// Drive a sustained stream of holding invocations for long enough to span several alarm firings
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			_, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodHold, nil)
			require.NoError(t, err)
		}

		// Stop the alarm and confirm it actually fired during the storm, so the no-overlap result is meaningful
		require.NoError(t, svc.DeleteAlarm(ctx, shared.ProbeActorType, actorID, "a"))
		assert.GreaterOrEqual(t, shared.ProbeObserver.AlarmCount(actorID), 1, "the alarm should have fired during the invocation storm")
		assert.Equal(t, 1, shared.ProbeObserver.MaxHoldConcurrency(actorID), "an alarm and an invocation must not run concurrently on one actor")
	})

	// An actor can schedule an alarm on itself from inside an invocation, and that alarm then fires
	t.Run("actor arms its own alarm during invocation", func(t *testing.T) {
		const actorID = "arm-1"

		_, err := svc.Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodArmAlarm, nil)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return shared.ProbeObserver.AlarmCount(actorID) >= 1
		}, eventuallyTimeout, eventuallyTick, "the self-scheduled alarm should fire")

		fires := shared.ProbeObserver.AlarmFires(actorID)
		require.NotEmpty(t, fires)
		assert.Equal(t, shared.ProbeSelfAlarmName, fires[0].Name, "the fired alarm should be the one the actor scheduled")
	})
}
