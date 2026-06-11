//go:build integration

// Package crosshost holds scenarios that run multiple hosts sharing one backend, exercising cross-host actor placement and shared state on both runtimes
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

// Register the two-host scenarios across both runtimes
// On the local runtime only providers that coordinate across processes can back multiple hosts, while on the remote runtime coordination lives in the runtime, so every variant supports multiple hosts
func init() {
	for _, v := range provider.All() {
		if v.LocalMultiHost() {
			suite.Register(&invoke{kind: cluster.Local, variant: v})
			suite.Register(&alarmPlacement{kind: cluster.Local, variant: v})
		}

		suite.Register(&invoke{kind: cluster.Remote, variant: v})
		suite.Register(&alarmPlacement{kind: cluster.Remote, variant: v})
	}
}

// invoke brings up two hosts sharing one backend and verifies that an actor invoked from either host shares the same persisted state
type invoke struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *invoke) Name() string {
	return "crosshost/" + string(s.kind) + "/" + string(s.variant)
}

func (s *invoke) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors:  []frameworkhost.ActorReg{shared.CounterReg(time.Minute)},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *invoke) Run(t *testing.T) {
	const actorID = "counter-1"

	// Invoking from the first host places the actor and increments to 1
	env, err := s.cluster.Service(0).Invoke(t.Context(), shared.CounterActorType, actorID, "increment", nil)
	require.NoError(t, err)

	var out shared.CounterResult
	err = env.Decode(&out)
	require.NoError(t, err)
	require.Equal(t, int64(1), out.N)

	// Invoking the same actor from the second host hits the same placement and shared state, advancing the counter to 2
	env, err = s.cluster.Service(1).Invoke(t.Context(), shared.CounterActorType, actorID, "increment", nil)
	require.NoError(t, err)
	err = env.Decode(&out)
	require.NoError(t, err)
	require.Equal(t, int64(2), out.N)
}

// alarmPlacement verifies that an actor's alarm executes on the host the actor is placed on, not on some other host in the cluster
type alarmPlacement struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *alarmPlacement) Name() string {
	return "crosshost-alarm/" + string(s.kind) + "/" + string(s.variant)
}

func (s *alarmPlacement) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:               s.kind,
		Variant:            s.variant,
		Hosts:              2,
		Actors:             []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
		AlarmsPollInterval: 250 * time.Millisecond,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *alarmPlacement) Run(t *testing.T) {
	ctx := t.Context()

	// A unique actor ID per scenario keeps the process-global observation clean across variants and runtimes
	actorID := "xh-alarm-" + string(s.kind) + "-" + string(s.variant)

	// Label hosts so the probe can report where the actor and its alarm run
	for i := range s.cluster.Len() {
		shared.SetHostLabel(s.cluster.Service(i), "h"+strconv.Itoa(i))
	}

	// Place the actor on a definite host
	_, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodPing, nil)
	require.NoError(t, err)
	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	require.NotEmpty(t, placed)

	// Schedule a one-shot alarm and wait for this run's execution
	// The fire count is process-global, so measure against a baseline to stay correct even when the binary reruns the suite
	before := shared.ProbeObserver.AlarmCount(actorID)
	err = s.cluster.Service(0).SetAlarm(ctx, shared.ProbeActorType, actorID, "a", actor.AlarmProperties{
		DueTime: time.Now(),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.AlarmCount(actorID) > before
	}, 20*time.Second, 100*time.Millisecond, "the alarm should fire")

	// The alarm must have executed on the same host the actor is placed on, not elsewhere
	assert.Equal(t, placed, shared.ProbeObserver.LastAlarmHost(actorID), "an actor's alarm should execute on the host the actor is placed on")
}
