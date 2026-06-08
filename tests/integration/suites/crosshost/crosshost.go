//go:build integration

// Package crosshost holds scenarios that run multiple hosts sharing one backend, exercising cross-host actor placement and shared state on both runtimes
package crosshost

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// Register the two-host scenario across both runtimes
// On the local runtime only providers that coordinate across processes can back multiple hosts, while on the remote runtime coordination lives in the runtime, so every variant supports multiple hosts
func init() {
	for _, v := range provider.All() {
		if v.LocalMultiHost() {
			suite.Register(&invoke{kind: cluster.Local, variant: v})
		}
		suite.Register(&invoke{kind: cluster.Remote, variant: v})
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
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	// Invoking the same actor from the second host hits the same placement and shared state, advancing the counter to 2
	env, err = s.cluster.Service(1).Invoke(t.Context(), shared.CounterActorType, actorID, "increment", nil)
	require.NoError(t, err)
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(2), out.N)
}
