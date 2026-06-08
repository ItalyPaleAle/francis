//go:build integration

// Package state holds a single-host scenario that exercises actor state and invocation against every supported provider variant, on both the local and remote runtimes
package state

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

// Register the single-host scenario for every provider variant on both runtimes
// Every provider can back a single host, so this gives baseline coverage of state read and write and invocation across all of them
func init() {
	for _, v := range provider.All() {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&roundTrip{kind: k, variant: v})
		}
	}
}

// roundTrip runs one host on a given runtime and provider and checks a state round-trip and an actor invocation
type roundTrip struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *roundTrip) Name() string {
	return "state/" + string(s.kind) + "/" + string(s.variant)
}

func (s *roundTrip) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.CounterReg(time.Minute)},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *roundTrip) Run(t *testing.T) {
	svc := s.cluster.Service(0)

	// Direct state round-trip through the service
	require.NoError(t, svc.SetState(t.Context(), shared.CounterActorType, "x", shared.CounterState{N: 7}, nil))

	var got shared.CounterState
	require.NoError(t, svc.GetState(t.Context(), shared.CounterActorType, "x", &got))
	require.Equal(t, int64(7), got.N)

	// Actor invocation persists and returns the incremented value, twice
	var want int64
	for want = 1; want <= 2; want++ {
		env, err := svc.Invoke(t.Context(), shared.CounterActorType, "y", "increment", nil)
		require.NoError(t, err)
		var out shared.CounterResult
		require.NoError(t, env.Decode(&out))
		require.Equal(t, want, out.N)
	}
}
