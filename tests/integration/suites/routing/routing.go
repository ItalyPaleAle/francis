//go:build integration

// Package routing verifies how invocation routing fails when no placement can be found
package routing

import (
	"testing"
	"time"

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

// variants is the representative set covering the distinct placement implementations
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the routing scenario for every representative variant on both runtimes
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&routing{kind: k, variant: v})
		}
	}
}

// routing runs one host that registers only the probe actor and checks how invoking an unregistered type fails
type routing struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *routing) Name() string {
	return "routing/" + string(s.kind) + "/" + string(s.variant)
}

func (s *routing) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *routing) Run(t *testing.T) {
	// Invoking a type no host registered has nowhere to be placed
	// The routing layer does not distinguish an unknown type from a known type with no available host: both surface as ErrNoHost
	_, err := s.cluster.Service(0).Invoke(t.Context(), "unregistered-type", "x", shared.ProbeMethodPing, nil)
	require.ErrorIs(t, err, actor.ErrNoHost, "invoking an unregistered actor type should report no available host")
}
