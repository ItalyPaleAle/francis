//go:build integration

// Package bootstrap exercises how remote hosts authenticate when they join the runtime
//
// The rest of the remote-topology suites bootstrap hosts with the shared pre-shared key; this one drives the JWT bootstrap path end to end, where the runtime validates a signed token against an inline JWKS before issuing the host its workload certificate
package bootstrap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// variants is the representative set: JWT bootstrap is a runtime-side authentication path independent of the provider, so one persistent and the in-memory standalone variant are enough to cover it
var variants = []provider.Variant{provider.SQLite, provider.StandaloneMemory}

// Register the JWT bootstrap scenario for the representative variants
// It is only meaningful on the remote topology, where hosts bootstrap against a runtime; local hosts self-issue from the runtime PSK and never present a bootstrap credential
func init() {
	for _, v := range variants {
		suite.Register(&jwtBootstrap{variant: v})
	}
}

// jwtBootstrap brings up a remote cluster whose hosts authenticate with a JWT, and verifies they register and can serve actors
type jwtBootstrap struct {
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *jwtBootstrap) Name() string {
	return "bootstrap-jwt/remote/" + string(s.variant)
}

func (s *jwtBootstrap) Setup(t *testing.T) []framework.Option {
	// A fresh JWT authority mints the signing key and JWKS; the runtime validates against the JWKS and each host presents a token the cluster signs for it
	jwtBoot, err := clustersecret.NewJWTBootstrap()
	require.NoError(t, err, "failed to build JWT bootstrap authority")

	s.cluster = cluster.New(t, cluster.Options{
		Kind:         cluster.Remote,
		Variant:      s.variant,
		Hosts:        2,
		Actors:       []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
		BootstrapJWT: jwtBoot,
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *jwtBootstrap) Run(t *testing.T) {
	ctx := t.Context()

	// The framework gates Setup on every host reaching readiness, which on the remote topology means each host completed the JWT bootstrap handshake and registered with the runtime
	// Driving an actor through each host confirms the JWT-bootstrapped hosts are fully functional, not merely connected
	for i := range s.cluster.Len() {
		actorID := "bootstrap-jwt-" + string(s.variant) + "-" + string(rune('a'+i))
		env, err := s.cluster.Service(i).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
		require.NoError(t, err, "a JWT-bootstrapped host should serve invocations")
		var out shared.ProbeState
		require.NoError(t, env.Decode(&out))
		assert.Equal(t, int64(1), out.N)
	}
}
