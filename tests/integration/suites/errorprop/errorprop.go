//go:build integration

// Package errorprop verifies that an error returned by an actor propagates back to the caller, including across a peer or runtime boundary
package errorprop

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

// variants is the representative set covering the distinct transport and provider paths
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register the single-host scenario for every representative variant on both runtimes, and a two-host scenario wherever two hosts can share one backend
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&localError{
				kind:    k,
				variant: v,
			})

			if k == cluster.Remote || v.LocalMultiHost() {
				suite.Register(&crossHostError{
					kind:    k,
					variant: v,
				})
			}
		}
	}
}

// localError checks that an actor error reaches the caller on a single host, which on the remote runtime still crosses the host-to-runtime boundary
type localError struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *localError) Name() string {
	return "errorprop/" + string(s.kind) + "/" + string(s.variant)
}

func (s *localError) Setup(t *testing.T) []framework.Option {
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

func (s *localError) Run(t *testing.T) {
	_, err := s.cluster.Service(0).Invoke(t.Context(), shared.ProbeActorType, "err-1", shared.ProbeMethodFail, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, shared.ProbeFailMessage, "the actor's error message should reach the caller")
}

// crossHostError checks that an actor error propagates back across the peer or runtime hop to a caller on a different host
type crossHostError struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *crossHostError) Name() string {
	return "errorprop-crosshost/" + string(s.kind) + "/" + string(s.variant)
}

func (s *crossHostError) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.WithIdleTimeout(time.Minute))},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *crossHostError) Run(t *testing.T) {
	ctx := t.Context()
	const actorID = "err-xh"

	// Label hosts so we can learn where the actor is placed, then call it from a different host to force a cross-host hop
	labels := make([]string, s.cluster.Len())
	for i := range s.cluster.Len() {
		labels[i] = "h" + strconv.Itoa(i)
		shared.SetHostLabel(s.cluster.Service(i), labels[i])
	}

	// The first call activates the actor and fails
	// Activation still places it, so we learn its host
	_, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodFail, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, shared.ProbeFailMessage)

	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	require.NotEmpty(t, placed)

	// Call from a different host so the request and its error cross the peer or runtime boundary
	caller := 0
	for i, l := range labels {
		if l != placed {
			caller = i
			break
		}
	}
	_, err = s.cluster.Service(caller).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodFail, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, shared.ProbeFailMessage, "the actor's error should propagate back across the host boundary")
}
