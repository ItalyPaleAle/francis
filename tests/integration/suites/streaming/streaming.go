//go:build integration

// Package streaming exercises streamed actor invocation: the request body streams in, the actor echoes it back, and the response content type and body round-trip, including across a host boundary
package streaming

import (
	"bytes"
	"io"
	"strconv"
	"strings"
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

// variants is the representative set covering the distinct transport and provider paths
var variants = []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory}

// Register a single-host scenario for every representative variant on both runtimes, plus a two-host scenario wherever two hosts can share one backend
func init() {
	for _, v := range variants {
		for _, k := range []cluster.Kind{cluster.Local, cluster.Remote} {
			suite.Register(&localStream{kind: k, variant: v})
			if k == cluster.Remote || v.LocalMultiHost() {
				suite.Register(&crossHostStream{kind: k, variant: v})
			}
		}
	}
}

// echo runs one streamed invocation and returns the response content type and body
func echo(t *testing.T, svc *actor.Service, actorID, method, reqContentType string, payload []byte) (string, []byte) {
	t.Helper()
	respCT, resp, err := svc.InvokeStream(t.Context(), shared.ProbeActorType, actorID, method, reqContentType, bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Close()
	got, err := io.ReadAll(resp)
	require.NoError(t, err)
	return respCT, got
}

// localStream exercises streamed invocation against a single host
type localStream struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *localStream) Name() string {
	return "streaming/" + string(s.kind) + "/" + string(s.variant)
}

func (s *localStream) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   1,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *localStream) Run(t *testing.T) {
	svc := s.cluster.Service(0)

	// A small body round-trips, prefixed by the method, with the actor's content type
	t.Run("small body echoes", func(t *testing.T) {
		respCT, got := echo(t, svc, "stream-small", "echo", "text/plain", []byte("hello world"))
		assert.Equal(t, shared.ProbeStreamContentType, respCT)
		assert.Equal(t, "echo:hello world", string(got))
	})

	// A large body streams through intact
	t.Run("large body streams intact", func(t *testing.T) {
		payload := bytes.Repeat([]byte("abcdefghij"), 25_000) // 250 KB
		respCT, got := echo(t, svc, "stream-large", "echo", "application/octet-stream", payload)
		assert.Equal(t, shared.ProbeStreamContentType, respCT)
		require.Len(t, got, len("echo:")+len(payload))
		assert.True(t, strings.HasPrefix(string(got), "echo:"))
		assert.True(t, bytes.Equal([]byte(strings.TrimPrefix(string(got), "echo:")), payload), "the streamed body should round-trip unchanged")
	})
}

// crossHostStream exercises streamed invocation that crosses a host boundary
type crossHostStream struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
}

func (s *crossHostStream) Name() string {
	return "streaming-crosshost/" + string(s.kind) + "/" + string(s.variant)
}

func (s *crossHostStream) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors:  []frameworkhost.ActorReg{shared.ProbeReg(actorcore.RegisterActorOptions{IdleTimeout: time.Minute})},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *crossHostStream) Run(t *testing.T) {
	ctx := t.Context()
	actorID := "stream-xh-" + string(s.kind) + "-" + string(s.variant)

	// Label hosts and place the actor, so we can stream to it from a different host
	for i := range s.cluster.Len() {
		shared.SetHostLabel(s.cluster.Service(i), "h"+strconv.Itoa(i))
	}
	_, err := s.cluster.Service(0).Invoke(ctx, shared.ProbeActorType, actorID, shared.ProbeMethodPing, nil)
	require.NoError(t, err)
	placed := shared.ProbeObserver.LastInvokeHost(actorID)
	require.NotEmpty(t, placed)

	// Stream from a different host so the request and response cross the peer or runtime boundary
	caller := 0
	if placed == "h0" {
		caller = 1
	}
	payload := bytes.Repeat([]byte("0123456789"), 20_000) // 200 KB
	respCT, resp, err := s.cluster.Service(caller).InvokeStream(ctx, shared.ProbeActorType, actorID, "echo", "application/octet-stream", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Close()
	got, err := io.ReadAll(resp)
	require.NoError(t, err)

	assert.Equal(t, shared.ProbeStreamContentType, respCT)
	assert.True(t, bytes.Equal([]byte(strings.TrimPrefix(string(got), "echo:")), payload), "the streamed body should round-trip across hosts")
}
