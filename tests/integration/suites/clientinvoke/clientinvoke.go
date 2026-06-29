//go:build integration

// Package clientinvoke exercises the actor client's Invoke method:
//
//   - an actor can invoke another actor through its client and read back the reply
//   - invoking your own actor from within its turn deadlocks on the turn lock
package clientinvoke

import (
	"context"
	"fmt"
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
)

const (
	// callerActorType is the registered type of the actor under test
	callerActorType = "clientinvoke-caller"
	// calleeActorID is the fixed peer the caller invokes for the cross-actor case
	calleeActorID = "callee"

	// methodCallOther invokes the callee through the client and returns its reply
	methodCallOther = "call-other"
	// methodCallSelf invokes the actor's own ID through the client, which deadlocks on the turn lock
	methodCallSelf = "call-self"
	// methodEcho returns the actor's own ID, so a caller can confirm which actor replied
	methodEcho = "echo"
)

// callerActor invokes other actors through its client
type callerActor struct {
	actorID string
	client  actor.Client[struct{}]
}

func newCallerActor(actorID string, service *actor.Service) actor.Actor {
	return &callerActor{
		actorID: actorID,
		client:  actor.NewActorClient[struct{}](callerActorType, actorID, service),
	}
}

// Invoke handles the echo, call-other, and call-self methods
func (a *callerActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	switch method {
	case methodEcho:
		return a.actorID, nil

	case methodCallOther:
		// Invoke a different actor through the client and return whatever it replied
		env, err := a.client.Invoke(ctx, callerActorType, calleeActorID, methodEcho, nil)
		if err != nil {
			return nil, err
		}
		var reply string
		err = env.Decode(&reply)
		if err != nil {
			return nil, err
		}
		return reply, nil

	case methodCallSelf:
		// Invoke our own actor through the client, re-entering our own turn, which deadlocks on the turn lock
		_, err := a.client.Invoke(ctx, callerActorType, a.actorID, methodEcho, nil)
		return nil, err

	default:
		return nil, fmt.Errorf("unknown method %q", method)
	}
}

// matrix runs the scenario across representative topology/provider combinations
var matrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int
}{
	{cluster.Local, provider.SQLite, 2},
	{cluster.Local, provider.StandaloneMemory, 1},
	{cluster.Remote, provider.Postgres, 2},
}

func init() {
	for _, m := range matrix {
		suite.Register(&clientInvoke{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}
}

// clientInvoke drives a cluster whose actors invoke each other through the client
type clientInvoke struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster *cluster.Cluster
}

func (s *clientInvoke) Name() string {
	return "clientinvoke/" + string(s.kind) + "/" + string(s.variant)
}

func (s *clientInvoke) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   s.hosts,
		Actors: []frameworkhost.ActorReg{{
			Type:    callerActorType,
			Factory: newCallerActor,
			Opts:    actorcore.RegisterActorOptions{IdleTimeout: time.Minute},
		}},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *clientInvoke) Run(t *testing.T) {
	svc := s.cluster.Service(0)
	ctx := t.Context()

	// An actor invokes another actor through its client and gets that actor's reply back
	t.Run("actor invokes another actor", func(t *testing.T) {
		env, err := svc.Invoke(ctx, callerActorType, "caller", methodCallOther, nil)
		require.NoError(t, err)

		var reply string
		err = env.Decode(&reply)
		require.NoError(t, err)
		assert.Equal(t, calleeActorID, reply, "the reply should come from the invoked actor, not the caller")
	})

	// An actor invoking its own ID from within its turn re-enters the turn lock and deadlocks
	t.Run("invoking itself deadlocks", func(t *testing.T) {
		invokeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := svc.Invoke(invokeCtx, callerActorType, "self-caller", methodCallSelf, nil)
			done <- err
		}()

		// The self-invocation blocks on the actor's own turn lock, so the call must not complete
		select {
		case <-done:
			t.Fatal("self-invocation should deadlock, but it returned")
		case <-time.After(2 * time.Second):
			// Still blocked after the grace period, as expected
		}

		// Cancelling releases the blocked invocation, proving it was waiting on the lock and leaving no goroutine behind
		cancel()
		select {
		case err := <-done:
			require.Error(t, err, "the cancelled self-invocation should return an error")
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled self-invocation did not return")
		}
	})
}
