package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/protocol"
)

// Compile-time interface assertion
var _ management.Backend = (*managementBackend)(nil)

func TestManagementRouteFollowsAHostToItsNewReplica(t *testing.T) {
	rt, prov := newTestRuntime(t)
	b := &managementBackend{rt: rt}

	// The host is now connected to this replica with a new session
	res, err := prov.RegisterHost(t.Context(), components.RegisterHostReq{
		Address:    "127.0.0.1:9999",
		ActorTypes: []components.ActorHostType{{ActorType: "A", IdleTimeout: time.Minute}},
		SessionID:  "s2",
		RuntimeID:  rt.runtimeID,
	})
	require.NoError(t, err)
	conn := &hostConn{hostID: res.HostID, sessionID: "s2"}
	rt.hosts.Register(conn)

	// The caller still holds the row from before the move, naming a replica that no longer has a live membership
	stale := components.HostDetails{HostID: res.HostID, SessionID: "s1", RuntimeID: "gone-runtime"}

	// The request is routed again once a fresh read shows the host moved
	var delivered *hostConn
	err = b.route(t.Context(), stale, func(c *hostConn) error {
		delivered = c
		return nil
	}, protocol.KindRuntimeHostSnapshot, protocol.RuntimeHostRequest{}, nil)
	require.NoError(t, err)
	assert.Same(t, conn, delivered)
}

func TestManagementRouteReportsAHostThatDidNotMove(t *testing.T) {
	rt, prov := newTestRuntime(t)
	b := &managementBackend{rt: rt}

	// The row names a replica with no live membership, and a fresh read says the same
	res, err := prov.RegisterHost(t.Context(), components.RegisterHostReq{
		Address:    "127.0.0.1:9999",
		ActorTypes: []components.ActorHostType{{ActorType: "A", IdleTimeout: time.Minute}},
		SessionID:  "s1",
		RuntimeID:  "gone-runtime",
	})
	require.NoError(t, err)
	host, err := prov.GetHostDetails(t.Context(), res.HostID)
	require.NoError(t, err)

	// Nothing moved, so the failure is returned without delivering anything
	err = b.route(t.Context(), host, func(c *hostConn) error {
		t.Fatal("the request must not be delivered locally")
		return nil
	}, protocol.KindRuntimeHostSnapshot, protocol.RuntimeHostRequest{}, nil)
	require.ErrorIs(t, err, management.ErrHostUnavailable)
}

func TestIsUnspecifiedAddress(t *testing.T) {
	cases := map[string]bool{
		":8443":              true,
		"0.0.0.0:8443":       true,
		"[::]:8443":          true,
		"10.0.0.4:8443":      false,
		"127.0.0.1:8443":     false,
		"francis-0.svc:7400": false,
		"not-an-address":     false,
	}
	for address, want := range cases {
		assert.Equalf(t, want, isUnspecifiedAddress(address), "address %q", address)
	}
}

func TestManagementRouteExplainsAnUnreachableAdvertiseAddress(t *testing.T) {
	rt, prov := newTestRuntime(t)
	b := &managementBackend{rt: rt}

	// Another replica registered its unspecified bind address, which this replica can't dial
	err := prov.RegisterRuntime(t.Context(), components.RegisterRuntimeReq{RuntimeID: "other", Address: ":8443", TTL: time.Minute})
	require.NoError(t, err)
	res, err := prov.RegisterHost(t.Context(), components.RegisterHostReq{
		Address:    "127.0.0.1:9999",
		ActorTypes: []components.ActorHostType{{ActorType: "A", IdleTimeout: time.Minute}},
		SessionID:  "s1",
		RuntimeID:  "other",
	})
	require.NoError(t, err)
	host, err := prov.GetHostDetails(t.Context(), res.HostID)
	require.NoError(t, err)

	// The request fails with an explanation rather than dialing the address
	err = b.route(t.Context(), host, func(c *hostConn) error {
		t.Fatal("the request must not be delivered locally")
		return nil
	}, protocol.KindRuntimeHostSnapshot, protocol.RuntimeHostRequest{}, nil)
	require.ErrorIs(t, err, management.ErrHostUnavailable)
	assert.ErrorContains(t, err, "set advertiseAddress")
}

func TestDrainHostConnMarksTheSessionOnlyOnceTheHostAccepted(t *testing.T) {
	rt, _ := newTestRuntime(t)

	t.Run("a failed request leaves the session in service", func(t *testing.T) {
		c := &hostConn{hostID: "h1", sessionID: "s1"}
		rt.sendToHost = func(context.Context, *hostConn, *protocol.Envelope) (*protocol.Envelope, error) {
			return nil, errors.New("stream reset")
		}

		_, err := rt.drainHostConn(t.Context(), c, protocol.HostDrainRequest{})
		require.Error(t, err)
		assert.False(t, c.IsDraining(), "a host that never accepted the drain must keep getting alarms and jobs")
	})

	t.Run("an accepted drain marks the session", func(t *testing.T) {
		c := &hostConn{hostID: "h1", sessionID: "s1"}
		rt.sendToHost = func(_ context.Context, _ *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
			return env.ReplyWith(protocol.KindHostDrainResponse, protocol.HostDrainResponse{})
		}

		_, err := rt.drainHostConn(t.Context(), c, protocol.HostDrainRequest{})
		require.NoError(t, err)
		assert.True(t, c.IsDraining())
	})
}
