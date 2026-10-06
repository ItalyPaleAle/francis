package peer

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/protocol"
)

// startManagementServer runs a draining peer server whose management handler answers snapshots and records calls
func startManagementServer(t *testing.T, handler func(ctx context.Context, req *protocol.Envelope) *protocol.Envelope) (addr string, pc *Client) {
	t.Helper()

	addr = freeUDPAddr(t)
	srvTLS, cliTLS := peerTLSPair(t)

	ps := NewServer(ServerConfig{
		Bind:              addr,
		TLSConfig:         srvTLS,
		HostID:            func() string { return "host-b" },
		Handler:           echoHandler,
		Draining:          func() bool { return true },
		ManagementHandler: handler,
		Log:               slog.New(slog.DiscardHandler),
	})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() {
		_ = ps.Run(ctx)
	}()

	pc = NewClient(ClientConfig{
		TLSConfig:   cliTLS,
		DialTimeout: 5 * time.Second,
		Log:         slog.New(slog.DiscardHandler),
	})
	t.Cleanup(pc.Close)

	return addr, pc
}

// sendUntilUp retries a management request until the server accepts connections
func sendUntilUp(t *testing.T, pc *Client, addr string, hostID string, kind string, payload any, out any) *protocol.Error {
	t.Helper()

	var perr *protocol.Error
	deadline := time.Now().Add(10 * time.Second)
	for {
		reqCtx, reqCancel := context.WithTimeout(t.Context(), 2*time.Second)
		perr = pc.SendManagement(reqCtx, addr, hostID, kind, payload, out)
		reqCancel()
		if perr == nil || perr.Code != protocol.ErrCodeHostUnavailable || !time.Now().Before(deadline) {
			return perr
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPeerManagement(t *testing.T) {
	var calls atomic.Int32
	handler := func(_ context.Context, req *protocol.Envelope) *protocol.Envelope {
		calls.Add(1)
		switch req.Kind {
		case protocol.KindPeerHostSnapshot:
			var in protocol.HostSnapshotRequest
			err := req.DecodePayload(&in)
			if err != nil {
				return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "bad"))
			}
			resp, _ := req.ReplyWith(protocol.KindPeerHostSnapshotResponse, protocol.HostSnapshotResponse{HostID: "host-b", ActiveCount: in.Limit})
			return resp
		case protocol.KindPeerDeactivateActor:
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeActorNotHosted, "not here"))
		case protocol.KindPeerHostDrain:
			// A large reply that exceeds the client's bound
			resp, _ := req.ReplyWith(protocol.KindPeerHostDrainResponse, map[string]string{"pad": strings.Repeat("x", MaxManagementResponseSize+1)})
			return resp
		default:
			return nil
		}
	}
	addr, pc := startManagementServer(t, handler)

	t.Run("snapshot is served while draining", func(t *testing.T) {
		var out protocol.HostSnapshotResponse
		perr := sendUntilUp(t, pc, addr, "host-b", protocol.KindPeerHostSnapshot, protocol.HostSnapshotRequest{Limit: 7}, &out)
		require.Nil(t, perr, "unexpected error %v", perr)
		assert.Equal(t, "host-b", out.HostID)
		assert.Equal(t, 7, out.ActiveCount)
	})

	t.Run("structured errors are relayed", func(t *testing.T) {
		perr := sendUntilUp(t, pc, addr, "host-b", protocol.KindPeerDeactivateActor, protocol.DeactivateActorRequest{TargetHostID: "host-b", ActorType: "T", ActorID: "1"}, nil)
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeActorNotHosted, perr.Code)
	})

	t.Run("handler returning nil gets an internal error", func(t *testing.T) {
		perr := sendUntilUp(t, pc, addr, "host-b", "peer.mgmt.unknown", nil, nil)
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeInternal, perr.Code)
	})

	t.Run("oversized response is rejected", func(t *testing.T) {
		perr := sendUntilUp(t, pc, addr, "host-b", protocol.KindPeerHostDrain, protocol.HostDrainRequest{}, nil)
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeTransportFailure, perr.Code)
	})

	t.Run("peer identity is pinned", func(t *testing.T) {
		before := calls.Load()
		perr := sendUntilUp(t, pc, addr, "host-z", protocol.KindPeerHostSnapshot, protocol.HostSnapshotRequest{}, nil)
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeHostMismatch, perr.Code)
		assert.Equal(t, before, calls.Load(), "the request must not reach the wrong host")
	})

	t.Run("target host ID is required", func(t *testing.T) {
		perr := sendUntilUp(t, pc, addr, "", protocol.KindPeerHostSnapshot, protocol.HostSnapshotRequest{}, nil)
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeBadRequest, perr.Code)
	})

	t.Run("invocations are still rejected while draining", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer reqCancel()
		_, perr := pc.InvokeObject(reqCtx, addr, protocol.InvokeActorRequest{TargetHostID: "host-b", ActorType: "T", ActorID: "1", Method: "m"})
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeHostDraining, perr.Code)
	})
}

func TestPeerManagementWithoutHandler(t *testing.T) {
	addr, pc := startManagementServer(t, nil)

	// A server with no management handler treats management kinds as unexpected
	perr := sendUntilUp(t, pc, addr, "host-b", protocol.KindPeerHostSnapshot, protocol.HostSnapshotRequest{}, nil)
	require.NotNil(t, perr)
	assert.Equal(t, protocol.ErrCodeBadRequest, perr.Code)
}

func TestReadBoundedMessage(t *testing.T) {
	var buf bytes.Buffer
	env := protocol.NewEnvelope(protocol.KindHealthCheck, nil)
	err := protocol.WriteMessage(&buf, env)
	require.NoError(t, err)
	size := binary.BigEndian.Uint32(buf.Bytes()[:4])

	// A message within the bound is read normally
	got, err := readBoundedMessage(bytes.NewReader(buf.Bytes()), size)
	require.NoError(t, err)
	assert.Equal(t, protocol.KindHealthCheck, got.Kind)

	// A message over the bound is rejected from its length prefix
	_, err = readBoundedMessage(bytes.NewReader(buf.Bytes()), size-1)
	require.ErrorContains(t, err, "exceeds maximum")
}
