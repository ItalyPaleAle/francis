package local

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// handleManagement serves the management requests other hosts send over the peer server
func (h *Host) handleManagement(ctx context.Context, req *protocol.Envelope) *protocol.Envelope {
	switch req.Kind {
	case protocol.KindPeerHostSnapshot:
		// Decode and take the snapshot
		var payload protocol.HostSnapshotRequest
		err := req.DecodePayload(&payload)
		if err != nil {
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode host snapshot request"))
		}
		return replyWith(req, protocol.KindPeerHostSnapshotResponse, h.localSnapshot(payload))

	case protocol.KindPeerHostDrain:
		// Decode and accept the drain
		var payload protocol.HostDrainRequest
		err := req.DecodePayload(&payload)
		if err != nil {
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode host drain request"))
		}
		out, perr := h.localDrain(ctx, payload)
		if perr != nil {
			return req.ErrorReply(perr)
		}
		return replyWith(req, protocol.KindPeerHostDrainResponse, out)

	case protocol.KindPeerDeactivateActor:
		// Decode and deactivate the actor
		var payload protocol.DeactivateActorRequest
		err := req.DecodePayload(&payload)
		if err != nil {
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode deactivate actor request"))
		}
		perr := h.localDeactivate(payload)
		if perr != nil {
			return req.ErrorReply(perr)
		}
		return req.Reply(protocol.KindPeerDeactivateActorResponse, nil)

	default:
		return req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeBadRequest, "unexpected management message kind %q", req.Kind))
	}
}

// replyWith encodes a reply envelope, falling back to an internal error when the payload cannot be encoded
func replyWith(req *protocol.Envelope, kind string, payload any) *protocol.Envelope {
	resp, err := req.ReplyWith(kind, payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode management response"))
	}
	return resp
}

// localSnapshot returns a page of the actors active on this host
// The host reports itself draining as soon as it accepted a drain, which is what tells a management server whose drain request failed that the drain took effect anyway
func (h *Host) localSnapshot(req protocol.HostSnapshotRequest) protocol.HostSnapshotResponse {
	return h.core.HostSnapshot(req, h.HostID(), h.draining.Load() || h.adminDrain.Accepted())
}

// localDrain accepts an administrative drain of this host
// It marks the host draining before replying, and the caller triggers teardown after the acknowledgement
func (h *Host) localDrain(ctx context.Context, req protocol.HostDrainRequest) (protocol.HostDrainResponse, *protocol.Error) {
	// Record the drain, unless the host is not running or is already stopping
	timeout := time.Duration(max(req.TimeoutMs, 0)) * time.Millisecond
	already, err := h.adminDrain.Accept(timeout)
	if errors.Is(err, actorcore.ErrNotRunning) {
		return protocol.HostDrainResponse{}, protocol.NewError(protocol.ErrCodeHostUnavailable, "host is not running")
	}
	if already {
		return protocol.HostDrainResponse{AlreadyDraining: true}, nil
	}

	h.log.WarnContext(ctx, "Host drain requested by an administrator", slog.String("reason", req.Reason), slog.Duration("timeout", timeout))

	// Stop accepting invocations and placements before replying, so the caller observes the host as draining once it has the reply
	h.draining.Store(true)
	h.persistDraining()

	return protocol.HostDrainResponse{}, nil
}

// managementResponseWritten starts a peer drain only after the acknowledgement write has been attempted
func (h *Host) managementResponseWritten(req *protocol.Envelope, resp *protocol.Envelope) {
	if req.Kind != protocol.KindPeerHostDrain || resp.Kind != protocol.KindPeerHostDrainResponse {
		return
	}
	var ack protocol.HostDrainResponse
	err := resp.DecodePayload(&ack)
	if err == nil && !ack.AlreadyDraining {
		h.adminDrain.Trigger()
	}
}

// localDeactivate halts an actor active on this host
func (h *Host) localDeactivate(req protocol.DeactivateActorRequest) *protocol.Error {
	// The request must be for this host, or the caller's view of placement is stale
	if req.TargetHostID != h.HostID() {
		return protocol.NewError(protocol.ErrCodeHostMismatch, "deactivation is for a different host")
	}
	err := ref.ValidateComponents(req.ActorType, req.ActorID)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeBadRequest, "invalid actor reference: %v", err)
	}

	err = h.core.Halt(req.ActorType, req.ActorID)
	switch {
	case errors.Is(err, actor.ErrActorNotHosted):
		return protocol.NewError(protocol.ErrCodeActorNotHosted, "actor is not active on this host")
	case err != nil:
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to deactivate actor: %v", err)
	default:
		return nil
	}
}

// managementSnapshot returns a page of the actors active on the host hostID at address, serving it in-process when the target is this host
// Structured failures are returned as *protocol.Error
func (h *Host) managementSnapshot(ctx context.Context, hostID string, address string, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	// Serve this host's own snapshot in-process
	if h.isSelf(hostID) {
		return h.localSnapshot(req), nil
	}

	var out protocol.HostSnapshotResponse
	perr := h.peerClient.SendManagement(ctx, address, hostID, protocol.KindPeerHostSnapshot, req, &out)
	if perr != nil {
		return protocol.HostSnapshotResponse{}, perr
	}
	return out, nil
}

// managementDrain drains the host hostID at address, handling it in-process when the target is this host
// Structured failures are returned as *protocol.Error
func (h *Host) managementDrain(ctx context.Context, hostID string, address string, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
	// Drain this host in-process
	if h.isSelf(hostID) {
		out, perr := h.localDrain(ctx, req)
		if perr != nil {
			return protocol.HostDrainResponse{}, perr
		}

		// HTTP shutdown waits for the in-process management request to finish writing its response
		if !out.AlreadyDraining {
			h.adminDrain.Trigger()
		}

		return out, nil
	}

	var out protocol.HostDrainResponse
	perr := h.peerClient.SendManagement(ctx, address, hostID, protocol.KindPeerHostDrain, req, &out)
	if perr != nil {
		return protocol.HostDrainResponse{}, perr
	}
	return out, nil
}

// managementDeactivate halts an actor on the host hostID at address, handling it in-process when the target is this host
// It reports notActive when the actor was not active on that host, and returns other structured failures as *protocol.Error
func (h *Host) managementDeactivate(ctx context.Context, hostID string, address string, actorType string, actorID string) (notActive bool, err error) {
	req := protocol.DeactivateActorRequest{
		TargetHostID: hostID,
		ActorType:    actorType,
		ActorID:      actorID,
	}

	// Deactivate on this host in-process, or send the request to the target host
	var perr *protocol.Error
	if h.isSelf(hostID) {
		perr = h.localDeactivate(req)
	} else {
		perr = h.peerClient.SendManagement(ctx, address, hostID, protocol.KindPeerDeactivateActor, req, nil)
	}

	switch {
	case perr == nil:
		return false, nil
	case perr.Code == protocol.ErrCodeActorNotHosted:
		return true, nil
	default:
		return false, perr
	}
}

// isSelf reports whether hostID is this host's current registration
func (h *Host) isSelf(hostID string) bool {
	self := h.HostID()
	return self != "" && hostID == self
}
