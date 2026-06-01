package runtime

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// handleRegistration performs the registration handshake on the first stream of a session
// It returns true if the host was successfully registered
func (rt *Runtime) handleRegistration(ctx context.Context, c *hostConn) bool {
	stream, err := c.session.AcceptStream(ctx)
	if err != nil {
		return false
	}
	defer stream.Close()

	req, err := protocol.ReadMessage(stream)
	if err != nil {
		return false
	}

	if req.Kind != protocol.KindRegisterHost {
		_ = protocol.WriteMessage(stream, req.ErrorReply(
			protocol.NewError(protocol.ErrCodeBadRequest, "expected host registration as the first message"),
		))
		return false
	}

	resp := rt.handleRegister(ctx, c, req)
	_ = protocol.WriteMessage(stream, resp)

	// Registration succeeded unless the response is an error
	return resp.Kind != protocol.KindError
}

// dispatch routes a request to its handler and returns the response envelope
func (rt *Runtime) dispatch(ctx context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	// Reject stale sessions: a request must carry the session ID and host ID we assigned at registration
	if req.SessionID != "" && req.SessionID != c.sessionID {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeSessionSuperseded, "session has been superseded"))
	}
	if req.HostID != "" && req.HostID != c.hostID {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeHostMismatch, "request is for a different host"))
	}

	switch req.Kind {
	case protocol.KindUnregisterHost:
		return rt.handleUnregister(ctx, c, req)
	case protocol.KindHealthCheck:
		return rt.handleHealthCheck(ctx, c, req)
	case protocol.KindLookupActor:
		return rt.handleLookupActor(ctx, c, req)
	case protocol.KindGetAlarm:
		return rt.handleGetAlarm(ctx, c, req)
	case protocol.KindSetAlarm:
		return rt.handleSetAlarm(ctx, c, req)
	case protocol.KindDeleteAlarm:
		return rt.handleDeleteAlarm(ctx, c, req)
	case protocol.KindGetState:
		return rt.handleGetState(ctx, c, req)
	case protocol.KindSetState:
		return rt.handleSetState(ctx, c, req)
	case protocol.KindDeleteState:
		return rt.handleDeleteState(ctx, c, req)
	default:
		return req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeBadRequest, "unknown message kind %q", req.Kind))
	}
}

// handleRegister registers or reattaches a host with the provider and tracks its session
func (rt *Runtime) handleRegister(ctx context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.RegisterHostRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode registration request"))
	}

	// Negotiate the protocol version
	verErr := protocol.CheckProtocolVersion(payload.ProtocolVersion)
	if verErr != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeProtocolVersion, verErr.Error()))
	}
	if payload.Address == "" {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "registration is missing the host address"))
	}

	// Register the host with the provider, reattaching to an existing registration if the host supplied a previous ID
	regCtx, cancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
	defer cancel()
	res, err := rt.provider.RegisterHost(regCtx, components.RegisterHostReq{
		Address:        payload.Address,
		ActorTypes:     protocolActorTypesToComponents(payload.ActorTypes),
		ExistingHostID: payload.PreviousHostID,
	})
	if err != nil {
		return req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeInternal, "failed to register host: %v", err))
	}

	// Record the host identity and session on the connection
	c.hostID = res.HostID
	c.sessionID = uuid.NewString()
	c.address = payload.Address
	c.actorTypes = payload.ActorTypes
	c.protocolVersion = protocol.NegotiateVersion(payload.ProtocolVersion)

	// Track the session, superseding any prior session for the same host
	superseded := rt.hosts.Register(c)
	if superseded != nil {
		rt.log.InfoContext(ctx, "Superseding host session",
			slog.String("hostId", c.hostID),
			slog.String("oldSessionId", superseded.sessionID),
			slog.String("newSessionId", c.sessionID),
		)
		superseded.close(sessionErrorSuperseded, "session superseded by a newer connection")
	}

	resp, err := req.ReplyWith(protocol.KindRegisterHostResponse, protocol.RegisterHostResponse{
		HostID:                c.hostID,
		SessionID:             c.sessionID,
		ProtocolVersion:       c.protocolVersion,
		HealthCheckIntervalMs: rt.provider.HealthCheckInterval().Milliseconds(),
		Reattached:            res.Reattached,
	})
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode registration response"))
	}
	return resp
}

// handleUnregister handles a graceful, drain-oriented host shutdown request
func (rt *Runtime) handleUnregister(ctx context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	// Mark the host draining so it is no longer selected for new placement or alarm work
	c.setDraining()

	// Remove the host from the provider so it is no longer eligible for new actor placement
	// This uses immediate removal, which is acceptable for the initial implementation
	unregCtx, cancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
	defer cancel()
	err := rt.provider.UnregisterHost(unregCtx, c.hostID)
	if err != nil {
		rt.log.WarnContext(ctx, "Error unregistering host from provider",
			slog.String("hostId", c.hostID),
			slog.Any("error", err),
		)
	}

	resp, err := req.ReplyWith(protocol.KindUnregisterHostResponse, protocol.UnregisterHostResponse{})
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode unregister response"))
	}
	return resp
}

// notImplemented is a placeholder for handlers implemented in later phases
func notImplemented(req *protocol.Envelope, what string) *protocol.Envelope {
	return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, what+" is not implemented yet"))
}

// The following handlers are implemented in Phase 4 (handlers) and Phase 5 (alarm dispatch)

func (rt *Runtime) handleHealthCheck(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "health check")
}

func (rt *Runtime) handleLookupActor(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "actor lookup")
}

func (rt *Runtime) handleGetAlarm(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "get alarm")
}

func (rt *Runtime) handleSetAlarm(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "set alarm")
}

func (rt *Runtime) handleDeleteAlarm(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "delete alarm")
}

func (rt *Runtime) handleGetState(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "get state")
}

func (rt *Runtime) handleSetState(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "set state")
}

func (rt *Runtime) handleDeleteState(_ context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	return notImplemented(req, "delete state")
}
