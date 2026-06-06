package runtime

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// lookupCacheMaxTTL caps how long a placement lookup is cached at the runtime
const lookupCacheMaxTTL = 5 * time.Second

// registrationReadTimeout bounds the registration handshake read more tightly than a regular request, since a new session must register promptly before it is allowed to do anything else
const registrationReadTimeout = 10 * time.Second

// cachedPlacement is a placement lookup result held in the runtime placement cache
type cachedPlacement struct {
	HostID        string
	Address       string
	IdleTimeoutMs int64
}

// handleRegistration performs the registration handshake on the first stream of a session
// It returns true if the host was successfully registered
func (rt *Runtime) handleRegistration(ctx context.Context, c *hostConn) bool {
	stream, err := c.session.AcceptStream(ctx)
	if err != nil {
		return false
	}
	defer stream.Close()

	req, err := protocol.ReadMessageWithTimeout(stream, registrationReadTimeout)
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
	case protocol.KindRemoveActor:
		return rt.handleRemoveActor(ctx, c, req)
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

	// Negotiate the session protocol version from the version the host advertised in the envelope
	negotiatedVersion, verErr := protocol.NegotiateVersion(req.ProtocolVersion)
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
		rt.log.ErrorContext(ctx, "Failed to register host", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to register host"))
	}

	// Record the host identity and session on the connection
	c.hostID = res.HostID
	c.sessionID = uuid.NewString()
	c.address = payload.Address
	c.setActorTypes(payload.ActorTypes)
	c.protocolVersion = negotiatedVersion

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
func (rt *Runtime) handleUnregister(_ context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	// Mark the host draining so it is no longer selected for new placement or alarm work
	// This alone makes the host ineligible for new work: ConnectedHostIDs excludes draining hosts and handleLookupActor returns retry-later for any actor resolved onto one
	c.setDraining()

	// Deliberately do not remove the host from the provider here
	// Its actors are still active and must stay resolvable until the host has finished draining them (provider removal happens once the session closes, in handleHostDisconnect)
	// Removing it now would delete every active-actor placement for the host, yanking actors that are still running

	resp, err := req.ReplyWith(protocol.KindUnregisterHostResponse, protocol.UnregisterHostResponse{})
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode unregister response"))
	}
	return resp
}

// handleHealthCheck persists a periodic host health report, optionally updating the supported actor types
func (rt *Runtime) handleHealthCheck(parentCtx context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.HealthCheckRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode health check request"))
	}

	updateReq := components.UpdateActorHostReq{
		UpdateLastHealthCheck: true,
	}

	// A non-nil actor type list replaces the host's supported types
	// A nil list leaves them unchanged
	if payload.ActorTypes != nil {
		updateReq.ActorTypes = protocolActorTypesToComponents(payload.ActorTypes)
		c.setActorTypes(payload.ActorTypes)
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.UpdateActorHost(ctx, c.hostID, updateReq)
	if errors.Is(err, components.ErrHostUnregistered) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeHostUnregistered, "host registration is no longer valid"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to persist health check", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to persist health check"))
	}

	return req.Reply(protocol.KindHealthCheckResponse, nil)
}

// handleLookupActor resolves the placement of an actor, consulting the runtime placement cache when allowed
func (rt *Runtime) handleLookupActor(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.LookupActorRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode lookup request"))
	}
	if payload.ActorType == "" || payload.ActorID == "" {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "lookup is missing the actor type or ID"))
	}

	aRef := ref.NewActorRef(payload.ActorType, payload.ActorID)
	key := aRef.String()

	// The cache only serves allocating lookups
	// Active-only lookups always consult the provider
	useCache := rt.placementCache != nil && !payload.ActiveOnly
	if useCache && !payload.SkipCache {
		cached, ok := rt.placementCache.Get(key)
		// A cached host that has since started draining must not be served
		if ok && !rt.hostDraining(cached.HostID) {
			return rt.reply(req, protocol.KindLookupActorResponse, protocol.LookupActorResponse{
				HostID:        cached.HostID,
				Address:       cached.Address,
				IdleTimeoutMs: cached.IdleTimeoutMs,
			})
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	res, err := rt.provider.LookupActor(ctx, aRef, components.LookupActorOpts{
		ActiveOnly: payload.ActiveOnly,
	})
	switch {
	case errors.Is(err, components.ErrNoHost):
		rt.deletePlacement(key)
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeNoHost, "no host is available to place the actor"))
	case errors.Is(err, components.ErrNoActor):
		// Only reachable for active-only lookups, and not retryable
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeActorNotActive, "actor is not currently active"))
	case err != nil:
		rt.deletePlacement(key)
		rt.log.ErrorContext(ctx, "Failed to look up actor", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to look up actor"))
	}

	// Never hand out a placement on a host that is draining on this runtime
	if rt.hostDraining(res.HostID) {
		rt.deletePlacement(key)
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeRetryLater, "actor is moving off a draining host").WithRetryAfter(rt.alarmsPollInterval))
	}

	// Always refresh the cache with the fresh placement
	if useCache {
		rt.placementCache.Set(key, &cachedPlacement{
			HostID:        res.HostID,
			Address:       res.Address,
			IdleTimeoutMs: res.IdleTimeout.Milliseconds(),
		}, rt.placementCacheTTL())
	}

	return rt.reply(req, protocol.KindLookupActorResponse, protocol.LookupActorResponse{
		HostID:        res.HostID,
		Address:       res.Address,
		IdleTimeoutMs: res.IdleTimeout.Milliseconds(),
	})
}

// handleRemoveActor clears the placement of an actor that the host has deactivated
// The host owns the actor lifecycle and tells the runtime once an actor is gone so callers stop being routed to it
func (rt *Runtime) handleRemoveActor(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.RemoveActorRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode remove actor request"))
	}
	if payload.ActorType == "" || payload.ActorID == "" {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "remove actor is missing the actor type or ID"))
	}

	aRef := ref.NewActorRef(payload.ActorType, payload.ActorID)

	// Drop any cached placement so callers are no longer routed to the deactivated actor
	rt.deletePlacement(aRef.String())

	// Remove the actor from the provider's set of active placements
	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.RemoveActor(ctx, aRef)
	if errors.Is(err, components.ErrNoActor) {
		// The actor was already gone, which is the desired end state, so report it without treating it as a hard failure
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeActorNotActive, "actor is not currently active"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to remove actor", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to remove actor"))
	}

	return req.Reply(protocol.KindRemoveActorResponse, nil)
}

// handleGetAlarm retrieves an alarm's properties
func (rt *Runtime) handleGetAlarm(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.GetAlarmRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode get alarm request"))
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	res, err := rt.provider.GetAlarm(ctx, ref.NewAlarmRef(payload.ActorType, payload.ActorID, payload.Name))
	if errors.Is(err, components.ErrNoAlarm) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeAlarmNotFound, "alarm does not exist"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to get alarm", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to get alarm"))
	}

	return rt.reply(req, protocol.KindGetAlarmResponse, protocol.GetAlarmResponse{
		AlarmProperties: refAlarmPropsToProtocol(res.AlarmProperties),
	})
}

// handleSetAlarm creates or replaces an alarm
func (rt *Runtime) handleSetAlarm(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.SetAlarmRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode set alarm request"))
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.SetAlarm(ctx, ref.NewAlarmRef(payload.ActorType, payload.ActorID, payload.Name), components.SetAlarmReq{
		AlarmProperties: protocolAlarmPropsToRef(payload.AlarmProperties),
	})
	if err != nil {
		rt.log.ErrorContext(ctx, "Failed to set alarm", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to set alarm"))
	}

	return req.Reply(protocol.KindSetAlarmResponse, nil)
}

// handleDeleteAlarm removes an alarm
func (rt *Runtime) handleDeleteAlarm(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.DeleteAlarmRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode delete alarm request"))
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.DeleteAlarm(ctx, ref.NewAlarmRef(payload.ActorType, payload.ActorID, payload.Name))
	if errors.Is(err, components.ErrNoAlarm) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeAlarmNotFound, "alarm does not exist"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to delete alarm", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to delete alarm"))
	}

	return req.Reply(protocol.KindDeleteAlarmResponse, nil)
}

// handleGetState retrieves an actor's persistent state
func (rt *Runtime) handleGetState(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.GetStateRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode get state request"))
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	data, err := rt.provider.GetState(ctx, ref.NewActorRef(payload.ActorType, payload.ActorID))
	if errors.Is(err, components.ErrNoState) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeStateNotFound, "no state found for the actor"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to get state", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to get state"))
	}

	return rt.reply(req, protocol.KindGetStateResponse, protocol.GetStateResponse{Data: data})
}

// handleSetState stores an actor's persistent state
func (rt *Runtime) handleSetState(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.SetStateRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode set state request"))
	}

	opts := components.SetStateOpts{}
	if payload.TTLMs > 0 {
		opts.TTL = time.Duration(payload.TTLMs) * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.SetState(ctx, ref.NewActorRef(payload.ActorType, payload.ActorID), payload.Data, opts)
	if err != nil {
		rt.log.ErrorContext(ctx, "Failed to set state", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to set state"))
	}

	return req.Reply(protocol.KindSetStateResponse, nil)
}

// handleDeleteState removes an actor's persistent state
func (rt *Runtime) handleDeleteState(parentCtx context.Context, _ *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.DeleteStateRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode delete state request"))
	}

	ctx, cancel := context.WithTimeout(parentCtx, rt.providerRequestTimeout)
	defer cancel()
	err = rt.provider.DeleteState(ctx, ref.NewActorRef(payload.ActorType, payload.ActorID))
	if errors.Is(err, components.ErrNoState) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeStateNotFound, "no state found for the actor"))
	} else if err != nil {
		rt.log.ErrorContext(ctx, "Failed to delete state", slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to delete state"))
	}

	return req.Reply(protocol.KindDeleteStateResponse, nil)
}

// reply builds a response envelope of the given kind, falling back to an internal error if encoding fails
func (rt *Runtime) reply(req *protocol.Envelope, kind string, payload any) *protocol.Envelope {
	resp, err := req.ReplyWith(kind, payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode response"))
	}
	return resp
}

// hostDraining reports whether the given host is connected to this runtime and currently draining
func (rt *Runtime) hostDraining(hostID string) bool {
	c, ok := rt.hosts.Get(hostID)
	return ok && c.IsDraining()
}

// deletePlacement removes a cache entry when the cache is enabled
func (rt *Runtime) deletePlacement(key string) {
	if rt.placementCache != nil {
		rt.placementCache.Delete(key)
	}
}

// placementCacheTTL returns the TTL used for placement cache entries, bounded by the health check deadline
func (rt *Runtime) placementCacheTTL() time.Duration {
	return min(lookupCacheMaxTTL, rt.hostHealthCheckDeadline)
}
