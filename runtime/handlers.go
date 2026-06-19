package runtime

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/channelbind"
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

	// Compute the channel binding once, used to verify a PSK challenge-response is bound to this exact TLS session
	cb, err := channelbind.Export(c.session)
	if err != nil {
		rt.log.WarnContext(ctx, "Failed to compute channel binding for registration", slog.Any("error", err))
		return false
	}

	req, err := protocol.ReadMessageWithTimeout(stream, registrationReadTimeout)
	if err != nil {
		return false
	}

	// A host either opens with a PSK challenge, or registers directly for JWT bootstrap or an mTLS reconnect
	preAuthed := false
	switch req.Kind {
	case protocol.KindRegisterHostAuth:
		regReq, ok := rt.handleAuthBegin(stream, c, cb, req)
		if !ok {
			return false
		}
		req = regReq
		preAuthed = true
	case protocol.KindRegisterHost:
		// No challenge round-trip is needed for these paths
	default:
		_ = protocol.WriteMessage(stream, req.ErrorReply(
			protocol.NewError(protocol.ErrCodeBadRequest, "expected host registration as the first message"),
		))
		return false
	}

	resp := rt.handleRegister(ctx, c, req, preAuthed)
	_ = protocol.WriteMessage(stream, resp)

	// Registration succeeded unless the response is an error
	return resp.Kind != protocol.KindError
}

// handleAuthBegin runs the runtime side of the PSK challenge-response and returns the follow-up registration envelope on success
// It proves to the host that the runtime holds the host PSK, then verifies the host's own proof, both bound to the channel binding
func (rt *Runtime) handleAuthBegin(stream protocol.Stream, _ *hostConn, cb []byte, req *protocol.Envelope) (*protocol.Envelope, bool) {
	// A PSK challenge is only valid when the cluster is configured for PSK bootstrap
	if rt.bootstrapMethod != bootstrapauth.MethodPSK {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "PSK bootstrap is not enabled")))
		return nil, false
	}

	var begin protocol.RegisterAuthBeginRequest
	err := req.DecodePayload(&begin)
	if err != nil || begin.Method != bootstrapauth.MethodPSK || len(begin.ClientNonce) == 0 {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "invalid PSK bootstrap request")))
		return nil, false
	}

	// Generate the server nonce and compute the runtime proof, which the host checks before sending its own proof
	serverNonce, err := bootstrapauth.Nonce()
	if err != nil {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to generate nonce")))
		return nil, false
	}
	serverProof := rt.bootstrapPSK.ServerProof(cb, begin.ClientNonce, serverNonce)

	challenge, err := req.ReplyWith(protocol.KindRegisterHostAuthChallenge, protocol.RegisterAuthChallengeResponse{
		ServerNonce: serverNonce,
		ServerProof: serverProof,
	})
	if err != nil {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode challenge")))
		return nil, false
	}
	err = protocol.WriteMessage(stream, challenge)
	if err != nil {
		return nil, false
	}

	// Read the follow-up registration carrying the host's proof
	regReq, err := protocol.ReadMessageWithTimeout(stream, registrationReadTimeout)
	if err != nil {
		return nil, false
	}
	if regReq.Kind != protocol.KindRegisterHost {
		_ = protocol.WriteMessage(stream, regReq.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "expected host registration after the challenge")))
		return nil, false
	}

	var payload protocol.RegisterHostRequest
	err = regReq.DecodePayload(&payload)
	if err != nil {
		_ = protocol.WriteMessage(stream, regReq.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode registration request")))
		return nil, false
	}

	// Verify the host proof over the same channel binding and nonces, which fails for a wrong PSK, a replay, or a MitM
	ok := payload.Auth.Method == bootstrapauth.MethodPSK && rt.bootstrapPSK.VerifyClientProof(cb, begin.ClientNonce, serverNonce, payload.Auth.Proof)
	if !ok {
		_ = protocol.WriteMessage(stream, regReq.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "PSK authentication failed")))
		return nil, false
	}

	return regReq, true
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
	case protocol.KindRenewCert:
		return rt.handleRenewCert(ctx, c, req)
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

// handleRegister authenticates, registers or reattaches a host with the provider, tracks its session, and issues a workload certificate when needed
// preAuthed is true when a PSK challenge-response already authenticated the session on this stream
func (rt *Runtime) handleRegister(ctx context.Context, c *hostConn, req *protocol.Envelope, preAuthed bool) *protocol.Envelope {
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

	// Authenticate the host and decide whether a fresh workload certificate must be issued
	// A PSK challenge already authenticated the session, while JWT and mTLS reconnect are authenticated here
	var (
		joinToken          string
		joinTokenExpiresAt time.Time
		reattachID         string
	)
	issueCert := preAuthed
	if !preAuthed {
		switch payload.Auth.Method {
		case bootstrapauth.MethodJWT:
			var authErr error
			joinToken, joinTokenExpiresAt, authErr = rt.authenticateJWT(ctx, payload.Auth.Token)
			if authErr != nil {
				return req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, authErr.Error()))
			}
			issueCert = true
		case "":
			// An mTLS reconnect carries no bootstrap credential and is identified by its client certificate
			hostID, certErr := rt.verifyHostClientCert(c)
			if certErr != nil {
				rt.log.WarnContext(ctx, "Client certificate authentication failed", slog.Any("error", certErr))
				return req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "client certificate authentication failed"))
			}
			reattachID = hostID
			// Mint a fresh certificate only when the host sent a new public key, otherwise it keeps the one it has
			issueCert = len(payload.WorkloadPubKey) > 0
		default:
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "unsupported bootstrap method"))
		}
	}

	// Resolve the identity to reattach to
	// Only an mTLS reconnect may reattach, because its identity is proven by the client certificate
	existingID := ""
	if reattachID != "" {
		// The certificate proves who we are, so any claimed PreviousHostID must match it
		if payload.PreviousHostID != "" && payload.PreviousHostID != reattachID {
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "previous host ID does not match the certificate identity"))
		}
		existingID = reattachID
	}

	// Register the host with the provider, reattaching to an existing registration when an identity is supplied
	regCtx, cancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
	defer cancel()
	res, err := rt.provider.RegisterHost(regCtx, components.RegisterHostReq{
		Address:            payload.Address,
		ActorTypes:         protocolActorTypesToComponents(payload.ActorTypes),
		ExistingHostID:     existingID,
		JoinToken:          joinToken,
		JoinTokenExpiresAt: joinTokenExpiresAt,
	})
	if errors.Is(err, components.ErrJoinTokenAlreadyConsumed) {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeUnauthorized, "join token has already been used"))
	} else if err != nil {
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

	// Build the response, always sending the current trust bundle so the host can pick up a root rotation
	resp := protocol.RegisterHostResponse{
		HostID:                c.hostID,
		SessionID:             c.sessionID,
		ProtocolVersion:       c.protocolVersion,
		HealthCheckIntervalMs: rt.provider.HealthCheckInterval().Milliseconds(),
		Reattached:            res.Reattached,
		CABundlePEM:           rt.caBundlePEM(),
	}

	// Issue a workload certificate when the host needs one, which is every bootstrap and any reconnect that rotated its key
	if issueCert {
		if len(payload.WorkloadPubKey) == 0 {
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "registration is missing the workload public key"))
		}
		der, notAfter, certErr := rt.issueWorkloadCert(c.hostID, payload.WorkloadPubKey)
		if certErr != nil {
			rt.log.ErrorContext(ctx, "Failed to issue workload certificate", slog.Any("error", certErr))
			return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to issue workload certificate"))
		}
		resp.WorkloadCertDER = der
		resp.CertNotAfterMs = notAfter.UnixMilli()
	}

	out, err := req.ReplyWith(protocol.KindRegisterHostResponse, resp)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode registration response"))
	}
	return out
}

// authenticateJWT validates a host bootstrap token, enforcing that the cluster is configured for JWT bootstrap
// Returns the join token and its expiry when the token carries a jti and an expiry; both are zero otherwise
func (rt *Runtime) authenticateJWT(ctx context.Context, token string) (joinToken string, expiresAt time.Time, err error) {
	if rt.bootstrapMethod != bootstrapauth.MethodJWT {
		return "", time.Time{}, errors.New("JWT bootstrap is not enabled")
	}
	if rt.bootstrapJWT == nil {
		return "", time.Time{}, errors.New("JWT validator is not ready")
	}

	// Log the real reason server-side but return a generic error to avoid leaking validation details to the caller
	subject, jt, jtExp, valErr := rt.bootstrapJWT.Validate(token)
	if valErr != nil {
		rt.log.WarnContext(ctx, "JWT validation failed", slog.Any("error", valErr))
		return "", time.Time{}, errors.New("JWT authentication failed")
	}
	rt.log.DebugContext(ctx, "Host authenticated via JWT", slog.String("subject", subject))
	return jt, jtExp, nil
}

// handleRenewCert issues a fresh workload certificate over an already authenticated session
func (rt *Runtime) handleRenewCert(_ context.Context, c *hostConn, req *protocol.Envelope) *protocol.Envelope {
	var payload protocol.RenewCertRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode renew cert request"))
	}
	if len(payload.WorkloadPubKey) == 0 {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "renew cert is missing the workload public key"))
	}

	// dispatch already verified the session and host ID, so the certificate is issued for this connection's host
	der, notAfter, err := rt.issueWorkloadCert(c.hostID, payload.WorkloadPubKey)
	if err != nil {
		rt.log.Error("Failed to renew workload certificate", slog.String("hostId", c.hostID), slog.Any("error", err))
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to issue certificate"))
	}

	return rt.reply(req, protocol.KindRenewCertResponse, protocol.RenewCertResponse{
		WorkloadCertDER: der,
		CABundlePEM:     rt.caBundlePEM(),
		CertNotAfterMs:  notAfter.UnixMilli(),
	})
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID, payload.Name)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID, payload.Name)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID, payload.Name)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
	err = ref.ValidateComponents(payload.ActorType, payload.ActorID)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, err.Error()))
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
