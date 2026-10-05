package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/netutils"
	"github.com/italypaleale/francis/internal/peer"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

const (
	// runtimeMembershipTTL is how long a replica's membership lease lasts unless renewed
	runtimeMembershipTTL = 30 * time.Second
	// runtimeMembershipRenewInterval is how often a replica renews its membership lease
	runtimeMembershipRenewInterval = 10 * time.Second
	// hostManagementTimeout bounds a management request sent to a host on behalf of another replica
	hostManagementTimeout = 30 * time.Second
)

// membershipAddress returns the address other replicas dial to reach this one
// Without an explicit advertise address it is derived from the bind address, falling back to the bind address itself when the host IP can't be detected
func (rt *Runtime) membershipAddress(ctx context.Context) string {
	if rt.advertiseAddress != "" {
		return rt.advertiseAddress
	}

	address, err := advertiseFromBind(rt.bind, netutils.GetHostAddress)
	if err != nil {
		rt.log.WarnContext(ctx,
			"Failed to detect the address to advertise to other runtime replicas; falling back to the bind address",
			slog.String("bind", rt.bind),
			slog.Any("error", err),
		)
		return rt.bind
	}

	return address
}

// advertiseFromBind derives the address to advertise to other replicas from the bind address
// A bind address with a specific host, including a loopback one, is the only address the server listens on, so it is advertised as-is
// A bind address with no host or an unspecified one listens on every interface, so its host is replaced with the IP returned by getHostIP
func advertiseFromBind(bind string, getHostIP func() (string, error)) (string, error) {
	if !isUnspecifiedAddress(bind) {
		return bind, nil
	}

	_, port, err := net.SplitHostPort(bind)
	if err != nil {
		return "", fmt.Errorf("invalid bind address '%s': %w", bind, err)
	}

	ip, err := getHostIP()
	if err != nil {
		return "", fmt.Errorf("failed to detect the host IP address: %w", err)
	}

	return net.JoinHostPort(ip, port), nil
}

// isUnspecifiedAddress reports whether an address has no host or an unspecified one, such as ":8443" or "0.0.0.0:8443"
// Another machine can't dial such an address: on Linux it even reaches the dialing machine itself
func isUnspecifiedAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// A malformed address fails with its own error when it is dialed
		return false
	}

	if host == "" {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// runMembership keeps this replica's membership lease registered until the context is canceled, then removes it
// A runtime ID held by another live replica is logged and retried rather than stopping the runtime, since the other replica may be a previous process that has not expired yet
func (rt *Runtime) runMembership(ctx context.Context) error {
	address := rt.membershipAddress(ctx)
	rt.log.InfoContext(ctx, "Advertising runtime address to other replicas", slog.String("address", address))

	// An unspecified address only matters once another replica needs to reach this one, so it is reported once another replica is seen
	warnUnreachable := isUnspecifiedAddress(address)

	register := func() {
		rCtx, cancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
		defer cancel()
		err := rt.provider.RegisterRuntime(rCtx, components.RegisterRuntimeReq{
			RuntimeID: rt.runtimeID,
			Address:   address,
			TTL:       runtimeMembershipTTL,
		})
		switch {
		case errors.Is(err, components.ErrRuntimeIDInUse):
			rt.log.ErrorContext(ctx,
				"Runtime ID is in use by another replica with a different address; every replica must have a unique runtime ID",
				slog.String("runtimeId", rt.runtimeID),
				slog.String("address", address),
			)
		case err != nil && ctx.Err() == nil:
			rt.log.WarnContext(ctx, "Failed to renew runtime membership", slog.Any("error", err))
		case err == nil && warnUnreachable:
			// Other replicas forward management requests to this address, which they can't dial
			runtimes, lErr := rt.provider.ListRuntimes(rCtx)
			if lErr == nil && len(runtimes) > 1 {
				rt.log.WarnContext(ctx, "Other runtime replicas can't reach this one at the address it advertises, so management requests for its hosts fail on them; set the advertise address, or the "+netutils.HostIPEnvVar+" environment variable, to an address they can dial", slog.String("address", address))
				warnUnreachable = false
			}
		}
	}

	// Register right away, then renew periodically
	register()
	ticker := rt.clock.NewTicker(runtimeMembershipRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C():
			register()
		case <-ctx.Done():
			// Remove the membership with a fresh context, so other replicas stop routing to this one without waiting for the lease to expire
			uCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rt.providerRequestTimeout)
			err := rt.provider.UnregisterRuntime(uCtx, rt.runtimeID, address)
			cancel()
			if err != nil {
				rt.log.Warn("Failed to remove runtime membership", slog.Any("error", err))
			}
			return nil
		}
	}
}

// newRuntimePeerClient returns the client used to send management requests to other runtime replicas
// It presents this replica's runtime certificate and accepts only peers with a runtime identity from the cluster CA
func newRuntimePeerClient(cas []*ca.CA, cert tls.Certificate, log *slog.Logger) *peer.Client {
	pool := ca.NewCertPool(cas)
	roots := func() *x509.CertPool {
		return pool
	}

	return peer.NewClient(peer.ClientConfig{
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{http3.NextProtoH3},
			Certificates: []tls.Certificate{cert},
			// #nosec G402 -- VerifyPeerCertificate performs all verification against the cluster CA
			InsecureSkipVerify: true,
			// #nosec G123 -- Session resumption reuses a session whose cert was already verified
			VerifyPeerCertificate: ca.VerifyPeerSPIFFE(roots, ca.RuntimePrefix, nil),
		},
		ConnectPath: protocol.RuntimePeerPath,
		PeerID:      ca.RuntimeIDFromCert,
		Log:         log,
	})
}

// handleRuntimePeerConnect upgrades a WebTransport session from another runtime replica
// Host workload certificates chain to the same CA, so the runtime SPIFFE namespace is required
func (rt *Runtime) handleRuntimePeerConnect(ctx context.Context, wtServer *webtransport.Server, handlers *sync.WaitGroup) http.HandlerFunc {
	pool := ca.NewCertPool(rt.cas)
	verify := ca.VerifyPeerSPIFFE(func() *x509.CertPool { return pool }, ca.RuntimePrefix, nil)

	return func(w http.ResponseWriter, r *http.Request) {
		// Authenticate the peer before upgrading
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		raw := make([][]byte, len(r.TLS.PeerCertificates))
		for i, c := range r.TLS.PeerCertificates {
			raw[i] = c.Raw
		}
		err := verify(raw, nil)
		if err != nil {
			rt.log.WarnContext(r.Context(), "Rejected runtime peer connection", slog.Any("error", err))
			w.WriteHeader(http.StatusForbidden)
			return
		}

		session, err := wtServer.Upgrade(w, r)
		if err != nil {
			rt.log.WarnContext(r.Context(), "Failed to upgrade runtime peer WebTransport session", slog.Any("error", err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		handlers.Go(func() {
			rt.serveRuntimePeerSession(ctx, session, handlers)
		})
	}
}

// serveRuntimePeerSession serves the management requests another runtime replica sends over a session
func (rt *Runtime) serveRuntimePeerSession(parentCtx context.Context, session *webtransport.Session, handlers *sync.WaitGroup) {
	sessCtx := session.Context()

	// Close the session if the runtime shuts down
	go func() {
		select {
		case <-parentCtx.Done():
			_ = session.CloseWithError(sessionErrorShutdown, "runtime shutting down")
		case <-sessCtx.Done():
		}
	}()

	for {
		stream, err := session.AcceptStream(sessCtx)
		if err != nil {
			return
		}

		handlers.Go(func() {
			rt.handleRuntimePeerStream(sessCtx, stream)
		})
	}
}

// handleRuntimePeerStream reads a single management request from another replica, serves it, and writes the response
func (rt *Runtime) handleRuntimePeerStream(ctx context.Context, stream *webtransport.Stream) {
	defer wt.CloseStream(stream)

	req, err := protocol.ReadMessageWithTimeout(stream, requestReadTimeout)
	if err != nil {
		return
	}

	reqCtx, cancel := context.WithTimeout(protocol.ExtractTraceContext(stream.Context(), req), hostManagementTimeout)
	defer cancel()

	resp := rt.dispatchRuntimePeer(reqCtx, req)
	err = protocol.WriteMessage(stream, resp)
	if err != nil {
		rt.log.WarnContext(ctx, "Failed to write response to runtime peer", slog.String("kind", req.Kind), slog.Any("error", err))
	}
}

// dispatchRuntimePeer serves a management request from another replica
func (rt *Runtime) dispatchRuntimePeer(ctx context.Context, req *protocol.Envelope) *protocol.Envelope {
	// The sessions request is not about a specific host
	if req.Kind == protocol.KindRuntimeSessions {
		return rt.reply(req, protocol.KindRuntimeSessionsResponse, rt.sessions())
	}

	var payload protocol.RuntimeHostRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode runtime peer request"))
	}

	// Deliver the request only to the session the caller expects
	c, perr := rt.ownedHostConn(payload.HostID, payload.SessionID)
	if perr != nil {
		return req.ErrorReply(perr)
	}

	switch {
	case req.Kind == protocol.KindRuntimeHostSnapshot && payload.Snapshot != nil:
		res, err := rt.hostSnapshot(ctx, c, *payload.Snapshot)
		if err != nil {
			return req.ErrorReply(asProtocolError(err))
		}
		return rt.reply(req, protocol.KindRuntimeHostSnapshotResponse, res)
	case req.Kind == protocol.KindRuntimeHostDrain && payload.Drain != nil:
		res, err := rt.drainHostConn(ctx, c, *payload.Drain)
		if err != nil {
			return req.ErrorReply(asProtocolError(err))
		}
		return rt.reply(req, protocol.KindRuntimeHostDrainResponse, res)
	case req.Kind == protocol.KindRuntimeDeactivateActor && payload.Terminate != nil:
		notActive, err := rt.deactivateOnHost(ctx, c, payload.Terminate.ActorType, payload.Terminate.ActorID)
		if err != nil {
			return req.ErrorReply(asProtocolError(err))
		}
		return rt.reply(req, protocol.KindRuntimeDeactivateActorResponse, protocol.TerminateActorResponse{NotActive: notActive})
	default:
		return req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeBadRequest, "unsupported runtime peer request kind %q", req.Kind))
	}
}

// sessions lists the host sessions this replica holds
func (rt *Runtime) sessions() protocol.RuntimeSessionsResponse {
	conns := rt.hosts.All()
	res := protocol.RuntimeSessionsResponse{
		RuntimeID: rt.runtimeID,
		Sessions:  make([]protocol.RuntimeSessionInfo, len(conns)),
	}
	for i, c := range conns {
		res.Sessions[i] = protocol.RuntimeSessionInfo{
			HostID:    c.ID(),
			SessionID: c.SessionID(),
			Draining:  c.IsDraining(),
		}
	}
	return res
}

// ownedHostConn returns this replica's session for a host, if it is the expected one
func (rt *Runtime) ownedHostConn(hostID string, sessionID string) (*hostConn, *protocol.Error) {
	c, ok := rt.hosts.Get(hostID)
	if !ok {
		return nil, protocol.NewErrorf(protocol.ErrCodeHostUnavailable, "runtime %s holds no session for host %s", rt.runtimeID, hostID)
	}
	if sessionID != "" && c.SessionID() != sessionID {
		return nil, protocol.NewErrorf(protocol.ErrCodeHostReattached, "host %s is connected with a different session", hostID)
	}
	return c, nil
}

// hostSnapshot asks a connected host for a snapshot of its activations
func (rt *Runtime) hostSnapshot(ctx context.Context, c *hostConn, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	var res protocol.HostSnapshotResponse
	err := rt.sendHostManagement(ctx, c, protocol.KindHostSnapshot, req, &res)
	return res, err
}

// drainHostConn asks a connected host to drain, and marks its session draining once the host accepted
// The session is marked only after the acknowledgement, so a host that never received the request keeps getting alarms and jobs from this replica
// A host whose acknowledgement was lost marks the session itself, since its teardown tells the runtime it is draining
func (rt *Runtime) drainHostConn(ctx context.Context, c *hostConn, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
	var res protocol.HostDrainResponse
	err := rt.sendHostManagement(ctx, c, protocol.KindHostDrain, req, &res)
	if err != nil {
		return res, err
	}

	// Stop leasing alarms and jobs to the host right away, rather than when its teardown reaches the runtime
	c.setDraining()
	return res, nil
}

// deactivateOnHost asks a connected host to halt one of its actors
func (rt *Runtime) deactivateOnHost(ctx context.Context, c *hostConn, actorType string, actorID string) (notActive bool, err error) {
	var res protocol.TerminateActorResponse
	err = rt.sendHostManagement(ctx, c, protocol.KindTerminateActor, protocol.TerminateActorRequest{ActorType: actorType, ActorID: actorID}, &res)
	perr, ok := errors.AsType[*protocol.Error](err)
	if ok && perr.Code == protocol.ErrCodeActorNotHosted {
		return true, nil
	} else if err != nil {
		return false, err
	}

	// Drop any cached placement for the actor, since it is going away
	rt.deletePlacement(ref.NewActorRef(actorType, actorID).String())

	return res.NotActive, nil
}

// sendHostManagement sends one management request to a connected host and decodes its reply
// A failure to reach the host is returned as ErrCodeHostUnavailable, and structured errors from the host as-is
func (rt *Runtime) sendHostManagement(ctx context.Context, c *hostConn, kind string, payload any, out any) error {
	env, err := protocol.NewRequest(kind, payload)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to encode request: %v", err)
	}

	resp, err := rt.sendToHost(ctx, c, env)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeHostUnavailable, "failed to send request to host %s: %v", c.ID(), err)
	}

	perr, isErr := resp.AsError()
	if isErr {
		return perr
	}

	// Older hosts reply to a terminate request with an empty body
	if out == nil || len(resp.Payload) == 0 {
		return nil
	}
	err = resp.DecodePayload(out)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to decode host response: %v", err)
	}
	return nil
}

// asProtocolError returns err as a protocol error, wrapping any other error as an internal one
func asProtocolError(err error) *protocol.Error {
	perr, ok := errors.AsType[*protocol.Error](err)
	if ok {
		return perr
	}

	return protocol.NewError(protocol.ErrCodeInternal, err.Error())
}
