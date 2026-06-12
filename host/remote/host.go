package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/italypaleale/go-kit/ttlcache"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/peer"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

const (
	defaultShutdownGracePeriod = 30 * time.Second
	defaultRequestTimeout      = 15 * time.Second
)

// Host is an actor host that connects to a standalone runtime cluster over WebTransport
// The runtime owns placement and alarm scheduling, while this host owns the lifecycle of the actors active on it
type Host struct {
	// Peer address this host advertises to the runtime and other hosts
	address string
	// Address the peer server binds to
	bind string

	running atomic.Bool
	// draining is set once graceful shutdown begins, so the peer server rejects new invocations while local actors drain
	draining atomic.Bool
	service  *actor.Service

	// core owns the active actors, their turn-based invocation, idle deactivation, and halting
	core *actorcore.Manager
	// resolver adapts this host to the placement resolver the shared messaging logic depends on
	resolver actorcore.PlacementResolver

	// runtimeClient maintains the persistent session to the runtime for placement, state, and alarm operations
	runtimeClient *runtimeClient
	// peerClient invokes actors owned by other hosts
	peerClient *peer.Client
	// peerServer serves invocations of actors owned by this host
	peerServer *peer.Server

	// Actor placement cache
	placementCache *ttlcache.Cache[string, *actorcore.Placement]

	// holder stores the live workload certificate and trust bundle shared by the runtime and peer TLS configs
	holder              *certholder.Holder
	requestTimeout      time.Duration
	shutdownGracePeriod time.Duration

	logSource *slog.Logger
	log       *slog.Logger
	clock     clock.WithTicker
}

// NewHost returns a new remote actor host.
func NewHost(opts ...HostOption) (*Host, error) {
	options := &newHostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return newHost(options)
}

func newHost(options *newHostOptions) (*Host, error) {
	// Validate the peer address
	if options.Address == "" {
		return nil, errors.New("option Address is required")
	}
	addrHost, addrPortStr, err := net.SplitHostPort(options.Address)
	if err != nil {
		return nil, fmt.Errorf("option Address is invalid: cannot split host and port: %w", err)
	}
	addrPort, err := strconv.Atoi(addrPortStr)
	if err != nil || addrPort == 0 {
		return nil, errors.New("option Address is invalid: port is invalid")
	}

	// At least one runtime address is required to connect to
	if len(options.RuntimeAddresses) == 0 {
		return nil, errors.New("option RuntimeAddresses is required")
	}

	// Set a default logger, which sends logs to /dev/null, if none is passed
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}

	// Set other default values
	if options.BindAddress == "" {
		options.BindAddress = addrHost
	}
	if options.BindPort <= 0 {
		options.BindPort = addrPort
	}
	if options.ShutdownGracePeriod <= 0 {
		options.ShutdownGracePeriod = defaultShutdownGracePeriod
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = defaultRequestTimeout
	}

	// Init a real clock if none is passed
	if options.clock == nil {
		options.clock = &clock.RealClock{}
	}

	// Resolve the host bootstrap method, which must be exactly one of PSK or JWT
	// PSK proves the host with a channel-bound challenge-response, while JWT presents a bearer token validated by the runtime
	hasPSK := len(options.BootstrapPSK) > 0
	hasJWT := options.BootstrapTokenFn != nil
	var bootstrapPSK *bootstrapauth.PSK
	switch {
	case hasPSK && hasJWT:
		return nil, errors.New("only one host bootstrap method may be configured")
	case hasPSK:
		bootstrapPSK, err = bootstrapauth.NewPSK(options.BootstrapPSK)
		if err != nil {
			return nil, fmt.Errorf("invalid host bootstrap PSK: %w", err)
		}
	case hasJWT:
		// The token provider is invoked on each bootstrap so a rotated token is re-read
	default:
		return nil, errors.New("a host bootstrap method is required: configure either a host PSK or JWT")
	}

	// The first-connection trust decision must be explicit: pin the CA, or opt out unsafely
	hasPinned := len(options.PinnedCAPEM) > 0
	switch {
	case hasPinned && options.UnsafeNoPinnedCA:
		return nil, errors.New("cannot set both WithPinnedCA and WithUnsafeNoPinnedCA")
	case !hasPinned && !options.UnsafeNoPinnedCA:
		return nil, errors.New("the cluster CA trust must be set explicitly: use WithPinnedCA to pin the CA, or WithUnsafeNoPinnedCA to trust the runtime on first connection")
	}

	// Seed the trust bundle with the pinned CA so the host can verify the runtime from its very first connection
	holder := certholder.New(nil, nil)
	if hasPinned {
		pool, poolErr := ca.PoolFromPEM(options.PinnedCAPEM)
		if poolErr != nil {
			return nil, fmt.Errorf("failed to parse pinned CA: %w", poolErr)
		}
		holder.SetRoots(pool)
	} else {
		// Without a pinned CA the host trusts the runtime certificate on its first connection, so warn loudly up front
		options.Logger.Warn("Connecting to a runtime without a pinned CA: trusting the runtime certificate on first use. Pin the cluster CA to close this gap, especially for JWT bootstrap")
	}

	// Build the TLS configurations, which all read the live certificate and trust bundle from the holder
	runtimeTLSConfig := hosttls.RuntimeClientTLSConfig(holder)
	peerClientTLSConfig := hosttls.PeerClientTLSConfig(holder)
	peerServerTLSConfig := hosttls.PeerServerTLSConfig(holder)

	// Create the host
	h := &Host{
		address:             options.Address,
		bind:                net.JoinHostPort(options.BindAddress, strconv.Itoa(options.BindPort)),
		holder:              holder,
		requestTimeout:      options.RequestTimeout,
		shutdownGracePeriod: options.ShutdownGracePeriod,
		logSource:           options.Logger,
		clock:               options.clock,
	}
	h.service = actor.NewService(h)

	// The actor core owns activation, turn-based invocation, idle deactivation, and halting
	// On deactivation it notifies the runtime that the actor is no longer placed here
	h.core = actorcore.NewManager(actorcore.Options{
		Service:                h.service,
		RemoveActor:            h.removeActor,
		Logger:                 options.Logger,
		Clock:                  options.clock,
		ProviderRequestTimeout: options.RequestTimeout,
		ShutdownGracePeriod:    options.ShutdownGracePeriod,
	})

	// The resolver lets the shared messaging logic resolve placement through the runtime and confirm ownership before activating an actor
	h.resolver = placementResolver{h: h}

	// The peer client invokes actors owned by other hosts, presenting this host's workload certificate for mutual authentication
	h.peerClient = peer.NewClient(peer.ClientConfig{
		TLSConfig:   peerClientTLSConfig,
		DialTimeout: options.RequestTimeout,
		Log:         options.Logger,
	})

	// The runtime client maintains the session to the runtime, bootstraps the host, installs the issued workload certificate, and serves runtime-initiated requests
	// Its actor types are filled in at Run, once all actor types have been registered
	h.runtimeClient = newRuntimeClient(runtimeClientConfig{
		addresses:        options.RuntimeAddresses,
		peerAddress:      options.Address,
		tlsConfig:        runtimeTLSConfig,
		holder:           holder,
		bootstrapPSK:     bootstrapPSK,
		bootstrapTokenFn: options.BootstrapTokenFn,
		requestTimeout:   options.RequestTimeout,
		log:              options.Logger,
		clock:            options.clock,
		onDrainStart:     func() { h.draining.Store(true) },
		// On graceful shutdown the runtime client drains local actors while the session is still alive, so their deactivation can still persist state and clear placement through the runtime
		onDrain: h.drainActors,
		// When a session ends, drop cached placements so they are re-resolved against the next session
		onSessionEnd: h.invalidateAllPlacements,
		handlers: runtimeHandlers{
			executeAlarm:   h.executeAlarm,
			terminateActor: h.terminateActor,
		},
	})

	// The peer server serves invocations of actors owned by this host
	// It reports our current runtime-assigned host ID so it can reject invocations aimed at a stale placement
	h.peerServer = peer.NewServer(peer.ServerConfig{
		Bind:                h.bind,
		TLSConfig:           peerServerTLSConfig,
		Handler:             h.peerInvokeObject,
		StreamHandler:       h.peerInvokeStream,
		Log:                 options.Logger,
		HostID:              h.runtimeClient.HostID,
		Draining:            h.isDraining,
		MaxInFlightRequests: options.MaxInFlightRequests,
		MaxRequestBodySize:  options.MaxRequestBodySize,
	})

	return h, nil
}

// Service returns a Service object configured to interact with this host.
func (h *Host) Service() *actor.Service {
	return h.service
}

// Run the host service.
// Note this function is blocking, and will return only when the service is shut down via context cancellation.
func (h *Host) Run(parentCtx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return errors.New("service is already running")
	}
	defer h.running.Store(false)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Start the actor core (idle processor) and the placement cache
	h.core.Start()
	defer h.core.Close()

	h.placementCache = ttlcache.NewCache[string, *actorcore.Placement](&ttlcache.CacheOptions{
		MaxTTL: placementCacheMaxTTL,
	})
	defer h.placementCache.Stop()

	// Tear down pooled outbound peer sessions once the host stops serving
	defer h.peerClient.Close()

	// Advertise the registered actor types to the runtime at registration time
	h.runtimeClient.cfg.actorTypes = componentsActorTypesToProtocol(h.core.RegisteredActorTypes())

	// Use the runtime client's logger once it learns the host ID
	h.log = h.logSource

	// On graceful shutdown the runtime client drains actors while its session is still alive, so this is only a fallback for an ungraceful exit where no runtime session was available
	// Halting an already-halted actor is a no-op, so running it after a graceful drain is harmless
	defer h.drainActors()

	// The peer server runs under its own context so it keeps serving while local actors drain, then is stopped only after the runtime client returns
	// This lets it reject new invocations with a retry-later error throughout the drain window, rather than tearing down alongside it
	peerCtx, stopPeer := context.WithCancel(context.WithoutCancel(parentCtx))
	defer stopPeer()

	peerErrCh := make(chan error, 1)
	go func() {
		peerErrCh <- h.peerServer.Run(peerCtx)
	}()

	// Maintain the persistent session to the runtime, reconnecting as needed
	// On graceful shutdown the runtime client sets the host draining, unregisters, and drains local actors before returning
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- h.runtimeClient.Run(ctx)
	}()

	// Wait for whichever service returns first
	// If the runtime client returns, its drain has already run, so stop the peer server and wait for it
	// If the peer server fails first, cancel the context to bring the runtime client down too
	var runErr, peerErr error
	select {
	case runErr = <-runErrCh:
		stopPeer()
		peerErr = <-peerErrCh
	case peerErr = <-peerErrCh:
		cancel()
		runErr = <-runErrCh
	}

	return errors.Join(runErr, peerErr)
}

// Ready returns a channel that is closed once the host has registered with a runtime for the first time
func (h *Host) Ready() <-chan struct{} {
	return h.runtimeClient.Ready()
}

// HostID returns the current runtime-assigned ID of the host, or empty if not yet registered.
func (h *Host) HostID() string {
	return h.runtimeClient.HostID()
}

// isDraining reports whether the host has begun graceful shutdown
func (h *Host) isDraining() bool {
	return h.draining.Load()
}

// HaltAll halts all actors active on the host, gracefully
func (h *Host) HaltAll() error {
	return h.core.HaltAll()
}

// drainActors halts all active actors during graceful shutdown, logging any error
// The runtime client calls this while its session is still alive, so actor deactivation can still persist state and clear placement through the runtime
func (h *Host) drainActors() {
	err := h.HaltAll()
	if err != nil {
		h.log.Warn("Error draining actors during shutdown", slog.Any("error", err))
	}
}

// Halt gracefully halts an actor that is hosted on the current host
func (h *Host) Halt(actorType string, actorID string) error {
	return h.core.Halt(actorType, actorID)
}

// HaltDeferred gracefully halts an actor that is hosted on the current host
// This is a non-blocking variant of the Halt method, which runs in background
func (h *Host) HaltDeferred(actorType string, actorID string) {
	h.core.HaltDeferred(actorType, actorID)
}

// removeActor notifies the runtime that an actor has been deactivated on this host
// It is the RemoveActor seam the manager calls when an actor is halted or idle-deactivated
func (h *Host) removeActor(ctx context.Context, r ref.ActorRef) error {
	err := h.runtimeClient.RemoveActor(ctx, protocol.RemoveActorRequest{
		ActorRef: protocol.ActorRef{ActorType: r.ActorType, ActorID: r.ActorID},
	})

	// The runtime reports an actor that was already gone with ErrCodeActorNotActive
	// Translate it to the provider's ErrNoActor so the manager treats it as a successful no-op
	if isProtocolErrorCode(err, protocol.ErrCodeActorNotActive) {
		return components.ErrNoActor
	} else if err != nil {
		return err
	}

	return nil
}
