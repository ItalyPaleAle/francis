package remote

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
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
	service *actor.Service

	// core owns the active actors, their turn-based invocation, idle deactivation, and halting
	core *actorcore.Manager

	// runtimeClient maintains the persistent session to the runtime for placement, state, and alarm operations
	runtimeClient *runtimeClient
	// peerClient invokes actors owned by other hosts
	peerClient *peerClient
	// peerServer serves invocations of actors owned by this host
	peerServer *peerServer

	// Actor placement cache
	placementCache *ttlcache.Cache[string, *actorPlacement]

	clientTLSConfig     *tls.Config
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

	// Init the TLS configuration for the peer server and the runtime/peer clients
	serverTLSConfig, clientTLSConfig, err := options.TLSOptions.GetTLSConfig()
	if err != nil {
		return nil, err
	}

	// Create the host
	h := &Host{
		address:             options.Address,
		bind:                net.JoinHostPort(options.BindAddress, strconv.Itoa(options.BindPort)),
		clientTLSConfig:     clientTLSConfig,
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

	// The peer client invokes actors owned by other hosts
	h.peerClient = newPeerClient(clientTLSConfig, options.RequestTimeout, options.Logger)

	// The runtime client maintains the session to the runtime and serves runtime-initiated requests
	// Its actor types are filled in at Run, once all actor types have been registered
	h.runtimeClient = newRuntimeClient(runtimeClientConfig{
		addresses:      options.RuntimeAddresses,
		peerAddress:    options.Address,
		tlsConfig:      clientTLSConfig,
		requestTimeout: options.RequestTimeout,
		log:            options.Logger,
		clock:          options.clock,
		handlers: runtimeHandlers{
			executeAlarm:   h.executeAlarm,
			terminateActor: h.terminateActor,
		},
	})

	// The peer server serves invocations of actors owned by this host
	// It reports our current runtime-assigned host ID so it can reject invocations aimed at a stale placement
	h.peerServer = newPeerServer(peerServerConfig{
		bind:          h.bind,
		tlsConfig:     serverTLSConfig,
		handler:       h.peerInvokeObject,
		streamHandler: h.peerInvokeStream,
		log:           options.Logger,
		hostID:        h.runtimeClient.HostID,
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

	h.placementCache = ttlcache.NewCache[string, *actorPlacement](&ttlcache.CacheOptions{
		MaxTTL: placementCacheMaxTTL,
	})
	defer h.placementCache.Stop()

	// Advertise the registered actor types to the runtime at registration time
	h.runtimeClient.cfg.actorTypes = componentsActorTypesToProtocol(h.core.RegisteredActorTypes())

	// Use the runtime client's logger once it learns the host ID
	h.log = h.logSource

	// Before returning, halt all remaining actors so they deactivate cleanly
	defer func() {
		haltErr := h.HaltAll()
		if haltErr != nil {
			h.log.Warn("Error halting actors", slog.Any("error", haltErr))
		}
	}()

	// Run all services
	// This blocks until the context is canceled or one of the services returns
	return servicerunner.
		NewServiceRunner(
			// Maintain the persistent session to the runtime, reconnecting as needed
			h.runtimeClient.Run,

			// Serve host-to-host invocations of actors owned by this host
			h.peerServer.Run,
		).
		Run(ctx)
}

// Ready returns a channel that is closed once the host has registered with a runtime for the first time
func (h *Host) Ready() <-chan struct{} {
	return h.runtimeClient.Ready()
}

// HostID returns the current runtime-assigned ID of the host, or empty if not yet registered.
func (h *Host) HostID() string {
	return h.runtimeClient.HostID()
}

// HaltAll halts all actors active on the host, gracefully
func (h *Host) HaltAll() error {
	return h.core.HaltAll()
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
