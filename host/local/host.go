package local

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	backoff "github.com/cenkalti/backoff/v5"
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/peer"
	"github.com/italypaleale/francis/internal/peerauth"
	"github.com/italypaleale/francis/internal/ref"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

const (
	defaultShutdownGracePeriod      = 30 * time.Second
	defaultProviderRequestTimeout   = 15 * time.Second
	defaultHostHealthCheckDeadline  = 20 * time.Second
	defaultAlarmsPollInterval       = 1500 * time.Millisecond
	defaultAlarmsLeaseDuration      = 20 * time.Second
	defaultAlarmsFetchAheadInterval = 2500 * time.Millisecond
	defaultAlarmsFetchAheadBatch    = 25
)

// SQLiteProviderOptions re-exports provider options
type SQLiteProviderOptions = sqlite.SQLiteProviderOptions

// Host is an actor host.
type Host struct {
	// Address the host is reachable at
	address string

	// Host ID for the registered host
	hostID string

	running       atomic.Bool
	actorProvider components.ActorProvider
	service       *actor.Service
	core          *actorcore.Manager
	// resolver adapts this host to the placement resolver the shared messaging logic depends on
	resolver actorcore.PlacementResolver

	// ready is closed once the host has registered and is safe to invoke
	// It also establishes a happens-before edge so a caller that waits on it observes the fully-initialized host
	readyOnce sync.Once
	ready     chan struct{}

	// peerClient invokes actors owned by other hosts over WebTransport
	peerClient *peer.Client
	// peerServer serves invocations of actors owned by this host over WebTransport
	peerServer *peer.Server

	alarmProcessor *eventqueue.Processor[string, *ref.AlarmLease]

	// Actor placement cache
	placementCache *ttlcache.Cache[string, *actorcore.Placement]

	// List of currently-active alarms
	activeAlarmsLock sync.Mutex
	activeAlarms     map[string]struct{}
	retryingAlarms   map[string]struct{}

	bind                   string
	alarmsPollInterval     time.Duration
	providerRequestTimeout time.Duration
	shutdownGracePeriod    time.Duration

	logSource *slog.Logger
	log       *slog.Logger
	clock     clock.WithTicker
}

func (o newHostOptions) getProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   o.HostHealthCheckDeadline,
		AlarmsLeaseDuration:       o.AlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  o.AlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: o.AlarmsFetchAheadBatchSize,
	}
}

// NewHost returns a new actor host.
func NewHost(opts ...HostOption) (h *Host, err error) {
	options := &newHostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return newHost(options)
}

func newHost(options *newHostOptions) (h *Host, err error) {
	// Validate the address
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
	if options.ProviderRequestTimeout <= 0 {
		options.ProviderRequestTimeout = defaultProviderRequestTimeout
	}
	if options.HostHealthCheckDeadline < time.Second {
		options.HostHealthCheckDeadline = defaultHostHealthCheckDeadline
	}
	if options.AlarmsPollInterval <= 100*time.Millisecond {
		options.AlarmsPollInterval = defaultAlarmsPollInterval
	}
	if options.AlarmsLeaseDuration < time.Second {
		options.AlarmsLeaseDuration = defaultAlarmsLeaseDuration
	}
	if options.AlarmsFetchAheadInterval < 100*time.Millisecond {
		options.AlarmsFetchAheadInterval = defaultAlarmsFetchAheadInterval
	}
	if options.AlarmsFetchAheadBatchSize <= 0 {
		options.AlarmsFetchAheadBatchSize = defaultAlarmsFetchAheadBatch
	}

	// Init a real clock if none is passed
	if options.clock == nil {
		options.clock = &clock.RealClock{}
	}

	// Get the provider
	var actorProvider components.ActorProvider
	switch x := options.ProviderOptions.(type) {
	case sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(options.Logger, x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case *sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(options.Logger, *x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case nil:
		return nil, errors.New("option ProviderOptions is required")
	default:
		return nil, fmt.Errorf("unsupported value for ProviderOptions: %T", options.ProviderOptions)
	}

	// Get the peer authentication method
	switch x := options.PeerAuthentication.(type) {
	case *peerauth.PeerAuthenticationSharedKey:
		err = x.Validate()
		if err != nil {
			return nil, fmt.Errorf("failed to validate PeerAuthenticationSharedKey: %w", err)
		}
	case *peerauth.PeerAuthenticationMTLS:
		err = x.Validate()
		if err != nil {
			return nil, fmt.Errorf("failed to validate PeerAuthenticationMTLS: %w", err)
		}

		// Cannot set certain TLSOption when peer authentication uses mTLS
		if options.TLSOptions.CACertificate != nil {
			return nil, errors.New("cannot set TLSOptions.CACertificate when peer authentication is mTLS")
		}
		if options.TLSOptions.ServerCertificate != nil {
			return nil, errors.New("cannot set TLSOptions.ServerCertificate when peer authentication is mTLS")
		}
		if options.TLSOptions.ClientCertificate != nil {
			return nil, errors.New("cannot set TLSOptions.ClientCertificate when peer authentication is mTLS")
		}
		if options.TLSOptions.InsecureSkipTLSValidation {
			return nil, errors.New("cannot set TLSOptions.InsecureSkipTLSValidation when peer authentication is mTLS")
		}

		// Update the TLS options
		x.SetTLSOptions(&options.TLSOptions)
	case nil:
		return nil, errors.New("option PeerAuthentication is required")
	default:
		return nil, fmt.Errorf("unsupported value for PeerAuthentication: %T", options.PeerAuthentication)
	}

	// Init the TLS configuration for the server and client
	serverTLSConfig, clientTLSConfig, err := options.TLSOptions.GetTLSConfig()
	if err != nil {
		return nil, err
	}

	// Create the host
	h = &Host{
		address:                options.Address,
		actorProvider:          actorProvider,
		activeAlarms:           map[string]struct{}{},
		retryingAlarms:         map[string]struct{}{},
		alarmsPollInterval:     options.AlarmsPollInterval,
		shutdownGracePeriod:    options.ShutdownGracePeriod,
		providerRequestTimeout: options.ProviderRequestTimeout,
		bind:                   net.JoinHostPort(options.BindAddress, strconv.Itoa(options.BindPort)),
		logSource:              options.Logger,
		clock:                  options.clock,
		ready:                  make(chan struct{}),
	}
	h.service = actor.NewService(h)

	// The actor core owns activation, turn-based invocation, idle deactivation, and halting
	// On deactivation it removes the actor from the provider
	h.core = actorcore.NewManager(actorcore.Options{
		Service:                h.service,
		RemoveActor:            actorProvider.RemoveActor,
		Logger:                 options.Logger,
		Clock:                  options.clock,
		ProviderRequestTimeout: options.ProviderRequestTimeout,
		ShutdownGracePeriod:    options.ShutdownGracePeriod,
	})

	// The resolver lets the shared messaging logic resolve placement through the provider and confirm ownership before activating an actor
	h.resolver = placementResolver{h: h}

	// The peer client invokes actors owned by other hosts over WebTransport, authenticating the session at establishment
	h.peerClient = peer.NewClient(peer.ClientConfig{
		TLSConfig:   clientTLSConfig,
		DialTimeout: options.ProviderRequestTimeout,
		Auth:        options.PeerAuthentication,
		Log:         options.Logger,
	})

	// The peer server serves invocations of actors owned by this host over WebTransport
	// It reports our host ID so it can reject invocations aimed at a stale placement, and authorizes incoming sessions
	h.peerServer = peer.NewServer(peer.ServerConfig{
		Bind:                h.bind,
		TLSConfig:           serverTLSConfig,
		Handler:             h.peerInvokeObject,
		StreamHandler:       h.peerInvokeStream,
		Auth:                options.PeerAuthentication,
		Log:                 options.Logger,
		HostID:              h.HostID,
		MaxInFlightRequests: options.MaxInFlightRequests,
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

	// Perform provider initialization steps
	initCtx, initCancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	err := h.actorProvider.Init(initCtx)
	initCancel()
	if err != nil {
		return fmt.Errorf("failed to init provider: %w", err)
	}

	// Register the host
	registerCtx, registerCancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	res, err := h.actorProvider.RegisterHost(registerCtx, components.RegisterHostReq{
		Address:    h.address,
		ActorTypes: h.core.RegisteredActorTypes(),
	})
	registerCancel()
	if err != nil {
		return fmt.Errorf("failed to register actor host: %w", err)
	}

	h.hostID = res.HostID
	h.log = h.logSource.With(slog.String("hostId", h.hostID))
	h.core.SetLogger(h.log)

	h.log.InfoContext(ctx, "Registered actor host", slog.String("address", h.address))

	// Signal readiness now that the host is registered and its fields are initialized
	// Closing the channel also publishes those writes to any goroutine that waits on Ready
	h.readyOnce.Do(func() { close(h.ready) })

	// Before returning, we halt all remaining actors
	defer func() {
		haltErr := h.HaltAll()
		if haltErr != nil {
			h.log.Warn("Error halting actors", slog.Any("error", haltErr))
		}
	}()

	// Upon returning, we unregister the host so it can be removed cleanly
	// If the application crashes and this code isn't executed, eventually the host will be removed for not sending health checks periodically
	defer func() {
		// Use a background context here as the parent one is likely canceled at this point
		unregisterCtx, unregisterCancel := context.WithTimeout(context.Background(), h.providerRequestTimeout)
		defer unregisterCancel()

		unregisterErr := h.actorProvider.UnregisterHost(unregisterCtx, res.HostID)
		if unregisterErr != nil {
			h.log.WarnContext(unregisterCtx, "Error unregistering actor host", slog.Any("error", unregisterErr))
			return
		}

		h.log.InfoContext(ctx, "Unregistered actor host")
		h.hostID = ""
	}()

	// Run all services
	// This blocks until the context is canceled or one of the services returns
	return servicerunner.
		NewServiceRunner(
			// Perform health checks in background
			h.runHealthChecks,

			// Run the alarm fetcher in background
			h.runAlarmFetcher,

			// In background also renew leases
			h.runLeaseRenewal,

			// Run the peer server that allows receiving invocations from other hosts
			h.peerServer.Run,

			// Run the actor provider
			h.actorProvider.Run,
		).
		Run(ctx)
}

// HaltAll halts all actors active on the host, gracefully
func (h *Host) HaltAll() error {
	return h.core.HaltAll()
}

// Ready returns a channel that is closed once the host has registered and is safe to invoke.
func (h *Host) Ready() <-chan struct{} {
	return h.ready
}

// HostID returns the ID of the host.
func (h *Host) HostID() string {
	return h.hostID
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

func (h *Host) runHealthChecks(parentCtx context.Context) error {
	var err error

	// Perform periodic health checks
	interval := h.actorProvider.HealthCheckInterval()
	h.log.DebugContext(parentCtx, "Starting background health checks", slog.Any("interval", interval))
	defer h.log.Debug("Stopped background health checks")

	t := h.clock.NewTicker(interval)
	defer t.Stop()

	retryOpts := []backoff.RetryOption{
		backoff.WithBackOff(backoff.NewConstantBackOff(500 * time.Millisecond)),
		backoff.WithMaxTries(3),
	}

	for {
		select {
		case <-t.C():
			h.log.DebugContext(parentCtx, "Sending health check to the provider")
			_, err = backoff.Retry(parentCtx, func() (r struct{}, rErr error) {
				ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
				defer cancel()
				rErr = h.actorProvider.UpdateActorHost(ctx, h.hostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
				switch {
				case errors.Is(rErr, components.ErrHostUnregistered):
					// Registration has expired, so no point in retrying anymore
					return r, backoff.Permanent(rErr)
				case rErr != nil:
					h.log.WarnContext(parentCtx, "Health check error; will retry", slog.Any("error", rErr))
					return r, rErr
				default:
					return r, nil
				}
			}, retryOpts...)
			if err != nil {
				h.log.ErrorContext(parentCtx, "Health check failed", slog.Any("error", err))
				return fmt.Errorf("failed to perform health check: %w", err)
			}
		case <-parentCtx.Done():
			// Stop when the context is canceled
			return parentCtx.Err()
		}
	}
}
