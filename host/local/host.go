package local

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
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
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/peer"
	"github.com/italypaleale/francis/internal/ref"
)

// localCertTTL is the lifetime of a self-issued workload certificate in local mode
// Local hosts issue their own certificate from the shared CA, so a long lifetime avoids a renewal path while the host process is alive
const localCertTTL = 90 * 24 * time.Hour

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

type (
	// SQLiteProviderOptions re-exports provider options
	SQLiteProviderOptions = sqlite.SQLiteProviderOptions

	// PostgresProviderOptions re-exports provider options
	PostgresProviderOptions = postgres.PostgresProviderOptions

	// StandaloneMemoryProviderOptions re-exports provider options
	StandaloneMemoryProviderOptions = standalone.StandaloneMemoryOptions

	// StandaloneSQLiteProviderOptions re-exports provider options
	StandaloneSQLiteProviderOptions = standalone.StandaloneSQLiteOptions

	// StandalonePostgresProviderOptions re-exports provider options
	StandalonePostgresProviderOptions = standalone.StandalonePostgresOptions
)

// Host is an actor host.
type Host struct {
	// Address the host is reachable at
	address string

	// Host ID for the registered host
	hostID string

	running  atomic.Bool
	draining atomic.Bool

	actorProvider components.ActorProvider
	service       *actor.Service
	core          *actorcore.Manager
	// singletonActors holds the reserved actor types of singleton actors registered before start, whose singleton instance the host bootstraps once ready
	singletonActors []string
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

	// cas is the cluster CA bundle derived from the runtime PSKs, index 0 being the primary used to self-issue this host's certificate
	cas []*ca.CA
	// holder stores this host's self-issued workload certificate and the trust bundle, read live by the peer TLS configs
	holder *certholder.Holder

	alarmProcessor *eventqueue.Processor[string, *ref.AlarmLease]
	// alarmWg counts all in-flight alarm goroutines so shutdown can wait for them to finish
	alarmWg sync.WaitGroup

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
	case postgres.PostgresProviderOptions:
		actorProvider, err = postgres.NewPostgresProvider(options.Logger, x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create Postgres provider: %w", err)
		}
	case *postgres.PostgresProviderOptions:
		actorProvider, err = postgres.NewPostgresProvider(options.Logger, *x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create Postgres provider: %w", err)
		}
	case standalone.StandaloneMemoryOptions:
		actorProvider, err = standalone.NewStandaloneMemory(options.Logger, x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone memory provider: %w", err)
		}
	case standalone.StandaloneSQLiteOptions:
		actorProvider, err = standalone.NewStandaloneSQLiteBacked(options.Logger, x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone SQLite provider: %w", err)
		}
	case standalone.StandalonePostgresOptions:
		actorProvider, err = standalone.NewStandalonePostgresBacked(options.Logger, x, options.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create standalone Postgres provider: %w", err)
		}
	case nil:
		return nil, errors.New("option ProviderOptions is required")
	default:
		return nil, fmt.Errorf("unsupported value for ProviderOptions: %T", options.ProviderOptions)
	}

	// Derive the cluster CA from the runtime PSKs so hosts that share the PSKs authenticate each other with mTLS
	if len(options.RuntimePSKs) == 0 {
		return nil, errors.New("option RuntimePSKs is required")
	}
	cas, err := ca.CABundle(options.RuntimePSKs)
	if err != nil {
		return nil, fmt.Errorf("failed to derive cluster CA: %w", err)
	}

	// The holder starts with the trust bundle, and this host's certificate is self-issued once its ID is known at registration
	holder := certholder.New(nil, ca.NewCertPool(cas))

	// Create the host
	h = &Host{
		address:                options.Address,
		actorProvider:          actorProvider,
		cas:                    cas,
		holder:                 holder,
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

	// The peer client invokes actors owned by other hosts over WebTransport, presenting this host's workload certificate for mutual authentication
	h.peerClient = peer.NewClient(peer.ClientConfig{
		TLSConfig:   hosttls.PeerClientTLSConfig(holder),
		DialTimeout: options.ProviderRequestTimeout,
		Log:         options.Logger,
	})

	// The peer server serves invocations of actors owned by this host over WebTransport
	// It reports our host ID so it can reject invocations aimed at a stale placement, and requires a host certificate from every caller
	// Draining is wired so callers receive a retry-later response rather than a hard reset during graceful shutdown
	h.peerServer = peer.NewServer(peer.ServerConfig{
		Bind:                h.bind,
		TLSConfig:           hosttls.PeerServerTLSConfig(holder),
		Handler:             h.peerInvokeObject,
		StreamHandler:       h.peerInvokeStream,
		Log:                 options.Logger,
		HostID:              h.HostID,
		Draining:            func() bool { return h.draining.Load() },
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

	// Self-issue this host's workload certificate now that its ID is known, so the peer server can present it and peers can verify it
	err = h.issueSelfCert()
	if err != nil {
		return fmt.Errorf("failed to issue workload certificate: %w", err)
	}

	h.log.InfoContext(ctx, "Registered actor host", slog.String("address", h.address))

	// Signal readiness now that the host is registered and its fields are initialized
	// Closing the channel also publishes those writes to any goroutine that waits on Ready
	h.readyOnce.Do(func() { close(h.ready) })

	// Bootstrap the singleton actors now that the host can serve invocations
	// Bootstrap runs on the cluster-wide singleton instance, so it is harmless for every host to do this
	if len(h.singletonActors) > 0 {
		go h.bootstrapSingletonActors(ctx)
	}

	// Set the draining flag as soon as the context is canceled so the peer server rejects new invocations with a retry-later error before any actors are halted, giving callers a chance to re-resolve
	go func() {
		<-ctx.Done()
		h.draining.Store(true)
	}()

	// Upon returning, we unregister the host so it can be removed cleanly
	// If the application crashes and this code isn't executed, eventually the host will be removed for not sending health checks periodically
	// Registered first so it runs second (LIFO): actors must be halted before the host registration is removed
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

	// Halt all remaining actors before the host unregisters
	// Registered second so it runs first (LIFO): actors are halted before the provider record is removed
	defer func() {
		haltErr := h.HaltAll()
		if haltErr != nil {
			h.log.Warn("Error halting actors", slog.Any("error", haltErr))
		}
	}()

	// Create the alarm processor here before the services start
	// The close is handled by runAlarmFetcher's defer, which intentionally leaves the field non-nil so in-flight re-enqueues receive ErrProcessorStopped instead of a nil-pointer panic
	h.alarmProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		ExecuteFn: h.executeAlarm,
	})

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

// issueSelfCert generates a key pair and signs this host's workload certificate from the primary CA, installing it in the holder
func (h *Host) issueSelfCert() error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate workload key: %w", err)
	}

	der, err := h.cas[0].IssueWorkloadCert(ca.HostURI(h.hostID), pub, localCertTTL)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return fmt.Errorf("failed to parse issued certificate: %w", err)
	}

	h.holder.SetCertificate(&tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        leaf,
	})

	return nil
}

// HaltAll halts all actors active on the host, gracefully
func (h *Host) HaltAll() error {
	return h.core.HaltAll()
}

// bootstrapSingletonActors drives the Bootstrap hook of each registered singleton actor once the host is ready
// It invokes the reserved bootstrap lifecycle on the singleton instance through the privileged client, which routes to the owning host and serializes on that instance's turn lock
// It retries with a short backoff because an invocation can briefly fail right after startup (Bootstrap is idempotent, so retrying is safe)
func (h *Host) bootstrapSingletonActors(ctx context.Context) {
	const maxAttempts = 5
	for _, at := range h.singletonActors {
		// The privileged client is allowed to target reserved built-in types and to send the reserved bootstrap method, both of which the public client rejects
		client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, at, actor.SingletonActorID, h.service)
		for i := 1; ; i++ {
			invokeCtx, cancel := context.WithTimeout(ctx, h.providerRequestTimeout)
			_, err := client.Invoke(invokeCtx, at, actor.SingletonActorID, ref.MethodBootstrap, nil)
			cancel()
			if err == nil {
				h.log.DebugContext(ctx, "Bootstrapped singleton actor", slog.String("actorType", at))
				break
			}

			if i >= maxAttempts || ctx.Err() != nil {
				h.log.WarnContext(ctx, "Failed to bootstrap singleton actor", slog.String("actorType", at), slog.Any("error", err))
				break
			}

			// Back off before the next attempt, but stop promptly if the host is shutting down
			t := h.clock.NewTimer(time.Duration(i) * 500 * time.Millisecond)
			select {
			case <-t.C():
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}
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
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

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
