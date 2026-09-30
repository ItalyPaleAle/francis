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
	"github.com/italypaleale/go-kit/utils"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/internal/peer"
	"github.com/italypaleale/francis/internal/providerfactory"
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
	hostIDLock sync.RWMutex
	hostID     string

	running  atomic.Bool
	draining atomic.Bool
	// drainPersisted is set once the draining flag was written to the provider for the current registration
	drainPersisted atomic.Bool
	// adminDrain tracks an administrative drain, which stops Run and keeps the host from running again
	adminDrain actorcore.AdminDrain

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
	// management serves the management API, nil when it is disabled
	management *management.Server

	// cas is the cluster CA bundle derived from the runtime PSKs, index 0 being the primary used to self-issue this host's certificate
	cas []*ca.CA
	// holder stores this host's self-issued workload certificate and the trust bundle, read live by the peer TLS configs
	holder *certholder.Holder

	alarmProcessor *eventqueue.Processor[string, *ref.AlarmLease]
	// alarmsDraining is protected by activeAlarmsLock so no alarm execution can increment alarmWg after shutdown starts waiting
	alarmsDraining bool
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
		MaxHosts:                  o.MaxHosts,
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
	options.BindPort = utils.PositiveOr(options.BindPort, addrPort)
	options.ShutdownGracePeriod = utils.PositiveOr(options.ShutdownGracePeriod, defaultShutdownGracePeriod)
	options.ProviderRequestTimeout = utils.PositiveOr(options.ProviderRequestTimeout, defaultProviderRequestTimeout)
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
	options.AlarmsFetchAheadBatchSize = utils.PositiveOr(options.AlarmsFetchAheadBatchSize, defaultAlarmsFetchAheadBatch)

	// Init a real clock if none is passed
	if options.clock == nil {
		options.clock = &clock.RealClock{}
	}

	// Get the provider
	actorProvider, err := providerfactory.New(options.Logger, options.ProviderOptions, options.getProviderConfig())
	if err != nil {
		return nil, err
	}

	// The provider is owned by the host from here on, so it must be released if the host cannot be created
	defer func() {
		if err != nil {
			_ = actorProvider.Close()
		}
	}()

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
		ManagementHandler:   h.handleManagement,
		MaxInFlightRequests: options.MaxInFlightRequests,
		MaxRequestBodySize:  options.MaxRequestBodySize,
	})

	// Create the management API server when it is enabled
	if options.Management != nil {
		h.management, err = h.newManagementServer(*options.Management, options.Logger.With(slog.String("scope", "management")))
		if err != nil {
			return nil, fmt.Errorf("failed to create management API server: %w", err)
		}
	}

	return h, nil
}

// Service returns a Service object configured to interact with this host.
func (h *Host) Service() *actor.Service {
	return h.service
}

// Run the host service.
// Note this function is blocking, and will return only when the service is shut down via context cancellation, when a service fails, or after an administrative drain.
// After an administrative drain the returned error matches host.ErrAdministrativeDrain, and the host cannot be run again.
func (h *Host) Run(parentCtx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return errors.New("service is already running")
	}
	defer h.running.Store(false)

	// A drained host is gone for good
	if !h.adminDrain.Start() {
		return host.ErrAdministrativeDrain
	}
	defer h.adminDrain.Stopped()

	// An administrative drain stops Run by canceling its context, which leads into the graceful teardown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	h.adminDrain.SetStop(cancel)

	// A new registration starts out not draining
	h.draining.Store(false)
	h.drainPersisted.Store(false)

	// Start the actor core (idle processor) and the placement cache
	h.core.Start()
	defer h.core.Close()

	h.placementCache = ttlcache.NewCache[string, *actorcore.Placement](&ttlcache.CacheOptions{
		MaxTTL: placementCacheMaxTTL,
	})
	defer h.placementCache.Stop()

	// Tear down pooled outbound peer sessions once the host stops serving
	defer h.peerClient.Close()

	// Release the provider's resources, including the database connection it established, once the host stops
	// The host built the provider in NewHost, so it owns it
	defer func() {
		closeErr := h.actorProvider.Close()
		if closeErr != nil {
			h.logSource.Warn("Error closing actor provider", slog.Any("error", closeErr))
		}
	}()

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

	h.setHostID(res.HostID)
	h.log = h.logSource.With(slog.String("hostId", res.HostID))
	h.core.SetLogger(h.log)

	// Self-issue this host's workload certificate now that its ID is known, so the peer server can present it and peers can verify it
	err = h.issueSelfCert()
	if err != nil {
		return fmt.Errorf("failed to issue workload certificate: %w", err)
	}

	h.log.InfoContext(ctx, "Registered actor host", slog.String("address", h.address))

	// Create the alarm processor before exposing readiness so newly-created alarms can be enqueued immediately
	h.activeAlarmsLock.Lock()
	h.alarmsDraining = false
	h.alarmProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ref.AlarmLease]{
		ExecuteFn: h.executeAlarm,
	})
	h.activeAlarmsLock.Unlock()

	// Signal readiness now that the host is registered and its fields are initialized
	// Closing the channel also publishes those writes to any goroutine that waits on Ready
	h.readyOnce.Do(func() { close(h.ready) })

	// Bootstrap the singleton actors now that the host can serve invocations
	if len(h.core.SingletonActorTypes()) > 0 {
		go h.core.BootstrapSingletons(ctx)
	}

	// Set the draining flag as soon as the context is canceled so the peer server rejects new invocations with a retry-later error before any actors are halted, giving callers a chance to re-resolve
	// A drain request that arrives from now on finds the host already stopping
	go func() {
		<-ctx.Done()
		h.draining.Store(true)
		h.adminDrain.BeginStopping()
	}()

	// The peer server runs under its own context, so it keeps serving while local actors drain and the host unregisters
	// Throughout that window it rejects new invocations with a retry-later error, and it can still write the reply to an administrative drain
	// Registered before the unregister and halt defers, so it runs after both (LIFO)
	stopPeer, watchPeer := runDetached(parentCtx, "peer server", h.peerServer.Run)
	defer stopPeer()

	// Upon returning, we unregister the host so it can be removed cleanly
	// If the application crashes and this code isn't executed, eventually the host will be removed for not sending health checks periodically
	// Registered before the health check and halt defers, so it runs after both (LIFO): actors must be halted before the host registration is removed
	defer func() {
		// Use a background context here as the parent one is likely canceled at this point
		unregisterCtx, unregisterCancel := context.WithTimeout(context.Background(), h.providerRequestTimeout)
		defer unregisterCancel()

		unregisterErr := h.actorProvider.UnregisterHost(unregisterCtx, res.HostID, components.UnregisterHostOpts{})
		if unregisterErr != nil {
			h.log.WarnContext(unregisterCtx, "Error unregistering actor host", slog.Any("error", unregisterErr))
			return
		}

		h.log.InfoContext(ctx, "Unregistered actor host")
		h.setHostID("")
	}()

	// Health checks run under their own context too, and stop only once the actors have halted
	// Halting can take longer than the health check deadline, and a registration that expired meanwhile would let other hosts activate actors that are still halting here
	// Registered between the unregister and halt defers, so it runs after the halt and before the unregister (LIFO)
	stopHealthChecks, watchHealthChecks := runDetached(parentCtx, "health checks", h.runHealthChecks)
	defer stopHealthChecks()

	// Halt all remaining actors before the host unregisters
	// Registered last so it runs first (LIFO): actors are halted before health checks stop and the provider record is removed
	defer func() {
		// Mark the host draining in the provider first, so no new actor is placed on it while the others halt
		h.draining.Store(true)
		h.persistDraining()

		// An administrative drain bounds the wait, while a regular shutdown waits for every actor
		_, timeout := h.adminDrain.BeginStopping()
		h.core.DrainAll(timeout)
	}()

	services := []servicerunner.Service{
		// Stop the host if health checks, which run on their own context, fail
		watchHealthChecks,

		// Run the alarm fetcher in background
		h.runAlarmFetcher,

		// In background also renew leases
		h.runLeaseRenewal,

		// Stop the host if the peer server, which runs on its own context, exits early
		watchPeer,

		// Run the actor provider
		h.actorProvider.Run,
	}

	// Serve the management API when it is enabled
	if h.management != nil {
		services = append(services, h.management.Run)
	}

	// Run all services
	// This blocks until the context is canceled or one of the services returns
	runErr := servicerunner.
		NewServiceRunner(services...).
		Run(ctx)

	// Report an administrative drain, which is the reason the services stopped
	if h.adminDrain.Accepted() {
		return errors.Join(host.ErrAdministrativeDrain, runErr)
	}
	return runErr
}

// issueSelfCert generates a key pair and signs this host's workload certificate from the primary CA, installing it in the holder
func (h *Host) issueSelfCert() error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate workload key: %w", err)
	}

	hostID := h.HostID()
	der, err := h.cas[0].IssueWorkloadCert(ca.HostURI(hostID), pub, localCertTTL)
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

// persistDraining marks the host draining in the provider, once per registration, so no new actor is placed on it
// A failure is logged and otherwise ignored, since the host is shutting down either way and its registration is removed shortly after
func (h *Host) persistDraining() {
	if !h.drainPersisted.CompareAndSwap(false, true) {
		return
	}

	hostID := h.HostID()
	if hostID == "" {
		return
	}

	// Use a background context, as the host's own context is likely canceled at this point
	ctx, cancel := context.WithTimeout(context.Background(), h.providerRequestTimeout)
	defer cancel()
	err := h.actorProvider.UpdateActorHost(ctx, hostID, components.UpdateActorHostReq{Draining: true})
	switch {
	case errors.Is(err, components.ErrHostUnregistered):
		// The registration is already gone, so there is nothing to mark
		h.log.Debug("Host registration is gone; not marking it draining")
	case err != nil:
		h.log.Warn("Error marking the host draining", slog.Any("error", err))
	}
}

// runDetached runs fn under a context that is not canceled with the host's, so it can keep going during the graceful teardown
// It returns a function that stops fn and waits for it to return, and a service that stops the host if fn returns before the host stops
func runDetached(parentCtx context.Context, name string, fn func(ctx context.Context) error) (stop func(), watch servicerunner.Service) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parentCtx))
	done := make(chan struct{})
	var err error
	go func() {
		err = fn(ctx)
		close(done)
	}()

	stop = func() {
		cancel()
		<-done
	}
	watch = func(ctx context.Context) error {
		select {
		case <-done:
			if err != nil {
				return fmt.Errorf("%s stopped: %w", name, err)
			}
			return fmt.Errorf("%s stopped unexpectedly", name)
		case <-ctx.Done():
			return nil
		}
	}
	return stop, watch
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
	h.hostIDLock.RLock()
	defer h.hostIDLock.RUnlock()
	return h.hostID
}

func (h *Host) setHostID(hostID string) {
	h.hostIDLock.Lock()
	h.hostID = hostID
	h.hostIDLock.Unlock()
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

	// Create one stateful policy that the retry loop resets before every sequence
	policy := h.actorProvider.HealthCheckPolicy()
	h.log.DebugContext(parentCtx, "Starting background health checks", slog.Any("interval", policy.Interval()))
	defer h.log.Debug("Stopped background health checks")

	// Start the periodic schedule early enough to leave the policy's full retry budget before expiry
	t := h.clock.NewTicker(policy.Interval())
	defer t.Stop()

	// Let the policy own retry exhaustion so its attempt counter and exponential delays stay together
	retryOpts := []backoff.RetryOption{
		backoff.WithBackOff(policy),
		backoff.WithNotify(func(err error, _ time.Duration) {
			h.log.WarnContext(parentCtx, "Health check error, will retry", slog.Any("error", err))
		}),
	}

	// Run one bounded attempt sequence on each scheduled health check
	for {
		select {
		case <-t.C():
			h.log.DebugContext(parentCtx, "Sending health check to the provider")
			_, err = backoff.Retry(parentCtx, func() (r struct{}, rErr error) {
				ctx, cancel := context.WithTimeout(parentCtx, policy.AttemptTimeout())
				defer cancel()
				hostID := h.HostID()
				rErr = h.actorProvider.UpdateActorHost(ctx, hostID, components.UpdateActorHostReq{
					UpdateLastHealthCheck: true,
					Retry:                 policy.Attempts() > 0,
				})
				switch {
				case errors.Is(rErr, components.ErrHostUnregistered):
					// Registration has expired, so no point in retrying anymore
					return r, backoff.Permanent(rErr)
				case rErr != nil:
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
