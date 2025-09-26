package host

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	backoff "github.com/cenkalti/backoff/v5"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/activeactor"
	"github.com/italypaleale/francis/internal/eventqueue"
	"github.com/italypaleale/francis/internal/peerauth"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/servicerunner"
	"github.com/italypaleale/francis/internal/ttlcache"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

const (
	defaultShutdownGracePeriod      = 30 * time.Second
	defaultActorsMapSize            = 128 // Must be a power of 2
	defaultProviderRequestTimeout   = 15 * time.Second
	defaultHostHealthCheckDeadline  = 20 * time.Second
	defaultAlarmsPollInterval       = 1500 * time.Millisecond
	defaultAlarmsLeaseDuration      = 20 * time.Second
	defaultAlarmsFetchAheadInterval = 2500 * time.Millisecond
	defaultAlarmsFetchAheadBatch    = 25

	// If an idle actor is getting deactivated, but it's still busy, will be re-enqueued with its idle timeout increased by this duration
	actorBusyReEnqueueInterval = 10 * time.Second
)

// Host is an actor host.
type Host struct {
	// Address the host is reachable at
	address string

	// Host ID for the registered host
	hostID string

	running       atomic.Bool
	actorProvider components.ActorProvider
	service       *actor.Service
	client        *http.Client

	// Actor factory methods; key is actor type
	actorFactories map[string]actor.Factory

	// Active actors; key is "actorType/actorID"
	actors             *haxmap.Map[string, *activeactor.Instance]
	idleActorProcessor *eventqueue.Processor[string, *activeactor.Instance]
	alarmProcessor     *eventqueue.Processor[string, *ref.AlarmLease]

	// Actor placement cache
	placementCache *ttlcache.Cache[*actorPlacement]

	// Map of actor configuration objects; key is actor type
	actorsConfig map[string]components.ActorHostType

	// List of currently-active alarms
	activeAlarmsLock sync.Mutex
	activeAlarms     map[string]struct{}
	retryingAlarms   map[string]struct{}

	bind                   string
	serverTLSConfig        *tls.Config
	peerAuth               peerauth.PeerAuthenticationMethod
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
		actorsConfig:           map[string]components.ActorHostType{},
		actorFactories:         map[string]actor.Factory{},
		actors:                 haxmap.New[string, *activeactor.Instance](defaultActorsMapSize),
		activeAlarms:           map[string]struct{}{},
		retryingAlarms:         map[string]struct{}{},
		alarmsPollInterval:     options.AlarmsPollInterval,
		shutdownGracePeriod:    options.ShutdownGracePeriod,
		providerRequestTimeout: options.ProviderRequestTimeout,
		peerAuth:               options.PeerAuthentication,
		bind:                   net.JoinHostPort(options.BindAddress, strconv.Itoa(options.BindPort)),
		logSource:              options.Logger,
		serverTLSConfig:        serverTLSConfig,
		clock:                  options.clock,
	}
	h.service = actor.NewService(h)

	// Init the HTTP client for the host
	// This is configured with HTTP3 support
	// TODO: Allow passing a client that is configured with OTel tracing
	h.client = &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: clientTLSConfig,
			QUICConfig:      &quic.Config{},
		},
	}

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

	// Init idle processor and ttlcache
	h.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeactor.Instance]{
		Clock:     h.clock,
		ExecuteFn: h.handleIdleActor,
	})
	defer h.idleActorProcessor.Close()

	h.placementCache = ttlcache.NewCache[*actorPlacement](&ttlcache.CacheOptions{
		MaxTTL: placementCacheMaxTTL,
	})
	defer h.placementCache.Stop()

	// Perform provider initialization steps
	initCtx, initCancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	err := h.actorProvider.Init(initCtx)
	initCancel()
	if err != nil {
		return fmt.Errorf("failed to init provider: %w", err)
	}

	// Register the host
	actorsConfigList := make([]components.ActorHostType, len(h.actorsConfig))
	var i int
	for _, ac := range h.actorsConfig {
		actorsConfigList[i] = ac
		i++
	}

	registerCtx, registerCancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	res, err := h.actorProvider.RegisterHost(registerCtx, components.RegisterHostReq{
		Address:    h.address,
		ActorTypes: actorsConfigList,
	})
	registerCancel()
	if err != nil {
		return fmt.Errorf("failed to register actor host: %w", err)
	}

	h.hostID = res.HostID
	h.log = h.logSource.With(slog.String("hostId", h.hostID))

	h.log.InfoContext(ctx, "Registered actor host", slog.String("address", h.address))

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

			// Run the server that allows receiving requests from other nodes
			h.runServer,

			// Run the actor provider
			h.actorProvider.Run,
		).
		Run(ctx)
}

// HaltAll halts all actors, gracefully
func (h *Host) HaltAll() error {
	// Deactivate all actors, each in its own goroutine
	errCh := make(chan error)
	var count int
	for _, act := range h.actors.Iterator() {
		count++
		go func(act *activeactor.Instance) {
			err := h.haltActiveActor(act, true)
			if err != nil {
				err = fmt.Errorf("failed to halt actor '%s': %w", act.Key(), err)
			}
			errCh <- err
		}(act)
	}

	// Collect all errors
	errs := make([]error, 0)
	for range count {
		err := <-errCh
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error halting actors: %w", errors.Join(errs...))
	}

	return nil
}

// HostID returns the ID of the host.
func (h *Host) HostID() string {
	return h.hostID
}

// Halt gracefully halts an actor that is hosted on the current host
func (h *Host) Halt(actorType string, actorID string) error {
	// Get the active actor object
	// Only actors that are active on the current host can be halted here
	aRef := ref.NewActorRef(actorType, actorID)
	act, ok := h.actors.Get(aRef.String())
	if !ok || act == nil {
		return actor.ErrActorNotHosted
	}

	// Gracefully halt the actor
	err := h.haltActiveActor(act, true)
	if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	return nil
}

// HaltDeferred gracefully halts an actor that is hosted on the current host
// This is a non-blocking variant of the Halt method, which runs in background
func (h *Host) HaltDeferred(actorType string, actorID string) {
	go func() {
		err := h.Halt(actorType, actorID)
		if err != nil {
			// Log the error, not much else we can do
			h.log.Error(
				"Failed to halt actor",
				slog.String("actorRef", ref.NewActorRef(actorType, actorID).String()),
				slog.Any("error", err),
			)
		}
	}()
}

func (h *Host) runHealthChecks(parentCtx context.Context) error {
	var err error

	// Perform periodic health checks
	interval := h.HealthCheckInterval()
	h.log.DebugContext(parentCtx, "Starting background health checks", slog.Any("interval", interval))
	defer h.log.Debug("Stopped background health checks")

	t := h.clock.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			// Perform the health check
			err = h.HostHealthCheck(parentCtx, h.hostID)
			if err != nil {
				// In case of errors, return, which causes the task to stop and makes the entire host shut down
				return err
			}
		case <-parentCtx.Done():
			// Stop when the context is canceled
			return parentCtx.Err()
		}
	}
}

// HealthCheckInterval returns the recommended health check interval for hosts.
func (h *Host) HealthCheckInterval() time.Duration {
	return h.actorProvider.HealthCheckInterval()
}

// HostHealthCheck performs the health-check for a host.
func (h *Host) HostHealthCheck(parentCtx context.Context, hostID string) error {
	log := h.log.With(slog.String("hostId", hostID))

	log.DebugContext(parentCtx, "Sending health check to the provider")

	_, err := backoff.Retry(parentCtx,
		func() (r struct{}, rErr error) {
			ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
			defer cancel()
			rErr = h.actorProvider.UpdateActorHost(ctx, hostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
			switch {
			case errors.Is(rErr, components.ErrHostUnregistered):
				// Registration has expired, so no point in retrying anymore
				return r, backoff.Permanent(rErr)
			case rErr != nil:
				log.WarnContext(parentCtx, "Health check error; will retry", slog.Any("error", rErr))
				return r, rErr
			default:
				return r, nil
			}
		},
		backoff.WithBackOff(backoff.NewConstantBackOff(500*time.Millisecond)),
		backoff.WithMaxTries(3),
	)

	if err != nil {
		log.ErrorContext(parentCtx, "Health check failed", slog.Any("error", err))
		return fmt.Errorf("failed to perform health check: %w", err)
	}

	return nil
}

func (h *Host) handleIdleActor(act *activeactor.Instance) {
	// Just because the actor is marked as idle, doesn't mean it's inactive
	// For example, there could be a long-running operation still in progress
	// We need to confirm the actor isn't busy, and we need to prevent others from starting new work on it
	// To do that, we use TryLock, which will give us a lock only if the actor isn't busy
	// If we get the lock, it means it's safe for us to dispose of it
	// (Note that TryLock does also reset the idleAt time, but we will ignore that)
	ok, _, err := act.TryLock()
	if err != nil {
		h.log.Error("Failed to try locking idle actor for deactivation", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}

	// If we did not acquire the lock, the actor is still busy.
	// We will increase its idle time and re-enqueue it
	if !ok {
		h.log.Debug("Actor is busy and will not be deactivated; re-enqueueing it", slog.String("actorRef", act.Key()))
		act.UpdateIdleAt(actorBusyReEnqueueInterval)
		return
	}

	// Proceed with halting in a background goroutine, so we don't block other idle actors from being deactivated
	go func() {
		// We don't need to drain the active calls because we just acquired the lock
		err = h.haltActiveActor(act, false)
		if err != nil {
			h.log.Error("Failed to deactivate idle actor", slog.String("actorRef", act.Key()), slog.Any("error", err))
			return
		}
	}()
}

// Gracefully halts an actor's instance
func (h *Host) haltActiveActor(act *activeactor.Instance, drain bool) error {
	key := act.Key()

	h.log.Debug("Halting actor", slog.String("actorRef", key))

	// First, signal the actor's instance to halt, so it drains the current call and prevents more calls
	// Note that this call blocks until the current in-process request stops
	err := act.Halt(drain)
	if errors.Is(err, activeactor.ErrActiveActorAlreadyHalted) {
		// The actor is already halting, so nothing else to do here...
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	// Send the actor a message it has been deactivated
	err = h.deactivateActor(act)
	if err != nil {
		// Even though the call to the actor's Deactivate method failed, we still need to continue with the deactivation process
		// Otherwise, we are in a state where the object is still in-memory and that will cause many issues
		h.log.Error("Actor returned an error during deactivation", slog.Any("error", err))
	}

	// Remove the actor from the table
	// This will prevent more state changes
	act, ok := h.actors.GetAndDel(key)
	if !ok || act == nil {
		// If nothing was loaded, the actor was already deactivated
		return nil
	}

	// Report to the provider that the actor has been deactivated
	// This uses a background context because at this point it needs to not be tied to the caller's context
	// Once the decision to deactivate an actor has been made, we must go through with it or we could have an inconsistent state
	// TODO: Handle this error - should retry, and then maybe gracefully exit?
	ctx, cancel := context.WithTimeout(context.Background(), h.providerRequestTimeout)
	defer cancel()
	err = h.actorProvider.RemoveActor(ctx, act.ActorRef())
	if errors.Is(err, components.ErrNoActor) {
		// If the error is ErrNoActor, it means that the actor was already deactivated on the provider, so we can just ignore the erro
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to remove actor from the provider: %w", err)
	}

	return nil
}

func (h *Host) deactivateActor(act *activeactor.Instance) error {
	h.log.Debug("Deactivated actor", slog.String("actorRef", act.Key()))

	// Check if the actor implements the Deactivate method
	obj, ok := act.Instance().(actor.ActorDeactivate)
	if !ok {
		// Not an error - this is an optional interface
		return nil
	}

	// If the timeout is empty, there's nothing to do
	timeout := h.actorsConfig[act.ActorType()].DeactivationTimeout
	if timeout <= 0 {
		return nil
	}

	// This uses a background context because it should be unrelated from the caller's context
	// Once the decision to deactivate an actor has been made, we must go through with it or we could have an inconsistent state
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Call the Deactivate method on the actor
	err := obj.Deactivate(ctx)
	if err != nil {
		return fmt.Errorf("error from actor: %w", err)
	}

	return nil
}
