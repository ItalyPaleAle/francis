package host

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	backoff "github.com/cenkalti/backoff/v5"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/utils/clock"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/components/sqlite"
	"github.com/italypaleale/actors/internal/eventqueue"
	"github.com/italypaleale/actors/internal/ref"
	"github.com/italypaleale/actors/internal/servicerunner"
)

// This file contains code adapted from https://github.com/dapr/dapr/tree/v1.14.5/
// Copyright (C) 2024 The Dapr Authors
// License: Apache2

const (
	defaultShutdownGracePeriod    = 30 * time.Second
	defaultActorsMapSize          = 128
	defaultIdleTimeout            = 5 * time.Minute
	defaultDeactivationTimeout    = 5 * time.Second
	defaultProviderRequestTimeout = 15 * time.Second
	// If an idle actor is getting deactivated, but it's still busy, will be re-enqueued with its idle timeout increased by this duration
	actorBusyReEnqueueInterval = 10 * time.Second

	minTLSVersion = tls.VersionTLS13
)

// Re-export provider options
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
	client        *http.Client

	// Actor factory methods; key is actor type
	actorFactories map[string]actor.Factory

	// Active actors; key is "actorType/actorID"
	actors             *haxmap.Map[string, *activeActor]
	idleActorProcessor *eventqueue.Processor[string, *activeActor]

	// Map of actor configuration objects; key is actor type
	actorsConfig map[string]components.ActorHostType

	bind                   string
	serverTLSConfig        *tls.Config
	providerRequestTimeout time.Duration
	shutdownGracePeriod    time.Duration

	logSource *slog.Logger
	log       *slog.Logger
	clock     clock.WithTicker
}

// RegisterActorOptions is the type for the options for the RegisterActor method.
type RegisterActorOptions struct {
	IdleTimeout         time.Duration
	DeactivationTimeout time.Duration
	ConcurrencyLimit    int
}

// HostTLSOptions contains the options for the host's TLS configuration.
// All fields are optional
type HostTLSOptions struct {
	// CA Certificate, used by all nodes in the cluster
	CACertificate *x509.Certificate
	// TLS certificate and key for the server
	// If empty, uses a self-signed certificate
	ServerCertificate *tls.Certificate
	// If true, skips validating TLS certificates presented by other hosts
	// This is required when using self-signed certificates
	InsecureSkipTLSValidation bool
}

type NewHostOptions struct {
	// Address where the host can be reached at
	Address string

	// Port for the server to listen on
	// If empty, will be extracted from Address
	BindPort int

	// Address to bind the server to
	// If empty, will be extracted from Address
	BindAddress string

	// TLS options
	TLSOptions *HostTLSOptions

	// Instance of a slog.Logger
	Logger *slog.Logger

	// Options for the provider
	ProviderOptions components.ProviderOptions

	// Maximum interval between pings received from an actor host.
	HostHealthCheckDeadline time.Duration

	// Alarms lease duration
	AlarmsLeaseDuration time.Duration

	// Pre-fetch interval for alarms
	AlarmsFetchAheadInterval time.Duration

	// Batch size for pre-fetching alarms
	AlarmsFetchAheadBatchSize int

	// Grace period for shutting down
	ShutdownGracePeriod time.Duration

	// Timeout for requests to the provider
	ProviderRequestTimeout time.Duration

	// Allows setting a clock for testing
	clock clock.WithTicker
}

func (o NewHostOptions) getProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   o.HostHealthCheckDeadline,
		AlarmsLeaseDuration:       o.AlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  o.AlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: o.AlarmsFetchAheadBatchSize,
	}
}

// NewHost returns a new actor host.
func NewHost(opts NewHostOptions) (h *Host, err error) {
	// Validate the options passed
	if opts.Address == "" {
		return nil, errors.New("option Address is required")
	}
	addrHost, addrPortStr, err := net.SplitHostPort(opts.Address)
	if err != nil {
		return nil, fmt.Errorf("option Address is invalid: cannot split host and port: %w", err)
	}
	addrPort, err := strconv.Atoi(addrPortStr)
	if err != nil || addrPort == 0 {
		return nil, errors.New("option Address is invalid: port is invalid")
	}

	// Set a default logger, which sends logs to /dev/null, if none is passed
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	// Set other default values
	if opts.BindAddress == "" {
		opts.BindAddress = addrHost
	}
	if opts.BindPort <= 0 {
		opts.BindPort = addrPort
	}
	if opts.ShutdownGracePeriod <= 0 {
		opts.ShutdownGracePeriod = defaultShutdownGracePeriod
	}
	if opts.ProviderRequestTimeout <= 0 {
		opts.ProviderRequestTimeout = defaultProviderRequestTimeout
	}

	// Init a real clock if none is passed
	if opts.clock == nil {
		opts.clock = &clock.RealClock{}
	}

	// Get the provider
	var actorProvider components.ActorProvider
	switch x := opts.ProviderOptions.(type) {
	case sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(opts.Logger, x, opts.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case *sqlite.SQLiteProviderOptions:
		actorProvider, err = sqlite.NewSQLiteProvider(opts.Logger, *x, opts.getProviderConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite provider: %w", err)
		}
	case nil:
		return nil, errors.New("option ProviderOptions is required")
	default:
		return nil, fmt.Errorf("unsupported value for ProviderOptions: %T", opts.ProviderOptions)
	}

	h = &Host{
		address:                opts.Address,
		actorProvider:          actorProvider,
		actorsConfig:           map[string]components.ActorHostType{},
		actorFactories:         map[string]actor.Factory{},
		actors:                 haxmap.New[string, *activeActor](defaultActorsMapSize),
		shutdownGracePeriod:    opts.ShutdownGracePeriod,
		providerRequestTimeout: opts.ProviderRequestTimeout,
		bind:                   net.JoinHostPort(opts.BindAddress, strconv.Itoa(opts.BindPort)),
		logSource:              opts.Logger,
		clock:                  opts.clock,
	}
	h.service = actor.NewService(h)
	h.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		Clock:     h.clock,
		ExecuteFn: h.handleIdleActor,
	})

	// Init the TLS certificate for the server
	clientTLSConfig, err := h.initTLS(opts.TLSOptions)
	if err != nil {
		return nil, err
	}

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

// RegisterActor registers a new actor in the host.
// Must be called before Run.
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot call RegisterActor after host has started")
	}

	switch {
	case opts.IdleTimeout == 0:
		// Set default idle timeout if empty
		opts.IdleTimeout = defaultIdleTimeout
	case opts.IdleTimeout < 0:
		// A negative number means no timeout
		opts.IdleTimeout = -1
	}

	switch {
	case opts.ConcurrencyLimit <= 0:
		opts.ConcurrencyLimit = 0
	case opts.ConcurrencyLimit > math.MaxInt32:
		return errors.New("option ConcurrencyLimit must fit in int32 (2^31-1)")
	}

	switch {
	case opts.DeactivationTimeout == 0:
		opts.DeactivationTimeout = defaultDeactivationTimeout
	case opts.DeactivationTimeout < 0:
		return errors.New("option DeactivationTimeout must not be negative")
	}

	h.actorsConfig[actorType] = components.ActorHostType{
		ActorType:           actorType,
		IdleTimeout:         opts.IdleTimeout,
		ConcurrencyLimit:    int32(opts.ConcurrencyLimit),
		DeactivationTimeout: opts.DeactivationTimeout,
	}

	h.actorFactories[actorType] = factory

	return nil
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

	// Perform initialization steps
	initCtx, initCancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
	defer initCancel()
	err := h.actorProvider.Init(initCtx)
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
	defer registerCancel()
	res, err := h.actorProvider.RegisterHost(registerCtx, components.RegisterHostReq{
		Address:    h.address,
		ActorTypes: actorsConfigList,
	})
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
	}()

	// Run all services
	// This blocks until the context is canceled or one of the services returns
	return servicerunner.
		NewServiceRunner(
			// Perform health checks in background
			h.runHealthChecks,

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
		go func(act *activeActor) {
			err := h.haltActiveActor(act)
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
	err := h.haltActiveActor(act)
	if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	return nil
}

func (h *Host) runHealthChecks(parentCtx context.Context) error {
	var err error

	// Perform periodic health checks
	interval := h.actorProvider.HealthCheckInterval()
	h.log.DebugContext(parentCtx, "Starting background health checks", slog.Any("interval", interval))

	t := time.NewTicker(interval)
	defer t.Stop()

	retryOpts := []backoff.RetryOption{
		backoff.WithBackOff(backoff.NewConstantBackOff(500 * time.Millisecond)),
		backoff.WithMaxTries(3),
	}

	for {
		select {
		case <-t.C:
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

func (h *Host) runLeaseRenewal(parentCtx context.Context) (err error) {
	var res components.RenewAlarmLeasesRes

	// Renew the alarm leases on a loop
	interval := h.actorProvider.RenewLeaseInterval()
	h.log.DebugContext(parentCtx, "Starting background alarm lease renewal", slog.Any("interval", interval))

	t := time.NewTicker(interval)
	defer t.Stop()

	hostList := []string{h.hostID}
	for {
		select {
		case <-t.C:
			ctx, cancel := context.WithTimeout(parentCtx, h.providerRequestTimeout)
			defer cancel()
			res, err = h.actorProvider.RenewAlarmLeases(ctx, components.RenewAlarmLeasesReq{
				Hosts: hostList,
			})
			if err != nil {
				// Log the error only
				h.log.ErrorContext(parentCtx, "Error while renewing leases for alarms", slog.Any("error", err))
			} else if len(res.Leases) > 0 {
				// Use the list of leases just for logging, to avoid potential issues with race conditions alongside execution of alarms
				h.log.DebugContext(parentCtx, "Renewed alarm leases", slog.Int("count", len(res.Leases)))
			}
		case <-parentCtx.Done():
			// Stop when the context is canceled
			return parentCtx.Err()
		}
	}
}

func (h *Host) handleIdleActor(act *activeActor) {
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
		act.updateIdleAt(actorBusyReEnqueueInterval)
		return
	}

	// Proceed with halting
	err = h.haltActiveActor(act)
	if err != nil {
		h.log.Error("Failed to deactivate idle actor", slog.String("actorRef", act.Key()), slog.Any("error", err))
		return
	}
}

// Gracefully halts an actor's instance
func (h *Host) haltActiveActor(act *activeActor) error {
	key := act.Key()

	h.log.Debug("Halting actor", slog.String("actorRef", key))

	// First, signal the actor's instance to halt, so it drains the current call and prevents more calls
	// Note that this call blocks until the current in-process request stops
	err := act.Halt()
	if err != nil {
		return fmt.Errorf("failed to halt actor: %w", err)
	}

	// Send the actor a message it has been deactivated
	err = h.deactivateActor(act)
	if err != nil {
		return fmt.Errorf("failed to deactivate actor: %w", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), h.providerRequestTimeout)
	defer cancel()
	err = h.actorProvider.RemoveActor(ctx, act.ref)
	if err != nil {
		return fmt.Errorf("failed to remove actor from the provider: %w", err)
	}

	return nil
}

func (h *Host) deactivateActor(act *activeActor) error {
	h.log.Debug("Deactivated actor", slog.String("actorRef", act.Key()))

	// Check if the actor implements the Deactivate method
	obj, ok := act.instance.(actor.ActorDeactivate)
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
