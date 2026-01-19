package remote

import (
	"bytes"
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
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/locker"
	"github.com/italypaleale/francis/internal/peerauth"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

const (
	defaultActorsMapSize    = 128
	placementCacheMaxTTL    = 5 * time.Second
	actorBusyReEnqueueInterval = 10 * time.Second

	// HTTP headers
	headerContentType = "Content-Type"
	headerXHostID     = "X-Host-Id"
	contentTypeMsgpack = "application/vnd.msgpack"
)

// Host is a remote actor host that connects to Francis
type Host struct {
	running atomic.Bool

	francisAddr    string
	clientAddr     string
	francisClient  *FrancisClient
	httpClient     *http.Client
	clientID       string

	// Actor factory methods; key is actor type
	actorFactories map[string]actor.Factory
	// Active actors; key is "actorType/actorID"
	actors             *haxmap.Map[string, *activeActor]
	idleActorProcessor *eventqueue.Processor[string, *activeActor]

	// Actor placement cache
	placementCache *ttlcache.Cache[string, *actorPlacement]

	// Map of actor configuration objects; key is actor type
	actorsConfig map[string]local.RegisterActorOptions

	bind                string
	serverTLSConfig     *tls.Config
	peerAuth            peerauth.PeerAuthenticationMethod
	shutdownGracePeriod time.Duration
	requestTimeout      time.Duration
	healthCheckInterval time.Duration

	service   *actor.Service
	logSource *slog.Logger
	log       *slog.Logger
	clock     clock.WithTicker
}

// actorPlacement contains cached placement info
type actorPlacement struct {
	HostID      string
	Address     string
	IdleTimeout time.Duration
}

// activeActor represents an active actor instance
type activeActor struct {
	ref      ref.ActorRef
	instance actor.Actor
	locker   locker.TurnBasedLocker

	idleTimeout time.Duration
	idleAt      time.Time

	haltCh   chan struct{}
	halted   atomic.Bool

	host *Host
}

func (a *activeActor) Key() string {
	return a.ref.String()
}

func (a *activeActor) ActorType() string {
	return a.ref.ActorType
}

func (a *activeActor) DueTime() time.Time {
	return a.idleAt
}

func (a *activeActor) TryLock() (bool, chan struct{}, error) {
	if a.halted.Load() {
		return false, nil, ErrActorHalted
	}

	ok, err := a.locker.TryLock()
	if err != nil {
		if errors.Is(err, locker.ErrStopped) {
			return false, nil, ErrActorHalted
		}
		return false, nil, err
	}

	if !ok {
		return false, nil, nil
	}

	return true, a.haltCh, nil
}

func (a *activeActor) Lock(ctx context.Context) (chan struct{}, error) {
	if a.halted.Load() {
		return nil, ErrActorHalted
	}

	err := a.locker.Lock(ctx)
	if err != nil {
		if errors.Is(err, locker.ErrStopped) {
			return nil, ErrActorHalted
		}
		return nil, err
	}

	return a.haltCh, nil
}

func (a *activeActor) Unlock() {
	a.locker.Unlock()
}

func (a *activeActor) updateIdleAt(offset time.Duration) {
	a.idleAt = a.host.clock.Now().Add(offset)
	a.host.idleActorProcessor.Enqueue(a)
}

func (a *activeActor) Halt(drain bool) error {
	if !a.halted.CompareAndSwap(false, true) {
		return errors.New("already halted")
	}

	close(a.haltCh)

	if drain {
		a.locker.StopAndWait()
	} else {
		a.locker.Stop()
	}

	return nil
}

// NewHost creates a new remote host
func NewHost(opts ...HostOption) (*Host, error) {
	options := &hostOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Validate required options
	if options.FrancisAddress == "" {
		return nil, errors.New("FrancisAddress is required")
	}
	if options.ClientAddress == "" {
		return nil, errors.New("ClientAddress is required")
	}

	// Parse client address
	addrHost, addrPortStr, err := net.SplitHostPort(options.ClientAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid ClientAddress: %w", err)
	}
	addrPort, err := strconv.Atoi(addrPortStr)
	if err != nil || addrPort == 0 {
		return nil, errors.New("invalid ClientAddress: port is invalid")
	}

	// Set defaults
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
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
	if options.HealthCheckInterval <= 0 {
		options.HealthCheckInterval = defaultHealthCheckInterval
	}
	if options.clock == nil {
		options.clock = &clock.RealClock{}
	}

	// Validate peer authentication
	switch x := options.PeerAuthentication.(type) {
	case *peerauth.PeerAuthenticationSharedKey:
		if err = x.Validate(); err != nil {
			return nil, fmt.Errorf("invalid PeerAuthenticationSharedKey: %w", err)
		}
	case *peerauth.PeerAuthenticationMTLS:
		if err = x.Validate(); err != nil {
			return nil, fmt.Errorf("invalid PeerAuthenticationMTLS: %w", err)
		}
		x.SetTLSOptions(&options.TLSOptions)
	case nil:
		return nil, errors.New("PeerAuthentication is required")
	default:
		return nil, fmt.Errorf("unsupported PeerAuthentication type: %T", options.PeerAuthentication)
	}

	// Get TLS config
	serverTLSConfig, clientTLSConfig, err := options.TLSOptions.GetTLSConfig()
	if err != nil {
		return nil, err
	}

	// Use Francis TLS config if provided, otherwise use peer TLS config
	francisTLSConfig := options.FrancisTLSConfig
	if francisTLSConfig == nil {
		francisTLSConfig = clientTLSConfig
	}

	h := &Host{
		francisAddr:         options.FrancisAddress,
		clientAddr:          options.ClientAddress,
		actorFactories:      make(map[string]actor.Factory),
		actorsConfig:        make(map[string]local.RegisterActorOptions),
		actors:              haxmap.New[string, *activeActor](defaultActorsMapSize),
		bind:                net.JoinHostPort(options.BindAddress, strconv.Itoa(options.BindPort)),
		serverTLSConfig:     serverTLSConfig,
		peerAuth:            options.PeerAuthentication,
		shutdownGracePeriod: options.ShutdownGracePeriod,
		requestTimeout:      options.RequestTimeout,
		healthCheckInterval: options.HealthCheckInterval,
		logSource:           options.Logger,
		clock:               options.clock,
	}

	h.service = actor.NewService(h)

	// Create Francis client
	h.francisClient = NewFrancisClient(
		options.FrancisAddress,
		francisTLSConfig,
		options.RequestTimeout,
		options.Logger.With(slog.String("component", "francis-client")),
	)

	// Create HTTP client for peer-to-peer
	h.httpClient = &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: clientTLSConfig,
			QUICConfig:      &quic.Config{},
		},
	}

	return h, nil
}

// RegisterActor registers an actor type with the host
func (h *Host) RegisterActor(actorType string, factory actor.Factory, opts local.RegisterActorOptions) error {
	if h.running.Load() {
		return errors.New("cannot register actors after host has started")
	}

	if err := opts.Validate(); err != nil {
		return err
	}

	h.actorsConfig[actorType] = opts
	h.actorFactories[actorType] = factory

	return nil
}

// Service returns the actor service
func (h *Host) Service() *actor.Service {
	return h.service
}

// Run starts the remote host
func (h *Host) Run(parentCtx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer h.running.Store(false)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Initialize processors and caches
	h.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
		Clock:     h.clock,
		ExecuteFn: h.handleIdleActor,
	})
	defer h.idleActorProcessor.Close()

	h.placementCache = ttlcache.NewCache[string, *actorPlacement](&ttlcache.CacheOptions{
		MaxTTL: placementCacheMaxTTL,
	})
	defer h.placementCache.Stop()

	// Connect to Francis
	connectCtx, connectCancel := context.WithTimeout(parentCtx, h.requestTimeout)
	err := h.francisClient.Connect(connectCtx)
	connectCancel()
	if err != nil {
		return fmt.Errorf("failed to connect to Francis: %w", err)
	}
	defer h.francisClient.Disconnect()

	// Register with Francis
	actorTypes := make([]protocol.ActorTypeRegistration, 0, len(h.actorsConfig))
	for actorType, opts := range h.actorsConfig {
		actorTypes = append(actorTypes, protocol.ActorTypeRegistration{
			ActorType:           actorType,
			IdleTimeout:         opts.IdleTimeout,
			ConcurrencyLimit:    int32(opts.ConcurrencyLimit),
			DeactivationTimeout: opts.DeactivationTimeout,
			MaxAttempts:         opts.MaxAttempts,
			InitialRetryDelay:   opts.InitialRetryDelay,
		})
	}

	registerCtx, registerCancel := context.WithTimeout(parentCtx, h.requestTimeout)
	res, err := h.francisClient.Register(registerCtx, &protocol.RegisterClientReq{
		Address:    h.clientAddr,
		ActorTypes: actorTypes,
	})
	registerCancel()
	if err != nil {
		return fmt.Errorf("failed to register with Francis: %w", err)
	}

	h.clientID = res.ClientID
	h.log = h.logSource.With(slog.String("clientId", h.clientID))

	h.log.InfoContext(ctx, "Registered with Francis", slog.String("address", h.clientAddr))

	// Cleanup on exit
	defer func() {
		if haltErr := h.HaltAll(); haltErr != nil {
			h.log.Warn("Error halting actors", slog.Any("error", haltErr))
		}
	}()

	defer func() {
		unregCtx, unregCancel := context.WithTimeout(context.Background(), h.requestTimeout)
		if unregErr := h.francisClient.Unregister(unregCtx); unregErr != nil {
			h.log.WarnContext(unregCtx, "Error unregistering from Francis", slog.Any("error", unregErr))
		}
		unregCancel()
		h.log.InfoContext(ctx, "Unregistered from Francis")
	}()

	// Run services
	return servicerunner.
		NewServiceRunner(
			h.runHealthChecks,
			h.runServer,
			h.runNotificationHandler,
		).
		Run(ctx)
}

func (h *Host) runHealthChecks(ctx context.Context) error {
	h.log.DebugContext(ctx, "Starting health checks", slog.Any("interval", h.healthCheckInterval))
	defer h.log.Debug("Stopped health checks")

	t := h.clock.NewTicker(h.healthCheckInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			hcCtx, hcCancel := context.WithTimeout(ctx, h.requestTimeout)
			err := h.francisClient.HealthCheck(hcCtx)
			hcCancel()
			if err != nil {
				h.log.WarnContext(ctx, "Health check failed", slog.Any("error", err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *Host) runServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// Invoke endpoint for peer-to-peer communication
	mux.HandleFunc("POST /v1/invoke/{actorType}/{actorID}/{method}", h.handleInvokeRequest)

	server := &http3.Server{
		Addr:       h.bind,
		TLSConfig:  h.serverTLSConfig,
		Handler:    mux,
		QUICConfig: &quic.Config{},
	}

	errCh := make(chan error, 1)
	go func() {
		h.log.InfoContext(ctx, "Starting peer-to-peer server", slog.String("address", h.bind))
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), h.shutdownGracePeriod)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (h *Host) runNotificationHandler(ctx context.Context) error {
	// Listen for notifications from Francis
	// This is handled by the FrancisClient's receiveNotifications
	<-ctx.Done()
	return ctx.Err()
}

func (h *Host) handleInvokeRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Validate host ID
	reqHostID := r.Header.Get(headerXHostID)
	if reqHostID == "" || reqHostID != h.clientID {
		w.WriteHeader(http.StatusConflict)
		return
	}

	// Authorize request
	ok, err := h.peerAuth.ValidateIncomingRequest(r)
	if err != nil || !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Read request body
	var reqData any
	ct := r.Header.Get(headerContentType)
	if ct == contentTypeMsgpack {
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		dec.Reset(r.Body)
		if err = dec.Decode(&reqData); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// Invoke actor
	actorType := r.PathValue("actorType")
	actorID := r.PathValue("actorID")
	method := r.PathValue("method")

	outData, err := h.InvokeLocal(r.Context(), actorType, actorID, method, reqData)
	if err != nil {
		if errors.Is(err, ErrActorNotHosted) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if errors.Is(err, ErrActorHalted) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if outData == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set(headerContentType, contentTypeMsgpack)
	w.WriteHeader(http.StatusOK)

	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(w)
	enc.Encode(outData)
}

// Invoke invokes an actor method
func (h *Host) Invoke(ctx context.Context, actorType, actorID, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	// Look up actor location
	placement, err := h.lookupActor(ctx, actorType, actorID, false)
	if err != nil {
		return nil, err
	}

	// If local, invoke directly
	if placement.HostID == h.clientID {
		return h.InvokeLocal(ctx, actorType, actorID, method, data)
	}

	// Remote invocation
	return h.invokeRemote(ctx, placement, actorType, actorID, method, data)
}

func (h *Host) lookupActor(ctx context.Context, actorType, actorID string, activeOnly bool) (*actorPlacement, error) {
	key := actorType + "/" + actorID

	// Check cache first
	if cached, ok := h.placementCache.Get(key); ok {
		return cached, nil
	}

	// Query Francis
	lookupCtx, lookupCancel := context.WithTimeout(ctx, h.requestTimeout)
	res, err := h.francisClient.LookupActor(lookupCtx, actorType, actorID, activeOnly)
	lookupCancel()
	if err != nil {
		return nil, err
	}

	placement := &actorPlacement{
		HostID:      res.HostID,
		Address:     res.Address,
		IdleTimeout: res.IdleTimeout,
	}

	// Cache the result
	h.placementCache.SetWithTTL(key, placement, placementCacheMaxTTL)

	return placement, nil
}

// InvokeLocal invokes an actor on the local host
func (h *Host) InvokeLocal(ctx context.Context, actorType, actorID, method string, data any) (actor.Envelope, error) {
	act, err := h.getOrCreateActor(ctx, actorType, actorID)
	if err != nil {
		return nil, err
	}

	// Acquire lock
	_, err = act.Lock(ctx)
	if err != nil {
		return nil, err
	}
	defer act.Unlock()

	// Check if halted
	if act.halted.Load() {
		return nil, ErrActorHalted
	}

	// Update idle time
	if act.idleTimeout > 0 {
		act.updateIdleAt(act.idleTimeout)
	}

	// Invoke
	invokable, ok := act.instance.(actor.ActorInvoke)
	if !ok {
		return nil, errors.New("actor does not implement Invoke")
	}

	result, err := invokable.Invoke(ctx, method, newEnvelope(data))
	if err != nil {
		return nil, err
	}

	return newEnvelope(result), nil
}

func (h *Host) invokeRemote(ctx context.Context, placement *actorPlacement, actorType, actorID, method string, data any) (actor.Envelope, error) {
	url := fmt.Sprintf("https://%s/v1/invoke/%s/%s/%s", placement.Address, actorType, actorID, method)

	var body bytes.Buffer
	if data != nil {
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(&body)
		if err := enc.Encode(data); err != nil {
			return nil, fmt.Errorf("failed to encode request: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &body)
	if err != nil {
		return nil, err
	}

	req.Header.Set(headerContentType, contentTypeMsgpack)
	req.Header.Set(headerXHostID, placement.HostID)
	h.peerAuth.AddAuthenticationHeaders(req)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		// Host ID mismatch - invalidate cache and retry
		h.placementCache.Delete(actorType + "/" + actorID)
		return nil, errors.New("host mismatch, please retry")
	}

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote invocation failed with status %d", resp.StatusCode)
	}

	// Decode response
	var result any
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.Reset(resp.Body)
	if err = dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return newEnvelope(result), nil
}

func (h *Host) getOrCreateActor(ctx context.Context, actorType, actorID string) (*activeActor, error) {
	key := actorType + "/" + actorID

	// Check if already exists
	if act, ok := h.actors.Get(key); ok {
		return act, nil
	}

	// Check if we have a factory
	factory, ok := h.actorFactories[actorType]
	if !ok {
		return nil, ErrActorTypeUnsupported
	}

	// Create new actor
	act, _ := h.actors.GetOrCompute(key, func() *activeActor {
		config := h.actorsConfig[actorType]
		newAct := &activeActor{
			ref:         ref.NewActorRef(actorType, actorID),
			locker:      locker.TurnBasedLocker{},
			idleTimeout: config.IdleTimeout,
			haltCh:      make(chan struct{}),
			host:        h,
		}
		newAct.instance = factory(actorID, h.service)

		// Start idle timer
		if config.IdleTimeout > 0 {
			newAct.updateIdleAt(config.IdleTimeout)
		}

		return newAct
	})

	return act, nil
}

func (h *Host) handleIdleActor(act *activeActor) {
	ok, _, err := act.TryLock()
	if err != nil {
		h.log.Error("Failed to lock idle actor", slog.String("actor", act.Key()), slog.Any("error", err))
		return
	}

	if !ok {
		// Actor is busy, re-enqueue
		h.log.Debug("Actor is busy, re-enqueueing", slog.String("actor", act.Key()))
		act.updateIdleAt(actorBusyReEnqueueInterval)
		return
	}

	// We acquired the lock, proceed to halt in background
	// Don't need to drain since we already hold the lock
	go func() {
		if err := h.haltActiveActor(act, false); err != nil {
			h.log.Error("Failed to halt idle actor", slog.String("actor", act.Key()), slog.Any("error", err))
		}
	}()
}

func (h *Host) haltActiveActor(act *activeActor, drain bool) error {
	key := act.Key()

	h.log.Debug("Halting actor", slog.String("actor", key))

	if err := act.Halt(drain); err != nil {
		return err
	}

	// Call Deactivate if implemented
	if deactivatable, ok := act.instance.(actor.ActorDeactivate); ok {
		config := h.actorsConfig[act.ActorType()]
		if config.DeactivationTimeout > 0 {
			deactCtx, deactCancel := context.WithTimeout(context.Background(), config.DeactivationTimeout)
			if err := deactivatable.Deactivate(deactCtx); err != nil {
				h.log.Error("Error during actor deactivation", slog.String("actor", key), slog.Any("error", err))
			}
			deactCancel()
		}
	}

	// Remove from map
	h.actors.Del(key)

	// Notify Francis
	notifyCtx, notifyCancel := context.WithTimeout(context.Background(), h.requestTimeout)
	h.francisClient.SendActorTerminateComplete(notifyCtx, &protocol.ActorTerminateCompleteReq{
		ClientID:  h.clientID,
		ActorType: act.ref.ActorType,
		ActorID:   act.ref.ActorID,
		Success:   true,
	})
	notifyCancel()

	return nil
}

// HaltAll halts all actors
func (h *Host) HaltAll() error {
	var errs []error
	var wg sync.WaitGroup

	for kv := range h.actors.Iterator() {
		wg.Add(1)
		go func(act *activeActor) {
			defer wg.Done()
			if err := h.haltActiveActor(act, true); err != nil {
				errs = append(errs, fmt.Errorf("failed to halt %s: %w", act.Key(), err))
			}
		}(kv.Value())
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Halt halts a specific actor
func (h *Host) Halt(actorType, actorID string) error {
	key := actorType + "/" + actorID
	act, ok := h.actors.Get(key)
	if !ok {
		return ErrActorNotHosted
	}
	return h.haltActiveActor(act, true)
}

// HaltDeferred halts an actor in background
func (h *Host) HaltDeferred(actorType, actorID string) {
	go func() {
		if err := h.Halt(actorType, actorID); err != nil {
			h.log.Error("Failed to halt actor", slog.String("actor", actorType+"/"+actorID), slog.Any("error", err))
		}
	}()
}

// GetAlarm gets an alarm from Francis
func (h *Host) GetAlarm(ctx context.Context, actorType, actorID, name string) (actor.AlarmProperties, error) {
	res, err := h.francisClient.GetAlarm(ctx, actorType, actorID, name)
	if err != nil {
		if errors.Is(err, ErrAlarmNotFound) {
			return actor.AlarmProperties{}, actor.ErrAlarmNotFound
		}
		return actor.AlarmProperties{}, err
	}

	var data any
	if len(res.Data) > 0 {
		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(res.Data))
		defer msgpack.PutDecoder(dec)
		dec.Decode(&data)
	}

	return actor.AlarmProperties{
		DueTime:  res.DueTime,
		Interval: res.Interval,
		TTL:      res.TTL,
		Data:     data,
	}, nil
}

// SetAlarm sets an alarm in Francis
func (h *Host) SetAlarm(ctx context.Context, actorType, actorID, name string, properties actor.AlarmProperties) error {
	var data []byte
	if properties.Data != nil {
		var buf bytes.Buffer
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(&buf)
		if err := enc.Encode(properties.Data); err != nil {
			return err
		}
		data = buf.Bytes()
	}

	return h.francisClient.SetAlarm(ctx, &protocol.SetAlarmReq{
		ActorType: actorType,
		ActorID:   actorID,
		AlarmName: name,
		DueTime:   properties.DueTime,
		Interval:  properties.Interval,
		TTL:       properties.TTL,
		Data:      data,
	})
}

// DeleteAlarm deletes an alarm from Francis
func (h *Host) DeleteAlarm(ctx context.Context, actorType, actorID, name string) error {
	err := h.francisClient.DeleteAlarm(ctx, actorType, actorID, name)
	if errors.Is(err, ErrAlarmNotFound) {
		return actor.ErrAlarmNotFound
	}
	return err
}

// GetState gets state from Francis
func (h *Host) GetState(ctx context.Context, actorType, actorID string, dest any) error {
	data, err := h.francisClient.GetState(ctx, actorType, actorID)
	if err != nil {
		if errors.Is(err, ErrStateNotFound) {
			return actor.ErrStateNotFound
		}
		return err
	}

	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.Reset(bytes.NewReader(data))
	return dec.Decode(dest)
}

// SetState sets state in Francis
func (h *Host) SetState(ctx context.Context, actorType, actorID string, state any, opts *actor.SetStateOpts) error {
	var buf bytes.Buffer
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(&buf)
	if err := enc.Encode(state); err != nil {
		return err
	}

	var ttl time.Duration
	if opts != nil {
		ttl = opts.TTL
	}

	return h.francisClient.SetState(ctx, actorType, actorID, buf.Bytes(), ttl)
}

// DeleteState deletes state from Francis
func (h *Host) DeleteState(ctx context.Context, actorType, actorID string) error {
	err := h.francisClient.DeleteState(ctx, actorType, actorID)
	if errors.Is(err, ErrStateNotFound) {
		return actor.ErrStateNotFound
	}
	return err
}

// envelope wraps data for actor invocation
type envelope struct {
	data any
}

func newEnvelope(data any) *envelope {
	return &envelope{data: data}
}

func (e *envelope) Decode(dest any) error {
	if e.data == nil {
		return nil
	}

	// If data is already bytes, unmarshal it
	if b, ok := e.data.([]byte); ok {
		return msgpack.Unmarshal(b, dest)
	}

	// Otherwise, marshal and unmarshal to convert types
	b, err := msgpack.Marshal(e.data)
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(b, dest)
}
