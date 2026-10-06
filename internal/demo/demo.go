// Package demo runs a complete Francis cluster in one process, filled with sample data, for working on the management dashboard
// It's a real runtime with the in-memory provider and real hosts, so everything the dashboard shows comes from the same code a production cluster runs
// The runtime binary only includes it when built with the "demo" build tag
package demo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/cronjob"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/host"
	"github.com/italypaleale/francis/host/remote"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/internal/providerfactory"
	"github.com/italypaleale/francis/internal/runtime"
)

// The demo's credentials are fixed, so they're easy to paste into the dashboard
// They're only for the demo, which listens on loopback by default
const (
	ReadOnlyToken    = "demo-readonly-token-0123456789abcdefghij"
	ManagementToken  = "demo-management-token-0123456789abcdefgh"
	runtimePSK       = "demo-runtime-psk-0123456789abcdef"
	hostBootstrapPSK = "demo-host-bootstrap-psk-0123456789abcd"
)

// DefaultManagementBind is where the demo serves the management API and the dashboard by default, which is where the dashboard's dev server proxies to
const DefaultManagementBind = "127.0.0.1:7401"

// Options contains the options for Run
type Options struct {
	// ManagementBind is the TCP address of the management API and the dashboard
	ManagementBind string
	// Dashboard holds the compiled dashboard, or nil to serve only the API
	Dashboard fs.FS
	// AllowedOrigins lists the browser origins that may call the API, such as the dashboard's dev server
	AllowedOrigins []string
	// HoldLease holds the cluster's exclusive-access lease once the data is seeded, as a restore would
	// While it's held, the provider evicts the hosts and refuses to register them again, and the API refuses drains and workflow controls
	HoldLease bool
	// ManyJobs dispatches more jobs than the cluster summary counts, so it shows capped counts
	ManyJobs bool
	// Logger is the slog logger
	// If nil, logs are discarded
	Logger *slog.Logger
	// OnReady, if set, is called once the sample data is in place
	OnReady func()
}

// cluster is the running demo
type cluster struct {
	opts     Options
	log      *slog.Logger
	provider components.ActorProvider

	// alpha is the live instance of the host the demo seeds data and runs its activity through, which changes when it's drained and replaced
	alpha atomic.Pointer[liveHost]
	// firstReady is closed for each host once it first connects
	firstReady map[string]chan struct{}
}

// liveHost is a running instance of a demo host
type liveHost struct {
	svc       *actor.Service
	workflows *workflows
	ready     <-chan struct{}
}

// hostSpec describes one of the demo's hosts
type hostSpec struct {
	name    string
	address string
	// register adds the host's actor types and workflows, returning the sample workflows when the host serves them
	register func(h *remote.Host) (*workflows, error)
}

// Run starts the demo cluster, seeds it, and keeps it busy until the context is canceled
func Run(ctx context.Context, opts Options) error {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.ManagementBind == "" {
		opts.ManagementBind = DefaultManagementBind
	}
	log := opts.Logger

	// The in-memory provider keeps the whole cluster in this process
	provider, err := providerfactory.New(log.With("scope", "provider"), standalone.StandaloneMemoryOptions{}, components.ProviderConfig{
		HostHealthCheckDeadline:   components.DefaultHostHealthCheckDeadline,
		AlarmsLeaseDuration:       components.DefaultAlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  components.DefaultAlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: components.DefaultAlarmsFetchAheadBatchSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create the provider: %w", err)
	}
	defer func() {
		_ = provider.Close()
	}()

	// The runtime and the hosts talk over loopback on ports picked now, so the demo never collides with a cluster already running
	runtimeAddress, err := freeUDPAddress()
	if err != nil {
		return err
	}
	caPEM, err := runtime.CABundlePEM([]byte(runtimePSK))
	if err != nil {
		return fmt.Errorf("failed to derive the cluster CA: %w", err)
	}

	rt, err := runtime.NewRuntime(provider,
		runtime.WithBind(runtimeAddress),
		runtime.WithRuntimePSKs([]byte(runtimePSK)),
		runtime.WithHostBootstrapPSK([]byte(hostBootstrapPSK)),
		runtime.WithLogger(log.With("scope", "runtime")),
		runtime.WithShutdownGracePeriod(5*time.Second),
		// The archive job runs for longer than the runtime waits for a job by default, and it should stay active rather than be retried
		runtime.WithAlarmExecutionTimeout(archiveJobDuration+time.Minute),
		runtime.WithManagement(management.Config{
			Bind:             opts.ManagementBind,
			ReadOnlyTokens:   []string{ReadOnlyToken},
			ManagementTokens: []string{ManagementToken},
			Dashboard:        opts.Dashboard,
			AllowedOrigins:   opts.AllowedOrigins,
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create the runtime: %w", err)
	}

	specs, err := hostSpecs()
	if err != nil {
		return err
	}

	c := &cluster{
		opts:       opts,
		log:        log,
		provider:   provider,
		firstReady: make(map[string]chan struct{}, len(specs)),
	}

	// Every host connects to the runtime on its own schedule, and the seeding waits for all of them
	services := []servicerunner.Service{c.runUnreachableHost, c.runSeeding}
	for _, spec := range specs {
		c.firstReady[spec.name] = make(chan struct{})
		services = append(services, c.runHost(spec, runtimeAddress, caPEM))
	}

	// The runtime outlives the hosts, which need it to halt their actors when the demo stops
	// If the runtime stops on its own, such as when it can't listen, the hosts stop too since they have nothing to connect to
	hostsCtx, hostsCancel := context.WithCancel(ctx)
	defer hostsCancel()
	rtCtx, rtCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer rtCancel()
	rtErr := make(chan error, 1)
	go func() {
		rtErr <- rt.Run(rtCtx)
		hostsCancel()
	}()

	// Stop the runtime once the hosts have stopped
	err = servicerunner.NewServiceRunner(services...).Run(hostsCtx)
	rtCancel()
	return errors.Join(err, <-rtErr)
}

// hostSpecs returns the demo's hosts, each serving a different mix of actor types
// Alpha is the only server of the notifier type, so draining it is refused unless forced, and beta and gamma serve different definitions of the export workflow
func hostSpecs() ([]hostSpec, error) {
	addresses := make([]string, 3)
	for i := range addresses {
		addr, err := freeUDPAddress()
		if err != nil {
			return nil, err
		}
		addresses[i] = addr
	}

	mailerOpts := []remote.RegisterActorOption{
		remote.WithCompletedJobRetention(6 * time.Hour),
		remote.WithCapacityGroup("outbound", 4),
	}
	inventoryOpts := []remote.RegisterActorOption{
		remote.WithConcurrencyLimit(40),
		remote.WithIdleTimeout(time.Hour),
	}
	sessionOpts := []remote.RegisterActorOption{
		remote.WithIdleTimeout(30 * time.Minute),
	}

	return []hostSpec{
		{
			name:    "alpha",
			address: addresses[0],
			register: func(h *remote.Host) (*workflows, error) {
				err := errors.Join(
					h.RegisterActor(cartType, newCart, sessionOpts...),
					h.RegisterActor(userType, newUser, sessionOpts...),
					h.RegisterActor(notifierType, newNotifier, remote.WithIdleTimeout(10*time.Minute)),
				)
				if err != nil {
					return nil, err
				}

				cleanup, err := cronjob.New("cleanup-sessions",
					cronjob.WithInterval(10*time.Minute),
					cronjob.WithJob(func(ctx context.Context) error { return nil }),
				)
				if err != nil {
					return nil, err
				}
				err = h.RegisterBuiltInActor(cleanup)
				if err != nil {
					return nil, err
				}

				return registerWorkflows(h)
			},
		},
		{
			name:    "beta",
			address: addresses[1],
			register: func(h *remote.Host) (*workflows, error) {
				export, err := newExport(1)
				if err != nil {
					return nil, err
				}
				err = errors.Join(
					h.RegisterActor(cartType, newCart, sessionOpts...),
					h.RegisterActor(userType, newUser, sessionOpts...),
					h.RegisterActor(mailerType, newMailer, mailerOpts...),
					h.RegisterActor(inventoryType, newInventory, inventoryOpts...),
					h.RegisterBuiltInActor(export),
				)
				if err != nil {
					return nil, err
				}
				return registerWorkflows(h)
			},
		},
		{
			name:    "gamma",
			address: addresses[2],
			register: func(h *remote.Host) (*workflows, error) {
				export, err := newExport(2)
				if err != nil {
					return nil, err
				}
				return nil, errors.Join(
					h.RegisterActor(mailerType, newMailer, mailerOpts...),
					h.RegisterActor(inventoryType, newInventory, inventoryOpts...),
					h.RegisterBuiltInActor(export),
				)
			},
		},
	}, nil
}

func registerWorkflows(h *remote.Host) (*workflows, error) {
	wf, err := newWorkflows()
	if err != nil {
		return nil, err
	}

	err = errors.Join(
		h.RegisterBuiltInActor(wf.checkout),
		h.RegisterBuiltInActor(wf.onboarding),
		h.RegisterBuiltInActor(wf.kyc),
		h.RegisterBuiltInActor(wf.report),
	)
	if err != nil {
		return nil, err
	}
	return wf, nil
}

// runHost runs a demo host, and starts a replacement whenever the dashboard drains it, as an orchestrator would
func (c *cluster) runHost(spec hostSpec, runtimeAddress string, caPEM [][]byte) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		var signalFirstReady sync.Once
		for {
			h, err := remote.NewHost(
				remote.WithAddress(spec.address),
				remote.WithRuntimeAddresses(runtimeAddress),
				remote.WithHostBootstrapPSK([]byte(hostBootstrapPSK)),
				remote.WithPinnedCA(caPEM...),
				remote.WithLogger(c.log.With("scope", "host", "host", spec.name)),
				remote.WithShutdownGracePeriod(5*time.Second),
			)
			if err != nil {
				return fmt.Errorf("failed to create host %s: %w", spec.name, err)
			}

			wf, err := spec.register(h)
			if err != nil {
				return fmt.Errorf("failed to register the actors of host %s: %w", spec.name, err)
			}

			// The seeding and the activity run through alpha, so they follow its current instance
			if spec.name == "alpha" {
				c.alpha.Store(&liveHost{svc: h.Service(), workflows: wf, ready: h.Ready()})
			}
			go func() {
				select {
				case <-h.Ready():
					signalFirstReady.Do(func() { close(c.firstReady[spec.name]) })
				case <-ctx.Done():
				}
			}()

			// The demo stopping ends the host, whatever it returns
			err = h.Run(ctx)
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, host.ErrAdministrativeDrain):
				c.log.InfoContext(ctx, "Demo host was drained; starting a replacement", slog.String("host", spec.name))
			case err != nil:
				return fmt.Errorf("host %s stopped: %w", spec.name, err)
			default:
				return nil
			}

			// The drained host has just unregistered, so its address is free again
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// runUnreachableHost keeps a host registered in the provider without ever connecting it to the runtime, so the dashboard shows an unreachable host
// It serves an actor type nobody calls, so nothing is ever placed on it
func (c *cluster) runUnreachableHost(ctx context.Context) error {
	res, err := c.provider.RegisterHost(ctx, components.RegisterHostReq{
		Address: "10.0.4.17:7571",
		ActorTypes: []components.ActorHostType{
			{ActorType: ghostType, IdleTimeout: 10 * time.Minute},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to register the unreachable host: %w", err)
	}

	// Health checks keep the registration alive, at a third of the deadline after which the provider would drop it
	ticker := time.NewTicker(components.DefaultHostHealthCheckDeadline / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err = c.provider.UpdateActorHost(ctx, res.HostID, components.UpdateActorHostReq{UpdateLastHealthCheck: true})
			if err != nil && ctx.Err() == nil {
				c.log.WarnContext(ctx, "Failed to renew the unreachable host's registration", slog.Any("error", err))
			}
		case <-ctx.Done():
			_ = c.provider.UnregisterHost(context.WithoutCancel(ctx), res.HostID, components.UnregisterHostOpts{})
			return nil
		}
	}
}

// holdLease holds the cluster's exclusive-access lease until the context ends, as a restore would
func (c *cluster) holdLease(ctx context.Context) {
	const (
		owner = "francis restore (demo)"
		ttl   = 2 * time.Minute
	)

	_, err := c.provider.AcquireExclusiveLease(ctx, owner, ttl)
	if err != nil {
		c.log.WarnContext(ctx, "Failed to acquire the exclusive-access lease", slog.Any("error", err))
		return
	}

	ticker := time.NewTicker(ttl / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err = c.provider.RenewExclusiveLease(ctx, owner, ttl)
			if err != nil && ctx.Err() == nil {
				c.log.WarnContext(ctx, "Failed to renew the exclusive-access lease", slog.Any("error", err))
			}
		case <-ctx.Done():
			_ = c.provider.ReleaseExclusiveLease(context.WithoutCancel(ctx), owner)
			return
		}
	}
}

// freeUDPAddress returns a loopback address with a UDP port that is free right now
func freeUDPAddress() (string, error) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("failed to find a free UDP port: %w", err)
	}
	addr := conn.LocalAddr().String()
	_ = conn.Close()
	return addr, nil
}
