// Package clusteradmin administers a Francis cluster from outside of a host, such as taking exclusive access for a data restore
package clusteradmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/providerfactory"
)

const (
	// defaultExclusiveLeaseDuration is the default TTL of the exclusive-access lease
	// It is deliberately generous: the lease only needs to outlast a pause of the holder process, since it is renewed while the holder is alive
	defaultExclusiveLeaseDuration = 5 * time.Minute
	// defaultExclusiveRenewInterval is the default interval at which the lease is renewed while held
	defaultExclusiveRenewInterval = 2 * time.Minute
	// adminOpTimeout bounds each individual provider call the admin makes
	adminOpTimeout = 15 * time.Second
	// drainPollInterval is how often AcquireExclusive re-checks the host count while waiting for the cluster to drain
	drainPollInterval = time.Second
)

// Options configures an Admin
type Options struct {
	// HostHealthCheckDeadline should match the value used by the cluster's hosts
	// It determines when a host that stopped health-checking is considered gone, which bounds how long AcquireExclusive waits for the cluster to drain
	// Defaults to components.DefaultHostHealthCheckDeadline when zero
	HostHealthCheckDeadline time.Duration

	// ExclusiveLeaseDuration is the TTL of the exclusive-access lease
	// Defaults to 5 minutes when zero
	ExclusiveLeaseDuration time.Duration

	// ExclusiveRenewInterval is how often the lease is renewed while it is held
	// Defaults to 2 minutes when zero
	ExclusiveRenewInterval time.Duration

	// Logger is the slog logger (optional)
	Logger *slog.Logger
}

// AcquireOptions configures a call to AcquireExclusive
type AcquireOptions struct {
	// Force evicts running hosts and waits for the cluster to drain
	// When false, AcquireExclusive returns components.ErrHostsConnected immediately if any host is currently connected
	Force bool
}

// Admin performs cluster-wide administrative operations that run outside of a host, such as taking exclusive access for a data restore
// It is built from the same provider options a host uses and talks directly to the shared database
type Admin struct {
	provider  components.ActorProvider
	exclusive components.ExclusiveController
	owner     string
	log       *slog.Logger

	leaseTTL      time.Duration
	renewInterval time.Duration

	mu   sync.Mutex
	stop chan struct{}
	lost chan struct{}
}

// New builds an Admin from the given provider options
// The provider options are the same value passed to a host (for example a sqlite.SQLiteProviderOptions or postgres.PostgresProviderOptions)
// It initializes the provider, which applies any pending schema migrations, so it also works against a brand-new database
// It returns components.ErrExclusiveNotSupported if the provider does not support exclusive-access leases (the standalone providers do not)
func New(ctx context.Context, providerOptions components.ProviderOptions, opts Options) (*Admin, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.HostHealthCheckDeadline <= 0 {
		opts.HostHealthCheckDeadline = components.DefaultHostHealthCheckDeadline
	}
	if opts.ExclusiveLeaseDuration <= 0 {
		opts.ExclusiveLeaseDuration = defaultExclusiveLeaseDuration
	}
	if opts.ExclusiveRenewInterval <= 0 {
		opts.ExclusiveRenewInterval = defaultExclusiveRenewInterval
	}

	// Build the provider from a config whose only meaningful value is the health check deadline
	// The admin never registers hosts or runs alarms
	cfg := components.NewProviderConfig()
	cfg.HostHealthCheckDeadline = opts.HostHealthCheckDeadline

	provider, err := providerfactory.New(opts.Logger, providerOptions, cfg)
	if err != nil {
		return nil, err
	}

	// The admin needs a provider that supports exclusive-access leases
	exclusive, ok := provider.(components.ExclusiveController)
	if !ok {
		return nil, components.ErrExclusiveNotSupported
	}

	// Initialize the provider so its schema (including the cluster-admission row) exists
	initCtx, cancel := context.WithTimeout(ctx, adminOpTimeout)
	defer cancel()
	err = provider.Init(initCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize provider: %w", err)
	}

	return &Admin{
		provider:      provider,
		exclusive:     exclusive,
		owner:         uuid.NewString(),
		log:           opts.Logger,
		leaseTTL:      opts.ExclusiveLeaseDuration,
		renewInterval: opts.ExclusiveRenewInterval,
	}, nil
}

// AcquireExclusive takes an exclusive-access lease on the cluster
// It blocks new host registrations and causes running hosts to self-terminate on their next health check, then blocks until no host is connected
// With Force false, it returns components.ErrHostsConnected immediately if any host is currently connected
// With Force true, it waits until the cluster is empty (bounded by ctx and the health check deadline)
// On success it starts a background renewal loop and returns a channel that is closed if the lease is ever lost, so the caller can abort its maintenance operation
// The channel is not closed on a normal ReleaseExclusive or Close
func (a *Admin) AcquireExclusive(ctx context.Context, opts AcquireOptions) (lost <-chan struct{}, err error) {
	// Take the lease
	acquireCtx, cancel := context.WithTimeout(ctx, adminOpTimeout)
	_, err = a.exclusive.AcquireExclusiveLease(acquireCtx, a.owner, a.leaseTTL)
	cancel()
	if err != nil {
		return nil, err
	}

	// Start the renewal loop
	// The stop channel ends it on release or close, and the lost channel is closed if the lease can no longer be guaranteed
	stop := make(chan struct{})
	lostCh := make(chan struct{})
	a.mu.Lock()
	a.stop = stop
	a.lost = lostCh
	a.mu.Unlock()

	// The renewal loop outlives this call, so it detaches from ctx's cancellation while keeping its values
	go a.renewLoop(context.WithoutCancel(ctx), stop, lostCh)

	// Wait for the cluster to drain
	err = a.waitForEmpty(ctx, opts.Force)
	if err != nil {
		// Undo the lease and stop the renewal loop so a failed acquire does not hold the cluster or leak a goroutine
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), adminOpTimeout)
		_ = a.ReleaseExclusive(releaseCtx)
		releaseCancel()
		return nil, err
	}

	return lostCh, nil
}

// waitForEmpty blocks until no host is connected, or returns ErrHostsConnected when hosts are present and force is false
func (a *Admin) waitForEmpty(ctx context.Context, force bool) error {
	for {
		listCtx, cancel := context.WithTimeout(ctx, adminOpTimeout)
		hosts, err := a.provider.ListHosts(listCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to list hosts: %w", err)
		}
		if len(hosts) == 0 {
			return nil
		}
		if !force {
			return components.ErrHostsConnected
		}

		// Wait for the evicted hosts to notice the lease and drop out, re-checking on each tick
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(drainPollInterval):
		}
	}
}

// renewLoop keeps the lease alive until it is stopped, closing lost if the lease can no longer be guaranteed
func (a *Admin) renewLoop(ctx context.Context, stop <-chan struct{}, lost chan struct{}) {
	ticker := time.NewTicker(a.renewInterval)
	defer ticker.Stop()

	lastRenew := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, adminOpTimeout)
			_, err := a.exclusive.RenewExclusiveLease(renewCtx, a.owner, a.leaseTTL)
			cancel()

			switch {
			case err == nil:
				lastRenew = time.Now()
			case errors.Is(err, components.ErrExclusiveHeld):
				// The lease is definitively lost, so signal the caller to abort its operation
				a.log.Error("Exclusive-access lease lost, aborting")
				close(lost)
				return
			default:
				// A transient error
				// Keep trying, but give up before the lease could expire out from under us
				a.log.Warn("Failed to renew exclusive-access lease, will retry", slog.Any("error", err))
				if time.Since(lastRenew) >= a.leaseTTL-a.renewInterval {
					a.log.Error("Exclusive-access lease could no longer be guaranteed, aborting")
					close(lost)
					return
				}
			}
		}
	}
}

// ReleaseExclusive stops renewal and clears the lease if it is still held by this admin
// It is idempotent and safe to call in a deferred cleanup
func (a *Admin) ReleaseExclusive(ctx context.Context) error {
	a.stopRenew()

	err := a.exclusive.ReleaseExclusiveLease(ctx, a.owner)
	if err != nil {
		return fmt.Errorf("failed to release exclusive lease: %w", err)
	}
	return nil
}

// stopRenew ends the renewal loop if one is running
func (a *Admin) stopRenew() {
	a.mu.Lock()
	stop := a.stop
	a.stop = nil
	a.lost = nil
	a.mu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// Close stops any renewal loop
// It does not close the underlying database connection, which is owned by the caller through the provider options
func (a *Admin) Close() error {
	a.stopRenew()
	return nil
}
