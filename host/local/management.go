package local

import (
	"cmp"
	"context"
	"crypto/tls"
	"log/slog"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// ManagementOptions configures the management API served by a local host
type ManagementOptions struct {
	// Bind is the TCP address the management API listens on, which defaults to 127.0.0.1:7401
	// The default only accepts connections from the same machine, since an embedded host has no container boundary around it
	Bind string
	// ReadOnlyTokens are bearer tokens that receive every scope except those ending in ":manage"
	// Each token must be at least 32 characters long
	ReadOnlyTokens []string
	// ManagementTokens are bearer tokens that receive every scope
	// Each token must be at least 32 characters long
	ManagementTokens []string
	// TLSConfig optionally serves the management API over HTTPS
	TLSConfig *tls.Config
}

// WithManagementAPI enables the management REST API on this host
// Any host of the cluster that enables it can inspect and manage every other host, through their peer servers
func WithManagementAPI(opts ManagementOptions) HostOption {
	return func(o *newHostOptions) { o.Management = &opts }
}

// defaultManagementBind is the listen address of a local host's management API when the options do not set one
const defaultManagementBind = "127.0.0.1:7401"

// newManagementServer returns the management API server for the host
func (h *Host) newManagementServer(opts ManagementOptions, log *slog.Logger) (*management.Server, error) {
	cfg := management.Config{
		Bind:             cmp.Or(opts.Bind, defaultManagementBind),
		ReadOnlyTokens:   opts.ReadOnlyTokens,
		ManagementTokens: opts.ManagementTokens,
		TLSConfig:        opts.TLSConfig,
	}

	return management.NewServer(management.ServerOptions{
		Config:  cfg,
		Backend: &managementBackend{h: h},
		Logger:  log,
	})
}

// managementBackend implements management.Backend for a local host
// Every host is reached through its own peer server, and requests for this host are handled in-process
type managementBackend struct {
	h *Host
}

func (b *managementBackend) Topology() management.Topology {
	return management.TopologyLocal
}

func (b *managementBackend) Provider() components.ActorProvider {
	return b.h.actorProvider
}

func (b *managementBackend) HostReachability(_ context.Context, hosts []components.HostDetails) map[string]management.HostReachability {
	// Every registered host serves its own peer server, so it is reachable as long as it is registered
	res := make(map[string]management.HostReachability, len(hosts))
	for _, h := range hosts {
		res[h.HostID] = management.HostReachability{Reachable: true}
	}
	return res
}

func (b *managementBackend) HostSnapshot(ctx context.Context, host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	res, err := b.h.managementSnapshot(ctx, host.HostID, host.Address, req)
	return res, management.FromProtocolError(err)
}

func (b *managementBackend) DrainHost(ctx context.Context, host components.HostDetails, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
	res, err := b.h.managementDrain(ctx, host.HostID, host.Address, req)
	return res, management.FromProtocolError(err)
}

func (b *managementBackend) DeactivateActor(ctx context.Context, host components.HostDetails, actorType string, actorID string) (bool, error) {
	notActive, err := b.h.managementDeactivate(ctx, host.HostID, host.Address, actorType, actorID)
	return notActive, management.FromProtocolError(err)
}

func (b *managementBackend) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (bool, error) {
	_, created, err := b.h.storeJob(ctx, aRef, req)
	return created, err
}

func (b *managementBackend) Runtimes(context.Context) ([]management.RuntimeStatus, error) {
	return nil, management.ErrNotApplicable
}
