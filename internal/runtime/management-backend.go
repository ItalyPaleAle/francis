package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

const (
	// runtimePeerTimeout bounds a request to another runtime replica that does not reach a host
	runtimePeerTimeout = 5 * time.Second
	// maxRouteAttempts bounds how many times a request is routed again after the host's session moved
	maxRouteAttempts = 3
)

// managementBackend implements management.Backend for the standalone runtime
// A host is reached through the replica that owns its session: this replica directly, or another replica over the runtime peer endpoint
type managementBackend struct {
	rt *Runtime
}

func (b *managementBackend) Topology() management.Topology {
	return management.TopologyRemote
}

func (b *managementBackend) Provider() components.ActorProvider {
	return b.rt.provider
}

func (b *managementBackend) HostReachability(ctx context.Context, hosts []components.HostDetails) map[string]management.HostReachability {
	res := make(map[string]management.HostReachability, len(hosts))

	// Hosts owned by this replica are checked locally, and the other owners are asked for their sessions once each
	remote := map[string]struct{}{}
	for _, h := range hosts {
		switch {
		case h.RuntimeID == b.rt.runtimeID:
			c, ok := b.rt.hosts.Get(h.HostID)
			res[h.HostID] = management.HostReachability{
				Reachable:      ok && c.SessionID() == h.SessionID,
				OwnerRuntimeID: h.RuntimeID,
			}
		case h.RuntimeID != "":
			remote[h.RuntimeID] = struct{}{}
		}
	}
	if len(remote) == 0 {
		return res
	}

	// Query the other owners concurrently
	sessions := b.remoteSessions(ctx, remote)
	for _, h := range hosts {
		if h.RuntimeID == "" || h.RuntimeID == b.rt.runtimeID {
			continue
		}
		owned, ok := sessions[h.RuntimeID]
		res[h.HostID] = management.HostReachability{
			Reachable:      ok && owned.err == nil && owned.sessions[h.HostID] == h.SessionID,
			OwnerRuntimeID: h.RuntimeID,
		}
	}

	return res
}

// runtimeSessions is the result of asking a replica for its host sessions
type runtimeSessions struct {
	// sessions maps each host ID to its session ID
	sessions map[string]string
	err      error
}

// remoteSessions asks each of the given replicas for its host sessions
func (b *managementBackend) remoteSessions(ctx context.Context, runtimeIDs map[string]struct{}) map[string]runtimeSessions {
	res := make(map[string]runtimeSessions, len(runtimeIDs))

	addresses, err := b.runtimeAddresses(ctx)
	if err != nil {
		for id := range runtimeIDs {
			res[id] = runtimeSessions{err: err}
		}
		return res
	}

	var (
		lock sync.Mutex
		wg   sync.WaitGroup
	)
	for id := range runtimeIDs {
		wg.Go(func() {
			owned, err := b.querySessions(ctx, id, addresses[id])
			lock.Lock()
			res[id] = runtimeSessions{sessions: owned, err: err}
			lock.Unlock()
		})
	}
	wg.Wait()

	return res
}

// querySessions asks one replica for its host sessions
func (b *managementBackend) querySessions(ctx context.Context, runtimeID string, address string) (map[string]string, error) {
	err := checkPeerAddress(runtimeID, address)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, runtimePeerTimeout)
	defer cancel()

	var resp protocol.RuntimeSessionsResponse
	perr := b.rt.peers.SendManagement(ctx, address, runtimeID, protocol.KindRuntimeSessions, struct{}{}, &resp)
	if perr != nil {
		return nil, perr
	}

	res := make(map[string]string, len(resp.Sessions))
	for _, s := range resp.Sessions {
		res[s.HostID] = s.SessionID
	}
	return res, nil
}

// checkPeerAddress returns an error when another replica can't be reached at the address its membership advertises
// Dialing an unspecified address would reach this replica itself and fail with a confusing identity mismatch, so it is refused with an explanation instead
func checkPeerAddress(runtimeID string, address string) error {
	switch {
	case address == "":
		return fmt.Errorf("runtime %s has no live membership", runtimeID)
	case isUnspecifiedAddress(address):
		return fmt.Errorf("runtime %s advertises the address '%s', which other replicas can't dial; set advertiseAddress on that replica", runtimeID, address)
	default:
		return nil
	}
}

// runtimeAddresses returns the peer address of every replica with a live membership
func (b *managementBackend) runtimeAddresses(ctx context.Context) (map[string]string, error) {
	runtimes, err := b.rt.provider.ListRuntimes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list runtimes: %w", err)
	}

	res := make(map[string]string, len(runtimes))
	for _, r := range runtimes {
		res[r.RuntimeID] = r.Address
	}
	return res, nil
}

func (b *managementBackend) HostSnapshot(ctx context.Context, host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	var res protocol.HostSnapshotResponse
	err := b.route(ctx, host,
		func(c *hostConn) (err error) {
			res, err = b.rt.hostSnapshot(ctx, c, req)
			return err
		},
		protocol.KindRuntimeHostSnapshot, protocol.RuntimeHostRequest{Snapshot: &req}, &res,
	)
	return res, err
}

func (b *managementBackend) DrainHost(ctx context.Context, host components.HostDetails, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
	var res protocol.HostDrainResponse
	err := b.route(ctx, host,
		func(c *hostConn) (err error) {
			res, err = b.rt.drainHostConn(ctx, c, req)
			return err
		},
		protocol.KindRuntimeHostDrain, protocol.RuntimeHostRequest{Drain: &req}, &res,
	)
	return res, err
}

func (b *managementBackend) DeactivateActor(ctx context.Context, host components.HostDetails, actorType string, actorID string) (bool, error) {
	var res protocol.TerminateActorResponse
	err := b.route(ctx, host,
		func(c *hostConn) (err error) {
			res.NotActive, err = b.rt.deactivateOnHost(ctx, c, actorType, actorID)
			return err
		},
		protocol.KindRuntimeDeactivateActor, protocol.RuntimeHostRequest{Terminate: &protocol.TerminateActorRequest{ActorType: actorType, ActorID: actorID}}, &res,
	)
	return res.NotActive, err
}

// route delivers a request to the replica that owns the host's session
// When the session moved before the request was delivered, the host is read again and the request routed to its new owner, a bounded number of times
// A move shows up as a reattached host when the same replica holds a new session, and as an unavailable host when the session moved to another replica, so both are checked against a fresh read
func (b *managementBackend) route(ctx context.Context, host components.HostDetails, local func(c *hostConn) error, kind string, req protocol.RuntimeHostRequest, out any) error {
	for attempt := 1; ; attempt++ {
		err := b.routeOnce(ctx, host, local, kind, req, out)
		maybeMoved := errors.Is(err, management.ErrHostReattached) || errors.Is(err, management.ErrHostUnavailable)
		if !maybeMoved || attempt >= maxRouteAttempts || ctx.Err() != nil {
			return err
		}

		// Read the host again to find its current session and owner
		fresh, gErr := b.rt.provider.GetHostDetails(ctx, host.HostID)
		if errors.Is(gErr, components.ErrHostUnregistered) {
			return fmt.Errorf("%w: host %s is no longer registered", management.ErrHostUnavailable, host.HostID)
		} else if gErr != nil {
			return err
		}

		if fresh.SessionID == host.SessionID && fresh.RuntimeID == host.RuntimeID {
			return err
		}

		host = fresh
	}
}

// routeOnce delivers a request to the replica the host row names as the owner of its session
func (b *managementBackend) routeOnce(ctx context.Context, host components.HostDetails, local func(c *hostConn) error, kind string, req protocol.RuntimeHostRequest, out any) error {
	// This replica owns the session
	if host.RuntimeID == b.rt.runtimeID {
		c, perr := b.rt.ownedHostConn(host.HostID, host.SessionID)
		if perr != nil {
			return management.FromProtocolError(perr)
		}
		return management.FromProtocolError(local(c))
	}

	if host.RuntimeID == "" {
		return fmt.Errorf("%w: no runtime holds a session for host %s", management.ErrHostUnavailable, host.HostID)
	}

	// Another replica owns the session, so forward the request to it
	addresses, err := b.runtimeAddresses(ctx)
	if err != nil {
		return err
	}
	address := addresses[host.RuntimeID]
	err = checkPeerAddress(host.RuntimeID, address)
	if err != nil {
		return fmt.Errorf("%w: cannot reach the runtime that owns the session of host %s: %w", management.ErrHostUnavailable, host.HostID, err)
	}

	req.HostID = host.HostID
	req.SessionID = host.SessionID
	perr := b.rt.peers.SendManagement(ctx, address, host.RuntimeID, kind, req, out)
	if perr != nil {
		return management.FromProtocolError(perr)
	}

	return nil
}

func (b *managementBackend) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (bool, error) {
	_, created, err := b.rt.storeJob(ctx, aRef, req)
	return created, err
}

func (b *managementBackend) Runtimes(ctx context.Context) ([]management.RuntimeStatus, error) {
	runtimes, err := b.rt.provider.ListRuntimes(ctx)
	if err != nil {
		return nil, err
	}

	res := make([]management.RuntimeStatus, len(runtimes))
	var wg sync.WaitGroup
	for i, r := range runtimes {
		res[i].RuntimeInfo = r

		// This replica counts its own sessions
		if r.RuntimeID == b.rt.runtimeID {
			n := b.rt.hosts.Count()
			res[i].Self = true
			res[i].ConnectedHosts = &n
			continue
		}

		wg.Go(func() {
			owned, err := b.querySessions(ctx, r.RuntimeID, r.Address)
			if err != nil {
				res[i].Error = err.Error()
				return
			}
			n := len(owned)
			res[i].ConnectedHosts = &n
		})
	}
	wg.Wait()

	return res, nil
}
