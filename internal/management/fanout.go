package management

import (
	"context"
	"net/http"
	"sync"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// snapshotResult is the outcome of a snapshot request sent to one host
type snapshotResult struct {
	Host     components.HostDetails
	Snapshot protocol.HostSnapshotResponse
	Err      error
}

// snapshotHosts sends a snapshot request to every host with a bounded number of concurrent requests
// Results are returned in the order of the hosts
func (s *Server) snapshotHosts(ctx context.Context, hosts []components.HostDetails, req protocol.HostSnapshotRequest) []snapshotResult {
	res := make([]snapshotResult, len(hosts))
	parallelFor(len(hosts), s.fanOutConcurrency, func(i int) {
		res[i].Host = hosts[i]
		res[i].Snapshot, res[i].Err = s.hostSnapshot(ctx, hosts[i], req)
	})
	return res
}

// parallelFor calls fn for every index from 0 to n-1, running at most concurrency calls at a time
func parallelFor(n int, concurrency int, fn func(i int)) {
	sem := make(chan struct{}, max(concurrency, 1))
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() {
				<-sem
			}()
			fn(i)
		})
	}
	wg.Wait()
}

// hostSnapshot sends one snapshot request to a host, bounded by the host timeout
func (s *Server) hostSnapshot(ctx context.Context, host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	hostCtx, cancel := context.WithTimeout(ctx, s.hostTimeout)
	defer cancel()
	return s.backend.HostSnapshot(hostCtx, host, req)
}

// hostContext returns a context for a request sent to a host on behalf of an HTTP request
func (s *Server) hostContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.hostTimeout)
}

// hostErrorJSON reports a host that could not be queried
type hostErrorJSON struct {
	HostID  string `json:"hostId"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func newHostError(hostID string, err error) hostErrorJSON {
	apiErr := backendError(err)
	if apiErr != nil {
		return hostErrorJSON{HostID: hostID, Code: apiErr.Code, Message: err.Error()}
	}
	return hostErrorJSON{HostID: hostID, Code: CodeHostUnavailable, Message: err.Error()}
}
