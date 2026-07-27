//go:build integration

// Package netfault provides a severable network link that a scenario can place in front of a Francis endpoint
//
// Hosts and runtimes speak WebTransport, which is QUIC over UDP, so a link is a UDP relay: clients are pointed at the link's address and it forwards their datagrams to the real endpoint and the replies back
// Severing the link makes it a black hole, dropping datagrams in both directions without closing any socket, which is what a cut network looks like to QUIC: no resets, no refusals, just silence until the idle timers fire
// Restoring it lets datagrams flow again, so a scenario can assert that the cluster heals once connectivity returns
package netfault

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// readBufferSize is comfortably larger than any QUIC datagram, so a relayed packet is never truncated
const readBufferSize = 65535

// Link is a UDP relay in front of an upstream endpoint, which a scenario can sever and restore
type Link struct {
	listenAddr string
	upstream   string

	severed atomic.Bool
	closed  atomic.Bool

	conn         *net.UDPConn
	upstreamAddr *net.UDPAddr

	// flows maps a client address to the socket used to reach the upstream on its behalf, so replies come back on the same path
	mu    sync.Mutex
	flows map[string]*net.UDPConn

	wg sync.WaitGroup
}

// New returns a link that listens on listenAddr and forwards to upstreamAddr
// It does not bind anything: pass it to framework.Run, ahead of the processes that depend on it
func New(listenAddr string, upstreamAddr string) *Link {
	return &Link{
		listenAddr: listenAddr,
		upstream:   upstreamAddr,
		flows:      make(map[string]*net.UDPConn),
	}
}

// Address returns the address clients should be pointed at to reach the upstream through this link
func (l *Link) Address() string {
	return l.listenAddr
}

func (l *Link) Run(t *testing.T) {
	t.Helper()

	listenAddr, err := net.ResolveUDPAddr("udp", l.listenAddr)
	require.NoError(t, err, "failed to resolve link address %s", l.listenAddr)
	upstreamAddr, err := net.ResolveUDPAddr("udp", l.upstream)
	require.NoError(t, err, "failed to resolve upstream address %s", l.upstream)
	l.upstreamAddr = upstreamAddr

	conn, err := net.ListenUDP("udp", listenAddr)
	require.NoError(t, err, "failed to listen on link address %s", l.listenAddr)
	l.conn = conn

	// Relay in background so Run returns as soon as the link is carrying traffic
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.relayFromClients()
	}()
}

// Sever turns the link into a black hole, dropping every datagram in both directions until Restore
// Sockets stay open, so neither side sees a reset: they see a network that has gone quiet
func (l *Link) Sever(t *testing.T) {
	t.Helper()
	l.severed.Store(true)
}

// Restore lets datagrams flow again
// The endpoints keep their sockets across the outage, so a connection that has not yet timed out resumes and one that has is re-established by the client's own reconnect logic
func (l *Link) Restore(t *testing.T) {
	t.Helper()
	l.severed.Store(false)
}

func (l *Link) Cleanup(t *testing.T) {
	t.Helper()

	if !l.closed.CompareAndSwap(false, true) {
		return
	}

	// Closing the sockets unblocks every relay goroutine
	if l.conn != nil {
		_ = l.conn.Close()
	}

	l.mu.Lock()
	for _, up := range l.flows {
		_ = up.Close()
	}
	clear(l.flows)
	l.mu.Unlock()

	l.wg.Wait()
}

// relayFromClients forwards datagrams from any client to the upstream, starting a return path the first time a client is seen
func (l *Link) relayFromClients() {
	buf := make([]byte, readBufferSize)
	for {
		n, client, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			// The listener was closed by Cleanup, so the link is done
			return
		}

		// A severed link drops the datagram without telling the sender
		if l.severed.Load() {
			continue
		}

		up := l.upstreamFor(client)
		if up == nil {
			continue
		}
		_, _ = up.Write(buf[:n])
	}
}

// upstreamFor returns the socket that carries this client's traffic to the upstream, creating it and its return path on first use
func (l *Link) upstreamFor(client *net.UDPAddr) *net.UDPConn {
	key := client.String()

	l.mu.Lock()
	defer l.mu.Unlock()

	// A closed link must not resurrect a flow after Cleanup has drained the goroutines
	if l.closed.Load() {
		return nil
	}

	up, ok := l.flows[key]
	if ok {
		return up
	}

	up, err := net.DialUDP("udp", nil, l.upstreamAddr)
	if err != nil {
		return nil
	}
	l.flows[key] = up

	// Each flow gets its own return path, writing replies back to the client through the listening socket
	clientAddr := *client
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.relayToClient(up, &clientAddr)
	}()

	return up
}

// relayToClient forwards datagrams coming back from the upstream to the client this flow belongs to
func (l *Link) relayToClient(up *net.UDPConn, client *net.UDPAddr) {
	buf := make([]byte, readBufferSize)
	for {
		n, err := up.Read(buf)
		if err != nil {
			// The flow's socket was closed by Cleanup
			return
		}

		// Replies are dropped too, so a severed link is symmetric
		if l.severed.Load() {
			continue
		}

		_, _ = l.conn.WriteToUDP(buf[:n], client)
	}
}
