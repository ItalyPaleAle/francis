package actorcore

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// echoActor is a minimal actor that echoes the decoded request back as the result
type echoActor struct{}

func (e *echoActor) Invoke(_ context.Context, method string, data actor.Envelope) (any, error) {
	var s string
	if data != nil {
		_ = data.Decode(&s)
	}
	return "echo:" + method + ":" + s, nil
}

// streamActor is a minimal actor that echoes the request body back through the streamed response
type streamActor struct{}

func (s *streamActor) InvokeStream(_ context.Context, method string, reqContentType string, body io.Reader, w actor.StreamResponseWriter) error {
	in, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	w.SetContentType(reqContentType)
	_, err = w.Write([]byte("stream:" + method + ":" + string(in)))
	return err
}

func streamFactory(_ string, _ *actor.Service) actor.Actor { return &streamActor{} }

// fakeResolver is a scripted PlacementResolver: each Resolve call returns the next placement, and IsLocal compares the placement to localHostID
type fakeResolver struct {
	localHostID  string
	placements   []*Placement
	resolveErrs  []error
	resolveCalls int
	confirmErr   error
	confirmCalls int
	invalidated  int
}

func (f *fakeResolver) Resolve(_ context.Context, _ ref.ActorRef, _ bool, _ bool) (*Placement, error) {
	i := f.resolveCalls
	f.resolveCalls++
	if i < len(f.resolveErrs) && f.resolveErrs[i] != nil {
		return nil, f.resolveErrs[i]
	}
	if i < len(f.placements) {
		return f.placements[i], nil
	}
	return nil, errors.New("fakeResolver: no more placements")
}

func (f *fakeResolver) ConfirmLocal(_ context.Context, _ ref.ActorRef) error {
	f.confirmCalls++
	return f.confirmErr
}

func (f *fakeResolver) Invalidate(_ ref.ActorRef) { f.invalidated++ }

func (f *fakeResolver) IsLocal(p *Placement) bool {
	return p != nil && p.HostID == f.localHostID
}

// fakePeer is a scripted PeerInvoker: each InvokeObject call returns the next result
type fakePeer struct {
	results  []peerObjResult
	calls    int
	lastReq  protocol.InvokeActorRequest
	lastAddr string
}

type peerObjResult struct {
	resp protocol.InvokeActorResponse
	err  *protocol.Error
}

func (f *fakePeer) InvokeObject(_ context.Context, address string, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	f.lastReq = req
	f.lastAddr = address
	i := f.calls
	f.calls++
	if i < len(f.results) {
		return f.results[i].resp, f.results[i].err
	}
	return protocol.InvokeActorResponse{}, protocol.NewError(protocol.ErrCodeInternal, "fakePeer: no more results")
}

func (f *fakePeer) InvokeStream(_ context.Context, _ string, _ protocol.InvokeActorRequest, _ io.Reader) (string, io.ReadCloser, *protocol.Error) {
	return "", nil, protocol.NewError(protocol.ErrCodeInternal, "fakePeer: stream not implemented")
}

// newMessagingManager builds a Manager wired with a factory for the "testactor" type
func newMessagingManager(t *testing.T, factory actor.Factory) *Manager {
	t.Helper()
	clock := clocktesting.NewFakeClock(time.Now())
	m := &Manager{
		Actors:              haxmap.New[string, *ActiveActor](8),
		log:                 slog.New(slog.DiscardHandler),
		clock:               clock,
		shutdownGracePeriod: 5 * time.Second,
		ActorsConfig: map[string]components.ActorHostType{
			"testactor": {IdleTimeout: 5 * time.Minute},
		},
		ActorFactories: map[string]actor.Factory{
			"testactor": factory,
		},
	}
	m.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
		ExecuteFn: m.HandleIdleActor,
		Clock:     clock,
	})
	t.Cleanup(func() { _ = m.IdleProcessor.Close() })
	return m
}

func echoFactory(_ string, _ *actor.Service) actor.Actor { return &echoActor{} }

func decodeEnvelope(t *testing.T, env actor.Envelope) string {
	t.Helper()
	require.NotNil(t, env)
	var s string
	require.NoError(t, env.Decode(&s))
	return s
}

func TestManagerInvokeLocal(t *testing.T) {
	t.Run("claims and invokes a local actor", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}},
		}
		peer := &fakePeer{}

		env, err := m.Invoke(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "x", false)
		require.NoError(t, err)
		assert.Equal(t, "echo:ping:x", decodeEnvelope(t, env))

		// The actor was not active, so it was authoritatively claimed before activation
		assert.Equal(t, 1, resolver.confirmCalls)
		assert.Equal(t, 0, peer.calls)
	})

	t.Run("a warm local actor is invoked without a claim", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}, {HostID: "h1", Address: "addr1"}},
		}
		peer := &fakePeer{}
		aRef := ref.NewActorRef("testactor", "a1")

		// First call activates the actor and claims it
		_, err := m.Invoke(t.Context(), resolver, peer, aRef, "ping", "x", false)
		require.NoError(t, err)
		// Second call finds it already active, so no further claim is made
		_, err = m.Invoke(t.Context(), resolver, peer, aRef, "ping", "y", false)
		require.NoError(t, err)

		assert.Equal(t, 1, resolver.confirmCalls)
	})

	t.Run("a stale local placement re-resolves to the owning peer", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		// The first resolution routes here (stale), the second routes to the real owner
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}, {HostID: "h2", Address: "addr2"}},
			confirmErr:  actor.ErrActorNotHosted,
		}
		out, _ := msgpack.Marshal("from-peer")
		peer := &fakePeer{results: []peerObjResult{{resp: protocol.InvokeActorResponse{Data: out}}}}

		env, err := m.Invoke(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "x", false)
		require.NoError(t, err)
		assert.Equal(t, "from-peer", decodeEnvelope(t, env))

		// The stale placement was invalidated and the call retried against the peer
		assert.Equal(t, 1, resolver.invalidated)
		assert.Equal(t, 2, resolver.resolveCalls)
		assert.Equal(t, 1, peer.calls)
		assert.Equal(t, "addr2", peer.lastAddr)
	})
}

func TestManagerInvokePeer(t *testing.T) {
	t.Run("invokes an actor on a peer", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h2", Address: "addr2"}},
		}
		out, _ := msgpack.Marshal("peer-result")
		peer := &fakePeer{results: []peerObjResult{{resp: protocol.InvokeActorResponse{Data: out}}}}

		env, err := m.Invoke(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "x", false)
		require.NoError(t, err)
		assert.Equal(t, "peer-result", decodeEnvelope(t, env))
		assert.Equal(t, "h2", peer.lastReq.TargetHostID)
		assert.Equal(t, 0, resolver.confirmCalls)
	})

	t.Run("a stale peer placement is invalidated and retried once", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h2", Address: "addr2"}, {HostID: "h3", Address: "addr3"}},
		}
		out, _ := msgpack.Marshal("second-peer")
		peer := &fakePeer{results: []peerObjResult{
			{err: protocol.NewError(protocol.ErrCodeHostMismatch, "stale")},
			{resp: protocol.InvokeActorResponse{Data: out}},
		}}

		env, err := m.Invoke(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "x", false)
		require.NoError(t, err)
		assert.Equal(t, "second-peer", decodeEnvelope(t, env))
		assert.Equal(t, 1, resolver.invalidated)
		assert.Equal(t, 2, peer.calls)
		assert.Equal(t, "addr3", peer.lastAddr)
	})
}

func TestManagerPeerInvokeObject(t *testing.T) {
	t.Run("claims and invokes for a peer caller", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{localHostID: "h1"}

		argData, _ := msgpack.Marshal("hi")
		resp, perr := m.PeerInvokeObject(t.Context(), resolver, protocol.InvokeActorRequest{
			ActorType: "testactor",
			ActorID:   "a1",
			Method:    "ping",
			Data:      argData,
		})
		require.Nil(t, perr)
		assert.Equal(t, 1, resolver.confirmCalls)

		var got string
		require.NoError(t, msgpack.Unmarshal(resp.Data, &got))
		assert.Equal(t, "echo:ping:hi", got)
	})

	t.Run("an actor owned elsewhere is rejected with actor_not_hosted", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{localHostID: "h1", confirmErr: actor.ErrActorNotHosted}

		_, perr := m.PeerInvokeObject(t.Context(), resolver, protocol.InvokeActorRequest{
			ActorType: "testactor",
			ActorID:   "a1",
			Method:    "ping",
		})
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeActorNotHosted, perr.Code)
	})

	t.Run("an active-only invocation never claims and rejects an inactive actor", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{localHostID: "h1"}

		_, perr := m.PeerInvokeObject(t.Context(), resolver, protocol.InvokeActorRequest{
			ActorType:  "testactor",
			ActorID:    "a1",
			Method:     "ping",
			ActiveOnly: true,
		})
		require.NotNil(t, perr)
		assert.Equal(t, protocol.ErrCodeActorNotActive, perr.Code)
		assert.Equal(t, 0, resolver.confirmCalls)
	})
}

func TestManagerInvokeStreamStaleLocal(t *testing.T) {
	t.Run("a stale local placement is invalidated when the actor is owned elsewhere", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		// A stale cache routes us here, but the claim reveals the actor is owned by another host
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}},
			confirmErr:  actor.ErrActorNotHosted,
		}
		peer := &fakePeer{}

		_, _, err := m.InvokeStream(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "", nil, false)
		require.ErrorIs(t, err, actor.ErrActorNotHosted)

		// The one-shot body cannot be replayed, so the call is not retried, but the stale entry is dropped so the next call re-resolves
		assert.Equal(t, 1, resolver.invalidated)
		assert.Equal(t, 1, resolver.resolveCalls)
	})

	t.Run("an inactive local placement is invalidated for an active-only invocation", func(t *testing.T) {
		m := newMessagingManager(t, echoFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}},
		}
		peer := &fakePeer{}

		// An active-only stream invocation never claims the actor and finds it inactive here
		_, _, err := m.InvokeStream(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "", nil, true)
		require.ErrorIs(t, err, actor.ErrActorNotActive)

		assert.Equal(t, 1, resolver.invalidated)
		assert.Equal(t, 0, resolver.confirmCalls)
	})

	t.Run("a healthy local stream is not invalidated", func(t *testing.T) {
		m := newMessagingManager(t, streamFactory)
		resolver := &fakeResolver{
			localHostID: "h1",
			placements:  []*Placement{{HostID: "h1", Address: "addr1"}},
		}
		peer := &fakePeer{}

		ct, resp, err := m.InvokeStream(t.Context(), resolver, peer, ref.NewActorRef("testactor", "a1"), "ping", "text/plain", strings.NewReader("hi"), false)
		require.NoError(t, err)
		assert.Equal(t, "text/plain", ct)
		got, err := io.ReadAll(resp)
		require.NoError(t, err)
		require.NoError(t, resp.Close())
		assert.Equal(t, "stream:ping:hi", string(got))

		// A successful invocation leaves the cached placement in place
		assert.Equal(t, 0, resolver.invalidated)
		assert.Equal(t, 1, resolver.confirmCalls)
	})
}
