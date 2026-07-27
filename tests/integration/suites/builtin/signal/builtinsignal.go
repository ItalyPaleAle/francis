//go:build integration

// Package signal exercises the built-in signal actor end to end:
//
//   - one completion releases every caller waiting on the signal, from any host in the cluster
//   - a caller arriving after the completion is answered immediately, including once the actor has been deactivated
//   - a wait that is interrupted by its actor being halted re-resolves on its own and is still released
//   - only the first completion takes effect, whichever host issues it
//   - a signal rejects Peek, because a shared-lock actor has no read-only invocations
//   - clients cannot invoke a built-in actor directly
package signal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/signal"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/builtinkey"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
)

// waitTimeout bounds a wait that the scenario expects to be released, so a hung wait fails the test instead of the suite deadline
const waitTimeout = 45 * time.Second

// payload is what the scenarios broadcast, so waiters assert on real data rather than just on being released
type payload struct {
	Version string
}

// matrix runs the scenario across representative topology/provider combinations
// Multi-host entries are the point of this suite: they put parked waiters on hosts that do not own the signal, so the completion has to travel over the peer transport to release them
var matrix = []struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int
}{
	{cluster.Local, provider.SQLite, 2},
	{cluster.Local, provider.StandaloneMemory, 1},
	{cluster.Remote, provider.Postgres, 2},
}

func init() {
	for _, m := range matrix {
		suite.Register(&builtinSignal{kind: m.kind, variant: m.variant, hosts: m.hosts})
	}

	// Host failover needs a survivor to drive the cluster after one host is gone
	for _, m := range matrix {
		if m.hosts > 1 {
			suite.Register(&signalFailover{kind: m.kind, variant: m.variant, hosts: m.hosts})
		}
	}
}

// builtinSignal drives a cluster whose hosts register a built-in signal actor and asserts its broadcast behavior
type builtinSignal struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster    *cluster.Cluster
	sig        *signal.Signal
	signalType string
}

func (s *builtinSignal) Name() string {
	return "builtinsignal/" + string(s.kind) + "/" + string(s.variant)
}

func (s *builtinSignal) Setup(t *testing.T) []framework.Option {
	// Retention is left at the default, since these scenarios read completions back immediately rather than testing expiry
	sigActor, err := signal.New("e2e")
	require.NoError(t, err)

	s.sig = sigActor

	// The host registers the actor under the reserved prefix, so the direct-target guard and the halt probe use the full type
	s.signalType = builtinactor.FullActorType(sigActor.ActorType())

	s.cluster = cluster.New(t, cluster.Options{
		Kind:          s.kind,
		Variant:       s.variant,
		Hosts:         s.hosts,
		BuiltInActors: []builtinactor.BuiltInActor{sigActor},
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *builtinSignal) Run(t *testing.T) {
	ctx := t.Context()

	// One completion releases every waiter, wherever in the cluster it is waiting from
	t.Run("broadcasts to waiters on every host", func(t *testing.T) {
		const (
			signalID         = "broadcast-1"
			waitersPerHost   = 5
			completingHostIx = 0
		)

		waiters := s.parkWaiters(t, signalID, waitersPerHost)

		// Give the waiters time to reach the owning host, so the completion really does have to release parked calls
		time.Sleep(time.Second)

		err := s.svc(completingHostIx).Complete(ctx, signalID, payload{Version: "v2"})
		require.NoError(t, err)

		waiters.assertAllReleased(t, "v2")
	})

	// A caller that shows up after the completion never blocks, on any host
	t.Run("answers a caller arriving after the completion", func(t *testing.T) {
		const signalID = "late-1"

		err := s.svc(0).Complete(ctx, signalID, payload{Version: "v3"})
		require.NoError(t, err)

		for i := range s.cluster.Len() {
			waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)

			env, waitErr := s.svc(i).Wait(waitCtx, signalID)
			require.NoError(t, waitErr, "host %d", i)

			var got payload
			require.NoError(t, env.Decode(&got), "host %d", i)
			assert.Equal(t, "v3", got.Version, "host %d", i)

			cancel()
		}
	})

	// Once the activation is gone, the answer has to come from the durable completion record
	t.Run("answers from the durable record after the actor is deactivated", func(t *testing.T) {
		const signalID = "durable-1"

		err := s.svc(0).Complete(ctx, signalID, payload{Version: "v4"})
		require.NoError(t, err)

		s.haltOwner(t, signalID)

		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()

		env, err := s.svc(0).Wait(waitCtx, signalID)
		require.NoError(t, err)

		var got payload
		require.NoError(t, env.Decode(&got))
		assert.Equal(t, "v4", got.Version, "a deactivated signal should be answered from its durable record")
	})

	// A parked wait whose actor is halted underneath it must re-resolve on its own, so callers never see the interruption
	t.Run("survives its actor being halted mid-wait", func(t *testing.T) {
		const signalID = "halted-1"

		waiters := s.parkWaiters(t, signalID, 3)
		time.Sleep(time.Second)

		// Halting the owning activation releases the parked invocations with a retryable error, which the wait absorbs by re-resolving
		s.haltOwner(t, signalID)

		// The waiters must be parked again on the new activation before the completion lands, which is the whole point of the retry loop
		time.Sleep(time.Second)

		err := s.svc(0).Complete(ctx, signalID, payload{Version: "v5"})
		require.NoError(t, err)

		waiters.assertAllReleased(t, "v5")
	})

	// A signal fires once, and the host that issues the second completion does not change that
	t.Run("only the first completion takes effect", func(t *testing.T) {
		const signalID = "idempotent-1"

		err := s.svc(0).Complete(ctx, signalID, payload{Version: "first"})
		require.NoError(t, err)

		// Use a different host when there is one, so the second completion crosses the cluster to reach the same instance
		other := (1) % s.cluster.Len()
		err = s.svc(other).Complete(ctx, signalID, payload{Version: "second"})
		require.ErrorIs(t, err, signal.ErrAlreadyCompleted)

		waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
		defer cancel()

		env, err := s.svc(other).Wait(waitCtx, signalID)
		require.NoError(t, err)

		var got payload
		require.NoError(t, env.Decode(&got))
		assert.Equal(t, "first", got.Version, "the first completion's payload is the one callers receive")
	})

	// A signal runs its invocations under the shared lock and mutates its own state, so none of them are read-only
	t.Run("rejects a Peek", func(t *testing.T) {
		for i := range s.cluster.Len() {
			// The privileged client is how the framework reaches a built-in actor, and it is the only way to aim a Peek at one
			client := actor.NewBuiltInActorClient[any](builtinkey.Key{}, s.signalType, "peek-1", s.cluster.Service(i))

			_, err := client.Peek(ctx, s.signalType, "peek-1", "check", nil)
			require.Error(t, err, "host %d", i)
			require.ErrorContains(t, err, "does not implement the requested method", "host %d", i)
		}
	})

	// Clients cannot target a built-in actor through the public Service, on any host
	t.Run("cannot be targeted directly", func(t *testing.T) {
		for i := range s.cluster.Len() {
			s.assertClientRejected(t, s.cluster.Service(i), i)
		}
	})
}

// svc returns the signal service bound to the i-th host
func (s *builtinSignal) svc(i int) *signal.SignalService {
	return s.sig.Service(s.cluster.Service(i))
}

// parkWaiters starts perHost waiters on every host and returns a handle for asserting they are all released
// Each host gets its own service, so the waiters exercise both the local path and the peer transport
func (s *builtinSignal) parkWaiters(t *testing.T, signalID string, perHost int) *waiterGroup {
	t.Helper()

	total := perHost * s.cluster.Len()
	g := &waiterGroup{
		results: make(chan payload, total),
		errs:    make(chan error, total),
		total:   total,
	}

	for i := range s.cluster.Len() {
		svc := s.svc(i)
		for range perHost {
			g.wg.Add(1)
			go func() {
				defer g.wg.Done()

				waitCtx, cancel := context.WithTimeout(t.Context(), waitTimeout)
				defer cancel()

				env, err := svc.Wait(waitCtx, signalID)
				if err != nil {
					g.errs <- err
					return
				}

				var got payload
				err = env.Decode(&got)
				if err != nil {
					g.errs <- err
					return
				}
				g.results <- got
			}()
		}
	}

	return g
}

// haltOwner deactivates a signal's activation on whichever host currently owns it
// Halt reports actor.ErrActorNotHosted from the hosts that do not own the actor, so it doubles as the placement probe
func (s *builtinSignal) haltOwner(t *testing.T, signalID string) {
	t.Helper()

	var halted bool
	for i := range s.cluster.Len() {
		err := s.cluster.Host(i).Halt(s.signalType, signalID)
		if err == nil {
			halted = true
			continue
		}

		require.ErrorIs(t, err, actor.ErrActorNotHosted, "host %d", i)
	}

	require.True(t, halted, "no host reported owning the signal")
}

// assertClientRejected checks that the Service methods that target an actor by type reject the built-in signal type with ErrActorTypeReserved
func (s *builtinSignal) assertClientRejected(t *testing.T, svc *actor.Service, host int) {
	t.Helper()
	ctx := t.Context()

	const signalID = "some-signal"

	_, invErr := svc.Invoke(ctx, s.signalType, signalID, "wait", nil)
	require.ErrorIs(t, invErr, actor.ErrActorTypeReserved, "host %d Invoke", host)

	_, peekErr := svc.Peek(ctx, s.signalType, signalID, "check", nil)
	require.ErrorIs(t, peekErr, actor.ErrActorTypeReserved, "host %d Peek", host)

	_, _, streamErr := svc.InvokeStream(ctx, s.signalType, signalID, "wait", "", nil)
	require.ErrorIs(t, streamErr, actor.ErrActorTypeReserved, "host %d InvokeStream", host)

	setStateErr := svc.SetState(ctx, s.signalType, signalID, struct{}{}, nil)
	require.ErrorIs(t, setStateErr, actor.ErrActorTypeReserved, "host %d SetState", host)

	deleteStateErr := svc.DeleteState(ctx, s.signalType, signalID)
	require.ErrorIs(t, deleteStateErr, actor.ErrActorTypeReserved, "host %d DeleteState", host)

	haltErr := svc.Halt(s.signalType, signalID)
	require.ErrorIs(t, haltErr, actor.ErrActorTypeReserved, "host %d Halt", host)
}

// waiterGroup collects the outcome of a batch of parked waiters
type waiterGroup struct {
	wg      sync.WaitGroup
	results chan payload
	errs    chan error
	total   int
}

// assertAllReleased waits for every waiter to return and requires them all to have received the given version
func (g *waiterGroup) assertAllReleased(t *testing.T, version string) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("not every waiter returned")
	}

	select {
	case err := <-g.errs:
		t.Fatalf("a waiter failed: %v", err)
	default:
	}

	require.Len(t, g.results, g.total, "every waiter should have been released")
	for range g.total {
		got := <-g.results
		assert.Equal(t, version, got.Version)
	}
}

// signalFailover parks waiters on a surviving host, stops another host, and verifies the waiters are still released
// A wait that was parked on the stopped host loses its connection and has to re-resolve, which the caller must never see
type signalFailover struct {
	kind    cluster.Kind
	variant provider.Variant
	hosts   int

	cluster *cluster.Cluster
	sig     *signal.Signal
}

func (s *signalFailover) Name() string {
	return "builtinsignalfailover/" + string(s.kind) + "/" + string(s.variant)
}

func (s *signalFailover) Setup(t *testing.T) []framework.Option {
	sigActor, err := signal.New("e2efailover")
	require.NoError(t, err)

	s.sig = sigActor
	s.cluster = cluster.New(t, cluster.Options{
		Kind:          s.kind,
		Variant:       s.variant,
		Hosts:         s.hosts,
		BuiltInActors: []builtinactor.BuiltInActor{sigActor},
	})

	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *signalFailover) Run(t *testing.T) {
	ctx := t.Context()
	const (
		signalID = "failover-1"
		waiters  = 5
	)

	// Host 0 is the one that goes away, so the waiters and the completion are driven from the survivor
	const survivor = 1
	svc := s.sig.Service(s.cluster.Service(survivor))

	results := make(chan payload, waiters)
	errs := make(chan error, waiters)
	var wg sync.WaitGroup
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()

			waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
			defer cancel()

			env, err := svc.Wait(waitCtx, signalID)
			if err != nil {
				errs <- err
				return
			}

			var got payload
			err = env.Decode(&got)
			if err != nil {
				errs <- err
				return
			}
			results <- got
		}()
	}

	// Let the waiters settle wherever the signal was placed before the topology changes under them
	time.Sleep(time.Second)

	// Stop a host, which breaks the wait outright when it was the one hosting the signal
	s.cluster.Host(0).Stop(t)

	// Re-placement settles after the stopped host deregisters, so retry the completion until a surviving host accepts it
	require.Eventually(t, func() bool {
		completeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		err := svc.Complete(completeCtx, signalID, payload{Version: "after-failover"})
		return err == nil
	}, waitTimeout, 500*time.Millisecond, "the signal should be completable on a surviving host")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("the waiters were not released after the failover")
	}

	select {
	case err := <-errs:
		t.Fatalf("a waiter failed to survive the failover: %v", err)
	default:
	}

	require.Len(t, results, waiters)
	for range waiters {
		got := <-results
		assert.Equal(t, "after-failover", got.Version)
	}
}
