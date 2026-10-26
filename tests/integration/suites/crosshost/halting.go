//go:build integration

package crosshost

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/tests/integration/framework"
	"github.com/italypaleale/francis/tests/integration/framework/cluster"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
	"github.com/italypaleale/francis/tests/integration/framework/process/provider"
	"github.com/italypaleale/francis/tests/integration/suite"
	"github.com/italypaleale/francis/tests/integration/suites/shared"
)

const (
	// haltingIdleTimeout is short so the actor starts deactivating soon after it is placed
	haltingIdleTimeout = time.Second
	// haltingDeactivationTimeout leaves the held Deactivate hook plenty of room, so the scenario releases it rather than the timeout
	haltingDeactivationTimeout = 30 * time.Second
	// haltingHoldCheck is how long the racing call must stay parked to show it is waiting for the teardown rather than failing on the halted activation
	haltingHoldCheck = time.Second
	// haltingTimeout bounds every other wait in the scenario
	haltingTimeout = 30 * time.Second
	// haltingPollInterval is how often the scenario checks the probe's observations while waiting
	haltingPollInterval = 50 * time.Millisecond
)

// Register the halting race on both runtimes for each distinct provider implementation
// On the local runtime only providers that coordinate across processes can back multiple hosts
func init() {
	for _, v := range []provider.Variant{provider.SQLite, provider.Postgres, provider.StandaloneMemory} {
		if v.LocalMultiHost() {
			suite.Register(&haltingRace{kind: cluster.Local, variant: v})
		}
		suite.Register(&haltingRace{kind: cluster.Remote, variant: v})
	}
}

// haltingRace invokes an actor while its owning host is tearing it down, with the call coming either from the owner itself or from the other host
// The owner keeps the halted activation registered until its placement is cleared, so a call that retried straight away would resolve back to it and fail with "actor is halted"
// The probe's Deactivate hook holds that window open, which makes the race deterministic
type haltingRace struct {
	kind    cluster.Kind
	variant provider.Variant

	cluster *cluster.Cluster
	labels  []string
}

// invokeResult is the outcome of an invocation started in the background
type invokeResult struct {
	env actor.Envelope
	err error
}

func (s *haltingRace) Name() string {
	return "crosshost-halting/" + string(s.kind) + "/" + string(s.variant)
}

func (s *haltingRace) Setup(t *testing.T) []framework.Option {
	s.cluster = cluster.New(t, cluster.Options{
		Kind:    s.kind,
		Variant: s.variant,
		Hosts:   2,
		Actors: []frameworkhost.ActorReg{
			shared.ProbeReg(
				actorcore.WithIdleTimeout(haltingIdleTimeout),
				actorcore.WithDeactivationTimeout(haltingDeactivationTimeout),
			),
		},
	})
	return []framework.Option{
		framework.WithProcesses(s.cluster.Processes()...),
	}
}

func (s *haltingRace) Run(t *testing.T) {
	// Label hosts so the probe can report which one owns the actor
	s.labels = make([]string, s.cluster.Len())
	for i := range s.labels {
		s.labels[i] = "h" + strconv.Itoa(i)
		shared.SetHostLabel(s.cluster.Service(i), s.labels[i])
	}

	fromOwner := func(owner int) int {
		return owner
	}
	fromPeer := func(owner int) int {
		return (owner + 1) % s.cluster.Len()
	}

	t.Run("a call from the owning host while the actor deactivates", func(t *testing.T) {
		s.raceDeactivation(t, "owner", fromOwner)
	})
	t.Run("a call from another host while the actor deactivates", func(t *testing.T) {
		s.raceDeactivation(t, "peer", fromPeer)
	})
	t.Run("a call from the owning host halted mid-flight", func(t *testing.T) {
		s.raceMidFlight(t, "owner", fromOwner)
	})
	t.Run("a call from another host halted mid-flight", func(t *testing.T) {
		s.raceMidFlight(t, "peer", fromPeer)
	})
}

// raceDeactivation holds an idle actor's deactivation open on its owner, then invokes it from the host that pickCaller chooses
// The call finds the actor already halted, which is the race behind an idle timeout that matches the interval of a periodic caller
func (s *haltingRace) raceDeactivation(t *testing.T, name string, pickCaller func(owner int) int) {
	actorID := s.actorID("deactivation-" + name)

	// Arm the Deactivate hook before placing the actor, so the idle deactivation that follows is sure to hold open
	shared.ProbeObserver.ArmDeactivateBlock(actorID)
	t.Cleanup(func() {
		shared.ProbeObserver.ReleaseDeactivateBlock(actorID)
	})

	caller := pickCaller(s.place(t, actorID))

	// Wait for the idle deactivation to reach the held hook, at which point the actor is halted but still registered on its owner
	s.awaitDeactivation(t, actorID)

	// Invoke the actor inside that window, and require it to wait for the teardown rather than fail on the halted activation
	resultCh := s.invokeAsync(t, caller, actorID, shared.ProbeMethodIncrement)
	requireParked(t, resultCh)

	// Let the deactivation finish, which clears the placement and releases the call
	shared.ProbeObserver.ReleaseDeactivateBlock(actorID)

	// The call succeeds on a fresh activation, which resumed from the persisted state
	res := awaitResult(t, resultCh)
	var out shared.ProbeState
	require.NoError(t, res.env.Decode(&out))
	assert.Equal(t, int64(2), out.N, "the fresh activation should resume from the persisted state")
}

// raceMidFlight halts an actor on its owner while a call from the host that pickCaller chooses is running inside it, and holds the teardown open
// The actor fails the call with ErrActorHalted, which the framework must hold until the teardown finishes before retrying against a fresh activation
func (s *haltingRace) raceMidFlight(t *testing.T, name string, pickCaller func(owner int) int) {
	actorID := s.actorID("mid-flight-" + name)
	owner := s.place(t, actorID)
	caller := pickCaller(owner)

	// Start a call that parks inside the actor until it starts halting
	shared.ProbeObserver.ArmBlock(actorID)
	t.Cleanup(func() {
		shared.ProbeObserver.ReleaseBlock(actorID)
	})
	resultCh := s.invokeAsync(t, caller, actorID, shared.ProbeMethodBlockUntilHalt)
	require.Eventually(t, func() bool {
		return shared.ProbeObserver.BlockEnteredCount(actorID) >= 1
	}, haltingTimeout, haltingPollInterval, "the call should reach the actor before it is halted")

	// Arm the Deactivate hook only now, since the parked call keeps the actor busy and safe from idle deactivation
	shared.ProbeObserver.ArmDeactivateBlock(actorID)
	t.Cleanup(func() {
		shared.ProbeObserver.ReleaseDeactivateBlock(actorID)
	})

	// Halt the actor on its owner, which makes the parked call report the halt while the armed Deactivate hook holds the teardown open
	// Halt returns only once the teardown has finished, so it runs in the background
	haltErrCh := make(chan error, 1)
	go func() {
		haltErrCh <- s.cluster.Service(owner).Halt(shared.ProbeActorType, actorID)
	}()
	s.awaitDeactivation(t, actorID)

	// The call has already left the actor with ErrActorHalted, and must not retry while the halted activation is still registered on its owner
	requireParked(t, resultCh)

	// Disarm the blocking method so the retry answers at once, then let the teardown finish
	shared.ProbeObserver.ReleaseBlock(actorID)
	shared.ProbeObserver.ReleaseDeactivateBlock(actorID)
	select {
	case err := <-haltErrCh:
		require.NoError(t, err)
	case <-time.After(haltingTimeout):
		t.Fatal("halting the actor never finished")
	}

	// The retry reaches a fresh activation, which answers rather than parking
	res := awaitResult(t, resultCh)
	var reply string
	require.NoError(t, res.env.Decode(&reply))
	assert.Equal(t, shared.ProbeBlockUntilHaltReply, reply)
}

// actorID returns an actor ID that is unique to the scenario, which keeps the process-global observations clean across variants and runtimes
func (s *haltingRace) actorID(name string) string {
	return "xh-halting-" + name + "-" + string(s.kind) + "-" + string(s.variant)
}

// place activates the actor with some persisted state and returns the index of the host that owns it
func (s *haltingRace) place(t *testing.T, actorID string) int {
	t.Helper()

	env, err := s.cluster.Service(0).Invoke(t.Context(), shared.ProbeActorType, actorID, shared.ProbeMethodIncrement, nil)
	require.NoError(t, err)
	var out shared.ProbeState
	require.NoError(t, env.Decode(&out))
	require.Equal(t, int64(1), out.N)

	owner := slices.Index(s.labels, shared.ProbeObserver.LastInvokeHost(actorID))
	require.GreaterOrEqual(t, owner, 0, "the probe should have recorded the host that owns the actor")
	return owner
}

// awaitDeactivation waits until the actor's armed Deactivate hook is holding, at which point the actor is halted but still registered on its owner
func (s *haltingRace) awaitDeactivation(t *testing.T, actorID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return shared.ProbeObserver.DeactivateBlockEnteredCount(actorID) >= 1
	}, haltingTimeout, haltingPollInterval, "the actor should start deactivating")
}

// invokeAsync invokes the actor from the given host in the background and returns a channel that receives the outcome
func (s *haltingRace) invokeAsync(t *testing.T, caller int, actorID string, method string) chan invokeResult {
	t.Helper()

	resultCh := make(chan invokeResult, 1)
	go func() {
		env, err := s.cluster.Service(caller).Invoke(t.Context(), shared.ProbeActorType, actorID, method, nil)
		resultCh <- invokeResult{env: env, err: err}
	}()
	return resultCh
}

// requireParked fails the test if the call returns before the halted activation has been torn down
func requireParked(t *testing.T, resultCh chan invokeResult) {
	t.Helper()

	select {
	case res := <-resultCh:
		t.Fatalf("the call returned while the actor was still being torn down: %v", res.err)
	case <-time.After(haltingHoldCheck):
	}
}

// awaitResult waits for the call to return once the teardown has finished, and requires it to have succeeded with a response
func awaitResult(t *testing.T, resultCh chan invokeResult) invokeResult {
	t.Helper()

	select {
	case res := <-resultCh:
		require.NoError(t, res.err, "the call should succeed once the teardown finished")
		require.NotNil(t, res.env, "the call should return the actor's response")
		return res
	case <-time.After(haltingTimeout):
		t.Fatal("the call never returned after the teardown finished")
		return invokeResult{}
	}
}
