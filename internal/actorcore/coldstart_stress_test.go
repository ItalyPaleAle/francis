package actorcore

import (
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// TestGetOrCreateActorConcurrentColdStart guards the single-activation invariant: when many callers invoke the same cold actor at once, they must all receive the one instance that is stored in the map
// Handing different callers different instances would give each its own turn-based lock and state cache, allowing concurrent execution and lost updates
// The map's get-or-compute cannot provide this on its own, since it returns each racing caller its own computed value, so creation is serialized in getOrCreateActor
func TestGetOrCreateActorConcurrentColdStart(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	m := &Manager{
		Actors:              haxmap.New[string, *ActiveActor](8),
		log:                 slog.New(slog.DiscardHandler),
		clock:               clock,
		shutdownGracePeriod: 5 * time.Second,
		ActorsConfig: map[string]components.ActorHostType{
			"t": {IdleTimeout: 5 * time.Minute},
		},
		ActorFactories: map[string]actor.Factory{
			"t": func(actorID string, service *actor.Service) actor.Actor { return struct{}{} },
		},
	}
	m.IdleProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *ActiveActor]{
		ExecuteFn: m.HandleIdleActor,
		Clock:     clock,
	})
	defer m.IdleProcessor.Close()

	// Each round cold-starts a fresh actor from several goroutines at once, so the creation race is exercised repeatedly
	const rounds = 500
	const callers = 16
	for i := range rounds {
		r := ref.NewActorRef("t", strconv.Itoa(i))

		got := make([]*ActiveActor, callers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for c := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Release all callers together to maximize the overlap on the cold start
				<-start
				a, err := m.getOrCreateActor(r)
				require.NoError(t, err)
				got[c] = a
			}()
		}
		close(start)
		wg.Wait()

		// Every caller must have received the exact same instance, which must also be the one stored in the map
		stored, ok := m.Actors.Get(r.String())
		require.True(t, ok)
		for _, a := range got {
			require.Same(t, stored, a, "all concurrent cold-start callers must receive the single stored instance")
		}
	}
}
