package host

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/eventqueue"
	actor_mocks "github.com/italypaleale/francis/internal/mocks/actor"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/testutil"
	"github.com/italypaleale/francis/internal/ttlcache"
)

func TestLookupActor(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	log := slog.New(slog.DiscardHandler)

	newHost := func() (*Host, *components_mocks.MockActorProvider) {
		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)

		// Create a minimal host for testing
		host := &Host{
			hostID:                 "test-host-123",
			address:                "localhost:8080",
			actorProvider:          provider,
			actors:                 haxmap.New[string, *activeActor](8),
			log:                    log,
			clock:                  clock,
			providerRequestTimeout: 30 * time.Second,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					IdleTimeout: 5 * time.Minute,
				},
			},
			actorFactories: map[string]actor.Factory{
				"testactor": func(actorID string, service *actor.Service) actor.Actor {
					return &actor_mocks.MockActorDeactivate{}
				},
			},
		}
		host.service = actor.NewService(host)
		host.idleActorProcessor = eventqueue.NewProcessor(eventqueue.Options[string, *activeActor]{
			ExecuteFn: host.handleIdleActor,
			Clock:     clock,
		})

		// Initialize placement cache
		host.placementCache = ttlcache.NewCache[*actorPlacement](&ttlcache.CacheOptions{
			MaxTTL:      placementCacheMaxTTL,
			InitialSize: 8,
		})

		return host, provider
	}

	t.Run("actor is active locally", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor1")

		// Create and register an active actor locally
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// No provider calls should be made when actor is local
		provider.AssertNotCalled(t, "LookupActor")

		// Test lookup with skipCache = false
		result, err := host.lookupActor(t.Context(), actorRef, false, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "test-host-123", result.HostID)
		assert.Equal(t, "localhost:8080", result.Address)

		// Test lookup with skipCache = true
		result2, err := host.lookupActor(t.Context(), actorRef, true, false)
		require.NoError(t, err)
		require.NotNil(t, result2)
		assert.Equal(t, "test-host-123", result2.HostID)
		assert.Equal(t, "localhost:8080", result2.Address)
	})

	t.Run("cache hit", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor2")

		// Add an entry to the cache
		cachedPlacement := &actorPlacement{
			HostID:  "cached-host-456",
			Address: "cached.example.com:8080",
		}
		host.placementCache.Set(actorRef.String(), cachedPlacement, 30*time.Second)

		// No provider calls should be made when using cache
		provider.AssertNotCalled(t, "LookupActor")

		// Test lookup with skipCache = false (should use cache)
		result, err := host.lookupActor(t.Context(), actorRef, false, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "cached-host-456", result.HostID)
		assert.Equal(t, "cached.example.com:8080", result.Address)
	})

	t.Run("cache hit with skipCache true calls provider", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor3")

		// Add an entry to the cache
		cachedPlacement := &actorPlacement{
			HostID:  "cached-host-456",
			Address: "cached.example.com:8080",
		}
		host.placementCache.Set(actorRef.String(), cachedPlacement, 30*time.Second)

		// Set up provider expectation
		providerResponse := components.LookupActorRes{
			HostID:      "provider-host-789",
			Address:     "provider.example.com:8080",
			IdleTimeout: 10 * time.Minute,
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(providerResponse, nil).
			Once()

		// Test lookup with skipCache = true (should call provider)
		result, err := host.lookupActor(t.Context(), actorRef, true, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "provider-host-789", result.HostID)
		assert.Equal(t, "provider.example.com:8080", result.Address)

		// Verify provider was called and result was cached
		provider.AssertExpectations(t)

		// Verify the cache was updated
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "provider-host-789", cached.HostID)
		assert.Equal(t, "provider.example.com:8080", cached.Address)
	})

	t.Run("cache miss calls provider successfully", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor4")

		// Set up provider expectation
		providerResponse := components.LookupActorRes{
			HostID:      "remote-host-999",
			Address:     "remote.example.com:8080",
			IdleTimeout: 15 * time.Minute,
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(providerResponse, nil).
			Once()

		// Test lookup
		result, err := host.lookupActor(t.Context(), actorRef, false, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "remote-host-999", result.HostID)
		assert.Equal(t, "remote.example.com:8080", result.Address)

		// Verify provider was called and result was cached
		provider.AssertExpectations(t)

		// Verify the result was cached with the provider's IdleTimeout
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "remote-host-999", cached.HostID)
		assert.Equal(t, "remote.example.com:8080", cached.Address)
	})

	t.Run("provider returns ErrNoHost", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "error-no-host-actor")

		// Add an entry to the cache first to verify it gets deleted
		cachedPlacement := &actorPlacement{
			HostID:  "cached-host-456",
			Address: "cached.example.com:8080",
		}
		host.placementCache.Set(actorRef.String(), cachedPlacement, 30*time.Second)

		// Set up provider to return ErrNoHost
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(components.LookupActorRes{}, components.ErrNoHost).
			Once()

		// Test lookup with skipCache=true to force provider call
		result, err := host.lookupActor(t.Context(), actorRef, true, false)
		require.Error(t, err)
		require.ErrorIs(t, err, actor.ErrActorTypeUnsupported)
		assert.Nil(t, result)

		// Verify provider was called
		provider.AssertExpectations(t)

		// Verify the cache entry was deleted
		_, ok := host.placementCache.Get(actorRef.String())
		assert.False(t, ok)
	})

	t.Run("provider returns other error", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "error-other-actor")

		// Add an entry to the cache first to verify it gets deleted
		cachedPlacement := &actorPlacement{
			HostID:  "cached-host-456",
			Address: "cached.example.com:8080",
		}
		host.placementCache.Set(actorRef.String(), cachedPlacement, 30*time.Second)

		// Set up provider to return a generic error
		providerError := errors.New("database connection failed")
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(components.LookupActorRes{}, providerError).
			Once()

		// Test lookup with skipCache=true to force provider call
		result, err := host.lookupActor(t.Context(), actorRef, true, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "provider returned an error")
		require.ErrorContains(t, err, "database connection failed")
		assert.Nil(t, result)

		// Verify provider was called
		provider.AssertExpectations(t)

		// Verify the cache entry was deleted
		_, ok := host.placementCache.Get(actorRef.String())
		assert.False(t, ok)
	})

	t.Run("provider timeout", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		// Set a very short timeout
		host.providerRequestTimeout = 50 * time.Millisecond

		actorRef := ref.NewActorRef("testactor", "actor7")

		// Set up provider to simulate a timeout by sleeping longer than the timeout
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Run(func(args mock.Arguments) {
				// Wait for context cancellation
				//nolint:forcetypeassert
				ctx := args.Get(0).(context.Context)
				<-ctx.Done()
			}).
			Return(components.LookupActorRes{}, context.DeadlineExceeded).
			Once()

		// Test lookup
		start := time.Now()
		result, err := host.lookupActor(t.Context(), actorRef, false, false)

		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, result)

		// Should have taken at least the timeout duration
		assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)

		// Verify provider was called
		provider.AssertExpectations(t)
	})

	t.Run("parent context cancellation", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor8")

		// Create a context that will be canceled
		ctx, cancel := context.WithCancel(t.Context())

		// Set up provider to wait for context cancellation
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Run(func(args mock.Arguments) {
				// Wait for context cancellation
				//nolint:forcetypeassert
				ctx := args.Get(0).(context.Context)
				<-ctx.Done()
			}).
			Return(components.LookupActorRes{}, context.Canceled).
			Once()

		// Start lookup in a goroutine
		errCh := make(chan error, 1)
		go func() {
			_, err := host.lookupActor(ctx, actorRef, false, false)
			errCh <- err
		}()

		// Cancel the context after a short delay
		time.Sleep(10 * time.Millisecond)
		cancel()

		// Wait for the result
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("lookupActor did not return within timeout")
		}

		// Verify provider was called
		provider.AssertExpectations(t)
	})

	t.Run("cache TTL behavior with zero idle timeout", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor9")

		// Set up provider with zero idle timeout
		providerResponse := components.LookupActorRes{
			HostID:      "zero-timeout-host",
			Address:     "zero.example.com:8080",
			IdleTimeout: 0, // Zero timeout
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(providerResponse, nil).
			Once()

		// Test lookup
		result, err := host.lookupActor(t.Context(), actorRef, false, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "zero-timeout-host", result.HostID)
		assert.Equal(t, "zero.example.com:8080", result.Address)

		// Verify provider was called
		provider.AssertExpectations(t)

		// Verify the result was cached (should use default timeout)
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "zero-timeout-host", cached.HostID)
	})

	t.Run("cache TTL behavior with non-zero idle timeout", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "actor10")

		// Set up provider with specific idle timeout
		providerResponse := components.LookupActorRes{
			HostID:      "timeout-host",
			Address:     "timeout.example.com:8080",
			IdleTimeout: 2 * time.Minute,
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(providerResponse, nil).
			Once()

		// Test lookup
		result, err := host.lookupActor(t.Context(), actorRef, false, false)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "timeout-host", result.HostID)
		assert.Equal(t, "timeout.example.com:8080", result.Address)

		// Verify provider was called
		provider.AssertExpectations(t)

		// Verify the result was cached
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "timeout-host", cached.HostID)
	})

	t.Run("multiple concurrent lookups same actor", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "concurrent-actor")

		// Set up provider with a delay to test concurrency
		providerResponse := components.LookupActorRes{
			HostID:      "concurrent-host",
			Address:     "concurrent.example.com:8080",
			IdleTimeout: 5 * time.Minute,
		}

		// Provider should be called multiple times since we don't have internal deduplication
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{}).
			Return(providerResponse, nil).
			Maybe()

		const numGoroutines = 5
		results := make([]*actorPlacement, numGoroutines)
		errors := make([]error, numGoroutines)

		// Launch multiple concurrent lookups
		done := make(chan struct{})
		for i := range numGoroutines {
			go func(i int) {
				results[i], errors[i] = host.lookupActor(t.Context(), actorRef, false, false)
				done <- struct{}{}
			}(i)
		}

		// Wait for all goroutines to complete
		for range numGoroutines {
			<-done
		}

		// Verify all lookups succeeded
		for i := range numGoroutines {
			require.NoError(t, errors[i], "Lookup %d failed", i)
			require.NotNil(t, results[i], "Result %d is nil", i)
			assert.Equal(t, "concurrent-host", results[i].HostID, "HostID mismatch for result %d", i)
			assert.Equal(t, "concurrent.example.com:8080", results[i].Address, "Address mismatch for result %d", i)
		}

		// Verify the result is cached
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "concurrent-host", cached.HostID)
	})

	t.Run("multiple concurrent lookups different actors", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		const numActors = 5
		actorRefs := make([]ref.ActorRef, numActors)
		expectedHosts := make([]string, numActors)

		// Set up different actors and expectations
		for i := range numActors {
			actorRefs[i] = ref.NewActorRef("testactor", "actor-"+string(rune('A'+i)))
			expectedHosts[i] = "host-" + string(rune('A'+i))

			providerResponse := components.LookupActorRes{
				HostID:      expectedHosts[i],
				Address:     expectedHosts[i] + ".example.com:8080",
				IdleTimeout: 5 * time.Minute,
			}
			provider.
				On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRefs[i], components.LookupActorOpts{}).
				Return(providerResponse, nil).
				Once()
		}

		results := make([]*actorPlacement, numActors)
		errors := make([]error, numActors)

		// Launch concurrent lookups for different actors
		done := make(chan struct{})
		for i := range numActors {
			go func(index int) {
				results[index], errors[index] = host.lookupActor(t.Context(), actorRefs[index], false, false)
				done <- struct{}{}
			}(i)
		}

		// Wait for all goroutines to complete
		for range numActors {
			<-done
		}

		// Verify all lookups succeeded with correct results
		for i := range numActors {
			require.NoError(t, errors[i], "Lookup %d failed", i)
			require.NotNil(t, results[i], "Result %d is nil", i)
			assert.Equal(t, expectedHosts[i], results[i].HostID, "HostID mismatch for result %d", i)
			assert.Equal(t, expectedHosts[i]+".example.com:8080", results[i].Address, "Address mismatch for result %d", i)
		}

		// Verify all results are cached
		for i := range numActors {
			cached, ok := host.placementCache.Get(actorRefs[i].String())
			assert.True(t, ok, "Actor %d not cached", i)
			assert.Equal(t, expectedHosts[i], cached.HostID, "Cached HostID mismatch for actor %d", i)
		}

		// Verify provider expectations
		provider.AssertExpectations(t)
	})

	t.Run("activeOnly true - returns existing active actor", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "active-local-actor")

		// Create and register an active actor locally
		instance := &actor_mocks.MockActorDeactivate{}
		activeAct := newActiveActor(actorRef, instance, 5*time.Minute, host.idleActorProcessor, clock)
		host.actors.Set(actorRef.String(), activeAct)

		// No provider calls should be made when actor is local
		provider.AssertNotCalled(t, "LookupActor")

		// Test lookup with activeOnly = true
		result, err := host.lookupActor(t.Context(), actorRef, false, true)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "test-host-123", result.HostID)
		assert.Equal(t, "localhost:8080", result.Address)
	})

	t.Run("activeOnly true - returns active actor from provider", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "active-remote-actor")

		// Set up provider expectation with ActiveOnly: true
		providerResponse := components.LookupActorRes{
			HostID:      "remote-active-host",
			Address:     "remote-active.example.com:8080",
			IdleTimeout: 10 * time.Minute,
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{ActiveOnly: true}).
			Return(providerResponse, nil).
			Once()

		// Test lookup with activeOnly = true
		result, err := host.lookupActor(t.Context(), actorRef, false, true)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "remote-active-host", result.HostID)
		assert.Equal(t, "remote-active.example.com:8080", result.Address)

		// Verify provider was called with ActiveOnly: true
		provider.AssertExpectations(t)

		// Verify the result was cached
		cached, ok := host.placementCache.Get(actorRef.String())
		assert.True(t, ok)
		assert.Equal(t, "remote-active-host", cached.HostID)
	})

	t.Run("activeOnly true - returns ErrActorNotActive for inactive actor", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "inactive-actor")

		// Set up provider to return ErrNoActor for activeOnly lookup
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{ActiveOnly: true}).
			Return(components.LookupActorRes{}, components.ErrNoActor).
			Once()

		// Test lookup with activeOnly = true
		result, err := host.lookupActor(t.Context(), actorRef, false, true)
		require.Error(t, err)
		require.ErrorIs(t, err, actor.ErrActorNotActive)
		assert.Nil(t, result)

		// Verify provider was called
		provider.AssertExpectations(t)
	})

	t.Run("activeOnly false vs true - different provider calls", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		host, provider := newHost()
		defer host.idleActorProcessor.Close()
		defer host.placementCache.Stop()

		actorRef := ref.NewActorRef("testactor", "placement-actor")

		// Set up provider expectation for activeOnly = false
		placementResponse := components.LookupActorRes{
			HostID:      "placement-host",
			Address:     "placement.example.com:8080",
			IdleTimeout: 5 * time.Minute,
		}
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{ActiveOnly: false}).
			Return(placementResponse, nil).
			Once()

		// Set up provider expectation for activeOnly = true
		provider.
			On("LookupActor", mock.MatchedBy(testutil.MatchContextInterface), actorRef, components.LookupActorOpts{ActiveOnly: true}).
			Return(components.LookupActorRes{}, components.ErrNoActor).
			Once()

		// Test lookup with activeOnly = false (should succeed and create actor)
		result1, err1 := host.lookupActor(t.Context(), actorRef, true, false)
		require.NoError(t, err1)
		require.NotNil(t, result1)
		assert.Equal(t, "placement-host", result1.HostID)

		// Test lookup with activeOnly = true (should fail because actor is not active)
		result2, err2 := host.lookupActor(t.Context(), actorRef, true, true)
		require.Error(t, err2)
		require.ErrorIs(t, err2, actor.ErrActorNotActive)
		assert.Nil(t, result2)

		// Verify both provider calls were made with different options
		provider.AssertExpectations(t)
	})
}

func TestIsLocal(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	t.Run("local placement", func(t *testing.T) {
		host := &Host{
			hostID: "test-host-123",
			log:    log,
		}

		placement := &actorPlacement{
			HostID:  "test-host-123",
			Address: "localhost:8080",
		}

		assert.True(t, host.isLocal(placement))
	})

	t.Run("remote placement", func(t *testing.T) {
		host := &Host{
			hostID: "test-host-123",
			log:    log,
		}

		placement := &actorPlacement{
			HostID:  "remote-host-456",
			Address: "remote.example.com:8080",
		}

		assert.False(t, host.isLocal(placement))
	})
}

func TestDeactivationTimeoutForActorType(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	t.Run("actor type with deactivation timeout", func(t *testing.T) {
		host := &Host{
			log: log,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 30 * time.Second,
				},
			},
		}

		timeout := host.deactivationTimeoutForActorType("testactor")
		assert.Equal(t, 30*time.Second, timeout)
	})

	t.Run("actor type with zero deactivation timeout", func(t *testing.T) {
		host := &Host{
			log: log,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 0,
				},
			},
		}

		timeout := host.deactivationTimeoutForActorType("testactor")
		assert.Equal(t, time.Duration(0), timeout)
	})

	t.Run("unknown actor type", func(t *testing.T) {
		host := &Host{
			log: log,
			actorsConfig: map[string]components.ActorHostType{
				"testactor": {
					DeactivationTimeout: 30 * time.Second,
				},
			},
		}

		timeout := host.deactivationTimeoutForActorType("unknown")
		assert.Equal(t, time.Duration(0), timeout)
	})
}
