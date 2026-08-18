package local

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/host"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/testutil"
)

// Interface assertion
var _ host.Host = (*Host)(nil)

func TestRunHealthChecks(t *testing.T) {
	t.Run("successful health checks with regular interval", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Now())

		// Create logger
		logBuf := &testutil.ConcurrentBuffer{}
		log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)
		const healthCheckInterval = 30 * time.Second
		provider.
			On("HealthCheckPolicy").
			Return(components.NewHealthCheckPolicy(40 * time.Second)).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-123",
			providerRequestTimeout: 15 * time.Second,
		}

		// Set up expectations for successful health checks
		healthChecks := &atomic.Int32{}
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-123",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true},
			).
			Run(func(mock.Arguments) {
				healthChecks.Add(1)
			}).
			Return(nil).
			Times(3)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the ticker to exist so the first clock advance cannot race with startup
		require.Eventually(t, clock.HasWaiters, time.Second, 10*time.Millisecond, "health check ticker did not start")

		// Consume each tick before advancing again because the fake ticker only retains one pending event
		for want := int32(1); want <= 3; want++ {
			clock.Step(healthCheckInterval)
			require.Eventually(t, func() bool {
				return healthChecks.Load() == want
			}, time.Second, 10*time.Millisecond, "health check %d did not run", want)
		}

		// Cancel the context to stop the health checks
		cancel()

		// Wait for the function to return
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("runHealthChecks did not return within expected time")
		}

		// Verify the stop message was logged
		assert.Contains(t, logBuf.String(), "Stopped background health checks")

		// Assert expected method calls
		provider.AssertExpectations(t)
	})

	t.Run("health check with retries on temporary errors", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Now())

		// Create logger
		logBuf := &testutil.ConcurrentBuffer{}
		log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)
		const healthCheckInterval = 100 * time.Millisecond
		provider.
			On("HealthCheckPolicy").
			Return(components.NewHealthCheckPolicy(2 * healthCheckInterval)).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-456",
			providerRequestTimeout: 5 * time.Second,
		}

		// Make the first attempt and its retry fail before the final retry succeeds
		// A following sequence verifies that the policy resets the retry state
		tempError := errors.New("temporary network error")
		callCount := &atomic.Int32{}
		recordCall := func(mock.Arguments) {
			callCount.Add(1)
		}
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-456",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: false},
			).
			Run(recordCall).
			Return(tempError).
			Once()
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-456",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: true},
			).
			Run(recordCall).
			Return(tempError).
			Once()
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-456",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: true},
			).
			Run(recordCall).
			Return(nil).
			Once()
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-456",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: false},
			).
			Run(recordCall).
			Return(nil).
			Once()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the method to start
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Starting background health checks")
		}, time.Second, 10*time.Millisecond)

		// Wait for the ticker before advancing the fake clock so startup cannot lose the tick
		require.Eventually(t, clock.HasWaiters, time.Second, 10*time.Millisecond, "health check ticker did not start")

		// Trigger one sequence and wait for all three attempts to finish
		clock.Step(healthCheckInterval)
		require.Eventually(t, func() bool {
			return callCount.Load() == 3
		}, time.Second, 10*time.Millisecond, "health check retries did not finish")

		// Confirm temporary errors were reported before the successful retry
		require.Eventually(t, func() bool {
			logContent := logBuf.String()
			return strings.Contains(logContent, "Health check error, will retry") &&
				strings.Contains(logContent, "temporary network error")
		}, time.Second, 10*time.Millisecond)

		// Trigger the next sequence and verify its first attempt is not marked as a retry
		clock.Step(healthCheckInterval)
		require.Eventually(t, func() bool {
			return callCount.Load() == 4
		}, time.Second, 10*time.Millisecond, "next health check sequence did not run")

		// Cancel the context to stop the health checks
		cancel()

		// Wait for the function to return
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("runHealthChecks did not return within expected time")
		}

		// Assert expected method calls
		provider.AssertExpectations(t)
	})

	t.Run("health check fails permanently with ErrHostUnregistered", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Now())

		// Create logger
		logBuf := &testutil.ConcurrentBuffer{}
		log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)
		const healthCheckInterval = 5 * time.Second
		provider.
			On("HealthCheckPolicy").
			Return(components.NewHealthCheckPolicy(10 * time.Second)).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-789",
			providerRequestTimeout: 5 * time.Second,
		}

		// Set up expectations: call fails with ErrHostUnregistered (permanent error)
		// Should only be called once, no retries for permanent errors
		provider.
			On(
				"UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-789",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true},
			).
			Return(components.ErrHostUnregistered).
			Once()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the method to start
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Starting background health checks")
		}, time.Second, 10*time.Millisecond)

		// Advance time to trigger first health check (which will fail permanently)
		clock.Step(healthCheckInterval)

		// Wait for the function to return with the permanent error
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.ErrorContains(t, err, "failed to perform health check")
			require.ErrorIs(t, err, components.ErrHostUnregistered)
		case <-time.After(2 * time.Second):
			t.Fatal("runHealthChecks did not return within expected time")
		}

		// Verify the error was logged
		assert.Contains(t, logBuf.String(), "Health check failed")

		// Assert expected method calls
		provider.AssertExpectations(t)
	})

	t.Run("health check fails after max retries", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Now())

		// Create logger
		logBuf := &testutil.ConcurrentBuffer{}
		log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)
		healthCheckInterval := 10 * time.Second
		policy := components.NewHealthCheckPolicy(20 * time.Second)
		provider.
			On("HealthCheckPolicy").
			Return(policy).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-retry",
			providerRequestTimeout: 5 * time.Second,
		}

		// Set up expectations: all calls fail with temporary error (will exhaust retries)
		// The first attempt is not flagged as a retry, while every attempt after it is, so the provider can check whether an earlier attempt landed before writing again
		persistentError := errors.New("persistent error")
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-retry",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: false},
			).
			Return(persistentError).
			Once()
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-retry",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true, Retry: true},
			).
			Return(persistentError) // Return error always

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the method to start
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Starting background health checks")
		}, time.Second, 10*time.Millisecond)

		// Advance time to trigger first health check (which will exhaust retries and fail)
		clock.Step(healthCheckInterval)

		// Wait for the function to return with error after retries are exhausted
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.ErrorContains(t, err, "failed to perform health check")
			require.ErrorContains(t, err, "persistent error")
		case <-time.After(5 * time.Second):
			t.Fatal("runHealthChecks did not return within expected time")
		}

		// Verify retry warnings appeared in logs
		logContent := logBuf.String()
		assert.Contains(t, logContent, "Health check error, will retry")
		assert.Contains(t, logContent, "Health check failed")

		// Assert the expected first attempt and retry calls were made
		provider.AssertExpectations(t)

		// The policy allows one initial attempt followed by the remaining attempts marked as retries
		provider.AssertNumberOfCalls(t, "UpdateActorHost", policy.MaxAttempts())
	})

	t.Run("context cancellation during health check", func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Now())

		// Create logger
		logBuf := &testutil.ConcurrentBuffer{}
		log := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Create a mocked actor provider
		provider := components_mocks.NewMockActorProvider(t)
		healthCheckInterval := 1 * time.Hour // Long interval so we control when health checks happen
		provider.
			On("HealthCheckPolicy").
			Return(components.NewHealthCheckPolicy(healthCheckInterval + 10*time.Second)).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-cancel",
			providerRequestTimeout: 5 * time.Second,
		}

		ctx, cancel := context.WithCancel(t.Context())

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the method to start
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Starting background health checks")
		}, time.Second, 10*time.Millisecond)

		// Cancel the context before any health checks happen
		cancel()

		// Wait for the function to return with context cancellation error
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("runHealthChecks did not return within expected time")
		}

		// Verify the stop message was logged
		assert.Contains(t, logBuf.String(), "Stopped background health checks")

		// No UpdateActorHost calls should have been made
		provider.AssertExpectations(t)
	})
}
