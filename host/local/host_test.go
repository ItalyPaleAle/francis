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
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/testutil"
)

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
			On("HealthCheckInterval").
			Return(healthCheckInterval).
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
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-123",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true},
			).
			Return(nil).
			Times(3) // Expect 3 health checks

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Start runHealthChecks in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- host.runHealthChecks(ctx)
		}()

		// Wait for the method to start and log the initial message
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Starting background health checks")
		}, time.Second, 10*time.Millisecond)

		// Advance time to trigger first health check
		clock.Step(healthCheckInterval)

		// Wait for first health check to be logged
		assert.Eventually(t, func() bool {
			return strings.Contains(logBuf.String(), "Sending health check to the provider")
		}, time.Second, 10*time.Millisecond)

		// Advance time to trigger 2 more health checks
		for range 2 {
			clock.Step(healthCheckInterval)
		}

		// Wait a bit for all health checks to complete
		time.Sleep(100 * time.Millisecond)

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
		const healthCheckInterval = 10 * time.Second
		provider.
			On("HealthCheckInterval").
			Return(healthCheckInterval).
			Maybe()

		// Create a minimal host for testing
		host := &Host{
			actorProvider:          provider,
			log:                    log,
			clock:                  clock,
			hostID:                 "test-host-456",
			providerRequestTimeout: 5 * time.Second,
		}

		// Set up expectations: first two calls fail with temporary error, third succeeds
		tempError := errors.New("temporary network error")
		callCount := &atomic.Int32{}
		provider.
			On(
				"UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-456",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true},
			).
			Return(func(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
				c := callCount.Add(1)
				if c < 3 {
					return tempError
				}
				return nil
			})

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

		// Advance time to trigger first health check (which will retry and eventually succeed)
		clock.Step(healthCheckInterval)

		// Wait for retry warnings to appear in logs
		assert.Eventually(t, func() bool {
			logContent := logBuf.String()
			return strings.Contains(logContent, "Health check error; will retry") &&
				strings.Contains(logContent, "temporary network error")
		}, 2*time.Second, 10*time.Millisecond)

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
			On("HealthCheckInterval").
			Return(healthCheckInterval).
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
		provider.
			On("HealthCheckInterval").
			Return(healthCheckInterval).
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
		persistentError := errors.New("persistent error")
		provider.
			On("UpdateActorHost",
				mock.MatchedBy(testutil.MatchContextInterface),
				"test-host-retry",
				components.UpdateActorHostReq{UpdateLastHealthCheck: true},
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
		assert.Contains(t, logContent, "Health check error; will retry")
		assert.Contains(t, logContent, "Health check failed")

		// Assert that at least one call was made - exact count might vary due to timing
		// but we know it should try at least once
		provider.AssertExpectations(t)
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
			On("HealthCheckInterval").
			Return(healthCheckInterval).
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
