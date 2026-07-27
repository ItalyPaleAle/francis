package signal

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/host/local"
	"github.com/italypaleale/francis/internal/testutil"
)

// testRuntimePSK is the shared runtime PSK the functional tests derive their cluster CA from
var testRuntimePSK = []byte("signal-test-runtime-psk-0123456789")

// startHost builds and runs a local host with the given signal set registered, waiting until it is ready and cleaning it up when the test ends
func startHost(t *testing.T, dbPath string, sig *Signal) *local.Host {
	t.Helper()

	host, err := local.NewHost(
		local.WithAddress(testutil.FreeUDPAddr(t)),
		local.WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: dbPath}),
		local.WithRuntimePSKs(testRuntimePSK),
		local.WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(t, err)

	err = host.RegisterBuiltInActor(sig)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- host.Run(t.Context())
	}()

	select {
	case <-host.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("host did not register")
	}

	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("host did not shut down")
		}
	})

	return host
}

// TestBroadcastToWaiters verifies one completion releases every caller waiting on the signal, each with the payload
func TestBroadcastToWaiters(t *testing.T) {
	const waiters = 100

	sig, err := New("broadcast")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "broadcast.db"), sig)
	svc := sig.Service(host.Service())

	type payload struct {
		Version string
	}

	// Park every waiter before the signal fires
	results := make(chan payload, waiters)
	errs := make(chan error, waiters)
	var started sync.WaitGroup
	started.Add(waiters)
	for range waiters {
		go func() {
			started.Done()

			env, waitErr := svc.Wait(t.Context(), "deploy-1")
			if waitErr != nil {
				errs <- waitErr
				return
			}

			var got payload
			decodeErr := env.Decode(&got)
			if decodeErr != nil {
				errs <- decodeErr
				return
			}
			results <- got
		}()
	}
	started.Wait()

	// Give the waiters a moment to reach the actor, so the completion really does have to release parked callers
	time.Sleep(500 * time.Millisecond)

	err = svc.Complete(t.Context(), "deploy-1", payload{Version: "v2"})
	require.NoError(t, err)

	for range waiters {
		select {
		case got := <-results:
			assert.Equal(t, "v2", got.Version)
		case waitErr := <-errs:
			t.Fatalf("a waiter failed: %v", waitErr)
		case <-time.After(30 * time.Second):
			t.Fatal("not every waiter was released")
		}
	}
}

// TestWaitAfterCompletion verifies a caller arriving after the signal fired is answered immediately, including after the actor has been deactivated
func TestWaitAfterCompletion(t *testing.T) {
	sig, err := New("late")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "late.db"), sig)
	svc := sig.Service(host.Service())

	err = svc.Complete(t.Context(), "deploy-1", "done")
	require.NoError(t, err)

	// The live activation answers from memory
	env, err := svc.Wait(t.Context(), "deploy-1")
	require.NoError(t, err)
	var got string
	require.NoError(t, env.Decode(&got))
	assert.Equal(t, "done", got)

	// Halting the actor drops that memory, so the next wait has to come from the durable record
	require.NoError(t, host.Halt("francis.builtin.signal.late", "deploy-1"))

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	env, err = svc.Wait(ctx, "deploy-1")
	require.NoError(t, err)
	got = ""
	require.NoError(t, env.Decode(&got))
	assert.Equal(t, "done", got, "a deactivated signal was not answered from its durable record")
}

// TestCompleteIsIdempotent verifies only the first completion takes effect and the later ones say so
func TestCompleteIsIdempotent(t *testing.T) {
	sig, err := New("idempotent")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "idempotent.db"), sig)
	svc := sig.Service(host.Service())

	err = svc.Complete(t.Context(), "deploy-1", "first")
	require.NoError(t, err)

	err = svc.Complete(t.Context(), "deploy-1", "second")
	require.ErrorIs(t, err, ErrAlreadyCompleted)

	// The first payload is the one callers receive
	env, err := svc.Wait(t.Context(), "deploy-1")
	require.NoError(t, err)
	var got string
	require.NoError(t, env.Decode(&got))
	assert.Equal(t, "first", got)
}

// TestCheck verifies the non-blocking snapshot of a signal, before and after it fires
func TestCheck(t *testing.T) {
	sig, err := New("check")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "check.db"), sig)
	svc := sig.Service(host.Service())

	_, completed, err := svc.Check(t.Context(), "deploy-1")
	require.NoError(t, err)
	assert.False(t, completed)

	require.NoError(t, svc.Complete(t.Context(), "deploy-1", "done"))

	env, completed, err := svc.Check(t.Context(), "deploy-1")
	require.NoError(t, err)
	require.True(t, completed)

	var got string
	require.NoError(t, env.Decode(&got))
	assert.Equal(t, "done", got)
}

// TestCompleteWithoutPayload verifies a signal that carries no data still releases its waiters, reporting a nil payload
func TestCompleteWithoutPayload(t *testing.T) {
	sig, err := New("nopayload")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "nopayload.db"), sig)
	svc := sig.Service(host.Service())

	type waitOutcome struct {
		env actor.Envelope
		err error
	}
	outcomes := make(chan waitOutcome, 1)
	go func() {
		env, waitErr := svc.Wait(t.Context(), "deploy-1")
		outcomes <- waitOutcome{env: env, err: waitErr}
	}()

	time.Sleep(500 * time.Millisecond)
	require.NoError(t, svc.Complete(t.Context(), "deploy-1", nil))

	select {
	case got := <-outcomes:
		require.NoError(t, got.err)
		assert.Nil(t, got.env, "a signal with no payload should report a nil envelope")
	case <-time.After(30 * time.Second):
		t.Fatal("the waiter was not released")
	}
}

// TestWaitRespectsCallerContext verifies a caller that gives up stops waiting, and that doing so does not disturb the callers still attached to the same signal
func TestWaitRespectsCallerContext(t *testing.T) {
	sig, err := New("giveup")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "giveup.db"), sig)
	svc := sig.Service(host.Service())

	// One caller stays for the whole test, the other gives up early
	stayed := make(chan error, 1)
	go func() {
		_, waitErr := svc.Wait(t.Context(), "deploy-1")
		stayed <- waitErr
	}()

	leavingCtx, cancelLeaving := context.WithCancel(t.Context())
	left := make(chan error, 1)
	go func() {
		_, waitErr := svc.Wait(leavingCtx, "deploy-1")
		left <- waitErr
	}()

	time.Sleep(500 * time.Millisecond)
	cancelLeaving()

	select {
	case waitErr := <-left:
		require.ErrorIs(t, waitErr, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("the caller that gave up did not return")
	}

	// The caller that stayed must still be waiting, and must still be released by the completion
	select {
	case waitErr := <-stayed:
		t.Fatalf("the caller that stayed returned early: %v", waitErr)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, svc.Complete(t.Context(), "deploy-1", "done"))

	select {
	case waitErr := <-stayed:
		require.NoError(t, waitErr)
	case <-time.After(30 * time.Second):
		t.Fatal("the caller that stayed was not released")
	}
}

// TestRetentionExpiry verifies a completion stops being observable once its retention window has passed, which is the documented edge of the primitive
func TestRetentionExpiry(t *testing.T) {
	// A one-second window keeps the test quick while still exercising the real provider TTL
	sig, err := New("shortlived", WithRetention(time.Second))
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "retention.db"), sig)
	svc := sig.Service(host.Service())

	require.NoError(t, svc.Complete(t.Context(), "deploy-1", "done"))

	// Drop the in-memory activation, so what a later caller sees comes only from the stored record
	require.NoError(t, host.Halt("francis.builtin.signal.shortlived", "deploy-1"))

	// Within the window the completion is still there
	_, completed, err := svc.Check(t.Context(), "deploy-1")
	require.NoError(t, err)
	assert.True(t, completed)

	require.Eventually(t, func() bool {
		require.NoError(t, host.Halt("francis.builtin.signal.shortlived", "deploy-1"))
		_, stillCompleted, checkErr := svc.Check(t.Context(), "deploy-1")
		require.NoError(t, checkErr)
		return !stillCompleted
	}, 30*time.Second, 500*time.Millisecond, "the completion outlived its retention window")

	// Past the window the signal is indistinguishable from one that never fired, so a wait parks rather than returning
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, err = svc.Wait(ctx, "deploy-1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestPayloadSizeLimit verifies an oversized payload is rejected before it is sent anywhere
func TestPayloadSizeLimit(t *testing.T) {
	sig, err := New("bigpayload", WithMaxPayloadSize(16))
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "bigpayload.db"), sig)
	svc := sig.Service(host.Service())

	err = svc.Complete(t.Context(), "deploy-1", "this payload is comfortably over the limit")
	require.ErrorIs(t, err, ErrPayloadTooLarge)

	// The rejected completion never reached the actor, so the signal has not fired
	_, completed, err := svc.Check(t.Context(), "deploy-1")
	require.NoError(t, err)
	assert.False(t, completed)
}

// TestInvalidSignalID verifies the service rejects an unusable signal ID up front
func TestInvalidSignalID(t *testing.T) {
	sig, err := New("ids")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "ids.db"), sig)
	svc := sig.Service(host.Service())

	_, err = svc.Wait(t.Context(), "")
	require.ErrorContains(t, err, "signal ID is required")

	err = svc.Complete(t.Context(), "bad/id", nil)
	require.ErrorContains(t, err, "invalid signal ID")
}

// TestWaitIsAggregatedPerProcess verifies every local caller waiting on the same signal shares a single invocation, which is what keeps a signal with many local waiters down to one stream and one in-flight slot on the owning host
func TestWaitIsAggregatedPerProcess(t *testing.T) {
	const waiters = 25

	sig, err := New("aggregate")
	require.NoError(t, err)

	host := startHost(t, filepath.Join(t.TempDir(), "aggregate.db"), sig)
	svc := sig.Service(host.Service())

	// The signal never fires during this phase, so every caller stays parked and attached
	waitCtx, cancelWaiters := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Add(waiters)
	for range waiters {
		go func() {
			defer wg.Done()
			_, waitErr := svc.Wait(waitCtx, "deploy-1")
			assert.ErrorIs(t, waitErr, context.Canceled)
		}()
	}

	// All of them must end up on the same entry, however many callers there are
	require.Eventually(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()

		w, ok := svc.waits["deploy-1"]
		return ok && w.refs == waiters
	}, 15*time.Second, 50*time.Millisecond, "the callers did not attach to a single shared wait")

	svc.mu.Lock()
	entries := len(svc.waits)
	svc.mu.Unlock()
	assert.Equal(t, 1, entries, "a signal should hold one shared wait per process, not one per caller")

	// Once the last caller leaves, the entry goes with it rather than leaking
	cancelWaiters()
	wg.Wait()

	require.Eventually(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return len(svc.waits) == 0
	}, 15*time.Second, 50*time.Millisecond, "the shared wait outlived its last caller")
}
