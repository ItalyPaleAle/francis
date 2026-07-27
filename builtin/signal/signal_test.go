package signal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	actor_mocks "github.com/italypaleale/francis/internal/mocks/actor"
)

func TestNew(t *testing.T) {
	t.Run("requires a name", func(t *testing.T) {
		_, err := New("")
		require.ErrorContains(t, err, "name is required")
	})

	t.Run("rejects a name with a separator", func(t *testing.T) {
		_, err := New("bad/name")
		require.ErrorContains(t, err, "invalid signal name")
	})

	t.Run("registers under the prefixed type as a shared-lock, non-singleton actor", func(t *testing.T) {
		s, err := New("deploys")
		require.NoError(t, err)

		assert.Equal(t, "signal.deploys", s.ActorType())
		assert.False(t, s.Singleton())
		assert.NotNil(t, s.Factory())

		// The shared lock is what lets a parked Wait coexist with the Complete that releases it
		assert.Equal(t, actorcore.LockModeShared, s.RegisterOptions().LockMode)
		assert.Equal(t, defaultIdleTimeout, s.RegisterOptions().IdleTimeout)
	})

	t.Run("applies the retention conventions", func(t *testing.T) {
		tests := []struct {
			name      string
			retention time.Duration
			expect    time.Duration
		}{
			{name: "unset takes the default", retention: 0, expect: defaultRetention},
			{name: "positive is used as given", retention: time.Hour, expect: time.Hour},
			{name: "negative means no expiration", retention: -1, expect: 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s, err := New("deploys", WithRetention(tt.retention))
				require.NoError(t, err)

				// The retention reaches the actor as the TTL of its completion record, so build one and read it back off the write
				a, cl := newTestActor(t, s)
				cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

				var gotTTL time.Duration
				cl.EXPECT().
					SetState(mock.Anything, mock.Anything, mock.Anything).
					Run(func(_ context.Context, _ signalState, opts *actor.SetStateOpts) {
						gotTTL = opts.TTL
					}).
					Return(nil).
					Once()

				_, err = a.complete(t.Context(), nil)
				require.NoError(t, err)
				assert.Equal(t, tt.expect, gotTTL)
			})
		}
	})

	t.Run("falls back to the defaults for non-positive sizes and timeouts", func(t *testing.T) {
		s, err := New("deploys", WithIdleTimeout(-1), WithMaxPayloadSize(-1))
		require.NoError(t, err)

		assert.Equal(t, defaultIdleTimeout, s.RegisterOptions().IdleTimeout)
		assert.Equal(t, defaultMaxPayloadSize, s.maxPayloadSize)
	})
}

func TestSignalActorWait(t *testing.T) {
	t.Run("returns immediately when the signal already completed", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))

		// A fresh activation of an already-completed signal is answered from its durable record
		cl.EXPECT().
			GetState(mock.Anything).
			Return(signalState{Completed: true, Data: []byte("payload")}, nil).
			Once()

		res, err := a.wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, signalResult{Completed: true, Data: []byte("payload")}, res)
	})

	t.Run("parks until the signal completes", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()
		cl.EXPECT().SetState(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		type waitResult struct {
			res any
			err error
		}
		results := make(chan waitResult, 1)
		go func() {
			res, err := a.wait(t.Context())
			results <- waitResult{res: res, err: err}
		}()

		// The waiter must still be parked while nothing has completed the signal
		select {
		case got := <-results:
			t.Fatalf("wait returned before the signal completed: %+v", got)
		case <-time.After(100 * time.Millisecond):
		}

		_, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("payload")}))
		require.NoError(t, err)

		select {
		case got := <-results:
			require.NoError(t, got.err)
			assert.Equal(t, signalResult{Completed: true, Data: []byte("payload")}, got.res)
		case <-time.After(5 * time.Second):
			t.Fatal("wait was not released by the completion")
		}
	})

	t.Run("releases every waiter at once", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()
		cl.EXPECT().SetState(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		const waiters = 50
		results := make(chan any, waiters)
		for range waiters {
			go func() {
				res, err := a.wait(t.Context())
				assert.NoError(t, err)
				results <- res
			}()
		}

		_, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("payload")}))
		require.NoError(t, err)

		for range waiters {
			select {
			case res := <-results:
				assert.Equal(t, signalResult{Completed: true, Data: []byte("payload")}, res)
			case <-time.After(5 * time.Second):
				t.Fatal("not every waiter was released")
			}
		}
	})

	t.Run("returns when the caller gives up", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := a.wait(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("returns when the actor starts halting", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		// The framework stamps the halt channel into the invocation context, so a parked waiter can leave without waiting out the shutdown grace period
		haltCh := make(chan struct{})
		ctx := actor.WithHalting(t.Context(), haltCh)

		errs := make(chan error, 1)
		go func() {
			_, err := a.wait(ctx)
			errs <- err
		}()

		close(haltCh)

		select {
		case err := <-errs:
			require.ErrorIs(t, err, actor.ErrActorHalted)
		case <-time.After(5 * time.Second):
			t.Fatal("wait did not observe the halt signal")
		}
	})

	t.Run("surfaces a failure to read the durable record", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))

		readErr := errors.New("store is down")
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, readErr).Once()

		_, err := a.wait(t.Context())
		require.ErrorIs(t, err, readErr)
	})
}

func TestSignalActorComplete(t *testing.T) {
	t.Run("persists the completion before making it observable", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		var stored signalState
		cl.EXPECT().
			SetState(mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, state signalState, _ *actor.SetStateOpts) {
				stored = state

				// The broadcast must not have happened yet: a crash here has to leave the signal genuinely unfired
				select {
				case <-a.done:
					t.Error("the completion was broadcast before it was persisted")
				default:
				}
			}).
			Return(nil).
			Once()

		res, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("payload")}))
		require.NoError(t, err)
		assert.Nil(t, res)

		assert.True(t, stored.Completed)
		assert.Equal(t, []byte("payload"), stored.Data)
		assert.False(t, stored.CompletedAt.IsZero())
	})

	t.Run("reports a second completion without changing anything", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		// Exactly one write, however many completions arrive
		cl.EXPECT().SetState(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		_, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("first")}))
		require.NoError(t, err)

		res, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("second")}))
		require.NoError(t, err)
		assert.Equal(t, completeResult{AlreadyCompleted: true}, res)

		// The first payload is the one every waiter receives
		got, err := a.wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, signalResult{Completed: true, Data: []byte("first")}, got)
	})

	t.Run("leaves the signal unfired when the write fails", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		writeErr := errors.New("store is down")
		cl.EXPECT().SetState(mock.Anything, mock.Anything, mock.Anything).Return(writeErr).Once()

		_, err := a.complete(t.Context(), nil)
		require.ErrorIs(t, err, writeErr)

		// Nothing was broadcast, so a check still reports the signal as pending
		res, err := a.check(t.Context())
		require.NoError(t, err)
		assert.Equal(t, signalResult{Completed: false}, res)
	})

	t.Run("reports a completion already in the store", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))

		// This activation is cold, and the signal fired before it existed
		cl.EXPECT().
			GetState(mock.Anything).
			Return(signalState{Completed: true, Data: []byte("earlier")}, nil).
			Once()

		res, err := a.complete(t.Context(), envelopeOf(t, completeRequest{Data: []byte("later")}))
		require.NoError(t, err)
		assert.Equal(t, completeResult{AlreadyCompleted: true}, res)
	})
}

func TestSignalActorCheck(t *testing.T) {
	t.Run("reports a pending signal", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().GetState(mock.Anything).Return(signalState{}, nil).Once()

		res, err := a.check(t.Context())
		require.NoError(t, err)
		assert.Equal(t, signalResult{Completed: false}, res)
	})

	t.Run("reports a completed signal and its payload", func(t *testing.T) {
		a, cl := newTestActor(t, mustSignal(t))
		cl.EXPECT().
			GetState(mock.Anything).
			Return(signalState{Completed: true, Data: []byte("payload")}, nil).
			Once()

		res, err := a.check(t.Context())
		require.NoError(t, err)
		assert.Equal(t, signalResult{Completed: true, Data: []byte("payload")}, res)
	})
}

func TestSignalActorInvoke(t *testing.T) {
	a, _ := newTestActor(t, mustSignal(t))

	_, err := a.Invoke(t.Context(), "nonsense", nil)
	require.ErrorContains(t, err, "unknown signal method")
}

// mustSignal returns a signal set with the default options
func mustSignal(t *testing.T) *Signal {
	t.Helper()

	s, err := New("test")
	require.NoError(t, err)
	return s
}

// newTestActor returns a signal actor wired to a mock client, so tests can drive its durable state directly
// The actor is built by hand rather than through the factory, which would bind a real client to a host
func newTestActor(t *testing.T, s *Signal) (*signalActor, *actor_mocks.MockClient[signalState]) {
	t.Helper()

	cl := actor_mocks.NewMockClient[signalState](t)
	return &signalActor{
		client:    cl,
		retention: s.retention,
		done:      make(chan struct{}),
	}, cl
}

// envelopeOf wraps a value the way the framework wraps an invocation's argument for a local call
func envelopeOf(t *testing.T, v any) actor.Envelope {
	t.Helper()

	return actorcore.NewObjectEnvelope(v)
}
