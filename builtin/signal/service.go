package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/types"
)

const (
	// initialRetryDelay is how long a failed wait backs off before re-resolving the signal's placement and trying again
	initialRetryDelay = 100 * time.Millisecond
	// maxRetryDelay caps the backoff, so a signal whose host is down is still picked up promptly once a host is available again
	maxRetryDelay = 5 * time.Second
)

// Service binds the signal set to an actor.Service, returning a SignalService that exposes Wait, Complete, and Check pre-configured for that service
// Obtain the service from a host with host.Service()
//
// Calling it more than once with the same actor.Service returns the same SignalService
// The waiters of one signal are aggregated onto a single invocation by the service they were started from, so handing every caller the same instance is what makes that aggregation hold no matter where in an application Service is called
func (s *Signal) Service(svc *actor.Service) *SignalService {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	existing, ok := s.services[svc]
	if ok {
		return existing
	}

	sigSvc := &SignalService{
		actorType:      s.actorType,
		maxPayloadSize: s.maxPayloadSize,
		svc:            svc,
		waits:          map[string]*sharedWait{},
	}
	if s.services == nil {
		s.services = map[*actor.Service]*SignalService{}
	}
	s.services[svc] = sigSvc

	return sigSvc
}

// SignalService exposes the operations of a signal set (Wait, Complete, and Check), bound to a specific actor.Service
// Obtain one from Signal.Service
type SignalService struct {
	actorType      string
	maxPayloadSize int
	svc            *actor.Service

	// mu guards waits
	mu sync.Mutex
	// waits holds the in-flight wait of each signal this process is currently waiting on
	// Every local caller waiting on the same signal attaches to the same entry, so this process holds one invocation per signal rather than one per caller, which is what keeps a signal with thousands of local waiters down to a single stream and a single in-flight slot on the owning host
	waits map[string]*sharedWait
}

// sharedWait is one signal's in-flight wait, shared by every local caller waiting on that signal
type sharedWait struct {
	// done is closed once the wait resolves, releasing every attached caller
	done chan struct{}
	// cancel stops the invocation, and runs once the last attached caller has left
	cancel context.CancelFunc
	// refs counts the callers currently attached, guarded by the service's mutex
	refs int

	// data and err carry the outcome, and are written before done is closed so an attached caller reading them after done is closed sees them fully
	data []byte
	err  error
}

// Wait blocks until the signal completes and returns its payload, or nil when the signal carried none
//
// It returns immediately when the signal has already completed and is still within its retention window, so a caller that disconnects and calls Wait again is always answered correctly
// Placement changes and host failures along the way are transparent: the wait re-resolves and resumes on its own, and returns only once the signal completes or ctx is done
//
// The signal ID is free-form (e.g. a deployment ID, a job ID, etc) and must not contain '/'
func (s *SignalService) Wait(ctx context.Context, signalID string) (actor.Envelope, error) {
	err := validateSignalID(signalID)
	if err != nil {
		return nil, err
	}

	// Attach to this process's wait for the signal, starting one when we are the first caller
	w := s.attach(signalID)
	defer s.detach(signalID, w)

	select {
	case <-w.done:
		if w.err != nil {
			return nil, w.err
		}
		return payloadEnvelope(w.data), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Complete fires the signal, releasing every caller waiting on it with the given payload, and returns as soon as the completion is durable
// Pass nil when the signal carries no payload
//
// It returns ErrAlreadyCompleted when the signal had already fired, which is safe to ignore: the first completion stands and its payload is the one every caller receives
// The signal ID is free-form and must not contain '/'
func (s *SignalService) Complete(ctx context.Context, signalID string, data any) error {
	err := validateSignalID(signalID)
	if err != nil {
		return err
	}

	// Encode the payload here so it stays opaque all the way to the callers that receive it, and so an oversized one fails before it is sent anywhere
	var payload []byte
	if data != nil {
		payload, err = msgpack.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to serialize signal payload using msgpack: %w", err)
		}
	}
	if s.maxPayloadSize > 0 && len(payload) > s.maxPayloadSize {
		return fmt.Errorf("%w: payload is %d bytes and the limit is %d", ErrPayloadTooLarge, len(payload), s.maxPayloadSize)
	}

	env, err := builtinactor.InvokeActor(ctx, s.svc, s.actorType, signalID, methodComplete, completeRequest{Data: payload})
	if err != nil {
		return err
	}

	// A completion that lost the race reports itself in the result rather than as an error from the actor, so it arrives the same way whether the actor ran here or on a peer
	var res completeResult
	if env != nil {
		err = env.Decode(&res)
		if err != nil {
			return fmt.Errorf("failed to decode signal completion response: %w", err)
		}
	}
	if res.AlreadyCompleted {
		return ErrAlreadyCompleted
	}

	return nil
}

// Check reports whether the signal has already completed, without blocking, returning its payload when it has
// The payload is nil when the signal completed without one, so completed is what to branch on
//
// A signal whose retention window has passed reads as not completed, since the completion record it would be answered from is gone
func (s *SignalService) Check(ctx context.Context, signalID string) (data actor.Envelope, completed bool, err error) {
	err = validateSignalID(signalID)
	if err != nil {
		return nil, false, err
	}

	env, err := builtinactor.InvokeActor(ctx, s.svc, s.actorType, signalID, methodCheck, nil)
	if err != nil {
		return nil, false, err
	}

	var res signalResult
	if env != nil {
		err = env.Decode(&res)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decode signal response: %w", err)
		}
	}
	if !res.Completed {
		return nil, false, nil
	}

	return payloadEnvelope(res.Data), true, nil
}

// attach returns this process's shared wait for a signal, starting one when this is the first local caller
func (s *SignalService) attach(signalID string) *sharedWait {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.waits[signalID]
	if ok {
		w.refs++
		return w
	}

	// The invocation runs on its own context rather than the first caller's, so one caller giving up never cancels the wait the others are still attached to
	ctx, cancel := context.WithCancel(context.Background())
	w = &sharedWait{
		done:   make(chan struct{}),
		cancel: cancel,
		refs:   1,
	}
	s.waits[signalID] = w

	go s.runWait(ctx, signalID, w)

	return w
}

// detach releases one caller's hold on a shared wait, cancelling the invocation once the last caller has left
func (s *SignalService) detach(signalID string, w *sharedWait) {
	s.mu.Lock()

	w.refs--
	last := w.refs <= 0
	if last {
		// Only drop the entry while it is still ours: a wait that has already resolved removed itself, and a later caller may have installed a fresh one in its place
		cur, ok := s.waits[signalID]
		if ok && cur == w {
			delete(s.waits, signalID)
		}
	}
	s.mu.Unlock()

	if last {
		w.cancel()
	}
}

// runWait drives a signal's wait invocation until it resolves, retrying the failures along the way rather than surfacing them
// A wait is long-lived by design, so a host dying or handing the actor over mid-wait is an ordinary event that the callers should never see: to them the signal has simply not fired yet
func (s *SignalService) runWait(ctx context.Context, signalID string, w *sharedWait) {
	var delay time.Duration
	for {
		res, err := s.invokeWait(ctx, signalID)
		switch {
		case err == nil:
			s.resolve(signalID, w, res.Data, nil)
			return
		case ctx.Err() != nil:
			// The last attached caller left, so there is nobody to deliver a result to
			s.resolve(signalID, w, nil, ctx.Err())
			return
		case !isRetryable(err):
			s.resolve(signalID, w, nil, err)
			return
		}

		// Back off before re-resolving the placement, so a signal whose host is down does not spin
		delay = nextRetryDelay(delay)
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			s.resolve(signalID, w, nil, ctx.Err())
			return
		}
	}
}

// invokeWait performs one wait invocation, which blocks on the signal's owning host until it completes
func (s *SignalService) invokeWait(ctx context.Context, signalID string) (signalResult, error) {
	var res signalResult

	env, err := builtinactor.InvokeActor(ctx, s.svc, s.actorType, signalID, methodWait, nil)
	if err != nil {
		return res, err
	}
	if env == nil {
		return res, errors.New("signal returned an empty response")
	}

	err = env.Decode(&res)
	if err != nil {
		return res, fmt.Errorf("failed to decode signal response: %w", err)
	}
	if !res.Completed {
		// A wait call only returns once the signal has completed, so a pending result means the invocation contract was violated
		return res, errors.New("signal returned a pending result for a wait call")
	}

	return res, nil
}

// resolve records a shared wait's outcome and releases every attached caller
// The entry is dropped from the map first, so a caller arriving after this point starts a fresh wait rather than attaching to a finished one
func (s *SignalService) resolve(signalID string, w *sharedWait, data []byte, err error) {
	s.mu.Lock()
	cur, ok := s.waits[signalID]
	if ok && cur == w {
		delete(s.waits, signalID)
	}
	s.mu.Unlock()

	// Written before done is closed, which is what publishes them to every attached caller
	w.data = data
	w.err = err
	close(w.done)
}

// isRetryable reports whether a failed wait invocation should be retried against a freshly resolved placement
// Only the failures that can never resolve themselves are fatal: everything else, including a transport error with no sentinel to match on, is the kind of interruption a long-lived wait exists to ride out
func isRetryable(err error) bool {
	switch {
	case errors.Is(err, actor.ErrActorTypeUnsupported),
		errors.Is(err, actor.ErrActorTypeReserved),
		errors.Is(err, actor.ErrMethodReserved),
		errors.Is(err, actor.ErrServiceNotInitialized):
		return false
	default:
		return true
	}
}

// nextRetryDelay returns the backoff that follows the given one, doubling up to the cap
func nextRetryDelay(current time.Duration) time.Duration {
	if current <= 0 {
		return initialRetryDelay
	}

	return min(2*current, maxRetryDelay)
}

// payloadEnvelope wraps a completion payload for the caller, returning nil when the signal carried none
func payloadEnvelope(data []byte) actor.Envelope {
	if len(data) == 0 {
		return nil
	}

	return types.MsgpackEnvelope(data)
}

// validateSignalID checks that a signal ID can be used as an actor ID
func validateSignalID(signalID string) error {
	if signalID == "" {
		return errors.New("signal ID is required")
	}

	err := ref.ValidateComponents(signalID)
	if err != nil {
		return fmt.Errorf("invalid signal ID: %w", err)
	}

	return nil
}
