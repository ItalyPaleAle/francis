package actorcore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
)

// ErrCapacityExhausted is returned by RunOccurrence when the actor type's capacity group is full on this host
// The host declines the occurrence so it is re-routed to another host, without counting an attempt
var ErrCapacityExhausted = errors.New("capacity group is full on this host")

// Occurrence is a single delivery of an alarm or a job to an actor
type Occurrence struct {
	// Job is true for a job, which is delivered to the actor's Job method rather than its Alarm method
	Job bool
	// Name is the alarm's name, passed to the Alarm method
	Name string
	// JobMethod is the method passed to the Job method
	JobMethod string
	// Data is the occurrence's payload, encoded as MessagePack
	Data []byte
	// RequestID identifies the occurrence, so the actor can detect a duplicate delivery of it
	RequestID string
}

// RunOccurrence delivers an alarm or job occurrence to an actor whose turn lock the caller holds
// It returns an error wrapping ErrActorMethodUnsupported when the actor does not implement the method, and one wrapping ErrCapacityExhausted when a job's capacity group is full
// Any other error is the actor's own, returned unwrapped so the caller can classify it
func (m *Manager) RunOccurrence(ctx context.Context, act *ActiveActor, o Occurrence) error {
	// Stamp a per-occurrence key into the context so the actor can detect duplicate deliveries of the same occurrence without confusing them with legitimate subsequent firings of a repeating one
	ctx = actor.WithRequestID(ctx, o.RequestID)

	if o.Job {
		// The actor must implement the Job method to receive jobs
		obj, ok := act.Instance.(actor.ActorJob)
		if !ok {
			return fmt.Errorf("actor of type '%s' does not implement the Job method: %w", act.ActorType(), ErrActorMethodUnsupported)
		}

		// Enforce the actor type's host-local capacity group before running the job
		release, admitted := m.TryAcquireCapacity(act.ActorType())
		if !admitted {
			return fmt.Errorf("%w for actor type '%s'", ErrCapacityExhausted, act.ActorType())
		}
		defer release()

		data, releaseData := dataEnvelope(o.Data)
		defer releaseData()
		return obj.Job(ctx, o.JobMethod, data)
	}

	// The actor must implement the Alarm method to receive alarms
	obj, ok := act.Instance.(actor.ActorAlarm)
	if !ok {
		return fmt.Errorf("actor of type '%s' does not implement the Alarm method: %w", act.ActorType(), ErrActorMethodUnsupported)
	}

	data, releaseData := dataEnvelope(o.Data)
	defer releaseData()
	return obj.Alarm(ctx, o.Name, data)
}

// RunJobFailed delivers the optional JobFailed hook of a dead-lettered job to its actor, on the actor's turn lock
// An actor that does not implement the hook is a no-op
func (m *Manager) RunJobFailed(ctx context.Context, r ref.ActorRef, jobID string, method string, data []byte, jobErr error) error {
	_, err := m.LockAndInvoke(ctx, r, func(invokeCtx context.Context, act *ActiveActor) (any, error) {
		obj, ok := act.Instance.(actor.ActorJobFailed)
		if !ok {
			return nil, nil
		}

		env, releaseData := dataEnvelope(data)
		defer releaseData()

		rErr := obj.JobFailed(invokeCtx, jobID, method, env, jobErr)
		return nil, rErr
	})
	return err
}

// dataEnvelope wraps MessagePack data in an envelope the actor can decode, and returns a function that releases it once the actor is done
// Empty data is left as a nil envelope, since there is nothing to decode from it
func dataEnvelope(data []byte) (actor.Envelope, func()) {
	if len(data) == 0 {
		return nil, func() {}
	}

	// The MessagePack decoder satisfies the Envelope interface
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	return dec, func() { msgpack.PutDecoder(dec) }
}
