package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// Dispatch sends a durable, fire-and-forget job to an actor through the runtime, returning the server-issued job ID.
func (h *Host) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, properties actor.JobProperties) (string, error) {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return "", err
	}

	err = properties.Validate()
	if err != nil {
		return "", err
	}

	// The idempotency key maps to the alarm name
	// Without one each dispatch gets a fresh random name so it is a distinct job
	name := properties.IdempotencyKey
	if name != "" {
		err = ref.ValidateComponents(name)
		if err != nil {
			return "", err
		}
	} else {
		nameObj, err := uuid.NewRandom()
		if err != nil {
			return "", fmt.Errorf("failed to generate UUID: %w", err)
		}
		name = nameObj.String()
	}

	props, err := actorJobPropsToProtocol(properties, data, h.clock.Now())
	if err != nil {
		return "", err
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.DispatchJob(reqCtx, protocol.DispatchJobRequest{
		ActorType:     actorType,
		ActorID:       actorID,
		Method:        method,
		Name:          name,
		JobProperties: props,
	})
	if err != nil {
		return "", fmt.Errorf("failed to dispatch job: %w", err)
	}

	return res.JobID, nil
}

// GetJob returns a job by its ID, spanning both live and dead-lettered jobs.
func (h *Host) GetJob(ctx context.Context, jobID string) (actor.JobInfo, error) {
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.GetJob(reqCtx, protocol.GetJobRequest{JobID: jobID})
	if isProtocolErrorCode(err, protocol.ErrCodeJobNotFound) {
		return actor.JobInfo{}, actor.ErrJobNotFound
	} else if err != nil {
		return actor.JobInfo{}, fmt.Errorf("failed to get job: %w", err)
	}

	return protocolJobInfoToActor(res.JobInfo), nil
}

// ListJobs returns all live and dead-lettered jobs for an actor.
func (h *Host) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.ListJobs(reqCtx, protocol.ListJobsRequest{
		ActorRef: protocol.ActorRef{ActorType: actorType, ActorID: actorID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	out := make([]actor.JobInfo, len(res.Jobs))
	for i := range res.Jobs {
		out[i] = protocolJobInfoToActor(res.Jobs[i])
	}

	return out, nil
}

// CancelJob cancels a live job for an actor.
func (h *Host) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	err = h.runtimeClient.CancelJob(reqCtx, protocol.CancelJobRequest{
		ActorType: actorType,
		ActorID:   actorID,
		JobID:     jobID,
	})
	if isProtocolErrorCode(err, protocol.ErrCodeJobNotFound) {
		return actor.ErrJobNotFound
	} else if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	return nil
}

// RetryJob re-dispatches a dead-lettered job and returns the new job ID.
func (h *Host) RetryJob(ctx context.Context, jobID string) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	res, err := h.runtimeClient.RetryJob(reqCtx, protocol.RetryJobRequest{JobID: jobID})
	if isProtocolErrorCode(err, protocol.ErrCodeJobNotFound) {
		return "", actor.ErrJobNotFound
	} else if err != nil {
		return "", fmt.Errorf("failed to retry job: %w", err)
	}

	return res.JobID, nil
}

// jobFailed runs an actor's optional JobFailed hook at the runtime's request, after the runtime has dead-lettered a job
// It is best-effort: an actor that does not implement the hook is a no-op, and the dead-letter record is already the source of truth
func (h *Host) jobFailed(ctx context.Context, req protocol.JobFailedRequest) *protocol.Error {
	aRef := ref.NewActorRef(req.ActorType, req.ActorID)

	_, err := h.core.LockAndInvoke(ctx, aRef, func(invokeCtx context.Context, act *actorcore.ActiveActor) (any, error) {
		obj, ok := act.Instance.(actor.ActorJobFailed)
		if !ok {
			return nil, nil
		}

		var data actor.Envelope
		if len(req.Data) > 0 {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(req.Data))
			defer msgpack.PutDecoder(dec)
			data = dec
		}

		return nil, obj.JobFailed(invokeCtx, req.JobID, req.Method, data, errors.New(req.ErrorMessage))
	})
	if err != nil {
		return actorcore.InvokeErrorToProtocol(err)
	}

	return nil
}
