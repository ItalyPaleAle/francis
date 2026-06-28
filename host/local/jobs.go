package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

// Dispatch sends a durable, fire-and-forget job to an actor, returning the server-issued job ID.
func (h *Host) Dispatch(ctx context.Context, actorType string, actorID string, method string, data any, properties actor.JobProperties) (string, error) {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return "", err
	}

	err = properties.Validate()
	if err != nil {
		return "", err
	}

	// The idempotency key maps to the alarm name: without one each dispatch gets a fresh random name so it is a distinct job
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

	req, err := jobPropertiesToSetAlarmReq(properties, method, data, h.clock.Now())
	if err != nil {
		return "", err
	}

	jobID, err := h.actorProvider.DispatchJob(ctx, ref.NewAlarmRef(actorType, actorID, name), req)
	if err != nil {
		return "", fmt.Errorf("failed to dispatch job: %w", err)
	}

	return jobID, nil
}

// GetJob returns a job by its ID, spanning both live and dead-lettered jobs.
func (h *Host) GetJob(ctx context.Context, jobID string) (actor.JobInfo, error) {
	res, err := h.actorProvider.GetJob(ctx, jobID)
	if errors.Is(err, components.ErrNoJob) {
		return actor.JobInfo{}, actor.ErrJobNotFound
	} else if err != nil {
		return actor.JobInfo{}, fmt.Errorf("failed to get job: %w", err)
	}

	return jobInfoToActor(res), nil
}

// ListJobs returns all live and dead-lettered jobs for an actor.
func (h *Host) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return nil, err
	}

	res, err := h.actorProvider.ListJobs(ctx, actorType, actorID)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	out := make([]actor.JobInfo, len(res))
	for i := range res {
		out[i] = jobInfoToActor(res[i])
	}
	return out, nil
}

// CancelJob cancels a live job for an actor.
func (h *Host) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	err := ref.ValidateComponents(actorType, actorID)
	if err != nil {
		return err
	}

	err = h.actorProvider.CancelJob(ctx, actorType, actorID, jobID)
	if errors.Is(err, components.ErrNoJob) {
		return actor.ErrJobNotFound
	} else if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	return nil
}

// RetryJob re-dispatches a dead-lettered job as a fresh immediate job and removes the dead-letter record.
func (h *Host) RetryJob(ctx context.Context, jobID string) (string, error) {
	// The provider re-dispatches and removes the dead-letter record atomically, so a crash cannot duplicate the job
	newID, err := h.actorProvider.RetryDeadJob(ctx, jobID)
	if errors.Is(err, components.ErrNoJob) {
		return "", actor.ErrJobNotFound
	} else if err != nil {
		return "", fmt.Errorf("failed to retry job: %w", err)
	}

	return newID, nil
}

// jobPropertiesToSetAlarmReq builds a provider job request from the public job properties, encoding the input as MessagePack.
func jobPropertiesToSetAlarmReq(p actor.JobProperties, method string, input any, now time.Time) (components.SetAlarmReq, error) {
	req := components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  p.EffectiveDueTime(now),
			Interval: p.Interval,
			Cron:     p.Cron,
		},
		Kind:      components.AlarmKindJob,
		JobMethod: method,
	}

	if input != nil {
		var data bytes.Buffer
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(&data)

		err := enc.Encode(input)
		if err != nil {
			return components.SetAlarmReq{}, fmt.Errorf("failed to serialize data using msgpack: %w", err)
		}
		req.Data = data.Bytes()
	}

	if !p.TTL.IsZero() {
		req.TTL = &p.TTL
	}

	return req, nil
}

// jobInfoToActor maps a provider JobInfo to the public actor JobInfo.
func jobInfoToActor(j components.JobInfo) actor.JobInfo {
	return actor.JobInfo{
		JobID:     j.JobID,
		ActorType: j.ActorType,
		ActorID:   j.ActorID,
		Method:    j.Method,
		Status:    jobStatusToActor(j.Status),
		DueTime:   j.DueTime,
		Interval:  j.Interval,
		Cron:      j.Cron,
		Attempts:  j.Attempts,
		LastError: j.LastError,
		CreatedAt: j.CreatedAt,
	}
}

// jobStatusToActor maps a provider job status to the public actor job status.
func jobStatusToActor(s components.JobStatus) actor.JobStatus {
	switch s {
	case components.JobStatusActive:
		return actor.JobStatusActive
	case components.JobStatusDeadLettered:
		return actor.JobStatusDeadLettered
	default:
		return actor.JobStatusPending
	}
}
