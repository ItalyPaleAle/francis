package taskpool

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

// TaskStatus is the lifecycle stage of a submitted task
type TaskStatus int

const (
	// TaskStatusPending indicates the task is queued and waiting to run
	TaskStatusPending TaskStatus = iota
	// TaskStatusActive indicates the task is currently running on a host
	TaskStatusActive
	// TaskStatusDeadLettered indicates the task exhausted its retries (or failed permanently) and was recorded in the dead-letter store
	TaskStatusDeadLettered
)

// String implements fmt.Stringer
func (s TaskStatus) String() string {
	switch s {
	case TaskStatusPending:
		return "pending"
	case TaskStatusActive:
		return "active"
	case TaskStatusDeadLettered:
		return "dead-lettered"
	default:
		return "unknown"
	}
}

// TaskInfo describes a submitted task, spanning both live (pending/active) and dead-lettered tasks
// Attempts and LastError are only populated once the task has been dead-lettered
type TaskInfo struct {
	// TaskID is the task's identifier, as returned by Submit
	TaskID string
	// Capability is the capability the task required, or an empty string when it required none
	Capability string
	// Status is the task's current lifecycle stage
	Status TaskStatus
	// DueTime is when the task is next scheduled to run
	DueTime time.Time
	// Attempts is the number of attempts made, populated once dead-lettered
	Attempts int
	// LastError is the last recorded error, populated once dead-lettered
	LastError string
	// CreatedAt is when the task was submitted
	CreatedAt time.Time
}

// Service binds the task pool to an actor.Service, returning a TaskPoolService that exposes Submit and task management pre-configured for that service
// Obtain the service from a host with host.Service()
func (p *TaskPool) Service(svc *actor.Service) *TaskPoolService {
	return &TaskPoolService{
		pool: p,
		svc:  svc,
	}
}

// TaskPoolService exposes the operations of a task pool (Submit and task management), bound to a specific actor.Service
// Obtain one from TaskPool.Service
type TaskPoolService struct {
	pool *TaskPool
	svc  *actor.Service
}

// SubmitOption configures a task submission
type SubmitOption func(*submitOptions)

type submitOptions struct {
	capability string
	taskKey    string
}

// WithRequiredCapability routes the task to a queue only hosts advertising the capability serve
// Without it the task goes to the base queue, which every host serves
func WithRequiredCapability(capability string) SubmitOption {
	return func(o *submitOptions) {
		o.capability = capability
	}
}

// WithTaskKey makes the submission idempotent, using the key as the task's identity
// Submitting twice with the same key (and the same required capability) produces a single task, and the key becomes the task ID
// Without a key each submission is a distinct task with a server-generated ID
func WithTaskKey(key string) SubmitOption {
	return func(o *submitOptions) {
		o.taskKey = key
	}
}

// Submit enqueues a task for the pool and returns its task ID
//
// The task runs once, on any host with free capacity that serves the required capability (or any host, when none is required)
// The input is the task's payload, decoded by the handler with Task.Decode; pass nil when the task carries no input
// A task pool does not return a result: the handler is responsible for communicating the outcome
func (s *TaskPoolService) Submit(ctx context.Context, input any, opts ...SubmitOption) (taskID string, err error) {
	var so submitOptions
	for _, opt := range opts {
		opt(&so)
	}

	// Resolve the queue (actor type) from the required capability; the empty capability is the base queue every host serves
	// The capability is not validated against this host's advertised capabilities on purpose: a task may require a capability only other hosts have, and it stays pending until a host that advertises it picks it up
	bareType := s.pool.baseType
	if so.capability != "" {
		err = ref.ValidateComponents(so.capability)
		if err != nil {
			return "", fmt.Errorf("invalid required capability: %w", err)
		}
		bareType = s.pool.baseType + "." + so.capability
	}

	// Each task is its own actor, so mint a unique ID unless the caller supplied a key for idempotency
	actorID := so.taskKey
	if actorID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return "", fmt.Errorf("failed to generate task ID: %w", err)
		}
		actorID = id.String()
	} else {
		err = ref.ValidateComponents(actorID)
		if err != nil {
			return "", fmt.Errorf("invalid task key: %w", err)
		}
	}

	// A supplied key also dedups the dispatch itself, so a retried submission does not enqueue duplicate work
	var jobOpts []actor.JobOption
	if so.taskKey != "" {
		jobOpts = append(jobOpts, actor.WithIdempotencyKey(so.taskKey))
	}

	// Dispatch through a privileged client, since the public service rejects built-in actor types
	client := builtinactor.NewClient[struct{}](bareType, actorID, s.svc)
	jobID, err := client.Dispatch(ctx, methodRun, input, jobOpts...)
	if err != nil {
		return "", fmt.Errorf("failed to submit task: %w", err)
	}

	return jobID, nil
}

// GetTask returns the information for a task by its ID, spanning both live and dead-lettered tasks
// It returns actor.ErrJobNotFound if the task cannot be found
func (s *TaskPoolService) GetTask(ctx context.Context, taskID string) (TaskInfo, error) {
	// GetJob is keyed only by the task ID and is not guarded against built-in types, so the public service serves it directly
	info, err := s.svc.GetJob(ctx, taskID)
	if err != nil {
		return TaskInfo{}, err
	}

	return s.toTaskInfo(info), nil
}

// CancelTask cancels a live (pending or active) task
// It returns actor.ErrJobNotFound if the task cannot be found among live tasks
func (s *TaskPoolService) CancelTask(ctx context.Context, taskID string) error {
	// Look up the task's worker so the cancel can target it through a privileged client
	info, err := s.svc.GetJob(ctx, taskID)
	if err != nil {
		return err
	}

	bareType := s.pool.bareTypeOf(info.ActorType)
	client := builtinactor.NewClient[struct{}](bareType, info.ActorID, s.svc)
	return client.CancelJob(ctx, taskID)
}

// RetryTask re-submits a dead-lettered task, scheduled to run as soon as possible, and returns the new task ID
// It returns actor.ErrJobNotFound if the dead-lettered task cannot be found
func (s *TaskPoolService) RetryTask(ctx context.Context, taskID string) (newTaskID string, err error) {
	// RetryJob is keyed only by the task ID and is not guarded against built-in types, so the public service serves it directly
	return s.svc.RetryJob(ctx, taskID)
}

// toTaskInfo maps a job's info to the pool's task info, translating the actor type back to its capability
func (s *TaskPoolService) toTaskInfo(j actor.JobInfo) TaskInfo {
	return TaskInfo{
		TaskID:     j.JobID,
		Capability: s.pool.capabilityFromType(j.ActorType),
		Status:     taskStatusFromJob(j.Status),
		DueTime:    j.DueTime,
		Attempts:   j.Attempts,
		LastError:  j.LastError,
		CreatedAt:  j.CreatedAt,
	}
}

// bareTypeOf strips the reserved built-in prefix from a full actor type, for building a client bound to a task's worker
func (p *TaskPool) bareTypeOf(fullType string) string {
	return strings.TrimPrefix(fullType, ref.BuiltInActorTypePrefix)
}

// taskStatusFromJob maps a job status to the pool's task status
func taskStatusFromJob(s actor.JobStatus) TaskStatus {
	switch s {
	case actor.JobStatusActive:
		return TaskStatusActive
	case actor.JobStatusDeadLettered:
		return TaskStatusDeadLettered
	default:
		return TaskStatusPending
	}
}
