// Package taskpool provides a built-in actor that runs a distributed pool of task workers, processing a strict number of long-running tasks per host and scaling out across the cluster
//
// Build one with New and register the result on a host with the host's RegisterBuiltInActor method, then obtain a TaskPoolService with Service to submit tasks
// Each submitted task becomes one durable job delivered to its own worker actor, which runs the task once and then frees its slot, so the tasks of a pool are drained from a shared queue by whichever hosts have spare capacity
//
// Concurrency is bounded per host by a strict, in-process limit (see WithConcurrency): a host runs at most that many tasks at once across all of the pool's queues, so more hosts mean more tasks run in parallel
// A task may optionally require a capability (such as "gpu"): a host advertises the capabilities it has with WithCapability, and a task that requires one is only ever run on a host that advertises it, while a task with no required capability runs anywhere
//
// A task pool does not track results: the handler is responsible for communicating its outcome, for example by writing to a database, calling an external API, or invoking another actor
package taskpool

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// taskPoolActorTypePrefix namespaces task pool actor types within the task pool's own bare type space
	// The reserved built-in prefix is added by the host when registering, so it is not included here
	taskPoolActorTypePrefix = "taskpool."

	// methodRun delivers a submitted task to its worker
	methodRun = "run"

	// defaultConcurrency is the strict per-host task limit when WithConcurrency is not set
	defaultConcurrency = 1
)

// Task is a unit of work handed to the pool's handler
// It carries the task's identity, its required capability (empty for a task with no requirement), and its opaque input, which Decode reads into a custom object
type Task interface {
	// ID returns the task's unique identifier, which is the actor ID of the worker running it
	ID() string
	// Capability returns the capability the task required, or an empty string when it required none
	Capability() string
	// Decode reads the task's input into the provided object, and is a no-op when the task carried no input
	Decode(into any) error
}

// TaskPool is a built-in task pool actor, returned by New and registered on a host with RegisterBuiltInActor
// It registers one reserved actor type per queue (a base queue plus one per advertised capability), all sharing a single strict per-host capacity group, and exposes a Service method for submitting and managing tasks
type TaskPool struct {
	// name is the pool's unique name
	name string
	// baseType is the bare actor type of the base (no-capability) queue
	baseType string
	// registrations is every reserved actor type this pool registers, with the base queue first
	registrations []builtinactor.BuiltInActorRegistration
}

// New builds a task pool built-in actor identified by name
//
// It registers a base queue served by every host, plus one queue per capability advertised with WithCapability, all drawing from a single strict per-host budget set by WithConcurrency (default 1)
// The function set with WithHandler runs each task and is required
//
// Register the returned value on a host with the host's RegisterBuiltInActor method, then submit tasks through the service returned by Service
// Register the same pool on every host that should run its tasks, advertising each host's own capabilities
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*TaskPool, error) {
	if name == "" {
		return nil, errors.New("task pool name is required")
	}

	err := ref.ValidateComponents(name)
	if err != nil {
		return nil, fmt.Errorf("invalid task pool name: %w", err)
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// A handler is the whole point of a pool
	if o.handler == nil {
		return nil, errors.New("WithHandler is required")
	}

	// The strict per-host limit defaults to one task at a time
	concurrency := o.concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}

	log := o.logger
	if log != nil {
		log = log.With(slog.String("taskPool", name))
	}

	// Validate the advertised capabilities up front, rejecting empties and duplicates
	seenCapabilities := make(map[string]struct{}, len(o.capabilities))
	for _, capName := range o.capabilities {
		if capName == "" {
			return nil, errors.New("capability name must not be empty")
		}
		err = ref.ValidateComponents(capName)
		if err != nil {
			return nil, fmt.Errorf("invalid capability %q: %w", capName, err)
		}
		_, dup := seenCapabilities[capName]
		if dup {
			return nil, fmt.Errorf("capability %q is declared more than once", capName)
		}
		seenCapabilities[capName] = struct{}{}
	}

	baseType := taskPoolActorTypePrefix + name

	// Every queue of this pool shares one strict per-host budget, keyed by the pool's full base type so it never collides with another pool's group
	group := builtinactor.FullActorType(baseType)

	// A worker halts itself once its task reaches a terminal outcome, so a finished worker frees its host slot immediately rather than lingering
	// The idle timeout is intentionally left at the framework default: it only backstops the rare dead-lettered worker, and it must stay comfortably above the retry backoff, otherwise a worker awaiting a retry would idle out and its lease would stop being renewed
	regOpts := actorcore.RegisterActorOptions{
		// The coarse, cluster-wide placement hint mirrors the strict limit so hosts are rarely handed more tasks than they can run
		ConcurrencyLimit:   concurrency,
		MaxAttempts:        o.maxAttempts,
		InitialRetryDelay:  o.initialRetryDelay,
		CapacityGroup:      group,
		CapacityGroupLimit: concurrency,
	}

	// The base (no-capability) queue is always registered first, followed by one queue per advertised capability
	queues := append([]string{""}, o.capabilities...)
	regs := make([]builtinactor.BuiltInActorRegistration, 0, len(queues))
	for _, capName := range queues {
		bareType := baseType
		if capName != "" {
			bareType = baseType + "." + capName
		}

		// Capture the per-queue values for the factory closure
		queueBareType := bareType
		queueCapability := capName
		regs = append(regs, builtinactor.BuiltInActorRegistration{
			ActorType: queueBareType,
			Factory: func(actorID string, service *actor.Service) actor.Actor {
				// Each task gets its own worker instance, bound to a privileged client so it can halt itself once done
				return &worker{
					handler:    o.handler,
					accept:     o.accept,
					actorID:    actorID,
					capability: queueCapability,
					log:        log,
					client:     builtinactor.NewClient[struct{}](queueBareType, actorID, service),
				}
			},
			RegisterOptions: regOpts,
			Singleton:       false,
		})
	}

	return &TaskPool{
		name:          name,
		baseType:      baseType,
		registrations: regs,
	}, nil
}

// ActorType returns the reserved base actor type registered for this pool
// The pool registers additional per-capability types too, exposed through Registrations
func (p *TaskPool) ActorType() string {
	return p.baseType
}

// Factory returns the factory for the base queue
// The host registers every queue through Registrations, so this is only the single-type fallback of the built-in contract
func (p *TaskPool) Factory() actor.Factory {
	return p.registrations[0].Factory
}

// RegisterOptions returns the registration options shared by every queue of the pool
func (p *TaskPool) RegisterOptions() actorcore.RegisterActorOptions {
	return p.registrations[0].RegisterOptions
}

// Singleton reports that a task pool is not a singleton: its workers are created on demand, one per task, and need no bootstrapping
func (p *TaskPool) Singleton() bool {
	return false
}

// Registrations returns every reserved actor type the pool registers: the base queue plus one per advertised capability, all sharing the pool's capacity group
func (p *TaskPool) Registrations() []builtinactor.BuiltInActorRegistration {
	return p.registrations
}

// capabilityFromType maps a full reserved actor type back to the capability it serves, returning an empty string for the base queue
func (p *TaskPool) capabilityFromType(fullType string) string {
	bareType := strings.TrimPrefix(fullType, ref.BuiltInActorTypePrefix)
	if bareType == p.baseType {
		return ""
	}
	return strings.TrimPrefix(bareType, p.baseType+".")
}

// taskEnvelope is the concrete Task passed to the handler, wrapping the occurrence's identity and input
type taskEnvelope struct {
	id         string
	capability string
	data       actor.Envelope
}

// ID returns the task's unique identifier
func (t *taskEnvelope) ID() string {
	return t.id
}

// Capability returns the capability the task required, or an empty string when it required none
func (t *taskEnvelope) Capability() string {
	return t.capability
}

// Decode reads the task's input into the provided object, and is a no-op when there was no input
func (t *taskEnvelope) Decode(into any) error {
	if t.data == nil {
		return nil
	}
	return t.data.Decode(into)
}
