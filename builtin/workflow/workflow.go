// Package workflow provides a built-in actor that runs durable workflows: sequences of steps, static parallel groups, dynamic fan-out, child workflows, waits on external events, suspend and resume, and compensations that roll back what already succeeded
//
// Build one with New and register the result on a host with the host's RegisterBuiltInActor method, then obtain a WorkflowService with Service to start and drive instances
// A workflow is a declared graph of named steps plus plain Go handler functions, and the engine is a state machine over a durable journal: there is no code-as-workflow SDK, no replay, and therefore no determinism constraints on user code
//
// The central rule is the orchestration boundary: the Workflow actor orchestrates and performs nothing
// It reads and writes its own journal, arms and drops its timers, and dispatches jobs
// Every unit of work, without exception, runs on a worker actor, which is where WithRun and WithCompensate are invoked
// That is enforced by the code rather than by convention: the definition exposes no hook that runs on the Workflow actor, advance is a pure function of the journal and the definition, and a fan-out's size and a step's condition are both outputs of steps rather than callbacks the orchestrator runs
//
// The engine owns its own failure handling: attempts, dead-letter recovery, and the deadline are recorded in the journal and driven by the same reconcile loop, rather than delegated to per-actor-type settings the engine cannot observe
// Execution is at-least-once, like everything else in Francis, so handlers must be idempotent
package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/cronjob"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/ref"
)

const (
	// workflowActorTypePrefix namespaces workflow actor types within the workflow's own bare type space
	// The reserved built-in prefix is added by the host when registering, so it is not included here
	workflowActorTypePrefix = "workflow."

	// workerTypeSuffix and undoTypeSuffix name the two worker type families, which are separate so compensations have their own per-host budget and cannot be starved by forward work
	workerTypeSuffix = ".worker"
	undoTypeSuffix   = ".undo"
	// registryTypeSuffix names the cluster-wide singleton that records each version's definition fingerprint
	registryTypeSuffix = ".registry"

	// idDelimiter joins the components of a worker's actor ID
	idDelimiter = "|"

	// orchestratorMaxAttempts and orchestratorRetryDelay are higher than the framework defaults
	// We trust that because each workflow is idempotent and we strive for workflows to complete
	orchestratorMaxAttempts = 20
	orchestratorRetryDelay  = 5 * time.Second
	// workerMaxAttempts covers the one case a worker returns an error to Francis: the report dispatch itself failed
	workerMaxAttempts = 5
	// orchestratorIdleTimeout keeps an instance from lingering between reports, which for a wide fan-out would hold an activation per instance for no reason
	orchestratorIdleTimeout = 5 * time.Minute
)

// Workflow is a built-in workflow actor, returned by New and registered on a host with RegisterBuiltInActor
// It registers the orchestrator type, the worker and undo types , the definition registry singleton, and, when WithAutoPurge is set, a cron job that sweeps terminated instances
type Workflow struct {
	name string
	// baseType is the bare actor type of the orchestrator, and the prefix of every other type this workflow registers
	baseType string
	def      *definition
	log      *slog.Logger
	metrics  *engineMetrics
	// registrations is every reserved actor type this workflow registers, with the orchestrator first
	registrations []builtinactor.BuiltInActorRegistration
	// purgeCron is the auto-purge cron job, when WithAutoPurge was set
	purgeCron *cronjob.CronJob

	// boundService is the actor.Service the host handed to this workflow's factories, which is what the auto-purge cron job's handler runs against
	boundService atomic.Pointer[actor.Service]
}

// definition is the validated, immutable graph a workflow runs, shared by every actor the workflow registers
type definition struct {
	name        string
	version     int
	fingerprint string
	steps       []*stepDef
	// byName indexes the top-level steps, and the members of groups, so a report or an event resolves to its step in one lookup
	byName map[string]*stepDef
	// order records the position of each top-level step, so advance walks the graph in declaration order
	order map[string]int

	timeout                   time.Duration
	retention                 RetentionPolicy
	outputStep                string
	maxInputSize              int
	maxOutputSize             int
	maxJournalSize            int
	maxDepth                  int
	unknownVersion            UnknownVersionPolicy
	compensationFailurePolicy CompensationFailurePolicy
}

// New builds a workflow built-in actor identified by name
//
// It validates the graph declared with WithSteps: step and event names must be unique, WithInputFrom, WithSkipOnFailure, and WithSkipIf must name steps that exist and stand in the right order, a fan-out's WithItemsFrom must name an earlier step, and a child definition must itself be valid
//
// Register the returned value on a host with the host's RegisterBuiltInActor method, then start instances through the service returned by Service
// Register the same workflow on every host that should run its steps, advertising each host's own capabilities, and register any child definition the same way on the same hosts
// Names must be unique within a cluster and must not contain '/'
func New(name string, opts ...Option) (*Workflow, error) {
	if name == "" {
		return nil, errors.New("workflow name is required")
	}

	err := ref.ValidateComponents(name)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow name: %w", err)
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// Apply the defaults before validating, so validation sees the values the engine will actually run with
	o.applyDefaults()
	err = o.validatePolicies()
	if err != nil {
		return nil, err
	}

	// Validate the advertised capabilities up front, rejecting empties and duplicates
	err = o.validateCapabilities()
	if err != nil {
		return nil, err
	}

	// Build and validate the graph
	def, err := o.newDefinition(name)
	if err != nil {
		return nil, err
	}

	log := o.logger
	if log != nil {
		log = log.With(slog.String("workflow", name))
	}

	meter := o.meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("github.com/italypaleale/francis/builtin/workflow")
	}
	metrics, err := newEngineMetrics(meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow metrics: %w", err)
	}

	wf := &Workflow{
		name:     name,
		baseType: workflowActorTypePrefix + name,
		def:      def,
		log:      log,
		metrics:  metrics,
	}

	err = wf.buildRegistrations(&o)
	if err != nil {
		return nil, err
	}

	return wf, nil
}

// applyDefaults fills in every option the caller left unset, so the rest of the engine never has to ask whether a value was configured
func (o *options) applyDefaults() {
	if o.version <= 0 {
		o.version = defaultVersion
	}
	if o.timeout <= 0 {
		o.timeout = defaultTimeout
	}
	if o.concurrency <= 0 {
		o.concurrency = defaultConcurrency
	}
	if o.compensateConcurrency <= 0 {
		o.compensateConcurrency = o.concurrency
	}
	if o.maxInputSize <= 0 {
		o.maxInputSize = defaultMaxInputSize
	}
	if o.maxOutputSize <= 0 {
		o.maxOutputSize = defaultMaxOutputSize
	}
	if o.maxJournalSize <= 0 {
		o.maxJournalSize = defaultMaxJournalSize
	}
	if o.maxDepth <= 0 {
		o.maxDepth = defaultMaxDepth
	}
	if o.unknownVersion == "" {
		o.unknownVersion = ParkUnknownVersion
	}
	if o.compensationFailurePolicy == "" {
		o.compensationFailurePolicy = ContinueUnwinding
	}
}

// validateCapabilities rejects an empty or duplicated capability, which would otherwise produce a queue no task can reach or two registrations of the same type
func (o *options) validateCapabilities() error {
	seen := make(map[string]struct{}, len(o.capabilities))
	for _, capName := range o.capabilities {
		if capName == "" {
			return errors.New("capability name must not be empty")
		}

		err := ref.ValidateComponents(capName)
		if err != nil {
			return fmt.Errorf("invalid capability %q: %w", capName, err)
		}

		_, dup := seen[capName]
		if dup {
			return fmt.Errorf("capability %q is declared more than once", capName)
		}
		seen[capName] = struct{}{}
	}

	return nil
}

// validatePolicies rejects misspelled policies instead of silently selecting a different failure behavior
func (o *options) validatePolicies() error {
	if o.unknownVersion != ParkUnknownVersion && o.unknownVersion != FailUnknownVersion {
		return fmt.Errorf("unknown workflow version policy %q", o.unknownVersion)
	}
	if o.compensationFailurePolicy != ContinueUnwinding && o.compensationFailurePolicy != AbortUnwinding {
		return fmt.Errorf("unknown compensation failure policy %q", o.compensationFailurePolicy)
	}
	return nil
}

// flattenSteps expands every loop declaration into its body steps followed by the loop node itself
// A loop's body steps are ordinary steps of the workflow, which is what lets a later step read what the last iteration produced and keeps the linear walk over the graph unchanged
func flattenSteps(specs []StepSpec) ([]*stepDef, error) {
	out := make([]*stepDef, 0, len(specs))
	for i, spec := range specs {
		d := spec.d.clone()
		if d == nil {
			return nil, fmt.Errorf("step at index %d is uninitialized", i)
		}
		if d.kind != KindLoop {
			out = append(out, d)
			continue
		}

		// The bound is resolved here so the fingerprint carries the number the engine actually enforces
		if d.maxIterations < 0 {
			return nil, fmt.Errorf("loop %q has a negative WithMaxIterations", d.name)
		}
		if d.maxIterations == 0 {
			d.maxIterations = defaultMaxIterations
		}

		// The body is emitted before the loop node, so the walk reaches the condition only once an iteration has finished
		d.body = make([]string, len(d.members))
		for j, m := range d.members {
			if m == nil {
				return nil, fmt.Errorf("loop %q has an uninitialized step at index %d", d.name, j)
			}
			d.body[j] = m.name
			out = append(out, m)
		}

		// The members are steps of the graph from here on, and the loop node refers to them by name
		d.members = nil
		out = append(out, d)
	}
	return out, nil
}

// newDefinition assembles the graph and validates every rule a definition has to satisfy before it can run
func (o *options) newDefinition(name string) (*definition, error) {
	if len(o.steps) == 0 {
		return nil, errors.New("WithSteps is required, with at least one step")
	}

	// A loop declares its body inline, so the declaration is flattened into the step list before anything is indexed and the graph the engine walks stays a flat sequence
	steps, err := flattenSteps(o.steps)
	if err != nil {
		return nil, err
	}

	def := &definition{
		name:                      name,
		version:                   o.version,
		steps:                     steps,
		byName:                    map[string]*stepDef{},
		order:                     map[string]int{},
		timeout:                   o.timeout,
		retention:                 o.retention,
		outputStep:                o.outputStep,
		maxInputSize:              o.maxInputSize,
		maxOutputSize:             o.maxOutputSize,
		maxJournalSize:            o.maxJournalSize,
		maxDepth:                  o.maxDepth,
		unknownVersion:            o.unknownVersion,
		compensationFailurePolicy: o.compensationFailurePolicy,
	}

	// Index every step and member by name, which is also where duplicate names and duplicate event names are caught
	eventNames := map[string]string{}
	for i, d := range def.steps {
		def.order[d.name] = i

		err = def.indexStep(d, eventNames)
		if err != nil {
			return nil, err
		}
	}

	// Validate each step against the indexed graph, now that every name is known
	for i, d := range def.steps {
		err = def.validateStep(d, i)
		if err != nil {
			return nil, err
		}
	}

	// WithOutput must name a step that exists, or the instance would terminate with no output and no way to tell why
	if def.outputStep != "" {
		_, ok := def.order[def.outputStep]
		if !ok {
			return nil, fmt.Errorf("WithOutput names step %q, which is not a top-level step of this workflow", def.outputStep)
		}
	}

	// Set the fingerprint in the definition
	def.setFingerprint()

	return def, nil
}

// indexStep records a step and, for a group, its members, rejecting a name or an event name that is already taken
func (def *definition) indexStep(d *stepDef, eventNames map[string]string) error {
	if d == nil {
		return errors.New("parallel member is uninitialized")
	}
	if d.name == "" {
		return errors.New("step name is required")
	}

	err := ref.ValidateComponents(d.name)
	if err != nil {
		return fmt.Errorf("invalid step name %q: %w", d.name, err)
	}

	// The delimiter joins a worker's actor ID, so a step name carrying it would make that ID ambiguous
	if strings.Contains(d.name, idDelimiter) {
		return fmt.Errorf("step name %q must not contain %q", d.name, idDelimiter)
	}

	_, dup := def.byName[d.name]
	if dup {
		return fmt.Errorf("step %q is declared more than once", d.name)
	}
	def.byName[d.name] = d

	// Two steps listening for the same event name would make a raised event ambiguous about which record it belongs to
	if d.kind == KindWait {
		evName := d.effectiveEventName()
		other, taken := eventNames[evName]
		if taken {
			return fmt.Errorf("steps %q and %q both listen for event %q", other, d.name, evName)
		}
		eventNames[evName] = d.name
	}

	for _, m := range d.members {
		err = def.indexStep(m, eventNames)
		if err != nil {
			return err
		}
	}

	return nil
}

// validateStep checks one step's options against the assembled graph, where index is its position among the top-level steps
func (def *definition) validateStep(d *stepDef, index int) error {
	switch d.kind {
	case KindStep:
		if d.run == nil {
			return fmt.Errorf("step %q requires WithRun", d.name)
		}
	case KindParallel:
		if len(d.members) == 0 {
			return fmt.Errorf("parallel group %q requires at least one member", d.name)
		}
		for _, m := range d.members {
			// A group's members are plain or child steps: nesting a group or a fan-out inside one would make a frame that is not a single unwind unit
			if m.kind != KindStep && m.kind != KindChild {
				return fmt.Errorf("parallel group %q may only contain plain or child steps, but %q is a %s", d.name, m.name, m.kind)
			}
			err := def.validateStep(m, index)
			if err != nil {
				return err
			}
			err = m.validateOptions(true)
			if err != nil {
				return err
			}
		}
	case KindForEach:
		if d.itemsFrom == "" {
			return fmt.Errorf("fan-out %q requires WithItemsFrom", d.name)
		}
		err := def.requireEarlierStep(d.name, index, d.itemsFrom, "WithItemsFrom")
		if err != nil {
			return err
		}
		if d.run == nil && d.child == nil {
			return fmt.Errorf("fan-out %q requires either WithRun or WithChild", d.name)
		}
		if d.run != nil && d.child != nil {
			return fmt.Errorf("fan-out %q sets both WithRun and WithChild, which are mutually exclusive", d.name)
		}
	case KindChild:
		if d.child == nil {
			return fmt.Errorf("child step %q requires WithDefinition", d.name)
		}
	case KindWait:
		if d.run != nil {
			return fmt.Errorf("wait step %q cannot have a handler, since it is completed by RaiseEvent rather than run", d.name)
		}
	case KindLoop:
		if len(d.body) == 0 {
			return fmt.Errorf("loop %q requires at least one step in its body", d.name)
		}
		if !d.hasUntil {
			return fmt.Errorf("loop %q requires WithUntil", d.name)
		}
		if !slices.Contains(d.body, d.untilStep) {
			return fmt.Errorf("loop %q names %q in WithUntil, which is not a step of its body", d.name, d.untilStep)
		}
		for _, member := range d.body {
			m := def.byName[member]
			if m == nil {
				return fmt.Errorf("loop %q names %q in its body, which is not a step of this workflow", d.name, member)
			}

			// A loop runs one task at a time, so a group or a fan-out inside one would make an iteration that is not a single unit of work
			if m.kind != KindStep && m.kind != KindChild && m.kind != KindWait {
				return fmt.Errorf("loop %q may only contain plain, child, or wait steps, but %q is a %s", d.name, member, m.kind)
			}
		}
	default:
		return fmt.Errorf("step %q has unknown kind %q", d.name, d.kind)
	}

	// Validate the shared option surface against the node's execution path before checking graph references
	err := d.validateOptions(false)
	if err != nil {
		return err
	}
	if d.child != nil && (d.child.def == nil || d.child.baseType == "" || len(d.child.def.steps) == 0) {
		return fmt.Errorf("step %q references an uninitialized child workflow", d.name)
	}

	// A step can only read the output of a step that has already produced one
	for _, from := range d.inputFrom {
		err := def.requireEarlierStep(d.name, index, from, "WithInputFrom")
		if err != nil {
			return err
		}
	}

	// A condition has to be decided before the step it gates runs
	if d.hasSkipIf {
		err := def.requireEarlierStep(d.name, index, d.skipIfStep, "WithSkipIf")
		if err != nil {
			return err
		}
	}

	// Skipping only makes sense for steps that have not run yet, so the named dependents must come after this one
	for _, dep := range d.skipOnFailure {
		pos, ok := def.order[dep]
		if !ok {
			return fmt.Errorf("step %q names %q in WithSkipOnFailure, which is not a top-level step of this workflow", d.name, dep)
		}
		if pos <= index {
			return fmt.Errorf("step %q names %q in WithSkipOnFailure, but %q does not run after it", d.name, dep, dep)
		}
	}

	// A step's required capability has to be a valid type component, since it becomes part of the worker's actor type
	if d.capability != "" {
		err := ref.ValidateComponents(d.capability)
		if err != nil {
			return fmt.Errorf("step %q has an invalid required capability: %w", d.name, err)
		}
	}

	return nil
}

// requireEarlierStep checks that a referenced step exists among the top-level steps and runs before the referring one
func (def *definition) requireEarlierStep(stepName string, index int, referenced string, option string) error {
	pos, ok := def.order[referenced]
	if !ok {
		return fmt.Errorf("step %q names %q in %s, which is not a top-level step of this workflow", stepName, referenced, option)
	}
	if pos >= index {
		return fmt.Errorf("step %q names %q in %s, but %q does not run before it", stepName, referenced, option, referenced)
	}
	return nil
}

// fingerprint hashes everything about a definition that the engine reads while running an instance, so two hosts cannot serve the same version and then apply different transitions to one journal
func (def *definition) setFingerprint() {
	// Includes the caps, the deadlines, the attempt policies, and the unknown-version and compensation-failure choices all decide what a turn does, so a host that disagrees about any of them needs a new version
	// Handler bodies are the one deliberate exception: changing one needs no new version, which is the direct consequence of not replaying code
	h := sha256.New()
	fmt.Fprintf(h, "workflow=%s;version=%d;output=%s\n", def.name, def.version, def.outputStep)
	fmt.Fprintf(h, "timeout=%d;maxInput=%d;maxOutput=%d;maxJournal=%d;maxDepth=%d;unknownVersion=%s;compFailure=%s\n",
		def.timeout, def.maxInputSize, def.maxOutputSize, def.maxJournalSize, def.maxDepth,
		def.unknownVersion, def.compensationFailurePolicy,
	)
	fmt.Fprintf(h, "retention=%d/%d/%d\n",
		def.retention.forStatus(StatusCompleted),
		def.retention.forStatus(StatusFailed),
		def.retention.forStatus(StatusCancelled),
	)

	for _, d := range def.steps {
		d.writeStepFingerprint(h)
	}

	def.fingerprint = hex.EncodeToString(h.Sum(nil))
}

// writeStepFingerprint writes one step's behavior-affecting options into the running hash
func (def *stepDef) writeStepFingerprint(w io.Writer) {
	// Plain values keep the established fingerprint while delimiter-bearing values are quoted so two different graphs cannot serialize identically
	fmt.Fprintf(w, "step=%s;kind=%s;inputFrom=%s;itemsFrom=%s;skipOnFailure=%s;optional=%t;policy=%s;compensable=%t;capability=%s;event=%s",
		fingerprintValue(def.name), def.kind,
		fingerprintList(def.inputFrom),
		fingerprintValue(def.itemsFrom),
		fingerprintList(def.skipOnFailure),
		def.optional,
		def.failurePolicy,
		def.compensate != nil,
		fingerprintValue(def.capability),
		fingerprintValue(def.eventName),
	)

	// The attempt and deadline policies decide how a failure or a timeout is folded into the journal, so a host that disagrees about them would advance the same journal differently
	fmt.Fprintf(w, ";attempts=%d;backoff=%d/%d;compAttempts=%d;compBackoff=%d/%d;stepTimeout=%d;eventTimeout=%d;maxParallel=%d;compensateOnFailure=%t",
		def.maxAttempts, def.retryInitial, def.retryMax,
		def.compMaxAttempt, def.compInitial, def.compMax,
		def.stepTimeout, def.eventTimeout, def.maxParallel, def.compensateOnFailure,
	)
	if def.hasSkipIf {
		fmt.Fprintf(w, ";skipIf=%s=%t", fingerprintValue(def.skipIfStep), def.skipIfValue)
	}
	if def.kind == KindLoop {
		fmt.Fprintf(w, ";body=%s;until=%s=%t;maxIterations=%d", fingerprintList(def.body), fingerprintValue(def.untilStep), def.untilValue, def.maxIterations)
	}
	if def.child != nil {
		fmt.Fprintf(w, ";child=%s@%d", fingerprintValue(def.child.def.name), def.child.def.version)
	}
	fmt.Fprint(w, "\n")

	for _, m := range def.members {
		fmt.Fprint(w, "  ")
		m.writeStepFingerprint(w)
	}
}

// fingerprintList renders a list unambiguously while preserving the previous encoding for ordinary names
func fingerprintList(values []string) string {
	encoded := make([]string, len(values))
	for i := range values {
		encoded[i] = fingerprintValue(values[i])
	}
	return strings.Join(encoded, ",")
}

// fingerprintValue quotes values that could otherwise inject field or list separators into the fingerprint source
func fingerprintValue(value string) string {
	if !strings.ContainsAny(value, ",;=\n\r\"\\") {
		return value
	}
	return strconv.Quote(value)
}

// buildRegistrations creates every reserved actor type the workflow registers on a host
func (w *Workflow) buildRegistrations(o *options) error {
	// Every worker queue of this workflow shares one strict per-host budget, keyed by the full base type so it never collides with another workflow's group
	workerGroup := builtinactor.FullActorType(w.baseType) + workerTypeSuffix
	undoGroup := builtinactor.FullActorType(w.baseType) + undoTypeSuffix

	regs := make([]builtinactor.BuiltInActorRegistration, 0, 2*(len(o.capabilities)+1)+2)

	// The orchestrator holds the journal and is reached at the instance ID
	// Its retry policy is generous because its turns are idempotent, and retrying stops a database blip from dead-lettering a report
	regs = append(regs, builtinactor.BuiltInActorRegistration{
		ActorType: w.baseType,
		Factory: func(actorID string, svc *actor.Service) actor.Actor {
			w.boundService.CompareAndSwap(nil, svc)
			return newOrchestrator(w, actorID, svc)
		},
		RegisterOptions: actorcore.RegisterActorOptions{
			IdleTimeout:              orchestratorIdleTimeout,
			MaxAttempts:              orchestratorMaxAttempts,
			InitialRetryDelay:        orchestratorRetryDelay,
			CompletedJobRetention:    w.def.jobRetention(),
			DeadLetteredJobRetention: w.def.jobRetention(),
		},
	})

	// The worker and undo families each get a base queue every host serves, plus one queue per advertised capability
	queues := append([]string{""}, o.capabilities...)
	for _, capName := range queues {
		regs = append(regs,
			w.workerRegistration(workerTypeSuffix, capName, workerGroup, o.concurrency, false),
			w.workerRegistration(undoTypeSuffix, capName, undoGroup, o.compensateConcurrency, true),
		)
	}

	// The registry is the cluster-wide singleton that records each version's fingerprint and answers the once-per-host consistency check
	registryType := w.baseType + registryTypeSuffix
	regs = append(regs, builtinactor.BuiltInActorRegistration{
		ActorType: registryType,
		Factory: func(actorID string, svc *actor.Service) actor.Actor {
			w.boundService.CompareAndSwap(nil, svc)
			return newRegistryActor(registryType, actorID, svc)
		},
		RegisterOptions: actorcore.RegisterActorOptions{
			IdleTimeout: orchestratorIdleTimeout,
		},
	})

	// The auto-purge sweep is an ordinary cron job built-in, registered alongside the workflow's own types so one call registers everything
	if o.autoPurgeCron != "" {
		var err error
		w.purgeCron, err = cronjob.New(w.name+".purge",
			cronjob.WithCron(o.autoPurgeCron),
			cronjob.WithLogger(w.log),
			cronjob.WithJob(func(ctx context.Context) error {
				// The sweep runs against the service created for this workflow's actors
				svc := w.boundService.Load()
				if svc == nil {
					// Should not have happened
					return errors.New("the workflow is not bound to a host service yet")
				}

				_, pErr := w.Service(svc).PurgeTerminated(ctx)
				if pErr != nil {
					return fmt.Errorf("failed to purge terminated workflows: %w", pErr)
				}
				return nil
			}),
		)
		if err != nil {
			return fmt.Errorf("failed to create the auto-purge cron job: %w", err)
		}

		// The cron job's own factory records the service too, so the sweep is bound even on a host that never activates an orchestrator
		for _, reg := range builtinactor.RegistrationsFor(w.purgeCron) {
			regs = append(regs, w.bindService(reg))
		}
	}

	w.registrations = regs
	return nil
}

// bindService wraps a registration's factory so the workflow records the actor.Service the host bound it to
// The auto-purge sweep needs a service to run against, and a built-in actor only ever receives one through its factory
func (w *Workflow) bindService(reg builtinactor.BuiltInActorRegistration) builtinactor.BuiltInActorRegistration {
	inner := reg.Factory
	reg.Factory = func(actorID string, svc *actor.Service) actor.Actor {
		w.boundService.CompareAndSwap(nil, svc)
		return inner(actorID, svc)
	}
	return reg
}

// workerRegistration builds the registration of one worker queue, for either the forward or the undo family
func (w *Workflow) workerRegistration(suffix string, capName string, group string, limit int, undo bool) builtinactor.BuiltInActorRegistration {
	bareType := w.baseType + suffix
	if capName != "" {
		bareType += "." + capName
	}

	return builtinactor.BuiltInActorRegistration{
		ActorType: bareType,
		Factory: func(actorID string, svc *actor.Service) actor.Actor {
			w.boundService.CompareAndSwap(nil, svc)
			return newWorker(w, bareType, actorID, svc, undo)
		},
		RegisterOptions: actorcore.RegisterActorOptions{
			// A worker only returns an error to Francis when its report dispatch failed
			MaxAttempts:              workerMaxAttempts,
			ConcurrencyLimit:         limit,
			CapacityGroup:            group,
			CapacityGroupLimit:       limit,
			CompletedJobRetention:    w.def.jobRetention(),
			DeadLetteredJobRetention: w.def.jobRetention(),
		},
	}
}

// ActorType returns the reserved base actor type registered for this workflow, which is the orchestrator's
func (w *Workflow) ActorType() string {
	return w.baseType
}

// Factory returns the factory for the orchestrator
// The host registers every type through Registrations, so this is only the single-type fallback of the built-in contract
func (w *Workflow) Factory() actor.Factory {
	return w.registrations[0].Factory
}

// RegisterOptions returns the registration options of the orchestrator
func (w *Workflow) RegisterOptions() actorcore.RegisterActorOptions {
	return w.registrations[0].RegisterOptions
}

// Singleton reports that the workflow's own type is not a singleton: one orchestrator exists per instance, created on demand
// The registry and the auto-purge cron job are singletons, and say so in their own registrations
func (w *Workflow) Singleton() bool {
	return false
}

// Registrations returns every reserved actor type the workflow registers: the orchestrator, the worker and undo queues, the registry, and the auto-purge cron job when one is configured
func (w *Workflow) Registrations() []builtinactor.BuiltInActorRegistration {
	return w.registrations
}

// Name returns the workflow's name
func (w *Workflow) Name() string {
	return w.name
}

// Version returns the definition's version, which is stamped on every instance this workflow starts
func (w *Workflow) Version() int {
	return w.def.version
}

// workerType returns the bare actor type serving a step's forward tasks, which is the base queue unless the step requires a capability
func (w *Workflow) workerType(capability string) string {
	return queueType(w.baseType+workerTypeSuffix, capability)
}

// undoType returns the bare actor type serving a step's compensations, on the queue of the same capability the forward task ran on
func (w *Workflow) undoType(capability string) string {
	return queueType(w.baseType+undoTypeSuffix, capability)
}

// registryType returns the bare actor type of the definition registry singleton
func (w *Workflow) registryType() string {
	return w.baseType + registryTypeSuffix
}

// queueType appends a capability to a queue family's base type, or returns the base queue when there is no requirement
func queueType(base string, capability string) string {
	if capability == "" {
		return base
	}
	return base + "." + capability
}

// workerActorID is the deterministic ID of the actor running one task, so a re-run of reconcile addresses the same actor and its idempotency key applies
// A hash of the three components would avoid constraining names, at the cost of unreadable IDs in logs and traces, and readability wins since these are what an operator greps for
func workerActorID(instanceID string, step string, index int) string {
	return instanceID + idDelimiter + step + idDelimiter + strconv.Itoa(index)
}
