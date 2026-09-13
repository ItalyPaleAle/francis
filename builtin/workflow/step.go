package workflow

import (
	"context"
	"time"
)

// Kind discriminates the node types a workflow's graph is built from
type Kind string

const (
	// KindStep is a plain step: one task, running one handler
	KindStep Kind = "step"
	// KindParallel is a static group whose members run concurrently
	KindParallel Kind = "parallel"
	// KindForEach is a dynamic fan-out, sized at runtime from an upstream step's output
	KindForEach Kind = "foreach"
	// KindChild is a step whose task is a whole instance of another definition
	KindChild Kind = "child"
	// KindWait is a step that parks the instance until an event arrives or its timeout elapses
	KindWait Kind = "wait"
)

// FailurePolicy decides what a failing task costs a parallel group or a fan-out
type FailurePolicy string

const (
	// FailFast fails the step on the first failure, cancelling the tasks that have not started
	FailFast FailurePolicy = "fail-fast"
	// CollectFailures runs every task to completion and then fails the step if any of them failed
	CollectFailures FailurePolicy = "collect-failures"
	// TolerateFailures runs every task to completion and succeeds regardless, leaving the failures visible in the step's output
	TolerateFailures FailurePolicy = "tolerate-failures"
)

// CompensationFailurePolicy decides what a failing compensation costs the rest of the unwind
type CompensationFailurePolicy string

const (
	// ContinueUnwinding records the failure and keeps unwinding the remaining frames, which usually leaves less state stranded than stopping does
	ContinueUnwinding CompensationFailurePolicy = "continue"
	// AbortUnwinding stops at the failed frame, and the journal names exactly which frames were not unwound
	AbortUnwinding CompensationFailurePolicy = "abort"
)

// UnknownVersionPolicy decides what the deadline alarm does when it fires on a host that does not have the instance's version (§14.4)
type UnknownVersionPolicy string

const (
	// ParkUnknownVersion re-arms the alarm and waits for a host that can serve the version
	ParkUnknownVersion UnknownVersionPolicy = "park"
	// FailUnknownVersion terminates the instance once its timeout has elapsed, without compensation, since no host can run the compensations either
	FailUnknownVersion UnknownVersionPolicy = "fail"
)

// RunFunc performs one attempt of a task and returns the output recorded in the journal
// It runs on a WorkflowWorker, never on the Workflow actor
// Returning an error records a failed attempt, retried per the step's policy; returning actor.ErrJobPermanentFailure fails the task without further attempts; returning actor.ErrJobRejected declines it so another host runs it, without counting an attempt
type RunFunc func(ctx context.Context, t Task) (output any, err error)

// CompensateFunc undoes the effect of one task that had completed successfully
// It runs on the compensation worker type, and is retried per the step's compensation policy
type CompensateFunc func(ctx context.Context, c Compensation) error

// stepDef is one node of a definition's graph, built by Step, Parallel, ForEach, Child, or WaitForEvent
type stepDef struct {
	name string
	kind Kind

	// run and compensate are the only places user code appears, and both are invoked exclusively by a worker (§4.3)
	run        RunFunc
	compensate CompensateFunc

	// members are the steps of a parallel group
	members []*stepDef
	// child is the definition a child step or a child fan-out runs
	child *Workflow
	// itemsFrom names the step whose output a fan-out iterates
	itemsFrom string
	// inputFrom names the extra steps whose outputs this step's tasks receive
	inputFrom []string

	// maxAttempts and the backoff bound the engine-owned attempts of a forward task
	maxAttempts    int
	retryInitial   time.Duration
	retryMax       time.Duration
	compMaxAttempt int
	compInitial    time.Duration
	compMax        time.Duration

	// stepTimeout bounds one step, and eventTimeout bounds a WaitForEvent step's wait
	stepTimeout  time.Duration
	eventTimeout time.Duration
	eventName    string

	// optional makes this step's failure cost the instance nothing but a record
	optional bool
	// skipOnFailure names the steps that are pointless without this one
	skipOnFailure []string
	// skipIfStep and skipIfValue skip this step when the named step's output equals the value, which is how a condition stays a recorded output rather than a hidden predicate (§7.9)
	skipIfStep  string
	skipIfValue bool
	hasSkipIf   bool

	// failurePolicy applies to a group or a fan-out
	failurePolicy FailurePolicy
	// maxParallel bounds how many of a fan-out's tasks are in flight per instance
	maxParallel int
	// compensateOnFailure opts a step into being compensated even when it failed, for handlers whose effect may be partial
	compensateOnFailure bool
	// capability routes this step's tasks to the queue only hosts advertising it serve
	capability string
}

// StepSpec is one node of a workflow's graph, produced by Step, Parallel, ForEach, Child, or WaitForEvent and passed to WithSteps
type StepSpec struct {
	d *stepDef
}

// StepOption configures a step built by one of the step constructors
type StepOption func(*stepDef)

// Step declares a plain step: one task, running the handler set with WithRun
func Step(name string, opts ...StepOption) StepSpec {
	return newStepSpec(name, KindStep, opts)
}

// ForEach declares a dynamic fan-out, with one task per element of the output of the step named by WithItemsFrom
// Each task runs the handler set with WithRun, or starts one child instance of the definition set with WithChild
func ForEach(name string, opts ...StepOption) StepSpec {
	return newStepSpec(name, KindForEach, opts)
}

// Child declares a step whose task is a whole instance of the definition set with WithDefinition
// The child keeps its own journal, and only its result enters this one
func Child(name string, opts ...StepOption) StepSpec {
	return newStepSpec(name, KindChild, opts)
}

// WaitForEvent declares a step that parks the instance until RaiseEvent delivers its event or the timeout set with WithEventTimeout elapses
// The event name defaults to the step's name and can be set with WithEventName
func WaitForEvent(name string, opts ...StepOption) StepSpec {
	return newStepSpec(name, KindWait, opts)
}

// Parallel declares a static group whose members run at the same time
// The group completes when every member has reported, and members receive the same upstream outputs and cannot read each other's
func Parallel(name string, steps ...StepSpec) StepSpec {
	d := &stepDef{
		name:    name,
		kind:    KindParallel,
		members: make([]*stepDef, len(steps)),
	}
	for i, s := range steps {
		d.members[i] = s.d
	}
	return StepSpec{d: d}
}

// newStepSpec builds a step of the given kind and applies its options
func newStepSpec(name string, kind Kind, opts []StepOption) StepSpec {
	d := &stepDef{
		name: name,
		kind: kind,
	}
	for _, opt := range opts {
		opt(d)
	}
	return StepSpec{d: d}
}

// WithRun sets the function that performs one attempt of the step's task
// It runs on a worker, so it may call the clock, do I/O, use randomness, and start goroutines; the only contract is idempotency, because at-least-once delivery means it can run twice
func WithRun(fn RunFunc) StepOption {
	return func(d *stepDef) {
		d.run = fn
	}
}

// WithCompensate sets the function that undoes the effect of a task of this step that had completed successfully
// It runs on the undo worker type and receives the output the forward task produced, which is usually what identifies the effect to undo
func WithCompensate(fn CompensateFunc) StepOption {
	return func(d *stepDef) {
		d.compensate = fn
	}
}

// WithMaxAttempts sets how many attempts a task of this step gets before it is failed, defaulting to 3
// Attempts are owned by the engine, counted in the journal, and independent of any actor-type setting
func WithMaxAttempts(n int) StepOption {
	return func(d *stepDef) {
		d.maxAttempts = n
	}
}

// WithRetryBackoff sets the delay before the second attempt and the cap the doubling stops at, defaulting to 2s and 1 minute
func WithRetryBackoff(initial time.Duration, max time.Duration) StepOption {
	return func(d *stepDef) {
		d.retryInitial = initial
		d.retryMax = max
	}
}

// WithCompensateMaxAttempts sets how many attempts this step's compensation gets before it is failed, defaulting to 10
// The default is higher than the forward policy because a failed rollback leaves the system inconsistent, so it is worth trying harder
func WithCompensateMaxAttempts(n int) StepOption {
	return func(d *stepDef) {
		d.compMaxAttempt = n
	}
}

// WithCompensateBackoff sets the delay before the second compensation attempt and the cap the doubling stops at, defaulting to 10s and 10 minutes
func WithCompensateBackoff(initial time.Duration, max time.Duration) StepOption {
	return func(d *stepDef) {
		d.compInitial = initial
		d.compMax = max
	}
}

// WithStepTimeout bounds how long this step may take, after which its outstanding attempts are failed
func WithStepTimeout(d time.Duration) StepOption {
	return func(s *stepDef) {
		s.stepTimeout = d
	}
}

// WithEventTimeout bounds how long a WaitForEvent step waits, after which the instance unwinds with an event timeout as its cause
func WithEventTimeout(d time.Duration) StepOption {
	return func(s *stepDef) {
		s.eventTimeout = d
	}
}

// WithEventName sets the event name a WaitForEvent step listens for, which defaults to the step's own name
func WithEventName(name string) StepOption {
	return func(d *stepDef) {
		d.eventName = name
	}
}

// WithOptional makes this step's failure cost the instance nothing: the failure is recorded and the workflow still completes
// It is the notification case, where the work the caller asked for was done and only a notification was lost
func WithOptional() StepOption {
	return func(d *stepDef) {
		d.optional = true
	}
}

// WithSkipOnFailure names the steps that are pointless without this one, which are recorded as skipped and never run when it fails
// It does not trigger an unwind, and skipping is not transitive
func WithSkipOnFailure(steps ...string) StepOption {
	return func(d *stepDef) {
		d.skipOnFailure = append(d.skipOnFailure, steps...)
	}
}

// WithSkipIf skips this step when the named upstream step's output equals value
// A condition is a step that returns a boolean rather than a predicate the orchestrator evaluates, so the decision is a recorded output rather than a hidden evaluation (§7.9)
func WithSkipIf(step string, value bool) StepOption {
	return func(d *stepDef) {
		d.skipIfStep = step
		d.skipIfValue = value
		d.hasSkipIf = true
	}
}

// WithInputFrom names extra upstream steps whose outputs this step's tasks receive, on top of the workflow input and the preceding step's output
// The engine never ships the whole journal to a worker, so a step's data dependencies are explicit and auditable from the definition alone
func WithInputFrom(steps ...string) StepOption {
	return func(d *stepDef) {
		d.inputFrom = append(d.inputFrom, steps...)
	}
}

// WithItemsFrom names the step whose output a fan-out iterates, which must decode to a JSON array
// The list is journaled when the upstream step reports, so a retried turn re-reads the recorded items rather than re-deriving them
func WithItemsFrom(step string) StepOption {
	return func(d *stepDef) {
		d.itemsFrom = step
	}
}

// WithChild makes each task of a fan-out start one child instance of the given definition, one per item
func WithChild(wf *Workflow) StepOption {
	return func(d *stepDef) {
		d.child = wf
	}
}

// WithDefinition sets the definition a child step runs
func WithDefinition(wf *Workflow) StepOption {
	return func(d *stepDef) {
		d.child = wf
	}
}

// WithMaxParallel bounds how many of a fan-out's tasks are in flight for one instance, as a sliding window over the tasks in index order
// It is separate from the per-host bound set with WithConcurrency, which limits how much work a host accepts across all instances
func WithMaxParallel(n int) StepOption {
	return func(d *stepDef) {
		d.maxParallel = n
	}
}

// WithFailurePolicy chooses what a failing task costs a parallel group or a fan-out, defaulting to FailFast
func WithFailurePolicy(p FailurePolicy) StepOption {
	return func(d *stepDef) {
		d.failurePolicy = p
	}
}

// WithCompensateOnFailure opts this step into being compensated even when it failed
// By default a step that failed is not compensated, on the saga convention that a step which did not complete did not take effect, so a handler whose effect may be partial needs this and a compensation written defensively
func WithCompensateOnFailure() StepOption {
	return func(d *stepDef) {
		d.compensateOnFailure = true
	}
}

// WithRequiredCapability routes this step's tasks to the queue only hosts advertising the capability serve
// The step's compensation is routed to the undo queue of the same capability, since the undo almost always needs the placement the forward task had
func WithRequiredCapability(capability string) StepOption {
	return func(d *stepDef) {
		d.capability = capability
	}
}

// effectiveEventName returns the event name a WaitForEvent step listens for, which defaults to the step's own name
func (d *stepDef) effectiveEventName() string {
	if d.eventName != "" {
		return d.eventName
	}
	return d.name
}

// isCompensable reports whether a completed task of this step is pushed onto the compensation stack
// A child step always is, because compensating it means asking the child to undo itself, which needs no handler here
func (d *stepDef) isCompensable() bool {
	if d.kind == KindChild {
		return true
	}
	if d.compensate != nil {
		return true
	}
	if d.kind == KindForEach && d.child != nil {
		return true
	}

	// A group is compensable when any of its members is, since the group is one frame holding its members' undos
	for _, m := range d.members {
		if m.isCompensable() {
			return true
		}
	}
	return false
}
