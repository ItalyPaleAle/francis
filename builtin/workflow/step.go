package workflow

import (
	"context"
	"fmt"
	"slices"
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
	// KindLoop is a control node that repeats the steps of its body until a condition holds
	KindLoop Kind = "loop"
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

// UnknownVersionPolicy decides what the deadline alarm does when it fires on a host that does not have the instance's version
type UnknownVersionPolicy string

const (
	// ParkUnknownVersion re-arms the alarm and waits for a host that can serve the version
	ParkUnknownVersion UnknownVersionPolicy = "park"
	// FailUnknownVersion terminates the instance once its timeout has elapsed, without compensation, since no host can run the compensations either
	FailUnknownVersion UnknownVersionPolicy = "fail"
)

// RunFunc performs one attempt of a task and returns the output recorded in the journal
// It runs on a WorkflowWorker, never on the Workflow actor
// Returning an error records a failed attempt, retried per the step's policy
// Returning actor.ErrJobPermanentFailure fails the task without further attempts
// Returning actor.ErrJobRejected declines it so another host runs it, without counting an attempt
type RunFunc func(ctx context.Context, t Task) (output any, err error)

// CompensateFunc undoes the effect of one task that had completed successfully
// It runs on the compensation worker type, and is retried per the step's compensation policy
type CompensateFunc func(ctx context.Context, c Compensation) error

// stepDecl is one node of the Go DSL before it is lowered into the language-neutral workflow IR
type stepDecl struct {
	stepData

	// run and compensate are the only places user code appears, and both are invoked exclusively by a worker
	run        RunFunc
	compensate CompensateFunc

	// members are the steps of a parallel group
	members []*stepDecl
	// child is the definition a child step or a child fan-out runs
	child *Workflow
}

// StepSpec is one node of a workflow's graph, produced by Step, Parallel, ForEach, Child, or WaitForEvent and passed to WithSteps
type StepSpec struct {
	d *stepDecl
}

// With applies step options to a spec that was built without them
// It is how a parallel group declares its own options, since its members take the slot the other constructors give to theirs:
//
//	Parallel("notify",
//		Step("email", WithRun(email)),
//		Step("sms", WithRun(sms)),
//	).With(WithFailurePolicy(TolerateFailures))
func (s StepSpec) With(opts ...StepOption) StepSpec {
	s.d = s.d.clone()
	if s.d == nil {
		return s
	}
	for _, opt := range opts {
		opt(s.d)
	}
	return s
}

// validateOptions keeps option applicability in one place so an accepted declaration cannot silently discard configured behavior
func (d *stepDecl) validateOptions(member bool) error {
	// Group members are single tasks, so conditions and policies that govern a whole step must be declared on the group
	workerKinds := []Kind{KindStep, KindForEach}
	taskKinds := []Kind{KindStep, KindForEach, KindChild}
	stepKinds := []Kind{KindStep, KindForEach, KindChild, KindParallel, KindLoop}
	allKinds := []Kind{KindStep, KindForEach, KindChild, KindParallel, KindWait, KindLoop}
	rules := []struct {
		name       string
		configured bool
		kinds      []Kind
		member     bool
	}{
		{"WithRun", d.run != nil, workerKinds, true},
		{"WithCompensate", d.compensate != nil, workerKinds, true},
		{"WithMaxAttempts", d.maxAttempts != 0, taskKinds, true},
		{"WithRetryBackoff", d.retryInitial != 0 || d.retryMax != 0, taskKinds, true},
		{"WithCompensateMaxAttempts", d.compMaxAttempt != 0, taskKinds, true},
		{"WithCompensateBackoff", d.compInitial != 0 || d.compMax != 0, taskKinds, true},
		{"WithStepTimeout", d.stepTimeout != 0, []Kind{KindStep, KindForEach, KindChild, KindParallel}, false},
		{"WithEventTimeout", d.eventTimeout != 0, []Kind{KindWait}, false},
		{"WithEventName", d.eventName != "", []Kind{KindWait}, false},
		{"WithOptional", d.optional, stepKinds, false},
		{"WithSkipOnFailure", len(d.skipOnFailure) != 0, stepKinds, false},
		{"WithSkipIf", d.hasSkipIf, allKinds, false},
		{"WithInputFrom", len(d.inputFrom) != 0, []Kind{KindStep, KindForEach, KindParallel}, true},
		{"WithItemsFrom", d.itemsFrom != "", []Kind{KindForEach}, false},
		{"WithDefinition or WithChild", d.child != nil, []Kind{KindChild, KindForEach}, true},
		{"WithFailurePolicy", d.failurePolicy != "", []Kind{KindParallel, KindForEach}, false},
		{"WithMaxParallel", d.maxParallel != 0, []Kind{KindForEach}, false},
		{"WithCompensateOnFailure", d.compensateOnFailure, taskKinds, true},
		{"WithRequiredCapability", d.capability != "", workerKinds, true},
		{"WithUntil", d.hasUntil, []Kind{KindLoop}, false},
		{"WithMaxIterations", d.maxIterations != 0, []Kind{KindLoop}, false},
	}
	for _, rule := range rules {
		if !rule.configured {
			continue
		}
		if !slices.Contains(rule.kinds, d.kind) {
			return fmt.Errorf("step %q cannot use %s on a %s node", d.name, rule.name, d.kind)
		}
		if member && !rule.member {
			return fmt.Errorf("parallel member %q cannot use %s; declare it on the group", d.name, rule.name)
		}
	}

	// A child fan-out never runs a parent worker, so worker-only settings cannot affect its tasks
	if d.kind == KindForEach && d.child != nil && (d.compensate != nil || len(d.inputFrom) != 0 || d.capability != "") {
		return fmt.Errorf("child fan-out %q cannot use WithCompensate, WithInputFrom, or WithRequiredCapability", d.name)
	}
	// Failure policies are closed sets even though the exported string types can be populated from configuration
	if d.failurePolicy != "" && d.failurePolicy != FailFast && d.failurePolicy != CollectFailures && d.failurePolicy != TolerateFailures {
		return fmt.Errorf("step %q has unknown failure policy %q", d.name, d.failurePolicy)
	}
	return nil
}

// clone copies a declaration recursively so validated definitions and reused specifications never share mutable graph nodes
func (d *stepDecl) clone() *stepDecl {
	if d == nil {
		return nil
	}
	out := *d
	out.inputFrom = append([]string(nil), d.inputFrom...)
	out.skipOnFailure = append([]string(nil), d.skipOnFailure...)
	out.body = append([]string(nil), d.body...)
	out.members = make([]*stepDecl, len(d.members))
	for i := range d.members {
		out.members[i] = d.members[i].clone()
	}
	return &out
}

// StepOption configures a step built by one of the step constructors
// New rejects options that the node's execution path does not support instead of silently ignoring them
type StepOption func(*stepDecl)

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
// The group completes when every member has reported, and no member can read another's output, since WithInputFrom only ever names a top-level step that ran before the group
// Options that apply to the group as a whole, such as WithFailurePolicy, are set with the returned spec's With method, since the members take the variadic slot
// Each member carries its own attempt, backoff, compensation, and WithInputFrom options, which the engine applies to that member's task alone
// Conditions, optionality, skip-on-failure rules, and step timeouts belong on the group and are rejected on individual members
func Parallel(name string, steps ...StepSpec) StepSpec {
	d := &stepDecl{
		name:    name,
		kind:    KindParallel,
		members: make([]*stepDecl, len(steps)),
	}
	for i, s := range steps {
		d.members[i] = s.d
	}
	return StepSpec{d: d}
}

// Loop declares a control node that repeats the steps of its body until a condition holds
// The body runs in order, one task at a time, and every step of it is an ordinary step of the workflow: a later step can read what the last iteration produced, and the graph shows the body rather than hiding it inside the loop
// The condition and the iteration bound are set with the returned spec's With method, since the body takes the variadic slot
//
//	Loop("poll",
//		Step("check", WithRun(checkReady)),
//		Step("pause", WithRun(pause)),
//	).With(WithUntil("check", true), WithMaxIterations(20))
//
// A body holds plain, child, and wait steps, because a loop runs one task at a time
// A parallel group or a fan-out inside a loop belongs in a child workflow the body starts
func Loop(name string, steps ...StepSpec) StepSpec {
	d := &stepDecl{
		name:    name,
		kind:    KindLoop,
		members: make([]*stepDecl, len(steps)),
	}
	for i, s := range steps {
		d.members[i] = s.d
	}
	return StepSpec{d: d}
}

// newStepSpec builds a step of the given kind and applies its options
func newStepSpec(name string, kind Kind, opts []StepOption) StepSpec {
	d := &stepDecl{
		name: name,
		kind: kind,
	}
	for _, opt := range opts {
		opt(d)
	}
	return StepSpec{d: d}
}

// WithRun sets the function that performs one attempt of the step's task
// It runs on a worker, so it may call the clock, do I/O, use randomness, and start goroutines
// The only requirement is idempotency, because at-least-once delivery means it could be invoked twice
func WithRun(fn RunFunc) StepOption {
	return func(d *stepDecl) {
		d.run = fn
	}
}

// WithCompensate sets the function that undoes the effect of a task of this step that had completed successfully
// It runs on the undo worker type and receives the output the forward task produced, which is usually what identifies the effect to undo
func WithCompensate(fn CompensateFunc) StepOption {
	return func(d *stepDecl) {
		d.compensate = fn
	}
}

// WithMaxAttempts sets how many attempts a task of this step gets before it is failed, defaulting to 3
// Attempts are owned by the engine, counted in the journal, and independent of any actor-type setting
func WithMaxAttempts(n int) StepOption {
	return func(d *stepDecl) {
		d.maxAttempts = n
	}
}

// WithRetryBackoff sets the delay before the second attempt and the cap the doubling stops at, defaulting to 2s and 1 minute
func WithRetryBackoff(initial time.Duration, max time.Duration) StepOption {
	return func(d *stepDecl) {
		d.retryInitial = initial
		d.retryMax = max
	}
}

// WithCompensateMaxAttempts sets how many attempts this step's compensation gets before it is failed, defaulting to 10
// The default is higher than the forward policy because a failed rollback leaves the system inconsistent, so it is worth trying harder
func WithCompensateMaxAttempts(n int) StepOption {
	return func(d *stepDecl) {
		d.compMaxAttempt = n
	}
}

// WithCompensateBackoff sets the delay before the second compensation attempt and the cap the doubling stops at, defaulting to 10s and 10 minutes
func WithCompensateBackoff(initial time.Duration, max time.Duration) StepOption {
	return func(d *stepDecl) {
		d.compInitial = initial
		d.compMax = max
	}
}

// WithStepTimeout bounds how long this step may take, after which its outstanding attempts are failed
func WithStepTimeout(d time.Duration) StepOption {
	return func(s *stepDecl) {
		s.stepTimeout = d
	}
}

// WithEventTimeout bounds how long a WaitForEvent step waits, after which the instance unwinds with an event timeout as its cause
func WithEventTimeout(d time.Duration) StepOption {
	return func(s *stepDecl) {
		s.eventTimeout = d
	}
}

// WithEventName sets the event name a WaitForEvent step listens for, which defaults to the step's own name
func WithEventName(name string) StepOption {
	return func(d *stepDecl) {
		d.eventName = name
	}
}

// WithOptional makes this step's failure cost the instance nothing: the failure is recorded and the workflow still completes
// It is the notification case, where the work the caller asked for was done and only a notification was lost
func WithOptional() StepOption {
	return func(d *stepDecl) {
		d.optional = true
	}
}

// WithSkipOnFailure names the steps that are pointless without this one, which are recorded as skipped and never run when it fails
// It does not trigger an unwind, and skipping is not transitive
func WithSkipOnFailure(steps ...string) StepOption {
	return func(d *stepDecl) {
		d.skipOnFailure = append(d.skipOnFailure, steps...)
	}
}

// WithSkipIf skips this step when the named upstream step's output equals value
// A condition is a step that returns a boolean rather than a predicate the orchestrator evaluates, so the decision is a recorded output rather than a hidden evaluation
func WithSkipIf(step string, value bool) StepOption {
	return func(d *stepDecl) {
		d.skipIfStep = step
		d.skipIfValue = value
		d.hasSkipIf = true
	}
}

// WithUntil ends a loop once the named step of its body has output the given value, and is required on a loop
// The condition is a step that returns a boolean rather than a predicate the orchestrator evaluates, exactly as WithSkipIf is, so what ended the loop is a recorded output the journal can show
// It is read after every iteration, so a loop always runs its body at least once
func WithUntil(step string, value bool) StepOption {
	return func(d *stepDecl) {
		d.untilStep = step
		d.untilValue = value
		d.hasUntil = true
	}
}

// WithMaxIterations bounds how many times a loop repeats its body, defaulting to 100
// A loop whose condition has not held after the last iteration fails, which is what keeps a condition that never becomes true from running the instance to its timeout
func WithMaxIterations(n int) StepOption {
	return func(d *stepDecl) {
		d.maxIterations = n
	}
}

// WithInputFrom names extra upstream steps whose outputs this step's tasks receive, on top of the workflow input and the preceding step's output
// The engine never ships the whole journal to a worker, so a step's data dependencies are explicit and auditable from the definition alone
func WithInputFrom(steps ...string) StepOption {
	return func(d *stepDecl) {
		d.inputFrom = append(d.inputFrom, steps...)
	}
}

// WithItemsFrom names the step whose output a fan-out iterates, which must decode to a JSON array
// The list is journaled when the upstream step reports, so a retried turn re-reads the recorded items rather than re-deriving them
func WithItemsFrom(step string) StepOption {
	return func(d *stepDecl) {
		d.itemsFrom = step
	}
}

// WithChild makes each task of a fan-out start one child instance of the given definition, one per item
func WithChild(wf *Workflow) StepOption {
	return func(d *stepDecl) {
		d.child = wf
	}
}

// WithDefinition sets the definition a child step runs
func WithDefinition(wf *Workflow) StepOption {
	return func(d *stepDecl) {
		d.child = wf
	}
}

// WithMaxParallel bounds how many of a fan-out's tasks are in flight for one instance, as a sliding window over the tasks in index order
// It is separate from the per-host bound set with WithConcurrency, which limits how much work a host accepts across all instances
func WithMaxParallel(n int) StepOption {
	return func(d *stepDecl) {
		d.maxParallel = n
	}
}

// WithFailurePolicy chooses what a failing task costs a parallel group or a fan-out, defaulting to FailFast
func WithFailurePolicy(p FailurePolicy) StepOption {
	return func(d *stepDecl) {
		d.failurePolicy = p
	}
}

// WithCompensateOnFailure opts this step into being compensated even when it failed
// By default a step that failed is not compensated, on the saga convention that a step which did not complete did not take effect, so a handler whose effect may be partial needs this and a compensation written defensively
func WithCompensateOnFailure() StepOption {
	return func(d *stepDecl) {
		d.compensateOnFailure = true
	}
}

// WithRequiredCapability routes this step's tasks to the queue only hosts advertising the capability serve
// The step's compensation is routed to the undo queue of the same capability, since the undo almost always needs the placement the forward task had
func WithRequiredCapability(capability string) StepOption {
	return func(d *stepDecl) {
		d.capability = capability
	}
}

// effectiveEventName returns the event name a WaitForEvent step listens for, which defaults to the step's own name
func (d *stepDecl) effectiveEventName() string {
	if d.eventName != "" {
		return d.eventName
	}
	return d.name
}
