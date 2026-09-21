package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/italypaleale/go-kit/utils"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	canonicalIRMagic    = "francis.workflow.ir"
	definitionIRVersion = 2
)

// definitionData is the pure workflow-wide configuration shared by the Go frontend and the IR
type definitionData struct {
	version                   int
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

// stepData is the pure node data shared by the Go frontend and the IR
type stepData struct {
	name string
	kind Kind

	hasRun        bool
	hasCompensate bool
	itemsFrom     string
	inputFrom     []string

	maxAttempts    int
	retryInitial   time.Duration
	retryMax       time.Duration
	compMaxAttempt int
	compInitial    time.Duration
	compMax        time.Duration

	attemptTimeout    time.Duration
	compensateTimeout time.Duration
	eventTimeout      time.Duration
	eventName         string

	optional      bool
	skipOnFailure []string
	skipIfStep    string
	skipIfValue   bool
	hasSkipIf     bool

	failurePolicy       FailurePolicy
	maxParallel         int
	compensateOnFailure bool
	capability          string

	body          []string
	untilStep     string
	untilValue    bool
	hasUntil      bool
	maxIterations int
}

// childDefinitionIR is the stable identity a parent needs to address a child workflow
type childDefinitionIR struct {
	name    string
	version int
}

// stepDef is one language-neutral node in the canonical workflow IR
type stepDef struct {
	stepData

	members []*stepDef
	child   *childDefinitionIR
}

// definitionIR contains only canonical data and can be encoded, versioned, and hashed without Go handlers or runtime indexes
type definitionIR struct {
	definitionData

	formatVersion int
	name          string
	steps         []*stepDef
}

// stepBinding links a language-neutral step to the Go code used by this host
type stepBinding struct {
	run        RunFunc
	compensate CompensateFunc
	child      *Workflow
}

// definition is the validated IR plus the indexes and host-local bindings needed to execute it
type definition struct {
	*definitionIR

	fingerprint string
	byName      map[string]*stepDef
	order       map[string]int
	bindings    map[string]stepBinding
}

// lowerDefinition compiles the cloned Go declarations into canonical IR and separate executable bindings
func (o *options) lowerDefinition(name string, declarations []*stepDecl) (*definition, error) {
	data := o.definitionData
	data.retention = RetentionPolicy{
		Completed: o.retention.forStatus(StatusCompleted),
		Failed:    o.retention.forStatus(StatusFailed),
		Cancelled: o.retention.forStatus(StatusCancelled),
	}
	ir := &definitionIR{
		definitionData: data,
		formatVersion:  definitionIRVersion,
		name:           name,
		steps:          make([]*stepDef, len(declarations)),
	}
	bindings := make(map[string]stepBinding)

	// Lower each declaration independently so no mutable DSL node survives construction
	for i, declaration := range declarations {
		step, err := lowerStep(declaration, bindings)
		if err != nil {
			return nil, err
		}
		ir.steps[i] = step
	}

	return &definition{
		definitionIR: ir,
		byName:       map[string]*stepDef{},
		order:        map[string]int{},
		bindings:     bindings,
	}, nil
}

// lowerStep resolves semantic defaults while keeping executable values out of the IR
func lowerStep(declaration *stepDecl, bindings map[string]stepBinding) (*stepDef, error) {
	if declaration == nil {
		return nil, errors.New("step declaration is uninitialized")
	}

	data := declaration.stepData
	data.hasRun = declaration.run != nil
	data.hasCompensate = declaration.compensate != nil
	data.inputFrom = canonicalReferences(declaration.inputFrom)
	data.attemptTimeout = utils.PositiveOr(declaration.attemptTimeout, 0)
	data.compensateTimeout = utils.PositiveOr(declaration.compensateTimeout, 0)
	data.eventTimeout = utils.PositiveOr(declaration.eventTimeout, 0)
	data.skipOnFailure = canonicalReferences(declaration.skipOnFailure)
	data.maxParallel = max(declaration.maxParallel, 0)
	data.body = append([]string{}, declaration.body...)
	step := &stepDef{stepData: data}

	// Resolve task policies into the values the engine actually enforces
	if declaration.kind == KindStep || declaration.kind == KindForEach || declaration.kind == KindChild {
		step.maxAttempts = utils.PositiveOr(declaration.maxAttempts, defaultMaxAttempts)
		step.retryInitial = utils.PositiveOr(declaration.retryInitial, defaultRetryInitial)
		step.retryMax = utils.PositiveOr(declaration.retryMax, defaultRetryMax)
		step.compMaxAttempt = utils.PositiveOr(declaration.compMaxAttempt, defaultCompMaxAttempts)
		step.compInitial = utils.PositiveOr(declaration.compInitial, defaultCompInitial)
		step.compMax = utils.PositiveOr(declaration.compMax, defaultCompMax)
	}

	// Resolve kind-specific defaults so equivalent declarations produce identical IR
	if declaration.kind == KindParallel || declaration.kind == KindForEach {
		step.failurePolicy = declaration.failurePolicy
		if step.failurePolicy == "" {
			step.failurePolicy = FailFast
		}
	}
	if declaration.kind == KindWait {
		step.eventName = declaration.effectiveEventName()
	}
	if declaration.child != nil {
		step.child = &childDefinitionIR{}
		if declaration.child.def != nil {
			step.child.name = declaration.child.def.name
			step.child.version = declaration.child.def.version
		}
	}

	// Lower parallel members into nested IR while retaining bindings by their globally unique names
	step.members = make([]*stepDef, len(declaration.members))
	for i, memberDeclaration := range declaration.members {
		memberStep, err := lowerStep(memberDeclaration, bindings)
		if err != nil {
			return nil, err
		}
		step.members[i] = memberStep
	}

	bindings[declaration.name] = stepBinding{
		run:        declaration.run,
		compensate: declaration.compensate,
		child:      declaration.child,
	}

	return step, nil
}

// validateDeclaration checks Go frontend constraints after structural IR validation has produced the most useful graph error
func validateDeclaration(declaration *stepDecl, member bool) error {
	if declaration == nil {
		return errors.New("step declaration is uninitialized")
	}

	// Reject options the selected node kind cannot execute
	err := declaration.validateOptions(member)
	if err != nil {
		return err
	}

	// A binding must point to a fully constructed workflow before it can be linked to the IR identity
	if declaration.child != nil && (declaration.child.def == nil || declaration.child.baseType == "" || len(declaration.child.def.steps) == 0) {
		return fmt.Errorf("step %q references an uninitialized child workflow", declaration.name)
	}

	// Parallel members retain their own task bindings but cannot configure group-wide behavior
	for _, memberDeclaration := range declaration.members {
		err = validateDeclaration(memberDeclaration, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// canonicalReferences normalizes fields whose runtime meaning is an unordered set of step names
func canonicalReferences(values []string) []string {
	out := append([]string{}, values...)
	slices.Sort(out)
	return slices.Compact(out)
}

// isCompensable reports whether a completed task is represented on the compensation stack
func (d *stepDef) isCompensable() bool {
	if d.kind == KindChild || d.hasCompensate || (d.kind == KindForEach && d.child != nil) {
		return true
	}

	for _, member := range d.members {
		if member.isCompensable() {
			return true
		}
	}

	return false
}

// binding resolves the executable values linked to a canonical step
func (def *definition) binding(d *stepDef) stepBinding {
	if d == nil {
		return stepBinding{}
	}

	return def.bindings[d.name]
}

// setFingerprint hashes the canonical IR bytes and excludes all host-local executable bindings
func (def *definition) setFingerprint() error {
	canonical, err := def.canonicalBytes()
	if err != nil {
		return fmt.Errorf("failed to encode workflow IR: %w", err)
	}

	sum := sha256.Sum256(canonical)
	def.fingerprint = hex.EncodeToString(sum[:])
	return nil
}

// canonicalBytes returns the explicit MessagePack array schema used for version identity
func (ir *definitionIR) canonicalBytes() ([]byte, error) {
	return msgpack.Marshal(canonicalDefinition(ir))
}

// canonicalDefinition projects the workflow-wide IR into the positional schema defined by IR format version 1
func canonicalDefinition(ir *definitionIR) []any {
	return []any{
		canonicalIRMagic,
		int64(ir.formatVersion),
		ir.name,
		int64(ir.version),
		canonicalSteps(ir.steps),
		int64(ir.timeout),
		int64(ir.retention.Completed),
		int64(ir.retention.Failed),
		int64(ir.retention.Cancelled),
		ir.outputStep,
		int64(ir.maxInputSize),
		int64(ir.maxOutputSize),
		int64(ir.maxJournalSize),
		int64(ir.maxDepth),
		string(ir.unknownVersion),
		string(ir.compensationFailurePolicy),
	}
}

// canonicalSteps preserves semantic declaration order without introducing maps into the encoded form
func canonicalSteps(steps []*stepDef) []any {
	encoded := make([]any, len(steps))
	for i, step := range steps {
		encoded[i] = canonicalStep(step)
	}
	return encoded
}

// canonicalStep projects one IR node into the positional schema defined by IR format version 1
func canonicalStep(step *stepDef) []any {
	var child any
	if step.child != nil {
		child = []any{step.child.name, int64(step.child.version)}
	}
	return []any{
		step.name,
		string(step.kind),
		step.hasRun,
		step.hasCompensate,
		canonicalSteps(step.members),
		child,
		step.itemsFrom,
		step.inputFrom,
		int64(step.maxAttempts),
		int64(step.retryInitial),
		int64(step.retryMax),
		int64(step.compMaxAttempt),
		int64(step.compInitial),
		int64(step.compMax),
		int64(step.attemptTimeout),
		int64(step.eventTimeout),
		step.eventName,
		step.optional,
		step.skipOnFailure,
		step.skipIfStep,
		step.skipIfValue,
		step.hasSkipIf,
		string(step.failurePolicy),
		int64(step.maxParallel),
		step.compensateOnFailure,
		step.capability,
		step.body,
		step.untilStep,
		step.untilValue,
		step.hasUntil,
		int64(step.maxIterations),
		int64(step.compensateTimeout),
	}
}
