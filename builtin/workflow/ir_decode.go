package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	msgpack "github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

const (
	// canonicalDefinitionFields and canonicalStepFields are how many positional fields the current IR format version has in a definition and in a step
	canonicalDefinitionFields = 16
	canonicalStepFields       = 32
)

// decodeCanonicalIR parses the bytes produced by canonicalBytes back into the IR they encode
// It accepts only the current format version, and only bytes that re-encode to themselves, so a decoded IR always carries the identity its bytes hash to
func decodeCanonicalIR(data []byte) (*definitionIR, error) {
	r := &irFieldReader{dec: msgpack.NewDecoder(bytes.NewReader(data))}
	ir := r.definition()
	if r.err != nil {
		return nil, fmt.Errorf("failed to decode the workflow IR: %w", r.err)
	}

	// Only canonical bytes hash to a fingerprint some host would compute, so anything else is rejected rather than described under an identity nothing recognizes
	// This also rejects trailing bytes, and integers or arrays encoded with a different width than the encoder chooses
	canonical, err := ir.canonicalBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode the decoded workflow IR: %w", err)
	}
	if !bytes.Equal(canonical, data) {
		return nil, errors.New("the workflow IR is not in canonical form: re-encoding it does not reproduce the same bytes")
	}

	return ir, nil
}

// irFieldReader reads the positional fields of the canonical IR in the order canonicalDefinition and canonicalStep write them
// It latches the first error, so the decoder reads field by field exactly like the encoder it mirrors, and every read after a failure is a no-op
type irFieldReader struct {
	dec *msgpack.Decoder
	err error
}

// definition reads the workflow-wide array, checking the magic and the format version before the field count so a newer format is reported as such
func (r *irFieldReader) definition() *definitionIR {
	n := r.arrayLen()
	if r.err == nil && n < 2 {
		r.err = fmt.Errorf("expected the definition array, found an array of %d fields", n)
	}

	magic := r.str()
	if r.err == nil && magic != canonicalIRMagic {
		r.err = fmt.Errorf("not a workflow IR: the magic is %q rather than %q", magic, canonicalIRMagic)
	}

	ir := &definitionIR{formatVersion: int(r.i64())}
	if r.err == nil && ir.formatVersion != definitionIRVersion {
		r.err = fmt.Errorf("unsupported IR format version %d, while this build reads version %d", ir.formatVersion, definitionIRVersion)
	}
	if r.err == nil && n != canonicalDefinitionFields {
		r.err = fmt.Errorf("the definition has %d fields, while IR format version %d has %d", n, definitionIRVersion, canonicalDefinitionFields)
	}

	ir.name = r.str()
	ir.version = int(r.i64())
	ir.steps = r.steps()
	ir.timeout = time.Duration(r.i64())
	ir.retention.Completed = time.Duration(r.i64())
	ir.retention.Failed = time.Duration(r.i64())
	ir.retention.Cancelled = time.Duration(r.i64())
	ir.outputStep = r.str()
	ir.maxInputSize = int(r.i64())
	ir.maxOutputSize = int(r.i64())
	ir.maxJournalSize = int(r.i64())
	ir.maxDepth = int(r.i64())
	ir.unknownVersion = UnknownVersionPolicy(r.str())
	ir.compensationFailurePolicy = CompensationFailurePolicy(r.str())

	return ir
}

// steps reads an array of step arrays, naming the position of a step that fails to decode
func (r *irFieldReader) steps() []*stepDef {
	n := r.arrayLen()
	if r.err != nil || n < 0 {
		return nil
	}

	out := make([]*stepDef, n)
	for i := range out {
		out[i] = r.step()
		if r.err != nil {
			r.err = fmt.Errorf("step at index %d: %w", i, r.err)
			return nil
		}
	}
	return out
}

// step reads one step array, whose members recurse through steps
func (r *irFieldReader) step() *stepDef {
	n := r.arrayLen()
	if r.err == nil && n != canonicalStepFields {
		r.err = fmt.Errorf("the step has %d fields, while IR format version %d has %d", n, definitionIRVersion, canonicalStepFields)
	}

	s := &stepDef{}
	s.name = r.str()
	s.kind = Kind(r.str())
	s.hasRun = r.boolean()
	s.hasCompensate = r.boolean()
	s.members = r.steps()
	s.child = r.child()
	s.itemsFrom = r.str()
	s.inputFrom = r.strs()
	s.maxAttempts = int(r.i64())
	s.retryInitial = time.Duration(r.i64())
	s.retryMax = time.Duration(r.i64())
	s.compMaxAttempt = int(r.i64())
	s.compInitial = time.Duration(r.i64())
	s.compMax = time.Duration(r.i64())
	s.attemptTimeout = time.Duration(r.i64())
	s.eventTimeout = time.Duration(r.i64())
	s.eventName = r.str()
	s.optional = r.boolean()
	s.skipOnFailure = r.strs()
	s.skipIfStep = r.str()
	s.skipIfValue = r.boolean()
	s.hasSkipIf = r.boolean()
	s.failurePolicy = FailurePolicy(r.str())
	s.maxParallel = int(r.i64())
	s.compensateOnFailure = r.boolean()
	s.capability = r.str()
	s.body = r.strs()
	s.untilStep = r.str()
	s.untilValue = r.boolean()
	s.hasUntil = r.boolean()
	s.maxIterations = int(r.i64())
	s.compensateTimeout = time.Duration(r.i64())

	return s
}

// child reads a step's child reference, which is nil for a step that starts no child and a two-field array otherwise
func (r *irFieldReader) child() *childDefinitionIR {
	if r.err != nil {
		return nil
	}

	code, err := r.dec.PeekCode()
	if err != nil {
		r.err = err
		return nil
	}
	if code == msgpcode.Nil {
		r.err = r.dec.DecodeNil()
		return nil
	}

	n := r.arrayLen()
	if r.err == nil && n != 2 {
		r.err = fmt.Errorf("the child reference has %d fields rather than 2", n)
	}
	return &childDefinitionIR{
		name:    r.str(),
		version: int(r.i64()),
	}
}

// arrayLen reads an array header, returning -1 for a nil array
func (r *irFieldReader) arrayLen() int {
	if r.err != nil {
		return 0
	}

	var n int
	n, r.err = r.dec.DecodeArrayLen()
	return n
}

// strs reads an array of strings, keeping a nil array distinct from an empty one so the bytes re-encode exactly
func (r *irFieldReader) strs() []string {
	n := r.arrayLen()
	if r.err != nil || n < 0 {
		return nil
	}

	out := make([]string, n)
	for i := range out {
		out[i] = r.str()
	}
	return out
}

// str reads one string field
func (r *irFieldReader) str() string {
	if r.err != nil {
		return ""
	}

	var s string
	s, r.err = r.dec.DecodeString()
	return s
}

// i64 reads one integer field, which the encoder always writes as a 64-bit integer
func (r *irFieldReader) i64() int64 {
	if r.err != nil {
		return 0
	}

	var v int64
	v, r.err = r.dec.DecodeInt64()
	return v
}

// boolean reads one boolean field
func (r *irFieldReader) boolean() bool {
	if r.err != nil {
		return false
	}

	var v bool
	v, r.err = r.dec.DecodeBool()
	return v
}
