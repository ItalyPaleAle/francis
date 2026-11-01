package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// IRFormat selects the human-readable form RenderIR produces
type IRFormat string

const (
	// IRFormatMarkdown renders a document with the definition's identity, a Mermaid diagram of its graph, its workflow-wide settings, and every step's resolved settings
	IRFormatMarkdown IRFormat = "markdown"
	// IRFormatMermaid renders only the Mermaid flowchart of the graph
	IRFormatMermaid IRFormat = "mermaid"
)

const (
	// mermaidStartID and mermaidEndID name the entry and exit nodes of the flowchart, avoiding "end", which Mermaid reserves
	mermaidStartID = "wfStart"
	mermaidEndID   = "wfEnd"
)

// CanonicalIR returns the canonical encoding of the workflow's definition IR
// It is the exact byte sequence whose SHA-256 is the definition fingerprint the registry records for this version, so it holds everything that governs how an instance runs and nothing host-local: no handlers, concurrency limits, advertised capabilities, logger, meter, or auto-purge schedule
// The encoding is versioned and internal to the engine, so read it with RenderIR rather than decoding it directly
func (w *Workflow) CanonicalIR() ([]byte, error) {
	return w.def.canonicalBytes()
}

// RenderIR decodes a definition's canonical IR, as returned by CanonicalIR, and renders it in a human-readable form
// It needs neither the Go handlers nor a registered workflow, so it can describe a definition captured from any host, and its output is deterministic, so two renderings can be diffed to see why two fingerprints differ
func RenderIR(data []byte, format IRFormat) (string, error) {
	if format != IRFormatMarkdown && format != IRFormatMermaid {
		return "", fmt.Errorf("unknown IR format %q", format)
	}

	ir, err := decodeCanonicalIR(data)
	if err != nil {
		return "", err
	}

	// The decoder confirmed the bytes are canonical, so their hash is the fingerprint a host computes for this definition
	sum := sha256.Sum256(data)
	r := newIRRenderer(ir, hex.EncodeToString(sum[:]))
	if format == IRFormatMermaid {
		return r.mermaid(), nil
	}
	return r.markdown(), nil
}

// irRenderer holds a decoded IR together with the indexes both renderings need
// It indexes by name rather than reusing the definition's indexes because a decoded IR has not been through New's validation, so it must render whatever the bytes say
type irRenderer struct {
	ir          *definitionIR
	fingerprint string
	// byName resolves the step references in the IR, keeping the first step of a name
	byName map[string]*stepDef
	// loopOf maps each loop body step to the first loop that names it
	loopOf map[string]*stepDef
	// ids holds the Mermaid node ID of every step and member, derived from its position so the output is stable
	ids map[*stepDef]string
}

// newIRRenderer indexes a decoded IR for rendering
func newIRRenderer(ir *definitionIR, fingerprint string) *irRenderer {
	r := &irRenderer{
		ir:          ir,
		fingerprint: fingerprint,
		byName:      map[string]*stepDef{},
		loopOf:      map[string]*stepDef{},
		ids:         map[*stepDef]string{},
	}
	for i, s := range ir.steps {
		r.index(s, "n"+strconv.Itoa(i))
		if s.kind != KindLoop {
			continue
		}
		for _, name := range s.body {
			_, taken := r.loopOf[name]
			if !taken {
				r.loopOf[name] = s
			}
		}
	}
	return r
}

// index records a step and its members under positional IDs
func (r *irRenderer) index(s *stepDef, id string) {
	r.ids[s] = id
	_, taken := r.byName[s.name]
	if !taken {
		r.byName[s.name] = s
	}
	for j, m := range s.members {
		r.index(m, id+"_"+strconv.Itoa(j))
	}
}

// markdown renders the full document
func (r *irRenderer) markdown() string {
	var b strings.Builder

	// The identity says which definition this is, which is what the registry compares across hosts
	fmt.Fprintf(&b, "# Workflow %s, version %d\n\n", mdCode(r.ir.name), r.ir.version)
	b.WriteString("This is the definition as the engine sees it: the canonical IR whose SHA-256 is the fingerprint the registry records for this version. Handlers, concurrency limits, the capabilities a host advertises, and `WithoutEventHistory` are not part of it.\n\n")
	b.WriteString("| Property | Value |\n| --- | --- |\n")
	writeMDRow(&b, "Name", mdCode(r.ir.name))
	writeMDRow(&b, "Version", strconv.Itoa(r.ir.version))
	writeMDRow(&b, "IR format version", strconv.Itoa(r.ir.formatVersion))
	writeMDRow(&b, "Fingerprint", mdCode(r.fingerprint))
	writeMDRow(&b, "Top-level steps", strconv.Itoa(len(r.ir.steps)))

	// The diagram comes before the tables, since the shape of the graph is what a reader looks for first
	b.WriteString("\n## Graph\n\n```mermaid\n")
	b.WriteString(r.mermaid())
	b.WriteString("```\n")

	// Workflow-wide settings, every one of which is part of the fingerprint
	b.WriteString("\n## Workflow settings\n\n| Setting | Value |\n| --- | --- |\n")
	writeMDRow(&b, "Instance timeout", durationOr(r.ir.timeout, "none"))
	writeMDRow(&b, "Retention of completed instances", durationOr(r.ir.retention.Completed, "none"))
	writeMDRow(&b, "Retention of failed instances", durationOr(r.ir.retention.Failed, "none"))
	writeMDRow(&b, "Retention of cancelled instances", durationOr(r.ir.retention.Cancelled, "none"))
	output := "the last completed step that produced an output"
	if r.ir.outputStep != "" {
		output = "the output of " + mdCode(r.ir.outputStep)
	}
	writeMDRow(&b, "Instance output", output)
	writeMDRow(&b, "Max input size", formatBytes(r.ir.maxInputSize))
	writeMDRow(&b, "Max task output size", formatBytes(r.ir.maxOutputSize))
	writeMDRow(&b, "Max journal size", formatBytes(r.ir.maxJournalSize))
	writeMDRow(&b, "Max child depth", strconv.Itoa(r.ir.maxDepth))
	writeMDRow(&b, "Unknown version policy", mdCode(string(r.ir.unknownVersion)))
	writeMDRow(&b, "Compensation failure policy", mdCode(string(r.ir.compensationFailurePolicy)))

	// Every top-level step in the order the engine walks them, with a group's members nested beneath it
	b.WriteString("\n## Steps\n")
	if len(r.ir.steps) == 0 {
		b.WriteString("\nThe definition has no steps.\n")
	}
	for i, s := range r.ir.steps {
		r.markdownStep(&b, s, strconv.Itoa(i+1), "###", nil)
	}

	return b.String()
}

// markdownStep renders one step's section, followed by the sections of its members
func (r *irRenderer) markdownStep(b *strings.Builder, s *stepDef, number string, heading string, group *stepDef) {
	title := fmt.Sprintf("\n%s %s. %s (%s", heading, number, mdCode(s.name), s.kind)
	if group != nil {
		title += ", member of " + mdCode(group.name)
	}
	b.WriteString(title + ")\n\n| Setting | Value |\n| --- | --- |\n")

	for _, row := range r.stepRows(s, group != nil) {
		if row.applies || !row.zero {
			writeMDRow(b, row.label, row.value)
		}
	}

	for j, m := range s.members {
		r.markdownStep(b, m, number+"."+strconv.Itoa(j+1), "####", s)
	}
}

// irRow is one setting of a step's table
type irRow struct {
	label string
	value string
	// applies is true when the setting governs the step's kind, so the row is printed even when it holds the zero value
	// A row that does not apply is printed only when it holds something, so a rendering never hides what the bytes say
	applies bool
	zero    bool
}

// stepRows lists every setting of a step, marking which ones govern its kind using the same rules New enforces on the Go declarations
func (r *irRenderer) stepRows(s *stepDef, member bool) []irRow {
	taskKind := kindIn(s.kind, KindStep, KindForEach, KindChild)
	workerKind := s.kind == KindStep || (s.kind == KindForEach && s.child == nil)
	stepKind := kindIn(s.kind, KindStep, KindForEach, KindChild, KindParallel, KindLoop)

	// A group's member receives whatever its group receives, on top of what it names itself
	inputBase := "the preceding step"
	if member {
		inputBase = "what its group receives"
	}
	inputFrom := "only " + inputBase
	if len(s.inputFrom) > 0 {
		inputFrom = inputBase + ", plus " + mdCodeList(s.inputFrom)
	}

	var rows []irRow

	// A loop body step is an ordinary top-level step, so the loop that repeats it is derived rather than read from the step itself
	loop := r.loopOf[s.name]
	if loop != nil && !member {
		pos := slices.Index(loop.body, s.name) + 1
		rows = append(rows, irRow{label: "Repeated by", value: fmt.Sprintf("loop %s, as step %d of %d of its body", mdCode(loop.name), pos, len(loop.body)), applies: true})
	}

	rows = append(rows,
		irRow{label: "Handler", value: yesNo(s.hasRun), applies: kindIn(s.kind, KindStep, KindForEach), zero: !s.hasRun},
		irRow{label: "Compensation handler", value: yesNo(s.hasCompensate), applies: workerKind, zero: !s.hasCompensate},
		irRow{label: "Child workflow", value: childRefText(s.child), applies: s.kind == KindChild, zero: s.child == nil},
		irRow{label: "Items from", value: mdCodeOr(s.itemsFrom, "none"), applies: s.kind == KindForEach, zero: s.itemsFrom == ""},
		irRow{label: "Input from", value: inputFrom, applies: kindIn(s.kind, KindStep, KindForEach, KindParallel), zero: len(s.inputFrom) == 0},
		irRow{label: "Attempts", value: strconv.Itoa(s.maxAttempts), applies: taskKind, zero: s.maxAttempts == 0},
		irRow{label: "Retry backoff", value: backoffText(s.retryInitial, s.retryMax), applies: taskKind, zero: s.retryInitial == 0 && s.retryMax == 0},
		irRow{label: "Attempt timeout", value: durationOr(s.attemptTimeout, "none"), applies: workerKind, zero: s.attemptTimeout == 0},
		irRow{label: "Compensation attempts", value: strconv.Itoa(s.compMaxAttempt), applies: taskKind, zero: s.compMaxAttempt == 0},
		irRow{label: "Compensation backoff", value: backoffText(s.compInitial, s.compMax), applies: taskKind, zero: s.compInitial == 0 && s.compMax == 0},
		irRow{label: "Compensation timeout", value: durationOr(s.compensateTimeout, "none"), applies: workerKind, zero: s.compensateTimeout == 0},
		irRow{label: "Compensate on failure", value: yesNo(s.compensateOnFailure), applies: taskKind, zero: !s.compensateOnFailure},
		irRow{label: "Required capability", value: mdCodeOr(s.capability, "none, so it runs on the base queue"), applies: workerKind, zero: s.capability == ""},
		irRow{label: "Event name", value: mdCodeOr(s.eventName, "none"), applies: s.kind == KindWait, zero: s.eventName == ""},
		irRow{label: "Event timeout", value: durationOr(s.eventTimeout, "none, so only the instance timeout bounds the wait"), applies: s.kind == KindWait, zero: s.eventTimeout == 0},
		irRow{label: "Failure policy", value: mdCodeOr(string(s.failurePolicy), "none"), applies: kindIn(s.kind, KindParallel, KindForEach), zero: s.failurePolicy == ""},
		irRow{label: "Max tasks in flight", value: countOr(s.maxParallel, "unbounded"), applies: s.kind == KindForEach, zero: s.maxParallel == 0},
		irRow{label: "Members", value: memberNames(s.members), applies: s.kind == KindParallel, zero: len(s.members) == 0},
		irRow{label: "Body", value: mdCodeListOr(s.body, "none"), applies: s.kind == KindLoop, zero: len(s.body) == 0},
		irRow{label: "Until", value: conditionText(s.hasUntil, s.untilStep, s.untilValue), applies: s.kind == KindLoop, zero: !s.hasUntil && s.untilStep == "" && !s.untilValue},
		irRow{label: "Max iterations", value: strconv.Itoa(s.maxIterations), applies: s.kind == KindLoop, zero: s.maxIterations == 0},
		irRow{label: "Optional", value: yesNo(s.optional), applies: stepKind && !member, zero: !s.optional},
		irRow{label: "Skip on failure", value: mdCodeListOr(s.skipOnFailure, "none"), applies: stepKind && !member, zero: len(s.skipOnFailure) == 0},
		irRow{label: "Skip if", value: conditionText(s.hasSkipIf, s.skipIfStep, s.skipIfValue), applies: !member, zero: !s.hasSkipIf && s.skipIfStep == "" && !s.skipIfValue},
	)
	return rows
}

// mermaid renders the flowchart of the graph
func (r *irRenderer) mermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")
	fmt.Fprintf(&b, "    %s([\"start\"])\n", mermaidStartID)

	// Declare every node in walk order, grouping each loop's body with the loop node that repeats it
	emitted := map[*stepDef]bool{}
	for _, s := range r.ir.steps {
		loop := r.loopOf[s.name]
		if s.kind == KindLoop {
			loop = s
		}
		if loop != nil {
			r.mermaidLoop(&b, loop, emitted)
		}
		if !emitted[s] {
			r.mermaidNode(&b, s, "    ")
			emitted[s] = true
		}
	}
	fmt.Fprintf(&b, "    %s([\"end\"])\n", mermaidEndID)

	// Control flow follows the top-level order, which is the walk the engine makes, and a loop's exit is labelled so it reads apart from its back edge
	prev := mermaidStartID
	var prevStep *stepDef
	for _, s := range r.ir.steps {
		mermaidEdge(&b, prev, r.ids[s], "-->", exitLabel(prevStep))
		prev = r.ids[s]
		prevStep = s
	}
	mermaidEdge(&b, prev, mermaidEndID, "-->", exitLabel(prevStep))
	for _, s := range r.ir.steps {
		if s.kind != KindLoop || len(s.body) == 0 {
			continue
		}
		first := r.byName[s.body[0]]
		if first != nil {
			mermaidEdge(&b, r.ids[s], r.ids[first], "-->", "repeat")
		}
	}

	// Data dependencies and conditions are dotted, so they read as what a step consumes rather than as where the walk goes next
	var optional []string
	r.walk(func(s *stepDef) {
		r.referenceEdge(&b, s.itemsFrom, s, "items")
		for _, name := range s.inputFrom {
			r.referenceEdge(&b, name, s, "input")
		}
		if s.hasSkipIf {
			r.referenceEdge(&b, s.skipIfStep, s, "skip if "+strconv.FormatBool(s.skipIfValue))
		}
		for _, name := range s.skipOnFailure {
			target := r.byName[name]
			if target != nil {
				mermaidEdge(&b, r.ids[s], r.ids[target], "-.->", "skip on failure")
			}
		}
		if s.optional {
			optional = append(optional, r.ids[s])
		}
	})
	if r.ir.outputStep != "" {
		source := r.byName[r.ir.outputStep]
		if source != nil {
			mermaidEdge(&b, r.ids[source], mermaidEndID, "-.->", "output")
		}
	}

	// An optional step's failure costs the instance nothing, which a dashed border marks
	if len(optional) > 0 {
		b.WriteString("    classDef optional stroke-dasharray: 5 5\n")
		fmt.Fprintf(&b, "    class %s optional\n", strings.Join(optional, ","))
	}

	return b.String()
}

// mermaidLoop declares a loop's body steps and the loop node inside one subgraph, once
func (r *irRenderer) mermaidLoop(b *strings.Builder, loop *stepDef, emitted map[*stepDef]bool) {
	if emitted[loop] {
		return
	}

	fmt.Fprintf(b, "    subgraph %s_loop[\"%s\"]\n", r.ids[loop], mermaidText("loop "+loop.name))
	for _, name := range loop.body {
		m := r.byName[name]
		if m == nil || emitted[m] || m.kind == KindLoop {
			continue
		}
		r.mermaidNode(b, m, "        ")
		emitted[m] = true
	}
	r.mermaidNode(b, loop, "        ")
	emitted[loop] = true
	b.WriteString("    end\n")
}

// mermaidNode declares one step, with a shape that says what kind of node it is
func (r *irRenderer) mermaidNode(b *strings.Builder, s *stepDef, indent string) {
	id := r.ids[s]

	// A group is a subgraph whose members sit side by side, since a top-down layout places nodes of the same rank in a row
	// Its title is kept short, because Mermaid wraps a subgraph title at a fixed width and draws the wrapped lines over the members, so failure policies are left to the Markdown tables
	if s.kind == KindParallel {
		fmt.Fprintf(b, "%ssubgraph %s[\"%s\"]\n", indent, id, mermaidText(s.name+" (parallel)"))
		for _, m := range s.members {
			r.mermaidNode(b, m, indent+"    ")
		}
		fmt.Fprintf(b, "%send\n", indent)
		return
	}

	// The second and later lines say what the node does beyond running its handler
	lines := []string{s.name}
	switch s.kind {
	case KindForEach:
		lines = append(lines, "for each item of "+s.itemsFrom)
		if s.child != nil {
			lines = append(lines, "child "+childRefPlain(s.child))
		}
		if s.maxParallel > 0 {
			lines = append(lines, fmt.Sprintf("at most %d in flight", s.maxParallel))
		}
	case KindChild:
		lines = append(lines, "child "+childRefPlain(s.child))
	case KindWait:
		wait := "waits for event " + s.eventName
		if s.eventTimeout > 0 {
			wait += ", up to " + s.eventTimeout.String()
		}
		lines = append(lines, wait)
	case KindLoop:
		lines = append(lines, fmt.Sprintf("until %s = %t", s.untilStep, s.untilValue), fmt.Sprintf("at most %d iterations", s.maxIterations))
	case KindStep:
		// A plain step's name says all there is to say about its shape
	default:
		lines = append(lines, "unknown kind "+string(s.kind))
	}
	if s.isCompensable() {
		lines = append(lines, "↺ compensable")
	}
	if s.capability != "" {
		lines = append(lines, "requires "+s.capability)
	}

	escaped := make([]string, len(lines))
	for i, line := range lines {
		escaped[i] = mermaidText(line)
	}
	label := strings.Join(escaped, "<br/>")

	open, closing := "[\"", "\"]"
	switch s.kind {
	case KindForEach:
		open, closing = "[/\"", "\"\\]"
	case KindChild:
		open, closing = "[[\"", "\"]]"
	case KindWait:
		open, closing = ">\"", "\"]"
	case KindLoop:
		open, closing = "{\"", "\"}"
	}
	fmt.Fprintf(b, "%s%s%s%s%s\n", indent, id, open, label, closing)
}

// referenceEdge draws a dotted edge from a referenced step to the step that reads it, when the reference resolves
func (r *irRenderer) referenceEdge(b *strings.Builder, from string, to *stepDef, label string) {
	if from == "" {
		return
	}
	source := r.byName[from]
	if source == nil {
		return
	}
	mermaidEdge(b, r.ids[source], r.ids[to], "-.->", label)
}

// walk visits every step and member in declaration order
func (r *irRenderer) walk(fn func(s *stepDef)) {
	var visit func(steps []*stepDef)
	visit = func(steps []*stepDef) {
		for _, s := range steps {
			fn(s)
			visit(s.members)
		}
	}
	visit(r.ir.steps)
}

// exitLabel labels the edge that leaves a loop node going forward, which is taken once the loop's condition holds
func exitLabel(prev *stepDef) string {
	if prev != nil && prev.kind == KindLoop {
		return "done"
	}
	return ""
}

// mermaidEdge writes one edge, with an optional label
func mermaidEdge(b *strings.Builder, from string, to string, arrow string, label string) {
	if label == "" {
		fmt.Fprintf(b, "    %s %s %s\n", from, arrow, to)
		return
	}
	fmt.Fprintf(b, "    %s %s|\"%s\"| %s\n", from, arrow, mermaidText(label), to)
}

// mermaidText escapes a value for a quoted Mermaid label, using numeric entity codes for every character that could end the label or start markup
func mermaidText(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '"', '#', '&', '<', '>', '`', '|':
			fmt.Fprintf(&b, "#%d;", c)
		case '\n', '\r', '\t':
			b.WriteByte(' ')
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// writeMDRow writes one row of a two-column table, escaping what would end a cell
func writeMDRow(b *strings.Builder, label string, value string) {
	fmt.Fprintf(b, "| %s | %s |\n", mdCell(label), mdCell(value))
}

// mdCell escapes a pipe, which ends a table cell even inside a code span
func mdCell(s string) string {
	return strings.ReplaceAll(s, "|", `\|`)
}

// mdCode renders a value as an inline code span, with a fence longer than any run of backticks inside it
func mdCode(s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	if s == "" {
		return "(empty)"
	}

	var longest, run int
	for _, c := range s {
		if c != '`' {
			run = 0
			continue
		}
		run++
		longest = max(longest, run)
	}

	// A code span that starts or ends with a backtick needs a space between it and the fence
	pad := ""
	if strings.HasPrefix(s, "`") || strings.HasSuffix(s, "`") {
		pad = " "
	}
	fence := strings.Repeat("`", longest+1)
	return fence + pad + s + pad + fence
}

// mdCodeOr renders a value as code, or the fallback text when it is empty
func mdCodeOr(s string, fallback string) string {
	if s == "" {
		return fallback
	}
	return mdCode(s)
}

// mdCodeList renders names as a comma-separated list of code spans
func mdCodeList(names []string) string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = mdCode(name)
	}
	return strings.Join(out, ", ")
}

// mdCodeListOr renders names as a list of code spans, or the fallback text when there are none
func mdCodeListOr(names []string, fallback string) string {
	if len(names) == 0 {
		return fallback
	}
	return mdCodeList(names)
}

// memberNames lists the members of a parallel group
func memberNames(members []*stepDef) string {
	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.name
	}
	return mdCodeListOr(names, "none")
}

// conditionText describes a condition that reads a step's boolean output, as WithSkipIf and WithUntil do
func conditionText(set bool, step string, value bool) string {
	if !set && step == "" {
		return "never"
	}
	text := fmt.Sprintf("when %s outputs %t", mdCode(step), value)
	if !set {
		text += " (recorded, but not enabled)"
	}
	return text
}

// childRefText describes the definition a child step starts, for a table cell
func childRefText(c *childDefinitionIR) string {
	if c == nil {
		return "none"
	}
	return fmt.Sprintf("%s, version %d", mdCode(c.name), c.version)
}

// childRefPlain describes the definition a child step starts, for a diagram label
func childRefPlain(c *childDefinitionIR) string {
	if c == nil {
		return "(missing)"
	}
	return fmt.Sprintf("%s v%d", c.name, c.version)
}

// backoffText describes a doubling backoff
func backoffText(initial time.Duration, maxDelay time.Duration) string {
	return fmt.Sprintf("%s, doubling up to %s", initial, maxDelay)
}

// durationOr renders a positive duration, or the fallback text when it is not set
func durationOr(d time.Duration, fallback string) string {
	if d <= 0 {
		return fallback
	}
	return d.String()
}

// countOr renders a positive count, or the fallback text when it is not set
func countOr(n int, fallback string) string {
	if n <= 0 {
		return fallback
	}
	return strconv.Itoa(n)
}

// formatBytes renders a size in bytes, adding the binary unit when it divides evenly
func formatBytes(n int) string {
	switch {
	case n > 0 && n%(1<<20) == 0:
		return fmt.Sprintf("%d bytes (%d MiB)", n, n>>20)
	case n > 0 && n%(1<<10) == 0:
		return fmt.Sprintf("%d bytes (%d KiB)", n, n>>10)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// yesNo renders a flag
func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

// kindIn reports whether a kind is one of the given ones
func kindIn(k Kind, kinds ...Kind) bool {
	return slices.Contains(kinds, k)
}
