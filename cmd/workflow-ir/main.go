// Command workflow-ir prints a workflow definition's canonical IR in a human-readable form
//
// It reads the bytes returned by workflow.Workflow.CanonicalIR, either raw or encoded as hex or base64, from a file or from standard input, and prints them as a Markdown document or as a Mermaid flowchart
// The output is deterministic, so diffing the renderings of two definitions shows why their fingerprints differ
//
// See builtin/workflow/design/02-definitions-and-ir.md for how the IR is built and what it contains
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/italypaleale/francis/builtin/workflow"
)

// Input encodings accepted by the -encoding flag
const (
	encodingAuto   = "auto"
	encodingBinary = "binary"
	encodingHex    = "hex"
	encodingBase64 = "base64"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run is the whole command, taking its streams as arguments so tests can drive it, and returns the process's exit code
func run(args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("workflow-ir", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", string(workflow.IRFormatMarkdown), "output format: \"markdown\" or \"mermaid\"")
	encoding := fs.String("encoding", encodingAuto, "input encoding: \"auto\", \"binary\", \"hex\", or \"base64\"")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: workflow-ir [flags] [file]\n\n")
		fmt.Fprintf(fs.Output(), "Prints a workflow definition's canonical IR, as returned by Workflow.CanonicalIR, in a human-readable form.\n")
		fmt.Fprintf(fs.Output(), "Reads the IR from file, or from standard input when file is omitted or \"-\".\n\n")
		fmt.Fprintf(fs.Output(), "Flags:\n")
		fs.PrintDefaults()
	}

	// Parse the flags, where asking for help is a success rather than a usage error
	err := fs.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return 2
	}
	if fs.NArg() > 1 {
		fmt.Fprintf(stderr, "Error: expected at most one file, got %d arguments\n", fs.NArg())
		fs.Usage()
		return 2
	}

	// Read the whole input, which is small: an IR is a few kilobytes even for a large graph
	raw, err := readInput(fs.Arg(0), stdin)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}

	// Undo whatever text encoding the IR was captured in, then render it
	data, err := decodeInput(raw, *encoding)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	out, err := workflow.RenderIR(data, workflow.IRFormat(*format))
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}

	_, err = io.WriteString(stdout, out)
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to write the output: %v\n", err)
		return 1
	}
	return 0
}

// readInput reads the named file, or standard input for an empty name or "-"
func readInput(name string, stdin io.Reader) ([]byte, error) {
	if name == "" || name == "-" {
		data, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("failed to read standard input: %w", err)
		}
		return data, nil
	}

	data, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", name, err)
	}
	return data, nil
}

// decodeInput turns the input into raw IR bytes according to the requested encoding
func decodeInput(raw []byte, encoding string) ([]byte, error) {
	switch encoding {
	case encodingBinary:
		return raw, nil
	case encodingHex:
		return decodeHex(raw)
	case encodingBase64:
		return decodeBase64(raw)
	case encodingAuto:
		return decodeAuto(raw)
	default:
		return nil, fmt.Errorf("unknown encoding %q", encoding)
	}
}

// decodeAuto recognizes raw IR by the MessagePack array header every IR starts with, and otherwise tries hex before base64, since every hex string is also valid base64
func decodeAuto(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("the input is empty")
	}

	// The canonical IR is a MessagePack array, whose header is a fixarray or an array16 or array32 marker, none of which is a printable character
	first := raw[0]
	if (first >= 0x90 && first <= 0x9f) || first == 0xdc || first == 0xdd {
		return raw, nil
	}

	data, err := decodeHex(raw)
	if err == nil {
		return data, nil
	}
	data, err = decodeBase64(raw)
	if err == nil {
		return data, nil
	}
	return nil, errors.New("the input is neither raw IR bytes nor hex or base64 text; set -encoding to say which it is")
}

// decodeHex decodes hex text, ignoring surrounding whitespace
func decodeHex(raw []byte) ([]byte, error) {
	data, err := hex.DecodeString(string(bytes.TrimSpace(raw)))
	if err != nil {
		return nil, fmt.Errorf("failed to decode the input as hex: %w", err)
	}
	return data, nil
}

// decodeBase64 decodes standard or URL-safe base64 text, padded or not, ignoring whitespace anywhere in it so wrapped output can be pasted as is
func decodeBase64(raw []byte) ([]byte, error) {
	text := strings.Join(strings.Fields(string(raw)), "")
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		data, err := enc.DecodeString(text)
		if err == nil {
			return data, nil
		}
	}
	return nil, errors.New("failed to decode the input as base64")
}
