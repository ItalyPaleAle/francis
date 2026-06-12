# Coding Style Guidelines

## Go

Never define variables inside `if` conditions. Always declare variables on a separate line before the conditional check.

```go
// Wrong
if err := something(); err != nil { ... }

// Wrong
if val, ok := something.(string); ok { ... }

// Right
err := something()
if err != nil { ... }

// Right
val, ok := something.(string)
if ok { ... }
```

If you modify `pkg/config.Config` or any struct referenced from it, always run `make gen-config` before finishing the task.

## Comments

- Exactly one sentence per line
- There is NO maximum line width: never wrap a single sentence across multiple comment lines, no matter how long that sentence is
- A new line in a comment means a new sentence; a wrapped line does not exist
- No trailing period on single-line comments
- Prefer comments that explain intent, invariants, or why a branch exists
- Avoid comments that simply restate the next line of code
- For multi-step logic, use short section comments to separate the steps and explain why each step exists
- Inside a function, put a one-sentence comment above each major action; the comments double as visual separators between sections and should say what the step does and why, not how
- Favor a few well-placed section comments over a wall of code; a reader should be able to skim the comments and understand the method's flow

```go
// Wrong — one sentence wrapped across multiple lines
// This function performs the main validation logic. It checks
// the input against the schema and returns an error if the
// input is invalid.

// Wrong — trailing period on single-line comment
// Validate the input.

// Right — one sentence per line, each line as long as it needs to be
// This function performs the main validation logic
// It checks the input against the schema and returns an error if the input is invalid

// Right
// Validate the input

// Right
// Normalize the request host so callers can pass either Host or X-Forwarded-Host values

// Right
// Browsers do not accept a cookie Domain attribute set to an IP address
// Returning an empty domain tells the caller to set a host-only cookie instead

// Wrong — restates the code
// Trim whitespace and lowercase the host
host = strings.TrimSpace(strings.ToLower(host))
```

Section comments inside a function — one sentence per major action, describing what and why, acting as visual separators:

```go
func (rc *runtimeClient) doRequest(ctx context.Context, kind string, payload any, out any) error {
	// Snapshot the live session and identity
	// Without a session there is nowhere to send the request
	session, hostID, sessionID := rc.snapshot()
	if session == nil {
		return errNotConnected
	}

	// Build the request and stamp our identity so the runtime can reject it if our session was superseded
	req, err := protocol.NewRequest(kind, payload)
	if err != nil {
		return err
	}
	req.HostID = hostID
	req.SessionID = sessionID

	// Each request gets its own stream, which WebTransport multiplexes over the connection
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream to runtime: %w", err)
	}
	defer stream.Close()

	// Send the request and wait for the correlated response
	resp, err := protocol.RoundTrip(ctx, stream, req)
	if err != nil {
		return err
	}

	// Surface a structured runtime error as the returned error
	perr, isErr := resp.AsError()
	if isErr {
		return perr
	}
	return resp.DecodePayload(out)
}
```

## Git

Do not stage or unstage changes unless the user explicitly asks you to.
