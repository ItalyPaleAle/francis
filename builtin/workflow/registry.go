package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
	"github.com/italypaleale/francis/internal/builtinkey"
)

// registryState is what the definition registry holds: the fingerprint first recorded for each version of the graph
type registryState struct {
	// Versions holds one entry per recorded version, and an entry is never overwritten, so the first deployment of a version defines it
	// It is a slice rather than a map keyed by version because a map with integer keys cannot survive the generic decode a cross-host response goes through
	Versions []registryEntry `msgpack:"versions,omitempty"`
	// NextGeneration is retained across resets so a revoked start identity can never become current again
	NextGeneration uint64 `msgpack:"nextGeneration,omitempty"`
}

// registryEntry is one version's recorded definition
type registryEntry struct {
	Version     int       `msgpack:"version"`
	Fingerprint string    `msgpack:"fingerprint"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt"`
	Generation  uint64    `msgpack:"generation,omitempty"`
}

// find returns the entry recorded for a version, or nil when the version is unknown
func (st *registryState) find(version int) *registryEntry {
	for i := range st.Versions {
		if st.Versions[i].Version == version {
			return &st.Versions[i]
		}
	}
	return nil
}

// registerRequest asks the registry whether this host's graph is the one recorded for a version
type registerRequest struct {
	Version     int    `msgpack:"version"`
	Fingerprint string `msgpack:"fingerprint"`
	// Generation makes this a confirmation of an already authorized start instead of a registration
	Generation uint64 `msgpack:"generation,omitempty"`
}

// registerResponse answers the consistency check, carrying what was recorded so a conflict can name both fingerprints
type registerResponse struct {
	Found       bool      `msgpack:"found"`
	OK          bool      `msgpack:"ok"`
	Fingerprint string    `msgpack:"fingerprint,omitempty"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt,omitzero"`
	Generation  uint64    `msgpack:"generation,omitempty"`
}

// forgetRequest removes a version that was registered wrongly and has no instances left
type forgetRequest struct {
	Version                int    `msgpack:"version"`
	ReplacementFingerprint string `msgpack:"replacementFingerprint,omitempty"`
}

// DefinitionInfo describes one version the registry holds, as returned by Definitions
type DefinitionInfo struct {
	// Version is the version number the definition was registered under
	Version int
	// Fingerprint is the hash of the graph first registered for the version
	Fingerprint string
	// FirstSeenAt is when the version was first recorded
	FirstSeenAt time.Time
	// Conflicts reports whether this host's own definition disagrees with what is recorded, which is the operator's view of a version bump that was forgotten
	Conflicts bool
}

// definitionsResponse carries what the registry holds back to the caller
type definitionsResponse struct {
	Entries []registryEntry `msgpack:"entries,omitempty"`
}

// registryActor is the cluster-wide singleton that records each version's definition fingerprint, so two hosts cannot serve different graphs under the same version
type registryActor struct {
	client       actor.Client[registryState]
	workflowType string
	svc          *actor.Service
}

// newRegistryActor builds the registry singleton
func newRegistryActor(bareType string, actorID string, svc *actor.Service) actor.Actor {
	return &registryActor{
		client:       builtinactor.NewClient[registryState](bareType, actorID, svc),
		workflowType: strings.TrimSuffix(bareType, registryTypeSuffix),
		svc:          svc,
	}
}

// Invoke answers the registry's three questions: register a version, list what is held, and forget one
func (r *registryActor) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	switch method {
	case methodRegister:
		return r.register(ctx, data)
	case methodDefinitions:
		return r.definitions(ctx)
	case methodForget:
		return nil, r.forget(ctx, data)
	default:
		return nil, fmt.Errorf("unknown workflow registry method %q", method)
	}
}

// Peek checks a known version under the registry's shared read lock so established workflows do not serialize every delivery
func (r *registryActor) Peek(ctx context.Context, method string, data actor.Envelope) (any, error) {
	if method != methodCheck {
		return nil, fmt.Errorf("unknown workflow registry peek method %q", method)
	}
	return r.check(ctx, data)
}

// check returns the recorded decision without creating one, leaving an unknown version for register's exclusive turn
func (r *registryActor) check(ctx context.Context, data actor.Envelope) (any, error) {
	var req registerRequest
	err := decodePayload(data, &req)
	if err != nil {
		return nil, err
	}

	st, err := r.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the registry state: %w", err)
	}

	existing := st.find(req.Version)
	if existing == nil {
		return registerResponse{}, nil
	}

	return registerResponse{
		Found:       true,
		OK:          existing.Fingerprint == req.Fingerprint,
		Fingerprint: existing.Fingerprint,
		FirstSeenAt: existing.FirstSeenAt,
		Generation:  existing.Generation,
	}, nil
}

// register records a version's fingerprint the first time it is seen, and otherwise answers whether the caller's matches
// It never overwrites, so the first deployment of a version defines it and every later disagreement is reported as a conflict
func (r *registryActor) register(ctx context.Context, data actor.Envelope) (any, error) {
	var req registerRequest
	err := decodePayload(data, &req)
	if err != nil {
		return nil, err
	}

	st, err := r.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the registry state: %w", err)
	}

	existing := st.find(req.Version)
	// Confirm only the exact identity granted before the journal write, without ever reclaiming a forgotten generation
	if req.Generation > 0 {
		if existing == nil {
			return registerResponse{}, nil
		}
		return registerResponse{
			Found: true, OK: existing.Fingerprint == req.Fingerprint && existing.Generation == req.Generation,
			Fingerprint: existing.Fingerprint, FirstSeenAt: existing.FirstSeenAt, Generation: existing.Generation,
		}, nil
	}
	if existing != nil {
		// Upgrade a legacy entry before handing out an identity that can be confirmed after persistence
		if existing.Fingerprint == req.Fingerprint && existing.Generation == 0 {
			st.Versions = append([]registryEntry(nil), st.Versions...)
			existing = st.find(req.Version)
			st.NextGeneration++
			existing.Generation = st.NextGeneration
			err = r.client.SetState(ctx, st, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to upgrade the definition identity: %w", err)
			}
		}
		return registerResponse{
			Found:       true,
			OK:          existing.Fingerprint == req.Fingerprint,
			Fingerprint: existing.Fingerprint,
			FirstSeenAt: existing.FirstSeenAt,
			Generation:  existing.Generation,
		}, nil
	}

	st.NextGeneration++
	entry := registryEntry{
		Version:     req.Version,
		Fingerprint: req.Fingerprint,
		FirstSeenAt: time.Now(),
		Generation:  st.NextGeneration,
	}
	st.Versions = append(append([]registryEntry(nil), st.Versions...), entry)

	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to record the definition: %w", err)
	}

	return registerResponse{Found: true, OK: true, Fingerprint: entry.Fingerprint, FirstSeenAt: entry.FirstSeenAt, Generation: entry.Generation}, nil
}

// definitions returns every version the registry holds, which is the operator's view of what has been deployed
func (r *registryActor) definitions(ctx context.Context) (any, error) {
	st, err := r.client.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the registry state: %w", err)
	}
	return definitionsResponse{Entries: st.Versions}, nil
}

// forget removes a version, which is the operator's reset for one that was registered wrongly and has no instances left
func (r *registryActor) forget(ctx context.Context, data actor.Envelope) error {
	var req forgetRequest
	err := decodePayload(data, &req)
	if err != nil {
		return err
	}
	if req.Version <= 0 {
		return errors.New("definition version must be positive")
	}

	// This listing and the registry update share the registry's exclusive turn with start confirmations
	// A start that persists after this listing must confirm after the reset and cannot dispatch under its revoked identity
	opts := &actor.ListStatesOpts{Limit: 1}
	opts.SetWorkflowLabels(builtinkey.Key{}, components.WorkflowLabels{Version: req.Version})
	instances := builtinactor.NewClient[instanceState](r.workflowType, "", r.svc)
	page, err := instances.ListStates(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to check instances before forgetting the definition: %w", err)
	}
	if len(page.States) > 0 {
		return fmt.Errorf("%w: version %d", ErrVersionInUse, req.Version)
	}

	st, err := r.client.GetState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the registry state: %w", err)
	}

	kept := make([]registryEntry, 0, len(st.Versions))
	for _, entry := range st.Versions {
		if entry.Version != req.Version {
			kept = append(kept, entry)
		}
	}
	if len(kept) == len(st.Versions) && req.ReplacementFingerprint == "" {
		return nil
	}
	if req.ReplacementFingerprint != "" {
		st.NextGeneration++
		kept = append(kept, registryEntry{
			Version: req.Version, Fingerprint: req.ReplacementFingerprint,
			FirstSeenAt: time.Now(), Generation: st.NextGeneration,
		})
	}

	st.Versions = kept
	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return fmt.Errorf("failed to forget the definition: %w", err)
	}
	return nil
}

// registryInvoke reaches the registry singleton under its exclusive turn, waiting out a placement that is still settling
// The singleton moves when a host joins or leaves, so a caller arriving mid-move gets a transient condition rather than an answer, and failing on it would fail the start that asked
func (w *Workflow) registryInvoke(ctx context.Context, svc *actor.Service, method string, req any) (actor.Envelope, error) {
	return retryWhilePlacementMoves(ctx, func(ctx context.Context) (actor.Envelope, error) {
		return builtinactor.Invoke(ctx, svc, w.registryType(), method, req)
	})
}

// registryPeek is the read-side counterpart of registryInvoke, and waits out a moving placement the same way
func (w *Workflow) registryPeek(ctx context.Context, svc *actor.Service, method string, req any) (actor.Envelope, error) {
	return retryWhilePlacementMoves(ctx, func(ctx context.Context) (actor.Envelope, error) {
		return builtinactor.Peek(ctx, svc, w.registryType(), actor.SingletonActorID, method, req)
	})
}

// authorizeDefinition grants a durable identity to a start before its journal exists
func (w *Workflow) authorizeDefinition(ctx context.Context, svc *actor.Service) (registerResponse, error) {
	return w.confirmDefinition(ctx, svc, registerRequest{Version: w.def.version, Fingerprint: w.def.fingerprint})
}

// confirmDefinition serializes start authorization and post-persist confirmation with registry resets
func (w *Workflow) confirmDefinition(ctx context.Context, svc *actor.Service, req registerRequest) (registerResponse, error) {
	env, err := w.registryInvoke(ctx, svc, methodRegister, req)
	if err != nil {
		return registerResponse{}, fmt.Errorf("failed to authorize the workflow definition: %w", err)
	}
	var resp registerResponse
	err = env.Decode(&resp)
	if err != nil {
		return registerResponse{}, fmt.Errorf("failed to decode the definition authorization: %w", err)
	}
	if !resp.OK {
		w.recordDefinitionConflict(ctx, req.Version, req.Fingerprint, resp)
		return resp, ErrDefinitionConflict
	}
	if resp.Generation == 0 {
		return resp, errRegistryGenerationUnavailable
	}
	return resp, nil
}

// serveVersion reports whether this host may serve a version, consulting the registry so an operator reset takes effect on every live host
func (w *Workflow) serveVersion(ctx context.Context, svc *actor.Service, version int) (bool, error) {
	return w.askRegistry(ctx, svc, version)
}

// askRegistry checks known versions concurrently, registering an unknown one under the registry's exclusive turn
func (w *Workflow) askRegistry(ctx context.Context, svc *actor.Service, version int) (bool, error) {
	// Only the version this host's code defines can be checked against this host's fingerprint
	// An instance stamped with another version is declined by the caller on the version mismatch itself, without consulting the registry
	if version != w.def.version {
		return false, nil
	}

	req := registerRequest{
		Version:     version,
		Fingerprint: w.def.fingerprint,
	}
	res, err := w.registryPeek(ctx, svc, methodCheck, req)
	registered := false
	if errors.Is(err, actorcore.ErrActorMethodUnsupported) {
		// A rolling deployment may place the singleton on an older host that does not implement the read path yet
		res, err = w.registryInvoke(ctx, svc, methodRegister, req)
		registered = true
	}
	if err != nil {
		return false, fmt.Errorf("failed to check the workflow definition registry: %w", err)
	}

	var resp registerResponse
	err = res.Decode(&resp)
	if err != nil {
		return false, fmt.Errorf("failed to decode the registry response: %w", err)
	}
	if registered {
		resp.Found = true
	}

	if !resp.Found {
		res, err = w.registryInvoke(ctx, svc, methodRegister, req)
		if err != nil {
			return false, fmt.Errorf("failed to register the workflow definition: %w", err)
		}
		err = res.Decode(&resp)
		if err != nil {
			return false, fmt.Errorf("failed to decode the registry response: %w", err)
		}
	}

	if resp.OK {
		return true, nil
	}

	w.recordDefinitionConflict(ctx, version, w.def.fingerprint, resp)
	return false, nil
}

// recordDefinitionConflict preserves deployment diagnostics for both registry decisions and local graph fences
func (w *Workflow) recordDefinitionConflict(ctx context.Context, version int, localFingerprint string, resp registerResponse) {
	w.metrics.definitionConflicts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", w.name),
		attribute.Int("version", version),
	))
	if w.log != nil {
		w.log.ErrorContext(ctx, "Workflow definition conflicts with the one registered for this version; declining its jobs",
			slog.Int("version", version),
			slog.String("registeredFingerprint", resp.Fingerprint),
			slog.String("localFingerprint", localFingerprint),
			slog.Time("firstSeenAt", resp.FirstSeenAt),
		)
	}
}
