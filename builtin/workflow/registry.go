package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/builtinactor"
)

// registryState is what the definition registry holds: the fingerprint first recorded for each version of the graph
type registryState struct {
	// Versions holds one entry per recorded version, and an entry is never overwritten, so the first deployment of a version defines it
	// It is a slice rather than a map keyed by version because a map with integer keys cannot survive the generic decode a cross-host response goes through
	Versions []registryEntry `msgpack:"versions,omitempty"`
}

// registryEntry is one version's recorded definition
type registryEntry struct {
	Version     int       `msgpack:"version"`
	Fingerprint string    `msgpack:"fingerprint"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt"`
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
}

// registerResponse answers the consistency check, carrying what was recorded so a conflict can name both fingerprints
type registerResponse struct {
	Found       bool      `msgpack:"found"`
	OK          bool      `msgpack:"ok"`
	Fingerprint string    `msgpack:"fingerprint,omitempty"`
	FirstSeenAt time.Time `msgpack:"firstSeenAt,omitzero"`
}

// forgetRequest removes a version that was registered wrongly and has no instances left
type forgetRequest struct {
	Version int `msgpack:"version"`
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
	client actor.Client[registryState]
}

// newRegistryActor builds the registry singleton
func newRegistryActor(bareType string, actorID string, svc *actor.Service) actor.Actor {
	return &registryActor{
		client: builtinactor.NewClient[registryState](bareType, actorID, svc),
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
	if existing != nil {
		return registerResponse{
			Found:       true,
			OK:          existing.Fingerprint == req.Fingerprint,
			Fingerprint: existing.Fingerprint,
			FirstSeenAt: existing.FirstSeenAt,
		}, nil
	}

	entry := registryEntry{
		Version:     req.Version,
		Fingerprint: req.Fingerprint,
		FirstSeenAt: time.Now(),
	}
	st.Versions = append(st.Versions, entry)

	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to record the definition: %w", err)
	}

	return registerResponse{Found: true, OK: true, Fingerprint: entry.Fingerprint, FirstSeenAt: entry.FirstSeenAt}, nil
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
	if len(kept) == len(st.Versions) {
		return nil
	}

	st.Versions = kept
	err = r.client.SetState(ctx, st, nil)
	if err != nil {
		return fmt.Errorf("failed to forget the definition: %w", err)
	}
	return nil
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
	res, err := builtinactor.Peek(ctx, svc, w.registryType(), actor.SingletonActorID, methodCheck, req)
	registered := false
	if errors.Is(err, actorcore.ErrActorMethodUnsupported) {
		// A rolling deployment may place the singleton on an older host that does not implement the read path yet
		res, err = builtinactor.Invoke(ctx, svc, w.registryType(), methodRegister, req)
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
		res, err = builtinactor.Invoke(ctx, svc, w.registryType(), methodRegister, req)
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

	// The failure mode is loud by design: whichever side deployed second conflicts, the metric fires within one turn, nothing corrupts, and the fix is a version bump
	w.metrics.definitionConflicts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", w.name),
		attribute.Int("version", version),
	))
	if w.log != nil {
		w.log.ErrorContext(ctx, "Workflow definition conflicts with the one registered for this version; declining its jobs",
			slog.Int("version", version),
			slog.String("registeredFingerprint", resp.Fingerprint),
			slog.String("localFingerprint", w.def.fingerprint),
			slog.Time("firstSeenAt", resp.FirstSeenAt),
		)
	}
	return false, nil
}
